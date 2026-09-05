# Storage Architecture

Status: Architecture Foundation. No storage engine is implemented yet.

This document defines the physical, low-level durable storage
primitives ChronicleDB will build on. It does not define log-record
semantics for Raft/WAL (see [`docs/wal.md`](wal.md)) or MVCC data
layout (see [`docs/mvcc.md`](mvcc.md)); it defines the substrate both
are built on: `internal/storage`.

## 1. Design direction

ChronicleDB uses the smallest technically real storage design that
supports its correctness goals:

- **Append-oriented durable storage.** All durable writes are appends
  to the end of a file. There is no in-place mutation of durably
  written bytes.
- **Explicit record encoding and framing.** Every durable unit of data
  has an explicit, versioned binary format with a length and a
  checksum. Nothing is persisted as an unframed byte blob.
- **Checksums on every record.** Corruption must be detectable, not
  merely assumed absent.
- **Monotonically ordered entries.** Records are assigned a
  monotonically increasing sequence number (segment-relative offset
  plus a logical index) so that ordering is a property of the storage
  layer, not something layered on top of it later.
- **Deterministic replay.** Reading a storage segment back, in order,
  from the start, must always reconstruct the same sequence of records
  a correct writer produced, or must fail closed at a well-defined
  point (see [`docs/wal.md`](wal.md) §Corruption and Truncation Rules).
- **In-memory materialized state**, rebuilt from durable history at
  startup; ChronicleDB V1 does not persist B-tree or LSM structures for
  the MVCC store itself. The durable log plus periodic state-machine
  snapshots (see [`docs/snapshots.md`](snapshots.md)) are the only
  on-disk truth; the live MVCC index is an in-memory structure rebuilt
  from that truth.

### What V1 deliberately avoids

ChronicleDB does not introduce, in V1:

- B-tree or LSM-tree storage engines with their own compaction,
  leveling, or page-cache machinery.
- A general-purpose buffer manager / page cache abstraction.
- A distributed query optimizer or a cost-based planner.
- Arbitrary PostgreSQL on-disk or wire compatibility.

If a future phase needs a more sophisticated storage structure (e.g.
because in-memory MVCC state no longer fits in memory, or point-read
latency on a large keyspace demands an index structure), that
structure must be justified by measured evidence or a concretely
demonstrated architectural requirement, recorded in a new ADR — not
introduced speculatively. See [`docs/non-goals.md`](non-goals.md).

## 2. Ownership boundary: `internal/storage` vs. `internal/wal`

- **`internal/storage`** owns *segment files*: creating them, naming
  them, appending raw framed byte ranges to them, fsyncing them,
  reading byte ranges back, and enumerating/deleting whole segment
  files (e.g. after compaction). It has no opinion about what the
  bytes mean.
- **`internal/wal`** owns *record semantics*: what a record type is
  (Raft log entry, Raft hard state, local metadata), how a record's
  length/checksum/type header is laid out, how logical WAL index
  numbers map to physical segment offsets, and replay/truncation
  policy. `internal/wal` is built on top of `internal/storage` and is
  the only intended consumer of it in V1.

This split exists so that a future second consumer of durable
append-only storage (e.g. a future secondary durable structure) does
not have to go through WAL-specific record semantics, and so that
`internal/wal`'s replay/corruption logic is not tangled with raw file
I/O concerns.

## 3. Segment model

- Durable history for a given log is stored as a sequence of
  **segment files**, each holding a contiguous, bounded range of
  records.
- Segments are immutable once closed: only the current (last) segment
  is ever appended to. Earlier segments are read-only until deleted by
  compaction (see [`docs/snapshots.md`](snapshots.md)).
- A new segment is opened when the current segment reaches a
  configured size threshold. Segment rotation is itself a durable
  operation: the new segment must exist and be fsynced (and, where the
  filesystem requires it, the containing directory fsynced) before the
  old segment is considered closed for writing purposes.
- Segment file naming encodes the first logical index the segment
  contains, so that startup can locate the correct segment for a given
  index without reading every segment's contents.

## 4. Directory layout (conceptual)

```
data/
  meta/                 node identity, format version, Raft hard state
                         pointer (see docs/wal.md)
  wal/
    0000000000000001.seg
    0000000000000482.seg   (current/open segment)
  snapshot/
    <lastIncludedIndex>-<checksum>.snap   (see docs/snapshots.md)
    tmp/                 in-progress snapshot files, never treated as
                         valid until atomically renamed
```

Exact file formats are implementation details of later phases; this
layout exists so that recovery ordering (metadata -> snapshot -> WAL,
see [`docs/recovery.md`](recovery.md)) has a concrete shape to reason
about.

## 5. Append and fsync semantics

- `Append(bytes) -> (offset, error)` writes to the OS page cache for
  the current segment's file handle. This is **appended**, not
  **persisted** (see [`docs/architecture.md`](architecture.md) §4).
- `Sync() -> error` issues an `fsync` (or platform equivalent) on the
  current segment file. Only after `Sync()` returns successfully are
  the appended bytes since the last successful `Sync()` **persisted**.
- Callers that require durability (all WAL callers, per
  [`docs/wal.md`](wal.md)) must not treat `Append` as sufficient; they
  must wait for the corresponding `Sync()` to complete before treating
  the write as durable. This is the mechanical basis for the
  durability contract in [`docs/replication.md`](replication.md).
- `internal/storage` does not batch or delay `Sync()` calls on its own
  initiative in a way that would hide latency from callers; batching
  policy (e.g. group commit) is a `internal/wal`-level decision made
  explicit in [`docs/wal.md`](wal.md), so that the durability boundary
  stays visible rather than buried in an opaque storage layer.

## 6. Checksums

- Every appended record carries a checksum over its own framed bytes
  (header + payload), computed by `internal/wal` and verified by
  `internal/storage`'s read path (or by `internal/wal` immediately
  after `internal/storage` returns the bytes — the exact layering is
  an implementation detail, but verification is mandatory before any
  record is handed to a caller).
- A checksum failure is never silently ignored. See
  [`docs/wal.md`](wal.md) §Corruption and Truncation Rules for the
  exact classification of checksum failures (torn final record vs.
  mid-log corruption) and their consequences.

## 7. Determinism and safety constraints

- `internal/storage` performs no logical interpretation of bytes: it
  must not know about `TxnID`, `RequestID`, SQL, or Raft terms. This
  keeps the lowest layer trivially reusable and testable in isolation.
- All file operations use bounded reads/writes; `internal/storage`
  never trusts a length field from disk without bounding it against
  the remaining bytes in the segment, to avoid over-read panics on
  corrupted input (see [`docs/failure-model.md`](failure-model.md)
  §Security and Safety Expectations).
- No component of `internal/storage` may depend on wall-clock time,
  randomness, or environment variables for correctness; segment
  rotation thresholds are explicit configuration, not derived from
  the clock.

## 8. Open questions deferred by design

- Whether segment files are pre-allocated to a fixed size or grow
  dynamically is an implementation detail deferred to Phase 1; either
  choice must preserve the append/fsync/checksum contract above.
- Disk-full and fsync-failure handling are specified at the policy
  level in [`docs/failure-model.md`](failure-model.md); the exact
  error surfaced by a given OS/filesystem combination is an
  implementation detail validated in Phase 1 testing.
