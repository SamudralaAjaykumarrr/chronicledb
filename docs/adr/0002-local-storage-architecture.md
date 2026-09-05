# ADR-0002: Local Storage Architecture

Status: Accepted

## Context

ChronicleDB needs a durable, local storage substrate for both the
standalone engine and, later, Raft log persistence. The storage
architecture must support correctness (checksums, ordering,
replayability) without introducing complexity the project's actual
requirements don't yet justify.

## Decision

Use **append-oriented durable storage**: immutable segment files,
append-only writes, explicit fsync-based durability boundary, explicit
record framing with checksums on every record, monotonically ordered
entries, and deterministic replay. Full detail in
[`docs/storage.md`](../storage.md).

Explicit split of ownership: `internal/storage` owns raw segment file
mechanics; `internal/wal` owns record semantics built on top of it
(see [`docs/wal.md`](../wal.md) §2). No page cache, buffer manager, or
tree-structured index is introduced in V1.

## Alternatives Considered

1. **A B-tree or LSM-tree storage engine (e.g. embed or reimplement
   something like a simplified RocksDB/BoltDB design) from the
   start.** Rejected for V1: neither is required to prove the
   correctness properties ChronicleDB is built to demonstrate (WAL
   durability, MVCC, transactions, Raft). Both introduce substantial
   independent complexity (compaction strategies, page management,
   free-space tracking) that would compete for engineering attention
   with the actually-hard problems (conflict detection, consensus
   safety) and would obscure, rather than showcase, the storage-engine
   fundamentals this project is meant to demonstrate directly. May be
   reconsidered later if in-memory MVCC state size becomes a proven
   bottleneck (see [`docs/non-goals.md`](../non-goals.md)).
2. **Use an existing embedded KV store (e.g. BoltDB, Badger, Pebble)
   as the durability layer.** Rejected: ChronicleDB's purpose
   explicitly includes demonstrating WAL/durability engineering
   directly (see [`docs/vision.md`](../vision.md)); outsourcing it
   would remove the exact skill the project exists to show, and would
   make it a "wrapper" project, which is an explicit non-goal.
3. **Single monolithic append-only file with no segmentation.**
   Rejected: makes log compaction (deleting old, snapshot-covered
   history) require either in-place truncation (unsafe/complex on most
   filesystems for arbitrary offsets) or full-file rewrites (expensive
   and momentarily doubles disk usage). Segment files make compaction
   a simple, safe file-deletion operation (see
   [`docs/snapshots.md`](../snapshots.md) §8).
4. **In-place mutable storage (e.g. a fixed-size record heap with
   free-list reuse).** Rejected: complicates crash-consistency
   reasoning significantly (in-place writes can be torn without a
   clean "valid prefix" recovery story) compared to append-only, which
   has a simple, well-understood torn-write recovery model (see
   [`docs/wal.md`](../wal.md) §6.1).

## Consequences

- Storage-space reclamation depends entirely on log compaction via
  snapshots ([`docs/snapshots.md`](../snapshots.md)); there is no
  independent in-place-storage compaction to reason about.
- Read performance for point lookups depends on the in-memory MVCC
  index rebuilt at startup, not on-disk index structures; startup time
  scales with the amount of log that must be replayed since the last
  snapshot (an explicit future benchmark target — see
  [`docs/roadmap.md`](../roadmap.md) §Performance Targets).

## Correctness Implications

- Append-only + explicit fsync boundary gives a simple, provable
  durability story: a record is durable iff a `Sync()` covering it has
  returned successfully (see
  [`docs/replication.md`](../replication.md) §1.1).
- Segment immutability (only the current segment is ever appended to)
  means corruption analysis (see [`docs/wal.md`](../wal.md) §6) never
  has to reason about a segment being modified after other parts of it
  were already read and trusted.

## Testing and Proof Obligations

- Component tests for `internal/storage`/`internal/wal` covering
  append/sync/replay round-trips and the torn-tail vs. mid-log
  corruption distinction (`docs/scenario-corpus.md` §Local Durability,
  LD-1 through LD-6).
- Revisiting the "no B-tree/LSM" decision requires a documented,
  measured evidence trail per [`docs/non-goals.md`](../non-goals.md)
  §Sophisticated storage structures ahead of need.
