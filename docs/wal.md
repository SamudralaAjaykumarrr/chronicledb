# Write-Ahead Log (WAL) / Durable Log Architecture

Status: Implemented since Phase 1 (`internal/wal`); see §9 for Phase 1's
implementation-time decisions, §10 for Phase 5's (suffix truncation,
`HardState` read/write support), and §11 for Phase 6's (live snapshot-
pointer/log-compaction support) — these sections resolve specific gaps
this document's earlier drafts left open, without changing anything
else here.

`internal/wal` is the physical persistence mechanism referenced
throughout this repository as "the durable log." It is built on top of
`internal/storage` (see [`docs/storage.md`](storage.md)). This
document defines record format, append/sync semantics, replay, and —
most importantly — the exact, non-negotiable rules for what happens
when the durable log is found to be damaged at startup.

## 1. Why ChronicleDB needs a WAL

Two consumers require an ordered, durable, replayable history that
survives process crashes:

1. **The standalone engine** (Phases 1-3, no Raft yet): every commit
   must be durable before it is acknowledged, and durable history must
   be replayable to reconstruct state after a crash.
2. **Raft** (Phase 4+): a Raft node must persist its log entries and
   its hard state (`currentTerm`, `votedFor`) before it can safely
   grant votes, accept entries, or acknowledge replication to a leader
   — this is required for Raft's safety proof, not an optimization.

Rather than build two separate durability mechanisms, ChronicleDB
builds one: `internal/wal`. The Raft log is persisted *through*
`internal/wal`; it does not get its own, independent physical log. See
[ADR-0003](adr/0003-wal-raft-log-responsibility-model.md).

## 2. Record types

`internal/wal` defines a small, closed set of record types, each
framed identically:

| Type | Payload | Purpose |
|---|---|---|
| `LogEntry` | `(term, index, command bytes)` | One Raft log entry (standalone-mode: `index` is the only ordering key; `term` is fixed/unused until Raft exists). |
| `HardState` | `(currentTerm, votedFor)` | Raft persistent state that must survive restart, written before related outbound messages are sent (see [`docs/raft.md`](raft.md)). |
| `Metadata` | node identity, format version | Node-local metadata not part of the replicated log. |

`internal/wal` does not know what a `command bytes` payload means
(SQL, MVCC mutation, etc.) — it stores and returns opaque bytes for
`LogEntry` payloads. Interpretation belongs to `internal/fsm`.

## 3. Record framing

Every record is framed as:

```
+----------+----------+----------+----------+------------------+----------+
| type (1B)| index(8B)| length(4B)|version(1B)| payload (length) |crc32(4B)|
+----------+----------+----------+----------+------------------+----------+
```

- `type` — one of the record types above.
- `index` — the record's logical WAL index (monotonically increasing,
  unique per WAL, gap-free for `LogEntry` records within a term of
  continuous operation; `HardState`/`Metadata` records are interleaved
  but do not consume `LogEntry` index space — they are tracked by
  "most recent record of this type" rather than by index).
- `length` — payload length in bytes; bounded at read time against
  remaining segment bytes (see [`docs/storage.md`](storage.md) §7).
- `version` — record format version, so future format changes can be
  detected and rejected explicitly rather than misparsed.
- `payload` — type-specific bytes.
- `crc32` — checksum over `type..payload` (everything except the
  checksum field itself).

## 4. Append, ordering, and synchronization semantics

- `Append(record) -> LogIndex` hands the framed record to
  `internal/storage.Append`. This is **appended**, not **persisted**.
- `Sync() -> error` calls `internal/storage.Sync`. A record is
  **persisted** only once a `Sync()` call that covers it has
  successfully returned.
- ChronicleDB uses **explicit synchronous durability**: a caller that
  needs a durability guarantee (every standalone commit; every Raft
  `HardState` update; every Raft `LogEntry` a follower is about to
  acknowledge) calls `Sync()` and waits for it before proceeding. V1
  does not silently defer or batch sync policy inside `internal/wal`
  without the caller's knowledge; a future group-commit optimization
  (batching multiple callers' `Append`s behind one `Sync()`) is
  permitted **only** if every batched caller still observes a
  `Sync()` completion before being told its record is durable — see
  [ADR-0002](adr/0002-local-storage-architecture.md).
- Records are read back and replayed strictly in the order they were
  appended. `internal/wal` never reorders records.

## 5. Replay

`Replay(fromIndex) -> iterator over records` reads a WAL from a given
starting index forward, in order, verifying each record's checksum,
and yields records to the caller (`internal/fsm` during recovery, or
`internal/raft` during restart to reconstruct its log). Replay is used
in two contexts:

- **Startup recovery** — see [`docs/recovery.md`](recovery.md).
- **Follower catch-up** without a snapshot — reading a range of
  already-durable entries to (re-)send to a lagging follower.

Replay never invents, skips, or reorders committed records; it either
returns a record, signals a well-defined end-of-log condition (§6), or
fails closed on unclassified corruption (§6).

## 6. Corruption and Truncation Rules — the required distinction

ChronicleDB draws a hard line between two situations that look similar
but are **not** the same:

### 6.1 Torn final record (safe to truncate automatically)

If the **last** record in the WAL is incomplete — the segment ends
mid-header, mid-payload, or the trailing checksum bytes are missing or
short — this is recognized as the signature of a crash that occurred
*during* an in-progress append (crash after partial `Append`, before
the record was fully written). This is expected, safe, and handled
automatically:

- The torn tail is truncated at the boundary of the last **fully
  framed, checksum-valid** record.
- Startup proceeds using history only up to that point.
- This is safe because a torn final record was, by construction, never
  `Sync()`-ed as part of a completed write the caller was told
  succeeded — see the durability contract in
  [`docs/replication.md`](replication.md). Nothing that was
  acknowledged as durable is lost by this truncation.

### 6.2 Fully framed record with a bad checksum (never silently dropped)

If a record is **fully framed** (correct length, all bytes present)
but its checksum does not match — whether it is the last record or,
critically, **any record before it** — this is **not** treated as a
torn tail. A fully framed record with a bad checksum indicates
corruption of data that may have already been acknowledged as durable
(bit rot, a storage-media fault, a filesystem bug, or a WAL
implementation bug). ChronicleDB never guesses that such a record can
be discarded.

- **Mid-log corruption** (a bad-checksum fully-framed record anywhere
  other than possibly at the very end after all valid final records)
  **always fails startup** for that node. The node refuses to
  participate (does not vote, does not serve reads, does not accept
  writes) until an operator resolves it. This is a hard invariant —
  see [`docs/invariants.md`](invariants.md) `RECOVERY-NON-INVENTION`.
- Resolution is operator-driven: restore the node from another
  healthy replica (in Raft mode, this is the normal, safe path — a
  node's local WAL is not the cluster's only copy of history), or from
  a known-good snapshot plus the surviving valid log suffix.
- ChronicleDB V1 does **not** attempt automatic self-healing of
  mid-log corruption (e.g. guessing which side of the corruption is
  "right," or silently skipping the bad record and continuing). Doing
  so could silently resurrect or silently lose committed state, which
  violates `RECOVERY-NON-INVENTION`.

### 6.3 Summary table

| Situation | Automatic recovery? | Startup outcome |
|---|---|---|
| Last record incomplete (torn tail) | Yes — truncate to last valid record | Proceeds normally |
| Fully framed record, bad checksum, anywhere | No | Startup refused; operator intervention required |
| Fully framed record, bad checksum, followed by more (apparently valid) records | No | Startup refused; the presence of "valid-looking" data after a corrupt record is itself suspicious and must not be trusted automatically |
| Empty WAL / no records | N/A | Normal cold start (or restore from snapshot only) |
| Unknown/unsupported record `version` | No | Startup refused; version skew must be resolved explicitly, not guessed at |

Explicitly out of V1 support: automatic quorum-based repair of a
single node's corrupted local WAL from peers (the operator performs
this manually by re-provisioning the node from a snapshot + peer log,
per [`docs/recovery.md`](recovery.md)); this may become automated in a
later phase once the manual procedure is proven.

## 7. Interaction with snapshots

Once a state-machine snapshot exists up to `lastIncludedIndex` (see
[`docs/snapshots.md`](snapshots.md)), WAL entries at or before that
index are eligible for deletion (log compaction), because the
snapshot is a self-sufficient substitute for replaying them. Deletion
of old WAL segments only proceeds after the snapshot has been fully
persisted and validated — never before, and never based on an
in-progress snapshot. See [`docs/snapshots.md`](snapshots.md) §Log
Truncation.

## 8. Persistent metadata

`internal/wal`'s `Metadata` record (and the `meta/` directory, see
[`docs/storage.md`](storage.md) §4) track:

- Node identity (stable across restarts).
- WAL format version.
- A pointer to the most recent valid snapshot (if any), so recovery
  knows where to start without scanning the whole snapshot directory.

Raft-specific persistent state (`currentTerm`, `votedFor`, log
entries) is described in [`docs/raft.md`](raft.md) §Persistent State
and is stored via the `HardState`/`LogEntry` record types defined
above — it is not a second, parallel metadata store.

## 9. Phase 1 implementation decisions (resolved)

- **Non-`LogEntry` index field**: per §2, `HardState`/`Metadata`
  records are "tracked by most recent record of this type rather than
  by index." The implemented choice is that such records carry `index
  = 0` in the frame and are identified purely by encountering them
  during a forward, in-order scan — the last one seen (of a given type)
  wins. `LogEntry` records use the field for their real, gap-free
  logical index, which the implementation validates on every replay
  (out-of-order or duplicate indices are treated as corruption per §6).
- **Maximum record payload size**: `MaxRecordPayloadSize` is 64 MiB.
  Any fully-framed record claiming a larger payload is rejected
  (`ErrRecordTooLarge`) before any allocation is attempted, per
  `docs/failure-model.md` §6's bounded-allocation requirement; a record
  that is merely torn (not enough bytes physically present, regardless
  of what its length field claims) is still classified as a torn tail,
  never as oversized.
- **`internal/wal.Open`** implements the Phase-1 subset of
  `docs/recovery.md` §1 (steps 1, 5-8): it locates segments, replays and
  checksum-verifies every record in order, truncates a torn tail found
  only in the current (last) segment, and refuses startup unconditionally
  on any other classification of corruption, an out-of-order log index,
  an unsupported per-record or per-metadata format version, or a
  non-empty log with no `Metadata` record at all.

## 10. Phase 5 implementation decisions (resolved)

Phase 4 (`docs/raft.md` §9.4) identified the specific gap blocking a
real `internal/wal`-backed `raft.Storage`: no suffix-truncation
capability. Phase 5 closes it with two additions, both keeping
`internal/wal` exactly as ignorant of Raft/transaction semantics as
before (§1's dependency rule) — the caller (`internal/node.WALStorage`)
still owns interpreting what the opaque payload bytes mean.

- **`WAL.Truncate(fromIndex)`** durably discards every `LogEntry`
  record at or after `fromIndex`, so a following `AppendLogEntry`
  resumes exactly at `fromIndex`. Crash safety comes from a strict,
  two-phase ordering: first, every segment strictly newer than the one
  holding `fromIndex` is deleted outright, highest id first, each
  deletion its own fsync'd directory operation (`storage.RemoveSegment`
  already fsyncs the directory per `docs/storage.md`); only once that
  is fully done is the segment actually holding `fromIndex` itself
  shrunk (`storage.Segment.Truncate`, which already fsyncs the
  file) — never the other way around. This ordering is load-bearing: a
  crash at any point leaves the surviving segments as a complete,
  gap-free, checksum-valid prefix of some legitimate state (either
  before, after, or partway through the intended truncation) that
  `Open` can always recover from cleanly, and an interrupted truncation
  is always safely re-triggerable (Raft's own divergent-suffix-repair
  protocol re-issues an equivalent `Truncate` call the next time this
  node exchanges `AppendEntriesRPC`s with a legitimate leader, so no
  operator action is ever required — see
  `internal/wal/truncate_test.go::TestTruncateInterruptedMidwayStillOpensToValidPrefix`
  for the exact scenario this proves). The reverse ordering (shrinking
  the target segment first, then deleting newer ones) would instead risk
  a crash leaving an out-of-order log index gap that `Open` correctly,
  but unhelpfully, refuses to start from — the chosen order never
  produces that state at any intermediate step.
- **`WAL.AppendHardState`/`WAL.LatestHardState`** give the previously
  reserved-but-unused `RecordTypeHardState` record type (§2) a real
  read path: `Open`'s recovery scan now also tracks the payload of the
  most recently encountered `HardState` record (§2/§9's "last one seen
  wins" rule, already true by construction of a forward, in-order scan)
  and exposes it via `LatestHardState()`; a live `AppendHardState` call
  updates the same in-memory value immediately. Both record and payload
  remain fully opaque to `internal/wal` — encoding/decoding
  `currentTerm`/`votedFor` is `internal/node.WALStorage`'s job (see
  `docs/raft.md` §10.1).
- Both additions are exercised by real on-disk tests, including a
  multi-segment truncation (forcing whole segment-file deletion, not
  just an in-place shrink) and the interrupted-truncation crash-safety
  scenario above — see `internal/wal/truncate_test.go`.

## 11. Phase 6 implementation decisions (resolved)

Phase 6 closes §7/§8's remaining gap — a live compaction mechanism —
with three additions, all still keeping `internal/wal` ignorant of
Raft/snapshot semantics beyond the opaque `Metadata.LatestSnapshotIndex`
pointer it already tracked since Phase 1 (`internal/snapshot` and
`internal/node` own everything about what a snapshot actually contains):

- **`WAL.AppendMetadataSnapshot(uptoIndex)`** durably records that a
  fully-persisted, validated snapshot now covers history up to and
  including `uptoIndex`: a fresh `Metadata` record (last-one-wins, §9)
  is appended to the *current* segment and synced. This also advances
  `FirstIndex()` immediately, live in this process — not only on a
  future restart's re-derivation from the record — because a caller
  installing a peer's snapshot mid-process (`internal/node.WALStorage.
  InstallSnapshot`) needs "the oldest index this WAL still logically
  serves" to move the instant it is durably adopted.
- **`WAL.CompactBefore(uptoIndex)`** deletes every whole segment file
  holding only `LogEntry` records at or before `uptoIndex` — the
  physical half of compaction, always called only after
  `AppendMetadataSnapshot` has already durably recorded the boundary it
  relies on (§7's "snapshot durable before truncation," which is what
  makes `LOG-COMPACTION-SAFETY` hold). Eligibility is decided purely
  from segment ids (a segment's id is the first logical index it holds,
  per `docs/storage.md` §3), never by re-reading contents; the current
  segment is never deleted. Safe to call repeatedly and safe to
  interrupt at any point — each segment deletion is its own fsync'd
  directory operation, so a crash mid-loop leaves extra, harmless,
  already-superseded history behind rather than losing anything a
  future `Open` needs.
- **`WAL.FirstIndex()`** exposes the oldest `LogEntry` index this WAL
  can still serve (`Metadata.LatestSnapshotIndex + 1`; 1 for a node
  that has never compacted) so a caller (`internal/node.Open`) can
  detect the one case recovery must refuse rather than silently
  under-replay: the durable log's own oldest physically-surviving entry
  starting strictly after where the (possibly snapshot-covered)
  recoverable boundary says it should (`docs/recovery.md` §4). `Open`'s
  own two-pass validation independently enforces the same rule at
  startup: a physically-scanned log's first `LogEntry` record is
  allowed to start at or before `Metadata.LatestSnapshotIndex + 1`
  (harmless not-yet-fully-compacted leftovers), never strictly after it
  (a genuine gap, refused as `ErrCorrupt`).
- All three are exercised by real on-disk tests, including multi-
  segment compaction, a live-process `FirstIndex()` advance with no
  intervening restart, and a restart immediately after compaction
  correctly replaying only the entries actually still needed — see
  `internal/wal/snapshot_test.go`.
