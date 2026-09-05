# Snapshots and Log Compaction

Status: Architecture Foundation. No snapshot implementation exists
yet.

Snapshots are correctness artifacts, not merely a disk-space
optimization. This document defines what a snapshot contains, how it
is created and installed safely, and how it relates to log compaction
and MVCC garbage collection.

## 1. One coordinated snapshot, not competing sources of truth

ChronicleDB uses **one** snapshot mechanism that carries the consensus
boundary (`lastIncludedIndex`/`lastIncludedTerm`) together with the
state-machine data it corresponds to. There is no separate "database
backup" format and "Raft snapshot" format that could drift out of sync
with each other — see [`docs/architecture.md`](architecture.md) §2.

## 2. Snapshot contents

A valid ChronicleDB snapshot contains everything required to resume
deterministic operation from a committed prefix:

- **Committed MVCC data**: the current value (or "absent") for every
  live key as of `lastIncludedIndex`.
- **Version chain data needed after the snapshot point**: specifically,
  for any key with an active reader whose `StartSeq` predates
  `lastIncludedIndex` but who has not yet finished (relevant mainly for
  in-progress long-lived transactions spanning a snapshot — V1 keeps
  this simple by requiring snapshot creation to respect the
  `GCWatermark` rule from [`docs/mvcc.md`](mvcc.md) §6, so a snapshot
  never discards a version some active snapshot view still needs).
- **Tombstones**: recorded so a deleted key is not resurrected by a
  point-in-time restore.
- **Durable `RequestID` outcomes**: the full idempotency outcome table
  (see [`docs/transactions.md`](transactions.md) §6) as of
  `lastIncludedIndex`, so idempotency guarantees hold across
  snapshot-based recovery, not just log-replay-based recovery.
- **Schema/catalog metadata**: reserved for the future SQL layer (see
  [`docs/non-goals.md`](non-goals.md)); not populated in V1, which has
  no schema.
- **State-machine metadata**: format version, and any other fields
  `internal/fsm` needs to describe its own serialized shape.
- **`lastIncludedIndex`** and **`lastIncludedTerm`**: the Raft log
  position this snapshot corresponds to.
- **Format version** and **checksum/integrity metadata**: covering the
  entire snapshot file, verified before the snapshot is ever trusted
  (§5).

## 3. Snapshot creation

- **Trigger**: V1 triggers snapshot creation based on durable log
  growth since the last snapshot exceeding a configured threshold
  (e.g. size or entry count) — not on a wall-clock timer, to keep the
  trigger a function of actual state, and not because a timer firing
  is itself meaningful to correctness.
- **Creation point**: a snapshot is taken at a specific `appliedIndex`
  on the node creating it (normally the leader, though any node may
  create local snapshots of its own applied state) — `lastIncludedIndex
  = appliedIndex` at the moment creation begins. State mutations that
  arrive (via further `Apply` calls) while the snapshot is being
  serialized must not be reflected in that snapshot; V1 achieves this
  either by taking a consistent point-in-time copy of state before
  resuming `Apply` calls, or by using a data structure that supports a
  cheap consistent read-only view (an implementation detail; the
  correctness requirement is that the resulting file's contents
  exactly match state as of `lastIncludedIndex`, no more, no less).
- **Temporary file**: the snapshot is written to a temporary file
  (`snapshot/tmp/...`, see [`docs/storage.md`](storage.md) §4), never
  directly to its final name.
- **Synchronization**: the temporary file is fully written and
  `fsync`-ed before being considered complete.
- **Atomic rename**: the temporary file is atomically renamed to its
  final, content-addressed name (e.g. including `lastIncludedIndex`
  and a checksum) only after the fsync in the previous step succeeds.
  The containing directory is fsynced after the rename where the
  filesystem requires that for rename durability. A snapshot file
  never exists at its final name in a partially-written state.
- **Restart discovery**: recovery (see
  [`docs/recovery.md`](recovery.md) §1) discovers the newest
  snapshot by consulting the pointer in persistent metadata
  ([`docs/wal.md`](wal.md) §8), not by guessing from directory
  contents alone (though a directory scan as a fallback/consistency
  check is a reasonable implementation detail).

## 4. Interrupted creation

If the process crashes while writing the temporary snapshot file:

- The temporary file is simply an incomplete, orphaned file on next
  startup. It was never renamed to a final name, so it is never
  considered a candidate snapshot. Recovery ignores (and may clean up)
  stale `snapshot/tmp/` contents.
- No special recovery logic is required beyond "temp files are not
  trusted"; this is a direct consequence of the atomic-rename rule in
  §3.

## 5. Validation (before trusting any snapshot)

Before a snapshot is used for recovery or installed on a follower:

1. Its format version must be recognized.
2. Its checksum must be verified over its full contents.
3. Its `lastIncludedIndex`/`lastIncludedTerm` must be internally
   consistent with the rest of its own contents (e.g. the `RequestID`
   outcome table and MVCC data must correspond to having applied
   exactly up through that index — a property maintained by
   construction at creation time, spot-checked at load time where
   practical).

## 6. Corrupted snapshot handling

- A snapshot that fails validation is **never** used. It is treated as
  if it does not exist.
- Recovery falls back to the next-older valid snapshot, if the node
  happens to retain more than one (V1 may choose to retain only the
  latest snapshot to save space — in that case, a corrupted single
  snapshot with a log that has already been compacted past it is a
  case requiring operator intervention, per
  [`docs/recovery.md`](recovery.md) §4 — this is the direct reason log
  truncation (§8) must be conservative about how many snapshots'
  worth of log it discards).
- A follower being sent a snapshot by the leader (§7) that fails
  validation on arrival rejects it and requests re-transmission; it
  never installs a snapshot it cannot verify.

## 7. Follower snapshot installation

When a follower is too far behind for normal log replication to catch
it up (the leader has already compacted the log entries the follower
would need — see §8), the leader instead sends its most recent valid
snapshot:

1. Leader streams/sends the snapshot file (chunked or whole, an
   implementation detail) to the follower.
2. Follower writes it to its own `snapshot/tmp/` and validates it (§5)
   before doing anything else with it.
3. On successful validation, the follower **atomically replaces** its
   entire state-machine state with the snapshot's contents (MVCC data,
   tombstones, `RequestID` outcomes) and sets its own
   `lastIncludedIndex`/`Term`, `appliedIndex` accordingly.
4. The follower discards any local log entries at or before the new
   `lastIncludedIndex` (they are superseded) and any divergent log
   entries that conflict with the snapshot boundary.
5. Only after steps 1-4 complete does the follower resume normal
   `AppendEntriesRPC` processing from `lastIncludedIndex + 1`.
6. If installation is interrupted (crash mid-transfer or mid-validate),
   the follower simply restarts installation from scratch on
   reconnection — no partial snapshot state is ever considered
   installed (the same atomic-rename discipline from §3 applies
   locally on the follower before its own state is replaced).

## 8. Log truncation (compaction) after snapshot

- Once a snapshot at `lastIncludedIndex` is fully persisted and
  validated (§3, §5), WAL segments containing **only** entries at or
  before `lastIncludedIndex` become eligible for deletion (see
  [`docs/wal.md`](wal.md) §7).
- Truncation deletes whole segment files only — never partial segments
  — and only after the snapshot they are superseded by is confirmed
  durable. This ordering (snapshot durable **before** truncation)
  guarantees `LOG-COMPACTION-SAFETY` (see
  [`docs/invariants.md`](invariants.md)): compaction never removes
  history still required to reconstruct legitimate state, because the
  snapshot is, by construction, a complete substitute for the
  discarded history.
- Log truncation is independent per node (each node compacts its own
  local log against its own local latest snapshot) — it does not
  require cluster-wide coordination beyond the normal Raft mechanism
  of a leader deciding when a follower needs a snapshot instead of a
  log range (§7).

## 9. Relationship to MVCC GC and Raft log compaction

Three distinct mechanisms, easy to conflate, kept explicitly separate:

| Mechanism | Reclaims | Triggered by | Defined in |
|---|---|---|---|
| **Database (state-machine) snapshot** | Nothing by itself — it is a checkpoint, a byproduct that *enables* the other two | Log growth threshold | This document |
| **Raft log compaction** | Durable WAL segments/entries at or before a snapshot's `lastIncludedIndex` | A confirmed, valid snapshot existing (§8) | This document §8, [`docs/wal.md`](wal.md) §7 |
| **MVCC version garbage collection** (not implemented in V1) | Old, no-longer-visible-to-any-snapshot MVCC versions of a key | `GCWatermark` advancing past a version's superseding version's `CommitSeq` | [`docs/mvcc.md`](mvcc.md) §6 |

A database snapshot capturing "current live state" and MVCC GC
"removing old versions" are related (a snapshot only needs to persist
versions that survive GC's rule) but are not the same operation and do
not share an implementation or a trigger. Conflating them (e.g. using
snapshot creation as the *only* moment old versions are ever
discarded, or vice versa) is exactly the kind of ambiguity this
document exists to prevent.
