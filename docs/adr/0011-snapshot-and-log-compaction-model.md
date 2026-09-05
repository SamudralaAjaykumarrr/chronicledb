# ADR-0011: Snapshot and Log-Compaction Model

Status: Accepted

## Context

An unbounded durable log grows forever, making restart and follower
catch-up progressively slower and disk usage unbounded. ChronicleDB
needs a way to bound log growth without risking the
two-competing-histories problem this project is designed to avoid.

## Decision

Use **one coordinated ChronicleDB snapshot** carrying both the
state-machine data (MVCC data, tombstones, `RequestID` outcomes) and
the consensus boundary (`lastIncludedIndex`/`lastIncludedTerm`) — see
[`docs/snapshots.md`](../snapshots.md). Creation uses temp-file +
fsync + atomic rename; log truncation only proceeds after a snapshot
is confirmed durable and validated; follower catch-up falls back to
snapshot install when the required log range has been compacted away.

## Alternatives Considered

1. **Separate "database backup" and "Raft snapshot" mechanisms.**
   Rejected: reintroduces the multiple-competing-sources-of-truth risk
   this project is explicitly designed to avoid (see
   [ADR-0003](0003-wal-raft-log-responsibility-model.md)) — the two
   could describe different, disagreeing points in history.
2. **In-place log truncation (rewrite the log file to drop old
   entries) instead of segment deletion.** Rejected: more complex and
   riskier to get crash-safe than deleting whole, already-closed
   segment files (see [ADR-0002](0002-local-storage-architecture.md)
   alternative 3's reasoning about in-place mutation).
3. **Truncate the log as soon as a snapshot *starts* being created,
   rather than waiting for it to be confirmed durable.** Rejected:
   directly violates `LOG-COMPACTION-SAFETY`
   ([`docs/invariants.md`](../invariants.md)) — a crash during
   snapshot creation, with the log already truncated, would leave no
   way to reconstruct the state between the last confirmed snapshot
   and the crash.
4. **Compact eagerly on every commit (keep no log history beyond the
   latest state at all).** Rejected: removes the ability to catch up a
   temporarily-lagging follower via incremental log replication at
   all, forcing every catch-up through a full snapshot transfer even
   for a follower that missed only one entry — needlessly expensive.

## Consequences

- Snapshot creation cost (serializing full state-machine data)
  is paid periodically rather than continuously; the trigger threshold
  (log growth since last snapshot) is a tunable trading off snapshot
  frequency against log length, to be measured, not guessed, in
  Phase 9.
- A node that has been offline long enough for its needed log range to
  be compacted must always catch up via a full snapshot transfer, not
  incremental replication — an accepted cost given the alternative
  (retaining unbounded log history) is worse.

## Correctness Implications

- Directly implements `SNAPSHOT-SAFETY` and `LOG-COMPACTION-SAFETY`
  ([`docs/invariants.md`](../invariants.md)).
- Keeps MVCC version GC, Raft log compaction, and database snapshots
  as three distinct, explicitly related mechanisms (see
  [`docs/snapshots.md`](../snapshots.md) §9) rather than conflating
  them, avoiding a documented class of real-world database bugs.

## Testing and Proof Obligations

- SN-1 through SN-6 in
  [`docs/scenario-corpus.md`](../scenario-corpus.md) §Snapshots,
  covering normal restore, crash during creation, interrupted
  installation, corruption, follower catch-up, and safe truncation
  ordering.
