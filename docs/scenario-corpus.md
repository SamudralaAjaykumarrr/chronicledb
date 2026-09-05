# Scenario Corpus

Status: LD-1 through LD-6 (Phase 1, `internal/wal`) and TX-1 through
TX-8 (Phase 2, `internal/mvcc` + `internal/txn`) have passing,
reproducible tests — see each scenario's **Status** line below for the
specific test. **Every other scenario in this document does not
currently pass, because it is not implemented yet.** This document
specifies the deterministic scenarios future test suites (see
[`docs/testing-strategy.md`](testing-strategy.md)) must implement and
pass before the corresponding maturity level
([`docs/roadmap.md`](roadmap.md)) can be claimed.

Each scenario specifies: initial state, action/failure, event
ordering, expected state, client-visible outcome, invariants involved,
future test oracle, and the roadmap phase in which it becomes
executable.

---

## Local durability

### LD-1: Single write + clean restart

- **Initial state**: empty database.
- **Action**: client commits a transaction writing one key; process
  shuts down cleanly; process restarts.
- **Event ordering**: write persisted -> applied -> acknowledged ->
  shutdown -> restart -> recovery.
- **Expected state**: the key's value is present after restart.
- **Client-visible**: read after restart returns the written value.
- **Invariants**: `DURABILITY`.
- **Oracle**: component test comparing pre-shutdown read and
  post-restart read.
- **Phase**: 1.

### LD-2: Committed write + crash

- **Initial state**: empty database.
- **Action**: client commits a transaction; process is killed
  (ungracefully) immediately after the commit is acknowledged.
- **Expected state**: the key's value is present after restart
  (it was persisted before acknowledgment — [`docs/replication.md`](replication.md) §1.1).
- **Invariants**: `DURABILITY`.
- **Oracle**: crash-injection test killing the process post-ack,
  verifying recovery.
- **Phase**: 1.

### LD-3: Crash before fsync

- **Initial state**: empty database.
- **Action**: `Append` succeeds, process is killed before the
  corresponding `Sync()` returns; no acknowledgment was ever sent.
- **Expected state**: the key's value **may** be absent after restart.
- **Client-visible**: client never received a success response for
  this write (it was still waiting), so absence is correct, not a bug.
- **Invariants**: `DURABILITY` (not violated — nothing acknowledged).
- **Oracle**: fault-injection test asserting no success response was
  ever delivered for this specific write, paired with the value's
  absence being an accepted outcome (not asserted as the only
  outcome, since a `Sync()` racing the kill could still land).
- **Phase**: 1.

### LD-4: Torn final WAL record

- **Initial state**: WAL contains N valid records.
- **Action**: crash occurs mid-append of record N+1 (partial bytes on
  disk).
- **Expected state**: recovery truncates the torn tail; records
  1..N are intact and replayed.
- **Client-visible**: N/A (server-internal); prior N commits' data is
  present.
- **Invariants**: `RECOVERY NON-INVENTION`.
- **Oracle**: byte-offset crash-injection test, per
  [`docs/wal.md`](wal.md) §6.1.
- **Phase**: 1.

### LD-5: Corrupt complete record

- **Initial state**: WAL contains N valid records.
- **Action**: one fully framed record (not the torn-tail case) has its
  checksum bytes flipped, simulating bit rot.
- **Expected state**: startup refuses; node does not become eligible
  to participate.
- **Client-visible**: node reports a startup failure requiring
  operator intervention; no silent partial recovery.
- **Invariants**: `RECOVERY NON-INVENTION`.
- **Oracle**: startup-refusal test per
  [`docs/wal.md`](wal.md) §6.2 / [`docs/recovery.md`](recovery.md) §4.
- **Phase**: 1.

### LD-6: Mid-log corruption

- **Initial state**: WAL contains N valid records.
- **Action**: a record strictly before the last record is corrupted
  (bad checksum), later records are otherwise intact.
- **Expected state**: startup refuses, regardless of the apparent
  validity of records after the corrupted one.
- **Invariants**: `RECOVERY NON-INVENTION`.
- **Oracle**: same as LD-5, specifically targeting a non-final record.
- **Phase**: 1.

---

## Transactions

### TX-1: Begin/read/write/commit

- **Initial state**: key `K` has committed value `v0`.
- **Action**: `BEGIN` (StartSeq=S), `READ(K)` -> `v0`, `WRITE(K, v1)`,
  `COMMIT`.
- **Expected state**: `K`'s version chain gains `(CommitSeq>S, v1)`.
- **Client-visible**: commit returns `COMMITTED`; subsequent reads by
  new transactions with `StartSeq >= CommitSeq` see `v1`.
- **Invariants**: `MVCC VISIBILITY`, `ATOMICITY`.
- **Oracle**: component test against `internal/mvcc` + `internal/txn`.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX1_BeginReadWriteCommit`.

### TX-2: Aborted transaction

- **Action**: `BEGIN`, `WRITE(K, v1)`, `ABORT`.
- **Expected state**: no new version of `K`.
- **Invariants**: `ABORT SAFETY`.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX2_AbortedTransaction`.

### TX-3: Multi-key atomic commit

- **Action**: `BEGIN`, write `K1`, `K2`, `K3`, `COMMIT` (no conflicts).
- **Expected state**: all three keys gain a new version at the same
  `CommitSeq`, or (if a concurrent test triggers a conflict on any one
  key) none do.
- **Invariants**: `ATOMICITY`.
- **Oracle**: concurrent-reader test asserting no reader ever observes
  exactly 1 or 2 of the 3 new versions.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX3_MultiKeyAtomicCommit`.

### TX-4: Concurrent non-conflicting transactions

- **Action**: `T1` writes `K1`, `T2` writes `K2`, both begin with the
  same `StartSeq`, both commit.
- **Expected state**: both commit successfully; both new versions
  exist.
- **Invariants**: `CONFLICT CORRECTNESS`.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX4_ConcurrentNonConflictingTransactions`;
  concurrent (goroutine-level, `-race`-clean) variant in
  `internal/txn/concurrent_test.go::TestConcurrentNonConflictingWritersAllCommit`.

### TX-5: Concurrent conflicting transactions

- **Action**: `T1` and `T2` both begin with `StartSeq=S`, both write
  `K`; `T1` commits first (gets `CommitSeq=C1 > S`); `T2` attempts to
  commit afterward.
- **Expected state**: `T2` aborts (latest committed `CommitSeq` for
  `K` is `C1 > T2.StartSeq`); `T1`'s write stands.
- **Client-visible**: `T2` receives `ABORTED` with a conflict reason.
- **Invariants**: `CONFLICT CORRECTNESS`.
- **Oracle**: exact reproduction of [`docs/mvcc.md`](mvcc.md) §4
  example.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX5_ConcurrentConflictingTransactions`;
  concurrent (goroutine-level, `-race`-clean) variant with 50 concurrent
  writers in `internal/txn/concurrent_test.go::TestConcurrentConflictingWritersExactlyOneWins`.

### TX-6: Read snapshot remains stable

- **Action**: `T1` begins (`StartSeq=S`), reads `K` -> `v0`; `T2`
  commits a new version of `K`; `T1` reads `K` again.
- **Expected state**: `T1`'s second read still returns `v0`.
- **Invariants**: `MVCC VISIBILITY`.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX6_ReadSnapshotStable`.

### TX-7: Delete/tombstone visibility

- **Action**: `K` has committed value `v0`; `T1` begins after a delete
  of `K` commits; `T1` reads `K`.
- **Expected state**: `T1` sees `K` as not-found (tombstone honored).
- **Invariants**: `MVCC VISIBILITY`.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX7_DeleteTombstoneVisibility`.

### TX-8: Snapshot Isolation write-skew example

- **Action**: exact reproduction of [`docs/mvcc.md`](mvcc.md) §1.1
  (`x`,`y` invariant example): both transactions read, both commit
  disjoint writes.
- **Expected state**: both commit; resulting state **violates** the
  application-level invariant `x + y >= 0` — this is the expected,
  documented behavior of SI, not a bug.
- **Client-visible**: both `COMMIT`s return `COMMITTED`.
- **Invariants**: `ISOLATION TRUTHFULNESS` (this test exists precisely
  to keep the write-skew possibility demonstrated and honest).
- **Oracle**: assert both commits succeed and the invariant is indeed
  violated — a "regression" here (the invariant no longer being
  violated) would mean either the implementation silently became
  stricter than documented (also a bug, since it would be
  undocumented/unproven SERIALIZABLE behavior) or the test itself
  drifted from the documented scenario.
- **Phase**: 2.
- **Status**: passing — `internal/txn/txn_test.go::TestTX8_SnapshotIsolationWriteSkew`.

---

## Idempotency

### ID-1: Duplicate `RequestID` before response

- **Action**: client sends `COMMIT` with `RequestID=R`; server applies
  it; before responding, an identical retry with `RequestID=R` arrives
  (e.g. client-side retry raced the original).
- **Expected state**: exactly one set of mutations applied.
- **Client-visible**: both requests eventually observe the same
  outcome.
- **Invariants**: `IDEMPOTENCY`.
- **Phase**: 3.

### ID-2: Duplicate `RequestID` after restart

- **Action**: `COMMIT` with `RequestID=R` applied and acknowledged;
  node restarts; client retries with `RequestID=R`.
- **Expected state**: recovery reconstructs `R`'s outcome; retry
  returns the identical outcome; no re-apply.
- **Invariants**: `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 3.

### ID-3: Committed request + lost response

- **Action**: `COMMIT` with `RequestID=R` applied; response dropped
  before reaching client; client calls `GetRequestOutcome(R)`.
- **Expected state**: outcome query returns `COMMITTED`.
- **Invariants**: `REQUEST OUTCOME STABILITY`.
- **Phase**: 3.

### ID-4: Same `RequestID` retry

- Same as ID-1/ID-2 but explicitly varying the delay between original
  and retry (immediate, after restart, after snapshot+compaction) to
  cover all three outcome-retention paths.
- **Invariants**: `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 3 (immediate/after-restart), 6 (after snapshot+compaction).

### ID-5: New `RequestID` with same mutations

- **Action**: client commits mutation set `M` with `RequestID=R1`;
  later submits the identical mutation set `M` with a **different**
  `RequestID=R2`.
- **Expected state**: two independent commit attempts are evaluated;
  `R2` is subject to its own conflict check against the then-current
  state (may commit or abort independently of `R1`'s outcome).
- **Client-visible**: `R1` and `R2` may have different outcomes; this
  is correct per [`docs/transactions.md`](transactions.md) §6 — no
  semantic deduplication beyond `RequestID` identity is provided.
- **Invariants**: `IDEMPOTENCY` (correctly *not* applying cross-request
  deduplication here).
- **Phase**: 3.

---

## Raft / replication

### RF-1: Normal leader replication

- **Action**: leader proposes an entry; followers acknowledge; leader
  commits and applies.
- **Expected state**: all three nodes eventually converge to identical
  applied state.
- **Invariants**: `STATE MACHINE SAFETY`.
- **Phase**: 5.

### RF-2: Follower lag

- **Action**: one follower's message delivery is delayed by the
  simulator while the other two nodes continue committing entries.
- **Expected state**: the lagging follower eventually catches up once
  delivery resumes; commit progress on the majority is not blocked by
  the lag.
- **Invariants**: `QUORUM SAFETY` (majority-only requirement),
  `RAFT LOG MATCHING`.
- **Phase**: 5.

### RF-3: Follower crash/restart

- **Action**: a follower is stopped mid-stream, then restarted.
- **Expected state**: rejoins, catches up via log replication or
  snapshot install, converges.
- **Invariants**: `STATE MACHINE SAFETY`.
- **Phase**: 5 (log catch-up), 6 (snapshot catch-up).

### RF-4: Leader crash before quorum

- **Action**: leader proposes an entry, crashes before any follower
  persists it.
- **Expected state**: entry is absent from the eventual new leader's
  log; safe to have vanished (§ [`docs/failure-model.md`](failure-model.md) §2.1-2.2).
- **Invariants**: none violated by its absence.
- **Phase**: 5.

### RF-5: Leader crash after quorum

- **Action**: leader proposes an entry, a majority persists it, leader
  crashes before applying or replying.
- **Expected state**: entry **is** committed; new leader's log/state
  includes it once elected and caught up.
- **Client-visible**: retry-by-`RequestID` against new leader returns
  `COMMITTED`.
- **Invariants**: `LEADER COMPLETENESS`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 5.

### RF-6: Leader crash after quorum before response

- Same as RF-5, explicitly framed around the client-visible `UNKNOWN`
  -> retry -> `COMMITTED` flow (see
  [`docs/transactions.md`](transactions.md) §7).
- **Invariants**: `REQUEST OUTCOME STABILITY`.
- **Phase**: 5.

### RF-7: Duplicate `AppendEntriesRPC`

- **Action**: simulator delivers the same `AppendEntriesRPC` twice.
- **Expected state**: no duplicate log entries; second delivery is a
  no-op or harmless re-confirmation.
- **Invariants**: `RAFT LOG MATCHING`.
- **Phase**: 5.

### RF-8: Stale `AppendEntriesRPC`

- **Action**: an `AppendEntriesRPC` from a since-superseded (lower)
  term is delivered late.
- **Expected state**: rejected by the receiver's term check; no state
  change.
- **Invariants**: `RAFT ELECTION SAFETY`.
- **Phase**: 5.

### RF-9: Divergent suffix

- **Action**: a former leader's uncommitted tail conflicts with the
  current leader's authoritative entries at the same positions.
- **Expected state**: divergent suffix truncated and overwritten;
  never affects already-committed entries.
- **Invariants**: `RAFT LOG MATCHING`, `LEADER COMPLETENESS`.
- **Phase**: 5.

### RF-10: Stale leader

- **Action**: a partitioned former leader continues believing it is
  leader; partition heals.
- **Expected state**: it observes a higher term and steps down
  immediately.
- **Invariants**: `RAFT ELECTION SAFETY`.
- **Phase**: 5.

### RF-11: Minority partition

- **Action**: one node is isolated from the other two.
- **Expected state**: isolated node cannot commit new entries; the
  majority side continues normally. Full contract in
  [`docs/replication.md`](replication.md) §5.
- **Invariants**: `QUORUM SAFETY`.
- **Phase**: 5, 7.

### RF-12: Majority election

- **Action**: current leader is stopped or partitioned away from a
  majority.
- **Expected state**: the majority elects a new, legitimate leader
  within bounded simulated time.
- **Invariants**: `RAFT ELECTION SAFETY`, `LEADER COMPLETENESS`.
- **Phase**: 5.

### RF-13: Old leader rejoin

- **Action**: following RF-10/RF-11, the old leader's partition heals
  and it rejoins.
- **Expected state**: it catches up (log replication or snapshot) and
  resumes as a normal follower; no committed entry lost.
- **Invariants**: `RAFT LOG MATCHING`, `LEADER COMPLETENESS`.
- **Phase**: 5, 7.

### RF-14: Slow follower

- **Action**: one follower's simulated network delay is set much
  higher than the others', without a full partition.
- **Expected state**: cluster continues committing via the two
  responsive nodes; slow follower eventually catches up.
- **Invariants**: `QUORUM SAFETY`.
- **Phase**: 5.

### RF-15: Repeated election

- **Action**: adversarial timer scheduling causes several elections in
  succession before one stabilizes (e.g. simulated split votes).
- **Expected state**: eventually converges to exactly one stable
  leader per term thereafter; no committed entry is ever lost or
  duplicated across the churn.
- **Invariants**: `RAFT ELECTION SAFETY`, `STATE MACHINE SAFETY`.
- **Phase**: 5, 7 (chaos variant).

---

## Snapshots

### SN-1: Normal snapshot restore

- **Action**: node creates a snapshot at index `I`; later restarts.
- **Expected state**: recovery restores from the snapshot and replays
  only entries after `I`.
- **Invariants**: `SNAPSHOT SAFETY`.
- **Phase**: 6.

### SN-2: Crash during snapshot creation

- **Action**: process killed while writing the temp snapshot file.
- **Expected state**: orphaned temp file ignored on restart; prior
  snapshot (if any) or full log replay used instead.
- **Invariants**: `SNAPSHOT SAFETY`.
- **Phase**: 6.

### SN-3: Interrupted snapshot installation

- **Action**: a follower receiving a snapshot is killed mid-transfer
  or mid-validation.
- **Expected state**: restarts installation from scratch on
  reconnection; no partial state ever considered installed.
- **Invariants**: `SNAPSHOT SAFETY`.
- **Phase**: 6.

### SN-4: Corrupted snapshot

- **Action**: a snapshot file's checksum is deliberately invalidated.
- **Expected state**: never used; fallback to an older valid snapshot
  or operator intervention per [`docs/recovery.md`](recovery.md) §4.
- **Invariants**: `SNAPSHOT SAFETY`, `RECOVERY NON-INVENTION`.
- **Phase**: 6.

### SN-5: Follower catch-up via snapshot

- **Action**: a follower's required log range has already been
  compacted away by the leader.
- **Expected state**: leader sends a snapshot instead of log entries;
  follower installs it and resumes normal replication.
- **Invariants**: `SNAPSHOT SAFETY`, `LOG COMPACTION SAFETY`.
- **Phase**: 6.

### SN-6: Safe log truncation after snapshot

- **Action**: snapshot at index `I` completes and validates; WAL
  segments fully at or before `I` are deleted.
- **Expected state**: deletion never precedes confirmed snapshot
  durability; recovery after a crash immediately following truncation
  still succeeds using the snapshot.
- **Invariants**: `LOG COMPACTION SAFETY`.
- **Phase**: 6.

---

## Roadmap-phase index

| Phase | Scenarios first executable |
|---|---|
| 1 | LD-1 .. LD-6 |
| 2 | TX-1 .. TX-8 |
| 3 | ID-1 .. ID-5 (partial) |
| 5 | RF-1 .. RF-15, ID-4 (immediate/after-restart already from 3) |
| 6 | SN-1 .. SN-6, ID-4 (after snapshot+compaction) |
| 7 | Chaos/combined variants of RF-11, RF-13, RF-15 under randomized fault schedules |

Only scenarios with an explicit **Status: passing** line above
currently pass (LD-1 through LD-6, TX-1 through TX-8); every other
scenario in this document is not claimed to pass. This table exists to
make future maturity claims falsifiable: a claim that ChronicleDB has
reached a given roadmap phase must be checked against whether the
scenarios listed for that phase (and all prior phases) actually have
passing, reproducible tests.
