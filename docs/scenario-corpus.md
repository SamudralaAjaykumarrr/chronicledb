# Scenario Corpus

Status: LD-1 through LD-6 (Phase 1, `internal/wal`), TX-1 through TX-8
(Phase 2, `internal/mvcc` + `internal/txn`), ID-1 through ID-7
immediate/after-restart and, as of Phase 6, ID-4's after-snapshot leg
(Phase 3/6), a Phase-4 subset of RF-1 through RF-15 (`internal/raft` +
`internal/fault`'s deterministic simulator), a Phase-5 subset of the
same RF-1 through RF-15 (`internal/node` + `internal/transport` against
real disk/network, and `cmd/chronicledb-node` against genuine separate
OS processes), SN-1 through SN-6 plus RF-3's snapshot-catch-up leg
(Phase 6, `internal/snapshot` + `internal/node` + `internal/wal` +
`internal/raft`), and, as of Phase 7, the chaos/combined-fault variants
of RF-11, RF-13, RF-15, SN-3, and SN-5 (`internal/fault/chaos_test.go`,
`internal/node/chaos_test.go`, `cmd/chronicledb-node/chaos_test.go`),
and, as of Phase 8, SQ-1 through SQ-9 (`internal/sql`, the constrained
SQL frontend, translating into the identical proven transaction path —
see [`docs/sql.md`](sql.md)) —
see each scenario's **Status** line below for
exactly which — have passing,
reproducible tests. As of Phase 10, several of these scenarios
additionally carry independent-reference-model and repeated-cycle
adversarial evidence, itemized in full in
[`docs/adversarial-testing.md`](adversarial-testing.md) rather than as
new Status lines below (Phase 10 adds no new numbered scenario — see
the roadmap-phase-index table's Phase 10 row). **Every other scenario
in this document does not currently pass, because it is not implemented
yet.** This document
specifies the deterministic scenarios future test suites (see
[`docs/testing-strategy.md`](testing-strategy.md)) must implement and
pass before the corresponding maturity level
([`docs/roadmap.md`](roadmap.md)) can be claimed.

**Phase 4 note on the RF-\* scenarios below**: a Status line reading
"passing in the deterministic simulator" means the scenario is proven
against real, unmodified `internal/raft.Core` instances wired through
`internal/fault`'s in-memory transport/clock/storage (Phase 4) — on its
own this does **not** mean the scenario has been proven against real
transport/disk in a multi-process deployment.

**Phase 5 note on the RF-\* scenarios below**: a Status line additionally
reading "passing (Phase 5) against real disk/network[/processes]" means
the scenario is proven either in-process against a real
`internal/wal`-backed durable log and a real TCP `internal/transport`
(`internal/node/node_test.go`), or via genuine separate OS processes
communicating over real sockets with real persistent data directories
and a real `SIGKILL` (`cmd/chronicledb-node/main_test.go`, gated behind
the `integration` build tag — see that file's doc comment for the exact
invocation). Not every RF-\* scenario has this Phase-5 leg: RF-2/RF-14
(delayed-but-not-dropped delivery) and RF-7/RF-8 (message-level
protocol edge cases already proven exhaustively, and independently of
any transport, against the identical unmodified `internal/raft.Core`
in Phase 4) are explicitly not re-proven against real sockets — see
each entry's Status line and [`docs/raft.md`](raft.md) §10.2 for why
this is a deliberate scope line, not an oversight. A scenario with no
Status line below is not yet covered by any test, in the simulator or
otherwise.

**Phase 7 note on the "chaos variant" lines below**: a Status line
additionally reading "the chaos variant is now also passing (Phase 7)"
means the scenario's already-proven-Phase-4/5/6 shape is additionally
exercised inside a seeded, randomized, combined-fault schedule (breadth
and adversarial combination per [`docs/roadmap.md`](roadmap.md) Phase
7's own framing — not new mechanism) at one or more of three layers:
`internal/fault/chaos_test.go` (the deterministic raft-core simulator,
cheapest per-iteration and run at the highest seed counts),
`internal/node/chaos_test.go` (real `internal/wal`-backed disk and real
TCP `internal/transport`, in-process), and
`cmd/chronicledb-node/chaos_test.go` (genuine separate OS processes,
`-tags=integration`, real SIGKILL). Not every scenario has a chaos
variant at every layer — check each entry's own Status line for which.
[`docs/testing-strategy.md`](testing-strategy.md) §6-7 is the
authoritative, itemized account of Phase 7's chaos capabilities, the
seeds/reproduction method, and the two genuine bugs plus one data race
this work found and fixed.

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
- **Status**: passing — `internal/txn/idempotency_test.go::TestID1_DuplicateRequestIDBeforeRestart`, `internal/txn/txn_test.go::TestRetryDoesNotAppendToWAL`.

### ID-2: Duplicate `RequestID` after restart

- **Action**: `COMMIT` with `RequestID=R` applied and acknowledged;
  node restarts; client retries with `RequestID=R`.
- **Expected state**: recovery reconstructs `R`'s outcome; retry
  returns the identical outcome; no re-apply.
- **Invariants**: `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 3.
- **Status**: passing — `internal/txn/idempotency_test.go::TestID2_DuplicateRequestIDAfterRestart`. Also proven for the `ABORTED` (conflict) case, which this scenario's wording does not explicitly distinguish but `REQUEST OUTCOME STABILITY` requires identically: `TestConflictOutcomeSurvivesRestartAndRetryRemainsConflict`.

### ID-3: Committed request + lost response

- **Action**: `COMMIT` with `RequestID=R` applied; response dropped
  before reaching client; client calls `GetRequestOutcome(R)`.
- **Expected state**: outcome query returns `COMMITTED`.
- **Invariants**: `REQUEST OUTCOME STABILITY`.
- **Phase**: 3.
- **Status**: passing — `internal/txn/txn_test.go::TestGetRequestOutcomeCommitted` (and `TestGetRequestOutcomeAborted` for the conflict analogue; `TestGetRequestOutcomeUnknown` for the never-submitted case).

### ID-4: Same `RequestID` retry

- Same as ID-1/ID-2 but explicitly varying the delay between original
  and retry (immediate, after restart, after snapshot+compaction) to
  cover all three outcome-retention paths.
- **Invariants**: `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 3 (immediate/after-restart), 6 (after snapshot+compaction).
- **Status**: immediate/after-restart passing (see ID-1/ID-2 above); after-snapshot+compaction now also passing (Phase 6) — `internal/fsm/snapshot_test.go::TestEncodeStateDecodeStateRoundTrip` proves a snapshot's `EncodeState`/`DecodeState` round-trips both a `COMMITTED` and an `ABORTED` `RequestID` outcome (including the fingerprint needed to detect a later mismatched-payload reuse) exactly as plain log replay does, and `internal/node/node_test.go::TestSN1_RestartRestoresFromSnapshotAndCompactsLog` proves the same end-to-end against a real disk-backed node restarting from a snapshot with no log replay involved at all.

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
- **Status**: passing — `internal/fsm/fsm_test.go::TestMultipleRequestIDsSameMutationsAreDistinct`, `internal/txn/idempotency_test.go::TestMultipleRequestIDsSameMutationsSurviveRestartAsDistinct`.

### ID-6: `RequestID` reused with a mismatched payload

- **Action**: `COMMIT` with `RequestID=R` applied; a later request
  reuses the identical `RequestID=R` but with a **different** `TxnID`,
  `StartSeq`, or `Mutations` than the original.
- **Expected state**: the reuse is rejected outright
  (`fsm.ErrRequestIDPayloadMismatch`); `R`'s originally recorded
  outcome is completely unchanged, both immediately and after a
  restart.
- **Invariants**: `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY` (this is
  the safe-default policy [`docs/transactions.md`](transactions.md) §6
  specifies for ambiguous reuse, not a scenario present in the
  original ADR-0006 proof-obligation list, but required by this
  phase's brief and consistent with that ADR's identity-only
  deduplication contract).
- **Phase**: 3.
- **Status**: passing — `internal/fsm/fsm_test.go::TestMismatchedRequestIDReuseRejected`, `internal/txn/txn_test.go::TestMismatchedRequestIDReuseRejected`, `internal/txn/idempotency_test.go::TestMismatchedRequestIDReuseRejectedAfterRestart`.

### ID-7: State-machine determinism / replay equivalence

- **Action**: apply an identical ordered `CommitTxn` command history
  (including a conflicting command and a duplicate `RequestID`) to two
  independently constructed `internal/fsm.FSM` instances.
- **Expected state**: byte-for-byte-equivalent `Outcome` values at
  every step, and equivalent MVCC visibility at every `(key, StartSeq)`
  pair.
- **Invariants**: `STATE MACHINE SAFETY`, `DETERMINISM BOUNDARY`.
- **Phase**: 3.
- **Status**: passing — `internal/fsm/fsm_test.go::TestDeterministicReplayEquivalence`, `TestReplayEquivalenceRepeatedRuns`, `TestEncodeDeterministicRegardlessOfConstructionPath`.

---

## Raft / replication

### RF-1: Normal leader replication

- **Action**: leader proposes an entry; followers acknowledge; leader
  commits and applies.
- **Expected state**: all three nodes eventually converge to identical
  applied state.
- **Invariants**: `STATE MACHINE SAFETY`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestRF1_NormalReplication` — **and**
  now passing (Phase 5) both in-process against real
  `internal/wal`-backed disk and a real TCP `internal/transport`
  (`internal/node/node_test.go::TestRF1_NormalReplicationConvergesAcrossRealNodes`)
  and via genuine separate OS processes
  (`cmd/chronicledb-node/main_test.go::TestRealProcesses_ElectionReplicationCrashRestartFailover`,
  `go test -tags=integration ./cmd/chronicledb-node/...`).

### RF-2: Follower lag

- **Action**: one follower's message delivery is delayed by the
  simulator while the other two nodes continue committing entries.
- **Expected state**: the lagging follower eventually catches up once
  delivery resumes; commit progress on the majority is not blocked by
  the lag.
- **Invariants**: `QUORUM SAFETY` (majority-only requirement),
  `RAFT LOG MATCHING`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestFollowerLagAndSlowFollower`. Real
  multi-process proof remains open: `internal/transport.Transport`
  (Phase 5) implements `Block`/`Unblock` for partition-style fault
  injection but not delayed (non-zero-latency, still-delivered)
  delivery, so this scenario's specific "delayed but not dropped"
  shape is not yet reproduced against real sockets — see
  [`docs/raft.md`](raft.md) §10.2's Phase 5 implementation note.

### RF-3: Follower crash/restart

- **Action**: a follower is stopped mid-stream, then restarted.
- **Expected state**: rejoins, catches up via log replication or
  snapshot install, converges.
- **Invariants**: `STATE MACHINE SAFETY`.
- **Phase**: 5 (log catch-up), 6 (snapshot catch-up).
- **Status**: log-catch-up leg passing in the deterministic simulator
  (Phase 4) —
  `internal/fault/cluster_test.go::TestRestartSafety_LogAndCommitmentSurvive`
  — **and** now passing (Phase 5) against real disk/network/process
  restart —
  `internal/node/node_test.go::TestFollowerRestartCatchesUpViaLogReplication`,
  `cmd/chronicledb-node/main_test.go::TestRealProcesses_ElectionReplicationCrashRestartFailover`
  (real SIGKILL + process restart). The snapshot-catch-up leg is now
  also passing (Phase 6) —
  `internal/node/node_test.go::TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction`
  (see SN-5 above).

### RF-4: Leader crash before quorum

- **Action**: leader proposes an entry, crashes before any follower
  persists it.
- **Expected state**: entry is absent from the eventual new leader's
  log; safe to have vanished (§ [`docs/failure-model.md`](failure-model.md) §2.1-2.2).
- **Invariants**: none violated by its absence.
- **Phase**: 5.
- **Status**: passing (Phase 5) against real disk/network —
  `internal/node/node_test.go::TestRF4_LeaderCrashBeforeQuorumEntryVanishes`.

### RF-5: Leader crash after quorum

- **Action**: leader proposes an entry, a majority persists it, leader
  crashes before applying or replying.
- **Expected state**: entry **is** committed; new leader's log/state
  includes it once elected and caught up.
- **Client-visible**: retry-by-`RequestID` against new leader returns
  `COMMITTED`.
- **Invariants**: `LEADER COMPLETENESS`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 5.
- **Status**: passing (Phase 5) against real disk/network —
  `internal/node/node_test.go::TestRF5_LeaderCrashAfterQuorumBeforeApply`,
  `cmd/chronicledb-node/main_test.go::TestRealProcesses_ElectionReplicationCrashRestartFailover`.
  This is proven for "committed but not yet applied *by this specific
  process* before it crashes" in the sense that a majority (of which
  this leader may or may not be one) has durably persisted the entry
  before the crash; `internal/node.Node.Propose` itself only ever
  returns after its own apply completes, so the precise sub-instant
  "quorum reached, this leader's own apply specifically still
  pending" is not independently isolated as its own test — the
  end-to-end guarantee it protects (the entry survives and a retry
  observes `COMMITTED`) is what is directly tested.

### RF-6: Leader crash after quorum before response

- Same as RF-5, explicitly framed around the client-visible `UNKNOWN`
  -> retry -> `COMMITTED` flow (see
  [`docs/transactions.md`](transactions.md) §7).
- **Invariants**: `REQUEST OUTCOME STABILITY`.
- **Phase**: 5.
- **Status**: passing (Phase 5) — same tests as RF-5, plus
  `internal/node/node_test.go::TestIdempotencyAcrossFailover` for the
  explicit retry-by-`RequestID`-resolves-identically framing, and
  `TestClusterRestartRecoversFSMAndRequestIDOutcomes` for the same
  property after a *full* cluster restart (see
  [`docs/raft.md`](raft.md) §10.3's Phase 5 implementation note on why
  that case specifically requires the client's own retry, not just
  automatic recovery).

### RF-7: Duplicate `AppendEntriesRPC`

- **Action**: simulator delivers the same `AppendEntriesRPC` twice.
- **Expected state**: no duplicate log entries; second delivery is a
  no-op or harmless re-confirmation.
- **Invariants**: `RAFT LOG MATCHING`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/raft/core_test.go::TestAppendEntriesDuplicateIsNoOp`,
  `internal/fault/cluster_test.go::TestDuplicateAndDroppedMessages_RF7`.
  Not independently re-proven against real sockets in Phase 5: this is
  `internal/raft.Core`-internal protocol logic, transport-independent
  by construction (docs/architecture.md §5), and the identical,
  unmodified `Core` now runs in production via `internal/node` — see
  [`docs/raft.md`](raft.md) §10.2.

### RF-8: Stale `AppendEntriesRPC`

- **Action**: an `AppendEntriesRPC` from a since-superseded (lower)
  term is delivered late.
- **Expected state**: rejected by the receiver's term check; no state
  change.
- **Invariants**: `RAFT ELECTION SAFETY`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/raft/core_test.go::TestAppendEntriesRejectsStaleTerm`. Not
  independently re-proven against real sockets in Phase 5, for the same
  reason as RF-7.

### RF-9: Divergent suffix

- **Action**: a former leader's uncommitted tail conflicts with the
  current leader's authoritative entries at the same positions.
- **Expected state**: divergent suffix truncated and overwritten;
  never affects already-committed entries.
- **Invariants**: `RAFT LOG MATCHING`, `LEADER COMPLETENESS`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/raft/core_test.go::TestDivergentSuffixTruncated`,
  `internal/fault/cluster_test.go::TestStaleLeaderSafety_HealAndStepDown`
  — **and** now passing (Phase 5) through the real
  `internal/wal`-backed `raft.Storage` adapter's `Truncate` —
  `internal/node/node_test.go::TestRF13_OldLeaderRejoinsAndConverges`
  (the isolated old leader's speculative, uncommitted entry is
  durably truncated from its real on-disk log and never resurfaces).

### RF-10: Stale leader

- **Action**: a partitioned former leader continues believing it is
  leader; partition heals.
- **Expected state**: it observes a higher term and steps down
  immediately.
- **Invariants**: `RAFT ELECTION SAFETY`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestStaleLeaderSafety_HealAndStepDown`
  — **and** now passing (Phase 5) against real disk/network —
  `internal/node/node_test.go::TestRF13_OldLeaderRejoinsAndConverges`.

### RF-11: Minority partition

- **Action**: one node is isolated from the other two.
- **Expected state**: isolated node cannot commit new entries; the
  majority side continues normally. Full contract in
  [`docs/replication.md`](replication.md) §5.
- **Invariants**: `QUORUM SAFETY`.
- **Phase**: 5, 7.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestQuorumSafety_MinorityPartitionCannotCommit`
  — **and** now passing (Phase 5) against a real TCP transport (using
  `internal/transport.Transport.Block`/`Unblock` for partition
  injection) —
  `internal/node/node_test.go::TestRF11_MinorityPartitionCannotCommit`.
  The chaos (randomized, combined-fault) variant is now also passing
  (Phase 7): `internal/fault/chaos_test.go::TestChaos_QuorumSafetyRandomizedPartitionTiming`
  (randomized-timing minority partition, many seeds, at the raft-core
  layer) and `internal/node/chaos_test.go::TestChaos_AsymmetricPartitionNoSafetyViolation`
  (a real, directional-only partition against real TCP, via the new
  `Transport.BlockSend`/`BlockRecv`) and `cmd/chronicledb-node/chaos_test.go::TestRealChaos_RealPartitionHeal`
  (a genuine partition between real OS processes via the new `/fault`
  control-plane endpoint).

### RF-12: Majority election

- **Action**: current leader is stopped or partitioned away from a
  majority.
- **Expected state**: the majority elects a new, legitimate leader
  within bounded simulated time.
- **Invariants**: `RAFT ELECTION SAFETY`, `LEADER COMPLETENESS`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestQuorumSafety_MinorityPartitionCannotCommit`,
  `TestStaleLeaderSafety_HealAndStepDown` — **and** now passing
  (Phase 5) against real disk/network/processes —
  `internal/node/node_test.go::TestRF12_MajorityElectsNewLeaderDuringPartition`,
  `cmd/chronicledb-node/main_test.go::TestRealProcesses_ElectionReplicationCrashRestartFailover`.

### RF-13: Old leader rejoin

- **Action**: following RF-10/RF-11, the old leader's partition heals
  and it rejoins.
- **Expected state**: it catches up (log replication or snapshot) and
  resumes as a normal follower; no committed entry lost.
- **Invariants**: `RAFT LOG MATCHING`, `LEADER COMPLETENESS`.
- **Phase**: 5, 7.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestStaleLeaderSafety_HealAndStepDown`
  — **and** now passing (Phase 5) against real disk/network —
  `internal/node/node_test.go::TestRF13_OldLeaderRejoinsAndConverges`
  (heals the partition, confirms the old leader steps down, its
  never-committed speculative write is truncated and never
  materializes, and all three real nodes converge on the committed
  value). The chaos variant is now also passing (Phase 7):
  `internal/fault/chaos_test.go::TestChaos_RepeatedPartitionHealAcrossLeaders`
  (raft-core layer, seeded, several partition/heal cycles across
  changing leaders, checked against a `committedOracle`) and
  `internal/node/chaos_test.go::TestChaos_RepeatedPartitionHealAcrossLeaders`
  (the real-disk/real-TCP equivalent).

### RF-14: Slow follower

- **Action**: one follower's simulated network delay is set much
  higher than the others', without a full partition.
- **Expected state**: cluster continues committing via the two
  responsive nodes; slow follower eventually catches up.
- **Invariants**: `QUORUM SAFETY`.
- **Phase**: 5.
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestFollowerLagAndSlowFollower`. Not
  re-proven against real sockets in Phase 5 for the same reason as
  RF-2 — `internal/transport` does not yet model non-zero, still-
  delivered latency, only outright block/unblock.

### RF-15: Repeated election

- **Action**: adversarial timer scheduling causes several elections in
  succession before one stabilizes (e.g. simulated split votes).
- **Expected state**: eventually converges to exactly one stable
  leader per term thereafter; no committed entry is ever lost or
  duplicated across the churn.
- **Invariants**: `RAFT ELECTION SAFETY`, `STATE MACHINE SAFETY`.
- **Phase**: 5, 7 (chaos variant).
- **Status**: passing in the deterministic simulator (Phase 4) —
  `internal/fault/cluster_test.go::TestElectionSafety_AtMostOneLeaderPerTerm`
  (forces repeated elections via leader crashes),
  `internal/fault/property_test.go::TestProperty_RandomizedScheduleNeverViolatesSafety`
  (40 randomized seeds combining elections, partitions, crashes, and
  restarts, asserting election safety and log matching hold after every
  scheduled action). Real deployments in this phase's own tests do
  observe more than one election in succession (e.g. a term-1 leader
  crashing and a term-2 leader being elected in
  `cmd/chronicledb-node/main_test.go`), but an adversarial,
  specifically-repeated-election stress scenario is not independently
  reproduced against real sockets/processes. The full chaos variant is
  now also passing (Phase 7):
  `internal/fault/chaos_test.go::TestChaos_CombinedRandomizedSchedule`
  (a much richer combined action space than the Phase 4 property test —
  elections, proposals, crashes/restarts, partitions/isolation/heals,
  message drop/duplicate/delay, and local compaction — checked after
  every action, seeded; default a fast CI-sized seed count, or a much
  larger count via the `CHRONICLEDB_CHAOS_SEEDS` environment variable)
  and `internal/fault/chaos_test.go::TestChaos_AsymmetricPartitionSafety`.
  A genuine Raft liveness bug this exact chaos scenario surfaced (a node
  stepping down from Leader/Candidate without granting the triggering
  vote/response could be left with no election timer ever armed again)
  is documented and fixed in
  [`docs/testing-strategy.md`](testing-strategy.md) §7.

---

## Snapshots

### SN-1: Normal snapshot restore

- **Action**: node creates a snapshot at index `I`; later restarts.
- **Expected state**: recovery restores from the snapshot and replays
  only entries after `I`.
- **Invariants**: `SNAPSHOT SAFETY`.
- **Phase**: 6.
- **Status**: passing — `internal/node/node_test.go::TestSN1_RestartRestoresFromSnapshotAndCompactsLog` proves both halves against a real disk-backed node: state as of `I` is restored from the snapshot alone (no replay needed), and the reopened WAL's own `FirstIndex()` confirms only entries after `I` remain durable at all.

### SN-2: Crash during snapshot creation

- **Action**: process killed while writing the temp snapshot file.
- **Expected state**: orphaned temp file ignored on restart; prior
  snapshot (if any) or full log replay used instead.
- **Invariants**: `SNAPSHOT SAFETY`.
- **Phase**: 6.
- **Status**: passing — `internal/snapshot/manager_test.go::TestManagerCleansStaleTempFilesOnOpen` (stale temp file never considered a candidate, cleaned up on next open) and `TestManagerCreateLeavesNoTempFileOnSuccess`/`TestManagerRetainsOnlyLatestAfterNewCreate` (a prior valid snapshot is never disturbed until a new one is itself fully durable).

### SN-3: Interrupted snapshot installation

- **Action**: a follower receiving a snapshot is killed mid-transfer
  or mid-validation.
- **Expected state**: restarts installation from scratch on
  reconnection; no partial state ever considered installed.
- **Invariants**: `SNAPSHOT SAFETY`.
- **Phase**: 6, 7 (chaos variant).
- **Status**: passing — `internal/snapshot/manager_test.go::TestManagerInstallValidatesBeforeWriting` (a payload that fails validation touches nothing on disk at all) and `TestManagerInstallSucceedsAndIsLoadable` (a successful install is immediately, durably loadable); `internal/node.Node.handleInstallSnapshot` calls `Manager.Install` before ever touching `raft.Core` or durable log state, so a kill at any point before `Install` returns leaves nothing installed to resume from but the prior state. The chaos variant is now also passing (Phase 7):
  `internal/fault/chaos_test.go::TestChaos_SnapshotMessageChaos` (leader
  compaction combined with drop/duplicate/delay chaos specifically
  targeting `MsgInstallSnapshotRequest`/`Response` traffic, seeded, at
  the raft-core message-protocol layer),
  `internal/node/chaos_test.go::TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly`
  (a real follower crashed and restarted immediately after healing, before
  catch-up could possibly have completed, against real disk/network), and
  `cmd/chronicledb-node/chaos_test.go::TestRealChaos_SIGKILLDuringSnapshotInstall`
  (genuine, repeated real-process SIGKILL attempts timed around a real
  snapshot install — best-effort timing, since a real OS process is not
  as controllable as the simulator, but the safety property — no
  partial/corrupt state, eventual clean convergence — is asserted
  unconditionally). This exact combination (a live, non-restarted
  snapshot install followed by further real replication) is what
  surfaced a genuine WAL bug, fixed and covered by its own deterministic
  regression test — see [`docs/testing-strategy.md`](testing-strategy.md)
  §7.

### SN-4: Corrupted snapshot

- **Action**: a snapshot file's checksum is deliberately invalidated.
- **Expected state**: never used; fallback to an older valid snapshot
  or operator intervention per [`docs/recovery.md`](recovery.md) §4.
- **Invariants**: `SNAPSHOT SAFETY`, `RECOVERY NON-INVENTION`.
- **Phase**: 6.
- **Status**: passing — `internal/snapshot/manager_test.go::TestManagerLoadFallsBackOnCorruption` (older valid snapshot used instead) and `internal/wal/snapshot_test.go::TestOpenRefusesWhenLogGapExceedsSnapshotPointer` (the operator-intervention leg: no valid snapshot covers the durable log's own gap, so `internal/node.Open` refuses to start — see `ErrRecoveryGap`).

### SN-5: Follower catch-up via snapshot

- **Action**: a follower's required log range has already been
  compacted away by the leader.
- **Expected state**: leader sends a snapshot instead of log entries;
  follower installs it and resumes normal replication.
- **Invariants**: `SNAPSHOT SAFETY`, `LOG COMPACTION SAFETY`.
- **Phase**: 6, 7 (chaos variant: catch-up followed by further real
  replication).
- **Status**: passing — `internal/raft/snapshot_test.go::TestAppendEntriesMessageSendsInstallSnapshotWhenPeerBehindSnapshot` (leader-side trigger) and `internal/node/node_test.go::TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction` (end-to-end against real disk/network: an isolated follower, healed after the leader has compacted past what it needs, is proven caught up via an actual installed snapshot — its own `FirstIndex()` boundary moves to match, which only a genuine install ever does — not merely eventual convergence by some other means). The Phase 7 chaos variant specifically stresses "resumes normal replication" beyond mere catch-up —
  `cmd/chronicledb-node/chaos_test.go::TestRealChaos_SIGKILLDuringSnapshotInstall`
  proposes further entries after a real follower's snapshot install and
  confirms they replicate normally; this is exactly the combination that
  surfaced a genuine `wal.WAL.Truncate` bug (a live, non-restarted
  snapshot install did not durably-equivalently advance the WAL's own
  next-log-index counter, fatally erroring the very next real append) —
  fixed, with a deterministic regression test at
  `internal/wal/snapshot_test.go::TestTruncateJumpsNextIndexForwardPastInstalledSnapshotGap`;
  see [`docs/testing-strategy.md`](testing-strategy.md) §7.

### SN-6: Safe log truncation after snapshot

- **Action**: snapshot at index `I` completes and validates; WAL
  segments fully at or before `I` are deleted.
- **Expected state**: deletion never precedes confirmed snapshot
  durability; recovery after a crash immediately following truncation
  still succeeds using the snapshot.
- **Invariants**: `LOG COMPACTION SAFETY`.
- **Phase**: 6.
- **Status**: passing — `internal/wal/snapshot_test.go::TestCompactBeforeDeletesOnlySegmentsFullyAtOrBeforeBoundary`, `TestCompactBeforeNeverDeletesCurrentSegment`, and `TestReopenAfterSnapshotAndCompactionReplaysOnlyRemainingEntries` (a real reopen immediately after compaction correctly replays only what remains, using the snapshot for everything before it).

---

## SQL

Per [ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md),
these scenarios were deliberately not added until Phase 8 began — they
test correct *translation* into the already-proven transaction API
(`docs/sql.md` §5.1), not durability/MVCC/Raft properties already
covered above.

### SQ-1: `CREATE TABLE` and duplicate-table rejection

- **Action**: `CREATE TABLE` a new table; `CREATE TABLE` the identical
  name again.
- **Expected state**: the first commits (schema durably recorded, real
  `CommitTxn` path); the second is rejected `ErrDuplicateTable` without
  mutating anything, checked against real committed state, not a local
  cache.
- **Invariants**: `CONSISTENT LOG RESPONSIBILITY` (schema is ordinary
  committed MVCC state, no second history).
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecCreateTable`, `TestExecCreateTableDuplicate`, `TestExecCreateTableInvalidSchema` (missing/multiple primary keys, unsupported types, duplicate columns — `schema_test.go::TestBuildSchema*`).

### SQ-2: `INSERT` `RequestID` retry safety

- **Action**: `INSERT` a row under `RequestID` R; retry the identical
  statement under the identical R.
- **Expected state**: the retry returns the original outcome (same
  `CommitSeq`) without re-evaluating the statement's own semantic
  guards (duplicate-primary-key check) against state that already
  reflects the original attempt's own effect — exactly one row exists,
  never a duplicate, never a false rejection.
- **Invariants**: `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY`.
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecInsertRetrySameRequestID` (standalone); `internal/sql/distributed_test.go::TestDistributedSQLLeaderFailoverRetry` (the identical property against a real cluster, across a genuine leader crash — see SQ-8).

### SQ-3: `INSERT` schema/type/primary-key validation

- **Action**: `INSERT` with a type-mismatched literal; `INSERT` a
  primary-key value that already has a visible row; `INSERT` naming an
  unknown table or column.
- **Expected state**: each rejected explicitly (`ErrTypeMismatch`,
  `ErrDuplicatePrimaryKey`, `ErrUnknownTable`/`ErrUnknownColumn`)
  before any write is attempted.
- **Invariants**: `CONFLICT CORRECTNESS` (the duplicate-key check is a
  SQL-level guard in front of the identical underlying conflict rule).
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecInsertTypeMismatch`, `TestExecInsertDuplicateKey`, `TestExecInsertUnknownTable`, `TestExecInsertColumnList`, `TestExecInsertMissingColumnRejected`.

### SQ-4: `SELECT` — primary-key lookup and full-table scan

- **Action**: `SELECT` with a primary-key equality predicate; `SELECT`
  with no predicate over a multi-row table; `SELECT` after inserting,
  updating, and deleting rows in the same explicit transaction, before
  `COMMIT`.
- **Expected state**: the point lookup returns exactly the matching row
  (or none); the full scan returns every currently-visible row,
  deterministically ordered, in `O(size of the whole store)` — an
  explicitly documented, un-indexed scan (`docs/sql.md` §5.2), not a
  claim of query-planner-level performance; a read inside an open
  explicit transaction sees its own not-yet-committed writes
  (docs/mvcc.md §3 step 1) merged correctly with committed data.
- **Invariants**: `MVCC VISIBILITY`.
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecSelectPrimaryKeyLookup`, `TestExecSelectMissingRow`, `TestExecSelectProjection`, `TestExecSelectFullScanDeterministic`; `internal/sql/engine_test.go::TestTxnScanPrefixMergesLocalAndCommitted` (own-write-shadowing merge into a full scan); `internal/sql/exec_test.go::TestExecExplicitTransactionCommit` (read-your-own-writes before `COMMIT`).

### SQ-5: `UPDATE`/`DELETE` — missing row, tombstones, re-insert

- **Action**: `UPDATE`/`DELETE` with a predicate matching no visible
  row; `DELETE` an existing row, then `SELECT` (point and full-scan);
  `INSERT` the identical primary key again after deleting it.
- **Expected state**: the missing-row case is an explicit
  `ErrRowNotFound` (this subset's documented deviation from a silent
  zero-rows-affected success, `docs/sql.md` §2.5); a deleted row is an
  ordinary MVCC tombstone, invisible to both lookup shapes; a primary
  key freed by `DELETE` may be reused by a later `INSERT`.
- **Invariants**: `MVCC VISIBILITY`, `ABORT SAFETY`'s tombstone
  handling.
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecUpdateMissingRow`, `TestExecDeleteMissingRow`, `TestExecDeleteValid`, `TestExecDeleteTombstoneNotResurrectedByFullScan`, `TestExecDeleteThenReinsertSamePrimaryKey`; type/column/primary-key-immutability guards in `TestExecUpdateInvalidColumn`, `TestExecUpdateTypeMismatch`, `TestExecUpdateCannotModifyPrimaryKey`.

### SQ-6: Explicit `BEGIN`/`COMMIT`/`ROLLBACK` transaction semantics

- **Action**: `BEGIN`; several `INSERT`s; `COMMIT`. Separately: `BEGIN`;
  an `INSERT`; `ROLLBACK`. Separately: `BEGIN`; a valid `INSERT`; a
  second `INSERT` that fails (duplicate key); observe the whole
  transaction's fate.
- **Expected state**: every statement between `BEGIN` and `COMMIT`
  accumulates into **one** deterministic `CommitTxn` command
  (docs/transactions.md §3), submitted only at `COMMIT` — not one
  command per statement; `ROLLBACK` leaves no trace at all,
  immediately and after restart; a failing statement aborts the
  **entire** open transaction, including its otherwise-valid earlier
  statements, not just the failing one.
- **Invariants**: `ATOMICITY`, `ABORT SAFETY`.
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecExplicitTransactionCommit`, `TestExecExplicitTransactionRollback`, `TestExecExplicitTransactionAbortsWholeTransactionOnStatementError`, `TestExecBeginWhileAlreadyActive`, `TestExecCommitWithoutBegin`, `TestExecRollbackWithoutBegin`; `internal/sql/restart_test.go::TestRestartSurvivesRolledBackAndAbortedWork` (the restart leg).

### SQ-7: Snapshot Isolation write skew through SQL

- **Action**: two explicit SQL transactions each read two rows' values,
  each write to a *different* one of the two based on what they read,
  and both `COMMIT`.
- **Expected state**: both commit successfully (no overlapping write
  set, so first-committer-wins never fires) even though the resulting
  state violates an invariant no serial execution could have produced
  — the textbook SI write-skew example (docs/mvcc.md §1.1), now
  demonstrated through the SQL frontend as a living counterexample
  against any accidental SERIALIZABLE claim.
- **Invariants**: `ISOLATION TRUTHFULNESS`.
- **Phase**: 8.
- **Status**: passing — `internal/sql/exec_test.go::TestExecWriteSkewIsPossibleUnderSnapshotIsolation`.

### SQ-8: Distributed SQL — replication and failover retry

- **Action**: `CREATE TABLE`/`INSERT` against a real three-node
  cluster's leader; confirm every node applies the identical row.
  Separately: `INSERT` under `RequestID` R against the leader, crash
  the leader, retry the identical statement under R against the newly
  elected leader.
- **Expected state**: every node's own `internal/mvcc.Store` becomes
  visible-identical for the row, and a `SELECT` against the leader
  returns it; the post-failover retry returns the identical
  `CommitSeq` with no duplicate row and no error — the same central
  Phase 5 acceptance scenario (`docs/roadmap.md`), now exercised
  through the SQL frontend rather than the raw `CommitTxnCommand` API.
- **Invariants**: `STATE MACHINE SAFETY`, `IDEMPOTENCY`,
  `REQUEST OUTCOME STABILITY`.
- **Phase**: 8.
- **Status**: passing — `internal/sql/distributed_test.go::TestDistributedSQLInsertReplicates`, `TestDistributedSQLLeaderFailoverRetry`. Building this scenario found and fixed a genuine Phase 5 liveness bug in `internal/node.Node.BeginReadIndex` (a newly elected leader with no proposal yet in its own term could stall `ReadIndex` indefinitely) — see [ADR-0014](adr/0014-election-no-op-for-readindex-liveness.md) and [`docs/testing-strategy.md`](testing-strategy.md) §7.

### SQ-9: SQL state survives restart and snapshot/compaction

- **Action**: create a table and rows via SQL; close and reopen the
  same on-disk WAL directory (standalone mode). Separately: create
  enough SQL-driven rows against a real cluster to cross a small
  `SnapshotThreshold`, crash a follower after it has created its own
  snapshot, restart it.
- **Expected state**: schema and every committed row are visible via
  SQL after the standalone restart, with duplicate-table and
  type-validation guards still enforced; the restarted follower's
  local `internal/mvcc.Store`, rebuilt from a real installed snapshot
  rather than full log replay, has every row a real Raft cluster
  actually committed.
- **Invariants**: `SNAPSHOT SAFETY`, `RECOVERY NON-INVENTION`.
- **Phase**: 8.
- **Status**: passing — `internal/sql/restart_test.go::TestRestartSurvivesSchemaAndData`; `internal/sql/distributed_test.go::TestDistributedSQLSnapshotCompactionSurvivesRestart`.

---

## Roadmap-phase index

| Phase | Scenarios first executable |
|---|---|
| 1 | LD-1 .. LD-6 |
| 2 | TX-1 .. TX-8 |
| 3 | ID-1 .. ID-5 (partial) |
| 4 | RF-1, RF-2, RF-3 (log-catch-up leg), RF-7 .. RF-15 — in the deterministic simulator only (see the Phase 4 note above) |
| 5 | RF-1, RF-3 (log-catch-up leg), RF-4, RF-5, RF-6, RF-9 .. RF-13 in a real multi-process/real-disk deployment (see the Phase 5 note above); RF-2, RF-7, RF-8, RF-14, RF-15 remain simulator-only by deliberate scope (see each entry and [`docs/raft.md`](raft.md) §10.2); ID-4 (immediate/after-restart already from 3) |
| 6 | SN-1 .. SN-6, ID-4 (after snapshot+compaction), RF-3's snapshot-catch-up leg |
| 7 | Chaos/combined variants of RF-11, RF-13, RF-15 and SN-3/SN-5 under randomized fault schedules, plus RequestID/transaction chaos and genuine real-process SIGKILL evidence not tied to a single numbered scenario (see the Phase 7 note above and [`docs/testing-strategy.md`](testing-strategy.md) §6-7) |
| 8 | SQ-1 .. SQ-9 |
| 9 | No new numbered scenarios: Phase 9 (benchmarks/observability) adds no new correctness invariant or scenario — per `docs/testing-strategy.md` §2's guiding principle, this corpus targets correctness properties, not performance/benchmark evidence. Its own testing evidence (benchmark correctness checks, observability tests) is itemized in [`docs/testing-strategy.md`](testing-strategy.md) §9 and [`docs/benchmarks.md`](benchmarks.md)/[`docs/observability.md`](observability.md), mirroring how Phase 7's non-scenario-mapped evidence is handled below. |
| 10 | No new numbered scenarios: Phase 10 is a verification pass over the invariant catalog and the scenarios already listed above, using an independent reference model and more deeply combined adversarial histories — it deliberately adds no new product functionality to translate into a new scenario shape. Its own evidence (model-based history suites, targeted Raft/WAL/cross-layer adversarial tests, a new snapshot-decoder fuzz target, exact seed counts and commands) is itemized in full in [`docs/adversarial-testing.md`](adversarial-testing.md) and summarized in [`docs/testing-strategy.md`](testing-strategy.md) §10. |

Scenarios with an explicit **Status: passing** line above currently
pass at the stated scope — LD-1 through LD-6, TX-1 through TX-8, ID-1
through ID-7 (immediate/after-restart and, as of Phase 6, ID-4's
after-snapshot+compaction leg), the Phase-4 simulator-only subset of
RF-1 through RF-15, the Phase-5 real-disk/real-network/real-process
subset of RF-1 through RF-15 listed in the Phase 5 row above, SN-1
through SN-6 plus RF-3's snapshot-catch-up leg (Phase 6), the
Phase-7 chaos variants of RF-11, RF-13, RF-15, SN-3, and SN-5 called
out in each of those entries' own Status lines above, and, as of
Phase 8, SQ-1 through SQ-9. Phase 10 adds no new entries to this list —
it strengthens the evidence behind several of the scenarios already on
it (see [`docs/adversarial-testing.md`](adversarial-testing.md)) without
changing which scenarios are claimed to pass. Every
other scenario in this document is not claimed to pass. A passing
Phase-4 (simulator-only) RF-\* status is not by itself a claim that
scenario's real multi-process (Phase 5) leg passes — check the specific
scenario's own Status line, since as of Phase 5 several (but not all)
now carry both, and likewise a Phase 5/6 status is not by itself a
claim that a scenario's Phase 7 chaos variant passes. This table exists
to make future maturity claims falsifiable: a claim that ChronicleDB
has reached a given roadmap phase must be checked against whether the
scenarios listed for that phase (and all prior phases), at the scope
that phase actually requires, have passing, reproducible tests. Not
every category Phase 7's own brief describes maps to a single numbered
scenario here (RequestID-retry-across-multiple-leadership-epochs,
transaction atomicity under a leader crash, and the real-process SIGKILL
evidence are proven by named tests cited in
[`docs/testing-strategy.md`](testing-strategy.md) §7 rather than a
dedicated corpus entry) — that document's §7 is the authoritative,
itemized Phase 7 evidence list; this table's Phase 7 row is a summary,
not the complete accounting.
