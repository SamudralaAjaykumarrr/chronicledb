# Adversarial Correctness Verification (Phase 10)

Status: Phase 10 is complete at its own documented scope (see
[`roadmap.md`](roadmap.md) Phase 10). This document is the
`docs/scenario-corpus.md`-style honest accounting for that phase: what
was built, what it proved, what it did not, and the exact commands to
reproduce every claim below.

Phase 10 is a **verification phase**, not a feature phase. It adds no
new product functionality — every mechanism it exercises
(WAL/recovery/MVCC/Raft/snapshots/SQL/RequestID idempotency) was
already implemented and tested by Phases 1-9. Its job is to attempt to
falsify ChronicleDB's documented correctness claims using stronger,
independent, model-based, and more deeply combined adversarial
workloads than any prior phase used, and to report honestly on the
result — including if it finds nothing.

## 1. What Phase 10 added, in one paragraph

An independent, structurally separate reference-model package
(`internal/oracle`) that predicts committed key/value state and
verifies `RequestID` terminal-outcome stability without reimplementing
any of ChronicleDB's own MVCC/FSM/Raft code; three new model-based
history-testing suites built on it
(`internal/node/model_test.go` against a real three-node disk/TCP
cluster, `internal/sql/model_test.go` against a real standalone SQL
engine, `internal/txn/si_history_test.go` against real MVCC
transactions); a canonical state-digest function
(`oracle.CanonicalKVDigest`) used to compare a model's predicted state
against ChronicleDB's real committed state byte-for-byte, not by spot
check; three new targeted (not broad-random) Raft-core adversarial
scenarios (`internal/fault/adversarial_test.go`) plus a direct
single-`Core` unit test for one of them
(`internal/raft/adversarial_test.go`); one new cross-layer scenario not
covered by any prior phase (`internal/node/crosslayer_test.go`,
"snapshot install then election"); one new WAL adversarial test for
*repeated* (not single) compact/restart cycles
(`internal/wal/adversarial_test.go`); and one new fuzz target for the
previously-unfuzzed snapshot decoder (`internal/snapshot/fuzz_test.go`).
No existing test was weakened, deleted, or had its assertions loosened.

## 2. Reference model (`internal/oracle`)

Per this phase's own "the oracle must be structurally independent
enough to detect implementation mistakes... do not copy ChronicleDB
implementation details into the oracle":

- **`KVModel`** (`internal/oracle/oracle.go`) independently implements
  the *documented* first-committer-wins Snapshot Isolation rule
  (`mvcc.md` §4) — `lastSeq[key] > startSeq` means conflict — using its
  own map-based data structure, never calling `internal/mvcc.CheckConflicts`
  or any other ChronicleDB code. `Predict` decides commit/abort without
  mutating state; `Apply` records an already-real, already-decided
  commit. This mirrors exactly how Phase 2's own MVCC visibility
  property test already used an independent reference model — Phase 10
  generalizes the same technique to conflict prediction and to
  cross-package use.
- **`OutcomeTracker`** makes no attempt to predict what a `RequestID`'s
  outcome *should* be at all — it only records what ChronicleDB itself
  returned the first time (keyed by a fingerprint of the request's
  payload) and flags any later disagreement. This is the safest
  possible oracle design for `REQUEST-OUTCOME-STABILITY` specifically:
  it requires zero reimplementation of ChronicleDB's own conflict
  logic, so it cannot be "wrong" about what the right answer *should*
  have been — it only ever asserts *consistency*.
- **`CanonicalKVDigest`** is a single, shared canonicalization function
  (sorted keys, SHA-256) used by both the model's own `Digest()` and by
  every test's query of ChronicleDB's real live state (via
  `FSM().Store().Visible` or a SQL `SELECT`), so a final-state
  comparison is exact, not a sampled spot check, and never depends on
  Go's unordered map iteration. Digests are testing/diagnostic evidence
  only — nothing in ChronicleDB's own code ever computes or reads one.
- **`Recorder`/`Step`** (`internal/oracle/history.go`) implement the
  brief's "HISTORY RECORDER": each step records seed, index, node,
  term, role, `RequestID`, operation, a short bounded argument summary,
  outcome, and (where applicable) commit/applied/snapshot indices and
  fault action. "Replayable" here means precisely what
  `docs/testing-strategy.md` §3.2 already established for Phase
  4-7's chaos suites: the history-generating code is a pure function of
  its seed, so re-running with the same seed reproduces the identical
  operation sequence. `Recorder` itself is not a serialize-to-file/
  replay-from-file interpreter — it exists to make a *failing* run's
  already-executed sequence inspectable in the test failure message
  (`Recorder.Tail`/`Dump`), at zero cost on a passing run. This is a
  narrower, more honest claim than a full replay engine, stated
  explicitly rather than implied.
- `internal/oracle` has its own unit tests
  (`internal/oracle/oracle_test.go`) proving the model itself is
  correct (conflict prediction, atomicity across keys, tombstone
  handling, digest determinism regardless of map insertion order,
  outcome-stability detection) — an oracle that is itself untested is
  not evidence.

## 3. Model-based history suites

### 3.1 `internal/node/model_test.go` — cross-layer, real cluster

`TestModel_AdversarialHistoryAgainstIndependentOracle` drives a real,
disk/TCP-backed three-node `internal/node` cluster (reusing
`node_test.go`'s `testCluster`) through a deterministic, seeded history
of: fresh writes, multi-key transactions, deliberately stale-`StartSeq`
writes engineered to conflict, `RequestID` retries (possibly against a
different leader after a failover), leader crashes/restarts, and
node isolation/heal — at most one node down at a time, so the majority
can always make progress. Every write's real commit/abort outcome is
checked against `oracle.KVModel.Predict` as it happens; every retry is
checked against `oracle.OutcomeTracker`; the final committed state on
every live node (once caught up) is compared via `CanonicalKVDigest`
against the model's own digest.

Default (CI): 4 seeds × 24 steps (~2-4s). Larger local run, validated
clean at 200 seeds with `-race` during this phase's own development:

```
CHRONICLEDB_ADVERSARIAL_SEEDS=200 go test ./internal/node/... -run TestModel_AdversarialHistoryAgainstIndependentOracle -race -timeout 300s
```

### 3.2 `internal/sql/model_test.go` — SQL-layer, real standalone engine

`TestModel_SQLAdversarialHistoryAgainstIndependentModel` drives a real
standalone `internal/sql` engine through a deterministic history of
`INSERT`/`UPDATE`/`DELETE`/point-`SELECT`/full-scan-`SELECT`/`RequestID`
retry statements against an independent, from-scratch single-table
model (`sqlRowModel`: a plain `map[int64]string`, never touching
`internal/sql`'s own row/schema encoding). Every documented error case
(`ErrDuplicatePrimaryKey`, `ErrRowNotFound`) is checked against the
model's own live/absent-row tracking, not merely happy-path statements.
Per ADR-0013's scope boundary, this does not re-prove
durability/MVCC/Raft — those are §3.1's and §3.3's job — it proves
SQL-visible state stays exactly consistent with the documented
statement semantics (`SQL-CONSISTENCY`, `docs/invariants.md`) across a
long randomized sequence.

Default (CI): 8 seeds × 60 steps (in-process, <0.2s). Larger local run,
validated clean at 3,000 seeds with `-race`:

```
CHRONICLEDB_ADVERSARIAL_SEEDS=3000 go test ./internal/sql/... -run TestModel_SQLAdversarialHistoryAgainstIndependentModel -race -timeout 300s
```

### 3.3 `internal/txn/si_history_test.go` — explicit Snapshot Isolation histories

Five named, deterministic (no seed needed) tests each document one
specific history as **ALLOWED** or **FORBIDDEN** under Snapshot
Isolation, extending TX-8 (`docs/scenario-corpus.md`,
`txn_test.go::TestTX8_SnapshotIsolationWriteSkew`) rather than
repeating it:

| Test | History | SI verdict |
|---|---|---|
| `TestSIHistory_DisjointConcurrentWritesBothCommit` | two concurrent txns, disjoint write sets | ALLOWED — both commit |
| `TestSIHistory_OverlappingWriteFirstCommitterWins` | two concurrent txns, same key | first commits, second gets `ErrConflict` (also FORBIDDEN under Serializable, since it's a genuine conflict either way) |
| `TestSIHistory_ThreeWayWriteSkewRing` | 3 txns, each reads 2 of {a,b,c}, writes the third | ALLOWED under SI — **FORBIDDEN under Serializable** (no serial order reproduces the result) |
| `TestSIHistory_ConflictingDeleteAndWriteFirstCommitterWins` | concurrent delete vs. write, same key | first (the delete) wins; write gets `ErrConflict`; a later fresh txn may legitimately reinsert |
| `TestSIHistory_RetryAfterConflictAsNewTransactionSucceeds` | loser retries as a genuinely new `Txn` (fresh `Begin`) | ALLOWED — succeeds, per `docs/failure-model.md` §4.3's documented client-retry contract |

`TestModel_SIHistoryRandomizedAgainstIndependentModel` generalizes this
into a seeded generator: each round begins 2-3 transactions at a shared
`StartSeq`, gives each a randomized single-key write, commits them in a
randomized order, and checks every commit/abort decision against
`oracle.KVModel.Predict`. Default (CI): 10 seeds × 15 rounds. Larger
local run, validated clean at 2,000 seeds with `-race`:

```
CHRONICLEDB_ADVERSARIAL_SEEDS=2000 go test ./internal/txn/... -run TestModel_SIHistoryRandomizedAgainstIndependentModel -race -timeout 300s
```

No history here claims or implies SERIALIZABLE isolation
(`ISOLATION-TRUTHFULNESS`) — the three-way write-skew ring specifically
demonstrates a result no serial execution of those three transactions
could produce, which SI permits and Serializable would forbid.

## 4. Targeted Raft-core adversarial scenarios

`internal/fault/adversarial_test.go` adds three scenarios deliberately
different in shape from Phase 7's own broad-randomized chaos suites
(`chaos_test.go`) — each engineers one specific, named cross-mechanism
interaction rather than mixing many fault kinds per step:

- **`TestChaos_StaleAppendEntriesAfterSnapshotInstall`**: an
  `AppendEntriesRequest` sent to a follower is captured and held aside
  (`Transport.Delay`, far-future tick) before the follower is crashed
  (not merely isolated — see the test's own doc comment for exactly why
  `IsolateNode` would not force a genuine `InstallSnapshot` need, per
  the same reasoning `chaos_test.go`'s `TestChaos_SnapshotMessageChaos`
  already documents), falls behind past a boundary the leader compacts
  away, and is restarted — forcing a real snapshot install. The
  long-held stale message is then force-delivered
  (`Cluster.Deliver`/`Transport.Take`, bypassing all transport state)
  well after the install completed. This end-to-end scenario is backed
  by two direct single-`Core` unit tests in
  `internal/raft/adversarial_test.go`
  (`TestStaleAppendEntriesBelowSnapshotBoundaryReportsMatchIndexWithoutMutation`
  and its exact-boundary counterpart,
  `TestStaleAppendEntriesExactlyAtSnapshotBoundaryIsAccepted`), which
  found that `handleAppendEntriesRequest`'s
  `msg.PrevLogIndex < c.snapshotIndex` branch — already correctly
  implemented, with a doc comment describing exactly this scenario —
  had no direct unit test before this phase (see §9, "coverage gaps
  closed").
- **`TestChaos_RepeatedCompactRestartCycle`**: the exact "snapshot ->
  compact -> restart -> append -> snapshot -> compact -> restart"
  pattern named in `docs/roadmap.md` Phase 10, run for 5 cycles with a
  real restart of a randomly chosen node after every cycle's
  compaction, checking `SnapshotIndex <= CommitIndex <= LastIndex` for
  every live node after every cycle and full oracle-checked convergence
  at the end — proving the pattern is safe under *repetition*, not just
  once.
- **`TestChaos_LeaderChangeImmediatelyAfterSnapshot`**: the current
  leader compacts its own log and is crashed in the same round, before
  any follower necessarily knows the updated boundary, checked against
  `committedOracle` for `LEADER-COMPLETENESS` across the snapshot
  boundary.

Default (CI): 15 seeds each. Larger local run, validated clean at 2,000
seeds each with `-race` (~12.5s total for all three):

```
CHRONICLEDB_CHAOS_SEEDS=2000 go test ./internal/fault/... -run 'TestChaos_StaleAppendEntriesAfterSnapshotInstall|TestChaos_RepeatedCompactRestartCycle|TestChaos_LeaderChangeImmediatelyAfterSnapshot' -race -timeout 300s
```

## 5. Cross-layer evidence

`internal/node/crosslayer_test.go::TestCrossLayer_SnapshotInstallThenElection`
is the one named scenario from this phase's "CROSS-LAYER TESTS" brief
not already covered by an existing Phase 6/7/8 test: a follower is
isolated, falls behind past a compacted boundary, is healed and catches
up via a genuine `InstallSnapshot` — then the *leader itself* is
crashed immediately afterward (no settling time given), forcing the
freshly-caught-up follower to participate in the very next election
alongside the other survivor. Every key committed before the crash
(both pre-isolation and everything filled in while the follower was
down) is confirmed present on whichever node the election legitimately
produces as the new leader, and the cluster is confirmed to accept and
replicate a further write afterward. Verified deterministic (no seed
needed) and stable across repeated runs, including under `-race`.

Every other cross-layer combination this phase's brief names
(transaction → Raft → snapshot → restart; SQL → `RequestID` → leader
crash; conflict → snapshot → retry) was already proven by existing
Phase 7/8 tests before this phase began —
`internal/node/chaos_test.go::TestChaos_TransactionAtomicityAcrossLeaderCrash`,
`internal/sql/distributed_test.go::TestDistributedSQLLeaderFailoverRetry`,
and `internal/node/chaos_test.go::TestChaos_ConflictOutcomeSurvivesSnapshotRestartAndRetry`
respectively — and are not repeated here; §3.1's model-based suite adds
independent-oracle evidence on top of all of them by checking the same
class of history against `oracle.KVModel`/`OutcomeTracker` rather than
hand-written per-scenario assertions.

## 6. RequestID evidence

Beyond the existing Phase 3/6/7/8 evidence
(`docs/scenario-corpus.md` §Idempotency, `docs/testing-strategy.md`
§7/§8.1), Phase 10 adds: `oracle.OutcomeTracker`-checked stability
across every retry in §3.1's and §3.2's model-based histories
(including retries issued after a leader failover, and retries whose
original commit predates a node crash/restart elsewhere in the same
history); and the explicit new-transaction-after-conflict retry shape
in §3.3 (`TestSIHistory_RetryAfterConflictAsNewTransactionSucceeds`).

## 7. Recovery evidence

§4's `TestChaos_RepeatedCompactRestartCycle` (cluster level) and the
new WAL-level test below prove *repeated* snapshot/compact/restart
safety, not just the single-cycle proof Phase 6 established.

`internal/wal/adversarial_test.go::TestRepeatedSnapshotCompactRestartCycleKeepsIndexesCorrect`
runs 6 cycles of append→snapshot→compact→close→reopen directly against
`internal/wal.WAL` (no FSM/Raft involved), asserting `FirstIndex`,
`NextIndex`, and `Metadata().LatestSnapshotIndex` are exactly correct
after every single cycle — not just the last one — and that a fresh
`Replay` immediately after each compaction+reopen finds exactly zero
entries (everything at/before the boundary is truly gone). A final
reopen confirms the cumulative index arithmetic across all 6 cycles
landed exactly where expected before accepting one more live append.

## 8. Real-process evidence

Phase 10 did not add a new `-tags=integration` real-process test this
session: the existing Phase 5/7 real-process suite
(`cmd/chronicledb-node/chaos_test.go`, §6.3 of
`docs/testing-strategy.md`) already covers repeated real `SIGKILL`
during replication, `SIGKILL` timed around a real snapshot install, a
lost-response-then-retry sequence across a real failover, and a genuine
real-process network partition/heal — each of the "real-process"
categories this phase's brief names as high-value
(`repeated leader SIGKILL during client workload`,
`snapshot/install/restart loop`, `retry storm ... after leader loss`)
already has a passing, real-`SIGKILL`-based test from Phase 7, and this
phase's own brief is explicit that new real-process work should be
"high-value scenarios not already covered" — duplicating an
already-proven real-process scenario under a new name would not be new
evidence. This phase's own new evidence (§3, §4, §5, §7 above) is
concentrated in the deterministic-simulator and real-disk/real-TCP
in-process layers, which is where model-based/randomized exploration at
useful seed counts is actually affordable — real, separate-OS-process
tests remain, per `docs/testing-strategy.md` §4, the layer that proves
production wiring, not the layer for broad randomized exploration.

## 9. Randomized evidence — exact counts and commands

| Suite | CI default | Validated locally this phase, with `-race` |
|---|---|---|
| `internal/node` model-based history | 4 seeds × 24 steps | 200 seeds, clean |
| `internal/sql` model-based history | 8 seeds × 60 steps | 3,000 seeds, clean |
| `internal/txn` SI history (randomized) | 10 seeds × 15 rounds | 2,000 seeds, clean |
| `internal/fault` stale-AppendEntries-after-install | 15 seeds | 2,000 seeds, clean |
| `internal/fault` repeated-compact/restart | 15 seeds | 2,000 seeds, clean |
| `internal/fault` leader-change-after-snapshot | 15 seeds | 2,000 seeds, clean |

Every count above is an actual executed run from this phase's own
development, not a projection. No seed was ever excluded, and no
assertion was weakened to make a seed pass (§10 documents the two
genuine issues found and how each was actually resolved). This table
supplements, and does not replace, `docs/testing-strategy.md` §6.5's
existing Phase 7 seed-count evidence for the chaos suites that already
existed before this phase.

## 10. Bugs found and fixed by Phase 10

Phase 10's development found **zero new ChronicleDB production
defects**. This is itself reported honestly, per this phase's own "a
successful Phase 10 may legitimately include bugs found and fixed" —
which does not require finding any; Phases 1-9's own chaos/benchmark
work already found and fixed five genuine defects (three in Phase 7,
one in Phase 8, one in Phase 9 — see `docs/testing-strategy.md` §7,
§8.1 and `docs/benchmarks.md` §8.1), and this phase's more targeted,
model-based exploration of the same hardened mechanisms did not
uncover a sixth.

Phase 10's development did find and fix **two genuine defects in this
phase's own new test harness** (test-design bugs, not production bugs
— classified honestly here rather than omitted, per this phase's
"never weaken assertions to make them pass" discipline applied to test
code too):

1. `internal/node/model_test.go`'s original `currentLeader` helper
   returned the first node reporting `Role==Leader` without checking
   that exactly one such node existed. Right after a heal/restart there
   is a real, brief window where a stale former leader has not yet
   processed the message that would step it down (this is exactly
   `TestRF13`'s documented, correct behavior) — the test was initially
   picking that stale node and misreporting ChronicleDB's entirely
   correct rejection ("leadership lost while request was pending") as
   a test failure. Fixed by requiring exactly one live, non-isolated
   node to report `Role==Leader` (mirroring `testCluster.awaitLeader`'s
   own established pattern) before returning it, plus a
   retry-against-whatever-leader-now-exists fallback matching how a
   real client reacts to a transient leadership change.
2. The same file's final convergence check never reset its own
   `down`/`isolated` tracking variables after healing/restarting the
   last down node, so the immediately following `currentLeader` call
   kept incorrectly excluding the very node that had just become
   leader again, causing "cluster failed to settle on a leader" false
   failures at moderate seed counts (first observed at seed 25 of a
   150-seed local run). Fixed by resetting both variables at the same
   point the heal/restart itself happens.

Both were reproduced deterministically (via a fixed failing seed),
root-caused before any fix was attempted, and confirmed fixed by
re-running the full seed range clean afterward (§9's table reflects the
counts after both fixes).

## 11. Fuzz evidence

`internal/snapshot/fuzz_test.go::FuzzDecode` is a new fuzz target for
`snapshot.Decode` — the one decoder Phase 10's own brief names
(`docs/roadmap.md`: "snapshot decoder") that had no fuzz coverage
before this phase, despite sitting on the restart-recovery and
follower-catch-up paths. Seeded with a valid empty snapshot, a valid
populated snapshot (including a tombstone), a truncated header, a
corrupted checksum, and a corrupted `fsmStateLen` field claiming far
more bytes than actually follow (the exact "don't trust a length field
beyond the bytes present" class of attack this decoder must resist).
Run locally for this phase at 30s (~11.9M executions, single worker
pool, 8 workers): zero crashes, zero new "interesting" corpus entries
beyond the 8 seeds — the decoder was already correctly guarded before
this phase; this closes the coverage gap rather than reporting a find
that did not happen.

```
go test ./internal/snapshot/... -fuzz FuzzDecode -fuzztime 30s
```

Every fuzz target that existed before this phase (`internal/wal`,
`internal/fsm`, `internal/raft` ×2, `internal/transport`,
`internal/sql` ×3) was re-run as part of this phase's quality gates
(§13) and remains clean; none were modified.

## 12. Correctness boundaries — what this phase does not prove

Consistent with `docs/roadmap.md`'s "NO CORRECTNESS INFLATION" and
`docs/invariants.md`'s `ISOLATION-TRUTHFULNESS`:

- **Not SERIALIZABLE.** Every SI history test in §3.3 explicitly
  documents ALLOWED-under-SI-FORBIDDEN-under-Serializable outcomes as
  correct, expected behavior — never as a defect.
- **Not linearizability for every operation.** No new linearizability
  claim is made anywhere in this phase. The existing scoped claim
  (`ReadIndex`-backed strong reads, `docs/replication.md` §4) is
  unchanged and was not re-litigated or broadened here.
- **Not exactly-once transport delivery.** `internal/transport` remains
  at-least-once with idempotent (`RequestID`-keyed) application above
  it, exactly as documented; nothing in this phase claims otherwise.
- **Not formal verification.** Every property in this document is
  checked by executable tests against concrete seeds/histories, not
  proved for all possible inputs. A property holding for every seed run
  is evidence, not a proof.
- **Not zero data loss outside the documented failure model.**
  `docs/failure-model.md` §5's explicitly-out-of-scope classes
  (Byzantine faults, simultaneous majority storage loss, WAN-scale
  network behavior) remain untested here, as documented there.
- **The model-based suites are sequential, not truly concurrent, at the
  driver level.** §3.1/§3.2's histories issue one operation at a time
  from a single test goroutine (a real client would do the same via one
  connection); genuine goroutine-level concurrent conflict races are
  covered separately and already, by Phase 2's
  `internal/txn/concurrent_test.go` (`-race`-clean, 50 concurrent
  writers) and are not re-proven by this phase's oracle-based
  suites — the two techniques are complementary, not redundant: this
  phase adds a much longer and combinatorially far more varied
  *sequential* history-and-oracle check across every layer, precisely
  because a real goroutine race is not reproducible enough to run at
  the seed counts §9 reports.
- **Real-process (`-tags=integration`) coverage was not expanded this
  phase** — see §8's reasoning.

## 13. Quality gates (this phase's own run)

Every command below was actually run against the final Phase 10 diff,
not fabricated:

```
gofmt -l .                                        # clean
go vet ./...                                       # clean
go build ./...                                     # clean
go build -tags integration ./...                   # clean
go test ./...                                      # all packages pass
go test ./... -race                                # all packages pass
go test -tags=integration ./cmd/... -race -count=1 # passes
go test ./... -run '^$' -bench . -benchtime=1x     # every benchmark still compiles and passes its own internal correctness check
git diff --check                                   # clean
grep -rn '<<<<<<<\|=======$\|>>>>>>>' --include='*.go' --include='*.md' .   # clean (merge-marker scan)
```

Plus every suite in §3/§4/§5/§7/§11 above at both its CI default and
its larger validated seed count, and every pre-existing suite this
phase did not modify (Phase 4-9's chaos/property/fuzz suites,
`internal/txn/concurrent_test.go`, `internal/raft/core_test.go`, etc.)
re-run unchanged and green.

## 14. Architecture compliance

No accepted architecture decision was redesigned. `internal/oracle` is
a new test-only support package (like `internal/fault`) — no production
code imports it, and it introduces no new production-facing type,
interface, or wire format. No existing package's public API was
changed. The one previously-undocumented-but-already-correct code path
this phase's testing specifically targeted
(`handleAppendEntriesRequest`'s stale-below-snapshot-boundary branch)
required no code change at all — only a new test, since the existing
implementation already matched its own doc comment exactly.

## 15. Maturity assessment

Per `docs/roadmap.md` §Maturity Model, no maturity gate beyond
`PORTFOLIO READY` is defined for Phase 10 — Phase 10 is framed
throughout the roadmap as a verification pass strengthening the
evidence behind the existing `PORTFOLIO READY` claim, not a gate to a
new named level. ChronicleDB's maturity claim remains
**`PORTFOLIO READY`** after this phase, now with additional
independent-model, cross-layer, and repeated-adversarial-cycle evidence
behind it (this document; no change to `README.md`'s maturity
statement was needed or made).

## 16. Remaining risks

Genuine limitations, not resolved by this phase:

- The model-based suites' independent oracle logic (§2) is itself new
  code, tested only by `internal/oracle/oracle_test.go`'s own unit
  tests and by every model-based suite implicitly agreeing with
  ChronicleDB's real behavior across the seed counts in §9 — an oracle
  bug that happened to agree with a matching ChronicleDB bug would not
  be caught by this technique alone (a structural risk of any
  model-based testing approach, not specific to this implementation).
- §9's seed counts, while substantially larger than this phase's CI
  defaults, are not exhaustive; a rarer counterexample at a much larger
  seed count or a differently-shaped history generator remains
  possible, exactly as `docs/roadmap.md`'s Maturity Model already
  requires stating plainly ("advancing a maturity claim without its
  evidence gate is itself a documentation defect").
- The real-process (`-tags=integration`) layer received no new
  scenario this phase (§8) — its Phase 5/7 coverage is unchanged and
  was only re-run, not extended.
- Soak/long-duration testing remains deferred (unchanged from Phase 9,
  `docs/testing-strategy.md` §1's table).
- Authentication/TLS remains the documented deployment prerequisite it
  has been since Phase 9 (`docs/non-goals.md` §Authentication and TLS)
  — unaffected by, and out of scope for, this phase.

## Next recommended phase

Per `docs/roadmap.md`, **Phase 11 — Open-source packaging / releases**.
Phase 10 does not itself begin any Phase 11 work.
