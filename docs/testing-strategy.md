# Testing Strategy

Status: Unit, component, property, and fuzz tests exist for Phase 1
(`internal/wal`, `internal/storage`), Phase 2 (`internal/mvcc`,
`internal/txn`), Phase 3 (`internal/fsm`, plus `internal/txn`'s
idempotency/recovery integration tests), and Phase 4 (`internal/raft`
unit/fuzz tests plus `internal/fault`'s deterministic simulator and its
own cluster-level invariant/property tests) — see
`docs/scenario-corpus.md` for exactly which scenarios pass. Phase 3
exercised deterministic replay equivalence (two independently
constructed `internal/fsm.FSM` instances fed the identical command
history) as a concrete instance of the "Deterministic distributed
simulation" row below, in miniature and without any networking/Raft;
Phase 4 implemented that row's full target design — real
`internal/raft.Core` instances wired through `internal/fault`'s
in-memory transport and logical clock — without production
transport/disk (see docs/raft.md §9). Phase 5 adds the "End-to-end
tests" row's real-transport/real-disk leg for real: `internal/node`'s
tests wire the identical `internal/raft.Core` against a real
`internal/wal`-backed `raft.Storage` and a real TCP `internal/transport`
(still within one test process, but genuine sockets/files, not the
simulator), and `cmd/chronicledb-node`'s tests go one step further —
genuine separate OS processes, real persistent data directories, and a
real `SIGKILL` — see §5 below. Phase 7 adds §6-7 below: seeded,
reproducible randomized chaos suites combining every fault class
individually proven in Phases 1-6 (`internal/fault/chaos_test.go` at
the deterministic raft-core layer, `internal/node/chaos_test.go`
against real disk/network, `cmd/chronicledb-node/chaos_test.go` against
genuine real OS processes with a real `SIGKILL`), which found and fixed
two genuine bugs and one data race — §7 gives the full, honest account.
Phase 8 adds §8 below: the same test categories applied to the
constrained SQL frontend (`internal/sql`), including its own real
three-node cluster distributed evidence — which found and fixed one
further genuine Phase 5 liveness bug, `internal/node.Node.BeginReadIndex`
stalling indefinitely immediately after a leader failover with no
intervening write (§8.1). Phase 9 adds §9 below: the **Benchmarks**
and **Soak tests** rows this document's own §1 table named back in
Phase 0 are now real (benchmarks; soak tests remain explicitly
deferred, unchanged), plus a new **Observability tests** category
proving metrics/status move on real events and are race-safe — see
`docs/benchmarks.md` and `docs/observability.md` for the full account,
including one genuine performance bug (WAL replay's O(n²) I/O pattern)
this phase's profiling found and fixed. This document specifies the
testing architecture future implementation phases must build toward;
it does not itself assert an aggregate coverage or pass-count claim
(see §2's guiding principle).

Testing targets **correctness properties** (the invariants in
[`docs/invariants.md`](invariants.md)), not coverage percentage.
Coverage percentage is not evidence of correctness and is never used
in this project as a maturity signal (see
[`docs/roadmap.md`](roadmap.md) §Maturity Model).

## 1. Test categories

| Category | Purpose | Example target |
|---|---|---|
| **Unit tests** | Single function/type behavior in isolation | `internal/mvcc` visibility rule for a hand-built version chain |
| **Component tests** | One package's behavior against its real dependencies (e.g. real `internal/storage` files on a temp dir) | `internal/wal` append/sync/replay round-trip |
| **Property tests** | Randomized inputs checked against a reference model/invariant, not a fixed expected output | MVCC visibility rule holds for randomly generated version chains and `StartSeq` values |
| **Invariant tests** | Directly assert one catalog invariant holds after a scripted sequence of operations | `CONFLICT-CORRECTNESS` after a scripted concurrent-write scenario |
| **Fuzz tests** | Adversarial/malformed byte input against decoders | WAL record decoder, snapshot decoder, future wire-protocol decoder — must never panic (see [`docs/failure-model.md`](failure-model.md) §6) |
| **Deterministic distributed simulation** | Whole-cluster behavior (multiple `internal/raft` cores + fake transport/clock/disk) driven by a single-threaded, seeded scheduler | Election safety, log matching, leader completeness across induced partitions/crashes |
| **Crash/restart tests** | Kill and restart a (simulated or real) process at a precise point, verify recovery | Every scenario in §1.3-1.9 of [`docs/failure-model.md`](failure-model.md) |
| **Partition tests** | Induce network partitions in the simulator, verify the network partition contract | [`docs/replication.md`](replication.md) §5 |
| **Fault injection** | Disk errors, message drops/delays/duplication/reordering, combined with the above | `internal/fault` harness (§3) |
| **End-to-end tests** | Full client-request-to-response path against a real (multi-process or in-process) cluster | Client commits a transaction, kills the leader mid-flight, retries, observes correct outcome |
| **Chaos tests** | Long-running randomized combinations of the above, seeded for reproducibility | Extended simulator runs with randomized fault schedules |
| **Benchmarks** | Measure, not assert; used to track performance trends over time, never to prove correctness | See [`docs/roadmap.md`](roadmap.md) §Performance Targets |
| **Soak tests** | Long-duration runs to catch slow leaks/drift, once the system is otherwise proven correct | Deferred to a later phase (§Roadmap Phase 9+) |

## 2. Guiding principle

A test suite that only proves "the happy path runs" is not sufficient
evidence of a transactional database's correctness. Every invariant in
[`docs/invariants.md`](invariants.md) lists explicit proof/test
obligations; the testing strategy exists to make those obligations
achievable, not to hit a coverage number. A future maturity claim
(see [`docs/roadmap.md`](roadmap.md)) requires pointing at the specific
tests that exercise the relevant invariants and scenarios, not an
aggregate pass count.

## 3. Deterministic distributed simulator (`internal/fault`)

The simulator is the primary vehicle for proving Raft- and
replication-level correctness without depending on real time, real
networking, or real disks — all of which make distributed bugs
non-reproducible.

### 3.1 What the simulator represents

- **Nodes**: each running a real, unmodified `internal/raft` core and
  a real, unmodified `internal/fsm`, wired to simulator-provided
  implementations of the transport/clock/persistence interfaces (see
  [ADR-0009](adr/0009-transport-clock-randomness-abstraction.md)).
- **Message queues**: an in-memory, explicitly ordered queue per
  node-pair; the simulator's scheduler decides delivery order, not the
  OS or a real network.
- **Logical time**: a single global logical clock the scheduler
  advances explicitly; election/heartbeat timers fire based on logical
  time, not `time.Sleep`. Raft tests do not depend on wall-clock
  sleeps for correctness assertions.
- **Election timer randomization**: seeded, reproducible — the same
  seed produces the same sequence of timeout choices, so a discovered
  bug's triggering run can be replayed exactly.
- **Durable state**: a simulated disk per node, backed by real
  `internal/wal`/`internal/storage` code writing to an in-memory or
  temp-file-backed store, so persistence logic is exercised for real,
  not mocked away.
- **Crashes and restarts**: the scheduler can stop a node (discarding
  its in-memory state, retaining its simulated durable state) and
  later restart it, driving it through the real recovery path
  ([`docs/recovery.md`](recovery.md)).
- **Partitions and healing**: the scheduler can prevent delivery
  between arbitrary subsets of nodes, and later restore it, at any
  logical-time point.
- **Delayed / dropped / duplicated delivery**: the scheduler can apply
  any of these to any message, deterministically, per the active fault
  schedule for a given run.
- **Scheduled message delivery**: the scheduler controls the exact
  order messages are delivered across all node pairs, enabling
  reproduction of specific interleavings (e.g. "deliver the election's
  votes in this exact order").
- **Disk failure injection**: the simulated disk can be configured to
  fail a specific write/fsync call (by call count, by logical time, or
  by targeting a specific record) to exercise
  [`docs/failure-model.md`](failure-model.md) §1.8-1.9 deterministically.

### 3.2 Reproducibility

Every simulator run is fully determined by: the initial cluster
configuration, the random seed (for timer randomization and any other
randomized choice), and the fault schedule (explicit or
randomly-generated-from-seed). A failing run can always be replayed
exactly by recording and reusing that triple. This reproducibility
requirement is what makes `STATE MACHINE SAFETY` and the Raft
invariants practically provable rather than just plausible.

### 3.3 Why not `time.Sleep`-based tests

Real-time-based tests are flaky (timing-dependent), slow (must wait
out real timeouts), and non-reproducible (a bug that depends on a
specific interleaving may not recur under the same wall-clock test
run). The simulator's logical clock and single-threaded, deterministic
scheduler eliminate all three problems for the Raft/replication test
surface. Real-time end-to-end tests (§1, "End-to-end tests") still have
a place for validating the production transport/clock adapters
themselves, but not for validating Raft safety logic.

### 3.4 Implementation status (Phase 4)

§3.1's design is implemented by `internal/fault`: `Transport` (message
queues + drop/duplicate/delay/partition/isolate/reorder controls),
`Node`'s per-timer tick countdowns (the logical clock), `MemoryStorage`
(the simulated disk — in-memory for Phase 4; a real `internal/wal`-
backed adapter is Phase 5, see docs/raft.md §9.4), and `Cluster` (the
scheduler tying them together). `internal/fault/transport_test.go`
component-tests the transport itself (ADR-0009's own proof obligation
for this simulator, independent of any Raft scenario it later runs);
`internal/fault/cluster_test.go` and `property_test.go` are where the
`docs/scenario-corpus.md` §Raft/Replication scenarios and the
determinism/reproducibility claim in §3.2 are actually exercised.

## 4. Real-process/real-network testing layer (Phase 5)

The deterministic simulator (§3) remains the primary vehicle for
proving Raft- and replication-level *safety* — it stays the layer new
adversarial schedules and property tests get added to first. Phase 5
adds a second, complementary layer whose job is specifically to prove
the *production wiring itself* works, per §3.3's original framing
("real-time end-to-end tests still have a place for validating the
production transport/clock adapters themselves, but not for validating
Raft safety logic"):

- **`internal/node`'s in-process/real-disk/real-network tests**
  (`internal/node/node_test.go`) wire real, unmodified
  `internal/raft.Core` instances to a real `internal/wal`-backed
  `raft.Storage` (temp-dir-backed segment files) and a real TCP
  `internal/transport` listening on localhost, all within one test
  process. `internal/transport.Transport.Block`/`Unblock` inject
  partition-shaped faults (drop, both directions, for a named peer) —
  the same shape `internal/fault.Transport.IsolateNode`/`HealNode`
  provide for the simulator, but over genuine sockets. A "crash" in
  these tests is `Node.Stop`, which is durability-equivalent to an
  ungraceful kill here: nothing beyond what was already fsynced
  survives either way (`docs/wal.md` §4), so the two are
  interchangeable for every scenario these tests check; only a
  genuinely separate process can prove the *literal* OS-level kill
  case, which the next layer does.
- **`cmd/chronicledb-node`'s real-process integration test**
  (`cmd/chronicledb-node/main_test.go`, gated behind the `integration`
  build tag so it does not slow down every `go test ./...`, but wired
  into CI per `.github/workflows/ci.yml`) spawns several genuine OS
  processes — each with its own persistent data directory and real
  listening sockets — drives them via a minimal local HTTP control
  plane, and sends a real `SIGKILL` to simulate a crash. This is the
  "real multi-process/real-disk proof" `docs/roadmap.md`'s
  `REPLICATED PROTOTYPE` gate and this phase's own brief require beyond
  the simulator and beyond `internal/node`'s in-process tests.

Both layers use bounded polling (a deadline loop checking a condition
every few milliseconds) to observe eventual outcomes — leader election,
convergence, catch-up — never an arbitrary fixed sleep, per §3.3's
reasoning about why real-time tests must still avoid guessing how long
something "probably" takes.

Not every `docs/scenario-corpus.md` §Raft/Replication scenario is
re-proven at this real layer: message-level protocol edge cases already
exhaustively proven against the identical, transport-independent
`internal/raft.Core` in Phase 4 (RF-7, RF-8), and delayed-but-not-
dropped delivery, which `internal/transport` does not yet model
(RF-2, RF-14), are deliberately left as simulator-only — see
`docs/raft.md` §10.2 and each scenario's own Status line in
`docs/scenario-corpus.md` for the specific, honest accounting.

## 5. Relationship to the scenario corpus

[`docs/scenario-corpus.md`](scenario-corpus.md) enumerates the specific
scenarios this testing strategy must eventually turn into executable
tests (simulator-based where the scenario involves Raft/replication;
component/property-based where it does not). Each scenario there names
the roadmap phase in which it becomes executable — this document
defines *how* those scenarios get tested; the corpus defines *what*
gets tested.

## 6. Phase 7: the chaos laboratory

Phase 7's brief (`docs/roadmap.md`) is explicit: breadth and
adversarial *combination* of scenarios already individually proven in
Phases 1-6, not new Raft/replication/snapshot mechanism. Three layers
implement this, each reusing exactly the production code the earlier
phases already proved correct in isolation:

### 6.1 `internal/fault/chaos_test.go` — deterministic raft-core chaos

The cheapest-per-iteration layer, run at the highest seed counts. It
extends `internal/fault` (the Phase 4 deterministic simulator) with
capabilities `docs/testing-strategy.md` §3.1 already documented as
in-scope for this package but Phase 4 never implemented:

- **Deterministic disk-fault injection**:
  `MemoryStorage.FailNextAppends`/`FailNextSetHardState`/`FailNextTruncate`
  configure the next *N* calls to fail with a specific injected error,
  modeling `docs/failure-model.md` §1.8 (disk write/fsync failure)
  reproducibly. A genuine injected failure now stops the affected
  simulated node gracefully (`Node.Failed`/`FailErr`, mirroring
  `internal/node.Node.fail`'s "do not silently mark unsuccessful
  proposals committed" policy) instead of panicking, which is what lets
  a chaos schedule combine disk faults with everything else without the
  whole run aborting on the first injected failure.
- **`Cluster.Compact`**: a thin wrapper around the already-existing
  `raft.Core.Compact` (Phase 6), letting a chaos schedule interleave a
  node's own local log compaction with every other fault kind. The
  simulator has no FSM of its own (see `internal/fault`'s package doc
  comment), so `Node.applyOutput` now also drives `Core.SetApplied` the
  instant an entry commits — the minimum wiring `Compact` needs
  (`docs/snapshots.md` §3: `uptoIndex <= appliedIndex`), not a
  reintroduction of FSM/snapshot-content semantics into this layer.
- **`Cluster.Seed`**: exposes the seed a `Cluster` was constructed with,
  for logging a failing chaos run's exact reproduction seed.
- **A `committedOracle` reference model**: the simplest possible
  model of COMMITTED-PREFIX-SAFETY — "index N was once observed
  committed as (term, data); it must always be observed that way
  again" — checked against every live node's cumulative
  `CommittedEntries()` after every scheduled action, independent of any
  single node's own (possibly later compacted or crashed-away) view.
  Deliberately much simpler than `raft.Core` itself, per this phase's
  brief's "do not create an oracle that simply copies the
  implementation."

Test suites in this file (each independently seeded, reproducible per
§3.2, with a fast default seed count for every push and a much larger
count available via the `CHRONICLEDB_CHAOS_SEEDS` environment
variable):

- `TestChaos_CombinedRandomizedSchedule` — the primary property test: a
  richer action space than `property_test.go`'s original smoke run
  (adds explicit message drop/duplicate/delay, single-node
  isolate/heal, and local compaction to elections/proposals/
  partitions/crashes/restarts), checked after every action against
  RAFT-ELECTION-SAFETY, RAFT-LOG-MATCHING (compared by logical
  `raft.Index`, not array position — see the comment at its call site
  for why that distinction specifically matters once compaction is in
  the mix), a `CommitIndex`-vs-own-log-length sanity bound, and the
  `committedOracle`.
- `TestChaos_QuorumSafetyRandomizedPartitionTiming` — randomized-timing
  minority partitions: a node is isolated at a randomized point for a
  randomized duration and must never commit a write proposed while
  isolated, while the majority side continues.
- `TestChaos_RepeatedPartitionHealAcrossLeaders` — several
  partition/heal cycles across changing leaders, checked with the
  oracle for no committed entry ever lost or altered by a later cycle.
- `TestChaos_AsymmetricPartitionSafety` — a directional-only partition
  (`Transport.IsolateLink`, already present since Phase 4) combined
  with randomized elections/proposals; the scenario that surfaced the
  election-timer liveness bug documented in §7.1 below.
- `TestChaos_SnapshotMessageChaos` — leader-side local compaction
  combined with drop/duplicate/delay chaos specifically targeting
  `MsgInstallSnapshotRequest`/`Response` traffic, using Crash+Restart
  (not `IsolateNode`) to model the catching-up node, because
  `IsolateNode` only ever *holds* traffic for later delivery rather
  than genuinely losing it (see the comment at that test's definition)
  — an isolated node would just replay the full backlog via ordinary
  `AppendEntries` once healed and never actually need the snapshot path
  this test exists to stress.
- `TestChaos_DiskFaultDuringPersistence` — a randomized node's next
  `Append` is configured to fail mid-run; asserts the affected node
  never falsely reports success, and the cluster (after a Crash+Restart
  standing in for a real process restart) keeps operating correctly
  afterward.

### 6.2 `internal/node/chaos_test.go` — real-disk/real-TCP chaos

Reuses `testCluster` (`node_test.go`'s real `internal/wal`-backed
storage and real TCP `internal/transport`, in-process) to prove the
same combined-fault shapes survive genuine fsync/socket/goroutine
scheduling, not just the simulator's synchronous model: repeated
crash/restart cycles on one flapping node; RequestID retry across more
than one leadership epoch; RequestID retry after the original entry has
been compacted away (only the restored snapshot's outcome table
resolves it); a conflict (`ABORTED`) outcome's stability across a
snapshot boundary and a full restart, not just a leader failover;
transaction atomicity (all-or-nothing visibility) across a leader crash
mid-proposal; a real, directional-only partition via `Transport`'s new
`BlockSend`/`BlockRecv` (§6.4); several real partition/heal cycles
across changing leaders, checked with the same oracle pattern as §6.1;
and a follower crashed and restarted immediately after healing from
isolation, before catch-up could possibly have completed, to stress
SN-3 (interrupted snapshot installation) against real disk.

### 6.3 `cmd/chronicledb-node/chaos_test.go` — real-process SIGKILL chaos

`-tags=integration`, genuine separate OS processes (extending
`main_test.go`'s Phase 5 infrastructure), a real `SIGKILL`: a follower
killed mid-replication and restarted to prove real catch-up; repeated
SIGKILL/restart attempts timed (best-effort — a real OS process is not
as controllable as the deterministic simulator) around a real snapshot
install, asserting no partial/corrupt state ever survives and the
follower eventually converges cleanly; a lost-response-then-retry
sequence that kills the leader without ever waiting for the original
HTTP response, then retries the identical `RequestID` against the new
leader and confirms a single, stable, non-duplicated outcome; and a
genuine (not simulated) network partition/heal between real processes
via a new minimal `/fault` control-plane endpoint (`main.go`'s
`handleFault`, exposing `internal/transport.Transport`'s
`Block`/`Unblock`/`BlockSend`/`BlockRecv`/`UnblockSend`/`UnblockRecv`
over the same local HTTP control plane `/propose`/`/status` already
use — the smallest expansion that lets a real-process test drive a real
partition, consistent with this phase's brief: "use controlled
transport fault hooks only if consistent with the repo architecture").

### 6.4 New fault-injection surface added this phase

- `internal/transport.Transport.BlockSend`/`UnblockSend`/`BlockRecv`/`UnblockRecv`
  — directional-only blocking, alongside the existing symmetric
  `Block`/`Unblock`. A purely symmetric block cannot express "A can
  send to B but B cannot send to A" (docs/roadmap.md Phase 7's
  asymmetric-partition topology) against a real socket; the
  deterministic simulator already had this via `Transport.IsolateLink`
  since Phase 4, so this specifically closes the real-transport gap.
- `cmd/chronicledb-node`'s `-snapshot-threshold` flag and `/fault`
  control-plane endpoint (§6.3).
- `internal/fault.MemoryStorage`'s `FailNext*` methods and
  `Node.Failed`/`FailErr` (§6.1).
- `internal/fault.Cluster.Compact`/`Seed` (§6.1).

None of these change any production correctness path — they are
test-only hooks (the `internal/fault` package is never imported by
production code; the `/fault` endpoint and `-snapshot-threshold` flag
are additions to a binary whose own doc comment already frames it as
existing specifically to support integration testing) or, in
`internal/transport`'s case, an addition alongside an already-existing,
identically-scoped test-only hook (`Block`/`Unblock` itself).

### 6.5 Reproducing a chaos run

Every chaos test prints (via its subtest name and/or an explicit
`seed %d: ...` failure message) the exact seed that produced a failure;
re-running `go test ./internal/fault/... -run 'TestChaos_X/seed=N'`
(substituting the actual test and seed) reproduces it exactly, per
§3.2's reproducibility triple (configuration + seed + explicit call
sequence — the combined chaos schedule's call sequence is itself
seed-derived, so the seed alone is sufficient here). A larger local or
nightly stress run:

```
CHRONICLEDB_CHAOS_SEEDS=5000 go test ./internal/fault/... -run TestChaos -timeout 300s
```

Tens of thousands of seeds per suite were run clean, repeatedly, during
this phase's own development (after the fixes in §7 below) — this
document does not claim that number is run on every push (see §6.6/CI).

### 6.6 CI vs. larger stress runs

`.github/workflows/ci.yml` runs the chaos suites (§6.1) and the
real-disk chaos suite (§6.2) at their fast default seed counts on every
push, alongside the existing `-race` and fuzz-smoke jobs, so Phase 7
coverage does not silently bit-rot; the real-process SIGKILL suite
(§6.3) runs alongside the existing `-tags=integration` job for the same
reason (fast and fully localhost-contained, like Phase 5's own
real-process test). The `CHRONICLEDB_CHAOS_SEEDS` larger stress
invocation (§6.5) is a documented manual/local command, not part of
ordinary CI, per this phase's brief's "a reasonable pattern... without
making normal CI unusably slow."

## 7. Bugs found and fixed by Phase 7 chaos testing

Per this phase's brief ("do not simply suppress or exclude bad seeds"),
every genuine defect this chaos work uncovered is documented here, with
its root cause, fix, and regression test — none were worked around or
hidden.

### 7.1 Raft election-timer liveness bug (correctness-adjacent, not a safety violation)

**Found by**: `internal/fault/chaos_test.go::TestChaos_AsymmetricPartitionSafety`,
seed 609, during this phase's own development (a directional-only
`IsolateLink` partition combined with randomized elections/proposals).

**Symptom**: after healing an asymmetric partition, the cluster failed
to reconverge on a single leader within the test's original bounded
poll window — not merely slowly, but never, across an extended
diagnostic run (300+ rounds, term climbing from 7 to 32).

**Root cause**: `internal/raft.Core.handleRequestVoteRequest`,
`handleRequestVoteResponse`, `handleAppendEntriesResponse`, and
`handleInstallSnapshotResponse` each correctly set `Output.SteppedDown`
when a Leader or Candidate observes a higher term and reverts to
Follower, but four of those code paths (every one where the triggering
message was itself a *rejection* — a denied vote request whose
candidate's log was not up to date, or any of the three response
handlers' higher-term-but-otherwise-irrelevant branch) did not also set
`Output.ResetElectionTimer`. A Leader has no election timer running at
all (only its heartbeat timer); a driver that only ever arms/rearms the
timer from `ResetElectionTimer` therefore left such a node with **no
election timer running, ever again**, once it stepped down this way.
If the only node still calling elections can never actually win (its
own log is behind, so every legitimate voter correctly denies it —
RAFT-ELECTION-SAFETY itself was never violated), and every other node
has silently lost its own ability to ever call a new election, the
cluster is permanently stuck with no leader — a genuine liveness bug,
distinct from (and not excused by) `docs/failure-model.md` §2.8's
accepted *transient* election-storm possibility.

**Fix**: `internal/raft/core.go` — each of the four identified
call sites now also sets `Output.ResetElectionTimer`/`ElectionTimeoutTicks`
whenever `stepDownTo` reports the node was previously Leader or
Candidate (`Output.SteppedDown`), mirroring the unconditional reset
`handleAppendEntriesRequest`/`handleInstallSnapshotRequest` already
performed on every path (an asymmetry between the "request" and
"response"/"rejection" handlers that was the actual gap).

**Regression tests**: `internal/raft/core_test.go` —
`TestRejectedVoteAfterStepDownStillResetsElectionTimer`,
`TestVoteResponseStepDownStillResetsElectionTimer`,
`TestAppendEntriesResponseStepDownStillResetsElectionTimer`,
`TestInstallSnapshotResponseStepDownStillResetsElectionTimer` — each
directly targets one of the four fixed code paths and was confirmed to
fail (deterministically, no seed needed) against the pre-fix code
before the fix landed. `TestChaos_AsymmetricPartitionSafety` (seed 609
specifically, plus tens of thousands of further seeds) passes cleanly
after the fix.

### 7.2 `internal/node.Node.FSM()` data race during snapshot install

**Found by**: `go test -race` on repeated runs of
`internal/node/chaos_test.go::TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly`
(a follower crashed and restarted immediately during snapshot
catch-up, polled concurrently via `FSM()`).

**Root cause**: `Node.fsmachine` was a plain `*fsm.FSM` field, written
by the event-loop goroutine's `handleInstallSnapshot` (a wholesale
pointer replacement on a successful install) and read by `Node.FSM()`
— documented as safe to call from any goroutine, and already exercised
that way throughout `node_test.go` — with no synchronization between
the two. `FSM.Store()`'s content is itself protected by `FSM`'s own
mutex, but the *pointer* to which `*fsm.FSM` a caller was even talking
to was not.

**Fix**: `internal/node/node.go` — `Node.fsmachine` is now an
`atomic.Pointer[fsm.FSM]`; every read/write site (`FSM()`, `Precheck`,
`Apply`, `snapMgr.Create`, and the install-time replacement) goes
through `Load`/`Store`.

**Regression test**: no new dedicated unit test was added — the fix is
verified by the pre-existing race-triggering condition
(`TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly`, plus the
whole `internal/node` suite) passing clean under `-race`, repeated,
after the fix; a targeted single-purpose race test would not add
coverage `-race` on the existing suite doesn't already provide.

### 7.3 `wal.WAL.Truncate` did not advance `nextLogIndex` past an installed-snapshot gap

**Found by**: `cmd/chronicledb-node/chaos_test.go::TestRealChaos_SIGKILLDuringSnapshotInstall`,
a real-process test proposing further entries after a real follower's
snapshot install completes (unlike `internal/node/node_test.go`'s
`TestSN5_...`, which stops checking at "caught up" and never proposes
anything further afterward — this is precisely why Phase 6's own tests
never surfaced this).

**Symptom**: the follower process fatally errored — "node: durable
persistence failed: raft: append 2 entries: node: WAL assigned index 1
for raft log index 11 (log responsibility mismatch)" — the moment it
needed to durably append its first entry after a live (not restarted)
snapshot install.

**Root cause**: `internal/node.WALStorage.InstallSnapshot` calls
`wal.WAL.Truncate(uptoIndex+1)` specifically to jump this WAL's own
`nextLogIndex` counter forward to match the newly-installed snapshot
boundary — the ordinary case for a follower whose log was simply
*behind* (not diverged), which by definition has nothing physically
present at or after `uptoIndex+1` to remove. `WAL.Truncate`'s no-op
guard (`if fromIndex >= w.nextLogIndex { return nil }`) exited without
ever updating `nextLogIndex`, silently violating the method's own
documented postcondition ("so a subsequent `AppendLogEntry` resumes
exactly at `fromIndex`") in exactly this case. `wal.Open`'s own restart
recovery already independently derives the correct value
(`meta.LatestSnapshotIndex+1`) from the durably-recorded snapshot
pointer when no `LogEntry` record survives — so the bug was specific to
a *live, not-yet-restarted* process continuing to operate after an
install, which is the common case for a follower that stays up long
enough to receive further real replication afterward.

**Fix**: `internal/wal/wal.go` — `Truncate`'s no-op branch now also
advances `w.nextLogIndex` to `fromIndex` when `fromIndex > w.nextLogIndex`,
bringing the live in-memory counter into agreement with what a restart
would already independently compute — no additional durable write is
required, since the durable snapshot pointer `AppendMetadataSnapshot`
already recorded (always called before `Truncate` within
`InstallSnapshot`) is what a future restart re-derives from either way.

**Regression test**: `internal/wal/snapshot_test.go::TestTruncateJumpsNextIndexForwardPastInstalledSnapshotGap`
— confirmed to fail against the pre-fix code before the fix landed.
`TestRealChaos_SIGKILLDuringSnapshotInstall` passes cleanly, repeatedly,
after the fix.

### 7.4 What Phase 7 chaos testing did *not* find

No violation of RAFT-ELECTION-SAFETY, RAFT-LOG-MATCHING, QUORUM-SAFETY,
LEADER-COMPLETENESS, ATOMICITY, IDEMPOTENCY, REQUEST-OUTCOME-STABILITY,
or SNAPSHOT-SAFETY was ever observed at any seed count run during this
phase's development, across all three chaos layers, after the three
fixes above. This is evidence *for* those invariants holding under the
combined-fault schedules actually exercised — not a claim that no
further counterexample exists at a schedule shape, duration, or seed
count not yet tried (see `docs/roadmap.md`'s Maturity Model: "advancing
a maturity claim without its evidence gate is itself a documentation
defect," and this phase's own "Remaining Risks" accounting in the
Phase 7 completion report).

## 8. Phase 8: SQL frontend testing

`internal/sql`'s test suite (see [`docs/sql.md`](sql.md) §9 and
[`docs/scenario-corpus.md`](scenario-corpus.md) §SQL, SQ-1 through
SQ-9, for the itemized scenario mapping) adds no new test *category*
beyond §1's table above — it applies unit, component, and (for the
distributed evidence) real-multi-process tests to the SQL frontend
specifically, per this phase's own brief that SQL testing "should focus
on correct translation to the transaction API... not on re-proving
durability/MVCC/Raft properties already covered by Phases 1-7's
scenario corpus" (ADR-0013). Concretely:

- Parser/lexer unit tests plus `FuzzParse`/`FuzzDecodeSchema`/
  `FuzzDecodeRow` (fuzz tests, per §1's table) — the identical
  never-panics/bounded-decode discipline `internal/fsm.FuzzDecodeCommitTxn`
  already established, applied to SQL source text and the two new
  durable record formats (`Schema`, row bytes).
- Component tests against a real `internal/wal`-backed standalone
  engine for every documented statement/error case, and a real restart
  of that same on-disk directory (mirroring `internal/txn`'s own
  restart-recovery test style, docs/testing-strategy.md §1's "Component
  tests" row).
- End-to-end tests against a real three-node `internal/node` cluster
  over genuine TCP/disk (`internal/sql/distributed_test.go`), built the
  same way `internal/node/node_test.go`'s own `testCluster` is (fast
  tick intervals, bounded polling, real `SIGKILL`-equivalent `Node.Stop`)
  — proving SQL `INSERT` → Raft commit → replicated state → `SELECT`,
  `RequestID` retry across a real leader failover, and SQL state
  surviving a real snapshot install and follower restart.

### 8.1 A genuine Phase 5 liveness bug found by Phase 8's real-cluster SQL tests

Building `internal/sql/distributed_test.go`'s failover-retry test
(SQ-8) found and fixed a real, previously-undiscovered bug in
`internal/node.Node.BeginReadIndex` — not a defect in `internal/sql`
itself, but in a Phase 5 mechanism this phase's testing happened to be
the first to exercise under the specific condition that exposes it.

**Found by**: `internal/sql/distributed_test.go::TestDistributedSQLLeaderFailoverRetry`,
during this phase's own development — a real three-node cluster, an
`INSERT` committed via the leader, an immediate real leader crash, and
a retry of the identical statement/`RequestID` against the newly
elected leader.

**Symptom**: the retry's own `Session.Begin` → `BeginReadIndex` call
hung indefinitely (bounded only by the test's own context timeout, not
by any internal bound) — reproduced directly against the raw
`internal/node.Node.BeginReadIndex` API with no SQL code involved at
all, confirming this was not a SQL-layer bug.

**Root cause**: Raft's current-term commit rule (`docs/raft.md` §4)
forbids a leader from advancing its own `commitIndex` past a
*previous* term's entries via `matchIndex` majority alone; it must
first commit an entry in its *own* current term. A newly elected
leader whose predecessor crashed before that fact ever reached the
other nodes (no follow-up heartbeat carrying the updated
`leaderCommit`) therefore cannot recognize an already-durably-
replicated prior-term entry as committed until *some* new proposal
lands in its own term — and nothing forces that to happen on its own.
`docs/raft.md` §9.5/§10.3 had already named this exact liveness window
as a known, deliberately deferred trade-off ("not a Phase 5 correctness
requirement... `§9.5`-anticipated future ADR territory"); Phase 8's SQL
testing is what turned it from a documented theoretical corner into an
observed, reproducible hang, because every SQL statement's
`Session.Begin` calls `BeginReadIndex` — including the very first
statement after a failover, with nothing else to unstick it.

**Fix**: `internal/node/node.go` — `Node.proposeElectionNoOp`, called
from `processOutput` on `Output.BecameLeader`, immediately proposes one
synthetic empty-`Mutations` `CommitTxn` command in the node's own new
term, unblocking the current-term commit rule's backward-scan
recognition of every earlier entry with no client action required. See
[ADR-0014](adr/0014-election-no-op-for-readindex-liveness.md) and
[`docs/replication.md`](replication.md) §4.3 for the full design and
why this belongs in `internal/node` (the driver) rather than
`internal/raft.Core`, whose own "no no-op on election" decision is
unchanged.

**Regression test**: `TestDistributedSQLLeaderFailoverRetry` itself —
confirmed to hang (verified by temporarily reverting the fix and
re-running) before the fix, and to pass in well under a second,
repeatedly, after it. Three pre-existing `internal/node` tests
(`TestSN1_RestartRestoresFromSnapshotAndCompactsLog`,
`TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction`,
`TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly`) had
hardcoded an exact `SnapshotIndex == numKeys` boundary that implicitly
assumed no entry ever precedes their own test proposals; the no-op's
extra leading entry can shift where a `SnapshotThreshold` boundary
lands, so those three were updated to wait for the boundary to cover
every key they actually check, computed dynamically, rather than
asserting one hardcoded absolute index — a test-robustness fix, not a
further production-code change, re-verified green under `-race` across
repeated runs of the full `internal/node` suite.

## 9. Phase 9: benchmarks and observability testing

Phase 9's brief is explicit that benchmarks "must still validate
enough behavior to ensure they are not benchmarking a no-op" and that
observability must be proven, not assumed. This section is the
`docs/scenario-corpus.md`-style honest accounting for that work; see
[`docs/benchmarks.md`](benchmarks.md) and
[`docs/observability.md`](observability.md) for the full methodology
and results this section summarizes the testing evidence for.

### 9.1 Benchmark correctness

Every microbenchmark and end-to-end benchmark added this phase
verifies real progress inside its timed loop where that check is cheap
relative to the operation being measured (never inside a loop
specifically isolating a cheap operation's own cost, e.g. `internal/mvcc`'s
no-I/O benchmarks) — a committed outcome's `Status`, a replayed
entry's count matching what was written, a `SELECT`'s row count
matching what was inserted, a snapshot's decoded key count, or (for the
Raft proposal/replication benchmark, `internal/fault`) the leader's own
`CommitIndex` actually advancing past its pre-proposal value. Setup
that would otherwise pollute a timed measurement (building a fixture
WAL/cluster/table, seeding rows before a scan/delete benchmark) always
runs before `b.ResetTimer()`, per this phase's brief's "use setup/
teardown correctly."

### 9.2 Observability tests

`internal/metrics/metrics_test.go`, `internal/txn/metrics_test.go`,
`internal/node/metrics_test.go`, and
`cmd/chronicledb-node/control_test.go` together prove, against real
clusters/managers (never a mock): role/term/commit-index/applied-index/
snapshot-boundary values are readable and correct (`Node.Status()`,
unchanged since Phase 5, re-verified here via the new `/metrics`
endpoint); elections and leader changes increment their counters on a
real 3-node election; proposals increment the correct one of
committed/aborted/rejected/duplicate counters for a real committed
write, a real conflict, a real not-leader rejection, and a real
`RequestID` retry, respectively (not vacuously for every call);
snapshot-created/installed counters move on a real create-then-install
cycle (mirroring `TestSN1`/`TestSN5`'s own scenario shape); and every
metrics/status read is race-safe under `-race` while concurrent real
proposal traffic is in flight. Restart-reset semantics (counters reset
to zero; `Status()` fields do not) are documented in
`docs/observability.md` §7, not separately asserted by a dedicated
restart test — the underlying fact (metrics are plain in-memory
`sync/atomic` fields with no recovery-path wiring at all) is
structurally guaranteed by the implementation's own absence of any
metrics-related code in `internal/node.Open`'s recovery sequence,
inspectable directly rather than needing a test to prove a negative.

### 9.3 Benchmark/observability CI smoke

Per this phase's brief ("do not run long performance benchmark suites
on every push... provide explicit local commands for full benchmark
runs"), `.github/workflows/ci.yml` runs a fast benchmark **compilation
and single-iteration smoke** step (`go test ./... -run '^$' -bench . -benchtime=1x`)
on every push — proving every benchmark still compiles and its first
iteration's own internal correctness checks (§9.1) still pass, without
paying for a full statistically-meaningful benchmark run in ordinary
CI. The observability tests in §9.2 run as part of the ordinary
`go test ./...`/`-race` jobs already in CI (they are ordinary Go tests,
not benchmarks) — no separate CI job was needed for them. A full,
representative benchmark run (the numbers actually published in
`docs/benchmarks.md`) is a documented manual/local command
(`docs/benchmarks.md` §5), matching how Phase 7's large chaos-seed runs
are handled (§6.5-6.6 above): fast/representative in CI, larger runs
available on demand, never silently expected to happen automatically.

### 9.4 What Phase 9 did not test

This phase did not add: a soak/long-duration test (still deferred, per
§1's table, to a later phase); a multi-machine network-latency
benchmark (every "real network" benchmark here is real TCP on
`127.0.0.1`, per `docs/benchmarks.md` §1); or a concurrent-multi-client
throughput benchmark (every benchmark added is single-client,
sequential). These are documented limitations, not silently assumed
coverage — see `docs/benchmarks.md` §1's complete "not measured" list.
