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
Phase 4 now implements that row's full target design — real
`internal/raft.Core` instances wired through `internal/fault`'s
in-memory transport and logical clock — though still without
production transport/disk (see docs/raft.md §9 and the Phase 4/5
boundary in docs/roadmap.md). This document specifies the testing
architecture future implementation phases must build toward; it does
not itself assert an aggregate coverage or pass-count claim (see §2's
guiding principle).

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

## 4. Relationship to the scenario corpus

[`docs/scenario-corpus.md`](scenario-corpus.md) enumerates the specific
scenarios this testing strategy must eventually turn into executable
tests (simulator-based where the scenario involves Raft/replication;
component/property-based where it does not). Each scenario there names
the roadmap phase in which it becomes executable — this document
defines *how* those scenarios get tested; the corpus defines *what*
gets tested.
