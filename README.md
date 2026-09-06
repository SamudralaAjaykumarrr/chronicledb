# ChronicleDB

ChronicleDB is a from-scratch, open-source distributed transactional
database project. It is being built to demonstrate real engineering in
durable storage, write-ahead logging and crash recovery, multi-version
concurrency control under Snapshot Isolation, transactions with
deterministic conflict detection, request idempotency, a deterministic
replicated state machine, Raft consensus, and deterministic
distributed-systems testing — not to be a CRUD app, a toy key-value
store, or a wrapper around an existing finished database or consensus
library. See [`docs/vision.md`](docs/vision.md) for the full statement
of intent and explicit non-goals.

Current maturity: **PORTFOLIO READY**

Phase 1 — durable append-only segment storage (`internal/storage`) and
a checksummed, replayable write-ahead log with crash recovery
(`internal/wal`) — is implemented and tested against the
`docs/scenario-corpus.md` §Local Durability scenarios (LD-1 through
LD-6), including real subprocess kill-based crash tests and direct
on-disk corruption injection. See [`docs/wal.md`](docs/wal.md) and
[`docs/storage.md`](docs/storage.md) for the implemented design.

Phase 2 — MVCC + transactions + Snapshot Isolation (`internal/mvcc`,
`internal/txn`) — is also implemented and tested against the
`docs/scenario-corpus.md` §Transactions scenarios (TX-1 through TX-8):
MVCC visibility, own-write shadowing, first-committer-wins write-write
conflicts, atomic multi-key commit, abort safety, tombstone visibility,
stable snapshots, and a demonstrated Snapshot-Isolation write-skew
example (ChronicleDB does **not** claim SERIALIZABLE isolation — see
[`docs/mvcc.md`](docs/mvcc.md) §1.1). Transaction commits are durably
integrated with Phase 1's WAL and proven to survive restart, including
a real subprocess kill-based crash test. See
[`docs/mvcc.md`](docs/mvcc.md) and
[`docs/transactions.md`](docs/transactions.md) for the implemented
design.

Phase 3 — the deterministic state-machine boundary (`internal/fsm`)
and `RequestID` idempotency — is also implemented and tested against
`docs/scenario-corpus.md` §Idempotency (ID-1 through ID-7; the
snapshot+compaction leg of ID-4 is proven as of Phase 6, see below).
`internal/fsm` is now the sole `Apply(index, command) -> outcome` boundary for
transaction commits, proven deterministic by feeding an identical
ordered command history into two independently constructed `FSM`
instances and asserting byte-identical outcomes. The `RequestID`
outcome table durably survives restart for both `COMMITTED` and
`ABORTED` (conflicted) outcomes, detects a mismatched-payload reuse of
the same `RequestID`, and is queryable via `Manager.GetRequestOutcome`
without resubmitting a mutation payload. See
[`docs/transactions.md`](docs/transactions.md) §6-7/§10 and
[`docs/recovery.md`](docs/recovery.md) §5-6 for the implemented
design.

Per [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model, `TRANSACTIONAL
ENGINE` is Phases 2-3 together; both are now complete and evidenced.

Phase 4 — a deterministic Raft consensus core (`internal/raft`) and a
deterministic distributed-systems test harness (`internal/fault`) — is
also implemented: real, unmodified `internal/raft.Core` instances
(follower/candidate/leader roles, term/vote safety, log replication and
divergent-suffix repair, the current-term commit rule) are proven
against election safety, vote safety across restart, quorum safety
under partition, stale-leader step-down, and determinism, all via
`internal/fault`'s in-memory transport/clock/storage — see
[`docs/raft.md`](docs/raft.md) §9 and
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) §Raft/Replication
for exactly what is (and is not) covered. Phase 4 deliberately stopped
short of real transport/disk — per [`docs/roadmap.md`](docs/roadmap.md),
that was Phase 5.

Phase 5 — real replicated storage, quorum commits, and leader failover
(`internal/node`, `internal/transport`) — is now also implemented: the
unchanged Phase 4 `internal/raft.Core` is wired to a real
`internal/wal`-backed `raft.Storage` adapter and a real TCP transport,
in a genuine three-node deployment. `internal/wal` gained the crash-safe
suffix-truncation capability Phase 4 identified as missing, so
divergent-suffix repair is now durable for real. The full client
mutation path (leader-only acceptance → Raft proposal → quorum
commit → deterministic `internal/fsm.Apply` → `RequestID` outcome →
response) is implemented and tested, including the central acceptance
scenario — a client's `RequestID` retry against a newly elected leader,
after the original leader crashes, resolves to the identical outcome
without double-applying anything — the full network-partition contract
(`docs/replication.md` §5), and ADR-0010's `ReadIndex` protocol for safe
strong reads. This is proven at two levels: real disk/real TCP within a
single test process (`internal/node/node_test.go`), and genuine
separate OS processes with real persistent data directories and a real
`SIGKILL` (`cmd/chronicledb-node`, wired into CI as
`go test -tags=integration ./cmd/chronicledb-node/...`). See
[`docs/raft.md`](docs/raft.md) §10 and
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) §Raft/Replication
for exactly which of RF-1 through RF-15 were re-proven against real
disk/network/processes versus remain correctly simulator-only by
deliberate scope decision (RF-2, RF-7, RF-8, RF-14, RF-15).

Per [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model,
`REPLICATED PROTOTYPE` is Phases 4-5 together; both are now complete
and evidenced.

Phase 6 — snapshots, snapshot-based restart recovery, and Raft log
compaction (`internal/snapshot`, extending `internal/node`/
`internal/wal`/`internal/raft`) — is now also implemented: a node
restarting restores its deterministic state (MVCC data, tombstones,
`RequestID` outcomes) from the newest validated snapshot instead of
always replaying its full durable log from index 1, falling back to an
older snapshot or empty state exactly as a corrupted or missing
snapshot requires; a node creates a fresh snapshot and compacts its own
log once durable growth crosses a configured threshold; and a follower
too far behind for ordinary log replication is caught up via a genuine
`MsgInstallSnapshotRequest`/`Response` round trip
(`internal/raft.Core`'s new snapshot-boundary-aware log and
`Core.Compact`) rather than being stuck. Tested against
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) §Snapshots (SN-1
through SN-6) at both the unit level (`internal/raft/snapshot_test.go`,
`internal/wal/snapshot_test.go`, `internal/snapshot/manager_test.go`)
and end-to-end against real disk/network (`internal/node/node_test.go`).

Phase 7 — network partitions, a crash laboratory, and combined,
randomized fault-injection chaos testing (extending
`internal/fault`, `internal/node`, and `cmd/chronicledb-node`, no new
mechanism) — is now also implemented. `internal/fault` gained
deterministic disk-fault injection
(`MemoryStorage.FailNextAppends`/`FailNextSetHardState`/`FailNextTruncate`),
a `Node.Failed`/`FailErr` graceful-stop path for a genuine injected
persistence failure (mirroring `internal/node.Node.fail` instead of
panicking), and `Cluster.Compact`/`Cluster.Seed` — and
`internal/fault/chaos_test.go` runs several seeded, reproducible
randomized property suites against a much richer combined action space
(elections, proposals, crashes/restarts, partitions/heals/isolation,
message drop/duplicate/delay, local compaction, asymmetric
(directional-only) partitions via `Transport.IsolateLink`, and injected
disk faults) — by default a fast CI-sized seed count per suite, or a
much larger count via the `CHRONICLEDB_CHAOS_SEEDS` environment
variable for local/nightly stress runs (tens of thousands of seeds
across the whole suite have been run clean locally during this phase's
own development). `internal/transport` gained directional-only
`BlockSend`/`BlockRecv` (an asymmetric-partition hook a purely symmetric
`Block`/`Unblock` cannot express), used by new real-disk/real-TCP chaos
tests in `internal/node/chaos_test.go` (repeated crash/restart cycles,
retry across multiple leadership epochs, retry after snapshot
compaction, conflict-outcome stability across a snapshot+restart,
asymmetric partition safety, repeated partition/heal across changing
leaders, transaction atomicity across a leader crash, and a follower
crash during snapshot catch-up) and genuine real-process
(`cmd/chronicledb-node/chaos_test.go`, `-tags=integration`) SIGKILL
scenarios: a follower killed mid-replication, repeated SIGKILL attempts
timed around a real snapshot install, a lost-response-then-retry
sequence that never waits for the original response, and a real
(not simulated) network partition/heal via a new minimal `/fault`
control-plane endpoint. This chaos work found and fixed two genuine
bugs — see [`docs/testing-strategy.md`](docs/testing-strategy.md) §7 for
the full account: (1) a Raft election-safety-adjacent **liveness** bug
in `internal/raft.Core` where a node stepping down from Leader/Candidate
without granting the triggering vote/response could be left with no
election timer armed at all, permanently, under an adversarial
asymmetric-partition schedule; and (2) a genuine data race on
`internal/node.Node`'s `FSM()` accessor during snapshot install
(fixed with `atomic.Pointer`), plus (3) a WAL bug where
`internal/node.WALStorage.InstallSnapshot`'s use of `wal.WAL.Truncate`
to jump this node's own next-log-index counter forward past a
snapshot-covered gap silently failed to do so when nothing physical
needed removing — fatally erroring the very next append after a live
(non-restarted) snapshot install. All three are fixed, each with its
own deterministic regression test, and proven clean afterward at high
seed counts. Per [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model,
`STRONG DISTRIBUTED V1` is Phases 6-7 together; both are now complete
and evidenced, which is this repository's current maturity claim above.
See [`docs/testing-strategy.md`](docs/testing-strategy.md) §6-7 and
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) for the precise,
honest accounting of what Phase 7 does and does not claim (untested
failure classes are documented explicitly, not implied covered).

Phase 8 — a small, constrained SQL frontend (`internal/sql`) over the
unchanged Phases 1-7 transactional engine — is now also implemented:
`CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, and explicit
`BEGIN`/`COMMIT`/`ROLLBACK`, with a single primary key per table, a
single equality-on-primary-key predicate, and three scalar types
(`INTEGER`/`TEXT`/`BOOLEAN`) — a hand-written lexer/parser producing an
explicit typed AST, a binder resolving against a table's real
committed schema, and execution flowing exclusively through
`internal/txn.Manager` (standalone) or `internal/node.Node`
(replicated), never touching `internal/mvcc`/`internal/storage`
directly (ADR-0013). Tested against
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) §SQL (SQ-1 through
SQ-9): every documented error case for each statement kind, parser/
decoder fuzzing, `RequestID` retry safety, explicit-transaction commit/
rollback/abort-on-error semantics, a living Snapshot-Isolation
write-skew demonstration (no accidental SERIALIZABLE claim), restart
survival, and, against a real three-node cluster over genuine TCP/disk:
replicated `INSERT`→`SELECT`, `RequestID` retry across a real leader
failover, and SQL state surviving a real snapshot install and follower
restart. Building that last real-cluster evidence found and fixed one
genuine Phase 5 liveness bug in `internal/node.Node.BeginReadIndex` (a
newly elected leader with no proposal yet in its own term could stall
`ReadIndex` indefinitely) — see
[ADR-0014](docs/adr/0014-election-no-op-for-readindex-liveness.md) and
[`docs/replication.md`](docs/replication.md) §4.3. See
[`docs/sql.md`](docs/sql.md) for the full grammar, data model, and
explicit compatibility boundaries (no joins, no predicates beyond
primary-key equality, no CLI, no wire-protocol compatibility).

Phase 9 — benchmarks, profiling, and observability
(`internal/metrics`, `internal/benchutil`, plus additions to
`internal/node`, `internal/txn`, and `cmd/chronicledb-node`) — is now
also implemented: real, measured Go benchmarks for every layer named in
this phase's brief (WAL append with/without the fsync durability
boundary, MVCC, the deterministic FSM boundary, the real standalone
transaction commit path, snapshot encode/decode, the Raft
proposal/replication path, SQL parsing/binding/execution, and
end-to-end single-node/three-node/mixed-workload/recovery-time
benchmarks in `internal/node`), documented with exact commands,
environment, and honest "what is not measured" boundaries in
[`docs/benchmarks.md`](docs/benchmarks.md). CPU/memory profiling found
one genuine hotspot — `internal/wal.readFrame` read every remaining
byte in a segment before decoding a single record, making log replay
O(n²) in the number of entries — fixed with a bounded,
semantics-preserving change (read the frame header first, then exactly
its declared length) proven byte-for-byte identical on every
correctness/corruption/fuzz test, measuring ~273x faster and ~2,180x
less memory at 10,000 entries; a second profiling pass correctly found
no further code-level optimization justified (fsync dominates commit-
path CPU time by design). A dedicated snapshot-latency experiment
measured, and honestly reported rather than hid, Phase 6's already-
documented synchronous-fsync-in-the-event-loop latency spike. New
counters (elections, leader changes, Raft messages, proposals by
outcome, `RequestID` duplicates, snapshots created/installed — every
one proven to move on a real event and race-safe under `-race`, never
a correctness dependency) and a `/metrics`
(Prometheus text)/`/health` HTTP endpoint pair are documented in full,
including exact restart-reset semantics and the deliberate omission of
a fabricated cluster-quorum signal, in
[`docs/observability.md`](docs/observability.md).

Phase 10 — a deep adversarial correctness verification pass — is now
also complete: a new, structurally-independent reference-model package
(`internal/oracle`, never imported by production code) predicts
committed key/value state and verifies `RequestID` terminal-outcome
stability without reusing any ChronicleDB implementation logic, backing
three new model-based history-testing suites against a real three-node
cluster, a real standalone SQL engine, and real MVCC transactions
(validated locally at 200-3,000 seeds per suite with `-race`), plus
targeted Raft/WAL/cross-layer adversarial scenarios closing specific
gaps Phase 7's own broad chaos suites did not aim at, and a new fuzz
target for the previously-unfuzzed snapshot decoder. This phase added
no new product functionality and found zero new ChronicleDB production
defects (Phases 1-9's own chaos/benchmark work already found and fixed
five genuine ones) — see
[`docs/adversarial-testing.md`](docs/adversarial-testing.md) for the
complete, itemized account, including the explicit correctness
boundaries this phase does not claim (no SERIALIZABLE claim, no
broadened linearizability claim, no formal-verification claim).

Per [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model,
`PORTFOLIO READY` requires Phases 8 **and** 9 together; both are
complete and evidenced, which is this repository's current maturity
claim above — Phase 10 strengthens the evidence behind that same claim
rather than advancing to a new named maturity level, since the roadmap
defines none beyond it for this phase. The Authentication/TLS gap
([`docs/non-goals.md`](docs/non-goals.md) §Authentication and TLS)
remains explicitly, prominently documented as a deployment prerequisite
rather than resolved — implementing it was out of Phase 9's own scope,
and the maturity gate's own wording permits either resolving it or
documenting it this way. See [`docs/roadmap.md`](docs/roadmap.md)
§Maturity Model for the evidence-based gates that govern every future
maturity claim, and [`docs/architecture.md`](docs/architecture.md) for
the system design itself.

## Documentation

Start at [`docs/README.md`](docs/README.md) for the full reading order.
Key documents:

- [`docs/vision.md`](docs/vision.md) — what ChronicleDB is and is not.
- [`docs/architecture.md`](docs/architecture.md) — system boundaries
  and binding terminology.
- [`docs/invariants.md`](docs/invariants.md) — the correctness
  invariant catalog.
- [`docs/roadmap.md`](docs/roadmap.md) — phase sequence and maturity
  model.
- [`docs/sql.md`](docs/sql.md) — the constrained SQL frontend (Phase 8):
  grammar, data model, execution semantics, compatibility boundaries.
- [`docs/benchmarks.md`](docs/benchmarks.md) — Phase 9 benchmark
  methodology, exact commands, environment, and measured results.
- [`docs/observability.md`](docs/observability.md) — Phase 9 metrics,
  node status/health API, and logging.
- [`docs/adversarial-testing.md`](docs/adversarial-testing.md) — Phase
  10 adversarial correctness verification: reference model, model-based
  history suites, exact seed counts/commands, and correctness
  boundaries.
- [`docs/adr/`](docs/adr/) — Architecture Decision Records.
