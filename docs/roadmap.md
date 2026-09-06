# Roadmap and Maturity Model

Status: this document defines the phase sequence and the evidence
gates that govern when a maturity claim is allowed. Phases 1-8 are
complete (each at its own documented scope — see that phase's own
section below for exactly what it does and does not claim); current
maturity is still `STRONG DISTRIBUTED V1` (§Maturity Model below) —
Phase 8 alone does not advance it, since `PORTFOLIO READY` requires
Phases 8 **and** 9 together, and Phase 9 (benchmarks/observability) has
not begun.

## Phase sequence

### Phase 0 — Architecture Foundation

Rigorous, internally consistent architecture documentation and ADRs
covering every system boundary listed in
[`docs/architecture.md`](architecture.md). No implementation. This is
the phase this repository is in as of this document.

### Phase 1 — Single-node durable storage + WAL + crash recovery — COMPLETE

`internal/storage`, `internal/wal`, and the recovery sequence in
[`docs/recovery.md`](recovery.md) (minus Raft-specific parts) are
implemented and tested against the scenarios in
[`docs/scenario-corpus.md`](scenario-corpus.md) §Local Durability
(LD-1 through LD-6), including a real subprocess kill-based crash test
(not merely a function-return simulation) and direct on-disk byte
manipulation to reproduce torn-tail and corruption scenarios. See
[`docs/storage.md`](storage.md) and [`docs/wal.md`](wal.md) for the
design as implemented.

### Phase 2 — MVCC + transactions + Snapshot Isolation — COMPLETE

`internal/mvcc` and `internal/txn` implement the visibility and
conflict rules in [`docs/mvcc.md`](mvcc.md) and
[`docs/transactions.md`](transactions.md), running on top of Phase 1's
durable log in standalone (Raft-less) mode. Tested and passing against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Transactions (TX-1
through TX-8), including: a property test checking MVCC visibility
against a reference model over randomized version chains; concurrent
(goroutine-level, race-detector-clean) non-conflicting and conflicting
writer tests; a real subprocess kill-based crash test proving a
committed transaction survives an ungraceful process kill; and restart-
recovery tests for committed single- and multi-key transactions,
aborted transactions, never-committed workspace state, tombstones, and
CommitSeq ordering, all via `internal/txn.Manager`'s real recovery path
(no in-memory shortcut). `internal/fsm` (the deterministic Apply
boundary) does not exist yet — Phase 2 plays that role directly inside
`internal/txn.Manager` for `CommitTxn` commands (docs/transactions.md
§9); factoring it out into `internal/fsm` is Phase 3 scope. Per
[`docs/roadmap.md`](roadmap.md) §Maturity Model, the `TRANSACTIONAL
ENGINE` maturity level formally requires Phase 3 (`RequestID`
idempotency) as well, which is explicitly out of scope for this phase
— see the maturity claim in [`README.md`](../README.md).

### Phase 3 — Deterministic state-machine boundary + `RequestID` idempotency — COMPLETE

`internal/fsm` is factored out as the deterministic `Apply` boundary
described in [`docs/architecture.md`](architecture.md) §5-6, and the
`RequestID` outcome table ([`docs/transactions.md`](transactions.md)
§6-7, §10) is implemented and durable — including detection of
mismatched-payload `RequestID` reuse and a `GetRequestOutcome` query,
both exercised by real disk-backed recovery, not just in-memory
shortcuts. Tested and passing against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Idempotency
(ID-1 through ID-7; the snapshot+compaction leg of ID-4 is proven as of
Phase 6), including deterministic-replay tests (two independently
constructed `internal/fsm.FSM` instances applying the identical
command history reach byte-identical outcomes) and restart-recovery
tests proving a conflicted `RequestID`'s `ABORTED` outcome — not just a
committed one — survives restart and is never re-evaluated on retry.

### Phase 4 — Raft state machine + deterministic transport/clock simulator — COMPLETE (at this phase's own scope)

`internal/raft` core (§1 of [`docs/raft.md`](raft.md); implementation
notes in §9 there) and `internal/fault`'s deterministic simulator
([`docs/testing-strategy.md`](testing-strategy.md) §3) are implemented
and proven against small in-simulator cluster scenarios — election
safety, vote safety (including across restart), log matching/divergent
suffix repair, the current-term commit rule, quorum safety under
partition, stale-leader step-down, and determinism/reproducibility —
per the Phase-4 subset of [`docs/scenario-corpus.md`](scenario-corpus.md)
§Raft/Replication (see that document's Phase 4 note for exactly what
"passing in the deterministic simulator" does and does not claim).
Production transport/disk are explicitly not wired in yet: `Storage` in
Phase 4 is `internal/fault.MemoryStorage`, not `internal/wal` (see
[`docs/raft.md`](raft.md) §9.4 for why — `internal/wal` does not yet
support the truncation Raft's log-matching repair needs). This phase
does **not** by itself advance the [`README.md`](../README.md) maturity
claim past `TRANSACTIONAL ENGINE` — per §Maturity Model below,
`REPLICATED PROTOTYPE` requires Phase 5 (a real multi-process
deployment) as well.

### Phase 5 — Real replicated storage + quorum commit + leader failover — COMPLETE (at this phase's own scope)

`internal/transport` (production TCP) and `internal/node` wire the
unchanged Phase 4 `internal/raft.Core` to a real `internal/wal`-backed
`raft.Storage` adapter (`internal/node.WALStorage`) and `internal/fsm`
in a real three-node deployment — both in-process against real
disk/sockets (`internal/node/node_test.go`) and via genuine separate OS
processes with real persistent data directories and a real `SIGKILL`
(`cmd/chronicledb-node`). `internal/wal` gained the suffix-truncation
capability Phase 4 identified as missing (`docs/wal.md` §10), making
divergent-suffix repair (`docs/raft.md` §3) durable for real, not just
in the deterministic simulator. The full client mutation path —
leader-only acceptance, `Propose` → Raft replication → quorum commit →
`internal/fsm.Apply` → `RequestID` outcome → response — is implemented
and proven, including the central Phase 5 acceptance scenario
(`RequestID` retry against a new leader after the original leader
crashes resolves to the identical, non-duplicated outcome) and the full
network-partition contract (`docs/replication.md` §5). `ReadIndex`
(`docs/replication.md` §4, ADR-0010) is also implemented
(`internal/node.Node.BeginReadIndex`) and proven safe under partition.
Tested against [`docs/scenario-corpus.md`](scenario-corpus.md)
§Raft/Replication (RF-1 through RF-15) — see that document's Phase 5
note for the precise, honest accounting of which of RF-1 through RF-15
were re-proven against real disk/network/processes versus remain
correctly simulator-only (RF-2, RF-7, RF-8, RF-14, RF-15 — deliberate
scope, not a gap; see [`docs/raft.md`](raft.md) §10.2). This phase does
**not** implement snapshots, log compaction, follower catch-up via
snapshot, dynamic membership, or a general client wire protocol
(`internal/protocol`) — those remain out of scope per the phase
boundary below and, where applicable, Phase 6.

### Phase 6 — Snapshots + restart recovery + log compaction — COMPLETE (at this phase's own scope)

`internal/snapshot` is implemented per
[`docs/snapshots.md`](snapshots.md): checksum/version-framed encoding
of `internal/fsm` state, crash-safe temp-file/fsync/atomic-rename
creation and load-with-fallback-on-corruption. `internal/node` wires
the driver-side lifecycle — restart restore (§Open, extending
[`docs/recovery.md`](recovery.md) §7 to start `commitIndex`/
`appliedIndex` at a restored snapshot's boundary instead of always 0),
live creation/compaction once durable log growth crosses
`Config.SnapshotThreshold`, and the `MsgInstallSnapshotRequest`/
`MsgInstallSnapshotResponse` wire protocol `internal/raft.Core` now
implements (a new `Core.Compact` for a node's own local compaction,
distinct from installing a peer's snapshot). `internal/wal` gained the
durable snapshot pointer and physical segment compaction
(`AppendMetadataSnapshot`/`CompactBefore`/`FirstIndex`, see
[`docs/wal.md`](wal.md) §11). Full recovery including snapshot-based
follower catch-up and log truncation is proven against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Snapshots (SN-1
through SN-6) — see `internal/raft/snapshot_test.go`,
`internal/wal/snapshot_test.go`, and
`internal/node/node_test.go::TestSN1_RestartRestoresFromSnapshotAndCompactsLog`/
`TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction`. Per
§Maturity Model below, `STRONG DISTRIBUTED V1` additionally required
Phase 7 (chaos/combined fault schedules), now also complete — see that
phase's own section below.

### Phase 7 — Network partitions + crash lab + fault injection / chaos — COMPLETE (at this phase's own scope)

Combined, randomized fault schedules (partitions, crashes, disk
faults, message-level faults, all together, at scale, over many
iterations) are run via `internal/fault`, per
[`docs/testing-strategy.md`](testing-strategy.md) §3.3 "chaos tests."
This phase is about *breadth and adversarial combination* of scenarios
already individually proven in Phases 1-6, not new mechanism — and, per
that guiding principle, no new Raft/replication/snapshot mechanism was
added; the additions are: deterministic disk-fault injection and a
graceful (non-panicking) failure path in `internal/fault`, a
directional-only partition hook (`Transport.BlockSend`/`BlockRecv`) in
`internal/transport` needed to express an asymmetric partition against
real sockets (a purely symmetric `Block`/`Unblock` cannot), and a
minimal `/fault` control-plane endpoint in `cmd/chronicledb-node` to
drive that same hook against genuine separate OS processes. Seeded,
reproducible randomized property suites
(`internal/fault/chaos_test.go`) combine elections, proposals,
crashes/restarts, partitions/isolation/heals, message drop/duplicate/
delay, local log compaction, asymmetric partitions, and injected disk
faults, checked after every action against RAFT-ELECTION-SAFETY,
RAFT-LOG-MATCHING, and a simplified reference-model oracle for
COMMITTED-PREFIX-SAFETY; `internal/node/chaos_test.go` and
`cmd/chronicledb-node/chaos_test.go` (the latter `-tags=integration`,
genuine SIGKILL) prove the same combined-fault shapes end to end
against real disk/network/processes. This chaos work found and fixed
two genuine bugs (a Raft liveness bug where a node stepping down from
Leader/Candidate without granting the triggering vote/response could be
left with no election timer ever armed again, under an adversarial
asymmetric partition; and a WAL bug where installing a snapshot that
advances a node past a gap it never physically held any entries for did
not durably-equivalent-ly advance the WAL's own next-log-index counter,
fatally erroring the next live append) and one genuine data race (a
plain-pointer `internal/node.Node.fsmachine` field read/written from
different goroutines during snapshot install, fixed with
`atomic.Pointer`) — see
[`docs/testing-strategy.md`](testing-strategy.md) §7 for the full,
honest account of each, its regression test, and what remains
untested. See [`docs/scenario-corpus.md`](scenario-corpus.md)'s Phase 7
note for the precise chaos-variant accounting of RF-11/RF-13/RF-15 and
the SN-\* scenarios.

### Phase 8 — Constrained SQL using real transaction machinery — COMPLETE (at this phase's own scope)

The SQL surface defined in [`docs/non-goals.md`](non-goals.md) §SQL
surface (`CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`,
`BEGIN`/`COMMIT`/`ROLLBACK`, one primary key per table, a single
equality-on-primary-key predicate, three scalar types) is implemented
in `internal/sql` — a hand-written lexer/parser producing an explicit
typed AST, a binder that resolves and type-checks against a table's
real committed schema, and execution that flows exclusively through
`internal/txn.Manager` (standalone mode) or `internal/node.Node`
(replicated mode) via a small `Engine`/`Txn` seam
(`internal/sql/engine.go`) — never touching `internal/mvcc` or
`internal/storage` directly, per
[ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md). See
[`docs/sql.md`](sql.md) for the full grammar, data model, execution
semantics, and explicit compatibility boundaries.

Tested against [`docs/scenario-corpus.md`](scenario-corpus.md) §SQL
(SQ-1 through SQ-9), including: parser/lexer malformed-input and fuzz
coverage (`FuzzParse`, `FuzzDecodeSchema`, `FuzzDecodeRow`); every
documented error case for each of the five DML/DDL statements against
a real `internal/wal`-backed standalone engine; `RequestID` retry
safety for auto-commit mutations; explicit-transaction commit/rollback/
abort-on-statement-error semantics; a living Snapshot-Isolation
write-skew demonstration (no accidental SERIALIZABLE claim,
`docs/invariants.md` ISOLATION TRUTHFULNESS); schema-and-row survival
across a real WAL close/reopen restart; and, against a real three-node
`internal/node` cluster over genuine TCP/disk: SQL `INSERT` → Raft
commit → replicated state → `SELECT` returning the identical row on
every node, `RequestID` retry against a newly elected leader after the
original leader crashes, and SQL-visible state surviving a real
snapshot-create-and-install/compaction cycle plus a follower
crash/restart.

Building those real-cluster tests found and fixed one genuine,
previously-undiscovered Phase 5 liveness bug — not new SQL-layer
mechanism, but an existing mechanism (`internal/node.Node.BeginReadIndex`)
this phase's testing happened to newly exercise (every SQL statement
calls it, including the first one after a failover) — see
[ADR-0014](adr/0014-election-no-op-for-readindex-liveness.md) and
[`docs/replication.md`](replication.md) §4.3 for the fix
(`internal/node.Node.proposeElectionNoOp`) and its regression test.

Not built in this phase, per [`docs/sql.md`](sql.md) §8 and
[`docs/non-goals.md`](non-goals.md): joins, subqueries, views,
triggers, foreign keys, any predicate beyond primary-key equality,
aggregation, `ORDER BY`/`LIMIT`, `NULL`/defaults, secondary indexes, a
cost-based optimizer, PostgreSQL wire-protocol compatibility, or a SQL
CLI — the roadmap did not place a CLI requirement in this phase, so
`internal/sql` remains a library, consumed directly by tests, not a
client-facing tool.

### Phase 9 — Benchmarks + observability + performance engineering

Diagnostic state (§Observability below) and benchmark targets
(§Performance Targets below) are implemented and measured for real.
Performance work in this phase must never weaken a correctness
invariant from [`docs/invariants.md`](invariants.md) — any
optimization that would requires its own ADR justifying the trade-off
explicitly, and none is currently anticipated as necessary.

### Phase 10 — Deep adversarial correctness pass

A dedicated pass specifically hunting for violations of the invariant
catalog, using the accumulated testing infrastructure from Phases
1-9 at maximum adversarial intensity (long-running chaos runs,
targeted fuzzing of every decoder, deliberate attempts to construct a
counterexample to each invariant).

### Phase 11 — Open-source / portfolio packaging + releases

Packaging, documentation polish for external readers, versioned
releases, and (if pursued) the deployment infrastructure explicitly
deferred in [`docs/non-goals.md`](non-goals.md) (Kubernetes operator,
etc.) — pursued only after correctness phases are complete.

### Phase 12 — External review / "Break ChronicleDB" challenge

An explicit external-review phase: inviting outside
database/storage/distributed-systems engineers to attempt to find
correctness violations, with a bug-bounty-style "break it" framing.
Maturity claims about external validation (see §Maturity Model) are
only permitted based on the actual outcome of this phase, not before.

**SQL and deployment work is not moved ahead of the correctness
foundations (Phases 1-7) without a new ADR providing a strong,
specific architectural reason** — this is a standing constraint on
this roadmap, not just a default ordering preference.

## Maturity Model

Maturity is determined by **evidence**, never by the mere existence of
files, commands, or infrastructure. The examples below are binding
interpretation guidance, not exhaustive:

- A WAL file existing does not prove durability — a passing
  crash-injection test against the documented durability contract
  does.
- `BEGIN`/`COMMIT` parsing does not prove transactions — passing tests
  against the MVCC visibility and conflict rules do.
- Leader election alone does not prove Raft safety — passing
  simulator tests against the full invariant catalog's Raft invariants
  do.
- Three processes running simultaneously does not prove correct
  replication — passing partition/crash/failover scenario tests do.
- A benchmark command existing does not prove performance — an actual
  measured, reported number does, and even then it proves only what
  was measured, not a general claim.
- Documentation claiming correctness does not prove correctness —
  only tests exercising the claimed property do.

| Maturity level | Evidence gate |
|---|---|
| `INITIALIZED` | Repository scaffolding exists; no architecture, no implementation. |
| `ARCHITECTURE FOUNDATION` | All documents/ADRs in [`docs/README.md`](README.md) exist, are internally consistent (see the cross-document consistency review performed for this phase), define every system boundary listed in [`docs/architecture.md`](architecture.md), and no database implementation has begun. |
| `SINGLE-NODE DURABLE ENGINE` | Phase 1 complete: `docs/scenario-corpus.md` §Local Durability scenarios pass, reproducibly, in CI. `internal/storage`/`internal/wal` implement and test LD-1 through LD-6 (see test files under those packages); a GitHub Actions workflow (`.github/workflows/ci.yml`) runs `go test -race ./...` on every push. |
| `TRANSACTIONAL ENGINE` | Phases 2-3 complete: §Transactions and §Idempotency (immediate/after-restart) scenarios pass. |
| `REPLICATED PROTOTYPE` | Phases 4-5 complete: §Raft/Replication scenarios pass in the deterministic simulator and in a real multi-process three-node deployment. See [`docs/scenario-corpus.md`](scenario-corpus.md)'s Phase 5 note for the specific per-scenario accounting: RF-1, RF-3 (log-catch-up leg), RF-4, RF-5, RF-6, RF-9 through RF-13 are proven against real disk/network/processes; RF-2, RF-7, RF-8, RF-14, and RF-15 remain proven only in the deterministic simulator, by deliberate, documented scope decisions ([`docs/raft.md`](raft.md) §10.2), not gaps in the wiring this gate is actually about. |
| `STRONG DISTRIBUTED V1` | Phases 6-7 complete: §Snapshots scenarios pass; chaos/combined fault schedules run for a meaningful duration without an invariant violation. **This repository's current state** — see [`docs/scenario-corpus.md`](scenario-corpus.md)'s Phase 7 note and [`docs/testing-strategy.md`](testing-strategy.md) §6-7 for the specific evidence: seeded randomized chaos suites at `internal/fault` (raft-core layer, tens of thousands of seeds run clean locally during this phase), real-disk/real-TCP chaos at `internal/node`, and genuine real-process SIGKILL chaos at `cmd/chronicledb-node`, plus the two genuine bugs and one data race this work found and fixed, each with a deterministic regression test. |
| `PORTFOLIO READY` | Phase 8-9 substantially complete: constrained SQL works end-to-end on the real engine; observability surfaces exist; auth/TLS gap from [`docs/non-goals.md`](non-goals.md) is resolved or explicitly, prominently documented as a deployment prerequisite. |
| `OPEN-SOURCE READY` | Phase 11 packaging complete on top of `PORTFOLIO READY`: license, contribution docs, versioned release, no known unresolved correctness gaps. |
| `EXTERNAL-REVIEW READY` | `OPEN-SOURCE READY` plus a specific, documented invitation/process for Phase 12 review is in place. |
| `STAFF/PRINCIPAL DISCUSSION READY` | Phase 12 has actually occurred and its actual findings (not a prediction of findings) are documented and, where applicable, resolved. |

Advancing a maturity claim without its evidence gate is itself a
documentation defect and must be corrected on discovery — see
[`docs/vision.md`](vision.md) §Guiding principle.

## Observability (future work, specified now)

Not built in Phase 0-8. Diagnostic state ChronicleDB should expose,
once built (Phase 9), for operational visibility — never as a
correctness dependency (a correct decision must never depend on
whether a metric was successfully recorded):

- Node ID, role (follower/candidate/leader), current term, known
  leader.
- `commitIndex`, `appliedIndex`, durable log range (first/last index
  on disk), WAL size, most recent snapshot index.
- Active transaction count, transaction conflict count/rate.
- `RequestID` retry count, duplicate-request count.
- Replication lag per follower (`matchIndex` vs. leader's log length).
- Election count/rate, leader-change history.
- Commit latency (observed, not targeted — see §Performance Targets).

## Performance Targets (future work; no numbers invented now)

Not measured in Phase 0-8. ChronicleDB does not invent benchmark
numbers, latency figures, throughput figures, or scale claims in this
architecture phase. The following are the categories Phase 9 will
measure and report actual numbers for, once real:

- Standalone writes/sec.
- Replicated writes/sec (quorum-committed).
- Read latency (leader-local strong read, per
  [`docs/replication.md`](replication.md) §4).
- Write/transaction-commit latency, standalone and replicated.
- WAL throughput and WAL sync latency.
- Recovery time (as a function of log length since last snapshot).
- Snapshot creation time and snapshot installation time.
- Leader failover time (election-to-service-restored).
- Scaling behavior with record count and with concurrent transaction
  count.
- CPU and allocation profiles for the hot commit path.

Any performance optimization proposed in or after Phase 9 must be
checked against [`docs/invariants.md`](invariants.md) before adoption;
an optimization that weakens a documented invariant requires a new ADR
explicitly reconsidering that invariant, not a silent trade-off.
