# Roadmap and Maturity Model

Status: Architecture Foundation. This document defines the phase
sequence and the evidence gates that govern when a maturity claim is
allowed. **No phase past Phase 0 has begun.**

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

### Phase 2 — MVCC + transactions + Snapshot Isolation

`internal/mvcc` and `internal/txn` implement the visibility and
conflict rules in [`docs/mvcc.md`](mvcc.md) and
[`docs/transactions.md`](transactions.md), running on top of Phase 1's
durable log in standalone (Raft-less) mode. Tested against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Transactions.

### Phase 3 — Deterministic state-machine boundary + `RequestID` idempotency

`internal/fsm` is factored out as the deterministic `Apply` boundary
described in [`docs/architecture.md`](architecture.md) §5-6, and the
`RequestID` outcome table ([`docs/transactions.md`](transactions.md)
§6) is implemented and durable. Tested against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Idempotency
(immediate and after-restart cases).

### Phase 4 — Raft state machine + deterministic transport/clock simulator

`internal/raft` core (§1 of [`docs/raft.md`](raft.md)) and
`internal/fault`'s deterministic simulator
([`docs/testing-strategy.md`](testing-strategy.md) §3) are implemented
and proven against single-node-equivalent and small in-simulator
cluster scenarios, without yet wiring in production transport/disk.

### Phase 5 — Real replicated storage + quorum commit + leader failover

`internal/transport` (production) and `internal/node` wire the Phase
4 Raft core to Phase 1-3's durable log and state machine in a real
three-node deployment. Tested against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Raft/Replication
(RF-1 through RF-15).

### Phase 6 — Snapshots + restart recovery + log compaction

`internal/snapshot` is implemented per
[`docs/snapshots.md`](snapshots.md); full recovery
([`docs/recovery.md`](recovery.md)) including snapshot-based follower
catch-up and log truncation is proven. Tested against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Snapshots.

### Phase 7 — Network partitions + crash lab + fault injection / chaos

Combined, randomized fault schedules (partitions, crashes, disk
faults, message-level faults, all together, at scale, over many
iterations) are run via `internal/fault`, per
[`docs/testing-strategy.md`](testing-strategy.md) §3.3 "chaos tests."
This phase is about *breadth and adversarial combination* of scenarios
already individually proven in Phases 1-6, not new mechanism.

### Phase 8 — Constrained SQL using real transaction machinery

The SQL surface defined in [`docs/non-goals.md`](non-goals.md) §SQL
surface is implemented strictly on top of the Phase 1-7 engine, per
[ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md).

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
| `SINGLE-NODE DURABLE ENGINE` | Phase 1 complete: `docs/scenario-corpus.md` §Local Durability scenarios pass, reproducibly, in CI. **This repository's current state.** `internal/storage`/`internal/wal` implement and test LD-1 through LD-6 (see test files under those packages); a GitHub Actions workflow (`.github/workflows/ci.yml`) runs `go test -race ./...` on every push, though this workflow has not yet executed on GitHub's servers since this repository has not yet been pushed there. |
| `TRANSACTIONAL ENGINE` | Phases 2-3 complete: §Transactions and §Idempotency (immediate/after-restart) scenarios pass. |
| `REPLICATED PROTOTYPE` | Phases 4-5 complete: §Raft/Replication scenarios pass in the deterministic simulator and in a real multi-process three-node deployment. |
| `STRONG DISTRIBUTED V1` | Phases 6-7 complete: §Snapshots scenarios pass; chaos/combined fault schedules run for a meaningful duration without an invariant violation. |
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
