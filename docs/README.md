# ChronicleDB Documentation

Status: current maturity is **PORTFOLIO READY** (Phases 0-10 complete;
Phase 11 — open-source packaging — is what added this page's grouped
map and the packaging/release documents below). See
[`roadmap.md`](roadmap.md) §Maturity Model for the evidence gate behind
that claim and [`../README.md`](../README.md) for the project-level
summary.

This directory is the authoritative technical specification for
ChronicleDB, kept intentionally in sync with the implementation — see
[`vision.md`](vision.md) for why that distinction (documentation vs.
implementation, claim vs. evidence) is enforced strictly throughout
this repository.

New here? Start with [`vision.md`](vision.md), then
[`quickstart.md`](quickstart.md) to actually run something, then
[`architecture.md`](architecture.md) for the system map and binding
terminology every other document depends on.

## Documentation map

### Getting started

- [`quickstart.md`](quickstart.md) — clone, build, test, run a node
  and a real three-node cluster, exercise the SQL surface. Every
  command shown was actually run to produce its output.
- [`configuration.md`](configuration.md) — every `chronicledb-node`
  flag.
- [`../examples/`](../examples/) — runnable Go example programs
  (`basic-transaction`, `sql-basics`).
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — development setup, test
  requirements, and how to propose a change.

### Architecture and vision

- [`vision.md`](vision.md) — what ChronicleDB is and is not, and the
  evidence-over-declaration principle that governs every claim in this
  repository.
- [`architecture.md`](architecture.md) — system boundaries, the
  four-histories model (logical/physical/materialized/snapshot),
  binding terminology (`TxnID`, `RequestID`, `StartSeq`, `CommitSeq`,
  appended/persisted/replicated/committed/applied/acknowledged), and
  package/dependency structure.
- [`non-goals.md`](non-goals.md) — deliberate scope boundaries, why
  each exists, and what would trigger revisiting them.
- [`roadmap.md`](roadmap.md) — phase sequence and the evidence-based
  maturity model every capability claim is gated on.
- [`adr/`](adr/) — numbered Architecture Decision Records recording the
  specific decisions behind everything below, including alternatives
  considered and rejected.

### Correctness foundations

- [`storage.md`](storage.md) — durable append-only segment file
  primitives (`internal/storage`).
- [`wal.md`](wal.md) — record framing, the durable log
  (`internal/wal`), and the binding corruption/truncation rules.
- [`recovery.md`](recovery.md) — the exact restart/recovery sequence
  and why a durable-but-uncommitted suffix is never blindly applied.
- [`invariants.md`](invariants.md) — the formal invariant catalog every
  implementation phase must be checked against.
- [`failure-model.md`](failure-model.md) — failure-by-failure analysis
  of what may be lost, what must survive, required client-visible
  behavior, and security/safety expectations.

### Transactions / MVCC

- [`mvcc.md`](mvcc.md) — Snapshot Isolation, the visibility rule,
  write-write conflicts, and why SI is not automatically Serializable.
- [`transactions.md`](transactions.md) — transaction lifecycle,
  deterministic commit/apply, `RequestID` idempotency, and uncertain
  commit outcomes.

### Distributed systems

- [`raft.md`](raft.md) — the Raft core as a deterministic component,
  persistent state, and the commit rule.
- [`replication.md`](replication.md) — the durability contract
  (standalone and replicated), read consistency (`ReadIndex`-style),
  and the network partition contract.
- [`snapshots.md`](snapshots.md) — snapshot contents, atomic
  creation/installation, and log compaction, kept distinct from MVCC
  garbage collection.

### SQL

- [`sql.md`](sql.md) — the constrained SQL frontend (Phase 8):
  supported grammar, data model, execution semantics, and explicit
  compatibility boundaries (no joins, no PostgreSQL wire protocol, no
  CLI).

### Testing

- [`testing-strategy.md`](testing-strategy.md) — test categories and
  the deterministic distributed simulator design.
- [`scenario-corpus.md`](scenario-corpus.md) — the concrete scenarios
  tests implement, mapped to roadmap phases.
- [`adversarial-testing.md`](adversarial-testing.md) — Phase 10's deep
  adversarial correctness verification: the independent reference
  model, model-based history-testing suites, exact seed
  counts/reproduction commands, and explicit correctness boundaries.

### Operations / observability

- [`observability.md`](observability.md) — metrics, node status/health
  API, and logging.
- [`benchmarks.md`](benchmarks.md) — Phase 9 benchmark methodology,
  exact commands, environment, and measured results.
- [`support-matrix.md`](support-matrix.md) — which platforms are
  actually developed/tested against vs. cross-compiled only.

### Packaging and releases (Phase 11)

- [`versioning.md`](versioning.md) — SemVer policy and what pre-1.0
  compatibility actually means here.
- [`releasing.md`](releasing.md) — the release checklist and exact
  reproducible-build commands.
- [`dependencies.md`](dependencies.md) — the zero-external-dependency
  policy and when adding one would be justified.
- [`../SECURITY.md`](../SECURITY.md) — vulnerability reporting and
  documented deployment assumptions (no auth/TLS).
- [`../CHANGELOG.md`](../CHANGELOG.md) — what changed, release by
  release.

## What this documentation does not claim

Phases 1-10 are implemented and tested: durable storage and the WAL
(`internal/storage`, `internal/wal`), MVCC and the transaction engine
(`internal/mvcc`, `internal/txn`), the deterministic state-machine
boundary and `RequestID` idempotency (`internal/fsm`), Raft and its
real transport/disk wiring (`internal/raft`, `internal/transport`,
`internal/node`), snapshots and log compaction (`internal/snapshot`),
chaos/fault-injection testing (`internal/fault`), a small constrained
SQL frontend as of Phase 8 (`internal/sql`), benchmarks/observability
as of Phase 9 (`internal/metrics`, `internal/benchutil`), and, as of
Phase 10, an independent reference-model package used only by tests
(`internal/oracle`) — see
[`storage.md`](storage.md), [`wal.md`](wal.md), [`mvcc.md`](mvcc.md),
[`transactions.md`](transactions.md), [`raft.md`](raft.md),
[`replication.md`](replication.md), [`recovery.md`](recovery.md),
[`snapshots.md`](snapshots.md), and [`sql.md`](sql.md) for the design
as actually built. No document in this directory claims PostgreSQL
wire-protocol compatibility, a broad SQL dialect, joins, or a SQL CLI
exist (see [`sql.md`](sql.md) §8's explicit compatibility boundaries),
and no document claims SERIALIZABLE isolation — only Snapshot Isolation
is implemented and proven, including through the SQL frontend (see
[`mvcc.md`](mvcc.md) §1.1, [`sql.md`](sql.md) §5.3). Phase 11
(packaging: license, contribution docs, release automation) does not
by itself claim any resolved correctness or security gap — in
particular, the authentication/TLS gap
([`non-goals.md`](non-goals.md) §Authentication and TLS) remains
unresolved, and no version has been tagged/released yet (see
[`releasing.md`](releasing.md)). See [`vision.md`](vision.md) and
[`roadmap.md`](roadmap.md) §Maturity Model for how capability claims
are gated on implementation evidence.
