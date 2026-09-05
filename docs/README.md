# ChronicleDB Architecture Documentation

Status: **Architecture Foundation** (Phase 0 of
[`roadmap.md`](roadmap.md)). This is the authoritative technical
specification for ChronicleDB. No database implementation exists yet;
see [`vision.md`](vision.md) for why that distinction is enforced
strictly throughout this repository.

Start with [`vision.md`](vision.md) for intent and scope, then
[`architecture.md`](architecture.md) for the system map and binding
terminology every other document depends on.

## Reading order

1. [`vision.md`](vision.md) — what ChronicleDB is and is not, and the
   evidence-over-declaration principle that governs every claim in
   this repository.
2. [`architecture.md`](architecture.md) — system boundaries, the
   four-histories model (logical/physical/materialized/snapshot),
   binding terminology (`TxnID`, `RequestID`, `StartSeq`, `CommitSeq`,
   appended/persisted/replicated/committed/applied/acknowledged), and
   package/dependency structure.
3. [`storage.md`](storage.md) — durable append-only segment file
   primitives (`internal/storage`).
4. [`wal.md`](wal.md) — record framing, the durable log
   (`internal/wal`), and the binding corruption/truncation rules.
5. [`mvcc.md`](mvcc.md) — Snapshot Isolation, the visibility rule,
   write-write conflicts, and why SI is not automatically Serializable.
6. [`transactions.md`](transactions.md) — transaction lifecycle,
   deterministic commit/apply, `RequestID` idempotency, and uncertain
   commit outcomes.
7. [`raft.md`](raft.md) — the Raft core as a deterministic component,
   persistent state, and the commit rule.
8. [`replication.md`](replication.md) — the durability contract
   (standalone and replicated), read consistency (`ReadIndex`-style),
   and the network partition contract.
9. [`recovery.md`](recovery.md) — the exact restart/recovery sequence
   and why a durable-but-uncommitted suffix is never blindly applied.
10. [`snapshots.md`](snapshots.md) — snapshot contents, atomic
    creation/installation, and log compaction, kept distinct from MVCC
    garbage collection.
11. [`failure-model.md`](failure-model.md) — failure-by-failure
    analysis of what may be lost, what must survive, and required
    client-visible behavior; also covers security/safety expectations.
12. [`invariants.md`](invariants.md) — the formal invariant catalog
    every implementation phase must be checked against.
13. [`testing-strategy.md`](testing-strategy.md) — test categories and
    the deterministic distributed simulator design.
14. [`scenario-corpus.md`](scenario-corpus.md) — the concrete
    scenarios future tests must implement, mapped to roadmap phases.
15. [`non-goals.md`](non-goals.md) — deliberate scope boundaries, why
    each exists, and what would trigger revisiting them.
16. [`roadmap.md`](roadmap.md) — phase sequence, evidence-based
    maturity model, and (as future work, not current claims)
    observability and performance target categories.
17. [`adr/`](adr/) — numbered Architecture Decision Records recording
    the specific decisions behind the above, including alternatives
    considered and rejected.

## What this documentation does not claim

No document in this directory claims that any storage engine,
transaction system, MVCC implementation, Raft consensus module, SQL
layer, or CLI currently exists or has been tested. See
[`vision.md`](vision.md) and [`roadmap.md`](roadmap.md) §Maturity
Model for how capability claims are gated on implementation evidence.
