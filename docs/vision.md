# ChronicleDB Vision

## What ChronicleDB is

ChronicleDB is a from-scratch, open-source distributed transactional
database built to demonstrate real engineering in the areas that make
databases hard: durable local storage, write-ahead logging and crash
recovery, multi-version concurrency control, transaction isolation and
conflict detection, a deterministic replicated state machine, Raft
consensus, and deterministic distributed-systems testing.

ChronicleDB is being built as:

- A serious open-source distributed transactional database project.
- A Staff/Principal-level distributed-systems technical discussion
  project — something whose design decisions can be defended in detail
  under adversarial review.
- A project a real database/storage/distributed-systems engineering
  team could plausibly inspect, question, and evaluate on its merits.

## What ChronicleDB is not

ChronicleDB is explicitly **not**:

- CRUD software, or a demonstration of a web framework.
- A toy key-value store whose interesting properties stop at "GET/SET
  works."
- A tutorial clone assembled by following a single blog series.
- A thin wrapper around an existing finished database engine.
- A thin wrapper around a finished, off-the-shelf consensus library
  (e.g. embedding `etcd/raft` or `hashicorp/raft` and calling the
  project "done"). ChronicleDB's architecture is designed to implement
  and expose real Raft mechanics as first-class, inspectable
  architecture — see [`docs/raft.md`](raft.md).
- A "fake" distributed system whose only proof of correctness is that
  three processes can be started at the same time. Starting three
  processes proves deployment, not correctness.
- A project that is "done" because it has a Dockerfile, a CI badge, a
  CLI, a README, or a test directory. None of those artifacts are
  evidence of the correctness properties a transactional database must
  hold.

See [`docs/non-goals.md`](non-goals.md) for the explicit, permanent
scope boundary, and [`docs/roadmap.md`](roadmap.md) for the maturity
model that governs when a claim of capability is allowed to be made.

## Guiding principle: evidence over declaration

No document in this repository is permitted to claim a capability
before implementation evidence for that capability exists. "ChronicleDB
supports MVCC" is not evidence. A passing, reproducible test that
exercises the exact visibility rule in [`docs/mvcc.md`](mvcc.md) is
evidence. Architecture documents describe what will be built and the
rules it must obey; they do not describe what has been built unless
that work has landed and is covered by tests referenced from
[`docs/testing-strategy.md`](testing-strategy.md) and
[`docs/scenario-corpus.md`](scenario-corpus.md).

## Initial system shape

ChronicleDB V1 begins as **one logical shard**, replicated across a
**static three-node Raft cluster**. Sharding, multi-shard transactions,
two-phase commit across shards, and dynamic cluster membership are
explicitly deferred (see [`docs/non-goals.md`](non-goals.md)) until a
correct, well-tested single-shard replicated database exists. A
distributed database that cannot get one shard right cannot be trusted
to get many shards right; ChronicleDB proves the small, correct core
first.

## Long-term technical surface

The architecture in this repository is written so that, over the
roadmap in [`docs/roadmap.md`](roadmap.md), ChronicleDB can eventually
demonstrate:

- Durable local storage with explicit record framing and checksums.
- A write-ahead log used for crash recovery and as the durability
  substrate for the replicated log.
- Multi-version concurrency control under Snapshot Isolation.
- Transactions with explicit begin/read/write/commit/abort semantics,
  deterministic write-write conflict detection, and atomic multi-key
  commit.
- Request idempotency and well-defined handling of uncertain commit
  outcomes.
- A deterministic replicated state machine driven by a real Raft
  implementation: leader election, log replication, quorum commit,
  leader failover, follower catch-up, and safe restart/rejoin.
- Snapshotting and log compaction that preserve every invariant proven
  for the unbounded log.
- Deterministic distributed-system simulation, fault injection, and
  chaos testing as first-class engineering artifacts, not afterthoughts.
- A small, constrained SQL layer, built strictly on top of the real
  transaction machinery, once that machinery is proven.

## Why this document exists

This document exists so that every other document, ADR, and future
implementation decision can be checked against a single statement of
intent. When a future contributor (including a future session of the
authors) is tempted to take a shortcut — hide correctness logic behind
a finished library, declare victory because a demo runs, or claim an
isolation level that hasn't been implemented — this document is the
reference for why that shortcut is out of scope for ChronicleDB.
