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

Current maturity: **ARCHITECTURE FOUNDATION**

No storage-engine, transaction, MVCC, replication, Raft, or SQL
implementation exists yet. This repository currently contains only the
architecture specification and Architecture Decision Records that will
guide that implementation. See
[`docs/roadmap.md`](docs/roadmap.md) §Maturity Model for the
evidence-based gates that govern every future maturity claim, and
[`docs/architecture.md`](docs/architecture.md) for the system design
itself.

The next milestone is **Phase 1 — Single-node durable storage + WAL +
crash recovery** (see [`docs/roadmap.md`](docs/roadmap.md)).
Implementation has not begun.

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
- [`docs/adr/`](docs/adr/) — Architecture Decision Records.
