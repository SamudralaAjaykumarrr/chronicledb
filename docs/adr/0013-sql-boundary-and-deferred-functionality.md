# ADR-0013: SQL Boundary and Deferred Functionality

Status: Accepted

## Context

ChronicleDB's long-term vision includes a small SQL layer, but the
project's core purpose is to prove correctness of the underlying
transactional/replicated engine. SQL implemented ahead of, or
bypassing, that engine would either sit on unproven foundations or
tempt shortcuts that undermine the very guarantees the project exists
to demonstrate.

## Decision

- SQL is **deferred to Phase 8**, after Phases 1-7 (durable storage
  through chaos testing) are complete (see
  [`docs/roadmap.md`](../roadmap.md)).
- Initial SQL surface, when built, is intentionally small: `CREATE
  TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`,
  `ROLLBACK`, primary keys, equality predicates, a limited type
  system — **no joins initially**.
- SQL must compile strictly into the real transaction/MVCC machinery:

  ```
  SQL / request layer -> internal/txn -> internal/mvcc/internal/fsm
    -> internal/raft (replicated commit path) -> internal/wal/internal/storage
  ```

- SQL must never bypass durability, transactions, MVCC, or replication
  — there is no direct-to-storage fast path for SQL statements.

## Alternatives Considered

1. **Build SQL early, in parallel with the transactional engine, to
   have a usable interface sooner.** Rejected: makes it impossible to
   tell whether a bug is in SQL parsing/planning or in the underlying
   engine, and creates pressure to "just make it work" by bypassing
   engine layers under deadline pressure — directly conflicting with
   the requirement that SQL never bypass the real machinery.
2. **Broad SQL compatibility (joins, subqueries, complex expressions)
   in the initial SQL layer.** Rejected: joins in particular imply a
   query planner/optimizer, a substantially different and separately
   hard problem from the transactional/consensus core this project
   exists to demonstrate; see
   [`docs/non-goals.md`](../non-goals.md) §SQL surface.
3. **PostgreSQL wire-protocol compatibility, to leverage existing
   client tooling.** Rejected for the same reason embedding an
   existing consensus library was rejected
   ([ADR-0008](0008-raft-architecture-and-persistent-state.md)
   alternative 1): wire compatibility is orthogonal to, and would
   distract from, demonstrating the project's actual engineering
   focus, and risks becoming "a wrapper" in spirit even if the storage
   underneath is original.
4. **No SQL at all, ever (pure key-value/transactional API).**
   Rejected: a constrained SQL layer is valuable as a demonstration
   that the transaction machinery is general enough to support a
   real query interface, and is explicitly part of the project's
   long-term vision — just correctly sequenced after the engine is
   proven.

## Consequences

- Until Phase 8, ChronicleDB has no query language — only the
  transactional key/value-oriented API described in
  [`docs/transactions.md`](../transactions.md).
- When SQL is built, its correctness inherits directly from the
  already-proven engine underneath, rather than needing its own
  separate durability/atomicity/isolation proof — it only needs to
  prove correct *translation* into the existing transaction API.

## Correctness Implications

- Enforces the architectural dependency direction in
  [`docs/architecture.md`](../architecture.md) §5 ("SQL must not
  bypass transactions").
- Prevents SQL-layer bugs from being mistaken for, or masking,
  engine-layer correctness bugs, by keeping the two cleanly separated
  and sequenced.

## Testing and Proof Obligations

- When Phase 8 begins: SQL-level tests should focus on correct
  translation to the transaction API (e.g. `UPDATE ... WHERE
  pk = ?` compiles to a `BEGIN`/read/write/`COMMIT` sequence
  equivalent to the manual API), not on re-proving durability/MVCC/
  Raft properties already covered by Phases 1-7's scenario corpus.
- No SQL scenario is added to
  [`docs/scenario-corpus.md`](../scenario-corpus.md) in this
  Architecture Foundation phase, since SQL is out of scope until
  Phase 8; SQL-specific scenarios will be added to that document when
  Phase 8 begins.
