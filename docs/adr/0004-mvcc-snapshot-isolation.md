# ADR-0004: MVCC Model and Snapshot Isolation

Status: Accepted

## Context

ChronicleDB needs a concurrency-control model that supports
non-blocking reads against concurrent writers, well-defined
transaction semantics, and a conflict-detection rule that can be
evaluated deterministically by every replica independently (a
requirement of the replicated state machine, see
[ADR-0007](0007-deterministic-replicated-state-machine-boundary.md)).

## Decision

Adopt **Multi-Version Concurrency Control (MVCC)** with **Snapshot
Isolation (SI)** as the initial, and only currently claimed, isolation
level. Full rules in [`docs/mvcc.md`](../mvcc.md):

- Each transaction captures `StartSeq` at `BEGIN` and reads the newest
  committed version of each key with `CommitSeq <= StartSeq`, shadowed
  by its own local write set.
- Write-write conflicts use **first-committer-wins**, evaluated
  deterministically at apply time (see
  [ADR-0005](0005-transaction-commit-and-conflict-model.md)).
- ChronicleDB explicitly does **not** claim SERIALIZABLE isolation.
  Snapshot Isolation permits write skew, and ChronicleDB documents this
  truthfully rather than overclaiming (see
  [`docs/mvcc.md`](../mvcc.md) §1.1 for a worked write-skew example).

## Alternatives Considered

1. **Serializable Snapshot Isolation (SSI) from the start** (as used
   by PostgreSQL's `SERIALIZABLE` level, via read/write dependency
   tracking to detect and abort dangerous structures). Rejected for
   V1: SSI requires tracking read sets in addition to write sets and
   detecting specific dependency cycles (rw-antidependencies) among
   concurrent transactions — meaningfully more complex to implement
   and to prove correct than first-committer-wins SI, and not required
   to demonstrate the project's core distributed-systems goals. A
   legitimate, explicitly-scoped future enhancement, not a V1 claim.
2. **Two-Phase Locking (2PL)** for strict serializability. Rejected:
   blocks readers behind writers (loses MVCC's key benefit of
   non-blocking reads), and deadlock detection/avoidance in a
   replicated setting adds significant complexity that would compete
   with proving the consensus layer correct first.
3. **Read Committed** (weaker than SI: no repeatable-read guarantee,
   each statement sees the latest committed data). Rejected: too weak
   for the transactional guarantees ChronicleDB wants to demonstrate,
   and doesn't meaningfully simplify implementation relative to SI once
   MVCC version chains already exist.
4. **Last-writer-wins** conflict resolution instead of
   first-committer-wins. Rejected: silently discards a concurrent
   writer's update instead of surfacing a conflict, which is a much
   weaker and more surprising guarantee for a system claiming
   transactional semantics — first-committer-wins makes conflicts
   explicit and client-visible (`ABORTED`), which is the standard,
   expected SI conflict behavior.

## Consequences

- Applications relying on ChronicleDB must be aware of, and
  potentially work around, write skew (§ [`docs/mvcc.md`](../mvcc.md)
  §1.1) if their invariants span multiple keys with disjoint writes.
- Old versions accumulate until MVCC GC is implemented (deferred, see
  [`docs/non-goals.md`](../non-goals.md)); this is an accepted,
  documented interim cost.

## Correctness Implications

- Directly implements the `MVCC-VISIBILITY`, `CONFLICT-CORRECTNESS`,
  and `ISOLATION-TRUTHFULNESS` invariants
  ([`docs/invariants.md`](../invariants.md)).
- The `ISOLATION-TRUTHFULNESS` invariant exists specifically because
  of this decision: it is a standing obligation on all future
  documentation and code comments never to claim a stronger isolation
  level than what has actually been implemented and proven.

## Testing and Proof Obligations

- Property-based visibility tests and the write-skew example (TX-8,
  [`docs/scenario-corpus.md`](../scenario-corpus.md) §Transactions),
  which is deliberately kept as a *passing, demonstrated* example of
  the documented limitation, not something to be "fixed" without a new
  ADR.
- Concurrent conflicting-transaction tests (TX-5) proving
  first-committer-wins behavior deterministically.
