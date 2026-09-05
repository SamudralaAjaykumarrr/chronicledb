# ADR-0005: Transaction Commit and Conflict Model

Status: Accepted

## Context

Given MVCC/SI ([ADR-0004](0004-mvcc-snapshot-isolation.md)), ChronicleDB
needs a concrete mechanism for submitting a transaction's mutations,
detecting conflicts, and guaranteeing atomic multi-key visibility —
one that every replica can evaluate identically, since the same
transaction commit must produce the same outcome on the leader and on
every follower that later applies it.

## Decision

- A transaction's entire write set is submitted as **one** logical
  command: `CommitTxn(RequestID, TxnID, StartSeq, Mutations...)` (see
  [`docs/transactions.md`](../transactions.md) §3).
- The command is evaluated **deterministically at apply time**
  (`internal/fsm.Apply`), not just pre-checked on the leader: for each
  key in `Mutations`, if the current latest committed `CommitSeq` for
  that key exceeds `StartSeq`, the whole transaction aborts; otherwise
  every mutation in the set is applied atomically at one shared
  `CommitSeq` (see [`docs/mvcc.md`](../mvcc.md) §4-5).
- A leader-side pre-check is permitted only as a latency optimization
  and is never authoritative on its own.

## Alternatives Considered

1. **Per-key commit (each mutation as its own command/log entry).**
   Rejected: makes multi-key atomicity require an additional
   coordination mechanism (readers would need to know a "transaction
   boundary" to avoid observing a partial set of the per-key commits)
   — effectively reinventing a mini 2PC unnecessarily, when a single
   combined command gives atomicity for free.
2. **Leader-only conflict validation, committed to the log only if
   the leader's pre-check passes (i.e. never propose a doomed
   transaction).** Rejected as the *sole* mechanism: after a leader
   failover, a new leader has no reliable way to re-validate an
   already-in-flight proposal using only the old leader's now-gone
   in-memory pre-check state; and followers, to independently verify
   `STATE MACHINE SAFETY`, must be able to reach the same conclusion
   themselves. Deterministic apply-time evaluation is required
   regardless; a pre-check can only ever be an optional add-on for
   reduced latency on transactions doomed to conflict, never a
   replacement for the deterministic check.
3. **Locking-based conflict prevention (acquire locks on write-set
   keys before commit, à la a lock manager).** Rejected: reintroduces
   blocking behavior MVCC is chosen to avoid, and lock-manager state
   would itself need to be part of the replicated state machine
   (deadlock detection across a distributed lock manager is a
   substantial additional problem), for no benefit over the simpler,
   already-deterministic first-committer-wins comparison.

## Consequences

- A `CommitTxn` command can and legitimately does end up as a
  **committed** Raft log entry whose deterministic apply result is
  `ABORTED` — this is expected and correct, not a failure to prevent
  doomed proposals from being committed to the log (see
  [`docs/mvcc.md`](../mvcc.md) §4).
- Conflict detection cost is O(size of write set) at apply time,
  independent of read-set size (SI's first-committer-wins rule, unlike
  SSI, does not need to track read sets — see
  [ADR-0004](0004-mvcc-snapshot-isolation.md) alternative 1).

## Correctness Implications

- Directly implements `CONFLICT-CORRECTNESS` and `ATOMICITY`
  ([`docs/invariants.md`](../invariants.md)).
- Ties directly into `STATE MACHINE SAFETY`: because the conflict
  decision is a pure function of the command and the current committed
  state, every replica independently reaches the identical decision.

## Testing and Proof Obligations

- TX-3 (multi-key atomic commit), TX-5 (concurrent conflicting
  transactions) in [`docs/scenario-corpus.md`](../scenario-corpus.md)
  §Transactions.
- Replicated-mode variant (once Phase 5 lands): verify all three nodes
  independently apply a given `CommitTxn` command and reach the
  identical `COMMITTED`/`ABORTED` decision.
