# ADR-0003: WAL / Durable-Log / Raft-Log Responsibility Model

Status: Accepted

## Context

A distributed database can accidentally accumulate multiple,
independently-authoritative "logs" — a Raft replicated log, a local
WAL, and a separate transaction commit/recovery log — that can drift
out of sync with each other. This is a well-known real-world failure
mode in database systems design and is explicitly called out as a risk
to avoid in ChronicleDB's founding requirements.

## Decision

ChronicleDB defines exactly one logical ordered history and one
physical persistence mechanism for it:

- The **Raft log** is the logical ordered history of commands
  (`(term, index) -> command`).
- **`internal/wal`** is the sole physical persistence mechanism for
  that logical history (and for Raft's other persistent state:
  `currentTerm`, `votedFor`).
- **There is no separate transaction log.** A transaction commit is
  encoded as a single deterministic command (`CommitTxn(RequestID,
  TxnID, StartSeq, Mutations...)`) that becomes one entry in the same
  logical history. Transaction state and outcomes are derived from
  replaying/inspecting that committed history via
  `internal/fsm.Apply` — never recorded in an independent recovery
  structure.
- Before Raft exists, the standalone engine's durable log plays the
  role of "the one committed history" directly (see
  [`docs/architecture.md`](../architecture.md) §2).

Full detail in [`docs/architecture.md`](../architecture.md) §2 and
[`docs/wal.md`](../wal.md) §1, §6.

## Alternatives Considered

1. **Separate transaction log, replayed independently of the Raft
   log at recovery time.** Rejected: creates exactly the
   two-sources-of-truth problem this ADR exists to avoid — a crash
   between writing to the transaction log and writing to the Raft log
   (or vice versa) could produce disagreement between "what the
   transaction log says happened" and "what the Raft log says was
   committed," with no principled way to resolve the disagreement.
2. **Raft log with its own independent physical durability mechanism,
   separate from a general-purpose WAL used for other metadata.**
   Rejected: doubles the amount of crash-consistency-sensitive code
   (two append/fsync/replay/corruption-handling implementations
   instead of one) for no correctness benefit — Raft's persistence
   requirements (ordered, durable, checksummed, replayable) are
   identical in kind to what any other durable history in the system
   needs.
3. **Treat the state-machine-materialized MVCC data itself as
   "the" durable source of truth (write-back mutable state directly,
   WAL only for crash recovery of that mutable state, à la a
   traditional page-based database).** Rejected: this couples
   durability format tightly to in-memory data structure layout,
   complicates replication (which needs to ship *commands*, not
   mutable-page diffs, to be Raft-compatible), and abandons the
   append-only simplicity chosen in
   [ADR-0002](0002-local-storage-architecture.md).

## Consequences

- Every durable, ordered fact in the system — Raft log entries, Raft
  hard state, and (implicitly, via replay) transaction outcomes —
  flows through one code path (`internal/wal`), so a correctness fix
  or a corruption-handling improvement to that code path benefits the
  entire system at once.
- There is no possibility of the Raft log and "the transaction log"
  disagreeing, because there is only one log.

## Correctness Implications

- This decision is the direct enabling mechanism for the
  `CONSISTENT LOG RESPONSIBILITY` invariant
  ([`docs/invariants.md`](../invariants.md)).
- It also simplifies `RECOVERY-NON-INVENTION`: recovery has exactly
  one history to validate and replay, not several to reconcile.

## Testing and Proof Obligations

- Architecture review obligation, restated from
  [`docs/invariants.md`](../invariants.md) `CONSISTENT LOG
  RESPONSIBILITY`: any future feature proposing a new durable-history
  mechanism must justify, in its own ADR, why it cannot be expressed
  as a command in the existing log.
- Scenario coverage: every transaction scenario in
  [`docs/scenario-corpus.md`](../scenario-corpus.md) §Transactions and
  §Idempotency implicitly tests this decision, since transaction
  outcomes are only ever derived from the one log's replay.
