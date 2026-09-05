# Transaction Model

Status: Architecture Foundation. No transaction implementation exists
yet.

This document defines the transaction lifecycle, idempotency, and
uncertain-outcome handling. MVCC visibility and conflict rules are
defined in [`docs/mvcc.md`](mvcc.md); this document is about the
transaction's lifecycle and its interaction with the replicated log.

## 1. Transaction lifecycle

```
BEGIN -> establishes TxnID, StartSeq, empty local write set
  |
  |-- READ(K)   -> MVCC visibility rule (docs/mvcc.md §3)
  |-- WRITE(K,V)-> local write set updated (not durable, not visible
  |                to any other transaction)
  |-- DELETE(K) -> local write set updated with a tombstone
  |
  +-- COMMIT(RequestID) -> submit CommitTxn command (§3) -> deterministic
  |                        outcome: COMMITTED or ABORTED (conflict)
  |
  +-- ABORT -> discard local write set, no durable trace, no
               RequestID outcome recorded (nothing was ever proposed)
```

- **`TxnID`** identifies the open session. It is created and held only
  in the leader's in-memory session state.
- **`StartSeq`** is fixed at `BEGIN` and never changes for the life of
  the transaction (see [`docs/mvcc.md`](mvcc.md) §3 for why this makes
  reads repeatable).
- **Local write set**: an in-memory map of `key -> value | tombstone`
  scoped to the `TxnID`. Reads within the transaction consult the
  local write set first, then the committed version chain (see
  [`docs/mvcc.md`](mvcc.md) §3).

## 2. Where uncommitted writes live

Uncommitted writes live **only** in the leader's in-memory session
state for that `TxnID`. They are:

- **Not** written to the durable log.
- **Not** replicated.
- **Not** visible to any other transaction, on any node.
- **Not** subject to durable-crash-recovery guarantees. If the process
  holding the session dies (leader crash, leader step-down, connection
  loss) before `COMMIT` is submitted, the transaction is gone. It does
  not need to be recovered, retried, or reported to the client as
  anything other than "this transaction never committed" — because,
  correctly, it did not.

This is why ChronicleDB does not need a separate durable "transaction
log" for in-flight work: there is nothing about an open, uncommitted
transaction that requires durability. Durability only enters the
picture at `COMMIT`, at which point the transaction stops being "open
session state" and becomes "one deterministic command" (§3).

## 3. Commit: one deterministic command

`COMMIT` does not write mutations one at a time. It submits **one**
logical command carrying everything every replica needs to reach the
same outcome independently:

```
CommitTxn(RequestID, TxnID, StartSeq, Mutations = {K1: V1 | tombstone, ...})
```

- This command is proposed to the replicated log (Phase 4+) or
  appended directly to the local durable log (standalone mode,
  Phases 1-3) — see [`docs/replication.md`](replication.md) for the
  before/after-Raft comparison of what "commit" means mechanically.
- The command is **self-contained**: it does not reference leader-only
  in-memory state. Any replica applying this command, using only the
  command's own fields plus its own local committed version chains and
  `RequestID` outcome table, computes the identical outcome (§4, §6).

## 4. Deterministic apply

When a `CommitTxn` command is applied (by `internal/fsm.Apply`, in
committed log order):

1. **Idempotency check first** (§6): if `RequestID` already has a
   recorded terminal outcome, return that recorded outcome unchanged.
   Do not re-evaluate conflicts, do not re-apply mutations.
2. **Conflict check** (see [`docs/mvcc.md`](mvcc.md) §4): for each key
   in `Mutations`, compare the current latest committed `CommitSeq`
   against `StartSeq`. If any key conflicts, the outcome is `ABORTED`;
   no mutation is applied.
3. **If no conflict**: assign this command's `CommitSeq` (the durable
   log index / committed Raft log index carrying this command — see
   [`docs/architecture.md`](architecture.md) §3), append one new
   version per key in `Mutations` at that `CommitSeq`, atomically.
   Outcome is `COMMITTED`.
4. **Record the terminal outcome** for `RequestID` (§6) before
   returning — recording the outcome is part of the same atomic apply
   step, not a follow-up action that could be skipped by a crash.

### 4.1 Why conflict detection happens at apply time, not just on the leader

A leader could optimistically check conflicts before proposing a
command, to avoid wasting a round of replication on a doomed
transaction. That check is a valid, optional performance optimization
for V1 (reduces latency on conflicting transactions the leader can see
in its own recent history) but it is **never sufficient by itself**:

- Between the leader's pre-check and the command's actual commit
  index being assigned, other commands may commit and change what
  "the latest committed version" is.
- After a leader failover, a *new* leader must be able to arrive at
  the exact same `COMMITTED`/`ABORTED` decision for any command still
  being processed, using only the command's fields and the committed
  log — it cannot rely on the old leader's now-gone in-memory
  pre-check.
- Followers must independently apply the same command and reach the
  same decision as the leader, or `STATE MACHINE SAFETY` (see
  [`docs/invariants.md`](invariants.md)) is violated.

Therefore the *authoritative* conflict decision is always made inside
`internal/fsm.Apply`, deterministically, from the command and the
current committed state — never trusted from a leader-side
pre-check alone.

## 5. Atomicity mechanism

`internal/fsm.Apply` for a `CommitTxn` command is implemented as a
single, non-interruptible logical step from the point of view of any
observer:

- No reader can observe a state where some of `Mutations` are applied
  and others are not.
- If the process crashes during `Apply`, recovery replays the durable
  command from the log and re-applies it in full from scratch (`Apply`
  is deterministic and, for a given committed command, idempotent to
  re-execute during replay — see [`docs/recovery.md`](recovery.md)).
  There is no partially-applied state to clean up, because `Apply`
  either fully updates the in-memory structures for this command or
  (on crash) leaves them exactly as they were before `Apply` was
  invoked; recovery re-runs the whole step.

## 6. `RequestID` idempotency

- `RequestID` is a client-supplied opaque token attached to a `COMMIT`
  request (and, in future phases, potentially other mutating request
  types).
- `internal/fsm` maintains a durable **RequestID outcome table**:
  `RequestID -> TerminalOutcome` where `TerminalOutcome` is one of
  `COMMITTED(CommitSeq)` or `ABORTED(reason)`.
- This table is populated as part of the same atomic `Apply` step that
  processes the command (§4, §5) — so it is durable and replicated
  exactly when the transaction's outcome is durable and replicated.
  It is included in state-machine snapshots (see
  [`docs/snapshots.md`](snapshots.md)) so it survives compaction.
- **Retry with the same `RequestID`**: `internal/fsm.Apply` (or a
  read-only outcome lookup, for a pure retry that need not go through
  the log again once the outcome is already known — an optimization,
  not a correctness requirement) returns the previously recorded
  outcome. The mutation set is **not** re-evaluated or re-applied. No
  duplicate versions are created.
- **Retry with a different `RequestID`, same mutations**: this is a
  **different logical request** by definition. ChronicleDB does not
  attempt to detect that two different `RequestID`s "mean the same
  thing" at the business/semantic level — that is the client's
  responsibility (reuse the same `RequestID` for what should be one
  logical operation). ChronicleDB guarantees identity-based
  deduplication only.
- **Retention**: V1 may retain `RequestID` outcomes indefinitely. This
  is deliberately the safe default rather than a premature
  correctness-risking optimization. A future phase may add
  time-bounded or client-session-bounded garbage collection of old
  outcomes once a safe expiry policy (e.g. client-acknowledged receipt,
  or a generous fixed TTL) is designed and justified — tracked as a
  deferred decision in [`docs/roadmap.md`](roadmap.md), not implemented
  speculatively now.

## 7. Uncertain commit outcomes

Scenario: client sends `COMMIT`, the server actually commits and
applies it, but the response is lost (network failure, client
timeout, proxy failure) before the client observes it. The client does
not know whether the commit succeeded.

ChronicleDB's contract:

- The client **must not guess**. It does not resend the mutation set
  as a "fresh" request with a new `RequestID` (that would be a
  different logical request — see §6 — and could double-apply the
  intended effect).
- The client **retries using the same `RequestID`**. Per §6, this is
  guaranteed to return the original terminal outcome, not to
  re-execute anything.
- ChronicleDB exposes (conceptually, from Phase 3 onward) an explicit
  outcome query, `GetRequestOutcome(RequestID)`, so a client can ask
  "what happened to this request" without needing to resend the full
  commit payload.

### 7.1 Database-knowledge vs. client-knowledge

These are explicitly different things:

| | Meaning |
|---|---|
| **Database's terminal outcome** | The actual, already-decided, durable fact: `COMMITTED(CommitSeq)` or `ABORTED(reason)`. Once `Apply` has run for this `RequestID`, this fact exists and will not change. |
| **Client's current knowledge** | What the client has actually observed. This can lag or fail to reach the database's terminal outcome — the client may be in state `UNKNOWN` (sent the request, got no confirmed response) even though the database has already reached `COMMITTED`. |

`UNKNOWN` is a **client-side knowledge state**, not a system state and
not a license to double-apply. The correct client behavior under
`UNKNOWN` is exactly §7's retry-by-`RequestID` or outcome-query path —
never "assume it failed and resend as new," and never "assume it
succeeded and skip retry-until-confirmed if the outcome actually
matters to the caller."

## 8. Open transactions and leadership changes

- An **open** (not-yet-committed) transaction is pure in-memory session
  state on one node (§2). It has no claim to survive that node's
  failure, a leadership change, or the client's connection dropping.
  V1 does not attempt to migrate or resurrect an in-flight interactive
  transaction across a leader change. This is intentional scope
  control (see [`docs/non-goals.md`](non-goals.md)) — it does not
  weaken any durability guarantee, because nothing about an open
  transaction was ever promised to be durable (§2).
- Once `COMMIT` has been submitted and the resulting `CommitTxn`
  command is durably represented in the log (persisted locally; in
  replicated mode, on its way to/through the replicated log), its
  outcome is no longer tied to the original client connection, the
  original leader, or the original in-memory session. It is discovered
  the same way regardless of what happened to those: via `RequestID`
  (§6, §7).
- This is the precise distinction the architecture requires between
  **open transaction state** (ephemeral, session-scoped, disposable)
  and **submitted durable commit command** (durable, replicated,
  outcome-stable). See [`docs/invariants.md`](invariants.md)
  `REQUEST-OUTCOME-STABILITY`.
