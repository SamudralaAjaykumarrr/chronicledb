# ADR-0006: RequestID Idempotency and Uncertain Outcomes

Status: Accepted

## Context

Network failures mean a client can never be certain a commit request's
response actually reflects what the server did — the request may have
succeeded with the response lost, or genuinely failed. Without an
idempotency mechanism, a naive retry risks double-applying a
transaction's effects.

## Decision

- Every commit request carries a client-supplied, opaque **`RequestID`**.
- `internal/fsm` maintains a durable **`RequestID` outcome table**
  (`RequestID -> COMMITTED(CommitSeq) | ABORTED(reason)`), populated as
  part of the same atomic apply step that processes the command (see
  [`docs/transactions.md`](../transactions.md) §6).
- Retrying with the **same** `RequestID` always returns the recorded
  outcome, without re-evaluating or re-applying anything.
- Retrying with a **different** `RequestID` is a different logical
  request — ChronicleDB does not attempt semantic deduplication beyond
  identity.
- V1 retains outcomes **indefinitely**; GC is deferred (see
  [`docs/non-goals.md`](../non-goals.md)).
- A conceptual `GetRequestOutcome(RequestID)` query lets a client
  resolve an uncertain outcome without resending the full mutation
  payload (see [`docs/transactions.md`](../transactions.md) §7).
- `UNKNOWN` is explicitly a **client-knowledge** state, not a
  system state, and never license to double-apply.

## Alternatives Considered

1. **No idempotency mechanism; rely on clients to design
   idempotent-by-construction mutations.** Rejected: pushes a hard,
   easy-to-get-wrong distributed-systems problem onto every client,
   for no benefit — a general `RequestID` mechanism solves it once,
   centrally, correctly.
2. **Time-bounded idempotency window (e.g. dedupe only within N
   minutes of the original request), from the start.** Rejected as
   the initial default: a client that is slow to retry (e.g. after an
   extended partition or client-side outage) past the window could
   double-apply — exactly the failure mode idempotency exists to
   prevent. Indefinite retention is the safe default; a bounded window
   is deferred until a specific, safe expiry trigger (e.g.
   client-acknowledged receipt) is designed (see
   [`docs/non-goals.md`](../non-goals.md) §`RequestID` outcome garbage
   collection).
3. **Deduplicate by mutation-content hash instead of an explicit
   `RequestID`.** Rejected: two logically distinct operations can have
   identical content (e.g. two separate `$10` transfers between the
   same accounts); content hashing would incorrectly conflate them.
   Explicit `RequestID` identity, chosen by the client, correctly
   represents "this is intentionally the same logical operation."
4. **Server-generated `RequestID` (returned to the client only after
   the request is received).** Rejected: doesn't solve the actual
   problem — the client needs an ID it can present on a *retry* of a
   request whose *original* response (which would carry a
   server-generated ID) may itself be the thing that was lost.

## Consequences

- `RequestID` outcome table grows unboundedly in V1 (accepted,
  documented interim cost, same category as MVCC version retention —
  see [ADR-0004](0004-mvcc-snapshot-isolation.md) Consequences).
- Clients are responsible for generating and correctly reusing
  `RequestID`s across retries of what they intend as one logical
  operation; ChronicleDB does not infer this on their behalf.

## Correctness Implications

- Directly implements `IDEMPOTENCY` and `REQUEST-OUTCOME-STABILITY`
  ([`docs/invariants.md`](../invariants.md)).
- Because the outcome table is part of state-machine snapshots (see
  [`docs/snapshots.md`](../snapshots.md) §2), this guarantee survives
  log compaction and full node replacement via snapshot install, not
  just simple restarts.

## Testing and Proof Obligations

- ID-1 through ID-5 in
  [`docs/scenario-corpus.md`](../scenario-corpus.md) §Idempotency,
  covering pre-response duplicates, post-restart duplicates,
  lost-response outcome queries, retry-after-compaction, and
  distinct-`RequestID`-same-mutations behavior.
