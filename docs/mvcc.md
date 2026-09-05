# MVCC Model

Status: Phase 2 implemented (`internal/mvcc`). The visibility and
conflict rules below are implemented as specified and tested against
`docs/scenario-corpus.md` §Transactions (TX-1 through TX-8). See §8 for
implementation-time decisions.

## 1. Isolation level: Snapshot Isolation, precisely scoped

ChronicleDB V1's isolation target is **Snapshot Isolation (SI)**.

ChronicleDB does **not** claim **SERIALIZABLE** isolation. This is a
deliberate, permanent-until-revisited scoping decision — see
[ADR-0004](adr/0004-mvcc-snapshot-isolation.md). Any document, log
message, or client-facing text that says ChronicleDB is serializable
is a bug in that document, not a description of a stronger guarantee
actually provided.

### 1.1 Why Snapshot Isolation is not automatically Serializable

Snapshot Isolation guarantees that every transaction reads from a
single consistent snapshot of committed data as of its `StartSeq`, and
that two transactions cannot both commit a write to the *same* key
based on a stale view of *that key* (write-write conflicts are
caught — see §4). It does **not** prevent **write skew**: two
concurrent transactions can each read a set of keys, each write to a
*different* key based on what they read, and both commit successfully,
even though no serial (one-at-a-time) execution of the two
transactions could have produced the resulting state. Example:

```
Invariant the application wants: x + y >= 0 (x and y independently
mutable, starting at x = 10, y = 10)

T1: StartSeq = S            T2: StartSeq = S
    read x = 10, y = 10          read x = 10, y = 10
    if x + y >= 0: write x -= 15      if x + y >= 0: write y -= 15
    commit (writes only x)            commit (writes only y)

Both commit under SI (no overlapping write key), final state:
x = -5, y = -5, x + y = -10 < 0 — invariant violated.
```

Neither transaction's write set overlapped, so the first-committer-wins
rule (§4) never fires. This is expected, textbook SI write skew, not a
bug. Applications that need to prevent this must either (a) take an
explicit write-intent on a key both transactions touch (turning it
into a write-write conflict), or (b) wait for a future, explicitly
proven SERIALIZABLE mode (out of V1 scope — see
[`docs/non-goals.md`](non-goals.md)).

## 2. Core concepts

- **`TxnID`**, **`RequestID`**, **`StartSeq`**, **`CommitSeq`** — defined
  in [`docs/architecture.md`](architecture.md) §3. This document uses
  them as defined there.
- **Version** — for key `K`, a tuple `(CommitSeq, ValueOrTombstone)`
  produced by exactly one committed transaction's write to `K`.
- **Version chain** — for key `K`, the ordered (by `CommitSeq`,
  ascending) list of all versions ever committed for `K`. Chains only
  grow by appending a new version at commit time; existing versions
  are never mutated. (Removal of old versions is a distinct future
  concern — see [`docs/mvcc.md`](mvcc.md) §6 MVCC Garbage Collection.)
- **Tombstone** — a version whose value is "deleted," produced by a
  committed delete of `K`. A tombstone is a real version with a real
  `CommitSeq`; it participates in visibility and conflict rules exactly
  like a value-bearing version.
- **Transaction-local write set** — the set of `(key -> value |
  tombstone)` pairs a transaction has written since `BEGIN`, held
  privately (in-memory, on the leader) until commit. Never visible to
  any other transaction. Never written to durable storage on its own —
  see [`docs/transactions.md`](transactions.md) §Where Uncommitted
  Writes Live.

## 3. Visibility rule (binding)

At `BEGIN`, transaction `T` captures `StartSeq = S` (see
[`docs/replication.md`](replication.md) §Read Consistency for exactly
how `S` is established in replicated mode).

For any key `K`, `T`'s read of `K` is defined as:

1. If `K` is present in `T`'s own local write set, return that local
   value (or "not found," if the local write is a tombstone). **Own
   writes always shadow committed data**, regardless of `CommitSeq`.
2. Otherwise, scan `K`'s version chain for the version with the
   **largest `CommitSeq` such that `CommitSeq <= S`**.
   - If such a version exists and is a value, return that value.
   - If such a version exists and is a tombstone, `K` does not exist
     for `T` (a delete visible as of `S`).
   - If no such version exists (every version of `K`, if any, has
     `CommitSeq > S`, or `K` has never been written), `K` does not
     exist for `T`.
3. Versions with `CommitSeq > S` are never visible to `T`, no matter
   which transaction produced them or whether that transaction has
   since committed. This is what makes `T`'s view a *snapshot*: it
   never changes across repeated reads within `T`, even if other
   transactions commit concurrently (**repeatable reads** are
   subsumed by SI).
4. Uncommitted writes made by any transaction other than `T` are never
   visible to `T` under any circumstance. `T` only ever sees versions
   that exist in the committed version chain (step 2) or in its own
   local write set (step 1). There is no "read uncommitted" mode.

### 3.1 Own-write visibility examples

```
K has committed versions: (CommitSeq=5, "a"), (CommitSeq=12, "b")

T begins with StartSeq = 10.
  read(K) -> "a"                     (5 <= 10 < 12)
T writes K = "local-c" (local only, not committed yet).
  read(K) -> "local-c"               (own write shadows committed "a")
T deletes K (local tombstone).
  read(K) -> not found               (own tombstone shadows committed "a")
```

### 3.2 Concurrent writer example

```
K has committed version (CommitSeq=5, "a").

T1 begins, StartSeq = 10.
T2 begins, StartSeq = 10.
T2 writes K = "b", commits -> new version (CommitSeq=11, "b").
T1 reads K -> still "a"     (11 > 10, not visible to T1's snapshot)
```

## 4. Write-write conflicts: first-committer-wins

ChronicleDB uses **first-committer-wins** for SI, per
[ADR-0005](adr/0005-transaction-commit-and-conflict-model.md).

**Rule:** for every key `K` in transaction `T`'s write set, at the
deterministic commit/apply point (see
[`docs/transactions.md`](transactions.md) §Deterministic Apply), the
state machine checks the *current* latest committed version of `K`
(as of the moment the commit command is applied, in committed log
order — not as of `T`'s `StartSeq`). If that latest version's
`CommitSeq > T.StartSeq`, `T` has a write-write conflict on `K` and
**the entire transaction `T` aborts** — see §5 for atomicity.

```
T.StartSeq = 10, T writes K and M.

At apply time:
  latest committed version of K has CommitSeq = 15  (15 > 10 -> CONFLICT)
  latest committed version of M has CommitSeq =  8  (8 <= 10 -> no conflict on M alone)

Because K conflicts, the whole transaction aborts. M is not written,
even though M alone had no conflict.
```

This decision **must** be made deterministically, in committed
log/apply order, by every replica independently arriving at the same
answer — it is not just a leader-side pre-check. See
[`docs/transactions.md`](transactions.md) §Why Conflict Detection
Happens At Apply Time, Not Just On The Leader. It is expected and
correct for a `CommitTxn` command to become a **committed Raft log
entry** whose deterministic apply result is `ABORTED` — the command
being committed to the log means "the cluster agrees this was
proposed and will be evaluated identically everywhere," not "the
transaction is guaranteed to succeed."

## 5. Atomicity of multi-key commit

A transaction's entire mutation set is evaluated and applied as **one**
deterministic state-machine operation:

- If no key in the write set conflicts (§4), **every** mutation in the
  write set becomes a new version, all sharing the same `CommitSeq`,
  atomically from the point of view of any reader (no reader can
  observe some-but-not-all of the transaction's writes).
- If **any** key in the write set conflicts, **zero** mutations are
  applied. The transaction's outcome is `ABORTED`; no partial state
  exists anywhere, ever, including after a crash mid-apply (`Apply`
  is defined to be all-or-nothing per invocation — see
  [`docs/transactions.md`](transactions.md) §Atomicity Mechanism).

## 6. MVCC Garbage Collection (not implemented in V1; rule defined now)

ChronicleDB does not implement version garbage collection in V1.
Old versions accumulate. The future safe rule is defined now so the
data model does not need to change shape later:

- Define `GCWatermark` = the smallest `StartSeq` among all currently
  active (not yet committed/aborted) transactions' snapshots, or, if
  no transaction is active, the most recent `CommitSeq` known to be
  applied.
- A version `(CommitSeq=c, ...)` for key `K` may be safely removed only
  if there exists a strictly newer version `(CommitSeq=c', ...)` for
  `K` with `c < c' <= GCWatermark` — i.e. a newer version already
  covers every snapshot that could still legally observe `c`, and no
  active transaction's `StartSeq` falls in `[c, c')`.
- The single newest version of `K` is never removed by this rule
  (there is nothing newer to make it redundant), even if it is a very
  old tombstone.
- **MVCC version GC is a distinct problem from Raft log compaction and
  from database snapshots** (see [`docs/snapshots.md`](snapshots.md)
  §Relationship to MVCC GC and Raft Log Compaction). MVCC GC reclaims
  old *values* no reader can legally see; Raft log compaction reclaims
  old *replicated log entries* superseded by a snapshot; a database
  snapshot is a checkpoint of currently-live MVCC state. Conflating
  any two of these has, in real systems, caused correctness bugs (e.g.
  reclaiming a version a long-lived snapshot still needs); ChronicleDB
  keeps them as three separate, explicitly related mechanisms.

## 7. Non-existent keys and repeated reads

- A key that has never been written has no version chain; every
  transaction sees it as not-found, for every `StartSeq`.
- Repeated reads of the same key within one transaction always return
  the same result (per §3, visibility is a pure function of
  `StartSeq`, the version chain as of apply time it was computed
  against, and `T`'s own write set) — SI subsumes repeatable read.

## 8. Phase 2 implementation decisions (resolved)

- **In-memory representation**: `internal/mvcc.Store` holds one
  `map[string][]Version` protected by a single `sync.RWMutex`, exactly
  the "in-memory materialized state, rebuilt from durable history at
  startup" §1 anticipates — no on-disk index, B-tree, or LSM structure.
  Each key's `[]Version` is maintained sorted ascending by `CommitSeq`
  purely as a consequence of how it is built (`ApplyCommit` only ever
  appends a strictly larger `CommitSeq` than the chain's current tail);
  `Visible` locates the newest version with `CommitSeq <= StartSeq` via
  binary search over that invariant, verified against a linear-scan
  reference model under randomized inputs
  (`internal/mvcc/mvcc_test.go`'s `TestVisibilityPropertyAgainstReferenceModel`).
- **Conflict check granularity**: `CheckConflicts` evaluates every
  mutation's key under one `RLock` (not one lock per key), so the
  answer reflects a single consistent instant of the store rather than
  a torn view across separate lookups — required because §4's rule
  ("if any key conflicts, the whole transaction aborts") must be
  evaluated as one atomic question, not key-by-key.
- **Atomicity implementation**: `ApplyCommit` validates every
  mutation's monotonicity precondition (new `CommitSeq` strictly
  greater than that key's current tail) in a first pass, and only then
  mutates any chain in a second pass — so a precondition failure on one
  key of a multi-key commit can never leave an earlier key in the same
  batch mutated while a later one is not (§5 ATOMICITY, tested by
  `TestApplyCommitAtomicOnMonotonicityViolation`). In correct usage
  (all writes funneled through `internal/txn.Manager`'s single
  serialization point) this precondition can never actually fail; it
  exists as a defensive invariant check, not an expected runtime path.
- **`GCWatermark`/version GC**: not implemented, per §6 and
  `docs/non-goals.md` §MVCC version garbage collection — every version
  ever committed is retained for the lifetime of the process.
