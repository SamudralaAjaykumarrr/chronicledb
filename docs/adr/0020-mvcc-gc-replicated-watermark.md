# ADR-0020: MVCC Garbage Collection as Replicated State, with a Snapshot Horizon

Status: **Proposed** (planning only — `docs/v0.6.0-plan.md` Part B; no
production code implements this yet, and this ADR moves to Accepted
only when that implementation lands)

## Context

`docs/mvcc.md` §6 has defined the safe GC rule since Phase 2 but
explicitly left it unimplemented: `internal/mvcc.Store.chains` only
ever grows, and the package's own doc comment says so.
`docs/enterprise-v1-plan.md` §10 designates `v0.6.0` as the
implementation point, specifying "a background, low-priority process
… that computes the current safe watermark from the live transaction
manager's active-snapshot set and walks version chains removing
eligible entries."

Five facts in the tree at `481ff93` make that literal design unsafe
or impossible as written:

1. **`internal/mvcc` is inside the determinism boundary.**
   `docs/invariants.md` `DETERMINISM BOUNDARY` names it explicitly,
   and `STATE MACHINE SAFETY`'s proof obligation is byte-identical
   state across independently constructed replicas — enforced today by
   `internal/fsm`'s `TestEncodeStateDeterministic`. "Which
   transactions are currently open" is a local, per-process,
   wall-clock-dependent fact. A background goroutine reclaiming
   versions from a locally computed watermark would make two replicas
   with identical committed histories hold different chains and emit
   different snapshot bytes.
2. **A follower has no active transactions at all.** Its locally
   computed watermark would be "latest applied `CommitSeq`", so it
   would reclaim far more aggressively than the leader — and a later
   failover onto that node would silently expose the
   more-aggressively-reclaimed state to readers.
3. **`internal/txn` is not in the replicated path.** In replicated
   mode `internal/sql`'s `replicatedEngine` calls
   `Node.BeginReadIndex` and `Node.Propose` directly; `txn.Manager` is
   standalone-only, and it does not track live `Txn`s in either mode
   (`Begin()` constructs and forgets). There is no active-snapshot set
   to expose from where §10 says to expose it.

4. **`Store.Visible` is not the only committed-read path.**
   `internal/sql`'s `mergeScan` (`internal/sql/engine.go:307`),
   reached from `replicatedTxn.ScanPrefix` (`:236`, `:241`) and
   `standaloneTxn.ScanPrefix` (`:154`), calls `mvcc.Store.Export()`
   — which takes no `startSeq` — and re-implements the visibility
   rule locally in `visibleInChain` (`:344`). A horizon guard placed
   only on `Visible` is bypassed by every prefix scan in the product,
   and because `ScanPrefix` already returns `([]KV, error)`, nothing
   about adding a `ScanVisible` forces the conversion: a missed
   conversion compiles and ships.
5. **`mvcc.Store` has no ordered key index.** `chains` is a bare
   `map[string][]Version`; the only sorted view is `Export()`, which
   materializes and sorts the entire key set. A design that says
   "walk keys in sorted order" without supplying ordered access
   re-sorts the whole keyspace on every call.

A sixth fact makes a naive GC actively dangerous rather than merely
nondeterministic. If version `c` of key `K` is reclaimed because some
`c'` exists with `c < c' <= W`, a transaction with
`StartSeq = S` where `c <= S < c'` is entitled to see `c`. After
reclamation, `Store.Visible(K, S)` returns the newest surviving
version at or below `S` — some older `c''`, or nothing. That is not a
missing row or an error: it is a **silent Snapshot Isolation
violation**, strictly worse than an abort, and directly contrary to
`ISOLATION TRUTHFULNESS`.

## Decision

**GC is replicated state.**

> The GC watermark is advanced only by a committed Raft log entry, and
> all reclamation happens inside `fsm.Apply` as a pure, deterministic
> function of that entry and prior FSM state.

The nondeterministic inputs (which transactions are open, how much
disk is free, what time it is) are computed **outside** the
determinism boundary, on the leader, and enter the state machine as
explicit command fields — the escape hatch `DETERMINISM BOUNDARY`
already specifies, and the identical pattern
`SetClusterVersionCommand` already uses for `FinalizeUpgrade`.

Concretely:

- **New FSM control command** `AdvanceGCWatermarkCommand{RequestID,
  Watermark, MaxVersions, MaxKeys}`, control kind byte `2` after
  `controlKindSetClusterVersion = 1`, with the same bounded-decoding
  discipline as `DecodeSetClusterVersion` and the same
  kind-range-collision guard.
- **New FSM state**: `gcWatermark`, `gcCursor`, `gcPasses`, and
  `gcPassSeq`, all in a generation-3-gated additive trailing block.
- **The watermark the leader proposes** is
  `min(minLiveReadLeaseStartSeq, appliedCommitSeq,
  appliedCommitSeq - gcMinRetainSeqs)`. The first two terms are
  `docs/mvcc.md` §6's rule verbatim; the third is an added lag floor.
- **The reclamation predicate is `docs/mvcc.md` §6's, unchanged**: a
  version `c` of `K` is removed iff some `c'` exists with
  `c < c' <= W`; the newest version of `K` — tombstone or not — is
  never removed.
- **`W` is the post-`max()` `f.gcWatermark`, never `cmd.Watermark`.**
  Both are deterministic, so this is not a determinism question; it
  is a question of how many horizons exist in the system, and the
  answer must be one. `f.gcWatermark` is what the read guard refuses
  below, what the commit-side check uses, and what the snapshot
  encodes. Reclaiming against a lower `cmd.Watermark` would make
  deletion and visibility two numbers that only usually agree, and a
  replayed lower watermark would be a no-op in one sense but not the
  other.
- **Apply is bounded and resumable in *two* dimensions**: keys are
  walked in sorted order from a persisted `gcCursor`, examining at
  most `MaxKeys` keys and removing at most `MaxVersions` versions per
  command, then advancing the cursor. Bounding removals alone leaves
  the walk `O(total keys)` whenever little is reclaimable — 10M
  singly-written keys would mean a full sorted walk per command, on
  every replica, reclaiming nothing. Both bounds are carried **in the
  command**, not read from local config, so two nodes with different
  flag values still do byte-identical work.
- **`mvcc.Store` gains ordered key access** (`KeysFrom(cursor,
  limit)`) whose cost is independent of the total key count. A
  per-Apply full sort is explicitly forbidden and is what SL-2b's
  negative control asserts against. The key set never shrinks under
  GC — the newest version of a key is never removed — so the index
  needs insertion and traversal but no deletion.
- **Continuation passes.** The leader proposes again whenever
  `gcCursor != ""`, not only when the watermark advances by
  `gc-min-advance-seqs`. Without this, reclamation is capped at
  `MaxVersions` per watermark advance, and a workload creating more
  versions than that per advance grows monotonically with GC enabled
  and correctly configured.
- **The deterministic `RequestID` carries `gcPassSeq`**, a monotone
  counter in FSM state incremented by every applied command:
  `"\x00chronicledb-gc\x00w=<W>\x00p=<gcPassSeq>"`. Keying it on
  `(W, MaxVersions)` alone — as an earlier draft did — would make a
  continuation pass at an unchanged watermark collide with its
  predecessor's `RequestID` and be discarded as a duplicate by
  `controlOutcomes`. With `gcPassSeq`, successive passes are distinct
  commands while a *retry* of the same pass stays idempotent. The
  leader keeps at most one proposal in flight so `gcPassSeq` is
  predictable from its own applied state.
- **The watermark is monotone** (`max()` on apply), so a lower value
  is a deterministic no-op and a restore can never lower it.

**The snapshot horizon is what makes `GC SAFETY` unconditional.**

The lag floor makes the leader's blind spot (transactions on a
previous leader) small; it does not make it empty. So the guard is
structural, inside `internal/mvcc` — but "the one place every read
must pass through" has to be *made* true (Context fact 4), not
assumed:

- `Store.Visible` and `Store.ScanVisible` gain an `error` return and
  refuse `startSeq < gcWatermark` with `ErrSnapshotTooOld`, checked
  under the same `RLock` that reads the chain — one atomic instant, no
  window.
- **`internal/sql`'s `mergeScan` is rewritten onto `ScanVisible`, and
  its local `visibleInChain` copy of the visibility rule is deleted.**
  `Store.Export` becomes snapshot-encoding-only, with an AST test
  asserting no package but `internal/fsm` calls it. Without this, a
  `SELECT ... LIKE 'K%'` at a `StartSeq` below the horizon returns
  the reclaimed row as *absent*, with no error — the exact silent SI
  violation this ADR exists to forbid, delivered through the
  product's primary scan path. A second implementation of the
  visibility rule outside the determinism boundary is what made the
  bypass possible, so the fix removes it rather than guarding it.
- **The guard must survive state restore.** `DecodeState` builds the
  store with `mvcc.RestoreStore` (`internal/fsm/snapshot.go:465`) and
  `handleInstallSnapshot` swaps the whole FSM in wholesale
  (`internal/node/node.go:2011`). If `SetGCWatermark` were called
  only from `fsm.Apply`, a node that restarted from a snapshot,
  installed one, or restored a backup would run with
  `f.gcWatermark = W` but a store watermark of 0 — the read guard
  silently disabled. `DecodeState` sets it; better still,
  `RestoreStore` takes it as a parameter so a restored store cannot
  be constructed without one. The binding, testable property is
  `Store.GCWatermark() == FSM.gcWatermark` at every instant on every
  node.
- `fsm.Apply` for a `CommitTxnCommand` checks the horizon **after**
  the idempotency lookup (so `REQUEST OUTCOME STABILITY` is preserved
  exactly) and **before** conflict detection, returning a new
  deterministic `StatusAbortedStale`.
- `CheckConflicts` needs no guard: it reads only the newest version,
  which GC never removes. This is asserted by test, not assumed.

The bound is exact: harm requires `c <= S < c' <= W`, hence `S < W`;
no snapshot at or above `W` can ever be harmed.

**Read leases, registered at capture.** `Node.BeginReadIndex` returns
a `*ReadLease` the caller releases at transaction end; the node keeps
the live set on its event-loop goroutine and takes the minimum for
the watermark computation.

The registration *instant* is load-bearing. `handleReadIndex`
captures `target := core.LastIndex()`
(`internal/node/node.go:1462`); `checkPendingReads` delivers it as
`StartSeq` many loop iterations later (`:1525`). **The lease is
created at capture, not at resolution.** Registering at resolution
would leave a captured-but-pending read invisible to `minLease`, so a
GC tick in that window could advance past a boundary the node had
already handed out; the horizon guard keeps that *safe*, but the
reader then gets a spurious `ErrSnapshotTooOld`. Since the registry
and the GC tick are both confined to the event-loop goroutine,
registering at capture closes the window entirely rather than
narrowing it, and costs nothing. The consequence:
`W <= minLease <= S` for every read the current leader has captured,
so a read against the current leader is never refused spuriously.

The full lifecycle — registration, release, resolution failure,
caller cancellation before or after resolution, step-down, expiry —
is tabulated in `docs/v0.6.0-plan.md` §15.3, because every one of
those paths must release or the lease leaks.

A **leaked lease stalls GC but can never make it unsafe** — the
fail-safe direction. `-read-lease-max-age` bounds the stall; it is a
liveness mechanism only, and no safety property depends on it firing.
The live-lease count is itself capped (`-max-live-read-leases`) and
`minLease` is maintained incrementally, because a lease is
client-caused per-node state that outlives its admission slot
([ADR-0019](0019-admission-control-architecture.md)).

**Standalone mode is excluded from GC in `v0.6.0`.** Standalone mode
has no cluster generation, so there is no gate to protect a `v0.5.0`
binary from a new durable record kind; and `txn.Manager.recover` calls
`fsm.DecodeCommitTxn` unconditionally on every replayed record. The
horizon guard is still compiled in, inert at watermark 0.

**Generation 3.** `internal/version.MaxSupportedGeneration` becomes
`3`, gating the command, the new status value, and the new snapshot
trailing block on **both** sides (a leader refuses to propose below
generation 3; a follower fails closed on receipt) — the two-sided gate
`v0.5.0`'s membership review established as necessary.

**GC ships disabled by default** (`-gc-interval=0`). Enabling it by
default is a `v1.0.0` gate item (`docs/enterprise-v1-plan.md` §17.1
item 3).

## Alternatives Considered

1. **Local, non-replicated GC; weaken `STATE MACHINE SAFETY` from
   "byte-identical" to "equivalent visible state".** Rejected. It is
   arguably visibility-safe, but it destroys the byte-identical
   snapshot property that `TestEncodeStateDeterministic`, the
   `internal/oracle` reference model, and the whole deterministic
   replay discipline rest on; it makes `GC SAFETY` a property every
   node must establish separately rather than one the log establishes
   once; and it gives a snapshot transferred by `InstallSnapshot` a GC
   state unrelated to the receiver's. This project's entire testing
   strategy is built on the determinism boundary; trading it to avoid
   one generation bump is a bad trade.
2. **Serve a below-horizon read best-effort from the surviving
   chain.** Rejected as unsafe: it is a silent SI violation
   (see Context), which is strictly worse than an abort and
   contradicts `ISOLATION TRUTHFULNESS`.
3. **Track active snapshots in `internal/txn`, as
   `docs/enterprise-v1-plan.md` §10 literally says.** Rejected as
   impossible for the replicated path: `internal/txn` is not in it.
   The registry moves to `internal/node` read leases; the deviation is
   recorded in `docs/v0.6.0-plan.md` §26/D2 rather than silently made.
4. **Unbounded reclamation in one Apply call.** Rejected: an
   `O(total keys)` walk on the single event-loop goroutine is exactly
   the kind of unbounded blocking the admission-control half of this
   release exists to prevent.
4a. **Bounding only the versions *removed* per Apply.** Rejected, and
   worth stating separately because an earlier draft did exactly
   this: a removal-only bound still walks the whole key set whenever
   little is reclaimable, and — given `mvcc.Store`'s bare map — sorts
   it first. Ten million singly-written keys would mean a
   full sort plus a full walk per command, on every replica,
   reclaiming nothing. That *is* alternative 4, arrived at by
   accident. `MaxKeys` plus ordered key access is what makes the
   bound real.
4b. **Proposing only when the watermark advances.** Rejected: combined
   with a per-pass removal bound, it caps reclamation at
   `MaxVersions` per advance, so a workload creating more versions
   than that per advance outruns GC permanently. Continuation passes
   at an unchanged watermark are required, which in turn requires
   `gcPassSeq` in the `RequestID` (see Decision).
5. **Reading `-gc-max-versions-per-pass` or `-gc-max-keys-per-pass`
   inside `Apply` instead of carrying them in the command.**
   Rejected: it makes Apply config-dependent, so two correctly
   configured nodes could legitimately diverge — the subtlest
   possible way to lose `GC DETERMINISM`.
5a. **Comparing versions against `cmd.Watermark` rather than the
   post-`max()` `f.gcWatermark`.** Rejected: both are deterministic,
   but it would create two horizons — one governing deletion, one
   governing visibility and the encoded snapshot — that agree only
   when commands arrive in order. One horizon, always.
6. **A time-based retention horizon (keep N seconds of versions).**
   Rejected: there is no wall clock inside the determinism boundary,
   and introducing one to serve GC would be a far larger concession
   than a `CommitSeq`-distance floor.
7. **Bumping `snapshot.FormatVersion` / `fsmStateVersion` for the new
   state.** Rejected as unnecessary: the existing
   `if clusterGeneration >= 2 { … }` trailing-block pattern extends to
   `>= 3` additively, a generation-2 snapshot written by `v0.6.0`
   stays byte-identical to `v0.5.0`'s, and a `v0.5.0` binary reading a
   generation-3 snapshot already fails closed on the trailing-bytes
   check.
8. **Short-circuiting a below-horizon commit in `Node.Propose` before
   proposing.** Rejected: it creates a second, local, possibly-stale
   decision path for what is supposed to be a replicated, recorded,
   stable outcome. The wasted Raft round is bounded by admission
   control.
9. **Pruning the `RequestID` outcome table as part of "GC".**
   Rejected and explicitly out of scope: it would directly weaken
   `IDEMPOTENCY` and `REQUEST OUTCOME STABILITY`. It is named as
   needing its own ADR and its own release, and the table's size is
   exported as a metric so its growth is visible rather than
   surprising.

## Consequences

- One new control-command kind, one new `Outcome` status value
  (`StatusAbortedStale`), four new FSM state fields (`gcWatermark`,
  `gcCursor`, `gcPasses`, `gcPassSeq`), and cluster generation 3. No
  new WAL record type, no new snapshot framing, no `FormatVersion`
  bump.
- `mvcc.Store.Visible` gains an `error` return — a mechanical but
  wide call-site change across `internal/txn` and `internal/sql`,
  deliberately sequenced as its own implementation slice with the
  watermark pinned at 0 so the mechanical and behavioral changes are
  never in one commit. The `mergeScan`/`Export` conversion rides in
  that slice but is **not** compile-forced (`ScanPrefix` already
  returns an `error`), so an AST test is what proves it landed.
- `mvcc.Store` gains an ordered key index and `KeysFrom`. This moves
  cost onto `ApplyCommit`'s first-write-of-a-key path; that cost is
  measured and recorded at the release gate, and if it proves
  unacceptable the answer is a different index structure, never
  dropping `MaxKeys`.
- `Node.BeginReadIndex` gains a lease return value, and the lease
  registry gains a ceiling and an `O(1)` minimum.
- **Crash safety for GC is free**: because reclamation is a pure
  function of committed history, a crash mid-Apply is covered by the
  replay machinery that already exists. GC has no durable bookkeeping
  of its own.
- Backup, restore, PITR, and `InstallSnapshot` all carry the
  watermark, cursor and `gcPassSeq` as ordinary FSM state, with no new
  format work — and each must re-establish
  `Store.GCWatermark() == FSM.gcWatermark` on the receiving side.
- `v0.6.0` additionally hardens `DecodeState` to reject unrecognized
  outcome status bytes — a pre-existing
  `NO SILENT FORMAT MISINTERPRETATION` gap the new status value made
  visible.
- **Honest limits**: the final tombstone of a deleted key is never
  reclaimed (there is no "key is fully absent" notion in
  `docs/mvcc.md` §7), and the `RequestID` outcome table still grows
  without bound. Both are documented and measured rather than quietly
  left out of the disk-usage claim. `v0.6.0` claims no globally
  bounded disk or memory anywhere.
- **A `v1.0.0` obligation this ADR creates rather than discharges.**
  `docs/enterprise-v1-plan.md` §17.1 item 3 requires a long-duration
  run proving **stable disk usage** with GC on by default. That is
  unsatisfiable while the outcome table is unbounded, so before
  `v1.0.0` either outcome-table retention lands (its own ADR — it
  touches `IDEMPOTENCY`) or §17.1 item 3's wording is formally
  rescoped. Recorded in `docs/roadmap.md` as a `v1.0.0` blocker with
  that named owner gate, not merely as a non-goal.

## Correctness Implications

- **`GC SAFETY`** (new): a version is removed only when
  `docs/mvcc.md` §6 permits it, and no transaction ever *observes* a
  removed version — the horizon guard refuses instead.
- **`GC DETERMINISM`** (new): the reclaimed set, cursor, watermark and
  `gcPassSeq` are a pure function of the applied committed prefix,
  identical on every replica and across replay, restart, and restore
  — which includes the leader's next `RequestID`, since it is derived
  from `gcPassSeq`.
- **`SNAPSHOT HORIZON ENFORCEMENT`** (new): structural refusal below
  the horizon, at the `internal/mvcc` boundary, with no bypass —
  which required closing the `Export`/`mergeScan` scan path and
  propagating the watermark through state restore, neither of which
  a guard on `Visible` alone would have covered.
- **`MVCC VISIBILITY`** is *narrowed in domain, not weakened*: it now
  applies to snapshots at or above the horizon; below it, the answer
  is an explicit error rather than a value. A refused read is not a
  wrong read.
- **`ISOLATION TRUTHFULNESS`** is *strengthened*: the horizon guard
  forbids exactly the silent weakening a naive GC would have
  introduced.
- **`STATE MACHINE SAFETY` and `DETERMINISM BOUNDARY` are preserved
  unchanged** — which is the entire reason for this ADR's central
  decision.
- **`IDEMPOTENCY` / `REQUEST OUTCOME STABILITY` preserved exactly**:
  the horizon check runs *after* the idempotency lookup, so an
  already-decided `RequestID` still returns its recorded outcome.
- **`NO SILENT FORMAT MISINTERPRETATION`** extended to control kind 2,
  status byte 3, and the generation-3 trailing block.

## Testing and Proof Obligations

`docs/v0.6.0-plan.md` §30.2 (SL-1…SL-26). In particular:

- **SL-1** (property, at the existing MVCC property-test seed
  discipline): randomized chains × randomized live-snapshot sets ×
  randomized watermarks; no surviving snapshot's required version is
  removed, and every removed-version read is refused.
- **SL-3, the negative control**: with the horizon guard disabled by a
  test-only hook, the same property test must *detect* the silent
  stale read. A GC test that passes with and without the guard proves
  nothing (`docs/testing-strategy.md` §11).
- **SL-4/SL-12/SL-13**: byte-identical `EncodeState` across
  independently constructed FSMs, across every live node under a
  chaos schedule with GC active, and between a node that caught up by
  log and one that caught up by `InstallSnapshot`.
- **SL-5**: the idempotency → horizon → conflict ordering, and that
  `CheckConflicts` is unaffected by GC.
- **SL-6**: every committed-read **entry point the product actually
  has** — `replicatedTxn.Get`/`ScanPrefix`,
  `standaloneTxn.Get`/`ScanPrefix`, plus every exported `mvcc.Store`
  read method — driven below the horizon and required to refuse.
  Scoped to *paths*, not to `Store`'s method set: a method-set test
  passes happily on an unused `ScanVisible` while `mergeScan` still
  reads `Export`, which is precisely the bug it would need to catch.
  **SL-6b** (AST) asserts `Export` has no caller outside
  `internal/fsm`; **SL-6a** asserts
  `Store.GCWatermark() == FSM.gcWatermark` as a `DecodeState`
  post-condition.
- **SL-2b**: with `MaxKeys`/`MaxVersions` fixed,
  `ApplyAdvanceGCWatermark`'s cost is flat as total key count grows
  10×. Its negative control is a deliberate full-sort implementation,
  which must fail the assertion — otherwise the test does not
  actually forbid what alternative 4a rejects.
- **SL-4a**: a command whose `Watermark` is below the applied
  `f.gcWatermark` reclaims against the *current* watermark, pinning
  the single-horizon rule.
- **SL-23**: read-lease release on every §15.3 lifecycle path,
  including caller cancellation both before and after resolution, and
  step-down — the paths where a capture-time registration could
  otherwise leak.
- **SL-16/SL-17**: generation-2 output byte-identical to `v0.5.0`'s,
  and a real `v0.5.0` binary failing closed on a generation-3 snapshot
  and backup — proven with binaries produced by the git-worktree
  technique, not a re-implemented old encoder.
- **SL-19**: a long-running real-cluster run proving
  `chronicledb_mvcc_versions` stabilizes over a bounded key set —
  scoped to the claim GC can actually support, not to "total disk
  usage stabilizes", which the outcome table makes false.
- **Declared proof-tier substitution**: `internal/fault` imports only
  `internal/raft` — no FSM, no `Node`, no WAL, no snapshot manager —
  so no GC scenario can run there. SL-2 stays a pure `internal/fsm`
  property test and SL-12's GC chaos runs at the `internal/node`
  `testCluster` tier (real disk, real TCP, deterministic action
  schedule). The same limitation retiers AC-6/AC-7 and SL-8; all four
  substitutions are enumerated in `docs/v0.6.0-plan.md` §29.3 and
  recorded as deviation D10, rather than one being declared and the
  rest assumed.
