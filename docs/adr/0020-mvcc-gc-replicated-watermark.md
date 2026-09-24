# ADR-0020: MVCC GC as Replicated State (a Watermark, Not a Background Sweep)

Status: Accepted

## Context

Through `v0.5.0`, `internal/mvcc.Store` retained every version ever
committed for every key, forever: `docs/mvcc.md` §6 named the eventual
reclamation rule but explicitly deferred implementing it. Measured
against the actual tree, the thing that grows without bound in
ChronicleDB is not a file — it is FSM state in RAM (`mvcc.Store.chains`),
which then makes every successive snapshot larger. `docs/enterprise-v1-plan.md`
§10 ("Storage Lifecycle") is the designated resolution point, released
as the second half of `v0.6.0` (`ADR-0019` records the first half,
Admission Control). This ADR records the single central design decision
of that half: **how** reclamation happens, and why it must be
replicated state rather than a local background process.

## The problem, stated exactly

A version is safe to reclaim once no live transaction can legally read
it — i.e., once its `CommitSeq` is behind every currently-open
transaction's snapshot `StartSeq` (`docs/mvcc.md` §6's rule, unchanged
by this decision). In a single-node system, "every currently-open
transaction" is locally observable. In a replicated system, it is not:
a read on the leader and a read on a lagging follower can both be
legally open against different, older snapshots at the same wall-clock
instant, and only the leader ever knows about a client's read lease at
all — a follower has no independent way to know what a live read on
some other node still needs.

## Decision: GC is replicated state

Reclamation is driven by a single, Raft-replicated value — the **GC
watermark** — proposed only by the current leader, applied
identically, deterministically, and only inside `fsm.Apply`, by every
replica:

1. **The leader decides.** `internal/node`'s leader-only GC proposer
   (`maybeProposeGC`, `internal/node/node.go`) computes
   `W = min(minLease, appliedIndex, appliedIndex - gcMinRetainSeqs)`
   on a fixed cadence (`-gc-interval`) and whenever disk/heap pressure
   enters LowSpace or Critical (`ADR-0019`). `minLease` is the floor of
   every currently-live read lease this leader itself has registered
   (`§15.3`, below) — the leader-local mechanism that answers the
   question a single-node system could answer locally.
2. **The mutation is replicated and deterministic.** The leader
   proposes `AdvanceGCWatermarkCommand{Watermark, MaxVersions,
   MaxKeys}` through the ordinary Raft log — the *exact* same
   `Propose`-shaped, event-loop-dispatched path every other command
   uses. `fsm.ApplyAdvanceGCWatermark` is a pure function of the
   command and prior FSM state, called once, in order, on every
   replica, live or replayed: `f.gcWatermark = max(f.gcWatermark,
   cmd.Watermark)` (monotone — a replayed or out-of-order lower value
   is a deterministic no-op), then a bounded walk of at most
   `MaxKeys` keys reclaiming at most `MaxVersions` versions, resuming
   from `f.gcCursor` on the next pass if the keyspace was not fully
   covered.
3. **The horizon is enforced everywhere, unconditionally.** Once
   `f.gcWatermark` advances, `internal/mvcc.Store` refuses any read at
   a `startSeq` below it (`StatusAbortedStale`) — not just on the
   leader, and not just for reads that happen to race a GC pass: every
   read path the product has (`replicatedTxn.Get`/`ScanPrefix`,
   `standaloneTxn.Get`/`ScanPrefix`, and every exported `mvcc.Store`
   read method) goes through the identical horizon check, proven by a
   dedicated per-entry-point test (`SL-6`) rather than a method-set-only
   test that a bypass like the deleted `visibleInChain` could pass
   silently.

This makes `GC SAFETY` (`§27.5`) unconditional rather than "true as
long as every reader also independently enforces the horizon
correctly": the horizon guard lives structurally inside `internal/mvcc`,
the one place every read path must pass through regardless of which
package calls it.

### Why the lag floor (`-gc-min-retain-seqs`) is required, not optional

`W = min(..., appliedIndex - gcMinRetainSeqs)` bounds how close the
watermark may approach the leader's own most recent commit. Without
this floor, a watermark computed purely from `minLease`/`appliedIndex`
could advance past a read lease that has not yet been registered —
`BeginReadIndex` registers its lease *at capture*, before the read
itself resolves (`§15.3`'s own "resolved: the lease is registered at
capture, not at resolution" decision), but the window between a client
deciding to read and that lease actually landing on the leader's event
loop is real, non-zero wall-clock time. The lag floor is what makes
that window safe by construction rather than by timing luck.

### Rejected alternative: local, non-replicated GC

The straightforward alternative — each node locally decides what it can
safely forget, based on its own local read activity — was rejected for
the reason stated in "The problem, stated exactly" above: a follower
has no way to know about a lease live only on the leader (or on a
*different* follower, if reads were ever served there). A locally-decided
GC boundary is either unsafe (it can reclaim a version some other node's
live read still needs) or maximally conservative to the point of never
reclaiming anything a replicated system could safely reclaim — neither
outcome satisfies `enterprise-v1-plan.md` §10.

### `DETERMINISM BOUNDARY`: the decision is local, the mutation is not

`docs/enterprise-v1-plan.md` §10 describes GC as "a background,
low-priority process... consuming `internal/admission`'s resource-pressure
signals." A background goroutine mutating `internal/mvcc` outside
`fsm.Apply` would violate this codebase's `DETERMINISM BOUNDARY`
invariant (state-machine mutation must be a pure function of committed
history, applied identically and only inside `Apply`). This decision
keeps both halves of §10's sentence true by splitting them: the
*decision* (when, and to what watermark) is a background,
pressure-aware, leader-local process; the *mutation* (which versions
actually disappear) happens only inside `Apply`. A leader that crashes
mid-decision loses nothing durable — nothing was mutated yet — and a
new leader simply starts deciding again from its own local view.

### Bounded, resumable, deterministic Apply

`AdvanceGCWatermarkCommand` carries `MaxVersions` (versions removed) and
`MaxKeys` (keys examined) as part of the replicated command itself —
not read from each replica's own local flag — so every replica performs
byte-identical work regardless of its own `-gc-max-versions-per-pass`/
`-gc-max-keys-per-pass` configuration (`GC DETERMINISM`, `§27.6`:
independently-constructed FSMs driven through the identical command
history produce byte-identical `EncodeState` output). `MaxKeys` exists
specifically so an `Apply` that reclaims nothing still costs
`O(MaxKeys)`, never `O(total keys)` — without it, a mostly-reclaimed
keyspace would force every pass back to a full linear scan to find the
few keys still worth visiting. `f.gcCursor` resumes an unfinished pass
at the *current* watermark on the next Apply, rather than restarting
from the beginning, so a full keyspace walk is eventually completed
across successive passes without ever doing more than `MaxKeys`/
`MaxVersions` work in any single one.

### Why standalone mode is excluded

`internal/txn`'s standalone (non-replicated) path has no leader, no
Raft log, and no read-lease registry — the entire mechanism this
decision depends on. GC is deliberately not implemented for standalone
mode in `v0.6.0` (`§14.6`): version chains still grow unbounded there,
exactly as they did before this release, and `docs/storage-lifecycle.md`
states this plainly rather than silently.

## Alternatives Considered

- **Local, non-replicated GC.** Rejected — see above.
- **A dedicated GC coordination protocol** (a side-channel gossip of
  every node's local read-activity floor). Rejected: reuses none of the
  already-proven Raft/FSM machinery, and introduces an entirely new
  class of "is this coordination protocol itself correct under
  partition/leader-change" proof burden this release has no budget for.
- **Reclaiming a fully-tombstoned key's very last version.** Deferred,
  not rejected outright: `v0.6.0`'s reclamation predicate (`docs/mvcc.md`
  §6, unchanged) never removes a key's newest version regardless of
  whether it is a tombstone — doing so would make a key's prior
  existence unobservable in a way this release does not attempt to
  reason about. Named explicitly in `docs/mvcc.md` §9 as an open
  question for a future release, not silently absent.

## Consequences

- `enterprise-v1-plan.md` §10's literal "storage-layer file-space
  reclamation for fully-superseded MVCC data" does not apply as
  written: there is no MVCC file to reclaim space from. GC reclaims
  memory immediately (the moment `Apply` runs) and disk only at the
  *next* snapshot boundary (the smaller reclaimed state simply produces
  a smaller snapshot next time one is taken) — `docs/storage-lifecycle.md`
  §1 states this as the corrected, accurate claim.
- GC ships **disabled by default** (`-gc-interval=0`): a `v0.6.0` node
  started with no new flags proposes zero `AdvanceGCWatermark` commands
  and is byte-identical in behavior to `v0.5.0` (release gate 9).
  Enabling it by default is named as a deferred `v1.0.0` item
  (`docs/roadmap.md`), not silently assumed safe to flip.
- Crash safety for GC is inherited, not reimplemented: because
  reclamation is a pure function of committed history, a crash mid-Apply
  either committed the entry (replay re-derives the identical state) or
  did not (nothing happened) — the identical guarantee every other
  committed entry already has, with no new durable bookkeeping of GC's
  own.

## Correctness Implications

See `docs/invariants.md`'s new "Admission Control / Storage Lifecycle
invariants (`v0.6.0`)" section: `GC SAFETY` (§27.5), `GC DETERMINISM`
(§27.6), `SNAPSHOT HORIZON ENFORCEMENT` (§27.7), and `RECLAMATION
BOUNDARY` (§27.8) are this ADR's four correctness claims. `docs/mvcc.md`
§9 records this decision in that document's own terms, alongside the
final-tombstone deferral named above.

## Testing and Proof Obligations

- Property (`internal/mvcc`, `internal/fsm`): `SL-1` (randomized
  chains/live-snapshots/watermarks — no surviving snapshot's required
  version removed, every below-horizon read refused), `SL-2`/`SL-2b`
  (bounded work in both dimensions, cursor correctness, flat Apply cost
  under key-count growth), `SL-3` (negative control: the horizon guard
  disabled via `SetSkipHorizonGuardForTest` — the oracle must detect the
  resulting silent stale read).
- Unit: `SL-4`/`SL-4a` (byte-identical `EncodeState` across
  independently-constructed FSMs; a watermark command below the
  currently-applied value is a deterministic no-op), `SL-5` (commit
  ordering: idempotency, then horizon, then conflict), `SL-6`/`SL-6a`/
  `SL-6b` (every read entry point refuses correctly; `Store.GCWatermark()
  == FSM.gcWatermark` as a decode postcondition; an AST test asserting
  `mvcc.Store.Export` is reachable only from `internal/fsm`).
- Real-cluster (`internal/node` `testCluster`): `SL-7` (a transaction
  held across a watermark advance aborts as stale, on every replica),
  `SL-13` (one follower catching up by log and another by
  `InstallSnapshot`, across a GC advance and at least one continuation
  pass, converge byte-identically), `SL-23` (read-lease lifecycle,
  every exit path, no leak, `-race` clean).
- Chaos (`SL-12`, this release's own combined-schedule extension,
  `internal/node/sl12_gc_chaos_test.go`): GC actively reclaiming through
  simultaneous crash/restart and partition/heal disruption of two
  different followers — every live node converges to byte-identical
  `EncodeState` and equal GC watermark at every checkpoint, and
  `Store.GCWatermark() == FSM.gcWatermark` holds throughout, not just at
  rest.
- Compatibility: `SL-16` (a `v0.6.0` generation-2 snapshot is
  byte-identical to a committed `v0.5.0` fixture), `SL-17` (a real,
  independently-built `v0.5.0` binary fails closed opening a
  generation-3 snapshot and restoring a generation-3 backup —
  `cmd/chronicledb-node/sl17_mixed_binary_test.go`).
- Backup/restore (`SL-18`, `cmd/chronicledb-node/sl18_test.go`): a
  generation-3 backup taken mid-GC restores its watermark exactly, with
  and without `-restore-until`, and the horizon guard is correct
  immediately on the restored node.
- Long-running (`SL-19`, `cmd/chronicledb-node/sl19_test.go`, scaled via
  `CHRONICLEDB_SL19_DURATION`): continuous load over a bounded key set
  with GC enabled — `chronicledb_mvcc_versions` stabilizes while
  `chronicledb_requestid_outcomes` is reported honestly as the separate,
  genuinely unbounded number it is (`§28.2`/D6, `docs/mvcc.md` §9).
