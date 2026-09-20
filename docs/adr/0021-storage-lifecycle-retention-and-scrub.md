# ADR-0021: Storage Lifecycle — Retention, Disk-Pressure Semantics, and Integrity Scrub

Status: **Proposed** (planning only — `docs/v0.6.0-plan.md` Part B; no
production code implements this yet, and this ADR moves to Accepted
only when that implementation lands)

## Context

This ADR covers the storage-lifecycle work of `v0.6.0` that is *not*
MVCC garbage collection (which is [ADR-0020](0020-mvcc-gc-replicated-watermark.md)):
what a node retains on disk, when it is safe to delete, what happens
as the disk fills, what happens when `fsync` fails, and how an
operator proactively verifies integrity.

State of the tree at `481ff93`:

- **WAL retention already works and is already safe.**
  `maybeSnapshot` performs a fixed six-step sequence — create snapshot
  durably, append the durable metadata pointer, compact `Core` and the
  in-memory mirror, re-affirm `HardState` into the current segment,
  then `CompactBefore` — and `CompactBefore` deletes whole segments
  only, never the current one, each deletion its own fsync'd directory
  operation. A crash anywhere in that sequence leaves a valid prefix.
  This is `LOG COMPACTION SAFETY` working as designed, and `v0.6.0`
  must not disturb it.
- **Snapshot retention is exactly one file**, pruned as soon as a
  newer one is durable. Meanwhile `processOutput` fills
  `MsgInstallSnapshotRequest` bytes from
  `snapMgr.Bytes(LastIncludedIndex)`; across two event-loop iterations
  a prune can remove the file that call wanted. The existing miss path
  logs and skips, and Core retries with the newer index — self-healing
  but **unmeasured**.
- **Disk-full semantics are one sentence.**
  `docs/failure-model.md` §1.9 says an out-of-space condition is
  "treated the same as a disk write failure", with proactive
  backpressure named as "a possible future enhancement". There is no
  space accounting, no `ENOSPC` classification, and no state machine.
- **fsync failure on the Raft path halts the node** via
  `Node.fail`. There is no health surface saying why, and no handling
  at all for syncs outside that path (snapshot creation, backup
  export, audit writes).
- **There is no scrub.** Corruption is detected only reactively, when
  the read or replay path happens to encounter it.

## Decision

**1. The reclamation boundary is named, and the ordering is frozen.**

> `ReclaimBoundary` = the `LastIncludedIndex` of the most recent
> snapshot that is (a) fully written and fsync'd, (b) recorded in
> durable WAL metadata, and (c) whose `HardState` has been re-affirmed
> into the current segment.

Nothing at or below it is needed by any future recovery of this node;
nothing above it may ever be deleted. `maybeSnapshot`'s six-step
ordering is preserved step for step, and a call-order test is added so
a future refactor cannot silently permute it. This is
`LOG COMPACTION SAFETY` restated at the exact ordering level, as the
new invariant `RECLAMATION BOUNDARY`, because `v0.6.0` adds retention
knobs that touch the same code.

**2. Retention becomes configurable, with defaults that change
nothing.**

- `-snapshot-retain-count` (default `1`): `Manager.pruneExcept`
  becomes `pruneKeepingNewest(n, keepIndex)`, still pruning only after
  the new snapshot is confirmed durable. `Manager.Load`'s existing
  newest-first fallback search already handles `n > 1` unchanged — its
  doc comment anticipates exactly this.
- `-wal-retain-extra-segments` (default `0`):
  `WAL.CompactBeforeRetaining(uptoIndex, n)` stops once `n` otherwise
  eligible segments remain, letting a lagging follower catch up by log
  instead of by a full `InstallSnapshot` (one of the most
  event-loop-expensive things a leader can be asked to do). Retaining
  *more* than required is always safe; only the stopping point moves,
  never the eligibility computation.
- The snapshot-serve miss is **measured**
  (`chronicledb_snapshot_serve_miss_total`), with a release-gate
  requirement that it be zero across the real-process suite and a
  negative-control test proving it can become nonzero.

**3. Disk pressure is a three-state machine that tightens admission
and never deletes anything.**

`Healthy → LowSpace → Critical`, driven by a sampled free-space
figure, with 10% hysteresis on de-escalation to prevent flapping
(each transition is an audited health event). `LowSpace` reduces the
client-write concurrency ceiling; `Critical` refuses client writes
with the distinct `disk_critical` reason. **Client reads, admin
actions, and all consensus traffic are unaffected in every state** — a
follower under `Critical` still appends and still votes, because
converting a local resource condition into a quorum-availability
failure would be strictly worse than running out of space.

Entering `LowSpace` or `Critical` triggers one immediate GC pass
evaluation. That is the only automatic action pressure takes, and it
deletes only what `docs/mvcc.md` §6's predicate permits. Stated as a
binding sentence in the operator doc:

> Disk pressure never deletes anything. It tightens admission and it
> asks GC to run. There is no code path in `v0.6.0` in which low disk
> space causes any datum a legal reader could still see to be removed.

**4. `ENOSPC` is classified, not flattened.**

`internal/wal` and `internal/storage` detect `syscall.ENOSPC` with
`errors.Is` and wrap it as `wal.ErrOutOfSpace` /
`storage.ErrOutOfSpace`, so the caller, `/health`, `/status`, the
audit log, and the client-facing error can all distinguish "out of
space" from "I/O error" from "permission denied". There is no "stuck
forever" state: the next successful poll above the hysteresis
threshold restores the previous state automatically, with no operator
action and no restart.

**5. fsync failure: the Raft path is unchanged; other paths get a
threshold.**

A failure in `raft.ApplyPersistRequest` still calls `Node.fail` and
halts the node — a `v0.5.0` guarantee, deliberately preserved. What
`v0.6.0` adds happens *before* the halt: classify the error, write the
audit record, set `/health` to not-ready, and increment a
path-labelled counter, so the operator learns why rather than finding
a stopped process.

Syncs outside that path (snapshot creation, backup export, audit
writes) increment a consecutive-failure counter; at
`-fsync-failure-threshold` (default 3) the node becomes
storage-unhealthy: not *ready*, but still *live* and still
participating in Raft. A node that can still vote is more useful to
the cluster than one that has removed itself.

**6. Scrub is read-only by construction, and never repairs.**

`POST /admin/storage/scrub`, admin-role-only and audited, runs on its
own goroutine (never the event loop), holds a **maintenance**-lane
slot (Lane A2 — never a control-lane slot, see
[ADR-0019](0019-admission-control-architecture.md)'s Rule CP-3) plus
a single-slot scrub lock, and is I/O-rate-limited by
`-scrub-bytes-per-sec`. The lane split exists because of this
operation specifically: a 200 GiB scrub at the default rate runs ~50
minutes, and if it shared the control lane's capacity-2 gate with a
concurrent backup, an emergency `/admin/membership/remove` would be
refused for that whole window. It validates every WAL segment's framing,
checksums, versions and index monotonicity; every retained snapshot
via `snapshot.Decode`; and the audit log's hash chain.

Non-destructiveness is **structural, not procedural**:
`storage.OpenSegmentReadOnly` opens with `os.O_RDONLY` and returns a
`*Segment` whose `Append`, `Sync`, and `Truncate` return
`ErrReadOnlySegment`; scrub uses only that constructor, enforced by an
AST test. A torn tail in the current segment is **not** a finding — it
is legal per `docs/wal.md` §6, and reporting it would train operators
to ignore scrub output. Files that disappear mid-walk (compacted away
concurrently) are skipped, not reported.

**7. The crash matrix covers every point at which durable state is
created, published, deleted, or adopted.**

`docs/v0.6.0-plan.md` §22 enumerates them. Beyond `maybeSnapshot`'s
six ordering steps and GC's replay-covered Apply, two families were
missing from an earlier draft and are now explicit, because §27.8's
`RECLAMATION BOUNDARY` names `WALStorage.InstallSnapshot` in its
scope and because ADR-0020 makes snapshot installation the point at
which the *GC horizon* — not just the data — is adopted:

- **Backup export and PITR restore.** `Node.Backup` never mutates
  this node's retained WAL or snapshot state, never advances the
  durable pointer, and never interacts with `maybeSnapshot`'s
  threshold, so a crash mid-export leaves node recovery identical to
  the crash-free path. The *partial backup* must be unusable rather
  than silently truncated: the manifest is written last and carries
  per-file checksums, so `Restore` fails closed. Restore writes into
  a fresh target directory and is never an in-place mutation of a
  live node. Required invariant: `DURABILITY` and
  `BACKUP CONSISTENCY`, unchanged. Proof tier: real OS process
  (SL-25) — partial files and fsync ordering are not honestly
  modelled in a single-process harness.
- **`handleInstallSnapshot`, at three points**: before
  `snapMgr.Install` completes (temp file only; `NewManager` clears
  `tmp/*`, the durable pointer never moved); after
  `storage.InstallSnapshot` but before the FSM swap (the pointer
  names the new boundary, and restart rebuilds the FSM — including
  the GC fields and the `Store.SetGCWatermark` call — from exactly
  that snapshot); and after the swap but before the generation adopt
  (also re-derived from the installed file). Required invariants:
  `SNAPSHOT SAFETY`, `RECLAMATION BOUNDARY`, and ADR-0020's
  `Store.GCWatermark() == FSM.gcWatermark`. Proof tier:
  `internal/node` `testCluster` with the new injection facility
  (SL-26).

## Alternatives Considered

1. **Time-based WAL retention ("keep 24 hours of log").** Rejected:
   there is no wall clock in the durable layer, and introducing one to
   serve a retention policy would be a large concession for a small
   operational convenience. Segment counts are the natural unit here.
2. **Partial-segment reclamation (rewrite a segment to drop old
   entries).** Rejected, for the same reason
   [ADR-0011](0011-snapshot-and-log-compaction-model.md) already
   rejected in-place truncation: more complex, and it replaces an
   atomic whole-file deletion with a rewrite that has its own crash
   window.
3. **Compacting before recording the durable snapshot pointer**, to
   shorten the window in which extra segments exist. Rejected
   outright: it is precisely the ordering inversion
   `LOG COMPACTION SAFETY` exists to forbid.
4. **Making disk pressure evict data.** Rejected as a violation of the
   project's standing rule that V1 never deletes data a legal read
   could still need. Pressure tightens admission; only GC deletes, and
   only under `docs/mvcc.md` §6's predicate.
5. **Gating Raft/follower traffic under disk pressure.** Rejected: it
   converts a local condition into a cluster-availability failure and
   would jeopardize `QUORUM SAFETY` and `LEADER COMPLETENESS`. If the
   disk is genuinely full, the append fails and the existing explicit
   `DURABILITY` failure path is the correct outcome.
6. **No hysteresis on state de-escalation.** Rejected: a workload
   hovering at the threshold would flap every poll, and each
   transition writes an audit record and changes `/health`.
7. **Treating an fsync failure on the Raft path as countable and
   tolerable, like the others.** Rejected: that would weaken a
   `v0.5.0` guarantee. The threshold applies only to paths that do not
   already halt the node.
8. **Self-terminating on storage-unhealthy.** Rejected: a node that
   can still vote helps the cluster; removing itself does not. It
   stops being ready, which is what operator automation keys on.
9. **Letting scrub repair what it finds** (truncate a bad tail,
   quarantine a segment). Rejected under `RECOVERY NON-INVENTION` and
   `docs/recovery.md` §4: scrub reports, the operator acts. There is
   no `?repair=true`.
10. **Running scrub on the event loop for consistency with backup.**
    Rejected: scrub is pure file reading, needs no `FSM.mu` and no
    `Core`, and putting it on the loop would create a long blocking
    window for no benefit — the opposite of what the admission half of
    this release is trying to achieve.
11. **A periodic scrub timer in `v0.6.0`.** Deferred to
    `docs/enterprise-v1-plan.md` §11 (Operations). Shipping the
    endpoint alone is the conservative choice; scheduling is a
    packaging concern.
12. **Using `golang.org/x/sys` for `Statfs`.** Rejected: it is an
    external dependency, and `docs/dependencies.md`'s
    zero-external-dependency policy holds. `syscall.Statfs` is
    standard library, behind a `unix` build tag, with an explicit
    unsupported stub elsewhere and a startup refusal if a
    disk-pressure flag is set on a platform that cannot honor it.

## Consequences

- Four new WAL/snapshot/storage APIs (`CompactBeforeRetaining`,
  `SetRetainCount`, `DiskUsage`, `OpenSegmentReadOnly`) and one new
  admin endpoint pair (`/admin/storage/scrub`,
  `/admin/storage/status`), plus an `authz` endpoint identifier and
  RBAC row for each.
- A new test-only crash-injection facility in `internal/node`
  (build-tag gated), because SL-8 and SL-26 have no existing harness
  that can halt the process between two steps of `maybeSnapshot` or
  `handleInstallSnapshot`. It is a budgeted implementation slice, not
  an assumed capability.
- Scrub and backup occupy a **maintenance** lane distinct from the
  control lane, so this release's own long-running operations cannot
  deny service to membership, upgrade or TLS-reload actions.
- Disk-pressure admission is Linux/Darwin-only by build tag and tested
  only on Linux amd64; `docs/support-matrix.md` gains a row saying so,
  and `/status` reports `unsupported` rather than implying protection
  exists.
- `/health` gains a ready/live distinction: `Critical` or
  storage-unhealthy means not-ready but still live. This is the
  Kubernetes-shaped surface `docs/enterprise-v1-plan.md` §12 will
  later consume.
- Every retention default is "exactly `v0.5.0`", so a `v0.6.0` node
  started with no new flags behaves identically on disk.
- `docs/failure-model.md` §1.9's "possible future enhancement"
  language is replaced with an implemented, tested state machine.

## Correctness Implications

- **`RECLAMATION BOUNDARY`** (new): the ordering constraint stated
  explicitly, with a call-order test and crash injection at each of
  the six steps.
- **`DISK-FULL EXPLICITNESS`** (new): an out-of-space condition is
  always distinguishable from a conflict, a generic I/O error, and an
  admission rejection — and never silently successful.
- **`SCRUB NON-DESTRUCTIVE`** (new): a property of the file
  descriptor, not of reviewer discipline.
- **`LOG COMPACTION SAFETY`, `SNAPSHOT SAFETY`, `DURABILITY`,
  `RECOVERY NON-INVENTION` are preserved unchanged.** Retention knobs
  can only cause *more* history to be retained, never less; the
  snapshot-durable-before-compaction ordering is untouched; the Raft
  fsync-failure path behaves exactly as at `v0.5.0`.
- **`AUDIT COMPLETENESS`** is extended: every storage-health state
  transition is an audit record, because these are facts an operator
  must be able to reconstruct after the event.
- Scrub reads potentially sensitive content, so it is admin-gated and
  audited (`NO UNAUTHENTICATED ADMIN ACTION`), even though it reports
  only locations, never values.

## Testing and Proof Obligations

`docs/v0.6.0-plan.md` §30.2 and §30.3. In particular:

- **SL-8**: crash injected at each of the six ordering steps, restart,
  assert recoverable and no committed entry lost. **Tier correction:**
  this runs at the `internal/node` `testCluster` tier using a new,
  purpose-built crash-injection facility (`docs/v0.6.0-plan.md` §33
  slice 10a), **not** in `internal/fault`. That harness imports only
  `internal/raft`; its `MemoryStorage` is a `raft.Storage` fake whose
  injectors are `FailNextAppends`, `FailNextSetHardState` and
  `FailNextTruncate`, and it contains no snapshot manager, no
  `internal/wal` and no `internal/storage` — so it cannot reach
  `snapMgr.Create`, `AppendMetadataSnapshot`, `Reaffirm` or
  `CompactBefore`. Nothing in `internal/node` can halt between two
  steps of `maybeSnapshot` today either, which is why the facility is
  a budgeted slice rather than an assumed capability. Recorded as
  deviation D10.
- **SL-26**: the same facility applied to the three
  `handleInstallSnapshot` crash points (`docs/v0.6.0-plan.md` §22):
  before `snapMgr.Install` completes, after `storage.InstallSnapshot`
  but before the FSM swap, and after the swap but before the
  generation adopt. Assert recovery, byte-identical convergence, and
  `Store.GCWatermark() == FSM.gcWatermark` at every restart.
- **SL-25**: `SIGKILL` during `/admin/backup` and during a PITR
  restore, on real OS processes — a partial-file/fsync-ordering fact
  a single-process harness cannot model honestly. Assert the node
  restarts clean with an unchanged data-directory hash (backup never
  mutates node state), and that the partial backup fails `Restore`
  closed on its manifest rather than restoring a truncated base.
- **SL-24**: a call-order assertion on `maybeSnapshot`, so the
  ordering cannot be silently permuted by a later refactor.
- **SL-9**: `-snapshot-retain-count=0` and a negative
  `-wal-retain-extra-segments` refuse startup.
- **SL-10**, with two negative controls: scrub detects every case the
  existing WAL/snapshot corruption-injection fixtures already cover;
  it produces **zero** findings on a clean tree **and** on a freshly
  torn tail; and the data directory's hash is byte-identical before
  and after. A scrub that cannot do all three is not calibrated.
- **SL-11**: a forced snapshot-serve miss proves the counter can
  become nonzero, so its zero value elsewhere means something.
- **AC-22** (shared with [ADR-0019](0019-admission-control-architecture.md)):
  a concurrent backup and scrub must not delay a membership change —
  the proof that the Lane A1/A2 split is real.
- **SL-14/SL-15**: real `ENOSPC` on a real small filesystem, on both
  the Raft path and a snapshot write — the classified error reaches
  the client and `/health`, is never reported as an SI abort, and the
  node recovers automatically once space is freed, with no restart.
- **AC-13** (shared with [ADR-0019](0019-admission-control-architecture.md)):
  admission tightens, then refuses, *before* any real write failure.
- **SL-22**: `TestScrubOnlyOpensReadOnly`, an AST test making
  read-only access a property the code cannot lose.
