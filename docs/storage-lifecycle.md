# Storage Lifecycle

Status: `v0.6.0` (`docs/enterprise-v1-plan.md` §10). This is the
operator guide; `ADR-0020` (MVCC GC) and `ADR-0021` (retention,
disk-full handling, scrub) record why the architecture is shaped this
way.

## 1. What actually consumes disk

| Consumer | Grows with | Reclaimed by | Bounded? |
|---|---|---|---|
| WAL segment files | every appended log entry/HardState record | `WAL.CompactBeforeRetaining` after a durable snapshot | Yes — bounded by `-snapshot-threshold` plus the current segment plus `-wal-retain-extra-segments` |
| Snapshot file(s) | the size of live FSM state | `Manager.Prune` | Yes — `-snapshot-retain-count` files |
| Audit log | admin actions and (as of `v0.6.0`) node-health transitions | Nothing | No — out of scope for this document (`enterprise-v1-plan.md` §11, Operations) |
| `snapshot/tmp` | in-progress writes | `NewManager` on restart | Yes |

The thing that actually grows without bound is not a file — it is FSM
state in RAM (`internal/mvcc.Store.chains`), which then makes each
successive snapshot larger. **MVCC GC reclaims memory immediately and
disk at the next snapshot boundary. It never rewrites a file, because
MVCC data has never been stored in one.**

## 2. MVCC garbage collection

### The model: a replicated watermark

GC is **disabled by default** (`-gc-interval=0`): a node started with
no new flags proposes zero `AdvanceGCWatermark` commands and is
byte-identical in behavior to `v0.5.0`.

When enabled, the current leader periodically computes a candidate
watermark `W = min(minLease, appliedIndex, appliedIndex -
gc-min-retain-seqs)` and, if it has advanced enough
(`-gc-min-advance-seqs`) or a prior pass is still unfinished, proposes
`AdvanceGCWatermarkCommand{Watermark, MaxVersions, MaxKeys}` through the
ordinary Raft log. Every replica applies the identical command
identically: `f.gcWatermark = max(f.gcWatermark, cmd.Watermark)` (never
decreases), then reclaims at most `MaxVersions` versions across at most
`MaxKeys` examined keys, resuming from a cursor on the next pass if the
keyspace was not fully covered. See `ADR-0020` for why this must be
replicated state rather than a local background sweep.

### The reclamation rule (verbatim from `docs/mvcc.md` §6)

> A version `(CommitSeq=c)` of key `K` may be removed **iff** there
> exists a version `(CommitSeq=c')` of the same `K` with `c < c' <= W`,
> where `W` is the applied GC watermark. The newest version of `K` is
> never removed. Nothing else is ever removed.

A consequence worth stating plainly: **deleting every key in the
database does not shrink the key count.** A tombstone is a version like
any other, and the newest version of a key — tombstone or not — is
never removed. One tombstone per ever-written key remains forever;
reclaiming the final tombstone of a key requires a "key is fully
absent" notion this release deliberately does not have (deferred, §7
below — measured via `chronicledb_mvcc_keys`, not silently dropped).

### The horizon guard

Once `f.gcWatermark` advances, any read or commit whose `StartSeq` is
below it is refused with `StatusAbortedStale` (HTTP `200`,
`status: "aborted_stale"`) — on every read path the product has, not
just the ones that happen to race a GC pass. This is what makes `GC
SAFETY` unconditional rather than "true as long as every reader also
happens to check correctly."

### Why the lag floor is required, not optional

The leader's `minLease` only covers transactions *this* leader knows
about — never one opened on a previous leader and still live on a
now-follower process, and never a reader whose `StartSeq` was obtained
immediately before a failover. `-gc-min-retain-seqs` makes that unsound
window quantitative and small; the horizon guard makes the property
unconditional regardless. Both are required; neither alone is
sufficient.

### Read leases

`BeginReadIndex` registers a lease **at capture** (when the read's
target index is decided), not at resolution — so `W <= minLease <= S`
holds for every read this leader has ever captured, and a read against
the current leader can never receive a spurious stale-snapshot
rejection. A lease is released on `Commit`/`Abort`/session close, on
caller cancellation, and (as a liveness backstop only, never a safety
mechanism) after `-read-lease-max-age` if a caller abandons it without
releasing. A leaked lease stalls GC — it never makes GC unsafe.

### The three rate flags are not free choices

```
reclaim_rate ≈ gc-max-versions-per-pass ÷ max(gc-interval, time to commit gc-min-advance-seqs entries)
```

`-gc-interval`, `-gc-min-advance-seqs`, and `-gc-max-versions-per-pass`
jointly cap how fast GC can reclaim. If `reclaim_rate` is below a
workload's own version-creation rate, `chronicledb_mvcc_versions` grows
monotonically **even with GC enabled and working correctly** — these
three values must be derived from a measured version-creation rate for
the actual workload, not chosen by intuition. `docs/benchmarks.md` §12
does this derivation against SL-19's own measured workload (108.5
versions/sec): with the `-gc-min-advance-seqs`/`-gc-max-versions-per-pass`
defaults (`256`/`4096`) and a recommended starting `-gc-interval=1s`,
`reclaim_rate ≈ 1,736 versions/sec`, a 16x margin. Re-derive this for any
workload whose sustained write rate is materially higher.
`-gc-max-keys-per-pass` is different in kind: it is a pure work bound
(any value >= 1 is safe and deterministic), not a rate bound, and is a
normal implementation-time tuning choice.

## 3. Retention flags

| Flag | Default | Meaning |
|---|---|---|
| `-gc-interval` | `0` (disabled) | How often the leader evaluates and, if warranted, proposes a watermark advance. |
| `-gc-min-retain-seqs` | `1024` | The lag floor (§2). |
| `-gc-max-versions-per-pass` | `4096` | Versions **removed** by one Apply. |
| `-gc-max-keys-per-pass` | `16384` | Keys **examined** by one Apply — without this, an Apply that reclaims nothing still costs `O(total keys)`. |
| `-gc-min-advance-seqs` | `256` | Do not propose a watermark *advance* below this much movement (does not gate a continuation pass at an unchanged watermark). |
| `-read-lease-max-age` | `10m` | Liveness bound on an abandoned lease. Never a safety mechanism. |
| `-snapshot-retain-count` | `1` | Snapshot files retained — `1` is exactly `v0.5.0` behavior. |
| `-wal-retain-extra-segments` | `0` | Whole segments retained beyond the compaction boundary, letting a lagging follower catch up by log instead of a full `InstallSnapshot` — `0` is exactly `v0.5.0` behavior. |
| `-fsync-failure-threshold` | `3` | Consecutive non-Raft-path fsync failures before storage-unhealthy (§5). |
| `-scrub-bytes-per-sec` | `64MiB` | Scrub's combined read-rate cap. `0` = unlimited. |

Raising `-snapshot-retain-count`/`-wal-retain-extra-segments` widens the
window in which a lagging follower can catch up by cheaper means, at
the cost of retaining more disk; neither is a correctness knob —
`RECLAMATION BOUNDARY` holds identically regardless of either value.

## 4. The disk-full state machine

```
Healthy  --(free < pressureThreshold)------->  LowSpace
LowSpace --(free < criticalThreshold)------->  Critical
Critical --(free >= criticalThreshold*1.1)-->  LowSpace
LowSpace --(free >= pressureThreshold*1.1)-->  Healthy
any      --(fsync/write failure)------------>  Failing   [terminal for this process, Raft path only]
```

| State | Client writes | Client reads | Admin | Raft/consensus | GC |
|---|---|---|---|---|---|
| Healthy | Full | Full | Full | Full | Scheduled |
| LowSpace | Tightened (`docs/admission-control.md` §5) | Full | Full | Full, ungated | Triggered immediately |
| Critical | Rejected, `disk_critical` | Full | Full | Full, ungated | Triggered immediately |
| Failing | The node has already halted, or is not-ready | — | — | — | — |

> **Disk pressure never deletes anything. It tightens admission and it
> asks GC to run. GC deletes only what `docs/mvcc.md` §6's rule
> permits. There is no code path in `v0.6.0` in which low disk space
> causes any datum a legal reader could still see to be removed.**

An actual `ENOSPC` is unchanged from `v0.5.0`: the write fails
explicitly, no acknowledgment is given, `DURABILITY` holds. `v0.6.0`
adds exactly two things on top: **classification** —
`internal/wal`/`internal/storage` wrap a `syscall.ENOSPC` (via
`errors.Is`, never string matching) into `wal.ErrOutOfSpace`/
`storage.ErrOutOfSpace`, so `/health`, `/status`, the audit log, and the
client-facing error can all distinguish "out of space" from a generic
I/O error from a permission error — and **automatic recovery**: the
next successful pressure sample above the hysteresis threshold restores
the previous state, with no operator action and no restart.

## 5. fsync-failure health

A failure persisting the Raft durable log (the consensus path) still
calls `Node.fail` — the node halts, exactly as in `v0.5.0` — but
`v0.6.0` classifies the error, writes an audit record, and increments
`chronicledb_fsync_failures_total{path="raft"}` **before** the halt, so
an operator learns why rather than finding a stopped process.

Snapshot creation and backup export are different: neither is on a
path a client's own acknowledgement depends on, and both already had
crash-safety properties tolerating abandonment mid-attempt. A failure
on either path increments a consecutive-failure counter
(`chronicledb_fsync_failures_total{path="snapshot"|"backup"|"audit"}`);
at `-fsync-failure-threshold` consecutive failures, the node marks
itself **storage-unhealthy**: `/health` reports `503` (not-ready) while
remaining alive, an audit record is written, and
`chronicledb_storage_health` becomes `1`. The node does **not**
self-terminate: it keeps voting and replicating as long as the
consensus path still works. A single subsequent success on that same
path resets the counter and clears storage-unhealthy automatically —
no restart required, the identical "no stuck-forever state" posture
disk pressure has.

## 6. Scrub

`POST /admin/storage/scrub` (admin-only, audited) reads every retained
WAL segment, every retained snapshot file, and the audit-log hash
chain, read-only, and reports every problem it finds without repairing
anything:

```json
{ "startedAt": "…", "durationMs": …, "bytesRead": …,
  "walSegmentsChecked": …, "walFramesChecked": …,
  "snapshotsChecked": …, "auditRecordsChecked": …,
  "findings": [ {"file": "…", "offset": 1234, "kind": "bad_checksum"} ],
  "truncated": false }
```

`GET /admin/storage/status` (operator+, read-only) reports the last
scrub's result.

**Finding vocabulary** (fixed): `bad_checksum`, `bad_framing`,
`unsupported_version`, `index_out_of_order`, `unreadable`,
`snapshot_decode_failed`, `audit_chain_broken`.

Scrub opens every file it reads with `internal/storage.OpenSegmentReadOnly`
— never the ordinary read-write constructor — so it is structurally
incapable of mutating anything it verifies, checked by an AST test
(`TestScrubOnlyOpensReadOnly`) rather than left to reviewer discipline.
It runs on its own goroutine, bounded by the shared Lane A2 maintenance
gate plus a dedicated single slot (so a scrub and a backup may overlap
each other, but a second concurrent scrub is refused
`admin_operation_in_progress`), rate-limited by `-scrub-bytes-per-sec`.
A torn tail in the current (still-open) WAL segment or audit segment is
legal and is never reported; the identical signature in an
already-closed file is corruption and is reported. A file removed
concurrently by ordinary compaction/pruning is skipped, not reported —
by definition, a file that disappeared mid-scrub was no longer needed.

**Scrub never repairs.** There is no `?repair=true`, no quarantine
move, no truncation. It reports; the operator acts
(`docs/recovery.md` §4).

## 7. Two honest limits this release does not fix

1. **The `RequestID` outcome table grows without bound.** Every
   distinct `RequestID` adds one entry, forever, in RAM and in every
   snapshot. GC does not touch it, and `v0.6.0` deliberately does not
   prune it: pruning would directly weaken `IDEMPOTENCY` and `REQUEST
   OUTCOME STABILITY`, guarantees this release is forbidden to weaken.
   `chronicledb_requestid_outcomes` is exported so the growth is
   visible. A workload with a unique `RequestID` per request has
   unbounded state growth **regardless of GC** — this is why
   `docs/roadmap.md`'s own long-running proof scopes its claim to
   *MVCC version count* stabilizing over a bounded key set, never to
   "total disk usage stabilizes," which this release cannot claim.
   Outcome-table retention (a bounded window, or a `RequestID`-expiry
   contract negotiated with clients) requires its own ADR and its own
   release, and is recorded as a **named `v1.0.0` blocker** in
   `docs/roadmap.md`: before `v1.0.0` is tagged, either retention lands,
   or `enterprise-v1-plan.md` §17.1 item 3's "stable disk usage" wording
   is formally rescoped. `v0.6.0` claims no globally bounded disk or
   memory anywhere in its shipped documentation.
2. **The final tombstone of a deleted key is never reclaimed** (§2,
   above). Reclaiming it needs a "key is fully absent" notion
   `docs/mvcc.md` §7 does not have. Deferred and measured (via
   `chronicledb_mvcc_keys`), not silently dropped.

Also out of scope, named so it is not mistaken for something this
release covers: the audit log has no rotation or retention policy of
its own and grows without bound — that is a Security-Foundation-owned
surface (`enterprise-v1-plan.md` §11, Operations), not a
storage-lifecycle one.

## 8. Metrics

| Metric | Type | Notes |
|---|---|---|
| `chronicledb_mvcc_keys` | gauge | Distinct keys with a live chain |
| `chronicledb_mvcc_versions` | gauge | Total versions across all chains — the number GC is supposed to bound |
| `chronicledb_mvcc_gc_watermark` | gauge | Applied watermark |
| `chronicledb_mvcc_gc_passes_total` | counter | Completed full-keyspace walks |
| `chronicledb_requestid_outcomes` | gauge | The unbounded table, §7 — measured precisely because it is not fixed |
| `chronicledb_storage_health` | gauge | `0` = healthy, `1` = unhealthy |
| `chronicledb_fsync_failures_total{path="raft"\|"snapshot"\|"backup"\|"audit"}` | counter | |
| `chronicledb_disk_probe_failures_total` | counter | A `DiskUsage` sample itself errored |
| `chronicledb_scrub_runs_total`, `_findings_total`, `_last_duration_seconds` | counter/counter/gauge | |

There is deliberately **no** "force GC now" endpoint: pressure already
triggers a pass automatically, and an unbounded manual trigger would be
an event-loop-cost lever with no matching safety story.

## 9. Related documents

- [`ADR-0020`](adr/0020-mvcc-gc-replicated-watermark.md) — the GC
  architecture decision and alternatives considered.
- [`ADR-0021`](adr/0021-storage-lifecycle-retention-and-scrub.md) —
  retention, disk-full handling, and scrub, and their alternatives.
- [`docs/admission-control.md`](admission-control.md) — the companion
  Admission Control half of `v0.6.0`, which shares this document's
  pressure-sampling infrastructure.
- [`docs/mvcc.md`](mvcc.md) §6/§9 — the reclamation rule itself and the
  resolved `v0.6.0` decisions in MVCC's own terms.
- [`docs/invariants.md`](invariants.md) — the full correctness argument
  (`GC SAFETY`, `GC DETERMINISM`, `SNAPSHOT HORIZON ENFORCEMENT`,
  `RECLAMATION BOUNDARY`, `DISK-FULL EXPLICITNESS`, `SCRUB
  NON-DESTRUCTIVE`).
- [`docs/recovery.md`](recovery.md) §4 — scrub's place in the recovery
  model.
