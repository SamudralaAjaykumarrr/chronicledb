# ADR-0021: Storage Lifecycle — Retention, Disk-Full Handling, and Scrub

Status: Accepted

## Context

`ADR-0020` records `v0.6.0`'s central Storage Lifecycle decision (MVCC
GC as replicated state). This ADR records the three remaining pieces of
`enterprise-v1-plan.md` §10 released alongside it: retention knobs for
the WAL and snapshot files GC's own reclamation eventually shrinks,
explicit handling of running out of disk space entirely, and a
storage-integrity verification tool (scrub).

## Decision

### Retention knobs default to exactly `v0.5.0` behavior

`-wal-retain-extra-segments` (default `0`) and `-snapshot-retain-count`
(default `1`) both reproduce today's exact pre-`v0.6.0` behavior at
their defaults — an operator who sets neither observes byte-for-byte
identical retention to `v0.5.0`. This is deliberate, not incidental:
`§10.3` of the plan requires every new default to be justified either
by the memory arithmetic already governing `-max-inflight-proposals`,
or by "exactly the pre-`v0.6.0` behavior," and these two flags are
squarely in the second category. Raising either widens, respectively,
the window in which a lagging follower can catch up by log replication
instead of a full `InstallSnapshot`, and the window in which
`chronicledb_snapshot_serve_miss_total` (a leader pruning a snapshot
file between deciding a follower needs it and actually filling the
message) can occur — both purely disk-space-for-availability trade-offs
an operator opts into, never a correctness knob (`RECLAMATION BOUNDARY`,
`§27.8`, holds identically regardless of either value).

### The reclamation boundary, restated

`internal/wal.WAL.CompactBeforeRetaining` is `CompactBefore` extended
with a retention parameter, not a replacement for it: the crash-safety
ordering `docs/wal.md` §7 already establishes (snapshot durable, then
the WAL metadata pointer moves, then old segments are deleted) is
preserved exactly — retention only changes *how many* otherwise-eligible
segments are deleted, never *when* deletion is safe to begin. A unit
test (`SL-24`) pins that the six-step ordering inside `maybeSnapshot`
cannot be silently permuted by a future edit.

### Disk-pressure states and hysteresis

Recorded in full in `ADR-0019` (the state machine is shared
infrastructure between admission control and storage lifecycle — the
same `PressureMonitor`/hysteresis logic that tightens Lane B admission
is what this ADR's own `ENOSPC`/fsync-failure handling below builds on
for its own "never a stuck-forever state" requirement).

### `ENOSPC` classification

`internal/storage` and `internal/wal` each classify a `syscall.ENOSPC`
(detected with `errors.Is`, never string-matching) into their own
`ErrOutOfSpace` sentinel, at every write/sync/create call site, so the
classification survives crossing a package boundary — `storage.ErrOutOfSpace`
is reclassified into `wal.ErrOutOfSpace` at `internal/wal`'s own
boundary, rather than leaking `internal/storage`'s error type to every
caller of `internal/wal` (most importantly `internal/node`, on the Raft
durable path). The threat this defends against is named explicitly in
`docs/v0.6.0-plan.md` §27.9: wrapping an `ENOSPC` into a generic
`fmt.Errorf("write failed: %w")` that loses the classification, leaving
a client unable to distinguish "the cluster is full" from "you lost a
conflict" — exactly the failure mode that makes a client retry the
wrong thing forever.

### Recovery-on-space-freed: no stuck-forever state

Once space is freed, the very next successful pressure sample restores
the previous state automatically — no operator action, no restart,
proven against a real small filesystem (`SL-15`). This mirrors the
"fail-safe direction" already established for pressure sampling itself
(`ADR-0019`): the system is designed to recover on its own the moment
the underlying condition clears, never to require an operator to notice
and intervene for a condition that was never a data-safety problem in
the first place.

### The fsync-failure threshold, and why the Raft path is unchanged

A failure persisting the Raft durable log (`raft.ApplyPersistRequest`)
still calls `Node.fail`, unconditionally, on the very first failure —
`v0.5.0`'s exact behavior, preserved exactly. An fsync failure on the
consensus path is not something to count and tolerate: durability is
this project's core correctness property, and continuing to serve
consensus traffic after a durability failure on that specific path
would risk acknowledging a write that was never actually safe. What
`v0.6.0` adds on that path happens **before** the halt: classify the
error, emit an audit record, and increment
`chronicledb_fsync_failures_total{path="raft"}` — so an operator learns
*why*, rather than finding a stopped process with no explanation.

Non-consensus durable writes — snapshot creation and backup export —
are different in kind: neither is on the path a client's own
acknowledgement depends on, and both already had, before this release,
crash-safety properties that tolerate being abandoned mid-attempt (every
row of the crash-safety table, `docs/v0.6.0-plan.md` §22, was already
true). `v0.6.0` makes failure on these paths **non-fatal**: a
consecutive-failure counter (`-fsync-failure-threshold`, default `3`)
marks the node storage-unhealthy — `/health` reports not-ready, an
audit record is written, `chronicledb_storage_health` becomes `1` — but
the node keeps participating in Raft (voting, replicating) as long as
the consensus path still works, because a node that can still vote is
more useful to the cluster than one that has removed itself over a
problem that has not (yet) affected its ability to durably persist
consensus state. A single subsequent success on that path resets the
counter and clears storage-unhealthy — the identical "no stuck-forever
state" posture disk pressure already has, applied here because nothing
in this release's own reasoning suggests storage-unhealthy should be a
one-way latch a healthy node can never leave without a restart.

### Scrub: read-only by construction, and never repairs

`internal/storage.OpenSegmentReadOnly` opens with `os.O_RDONLY` and
returns a `*Segment` whose `Append`/`Sync`/`Truncate` all fail closed
with `ErrReadOnlySegment`. Scrub (`internal/wal.Scrub`,
`internal/snapshot.Scrub`, `internal/audit.Scrub`, orchestrated by
`internal/node.Node.Scrub`) uses only that constructor — never
`storage.OpenSegment` — so `SCRUB NON-DESTRUCTIVE` (`§27.10`) is a
property of the file descriptor itself, not of reviewer discipline,
enforced by `TestScrubOnlyOpensReadOnly` (an AST test, with a negative
control proving the detector itself works) rather than left to
convention. Scrub reports every finding it detects (a fixed vocabulary:
`bad_checksum`, `bad_framing`, `unsupported_version`,
`index_out_of_order`, `unreadable`, `snapshot_decode_failed`,
`audit_chain_broken`) and repairs nothing — consistent with
`RECOVERY NON-INVENTION` (`docs/recovery.md` §4): scrub reports, the
operator acts. There is no `?repair=true`.

Scrub runs entirely on the calling goroutine, never dispatched onto
`internal/node`'s event loop — it touches no live `WAL`/`Manager`/`Core`
object, only the immutable directory paths already known to
`Config`, so a scrub can run concurrently with ordinary traffic and
with a concurrent snapshot/compaction cycle without any synchronization
against either. A torn tail in the current (still-open) file is legal,
matching each underlying package's own existing recovery convention,
and is never reported as a finding; the identical signature in an
earlier, already-closed file is corruption and is reported. A file
removed concurrently by ordinary compaction/pruning
(`os.IsNotExist`) is skipped, not reported — by definition, a file that
disappeared mid-scrub was no longer needed.

## Alternatives Considered

- **A rate limiter for `ENOSPC` retries** (treating disk-full as a
  transient condition to back off and retry against). Rejected: an
  `ENOSPC` write already failed explicitly and durably — there is
  nothing to retry that would change the outcome until space is
  actually freed, and disguising that fact behind a retry loop would
  delay the moment an operator (or an automated system reading
  `disk_critical`) learns the real problem.
- **Making scrub optionally repair a torn tail or a bad checksum.**
  Rejected outright, not merely deferred: `docs/recovery.md`'s own
  `RECOVERY NON-INVENTION` invariant already forbids inventing a
  plausible-looking repair for corruption this codebase cannot prove
  correct, and a verification tool that can also *mutate* what it
  verifies is one an operator will not trust to run when it is most
  needed.
- **A periodic, automatically-scheduled scrub timer.** Left as a named
  open question (`§35.1` item 4 of the plan) rather than decided either
  way in this release: `v0.6.0` ships scrub as an operator-triggered
  endpoint only (`POST /admin/storage/scrub`); whether periodic
  scheduling belongs in `internal/node` itself or in an external
  operational tool is a packaging question this release does not need
  to answer to ship a correct, useful scrub.
- **Marking a node storage-unhealthy after the very first non-Raft
  fsync failure, unconditionally.** Rejected as the *default* (though
  available as an explicit, stricter operator choice via
  `-fsync-failure-threshold=0` or `1`): a single transient failure
  (e.g. a momentary `EIO` from a flaky underlying device) is common
  enough in real deployments that treating it as an immediate readiness
  failure would produce false alarms disproportionate to the actual
  risk; a small consecutive-failure threshold distinguishes a
  persistent problem from a blip.

## Consequences

- The audit log is now written to for reasons that have nothing to do
  with an authenticated HTTP request: `internal/node`'s own
  health-transition events (disk-pressure state changes, storage-health
  transitions, a raft-path fsync failure) are audited unconditionally,
  regardless of `-auth-mode`. `cmd/chronicledb-node` now opens exactly
  one `*audit.Log` per process — previously, `-auth-mode=none` (the
  default) meant no audit log was ever opened at all; this release
  changes that, since `§20.3`'s auditing obligation does not depend on
  client authentication being configured.
- An operator now has an explicit, actionable signal — `/health`'s
  `503` plus `chronicledb_storage_health`/`chronicledb_fsync_failures_total`
  — for a class of problem that previously only surfaced as "the
  process is still running but something might be wrong," or, on the
  Raft path, as a stopped process with no recorded reason.
- Scrub's own read volume is rate-limited (`-scrub-bytes-per-sec`,
  default 64MiB/s) as a simple, coarse, per-file proportional-sleep
  pacer — real, but not a true sub-file token bucket; adequate for a
  background maintenance operation, not tuned for byte-level precision.

## Correctness Implications

See `docs/invariants.md`'s new "Admission Control / Storage Lifecycle
invariants (`v0.6.0`)" section: `DISK-FULL EXPLICITNESS` (§27.9) and
`SCRUB NON-DESTRUCTIVE` (§27.10) are this ADR's two correctness claims.
`docs/failure-model.md` §1.8/§1.9 and `docs/storage-lifecycle.md` carry
the full operational failure-semantics table.

## Testing and Proof Obligations

- Real filesystem (`internal/testfs`'s unprivileged user+mount-namespace
  harness — a genuine kernel `ENOSPC`, never mocked): `SL-14`/`SL-14b`
  (a real `ENOSPC` on the Raft path and on a snapshot write is
  classified correctly and never reported as an SI abort), `SL-15`
  (automatic recovery once space is freed, no restart) —
  `internal/wal/enospc_test.go`, `internal/storage/enospc_test.go`,
  `internal/node/ac13_test.go`.
- Real process, real `SIGKILL` (`SL-25`, `cmd/chronicledb-node/sl25_test.go`):
  a kill mid-`/admin/backup` leaves this node's own data directory
  untouched and the partial backup fails `Restore` closed on its
  manifest; a kill mid-PITR-restore is safely retryable from the same
  backup directory.
- Unit/property (`SL-9`, `SL-10`, `SL-20`, `SL-21`, `SL-22`): negative
  retention values refused at startup; scrub detects every injected
  corruption fixture plus a clean tree plus a freshly torn tail
  (directory hash byte-identical before/after —
  `internal/wal/scrub_test.go`, `internal/snapshot/scrub_test.go`,
  `internal/audit/scrub_test.go`); fuzzed `AdvanceGCWatermark`
  decoding never panics; `TestScrubOnlyOpensReadOnly` (an AST test,
  `internal/node/scrub_ast_test.go`) plus its negative control.
- Real cluster, combined orchestration (`§30.3`,
  `cmd/chronicledb-node/section30_3_test.go`): a real backup and a real
  scrub, verified genuinely still in flight (not assumed from timing),
  overlapping a membership call — Lane A2 saturation actually exercised
  against Lane A1, not a sequential arrangement that would pass
  vacuously.
