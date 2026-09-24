# Changelog

All notable changes to ChronicleDB are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); versioning
follows [`docs/versioning.md`](docs/versioning.md) (SemVer, pre-1.0).

## [Unreleased]

Admission Control / Resource Protection and Storage Lifecycle — the
fifth phase of the `docs/enterprise-v1-plan.md` Enterprise V1 roadmap
(§9-§10, target release `v0.6.0`). See `docs/v0.6.0-plan.md` and
`docs/adr/0019-admission-control-architecture.md`/
`docs/adr/0020-mvcc-gc-replicated-watermark.md`/
`docs/adr/0021-storage-lifecycle-retention-and-scrub.md` for the
complete design, `docs/admission-control.md` and
`docs/storage-lifecycle.md` for the operator runbooks. Implementation
and its proof obligations are complete on the `v0.6.0-implementation`
branch as of this entry; **not yet tagged** — this section's header
becomes `## [0.6.0] - <date>` at actual release time, per this file's
own established convention (see the `[0.5.0]` entry below).

### Behavior changes

Three additive (MINOR-compatible, `docs/versioning.md`), but real,
changes existing clients should be prepared to handle:

1. `503` (with `Retry-After`) is now a possible response to `/propose`
   and every `/admin/*` endpoint — this release's admission control
   rejecting a request under sustained overload or genuine resource
   pressure (`disk_pressure`, `disk_critical`, `queue_full`,
   `queue_timeout`, `concurrency_limit`, `memory_pressure`,
   `read_lease_limit`, `admin_operation_in_progress`, `shutting_down` —
   `docs/admission-control.md` §8.2). Previously impossible; always
   safe to retry (a `503` from this release's admission control never
   records a `RequestID` outcome).
2. `status: "ABORTED_STALE"` is a new possible value in the `/propose`
   and `/outcome` response `status` field — a transaction whose
   `StartSeq` fell below the applied MVCC GC watermark. Previously
   impossible, and still impossible unless GC is enabled
   (`-gc-interval` > `0`, off by default in this release).
3. HTTP server timeouts and connection caps are enforced by default
   (`-http-read-header-timeout`, `-http-read-timeout`,
   `-http-write-timeout`, `-http-idle-timeout`, `-max-http-connections`,
   `-max-peer-connections`) — see `docs/upgrades.md` §8c. This is the
   **only** default-behavior change this release makes; every other
   new flag defaults to exactly `v0.5.0`'s prior behavior.

### Added

- Bounded admission control: a four-lane model (client writes, client
  reads, control-plane admin, maintenance) with per-lane concurrency
  ceilings, a bounded waiting room, and a stable `503`/`Reason`/
  `Retry-After` contract — `internal/admission`, wired through
  `internal/node` and `internal/sql`.
- Disk/heap resource-pressure sampling and a `Healthy`/`LowSpace`/
  `Critical` state machine with 10% de-escalation hysteresis, tightening
  or refusing client writes and triggering an immediate GC pass on
  entering pressure.
- MVCC version garbage collection for replicated mode: a leader-
  proposed, Raft-replicated watermark, bounded and resumable per Apply
  call (two independent bounds: keys examined, versions removed), with
  read leases protecting a live transaction's snapshot from reclamation
  out from under it. Ships disabled by default (`-gc-interval=0`).
  `internal/fsm`'s generation-3 `AdvanceGCWatermark` control command;
  `internal/version.MaxSupportedGeneration` 2 -> 3.
- `ENOSPC` classification (`wal.ErrOutOfSpace`/`storage.ErrOutOfSpace`),
  never conflated with a generic I/O error or a transaction conflict;
  automatic recovery once space is freed, with no restart.
- fsync-failure health: the Raft durable path still halts
  unconditionally on a persist failure (unchanged since `v0.1.0`), now
  classified and audited first; non-Raft-path failures (snapshot
  creation, backup export, audit-log writes) mark the node
  storage-unhealthy at a configurable consecutive-failure threshold
  without halting, recovering automatically on the next success.
  `/health` now reports `503` (not-ready) when a node is disk-`Critical`
  or storage-unhealthy, while remaining alive.
- Storage-integrity scrub: a read-only, rate-limited, admin-gated,
  audited check (`POST /admin/storage/scrub`) of every retained WAL
  segment, snapshot file, and the audit-log hash chain, reporting
  findings without ever repairing them.
- WAL/snapshot retention knobs (`-wal-retain-extra-segments`,
  `-snapshot-retain-count`) shrinking the window in which a lagging
  follower must fall back to a full snapshot transfer.
- New metrics: see `docs/observability.md` §2.1a/§10 for the complete
  list (`chronicledb_storage_health`, `chronicledb_mvcc_*`,
  `chronicledb_scrub_*`, `chronicledb_fsync_failures_total{path=...}`,
  `chronicledb_requestid_outcomes`, and others).
- New `docs/admission-control.md` and `docs/storage-lifecycle.md`
  operator guides, `ADR-0019`/`ADR-0020`/`ADR-0021`, and ten new
  `docs/invariants.md` entries.

### Fixed

- A crash-injection facility (`internal/node/faultpoint.go`) exposed a
  real ordering bug in `maybeSnapshot`'s snapshot-file pruning: a crash
  between the new snapshot becoming durable and the old one being
  pruned could, on a specific interleaving, leave zero valid snapshots
  on disk. Pruning now runs strictly after every other durability step
  in the sequence has completed.
- The pressure state machine's first draft escalated straight to
  `Critical` on a disk-usage probe failure (a transient sampling error)
  instead of `LowSpace`, rejecting all client writes rather than merely
  tightening them — caught by a dedicated fail-safe-direction test
  before it shipped.
- `Config.setDefaults` silently overrode an explicit, deliberate zero
  value for a GC tuning field with its non-zero package default,
  defeating a caller's own explicit "0 means off/strict" configuration.

## [0.5.0] - 2026-09-20

Dynamic Membership — the fourth phase of the
`docs/enterprise-v1-plan.md` Enterprise V1 roadmap (§8, target release
`v0.5.0`). See
`docs/dynamic-membership-plan.md` and
`docs/adr/0018-dynamic-membership-architecture.md` for the complete
design, `docs/membership.md` for the operator runbook, and that ADR's
"Testing and Proof Obligations" section for exactly which parts of the
plan's full test matrix are implemented versus documented remaining
work as of this entry.

### Added

- Runtime cluster membership changes with no downtime: add a learner,
  promote a caught-up learner to voter, remove a voter or learner
  (including the leader removing itself) — `internal/raft.Configuration`/
  `ProposeConfigChange`, four admin-gated, audited HTTP endpoints
  (`/admin/membership/add`/`promote`/`remove`/`status`).
- Single-server, serialized configuration changes, append-time-
  effective, gated by three proposal-time premises (current-term
  commit, inherited-suffix floor, local serialization) with a formal
  quorum-intersection/branch-confinement safety proof.
- `internal/snapshot.FormatVersion` 1 -> 2 (a bounded
  `[MinReadVersion, FormatVersion]` read range replaces strict
  equality); `internal/version.MaxSupportedGeneration` 1 -> 2.
- `internal/backup` restore-side membership-isolation transform: a
  restored cluster's `Configuration` always comes from the operator's
  `-cluster`/`-peers` flags, never from the source cluster, on both the
  staged snapshot and the staged WAL suffix.
- Two narrow Raft-level liveness rules bounding a removed node's
  disruption (membership-scoped vote acceptance; leader-contact
  suppression), replacing no existing mechanism.
- New `docs/membership.md` operator runbook, `ADR-0018`, and twelve new
  `docs/invariants.md` entries.

### Fixed

- `internal/wal.SetClusterGeneration` rejected any request at or below
  the current durable generation as an error; correct only for a fresh
  commit, this broke restart replay of more than one historical
  generation transition, unreachable before this phase's own
  `MaxSupportedGeneration` bump past 1. Now a silent no-op.
- `internal/raft.Core`'s follower-side four-shape defense-in-depth
  re-check produced a false-positive fail-closed panic on a brand-new
  node's first catch-up batch when that batch contains, among older
  entries, the very `EntryConfig` entry that adds the node itself.
- A nil-map panic when the acknowledgement completing a self-removing
  leader's new quorum arrives mid-call.
- A self-removing leader stepped down on *any* commit advance after its
  removal entry was appended — including one committing an ordinary
  earlier entry below it — because the step-down test consulted only
  the append-time-effective `activeConfig`. §4.2 gates step-down on
  that entry itself committing; until it does, the node was stranded as
  a `selfRemoved()` follower, refusing client calls with
  `ErrNodeRemoved` while still a committed voter and neither
  campaigning nor granting votes. Found by the `v0.5.0` final
  correctness review.
- That same follower-side four-shape re-check compared whole `Member`
  values, including `Address`. Because every node seeds its bootstrap
  `Configuration` from its own `-listen` for itself and `-peers` for
  everyone else, two nodes legitimately hold different spellings of one
  endpoint (`0.0.0.0:9000` against a routable address, `localhost`
  against `127.0.0.1`), which made the first `EntryConfig` a correct
  leader proposed panic those nodes' event loops — before the entry was
  persisted, so a restart re-derived the same bootstrap and panicked
  again, permanently. The four shapes are now classified by `NodeID`
  alone, which is the membership property they actually constrain
  (§1.8: `-listen` is "process-local configuration, never part of
  replicated `Configuration`"). Found by the `v0.5.0` final correctness
  review.
- `handleInstallSnapshotRequest`'s staleness check compared only
  against `SnapshotIndex`, never `CommitIndex`; a stale or duplicated
  `InstallSnapshotRequest` delivered after ordinary replication had
  already advanced `CommitIndex` past it could still take the install
  branch and discard log down to a lower boundary, leaving
  `CommitIndex() > LastIndex()`. Found by DM-10's randomized combined
  fault schedule.
- `internal/node.handleInstallSnapshot`'s driver-side `willAdvance`
  predicate was not updated alongside that `CommitIndex` staleness
  check, leaving the two halves of one contract disagreeing: for a
  boundary strictly between `SnapshotIndex` and `CommitIndex` the
  driver durably installed a snapshot `Core` then rejected, discarding
  the committed WAL entries above that boundary, rolling the state
  machine and `appliedIndex` back below `Core`'s own `CommitIndex` (so
  they were never re-applied), and fail-stopping the node on the next
  replicated entry. The driver now mirrors both halves of `Core`'s
  check. Found by the `v0.5.0` final correctness review.
- Three reply-driven continuation sites
  (`handleAppendEntriesResponse`'s success and conflict-repair
  branches, `handleInstallSnapshotResponse`) kept sending further
  entries to a removed member based on stale `nextIndex`/`matchIndex`
  bookkeeping alone, without checking current membership — even though
  the two proactive fan-out sites already excluded it correctly. Found
  by DM-10.
- `decodeConfigChange` returned `establishes=true` with no error for a
  crafted `EntryConfig` payload carrying an empty voter set, a gap
  `Core.ProposeConfigChange`'s own construction-time check never
  covered on the decode path. Found by `FuzzDecodeEntryConfig`.
- `/admin/upgrade/finalize`'s HTTP response unconditionally reported
  the newly reached generation as `MaxSupportedGeneration`, rather than
  the generation it actually targeted (`current+1`); exposed once this
  phase's own generation bump made a single finalize call no longer
  sufficient to reach the binary's max.
- `-cluster` was mandatory, leaving no way to start a fresh node with a
  genuinely empty `Configuration` to join an existing cluster by
  replication rather than by CLI-declared bootstrap.
- A membership call (`add`/`promote`/`remove`) that lost leadership
  before its entry committed fell through to a generic `500` instead of
  the same retryable `409` already used for `NotLeaderError`.
- CI's real-binary tests build an old release's binary from a `git
  worktree`; `go build`'s automatic VCS stamping can walk past the
  worktree's own `.git` file to an unrelated `.git` directory further
  up the filesystem and fail. Fixed by disabling VCS stamping
  (`-buildvcs=false`) for that build, which was never load-bearing for
  the resulting binary's behavior. Test-only; no production code
  changed.

### Known limitations at this point

- Exactly one membership change may be outstanding at a time — no
  joint consensus, no concurrent multi-node reconfiguration. By design
  (`ADR-0018`), not a target for a future release at this project's
  scale.
- No automatic or failure-triggered membership changes — every add,
  promote, and remove is operator/admin-triggered.
- Beyond the existing `v0.2.0` mTLS certificate-to-`NodeID` binding, no
  additional transport-layer check that a connection's certificate
  identity matches the address's *currently configured* membership
  role is implemented in this release (`docs/dynamic-membership-plan.md`
  §13.2 describes such a check; it was not built). The two Raft-level
  liveness rules and certificate revocation remain the operative
  defenses (`docs/membership.md` §7).
- A removed node does not self-terminate, self-wipe, or self-report as
  retired; decommissioning it (stopping the process, wiping or
  archiving its data directory, revoking its certificate) is an
  operator procedure — `docs/membership.md` §7.

## [0.4.0] - 2026-09-10

Compatibility / Rolling Upgrades — the third phase of the
`docs/enterprise-v1-plan.md` Enterprise V1 roadmap (§7, target release
`v0.4.0`). See that document and
`docs/adr/0017-compatibility-and-rolling-upgrades.md` for the complete
design and `docs/upgrades.md` for the operational runbook this release
adds.

### Added

- **Compatibility / Rolling Upgrades** (`docs/enterprise-v1-plan.md`
  §7): explicit version/generation checking across all five surfaces
  that phase names — wire protocol (`internal/raft.Message.SenderGeneration`,
  gob-encoded so a pre-`v0.4.0` peer tolerates it for free), WAL format
  (`internal/wal.Metadata.ClusterGeneration`, additive/conditional
  encoding), snapshot format (no code change needed — already
  generation-agnostic by construction, see `docs/snapshots.md` §10), FSM
  command format (`internal/fsm.ControlCommandMarker`,
  `SetClusterVersionCommand`, dispatched ahead of the existing
  `CommitTxnCommand` decode path), and metadata/schema format
  (`internal/sql/schema.go`'s pre-existing version byte, unchanged).
- A cluster-wide, Raft-replicated "agreed generation"
  (`internal/fsm.ApplySetClusterVersion`): N/N+1-only, strictly-forward,
  deterministic across every replica.
- `internal/node.UpgradePrecheck`/`FinalizeUpgrade`, new admin-gated,
  audited HTTP endpoints `/admin/upgrade/precheck` (read-only) and
  `/admin/upgrade/finalize`, and a standalone `-upgrade-precheck` CLI
  dry-run flag.
- New invariants (`docs/invariants.md`): `NO SILENT FORMAT
  MISINTERPRETATION`, `ROLLBACK BOUNDARY HONESTY`, `MIXED-VERSION
  QUORUM SAFETY`. New `ADR-0017`. New `docs/upgrades.md` (the five
  surfaces, the precheck/finalize runbook, rollback/downgrade
  semantics, failure semantics, documented deviations from the plan's
  literal handshake wording). `docs/versioning.md`, `docs/wal.md`, and
  `docs/snapshots.md` each gain a resolved-decisions section for this
  phase. `docs/configuration.md` gains `-upgrade-precheck`.

Proven with a real mixed-binary cluster: two actual, independently
built `chronicledb-node` binaries (the pre-`v0.4.0` baseline commit,
checked out into a temporary `git worktree` and built from there, and
this working tree's own current binary) run together across the full
critical proof scenario — old-binary cluster start, continued traffic,
one-node-at-a-time upgrade, a forced real leader failover while
versions are mixed, continued traffic and a RequestID retry across that
failover, final-node upgrade, precheck, finalize, a full-cluster
restart, and every acknowledged RequestID's outcome verified on every
node — plus a dedicated real-binary proof of safe pre-finalize rollback
and correct, fail-closed post-finalize rollback refusal (the old
binary's own unmodified command decoder rejects the replicated finalize
command it cannot understand, and its process exits).

### Fixed

Found and fixed during two rounds of code review of the initial
implementation commit (`5da92a7`), before this release was tagged —
full regression suite, including the real mixed-binary proof, re-run
clean after each fix:

- `fsm.EncodeState`/`DecodeState` never serialized the
  `SetClusterVersionCommand` idempotency table, so a retried finalize
  proposal whose original commit predated a snapshot/restart could be
  re-evaluated instead of returning its original outcome.
- One `raft.Message` send site (a snapshot-install error reply)
  bypassed the generation-stamping path, and
  `/admin/upgrade/finalize` read a cached `Status()` racily instead of
  live post-finalize state — both could transiently report a stale `0`
  generation.
- A follower adopting its cluster generation via `InstallSnapshot`
  catch-up (rather than replaying the `SetClusterVersionCommand` entry
  directly) never persisted that generation to its own WAL metadata,
  silently defeating `wal.Open`'s `ErrUnsupportedGeneration`
  rollback-refusal check for that node.
- `FinalizeUpgrade`'s leadership precheck read cached status instead of
  a live Raft role, and precheck computed the target generation as a
  flat constant rather than `current+1` (harmless only because this
  release's single reachable generation step makes the two identical).
- `internal/backup.Restore` never persisted the source cluster's
  generation into the freshly-opened staging WAL, so a restored data
  directory silently reported generation 0 regardless of what was
  backed up. Found during release qualification; not exploitable in
  this release (the FSM-layer command decode already fails closed
  independent of WAL metadata), but a real defense-in-depth gap.
- CI's checkout used a shallow clone, so the real mixed-binary tests
  (which build the previous release's binary via a `git worktree`)
  failed before running; fixed by fetching full history. Test/CI-only;
  no production code changed.

### Compatibility

Additive and non-breaking for any cluster that does not opt into an
upgrade. "Generation 0" is, by definition, every WAL/snapshot/FSM-
command format exactly as it existed through `v0.3.0` — an existing
`v0.1.0`-`v0.3.0` data directory or deployment is unaffected by
installing this binary until an operator actually drives a rolling
upgrade. Within one generation, decode remains exact; across the one
supported step (N/N+1), a `v0.4.0`+ decoder still reads a
not-yet-finalized older generation's own exact bytes, and a genuine
pre-`v0.4.0` binary can still rejoin and replicate normally against a
cluster that has upgraded but not yet finalized. Rollback (redeploying
the older binary) is safe before `finalize` and explicitly,
fail-closed refused after it — see `docs/upgrades.md` §5 for the exact
boundary. `v0.2.0` security/RBAC/audit and `v0.3.0` backup/restore/PITR
behavior are unchanged and re-verified: both integration suites pass
unchanged alongside the new mixed-version suite.

### Known limitations at this point

- No automatic/unattended upgrade orchestration — an operator or
  external tool drives node-by-node restart; ChronicleDB provides the
  safety mechanism, not the orchestration.
- N/N+1 (adjacent-generation) upgrades only — skip-version (N/N+2)
  upgrades are not supported.
- No live schema migration (`ALTER TABLE`) tooling beyond the existing
  SQL DDL.
- The `-upgrade-precheck` CLI dry-run flag issues a plain HTTP GET with
  no TLS/auth support; a deployment requiring `-auth-mode`/TLS on its
  control-plane HTTP surface should query `/admin/upgrade/precheck`
  directly with an authenticated HTTP client instead.
- Every other `v0.3.0` known limitation (security opt-in, Snapshot
  Isolation not Serializable, no SQL joins/subqueries/secondary
  indexes, single static shard, Linux amd64 only actually tested,
  backup format not encrypted at rest / no cloud object-store
  integration) is unchanged — see the `v0.3.0` and `v0.2.0` entries
  below.

## [0.3.0] - 2026-09-08

Backup / Disaster Recovery / PITR — the second phase of the
`docs/enterprise-v1-plan.md` Enterprise V1 roadmap (§6, target release
`v0.3.0`). See that document and
`docs/adr/0016-backup-disaster-recovery-and-pitr.md` for the complete
design and `docs/backup.md` for the operational guide this release
adds.

### Added

- **Backup / Disaster Recovery / PITR** (`docs/enterprise-v1-plan.md`
  §6): a new `internal/backup` package — a self-describing, versioned,
  checksummed export of a consistent snapshot boundary plus a selectable
  WAL log suffix, built entirely on the existing `internal/snapshot`/
  `internal/wal` formats (never a second, independently-evolving
  format); `Export` (snapshot-only or continuous-WAL-archiving
  schedules) and `Restore` (checksum/consistency validation before any
  write, atomic staging-directory promotion, PITR to an arbitrary
  committed log-index boundary, destructive-restore isolation with an
  explicit force flag).
- `internal/node.Node.Backup`: live-node backup export, dispatched
  through the node's own event-loop goroutine for a consistent,
  non-racing read of its currently-durable state; never mutates the
  node's own retained WAL/snapshot state (backup is architecturally
  separate from Raft snapshot/compaction).
- New admin/operator-gated, audited HTTP endpoint `/admin/backup` on
  `cmd/chronicledb-node`.
- New CLI flags: `-restore-from`, `-restore-until`, `-force-overwrite`
  — see `docs/configuration.md`.
- New invariants (`docs/invariants.md`): `BACKUP INTEGRITY`, `BACKUP
  CONSISTENCY`, `DESTRUCTIVE RESTORE ISOLATION`.
- New doc `docs/backup.md` (format, RPO/RTO model measured via
  `internal/backup/bench_test.go`, restore runbook, non-goals,
  documented deviations from the plan's literal CLI text).
- `docs/failure-model.md` §5 gains §5.1, narrowing "simultaneous
  majority storage loss" to name backup/restore as its resolution path.
- A real destructive disaster-recovery drill, proven at both the
  in-process real-disk/real-TCP level
  (`internal/node/backup_test.go`) and the real-OS-subprocess level
  (`cmd/chronicledb-node/backup_integration_test.go`, `integration`
  build tag): back up a live three-node cluster, delete every node's
  data directory entirely, restore three brand-new directories from the
  backup alone, and confirm the restored cluster reaches a state
  consistent with everything backed up and accepts new writes normally.
- Release-qualification test evidence added while auditing this phase's
  acceptance criteria: an explicit MVCC-tombstone/atomic-multi-key
  backup-restore round-trip proof against an independent reference
  model (`TestExportRestore_MVCCTombstonesAndAtomicMultiKeyMutationsPreserved`,
  `internal/backup`); a real-compaction backup/restore proof — driving a
  cluster past its snapshot threshold so `Node.Backup` runs against a
  nonzero, already-compacted boundary, then restoring and confirming
  both the pre- and post-compaction commits survive
  (`TestBackup_AfterRealSnapshotCompactionCombinesBaseAndSuffixCorrectly`,
  `internal/node`); a real-OS-process proof that a restored cluster
  survives a further SIGKILL + restart cycle, not merely the one
  `node.Open` `Restore` itself performs
  (extends `TestRealBackup_DestructiveDisasterRecoveryDrill`,
  `cmd/chronicledb-node`, `integration` build tag).

### Compatibility

Additive, non-breaking. The backup format is new and independent of
the existing WAL/snapshot on-disk formats (it reuses them by copying
through their own existing encode paths, never a new encoding) — this
release does not change or version-bump either format, and does not
affect a `v0.1.0`/`v0.2.0` node's own on-disk data directory in any
way. `v0.2.0`'s Security Foundation behavior (TLS, auth, RBAC, audit)
is unchanged and re-verified: the full `cmd/chronicledb-node`
`integration`-tagged security suite
(`security_integration_test.go`) passes unchanged alongside the new
backup suite, and `/admin/backup` is gated through the same RBAC
decision table and audit log as every other administrative endpoint
(`TestRBAC_DecisionTable_HTTPLayer_EveryRoleEveryEndpoint`'s
`admin.backup` case).

### Known limitations at this point

- Backup artifacts are not encrypted at rest — operators must apply
  their own access control/encryption for wherever a backup directory
  is stored. See `docs/backup.md` §9.
- No built-in cloud object-store integration — backup/restore operate
  on local filesystem paths only.
- No continuous/streaming replication to a warm standby, and no
  in-process backup scheduler — operators drive `/admin/backup` on
  their own schedule (`cron` or equivalent).
- No cross-version compatibility guarantee for the backup format yet
  (`chronicledbVersion` in the manifest is diagnostic only) —
  deferred to `docs/enterprise-v1-plan.md` §7 (Compatibility / Rolling
  Upgrades, target `v0.4.0`).
- PITR boundaries are committed log indices only, never wall-clock
  timestamps — ChronicleDB has no commit-timestamp concept anywhere in
  its design. See `docs/backup.md` §8.
- Every other `v0.2.0` known limitation (security opt-in not
  on-by-default, Snapshot Isolation not Serializable, no SQL joins/
  subqueries/secondary indexes, single static shard, Linux amd64 only
  actually tested) is unchanged — see the `v0.2.0` entry below.

### Fixed

- Real-OS-process integration test harness (`cmd/chronicledb-node`):
  `newRealCluster`/`newRealClusterWithSnapshotThreshold`/
  `newSecureRealCluster` and the destructive-restore-drill's own
  restore-cluster construction each reserved a cluster's TCP ports one
  at a time (reserve, release, reserve the next), leaving a real window
  where the OS could hand the just-freed port straight back to the very
  next reservation and give two nodes the same port — the identical
  release-then-reacquire race `internal/node/node_test.go`'s
  `freeAddrs` was already fixed for the in-process `testCluster`
  harness, but never applied to this real-process harness. Measured
  directly (20,000-trial stress harness isolating just the port-
  reservation pattern): 0.215% collision rate for the one-at-a-time
  pattern vs. 0% for a batch-hold pattern across the same trials —
  a deterministic test-harness defect, not a ChronicleDB defect.
  Replaced with `freePorts`, which holds every reservation in a batch
  open simultaneously before releasing any (mirroring `freeAddrs`),
  plus a deterministic regression test
  (`TestFreePorts_NoDuplicatesUnderConcurrentPortContention`) that
  proves the batch-hold property under concurrent port contention.
  Test-harness-only; no production code changed.

## [0.2.0] - 2026-09-08

Security Foundation — the first phase of the `docs/enterprise-v1-plan.md`
Enterprise V1 roadmap (§5, target release `v0.2.0`). See that document
and `docs/adr/0015-security-foundation.md` for the complete design and
`docs/security.md` for the operational guide this release adds.

### Added

- **Security Foundation** (`docs/enterprise-v1-plan.md` §5): node
  identity bound to a TLS certificate's `CommonName`/SAN
  (`internal/identity`); peer mTLS for the Raft transport with no
  plaintext fallback (`internal/transport/tls.go`); client TLS on the
  control-plane HTTP server; pluggable authentication — static bearer
  token (constant-time comparison) or mTLS client-certificate identity
  (`internal/authn`); fixed-role RBAC (`admin`/`operator`/`read-only`)
  enforced per endpoint via a single decision table (`internal/authz`);
  a dedicated, hash-chained, checksummed administrative audit log,
  physically separate from the WAL, that fails the triggering action
  closed on a write failure (`internal/audit`); hot certificate
  rotation via `SIGHUP` or the new `admin`-only `/admin/reload-tls`
  endpoint with no dropped in-flight connections; `/fault` now
  structurally unregistered unless `-enable-fault-endpoint` is passed
  (previously always reachable).
- New CLI flags on `chronicledb-node`: `-tls-cert`/`-tls-key`/`-tls-ca`,
  `-peer-tls-cert`/`-peer-tls-key`/`-peer-tls-ca`, `-auth-mode`,
  `-auth-token-file`, `-rbac-mapping-file`, `-audit-log-dir`,
  `-enable-fault-endpoint` — see `docs/configuration.md`.
- New invariants (`docs/invariants.md`): `NO UNAUTHENTICATED ADMIN
  ACTION`, `NO PLAINTEXT PEER REPLICATION`, `FAULT SURFACE OFF BY
  DEFAULT`, `AUDIT COMPLETENESS`.
- New doc `docs/security.md` (threat model, configuration guide,
  migration steps), superseding the "no auth/TLS" posture previously
  described in `SECURITY.md` and `docs/non-goals.md` §Authentication
  and TLS; `docs/failure-model.md` §6 extended with the adversarial-
  network threat model this phase newly defends against.
- Test evidence: TLS handshake accept/reject fixtures (expired,
  wrong-CA, self-signed, no-cert); an RBAC decision-table test over
  every role x endpoint pair, both at the package level and through the
  full HTTP middleware chain; an audit hash-chain tamper-detection test
  and a `FuzzDecodeFrame` fuzz target; a real three-node cluster with
  peer mTLS proving normal replication, leader failover, a
  minority-partition/heal cycle across changing leaders, and
  snapshot-based follower catch-up all continue to work unchanged
  under mTLS (`internal/node/tls_test.go`); a real-process integration
  test proving a live `SIGHUP`-triggered certificate rotation drops
  zero commits under continuous authenticated write load
  (`cmd/chronicledb-node/security_integration_test.go`, `-tags
  integration`); a real-binary test proving `/fault` is unreachable
  (404, not 403) by default even with a valid `admin` credential.
- Phase 12 (external review infrastructure, process only — no behavior
  change): a public reviewer guide (`docs/break-chronicledb.md`)
  mapping the "Break ChronicleDB" challenge onto existing guarantees,
  non-guarantees, reviewer personas, a 20-scenario challenge matrix,
  and the exact existing build/test/chaos/adversarial reproduction
  commands; an evidence ledger (`docs/external-review-findings.md`),
  starting with zero entries since no external review has occurred
  yet; two optional fields added to
  `.github/ISSUE_TEMPLATE/correctness_bug.yml` for challenge-response
  reports.
- `docs/enterprise-v1-plan.md`: the planning-only Enterprise V1
  architecture roadmap (`v0.2.0` through `v1.0.0`) that this release's
  Security Foundation work implements the first phase of. No later
  phase in that plan (Backup/DR onward) is started by this release.

### Compatibility

Breaking for existing plaintext, unauthenticated `v0.1.0`-style
deployments **only if an operator opts in** to the new flags. `v0.2.0`
ships secure-**by-configuration**, not secure-by-default: every new
flag defaults to `v0.1.0`'s exact plaintext/unauthenticated behavior
(`-auth-mode=none`, no TLS), so an in-place binary swap does not
silently change behavior for an existing trusted-network deployment.
Running without TLS/auth configured now prints a loud, repeated
startup warning. Flipping the default to secure-by-default is deferred
to the `v1.0.0` gate (`docs/enterprise-v1-plan.md` §5/§13.2).

### Known limitations at this point

- Security is opt-in, not on by default — see Compatibility above and
  `SECURITY.md`'s "Deployment assumptions". Do not expose ChronicleDB
  directly to an untrusted or public network even with TLS/auth
  configured; Byzantine node behavior and CA compromise remain out of
  scope.
- The real-cluster mTLS proof (`internal/node/tls_test.go`) covers
  normal replication, leader failover, partition/heal across leaders,
  and snapshot-based follower catch-up — the scenarios that actually
  exercise the peer network layer — rather than literally re-running
  every scenario in `docs/scenario-corpus.md` (most Local
  Durability/Transactions/Idempotency scenarios are single-node and
  do not touch peer TLS at all). Distributed SQL over mTLS (SQ-8's
  shape) is not separately proven under mTLS in this release.
- No backup/restore, no PITR, no rolling upgrades, no dynamic
  membership, no admission control, no MVCC GC — unchanged from
  `v0.1.0`; see `docs/enterprise-v1-plan.md` for the planned order.
- No SQL joins, subqueries, secondary indexes, or PostgreSQL wire
  compatibility — see `docs/sql.md` §8.
- Only Snapshot Isolation is implemented/proven, not Serializable —
  see `docs/mvcc.md` §1.1.
- Single static three-node-style shard only; no sharding, no
  cross-region replication — see `docs/non-goals.md`.
- Linux amd64 is the only platform actually developed and tested
  against; other release archives are cross-compiled but unverified —
  see `docs/support-matrix.md`.

## [0.1.0] - 2026-09-06

First tagged release. Everything below reflects `main` as of Phase 11
(open-source packaging). See [`docs/roadmap.md`](docs/roadmap.md) for
the complete, itemized phase-by-phase account this summary draws
from — this section intentionally does not re-litigate exact
evidence/test counts already recorded there.

### Added

- Phases 1-10 (the engine): single-node durable storage and WAL with
  crash recovery; MVCC transactions under Snapshot Isolation; the
  deterministic `internal/fsm` Apply boundary and `RequestID`
  idempotency; a deterministic Raft core plus a real three-node
  replicated deployment (`internal/node`, `internal/transport`) with
  quorum commits and leader failover; snapshots and log compaction;
  network-partition/crash-lab chaos testing; a small constrained SQL
  frontend (`internal/sql`) over the same transactional engine;
  benchmarks and observability (`/metrics`, `/health`); and a deep
  adversarial correctness pass using an independent reference model
  (`internal/oracle`).
- Phase 11 (this release's packaging work): `LICENSE` (Apache-2.0),
  `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`
  (Contributor Covenant 2.1), GitHub issue templates and a PR template,
  a tag-triggered release workflow
  (`.github/workflows/release.yml`) producing checksummed
  cross-compiled archives, `internal/version` plus a `-version` flag
  on `chronicledb-node`, `scripts/build-release.sh` (reproducible local
  release builds) and `scripts/demo-local-cluster.sh` (a real local
  three-node cluster demo), runnable examples
  (`examples/basic-transaction`, `examples/sql-basics`), and new
  reference docs: `docs/quickstart.md`, `docs/configuration.md`,
  `docs/versioning.md`, `docs/releasing.md`, `docs/support-matrix.md`,
  `docs/dependencies.md`.

### Known limitations at this point

- No authentication or TLS on the Raft transport or the HTTP control
  plane — see [`SECURITY.md`](SECURITY.md) and
  [`docs/non-goals.md`](docs/non-goals.md) §Authentication and TLS.
  Not resolved in Phase 11; do not deploy outside a trusted network.
- No SQL joins, subqueries, secondary indexes, or PostgreSQL wire
  compatibility — see [`docs/sql.md`](docs/sql.md) §8.
- Only Snapshot Isolation is implemented/proven, not Serializable —
  see [`docs/mvcc.md`](docs/mvcc.md) §1.1.
- Single static three-node-style shard only; no sharding, no
  cross-region replication — see [`docs/non-goals.md`](docs/non-goals.md).
- Linux amd64 is the only platform actually developed and tested
  against; other release archives are cross-compiled but unverified —
  see [`docs/support-matrix.md`](docs/support-matrix.md).
