# Changelog

All notable changes to ChronicleDB are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); versioning
follows [`docs/versioning.md`](docs/versioning.md) (SemVer, pre-1.0).

## [Unreleased]

Compatibility / Rolling Upgrades — the third phase of the
`docs/enterprise-v1-plan.md` Enterprise V1 roadmap (§7, target release
`v0.4.0`, not yet tagged). See that document and
`docs/adr/0017-compatibility-and-rolling-upgrades.md` for the complete
design and `docs/upgrades.md` for the operational runbook this work
adds. This is **implementation work toward `v0.4.0`, not a release** —
no maturity claim changes, nothing here is tagged, and Enterprise V1 is
not claimed complete.

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
