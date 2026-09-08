# Changelog

All notable changes to ChronicleDB are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); versioning
follows [`docs/versioning.md`](docs/versioning.md) (SemVer, pre-1.0).

## [Unreleased]

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
