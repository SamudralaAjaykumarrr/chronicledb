# Changelog

All notable changes to ChronicleDB are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); versioning
follows [`docs/versioning.md`](docs/versioning.md) (SemVer, pre-1.0).

## [Unreleased]

### Added

- Phase 12 (external review infrastructure, process only — no behavior
  change): a public reviewer guide
  (`docs/break-chronicledb.md`) mapping the "Break ChronicleDB"
  challenge onto existing guarantees, non-guarantees, reviewer
  personas, a 20-scenario challenge matrix, and the exact existing
  build/test/chaos/adversarial reproduction commands; an evidence
  ledger (`docs/external-review-findings.md`), starting with zero
  entries since no external review has occurred yet; two optional
  fields added to `.github/ISSUE_TEMPLATE/correctness_bug.yml` for
  challenge-response reports. No production code, test assertion, or
  security-reporting process changed.

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
