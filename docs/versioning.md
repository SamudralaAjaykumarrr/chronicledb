# Versioning Policy

Status: Phase 11. This document defines ChronicleDB's version format,
what pre-1.0 compatibility actually means here, and what counts as a
breaking change. It governs tags/releases only — it does not, by
itself, change any maturity claim in
[`docs/roadmap.md`](roadmap.md) §Maturity Model.

## Format

ChronicleDB uses [Semantic Versioning](https://semver.org/): tags are
`vMAJOR.MINOR.PATCH` (e.g. `v0.1.0`), created directly on `main`. There
is no `v` in the version reported by `chronicledb-node -version` — that
matches the tag with the leading `v` stripped, e.g. tag `v0.1.0`
reports `chronicledb-node 0.1.0 (commit ..., built ...)`.

## Pre-1.0 (`v0.x.y`)

Per SemVer §4, `0.x.y` means the public API is **not** considered
stable: a `MINOR` bump (`0.1.0` → `0.2.0`) may include breaking
changes, not just additions. This is an honest reflection of where
ChronicleDB actually is — Phases 1-10 proved correctness of the
engine, not API stability of any Go package, wire format, or the SQL
surface. There is currently no committed plan for when `v1.0.0` will
be cut; it requires at minimum reaching `EXTERNAL-REVIEW READY` (see
[`docs/roadmap.md`](roadmap.md) §Maturity Model) plus an explicit
decision that the surfaces below are ready to be frozen.

Within `v0.x.y`, ChronicleDB does make one promise: a `PATCH` bump
(`0.1.0` → `0.1.1`) is always backward-compatible and contains **only**
bug fixes and documentation — never a deliberate behavior change to
any documented surface.

## What counts as "the public surface" today

There is no stable wire protocol and no stable client library — see
[`docs/non-goals.md`](non-goals.md) and [`docs/sql.md`](sql.md) §8. The
surfaces this policy tracks compatibility for are:

- The `chronicledb-node` CLI flags (`cmd/chronicledb-node/main.go`).
- The HTTP control-plane endpoints and their JSON shapes (`/status`,
  `/propose`, `/outcome`, `/fault`, `/metrics`, `/health` —
  [`docs/observability.md`](observability.md)).
- The on-disk WAL/snapshot formats ([`docs/wal.md`](wal.md),
  [`docs/snapshots.md`](snapshots.md)) — a format change that cannot
  read an older on-disk directory is a breaking change.
- The `internal/sql` grammar and semantics
  ([`docs/sql.md`](sql.md)) as consumed via `internal/sql.Engine`/
  `Session` — the only Go API surface intended for external use today,
  despite living under `internal/` (Go's `internal/` visibility rule
  means it is not actually importable outside this module; see
  [`docs/architecture.md`](architecture.md) for why no separate public
  `pkg/` API is offered yet).

A breaking change to any of the above is a `MINOR` bump pre-1.0 (would
be a `MAJOR` bump post-1.0). An addition that does not change existing
behavior (a new flag, a new metric, a new SQL statement kind) is a
`PATCH` or `MINOR` bump at the maintainer's discretion.

## Release tags

Releases are created by pushing an annotated tag matching `v*.*.*` to
`main`, which triggers `.github/workflows/release.yml` — see
[`docs/releasing.md`](releasing.md) for the full checklist and exact
commands. Tags are never force-pushed or deleted after a release has
been published.

## What this policy does not promise

Consistent with [`docs/non-goals.md`](non-goals.md) §Staff/Principal
validation and equivalence claims: this policy describes ChronicleDB's
own versioning discipline, not a claim of production-grade API
stability, long-term support windows, or compatibility guarantees
comparable to an established database. Pre-1.0 means exactly what
SemVer says it means.
