# Dependency Policy

Status: Phase 11. ChronicleDB currently has **zero external Go module
dependencies** — [`go.mod`](../go.mod) has no `require` directives and
there is no `go.sum`. Every package under `internal/` and
`cmd/chronicledb-node` is built from the Go standard library only.

## Why

- Durability, consensus, and MVCC correctness are the subject of this
  project (see [`docs/vision.md`](vision.md)) — pulling in an existing
  Raft, storage-engine, or SQL-parsing library would replace the thing
  ChronicleDB exists to build and prove from scratch. See
  [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md) and
  [ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md) for
  the specific decisions this follows from.
- Fewer dependencies means a smaller supply-chain surface, no transitive
  version conflicts, and a `go build`/`go test` that never needs
  network access to a module proxy in a clean environment beyond
  fetching the Go toolchain itself.
- This is a deliberate choice, not an accident of not having needed one
  yet — see [`docs/non-goals.md`](non-goals.md) for the general
  pattern of documenting scope decisions with an explicit trigger for
  revisiting them.

## When a new dependency would be justified

A future external dependency is not ruled out permanently, but each
one requires:

1. A specific, evidenced need that the standard library cannot
   reasonably meet (not developer convenience alone).
2. Its own justification recorded where the change is made (a commit
   message and, if it touches an architectural boundary, an ADR) —
   silently adding one to `go.mod` without explanation is not
   acceptable.
3. An actively maintained project with a compatible open-source
   license (Apache-2.0/MIT/BSD-style — compatible with this project's
   own [Apache-2.0 license](../LICENSE)), not an abandoned or
   single-maintainer package for anything security- or
   correctness-critical.

## Keeping dependencies current

If and when external dependencies are added:

- Go module versions are tracked via `go.sum` as normal; `go test
  ./...` and CI would then depend on `go.sum` being present and
  accurate.
- [Dependabot](../.github/dependabot.yml) is configured for the
  `gomod` ecosystem (in addition to `github-actions`, already relevant
  today for keeping CI/release workflow action versions current) so
  any future dependency's updates — including security advisories —
  surface automatically as pull requests.

## GitHub Actions dependencies

`.github/workflows/` files do reference external GitHub Actions
(`actions/checkout`, `actions/setup-go`). These are pinned to a major
version tag (e.g. `@v7`), reviewed for compatibility before bumping
(see this repository's Phase 11 packaging commit for the specific
versions verified via the GitHub API at the time), and kept current via
[Dependabot](../.github/dependabot.yml)'s `github-actions` ecosystem
entry.
