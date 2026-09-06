# Contributing to ChronicleDB

ChronicleDB is a from-scratch, evidence-driven distributed database
project (see [`docs/vision.md`](docs/vision.md)) — its guiding
principle is that a claim ("this is durable," "this is correct," "this
is fast") is only as good as the test that proves it. Contributions are
held to that same standard. Please read this document before opening
a pull request; it will save you a review round-trip.

## Before you start

- Read [`docs/vision.md`](docs/vision.md),
  [`docs/architecture.md`](docs/architecture.md), and
  [`docs/non-goals.md`](docs/non-goals.md) first. Many things that look
  like gaps (no joins, no auth/TLS, no config-file format, single
  shard) are deliberate, documented scope decisions with a stated
  trigger for revisiting them — not oversights waiting for a PR.
- For anything touching a correctness-relevant boundary (durability,
  MVCC, Raft, replication, snapshots, the SQL/transaction boundary),
  check [`docs/invariants.md`](docs/invariants.md) first and open an
  issue to discuss before writing code. A new ADR
  ([`docs/adr/`](docs/adr/), see `docs/adr/0000-template.md`) is
  usually expected for anything that changes an existing
  architectural decision.
- Small, well-scoped PRs (bug fixes, test additions, doc corrections,
  packaging/tooling) do not need a prior issue.

## Development setup

Prerequisites: Go (the version pinned in [`go.mod`](go.mod) and
`.github/workflows/ci.yml`), a POSIX-like shell, `git`. No other
tooling — ChronicleDB has zero external Go module dependencies (see
[`docs/dependencies.md`](docs/dependencies.md)), so `go build`/`go
test` need no network access beyond the Go toolchain itself.

```bash
git clone https://github.com/SamudralaAjaykumarrr/chronicledb.git
cd chronicledb
go build ./...
go test ./...
```

See [`docs/quickstart.md`](docs/quickstart.md) for building the node
binary and running a local cluster.

## Code style

- Run `gofmt` before committing (`gofmt -l .` must print nothing) and
  `go vet ./...` must pass.
- Match the existing documentation-comment style: package/exported-type
  doc comments explain *why*, cross-referencing the relevant `docs/*.md`
  section and invariant, not just restating the signature. Do not add
  comments that merely restate what the code already says.
- Keep packages dependency-minimal (see
  [`docs/dependencies.md`](docs/dependencies.md)) — do not add an
  external module for something the standard library already does
  adequately.
- Follow existing patterns for error handling: sentinel errors via
  `errors.go` files, wrapped with `%w` and context, never a bare
  `panic` on malformed external input (SQL text, wire messages, disk
  bytes) — see `internal/sql`'s fuzz-tested parser for the standard
  this is held to.

## Tests required before a PR

At minimum, before opening a PR:

```bash
gofmt -l .
go vet ./...
go build ./...
go test ./...
go test ./... -race
```

If your change touches `cmd/chronicledb-node` (the real multi-process
entry point):

```bash
go build -tags integration ./...
go test -tags=integration ./cmd/chronicledb-node/... -race -count=1
```

### Race testing

`-race` is not optional for anything touching concurrent access
(`internal/node`, `internal/raft`, `internal/txn`, `internal/fault`) —
Phase 7 found and fixed a genuine data race this way (see
[`docs/testing-strategy.md`](docs/testing-strategy.md) §7). A PR whose
new code path is only tested without `-race` will likely be asked to
add/re-run with it.

### Chaos / adversarial testing expectations

If your change touches `internal/raft`, `internal/node`,
`internal/fault`, `internal/wal`'s snapshot/compaction path, or
`internal/oracle`, also run the relevant suites at a higher-than-CI
seed count locally before opening the PR — see
[`docs/testing-strategy.md`](docs/testing-strategy.md) §6.5 and
[`docs/adversarial-testing.md`](docs/adversarial-testing.md) for the
`CHRONICLEDB_CHAOS_SEEDS`/`CHRONICLEDB_ADVERSARIAL_SEEDS` environment
variables and exact commands, e.g.:

```bash
CHRONICLEDB_CHAOS_SEEDS=2000 go test ./internal/fault/... -run TestChaos -timeout 300s
```

CI itself only runs a fast, CI-sized seed count on every push — passing
CI is necessary but not sufficient evidence for a change to a
correctness-sensitive path.

## How to add a regression test for a bug fix

Every bug fix needs a test that fails before the fix and passes after,
committed together with the fix (never a fix with no reproducing
test). Follow the existing pattern:

1. Reproduce the bug with the smallest test that demonstrates it —
   prefer the existing test infrastructure at the right layer
   (`internal/fault`'s deterministic simulator for a Raft-level bug,
   `internal/node`'s in-process real-disk/TCP tests for a replication-
   level bug, `cmd/chronicledb-node`'s `-tags=integration` tests for
   something that only reproduces with real separate OS
   processes/`SIGKILL`) rather than inventing a new harness.
2. If the bug was found via a seeded chaos/adversarial/fuzz run, name
   the test after the specific scenario (not just "regression test")
   and record the discovering seed/command in the test's doc comment —
   see `internal/raft/adversarial_test.go` or
   `internal/wal/adversarial_test.go` for the pattern.
3. Confirm the test fails against the pre-fix code (e.g. via `git
   stash` the fix), then passes after.

## Documentation expectations

- If you change behavior, update the `docs/*.md` file that documents
  it in the same PR — not as a follow-up. A doc describing a flag,
  endpoint, or SQL statement that no longer exists (or omitting one
  that now does) is treated as a bug.
- Do not weaken or remove an existing "what this does not claim"
  statement (e.g. in `docs/non-goals.md`, `docs/mvcc.md`'s Snapshot
  Isolation vs. Serializable distinction, `docs/sql.md` §8's
  compatibility boundaries) without the underlying capability actually
  changing. See the general principle in
  [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model: "advancing a
  maturity claim without its evidence gate is itself a documentation
  defect."
- New packages get a package-level doc comment explaining their role
  and, where relevant, a pointer to the `docs/*.md` file describing
  them — matching every existing package under `internal/`.

## How to avoid weakening correctness claims

This is the single most important rule in this repository:

- Never claim a property (durability, isolation level, replication
  safety, idempotency) is true because it seems true or because a
  quick manual check didn't find a problem — only a passing test
  against the specific scenario in
  [`docs/scenario-corpus.md`](docs/scenario-corpus.md) or
  [`docs/invariants.md`](docs/invariants.md) counts as evidence.
- If you're not sure whether a change affects an existing guarantee,
  say so explicitly in the PR description rather than asserting it
  doesn't.
- Never silence a failing test, skip it, or reduce a chaos suite's
  default seed count to make CI pass faster without an explicit,
  reviewed justification in the PR description.
- Never advance or imply a maturity claim
  ([`docs/roadmap.md`](docs/roadmap.md) §Maturity Model) beyond what
  this PR's own evidence supports.

## Commit / PR expectations

- Focused commits with a clear message; this repository's existing
  `git log` (`type: short description`, e.g. `fix: ...`, `feat: ...`,
  `test: ...`, `docs: ...`) is the style to match.
- Fill out the PR template's checklist honestly — an unchecked box
  with an explanation is better than a checked box that isn't true.
- Expect review focused on: does the test actually prove the claim,
  does the change respect existing invariants/non-goals, and is the
  documentation update accurate.

## Reporting bugs

Use a GitHub issue with the **Bug report** or **Correctness / safety
bug** template (`.github/ISSUE_TEMPLATE/`) — the latter if you suspect
data loss, a durability violation, an incorrect commit/read result, or
any other invariant violation, since it asks for the extra evidence
that kind of report needs. For a suspected security vulnerability, use
[`SECURITY.md`](SECURITY.md)'s private reporting process instead of a
public issue.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
