## What does this change and why?

<!-- Link the issue if there is one. If this touches durability, MVCC,
Raft, replication, snapshots, or the SQL boundary, say which
invariant(s) in docs/invariants.md are affected. -->

## Checklist

- [ ] `gofmt -l .` is clean and `go vet ./...` passes.
- [ ] `go test ./...` and `go test ./... -race` pass locally.
- [ ] If this touches `cmd/chronicledb-node`, real-process integration
      tests pass: `go test -tags=integration ./cmd/chronicledb-node/... -race -count=1`.
- [ ] If this fixes a bug, a regression test reproducing it was added
      (not just a fix with no test) — see `docs/testing-strategy.md`
      and, for a chaos/adversarial-style bug, `docs/adversarial-testing.md`.
- [ ] Docs updated for any behavior change (`docs/`, `README.md`) — no
      doc now describes a flag/endpoint/statement that doesn't exist,
      and no doc claims something stronger than what was actually
      tested (see `docs/roadmap.md` §Maturity Model).
- [ ] No new claim of production-readiness, broader SQL/consistency
      guarantees, or resolved auth/TLS gap unless this PR actually
      implements it (`docs/non-goals.md`).
- [ ] If this changes measured performance, `docs/benchmarks.md` was
      re-run and updated, not left describing stale numbers.
- [ ] No new external Go module dependency without justification (see
      `docs/dependencies.md`) — if one was added, it's explained here.

## Correctness impact

<!-- "None" is a fine answer for docs/tooling-only changes. Otherwise:
what could this break, and what test would catch it? -->

## Benchmark impact (if applicable)

<!-- Only if this touches a hot path in docs/benchmarks.md's scope. -->
