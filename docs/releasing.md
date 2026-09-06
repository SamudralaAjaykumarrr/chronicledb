# Releasing

Status: Phase 11. This is the checklist and exact commands a
maintainer follows to cut a ChronicleDB release. See
[`docs/versioning.md`](versioning.md) for the version-number policy
this process assumes.

No release has been published from this process yet — see
[`docs/roadmap.md`](roadmap.md) §Maturity Model's `OPEN-SOURCE READY`
gate for why that specifically matters to this repository's maturity
claim.

## Release checklist

Work through this in order. Do not skip a step because a similar one
passed recently — CI can be green on `main` and still not reflect the
exact commit being tagged.

1. **Clean branch.** `git status` shows a clean working tree on `main`,
   and `git log` confirms the commit to be tagged is the one intended
   (no local-only commits).
2. **CI green.** The latest `main` run of `.github/workflows/ci.yml`
   passed, for the exact commit being tagged.
3. **Version selected.** Pick the next version per
   [`docs/versioning.md`](versioning.md) (`vMAJOR.MINOR.PATCH`) based
   on what actually changed since the last tag.
4. **`CHANGELOG.md` updated.** Add a dated entry under the new version
   summarizing what changed, in the same honest, evidence-based style
   as [`docs/roadmap.md`](roadmap.md)'s phase write-ups — no marketing
   language, no unverified claims.
5. **Docs accurate.** `README.md` and `docs/roadmap.md`'s maturity
   claim match the code being released; no doc references a feature,
   flag, or endpoint that does not exist on this commit.
6. **Benchmark claims verified.** If [`docs/benchmarks.md`](benchmarks.md)
   is cited anywhere in release notes, its numbers were actually
   measured on this codebase, not carried over from an earlier phase
   without re-checking they still apply.
7. **Security limitations visible.** [`SECURITY.md`](../SECURITY.md)'s
   "Deployment assumptions" section (no auth/TLS) is still accurate and
   still prominent — not quietly removed or softened.
8. **Full quality gates pass**, run locally:
   ```bash
   gofmt -l .
   go vet ./...
   go build ./...
   go build -tags integration ./...
   go test ./...
   go test ./... -race
   go test -tags=integration ./cmd/chronicledb-node/... -race -count=1
   ```
9. **Chaos/adversarial suites green**, at a higher-than-CI seed count
   (see [`docs/testing-strategy.md`](testing-strategy.md) §6.5 and
   [`docs/adversarial-testing.md`](adversarial-testing.md) for the
   `CHRONICLEDB_CHAOS_SEEDS` / `CHRONICLEDB_ADVERSARIAL_SEEDS`
   environment variables and exact commands).
10. **Release artifacts build locally**:
    ```bash
    ./scripts/build-release.sh v<MAJOR>.<MINOR>.<PATCH>
    ```
    Confirm every target in [`docs/support-matrix.md`](support-matrix.md)
    produced an archive under `dist/`.
11. **Checksums generated** — `scripts/build-release.sh` writes
    `dist/checksums.txt` automatically; confirm it lists every archive.
12. **Tag creation** (this is the point of no return — see
    "Do not automatically release" below):
    ```bash
    git tag -a v<MAJOR>.<MINOR>.<PATCH> -m "ChronicleDB v<MAJOR>.<MINOR>.<PATCH>"
    git push origin v<MAJOR>.<MINOR>.<PATCH>
    ```
13. **GitHub Release verification.** Pushing the tag triggers
    `.github/workflows/release.yml`. Confirm in the Actions tab that
    it ran end to end, and confirm on the repository's Releases page
    that every expected artifact + `checksums.txt` is attached and the
    release notes are accurate.

## Reproducing a release build locally

Anyone (not only a maintainer) can reproduce the exact artifacts a
release workflow run would produce, for a given version string:

```bash
./scripts/build-release.sh v0.1.0
sha256sum -c dist/checksums.txt   # verify against a published release's checksums.txt
```

`scripts/build-release.sh` requires no network access beyond the Go
toolchain already being installed (`go env GOMODCACHE` — there are
zero external Go module dependencies to fetch, see
[`docs/dependencies.md`](dependencies.md)), builds only the targets in
[`docs/support-matrix.md`](support-matrix.md), embeds the version,
short git commit, and UTC build date into the binary via `-ldflags`
(`internal/version`), and only ever deletes/recreates its own `dist/`
output directory — never any other repository or filesystem state.

## Do not automatically release

Creating and pushing a version tag is a deliberate, manual, one-way
action a maintainer takes after completing the checklist above — it
is never done as a side effect of routine development, CI on `main`,
or documentation changes. `.github/workflows/release.yml` only
triggers on a pushed tag matching `v*.*.*`; it never runs on a branch
push or pull request, so it cannot accidentally publish a release.
