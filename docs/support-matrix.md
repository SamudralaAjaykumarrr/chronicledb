# Support Matrix

Status: Phase 11. This document distinguishes what is actually
developed and tested against, what is only cross-compiled for release
convenience, and what is unsupported — per the roadmap's evidence
principle ("a benchmark command existing does not prove performance");
the same logic applies here: a platform *building* is not evidence it
*works*.

## Go version

Developed and tested against the Go version pinned in
[`go.mod`](../go.mod) and `.github/workflows/ci.yml` (currently Go
1.26). CI installs exactly that version; other versions are untested.

## Platform support

| Platform | Development/testing | Release artifact | Notes |
|---|---|---|---|
| Linux amd64 | **Yes** — all CI (`.github/workflows/ci.yml`), all chaos/adversarial/integration suites, all benchmarks in [`docs/benchmarks.md`](benchmarks.md) run here. | Yes | The only platform this project is actually developed and continuously tested on. |
| Linux arm64 | No | Yes (cross-compiled) | Builds via `GOOS=linux GOARCH=arm64`; not run in CI, not benchmarked. No known reason it would fail (no cgo, no platform-specific code), but "compiles" is not "tested" — see `docs/roadmap.md` §Maturity Model. |
| Darwin (macOS) amd64/arm64 | No | Yes (cross-compiled) | Same caveat as Linux arm64. Real subprocess/SIGKILL integration tests (`cmd/chronicledb-node`) have only ever been run on Linux. |
| Windows amd64 | No | Yes (cross-compiled) | Same caveat. Additionally: the real-process integration tests use POSIX signals (`SIGKILL`) and Unix-style file paths in places; ChronicleDB has never been run, let alone tested, on Windows. Treat the Windows artifact as "it compiles," nothing more. |
| Any other GOOS/GOARCH | No | No | Not built by `scripts/build-release.sh` or `.github/workflows/release.yml`. Cross-compiling one yourself (`go build ./cmd/chronicledb-node`) will likely work (no cgo), but is entirely unverified. |

## What "release artifact" means here

A release artifact is a binary that **compiles** for that
`GOOS`/`GOARCH` via `go build` with `CGO_ENABLED=0` and is packaged by
`scripts/build-release.sh`. It does not mean the binary has been run,
tested, or benchmarked on that platform — only Linux amd64 has.

## Storage/filesystem assumptions

`internal/storage`/`internal/wal`'s durability contract
([`docs/wal.md`](wal.md), [`docs/failure-model.md`](failure-model.md))
assumes a POSIX-like filesystem with a working `fsync`/`fdatasync`. It
has only ever been exercised against Linux's `ext4`/`overlay2` (CI
runners, local development). Behavior on other filesystems (network
filesystems, other POSIX filesystems, NTFS) is untested and
unverified.
