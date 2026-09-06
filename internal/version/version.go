// Package version holds build-time version metadata for ChronicleDB
// binaries (docs/roadmap.md Phase 11, docs/releasing.md). Version,
// Commit, and Date are set at build time via `go build -ldflags`
// (scripts/build-release.sh; see that script and
// .github/workflows/release.yml for the exact flags) and default to
// placeholder values for a plain `go build`/`go run` with no ldflags,
// so a locally-built binary is never mistaken for a tagged release.
//
// Nothing in ChronicleDB's correctness path (WAL, MVCC, Raft, FSM,
// SQL) reads this package — it exists solely for --version output and
// operator-facing diagnostics, matching docs/observability.md's rule
// that diagnostic state is never a correctness dependency.
package version

// Version is the semantic version of this build (docs/versioning.md),
// e.g. "v0.1.0". "dev" for any build not produced by
// scripts/build-release.sh or the release workflow.
var Version = "dev"

// Commit is the short git commit hash this build was made from.
// "none" if not set at build time.
var Commit = "none"

// Date is the UTC build timestamp (RFC 3339). "unknown" if not set at
// build time.
var Date = "unknown"

// String returns a single human-readable line combining Version,
// Commit, and Date, suitable for a --version flag.
func String() string {
	return "chronicledb-node " + Version + " (commit " + Commit + ", built " + Date + ")"
}
