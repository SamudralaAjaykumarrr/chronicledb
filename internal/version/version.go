// Package version holds build-time version metadata for ChronicleDB
// binaries (docs/roadmap.md Phase 11, docs/releasing.md). Version,
// Commit, and Date are set at build time via `go build -ldflags`
// (scripts/build-release.sh; see that script and
// .github/workflows/release.yml for the exact flags) and default to
// placeholder values for a plain `go build`/`go run` with no ldflags,
// so a locally-built binary is never mistaken for a tagged release.
//
// Version/Commit/Date themselves are never a correctness dependency —
// they exist solely for --version output and operator-facing
// diagnostics, matching docs/observability.md's rule that diagnostic
// state is never a correctness dependency. MaxSupportedGeneration
// below is the one deliberate exception (docs/enterprise-v1-plan.md
// §7): it is a real, load-bearing correctness input read by
// internal/wal, internal/fsm, internal/node, and internal/transport to
// decide what this binary may safely write, apply, or accept.
package version

// Version is the semantic version of this build, without the tag's
// leading "v" (docs/versioning.md), e.g. "0.1.0" for tag v0.1.0. "dev"
// for any build not produced by scripts/build-release.sh or the
// release workflow.
var Version = "dev"

// Commit is the short git commit hash this build was made from.
// "none" if not set at build time.
var Commit = "none"

// Date is the UTC build timestamp (RFC 3339). "unknown" if not set at
// build time.
var Date = "unknown"

// MaxSupportedGeneration is the highest cluster-version "generation"
// (docs/enterprise-v1-plan.md §7, docs/upgrades.md) this build of
// ChronicleDB understands — for the durable WAL-metadata/FSM-command/
// snapshot-state generation concept those packages check against, and
// for the wire-protocol generation a peer advertises on every
// internal/raft.Message (internal/transport/internal/node populate
// Message.SenderGeneration from this constant; internal/raft.Core
// itself never reads or writes it).
//
// Generation 0 is, by definition, every format exactly as it existed
// through v0.3.0 — a binary built before this constant existed (and
// so implicitly MaxSupportedGeneration==0) never sets
// Message.SenderGeneration, never proposes or understands a
// cluster-version FSM command, and never writes WAL metadata/FSM state
// carrying a nonzero ClusterGeneration. Bumping this constant is
// itself a compatibility decision: it defines the next generation an
// operator can -admin/upgrade/finalize a cluster to, and is expected
// to move in lockstep with, at most, one MINOR version at a time
// (docs/versioning.md's N/N+1 policy applied to on-disk/wire formats,
// not just APIs).
//
// Unlike Version/Commit/Date, this is a real correctness input (it
// gates what a node will apply/decode), not build-time/diagnostic
// metadata — it is a Go constant, not an ldflags-overridable var, so
// it cannot be accidentally misconfigured per-deployment the way a
// flag could.
const MaxSupportedGeneration uint32 = 1

// String returns a single human-readable line combining Version,
// Commit, and Date, suitable for a --version flag.
func String() string {
	return "chronicledb-node " + Version + " (commit " + Commit + ", built " + Date + ")"
}
