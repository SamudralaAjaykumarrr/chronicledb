package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFsyncFailureThreshold_StorageUnhealthyThenRecovers is §20.2: a
// real, repeated permission-denied failure (not a mock) writing this
// node's own snapshot temp files — achieved by revoking write
// permission on <DataDir>/snapshot/tmp, so every maybeSnapshot attempt's
// storage.WriteFileDurable genuinely fails at the OS level — drives the
// consecutive non-Raft-path fsync-failure counter to
// -fsync-failure-threshold, at which point the node marks itself
// storage-unhealthy (never halting: proposals keep succeeding
// throughout, and maybeSnapshot itself keeps quietly retrying on every
// subsequent event-loop pass — a pre-existing, unconditional call this
// slice does not change). Restoring the permission lets the very next
// retry succeed, which resets storage-unhealthy with no restart (§19.3's
// "no stuck forever state" principle, applied here to §20.2 exactly as
// it is to disk pressure).
func TestFsyncFailureThreshold_StorageUnhealthyThenRecovers(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permission bits do not deny access")
	}
	const fsyncThreshold = 5
	tc := newTestClusterWithSnapshotThresholdAndAdmissionOverride(t, 1, 1, func(cfg *Config) {
		cfg.FsyncFailureThreshold = fsyncThreshold
	})
	leaderID := tc.awaitLeader(5 * time.Second)
	n := tc.node(leaderID)

	// One successful proposal (past SnapshotThreshold=1) so the first
	// snapshot succeeds normally before permissions are revoked —
	// proves this test is exercising a genuine subsequent-attempt
	// failure, not merely "snapshotting never worked at all here."
	if _, err := propose(t, n, cmd("sh-warmup", 0, 0, "k-warmup", "v"), 5*time.Second); err != nil {
		t.Fatalf("warmup propose: %v", err)
	}
	awaitCondition(t, 2*time.Second, "warmup snapshot created", func() bool {
		return n.Metrics().SnapshotsCreatedTotal > 0
	})
	if !n.Status().StorageHealthy {
		t.Fatalf("StorageHealthy = false before any failure was ever injected")
	}

	tmpDir := filepath.Join(tc.dirs[leaderID], "snapshot", "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("ensuring snapshot tmp dir exists: %v", err)
	}
	if err := os.Chmod(tmpDir, 0o000); err != nil {
		t.Fatalf("chmod snapshot tmp dir: %v", err)
	}
	restored := false
	restore := func() {
		if !restored {
			os.Chmod(tmpDir, 0o755)
			restored = true
		}
	}
	defer restore()

	// A further proposal advances appliedIndex past the (now
	// permanently unreachable) snapshot boundary, so maybeSnapshot has
	// something to keep retrying — every subsequent event-loop pass
	// (heartbeats included) attempts it again, quickly exceeding
	// fsyncThreshold with no further client action required.
	if _, err := propose(t, n, cmd("sh-trigger", 0, 0, "k-trigger", "v"), 5*time.Second); err != nil {
		t.Fatalf("trigger propose: %v", err)
	}

	pollUntil(t, 5*time.Second, func() bool { return !n.Status().StorageHealthy })
	if got := n.FsyncFailuresTotal()[FsyncPathSnapshot]; got < fsyncThreshold {
		t.Fatalf("FsyncFailuresTotal[snapshot] = %d, want >= %d", got, fsyncThreshold)
	}
	if n.Status().Ready {
		t.Fatalf("Status().Ready = true while storage-unhealthy, want false")
	}

	// The node must not have halted: an ordinary proposal still
	// succeeds while storage-unhealthy (§20.2: "does not self-terminate").
	if _, err := propose(t, n, cmd("sh-still-alive", 0, 0, "k-still-alive", "v"), 5*time.Second); err != nil {
		t.Fatalf("propose while storage-unhealthy: %v", err)
	}

	// Restore write access: the very next retry succeeds, and
	// storage-unhealthy clears with no restart — no further proposal is
	// needed, since maybeSnapshot keeps retrying on its own.
	restore()
	pollUntil(t, 5*time.Second, func() bool { return n.Status().StorageHealthy })
	if !n.Status().Ready {
		t.Fatalf("Status().Ready = false after recovery, want true")
	}
}
