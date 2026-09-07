package node

import (
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestPauseTicksForTestFreezesElectionClock is a regression test for a
// CI-only flake investigation (TestMetricsProposalsCountedByOutcome and
// TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly both
// intermittently failed with "node: not leader (leader unknown)" on a
// cluster with no deliberate isolation/crash in flight, or right after
// one node was isolated while the other two should have kept quorum).
// Root cause was NOT a Raft/Node correctness bug: it was proven to
// pre-date internal/node's ReadIndex-ack-correlation change (it
// reproduces identically on the commit before that fix, given an
// injected goroutine-scheduling stall) and to be a legitimate Raft
// re-election — a node genuinely observing a higher term and correctly
// stepping down (raft.Core.stepDownTo) — triggered whenever a host
// scheduling delay (GC pause, CPU contention on a shared/noisy CI
// runner) exceeds a testCluster node's tight-ish election-timeout
// budget while a test's scripted sequence assumed no re-election would
// occur. The fix is Node.PauseTicksForTest /
// testCluster.pauseTicking: freeze the election side of the clock so
// no node can time out and start a spontaneous election, without
// touching timeouts, without sleeps/retries around Propose, and
// without weakening any assertion.
//
// This test proves the freeze mechanism itself holds: with ticking
// paused, waiting several multiples of the cluster's configured
// election timeout must never produce a role or leader change anywhere
// in the cluster.
func TestPauseTicksForTestFreezesElectionClock(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(2 * time.Second)
	tc.pauseTicking()

	// newTestCluster's default (non-snapshot) config is
	// ElectionTimeoutTicks=5, ElectionTimeoutJitterTicks=5,
	// TickInterval=10ms: worst case a real, ticking node times out
	// within roughly 100ms. Waiting 5x that with ticking paused is a
	// comfortable margin to prove the freeze holds, not a timing
	// coincidence.
	time.Sleep(500 * time.Millisecond)

	for _, id := range tc.ids {
		st := tc.node(id).Status()
		wantRole := raft.Follower
		if id == leaderID {
			wantRole = raft.Leader
		}
		if st.Role != wantRole {
			t.Fatalf("node %s: role = %v after 500ms with ticking paused, want unchanged %v", id, st.Role, wantRole)
		}
	}

	if got := tc.awaitLeader(1 * time.Second); got != leaderID {
		t.Fatalf("leader changed from %s to %s while ticking was paused", leaderID, got)
	}
}
