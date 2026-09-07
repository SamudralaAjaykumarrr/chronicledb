package node

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestMetricsLeaderChangeAndElectionsCounted proves ElectionsTotal and
// LeaderChangesTotal actually move as a real cluster elects a leader
// (docs/roadmap.md Phase 9 §Observability tests: "elections increment
// counters", "leader changes increment counters").
func TestMetricsLeaderChangeAndElectionsCounted(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(2 * time.Second)
	leader := tc.node(leaderID)

	m := leader.Metrics()
	if m.LeaderChangesTotal == 0 {
		t.Errorf("LeaderChangesTotal = 0, want > 0 after this node became leader")
	}
	// At least one node in the cluster must have run an election to
	// produce any leader at all.
	var totalElections uint64
	for _, id := range tc.ids {
		totalElections += tc.node(id).Metrics().ElectionsTotal
	}
	if totalElections == 0 {
		t.Errorf("total ElectionsTotal across cluster = 0, want > 0")
	}
}

// TestMetricsProposalsCountedByOutcome proves the proposal counters
// track real commit/conflict/rejection events, not just increment
// vacuously (docs/roadmap.md Phase 9 §Benchmark correctness's
// "verify... not benchmarking a no-op" spirit applied to
// observability).
func TestMetricsProposalsCountedByOutcome(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(2 * time.Second)
	leader := tc.node(leaderID)
	follower := tc.node(tc.ids[0])
	if follower == leader {
		follower = tc.node(tc.ids[1])
	}
	// This test asserts on before/after metrics deltas for one specific
	// node across a fixed sequence of Propose calls; it never itself
	// exercises election/failover behavior, so any leadership change
	// mid-sequence (even a legitimate one caused by a host-scheduling
	// stall blowing the real testCluster's timeout budget) would attribute
	// its proposals to the wrong node's counters. See
	// testCluster.pauseTicking's doc comment.
	tc.pauseTicking()

	before := leader.Metrics()

	// A rejected proposal against a follower.
	if _, err := follower.Propose(context.Background(), cmd("reject-1", 1, 0, "k0", "v0")); err == nil {
		t.Fatalf("Propose against follower: expected NotLeaderError")
	}

	// A committed proposal against the leader.
	if _, err := propose(t, leader, cmd("commit-1", 1, 0, "k1", "v1"), 2*time.Second); err != nil {
		t.Fatalf("Propose commit-1: %v", err)
	}

	// A conflicting proposal (StartSeq stale relative to commit-1).
	outcome, err := propose(t, leader, cmd("conflict-1", 2, 0, "k1", "v2"), 2*time.Second)
	if err != nil {
		t.Fatalf("Propose conflict-1: unexpected error %v", err)
	}
	if outcome.Status.String() != "aborted" {
		t.Fatalf("conflict-1 outcome = %v, want aborted", outcome.Status)
	}

	// A duplicate retry of the already-committed RequestID.
	if _, err := leader.Propose(context.Background(), cmd("commit-1", 1, 0, "k1", "v1")); err != nil {
		t.Fatalf("retry commit-1: %v", err)
	}

	after := leader.Metrics()
	if got := after.ProposalsCommittedTotal - before.ProposalsCommittedTotal; got != 1 {
		t.Errorf("ProposalsCommittedTotal delta = %d, want 1", got)
	}
	if got := after.ProposalsAbortedTotal - before.ProposalsAbortedTotal; got != 1 {
		t.Errorf("ProposalsAbortedTotal delta = %d, want 1", got)
	}
	if got := after.RequestIDDuplicatesTotal - before.RequestIDDuplicatesTotal; got != 1 {
		t.Errorf("RequestIDDuplicatesTotal delta = %d, want 1", got)
	}
	followerAfter := follower.Metrics()
	if followerAfter.ProposalsRejectedTotal == 0 {
		t.Errorf("follower ProposalsRejectedTotal = 0, want > 0")
	}
}

// TestMetricsSnapshotCountersMoveOnRealSnapshot proves
// SnapshotsCreatedTotal/SnapshotsInstalledTotal move on a real
// create+install cycle, mirroring TestSN1/TestSN5's own snapshot
// scenarios but asserting on the counters instead of on FSM content.
func TestMetricsSnapshotCountersMoveOnRealSnapshot(t *testing.T) {
	const threshold = 5
	tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(3 * time.Second)
	leader := tc.node(leaderID)

	for i := 0; i < threshold+2; i++ {
		if _, err := propose(t, leader, cmd(fmt.Sprintf("snap-req-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"), 3*time.Second); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	awaitCondition(t, 3*time.Second, "leader creates a snapshot past threshold", func() bool {
		return leader.Metrics().SnapshotsCreatedTotal > 0
	})

	// Isolate a follower, let the leader compact further, then heal so
	// the follower must catch up via a real InstallSnapshot round trip.
	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}
	tc.isolate(followerID)
	for i := threshold + 2; i < 2*threshold+2; i++ {
		_, _ = propose(t, leader, cmd(fmt.Sprintf("snap-req-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"), 500*time.Millisecond)
	}
	tc.heal(followerID)

	follower := tc.node(followerID)
	awaitCondition(t, 5*time.Second, "isolated follower installs a snapshot after healing", func() bool {
		return follower.Metrics().SnapshotsInstalledTotal > 0
	})
}

// TestMetricsRaceSafeConcurrentReadsAndProposals runs Metrics() reads
// concurrently with real proposal traffic under -race (docs/roadmap.md
// Phase 9 §Observability tests: "metrics/status reads are race-safe").
func TestMetricsRaceSafeConcurrentReadsAndProposals(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(2 * time.Second)
	leader := tc.node(leaderID)

	stop := make(chan struct{})
	var readerDone sync.WaitGroup
	readerDone.Add(1)
	go func() {
		defer readerDone.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, id := range tc.ids {
					_ = tc.node(id).Metrics()
					_ = tc.node(id).Status()
				}
			}
		}
	}()

	for i := 0; i < 20; i++ {
		if _, err := propose(t, leader, cmd(fmt.Sprintf("race-%d", i), uint64(i+1), 0, fmt.Sprintf("rk%d", i), "v"), 2*time.Second); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	close(stop)
	readerDone.Wait()
}
