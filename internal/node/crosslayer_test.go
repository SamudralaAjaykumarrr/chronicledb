package node

import (
	"fmt"
	"time"

	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file is Phase 10's cross-layer evidence
// (docs/roadmap.md Phase 10 "CROSS-LAYER TESTS"): high-value sequences
// spanning transactions, Raft, snapshots, and restart together, against
// a real disk/TCP cluster — not merely re-running any single layer's
// own scenario. TestCrossLayer_SnapshotInstallThenElection is the one
// named scenario from that section not already covered by an existing
// Phase 6/7/8 test: TestSN5 (node_test.go) proves a follower catches up
// via a real snapshot install and TestChaos_RepeatedPartitionHealAcrossLeaders
// proves repeated leader changes preserve committed history, but no
// existing test specifically crashes the *leader* immediately after a
// follower has just finished installing a snapshot from it, forcing
// that freshly-caught-up follower to participate in the very next
// election.

// TestCrossLayer_SnapshotInstallThenElection: a follower is isolated,
// falls behind past what the leader is about to compact away, is
// healed and catches up via a genuine InstallSnapshot — then the
// leader itself is crashed immediately afterward, forcing the
// previously-lagging (but now fully caught-up) follower to participate
// in the ensuing election alongside the other survivor. Asserts no
// committed state is lost across the whole sequence (LEADER-COMPLETENESS,
// SNAPSHOT-SAFETY) and the cluster resumes normal operation.
func TestCrossLayer_SnapshotInstallThenElection(t *testing.T) {
	const threshold = 5
	tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(10 * time.Second)
	leader := tc.node(leaderID)

	var follower, other raft.NodeID
	for _, id := range tc.ids {
		if id == leaderID {
			continue
		}
		if follower == "" {
			follower = id
		} else {
			other = id
		}
	}

	// Establish some committed state before the follower falls behind,
	// so LEADER-COMPLETENESS has something concrete to check later.
	preOutcome, err := propose(t, leader, cmd("pre-isolate", 1, 0, "pre-key", "pre-val"), 3*time.Second)
	if err != nil || preOutcome.Status.String() != "committed" {
		t.Fatalf("initial Propose: outcome=%+v err=%v", preOutcome, err)
	}

	tc.isolate(follower)
	for i := 0; i < 3*threshold; i++ {
		if _, err := propose(t, leader, cmd(fmt.Sprintf("fill-%d", i), uint64(i+2), preOutcome.CommitSeq, fmt.Sprintf("k%d", i), "v"), 3*time.Second); err != nil {
			t.Fatalf("fill Propose %d: %v", i, err)
		}
	}
	awaitCondition(t, 10*time.Second, "leader creates a snapshot while the follower is isolated", func() bool {
		return uint64(leader.Status().SnapshotIndex) > 0
	})

	tc.heal(follower)
	awaitCondition(t, 10*time.Second, "isolated follower catches up via a real InstallSnapshot", func() bool {
		return uint64(tc.node(follower).Status().SnapshotIndex) == uint64(leader.Status().SnapshotIndex) && leader.Status().SnapshotIndex > 0
	})

	// Crash the leader immediately once the follower's snapshot install
	// is confirmed — no extra settling time given, so the follower has
	// only just become current.
	tc.crash(leaderID)

	var newLeaderID raft.NodeID
	awaitCondition(t, 10*time.Second, "the two survivors (including the freshly-caught-up follower) elect a new leader", func() bool {
		for _, id := range []raft.NodeID{follower, other} {
			if tc.node(id).Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})
	newLeader := tc.node(newLeaderID)

	// Every key committed before the crash — both the pre-isolation
	// write and everything filled in afterward — must still be visible
	// on the new leader, whether it was reconstructed from the
	// snapshot, the retained log suffix, or (for the follower
	// specifically) a genuine install.
	awaitCondition(t, 5*time.Second, "new leader applies up to the pre-crash state", func() bool {
		return uint64(newLeader.Status().AppliedIndex) >= preOutcome.CommitSeq
	})
	if v, ok := newLeader.FSM().Store().Visible("pre-key", uint64(newLeader.Status().AppliedIndex)); !ok || string(v) != "pre-val" {
		t.Fatalf("new leader lost pre-isolation committed key: got %q,%v", v, ok)
	}
	for i := 0; i < 3*threshold; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, ok := newLeader.FSM().Store().Visible(key, uint64(newLeader.Status().AppliedIndex)); !ok {
			t.Fatalf("new leader lost committed key %s (LEADER-COMPLETENESS violated across a snapshot-install-then-election sequence)", key)
		}
	}

	// The cluster must still accept and replicate new writes afterward.
	outcome, err := propose(t, newLeader, cmd("post-election", 9999, uint64(newLeader.Status().AppliedIndex), "post-key", "post-val"), 3*time.Second)
	if err != nil {
		t.Fatalf("post-election Propose: %v", err)
	}
	for _, id := range []raft.NodeID{follower, other} {
		id := id
		awaitCondition(t, 5*time.Second, fmt.Sprintf("node %s converges on the post-election write", id), func() bool {
			v, ok := tc.node(id).FSM().Store().Visible("post-key", outcome.CommitSeq)
			return ok && string(v) == "post-val"
		})
	}
}
