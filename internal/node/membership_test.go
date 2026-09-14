package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// mustFinalizeToMax brings tc's cluster all the way to this binary's own
// MaxSupportedGeneration via the leader, so membership operations (which
// require generation >= 2) are legal (dynamic-membership plan §8.2).
func mustFinalizeToMax(t *testing.T, tc *testCluster, leader *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	awaitCondition(t, 5*time.Second, "precheck reports Ready", func() bool {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := leader.UpgradePrecheck(c)
		return err == nil && res.Ready
	})
	finalizeToMax(t, leader, ctx)
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" converges on max generation", func() bool {
			return tc.node(id).Status().ClusterGeneration == leader.Status().MaxSupportedGeneration
		})
	}
}

func TestAddLearnerRefusedBeforeGeneration2(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := leader.AddLearner(ctx, "add1", "n4", "127.0.0.1:0")
	if !errors.Is(err, ErrMembershipNotPermitted) {
		t.Fatalf("AddLearner before finalize: err = %v, want ErrMembershipNotPermitted", err)
	}
}

// TestAddLearnerPromoteRemoveFullLifecycle exercises the complete
// add -> catch up -> promote -> remove sequence end to end against real
// processes (dynamic-membership plan §3, §4), using an in-process
// fourth Node joining an already-running 3-node cluster exactly as a
// real new process would: empty Peers, started fresh, learning its
// Configuration purely by replication (§3.1).
func TestAddLearnerPromoteRemoveFullLifecycle(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	// Commit a few ordinary writes first so the new learner has real
	// catch-up to do.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, err := propose(t, leader, cmd(fmt.Sprintf("r%d", i), uint64(i+1), 0, key, "v"), 3*time.Second); err != nil {
			t.Fatalf("propose #%d: %v", i, err)
		}
	}

	// Start the new learner process: empty Peers (§3.1), listening on its
	// own address, with the existing cluster's addresses as PeerAddrs so
	// its transport knows how to dial/accept them once it is named in a
	// Configuration.
	newAddr := freeAddrs(t, 1)[0]
	learnerID := raft.NodeID("n4")
	peerAddrs := make(map[raft.NodeID]string, len(tc.ids))
	for _, id := range tc.ids {
		peerAddrs[id] = tc.addrs[id]
	}
	learnerDir := t.TempDir()
	learner, err := Open(Config{
		ID:                         learnerID,
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 newAddr,
		DataDir:                    learnerDir,
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening new learner process: %v", err)
	}
	defer learner.Stop()
	if st := learner.Status(); st.VoterCount != 0 || st.LearnerCount != 0 {
		t.Fatalf("a brand-new, not-yet-added learner process must report the zero configuration, got VoterCount=%d LearnerCount=%d", st.VoterCount, st.LearnerCount)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := leader.AddLearner(ctx, "add-n4", learnerID, newAddr)
	if err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("AddLearner outcome = %+v, want Committed", outcome)
	}

	// Idempotent retry with the same RequestID must return the same
	// outcome without re-proposing (§10).
	outcome2, err := leader.AddLearner(ctx, "add-n4", learnerID, newAddr)
	if err != nil || outcome2 != outcome {
		t.Fatalf("AddLearner retry = %+v, %v; want identical %+v, nil", outcome2, err, outcome)
	}

	awaitCondition(t, 5*time.Second, "learner catches up to the leader's last index", func() bool {
		return learner.Status().AppliedIndex >= uint64(leader.Status().LastIndex)
	})
	awaitCondition(t, 5*time.Second, "learner reports itself in the active configuration", func() bool {
		st := learner.Status()
		return st.VoterCount == 3 && st.LearnerCount == 1
	})

	// Promote: should succeed once caught up.
	var promoted fsm.Outcome
	awaitCondition(t, 5*time.Second, "PromoteToVoter eventually succeeds", func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		defer pcancel()
		o, err := leader.PromoteToVoter(pctx, "promote-n4", learnerID, 0)
		if err != nil {
			var lag *ErrLearnerNotCaughtUp
			if errors.As(err, &lag) {
				return false // retryable, keep polling
			}
			t.Fatalf("PromoteToVoter: unexpected error %v", err)
		}
		promoted = o
		return true
	})
	if promoted.Status != fsm.StatusCommitted {
		t.Fatalf("PromoteToVoter outcome = %+v, want Committed", promoted)
	}
	awaitCondition(t, 5*time.Second, "cluster converges on 4 voters", func() bool {
		return leader.Status().VoterCount == 4 && leader.Status().LearnerCount == 0
	})

	// Remove one of the ORIGINAL three voters (not the new one), leaving
	// 3 voters — no confirmation required.
	var toRemove raft.NodeID
	for _, id := range tc.ids {
		if id != leader.cfg.ID {
			toRemove = id
			break
		}
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	removeOutcome, err := leader.RemoveServer(rctx, "remove-1", toRemove, 0)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if removeOutcome.Status != fsm.StatusCommitted {
		t.Fatalf("RemoveServer outcome = %+v, want Committed", removeOutcome)
	}
	awaitCondition(t, 5*time.Second, "cluster converges on 3 voters after removal", func() bool {
		return leader.Status().VoterCount == 3
	})
}

// TestRemoveServerRequiresConfirmationBelowThreeVoters pins §12.2.
func TestRemoveServerRequiresConfirmationBelowThreeVoters(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	var target raft.NodeID
	for _, id := range tc.ids {
		if id != leader.cfg.ID {
			target = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := leader.RemoveServer(ctx, "remove-noconfirm", target, 0)
	var confirmErr *ErrConfirmationRequired
	if !errors.As(err, &confirmErr) {
		t.Fatalf("RemoveServer without confirmVoterCount: err = %v, want *ErrConfirmationRequired", err)
	}
	if confirmErr.ResultingVoterCount != 2 {
		t.Fatalf("ErrConfirmationRequired.ResultingVoterCount = %d, want 2", confirmErr.ResultingVoterCount)
	}
	if leader.Status().VoterCount != 3 {
		t.Fatalf("VoterCount changed on a refused proposal: %d, want unchanged 3", leader.Status().VoterCount)
	}

	// Same RequestID, now with the correct confirmation — must succeed
	// as a fresh attempt (§10: a refused-before-proposal request records
	// nothing).
	outcome, err := leader.RemoveServer(ctx, "remove-noconfirm", target, 2)
	if err != nil {
		t.Fatalf("RemoveServer with confirmVoterCount=2: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("RemoveServer outcome = %+v, want Committed", outcome)
	}
}

// TestSelfRemovingLeaderStepsDownAndClusterElectsNewLeader is the
// real-process counterpart to the raft-level self-removal unit test
// (dynamic-membership plan §4.2, §16).
func TestSelfRemovingLeaderStepsDownAndClusterElectsNewLeader(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)
	leaderID := leader.cfg.ID
	mustFinalizeToMax(t, tc, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := leader.RemoveServer(ctx, "self-remove", leaderID, 2)
	if err != nil {
		t.Fatalf("self RemoveServer: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("self RemoveServer outcome = %+v, want Committed", outcome)
	}

	awaitCondition(t, 5*time.Second, "the self-removed former leader steps down", func() bool {
		return leader.Status().Role != raft.Leader
	})

	// A propose against the now-removed node must fail with ErrNodeRemoved,
	// not a generic NotLeaderError (§4.6).
	if _, err := propose(t, leader, cmd("after-removal", 999, 0, "k", "v"), time.Second); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("Propose against a self-removed node: err = %v, want ErrNodeRemoved", err)
	}

	// The remaining two nodes must elect a new leader and keep serving.
	var remaining []raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			remaining = append(remaining, id)
		}
	}
	awaitCondition(t, 5*time.Second, "remaining voters elect a new leader", func() bool {
		for _, id := range remaining {
			if tc.node(id).Status().Role == raft.Leader {
				return true
			}
		}
		return false
	})
}

// TestMembershipRequestIDRetryAcrossLeaderFailover is the real-process
// counterpart of DM-6 (§15, §10): a membership RequestID's outcome must
// resolve identically regardless of which node answers a retry, across
// a genuine leader failover, and a pre-proposal refusal must record
// nothing so the same RequestID remains freshly usable against whoever
// becomes leader next.
func TestMembershipRequestIDRetryAcrossLeaderFailover(t *testing.T) {
	t.Run("committed outcome survives failover and a retry does not re-propose", func(t *testing.T) {
		tc := newTestCluster(t, 3)
		leader1 := tc.leaderNode(5 * time.Second)
		leader1ID := leader1.cfg.ID
		mustFinalizeToMax(t, tc, leader1)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		outcome1, err := leader1.AddLearner(ctx, "dm6-add", "x", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("AddLearner: %v", err)
		}
		if outcome1.Status != fsm.StatusCommitted {
			t.Fatalf("AddLearner outcome = %+v, want Committed", outcome1)
		}

		tc.crash(leader1ID)
		newLeaderID := tc.awaitLeader(5 * time.Second)
		leader2 := tc.node(newLeaderID)
		awaitCondition(t, 5*time.Second, "new leader applies the already-committed AddLearner entry into its FSM outcome table", func() bool {
			return leader2.Status().AppliedIndex >= outcome1.CommitSeq
		})
		lastIndexBeforeRetry := leader2.Status().LastIndex

		outcome2, err := leader2.AddLearner(ctx, "dm6-add", "x", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("retry against new leader after failover: %v", err)
		}
		if outcome2 != outcome1 {
			t.Fatalf("retry outcome %+v != original outcome %+v — must resolve identically regardless of which node answers", outcome2, outcome1)
		}
		if got := leader2.Status().LastIndex; got != lastIndexBeforeRetry {
			t.Fatalf("idempotent retry re-proposed a duplicate entry: LastIndex went from %d to %d", lastIndexBeforeRetry, got)
		}

		// A conflicting request reusing the same RequestID for a
		// different target must be rejected distinctly, never silently
		// returning the unrelated original outcome.
		if _, err := leader2.AddLearner(ctx, "dm6-add", "y", "127.0.0.1:1"); !errors.Is(err, fsm.ErrRequestIDConflict) {
			t.Fatalf("AddLearner with a conflicting fingerprint under a reused RequestID: err = %v, want ErrRequestIDConflict", err)
		}
	})

	t.Run("a pre-proposal refusal records nothing and the same RequestID succeeds fresh after failover", func(t *testing.T) {
		tc := newTestCluster(t, 3)
		leader1 := tc.leaderNode(5 * time.Second)
		leader1ID := leader1.cfg.ID
		mustFinalizeToMax(t, tc, leader1)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var target raft.NodeID
		for _, id := range tc.ids {
			if id != leader1ID {
				target = id
				break
			}
		}

		// Refused before ever proposing (§12.2/§2.6a): no confirmVoterCount
		// against a 3-voter cluster.
		_, err := leader1.RemoveServer(ctx, "dm6-remove", target, 0)
		var confirmErr *ErrConfirmationRequired
		if !errors.As(err, &confirmErr) {
			t.Fatalf("RemoveServer without confirmVoterCount: err = %v, want *ErrConfirmationRequired", err)
		}
		if leader1.Status().VoterCount != 3 {
			t.Fatalf("VoterCount changed on a refused proposal: %d, want unchanged 3", leader1.Status().VoterCount)
		}

		tc.crash(leader1ID)
		newLeaderID := tc.awaitLeader(5 * time.Second)
		leader2 := tc.node(newLeaderID)

		// Retarget the removal to the crashed original leader itself:
		// removing anyone else would need a majority of a C_new that
		// still includes the crashed, unreachable leader1 (§4.3's C_new
		// majority rule), which could never commit — an unrelated
		// liveness dead-end this test must not trip over.
		target = leader1ID

		// The same RequestID, now against the new leader, with the
		// correct confirmation, must succeed as a genuinely fresh
		// attempt — nothing was recorded by the earlier refusal.
		// Bounded-poll, retrying on the transient post-election
		// not-ready window (§11/§2.2a's P1 gate): a brand-new leader
		// has not yet committed an entry of its own current term, and
		// this is expected to fire transiently right after failover
		// (docs/testing-strategy.md §4's bounded-polling discipline).
		var outcome fsm.Outcome
		awaitCondition(t, 5*time.Second, "RemoveServer retry eventually succeeds once the new leader clears the P1 gate", func() bool {
			rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
			defer rcancel()
			o, err := leader2.RemoveServer(rctx, "dm6-remove", target, 2)
			if err != nil {
				if errors.Is(err, raft.ErrConfigChangeNoCurrentTermCommit) || errors.Is(err, raft.ErrConfigChangeInheritedSuffixUncommitted) {
					return false // retryable, keep polling
				}
				t.Fatalf("RemoveServer retry with the same RequestID after failover: unexpected error %v", err)
			}
			outcome = o
			return true
		})
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("RemoveServer retry outcome = %+v, want Committed", outcome)
		}
	})
}
