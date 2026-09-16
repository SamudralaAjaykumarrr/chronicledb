package node

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// membershipCounts reads n's voter/learner counts through
// Node.MembershipStatus, the event-loop-serialized read path (§9, §14),
// never through the cached Status() snapshot.
//
// The distinction is load-bearing, and reading the cache instead was a
// real -race flake in TestDM19_...: Node.run refreshes the cached
// Status only at the END of each event-loop iteration
// (refreshStatusLocked, after the select arm returns), whereas a
// membership call's caller is released earlier, from resolveWaiter
// inside applyCommitted. So between "RemoveServer returned committed"
// and "the cached Status reflects it" there is a genuine window with no
// happens-before edge, and an assertion reading Status() in that window
// legitimately observes the PREVIOUS configuration.
//
// MembershipStatus has no such window: it dispatches a request onto the
// same single-threaded event loop, so it cannot be serviced until the
// iteration that resolved the caller has finished, and it then computes
// its answer from Core.ActiveConfig() live rather than from any cached
// value. Sending on its channel is the ordering edge the assertion
// needs — which is why this is the right oracle rather than a sleep, a
// retry, or a weakened expectation.
func membershipCounts(t *testing.T, n *Node) (voters, learners int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := n.MembershipStatus(ctx)
	if err != nil {
		t.Fatalf("MembershipStatus: %v", err)
	}
	return len(res.Voters), len(res.Learners)
}

// membershipVoterCount is membershipCounts' voter half.
func membershipVoterCount(t *testing.T, n *Node) int {
	t.Helper()
	voters, _ := membershipCounts(t, n)
	return voters
}

// liveConfig returns n's active raft.Configuration, read through the
// event loop rather than off Core directly.
//
// A live *Node's Core belongs exclusively to its run() goroutine
// (dynamic-membership plan §6.3a's event-loop-only rule), and
// activateFromAppendedEntries writes activeConfig from there the instant
// an EntryConfig is appended. A test that reaches into n.core from the
// test goroutine therefore races that write for real — not theoretically:
// under `-race -tags=integration` it is reported as a genuine DATA RACE
// between Core.ActiveConfig and Core.activateFromAppendedEntries, and it
// was doing so in roughly 1 run in 15 of this package. MembershipStatus
// already dispatches through the same event loop that owns Core, so it
// is the accessor tests must use for a live node. (Core may still be
// touched directly by a harness that runs no event loop at all — see
// newDeterministicLeaderForDM17 — because there is no second goroutine
// to race with there.)
//
// MatchIndex/Lag/Generation are deliberately dropped: this reconstructs
// exactly the ID/Address pairs raft.Configuration carries, so the result
// compares with Configuration.Equal as callers expect.
func liveConfig(t *testing.T, n *Node) raft.Configuration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := n.MembershipStatus(ctx)
	if err != nil {
		t.Fatalf("MembershipStatus: %v", err)
	}
	toMembers := func(ms []MemberStatus) []raft.Member {
		if len(ms) == 0 {
			return nil
		}
		out := make([]raft.Member, 0, len(ms))
		for _, m := range ms {
			out = append(out, raft.Member{ID: m.ID, Address: m.Address})
		}
		return out
	}
	return raft.Configuration{Voters: toMembers(res.Voters), Learners: toMembers(res.Learners)}
}

// mustFinalizeToMax brings tc's cluster all the way to this binary's own
// MaxSupportedGeneration via the leader, so membership operations (which
// require generation >= 2) are legal (dynamic-membership plan §8.2).
func mustFinalizeToMax(t *testing.T, tc *testCluster, leader *Node) {
	t.Helper()
	// Finalization is every caller's precondition, never the thing under
	// test: a multi-round-trip, leader-only sequence (precheck
	// convergence, then one ProposeControl per generation) driven
	// against a leader reference the caller already holds. A spontaneous
	// re-election anywhere inside it deposes that leader and surfaces
	// here as "not leader (leader unknown)" — indistinguishable, from
	// this helper, from a real defect.
	//
	// configFor's election budget is 50-100ms (5+jitter 5 ticks at
	// 10ms). That is ample for ordinary per-entry fsync latency, but
	// under `go test -race -tags=integration ./...` several
	// race-instrumented package binaries run concurrently and a
	// follower's event loop can be descheduled past it, so the
	// re-election is not rare: it cost roughly 1 run in 6 of this
	// package. Freeze the election clock for the duration instead of
	// widening that budget — testCluster.pauseTicking's own doc comment
	// explains why that is the right remedy, and heartbeat ticks keep
	// running so replication and catch-up are unaffected.
	tc.pauseTicking()
	defer tc.resumeTicking()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	awaitPrecheckReady(t, leader, "every node runs this same binary and all are reachable")
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
		voters, learners := membershipCounts(t, leader)
		return voters == 4 && learners == 0
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
		return membershipVoterCount(t, leader) == 3
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
	if got := membershipVoterCount(t, leader); got != 3 {
		t.Fatalf("VoterCount changed on a refused proposal: %d, want unchanged 3", got)
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

// TestDM19_SubThreeVoterConfirmationStaleAndZeroVoterCases extends
// TestRemoveServerRequiresConfirmationBelowThreeVoters with DM-19's
// (§15, §12.2) two remaining sub-cases: a stale confirmVoterCount
// (correct for an earlier cluster size, wrong for the current one) is
// refused just like a missing one, never silently honored; and
// RemoveServer down to zero voters is refused by Core itself
// (ErrLastVoterRemoval) regardless of what confirmVoterCount claims —
// §12.2's two-layer split, with the operator-policy layer
// (confirmVoterCount) and the absolute Core-level invariant each
// exercised independently.
func TestDM19_SubThreeVoterConfirmationStaleAndZeroVoterCases(t *testing.T) {
	t.Run("stale confirmVoterCount refused", func(t *testing.T) {
		// Start from 4 voters so the first removal (4 -> 3) needs no
		// confirmation at all, changing the cluster's size "in between"
		// from the operator's point of view.
		tc := newTestCluster(t, 4)
		leader := tc.leaderNode(5 * time.Second)
		mustFinalizeToMax(t, tc, leader)

		var toRemove []raft.NodeID
		for _, id := range tc.ids {
			if id != leader.cfg.ID {
				toRemove = append(toRemove, id)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 4 -> 3: no confirmation required.
		out1, err := leader.RemoveServer(ctx, "dm19-remove-1", toRemove[0], 0)
		if err != nil {
			t.Fatalf("RemoveServer 4->3: %v", err)
		}
		if out1.Status != fsm.StatusCommitted {
			t.Fatalf("RemoveServer 4->3 outcome = %+v, want Committed", out1)
		}
		if got := membershipVoterCount(t, leader); got != 3 {
			t.Fatalf("VoterCount after first removal = %d, want 3", got)
		}

		// The cluster has now shrunk to 3 voters. An operator who last
		// observed it at 4 voters and (wrongly, but plausibly) still
		// believes a further removal needs no confirmation submits
		// confirmVoterCount=0 — actual resulting count is 2, so this
		// must be refused exactly like a bare missing confirmation,
		// naming the real current count.
		_, err = leader.RemoveServer(ctx, "dm19-remove-2", toRemove[1], 0)
		var confirmErr *ErrConfirmationRequired
		if !errors.As(err, &confirmErr) {
			t.Fatalf("RemoveServer with a stale confirmVoterCount=0: err = %v, want *ErrConfirmationRequired", err)
		}
		if confirmErr.ResultingVoterCount != 2 {
			t.Fatalf("ErrConfirmationRequired.ResultingVoterCount = %d, want 2", confirmErr.ResultingVoterCount)
		}
		if got := membershipVoterCount(t, leader); got != 3 {
			t.Fatalf("VoterCount changed on a refused proposal: %d, want unchanged 3", got)
		}

		// A different stale value (3 — correct for no-confirmation-
		// needed, which is also wrong here) is refused identically.
		_, err = leader.RemoveServer(ctx, "dm19-remove-3", toRemove[1], 3)
		if !errors.As(err, &confirmErr) {
			t.Fatalf("RemoveServer with a stale confirmVoterCount=3: err = %v, want *ErrConfirmationRequired", err)
		}
		if confirmErr.ResultingVoterCount != 2 {
			t.Fatalf("ErrConfirmationRequired.ResultingVoterCount = %d, want 2", confirmErr.ResultingVoterCount)
		}

		// The correct, current confirmVoterCount succeeds.
		out2, err := leader.RemoveServer(ctx, "dm19-remove-4", toRemove[1], 2)
		if err != nil {
			t.Fatalf("RemoveServer with the correct confirmVoterCount=2: %v", err)
		}
		if out2.Status != fsm.StatusCommitted {
			t.Fatalf("RemoveServer outcome = %+v, want Committed", out2)
		}
	})

	t.Run("removal to zero voters refused by Core itself regardless of confirmation", func(t *testing.T) {
		tc := newTestCluster(t, 3)
		leader := tc.leaderNode(5 * time.Second)
		mustFinalizeToMax(t, tc, leader)

		var toRemove []raft.NodeID
		for _, id := range tc.ids {
			if id != leader.cfg.ID {
				toRemove = append(toRemove, id)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 3 -> 2 -> 1, both with correct confirmations, leaving the
		// leader as the cluster's sole voter.
		if _, err := leader.RemoveServer(ctx, "dm19-drain-1", toRemove[0], 2); err != nil {
			t.Fatalf("RemoveServer 3->2: %v", err)
		}
		if _, err := leader.RemoveServer(ctx, "dm19-drain-2", toRemove[1], 1); err != nil {
			t.Fatalf("RemoveServer 2->1: %v", err)
		}
		if got := membershipVoterCount(t, leader); got != 1 {
			t.Fatalf("VoterCount = %d, want 1", got)
		}

		// A wrong confirmVoterCount is refused by the operator-policy
		// layer, exactly as for any other sub-three-voter removal.
		_, err := leader.RemoveServer(ctx, "dm19-drain-3", leader.cfg.ID, 99)
		var confirmErr *ErrConfirmationRequired
		if !errors.As(err, &confirmErr) {
			t.Fatalf("RemoveServer(last voter) with a wrong confirmVoterCount: err = %v, want *ErrConfirmationRequired", err)
		}
		if confirmErr.ResultingVoterCount != 0 {
			t.Fatalf("ErrConfirmationRequired.ResultingVoterCount = %d, want 0", confirmErr.ResultingVoterCount)
		}

		// confirmVoterCount=0 is the *correct* resulting count, so it
		// clears the operator-policy layer — and must still be refused,
		// this time by Core's own absolute, non-overridable invariant.
		_, err = leader.RemoveServer(ctx, "dm19-drain-4", leader.cfg.ID, 0)
		if !errors.Is(err, raft.ErrLastVoterRemoval) {
			t.Fatalf("RemoveServer(last voter) with confirmVoterCount=0: err = %v, want raft.ErrLastVoterRemoval", err)
		}
		if got := membershipVoterCount(t, leader); got != 1 {
			t.Fatalf("VoterCount changed on a refused last-voter removal: %d, want unchanged 1", got)
		}
	})
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
		if got := membershipVoterCount(t, leader1); got != 3 {
			t.Fatalf("VoterCount changed on a refused proposal: %d, want unchanged 3", got)
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

// TestDM18_MembershipChangeGenerationGateBothSides regresses DM-18
// (§15, §8.2, §23/P3): a membership change must be refused before
// generation-2 finalization on the leader-propose side (a live
// assertion, mirroring TestAddLearnerRefusedBeforeGeneration2's own
// coverage, re-asserted here under DM-18's name for direct
// traceability) and, separately and independently, a committed
// EntryConfig delivered to a node whose own durable generation is
// still below 2 must cause that node to fail rather than silently
// accept it (applyConfigEntry's own defense-in-depth check). The
// follower-side half can never be reached via genuine replication — a
// v0.5.0 leader only ever proposes an EntryConfig once its own
// committed generation is already >= 2, and every node necessarily
// applies the generation-2 finalize (an earlier log entry) before it
// ever applies a later EntryConfig — so this half is proven by feeding
// applyConfigEntry a legitimately-encoded EntryConfig entry (harvested
// from a real, properly finalized cluster) directly, against a
// hand-built Node whose clusterGeneration is pinned at 1 and which was
// never started (so calling its unexported apply method directly from
// the test goroutine is race-free).
func TestDM18_MembershipChangeGenerationGateBothSides(t *testing.T) {
	t.Run("leader-side: refused before finalize", func(t *testing.T) {
		tc := newTestCluster(t, 3)
		leader := tc.leaderNode(5 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := leader.AddLearner(ctx, "dm18-add-early", "n4", "127.0.0.1:0")
		if !errors.Is(err, ErrMembershipNotPermitted) {
			t.Fatalf("AddLearner before finalize: err = %v, want ErrMembershipNotPermitted", err)
		}
	})

	t.Run("follower-side: apply-time gate on a synthetic sub-generation-2 node", func(t *testing.T) {
		// Harvest one genuinely-encoded, committed EntryConfig entry
		// from a real, properly finalized cluster.
		tc := newTestCluster(t, 3)
		leader := tc.leaderNode(5 * time.Second)
		mustFinalizeToMax(t, tc, leader)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		outcome, err := leader.AddLearner(ctx, "dm18-add", "harvested-learner", "127.0.0.1:1")
		if err != nil {
			t.Fatalf("AddLearner: %v", err)
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("AddLearner outcome = %+v, want Committed", outcome)
		}
		var entry raft.Entry
		found := false
		for _, e := range leader.core.Entries() {
			if uint64(e.Index) == outcome.CommitSeq {
				entry = e
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("could not locate the committed AddLearner entry at index %d in the leader's own log", outcome.CommitSeq)
		}
		if entry.Type != raft.EntryConfig {
			t.Fatalf("harvested entry Type = %v, want EntryConfig", entry.Type)
		}

		// A minimal, never-started Node: applyConfigEntry only touches
		// n.core (SetApplied) and n.clusterGeneration before it reaches
		// the generation check, so nothing else needs to be real.
		synthCore, err := raft.NewCore(raft.Config{
			ID:                         "synthetic",
			ElectionTimeoutTicks:       10,
			ElectionTimeoutJitterTicks: 5,
			HeartbeatTimeoutTicks:      1,
			Rand:                       rand.New(rand.NewSource(1)),
		}, raft.HardState{}, nil)
		if err != nil {
			t.Fatalf("constructing a standalone raft.Core: %v", err)
		}
		synth := &Node{
			cfg:               Config{ID: "synthetic"},
			core:              synthCore,
			clusterGeneration: 1, // deliberately below the generation-2 requirement
			stopCh:            make(chan struct{}),
		}

		ok := synth.applyConfigEntry(entry)
		if ok {
			t.Fatal("applyConfigEntry accepted a committed EntryConfig on a node whose own durable generation is below 2")
		}
		err = synth.Err()
		if !containsGenerationGateMessage(err) {
			t.Fatalf("applyConfigEntry's fatal error = %v, want a generation-gate message", err)
		}
	})
}

// containsGenerationGateMessage reports whether err's message names the
// generation-2 requirement applyConfigEntry's follower-side gate
// produces — a substring check, since the error is constructed with
// fmt.Errorf rather than a sentinel (dynamic-membership plan §8.2).
func containsGenerationGateMessage(err error) bool {
	return err != nil && strings.Contains(err.Error(), "requires cluster generation >= 2")
}

// TestMembershipStatusIsOrderedAfterACommittedMembershipChange pins the
// ordering property membershipCounts relies on, and that the cached
// Status() deliberately does not provide: once a membership call has
// returned StatusCommitted, a subsequent MembershipStatus must ALREADY
// reflect it, with no polling and no retry.
//
// The edge is structural, not timing: MembershipStatus dispatches onto
// Node.run's single-threaded event loop, so it cannot be serviced until
// the iteration that released the caller has completed, and it computes
// its answer from Core.ActiveConfig() live. Node.run's cached Status is
// refreshed only at the end of each iteration, after the caller has
// already been released from resolveWaiter — which is why reading it
// immediately after a membership call was a real -race flake
// (TestDM19_..., "VoterCount = 2, want 1").
//
// Several changes in a row, each asserted immediately, so a regression
// that only sometimes leaves the read unordered still fails here.
func TestMembershipStatusIsOrderedAfterACommittedMembershipChange(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := 1; i <= 5; i++ {
		learner := raft.NodeID(fmt.Sprintf("ordering-learner-%d", i))
		outcome, err := leader.AddLearner(ctx, fsm.RequestID(fmt.Sprintf("ordering-add-%d", i)), learner, "127.0.0.1:1")
		if err != nil {
			t.Fatalf("AddLearner %s: %v", learner, err)
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("AddLearner %s outcome = %+v, want Committed", learner, outcome)
		}
		voters, learners := membershipCounts(t, leader)
		if voters != 3 || learners != i {
			t.Fatalf("after AddLearner %s returned committed, MembershipStatus reports %d voters / %d learners, want 3/%d — the read was not ordered after the change", learner, voters, learners, i)
		}
	}

	// And the same immediately after a removal, the direction DM-19's
	// own flake was observed in.
	outcome, err := leader.RemoveServer(ctx, "ordering-remove-1", "ordering-learner-1", 0)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("RemoveServer outcome = %+v, want Committed", outcome)
	}
	if voters, learners := membershipCounts(t, leader); voters != 3 || learners != 4 {
		t.Fatalf("after RemoveServer returned committed, MembershipStatus reports %d voters / %d learners, want 3/4", voters, learners)
	}
}
