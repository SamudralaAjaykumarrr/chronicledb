package fault

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// commitNoOpAndSettle forces cl's current leader through an ordinary
// Propose/deliver/settle round so its own current-term entry commits
// (mirroring internal/node.proposeElectionNoOp's effect for test
// purposes, since the simulator drives raft.Core directly and has no
// driver of its own to invoke that production code path).
func commitNoOpAndSettle(cl *Cluster, leader raft.NodeID, data []byte) {
	cl.Propose(leader, data)
	for i := 0; i < 20; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
}

// TestDM12_NewLeaderRefusesSecondTransitionOnInheritedUncommittedTail
// is the deterministic regression for the single most important
// scenario in the dynamic-membership plan (§2.3, §15 DM-12): a newly
// elected leader must not stack a second configuration change onto an
// inherited, possibly-uncommitted tail until it has committed an entry
// of its own current term (P1, §2.2a).
func TestDM12_NewLeaderRefusesSecondTransitionOnInheritedUncommittedTail(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c", "d"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 12})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	leaders := cl.Leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly one leader, got %v", leaders)
	}
	a := leaders[0]
	// Commit a small history under a's term.
	for i := 0; i < 3; i++ {
		commitNoOpAndSettle(cl, a, []byte("x"))
	}
	preLen := cl.Node(a).Core().LastIndex()

	// a proposes RemoveServer(d), replicated to nobody: partition it away
	// immediately after proposing.
	others := otherThree(cl, a)
	cl.Partition([]raft.NodeID{a}, others)
	if err := cl.ProposeConfigChange(a, raft.RemoveServerChange, "remove-d", "d", ""); err != nil {
		t.Fatalf("ProposeConfigChange on a: %v", err)
	}
	if got := cl.Node(a).Core().LastIndex(); got != preLen+1 {
		t.Fatalf("a's own log did not grow by the proposed entry: got %d, want %d", got, preLen+1)
	}

	// The remaining 3 voters (a majority of the original 4-voter
	// configuration) elect a new leader among themselves — none of them
	// ever saw a's uncommitted RemoveServer entry.
	b := settleAmong(cl, others, 100)
	if b == "" {
		t.Fatal("no leader emerged among the remaining voters")
	}

	// b's own log never contained a's uncommitted entry, so P3 alone
	// would pass — this is exactly revision 1's defect. P1 must still
	// refuse: b has not yet committed anything in its own new term.
	if err := cl.ProposeConfigChange(b, raft.RemoveServerChange, "remove-a", "a", ""); err == nil {
		t.Fatal("expected ProposeConfigChange to be refused before b's own current-term commit, got nil error")
	} else if err != raft.ErrConfigChangeNoCurrentTermCommit {
		t.Fatalf("got %v, want ErrConfigChangeNoCurrentTermCommit", err)
	}

	// Nothing entered b's log on the refused attempt.
	lenAfterRefusal := cl.Node(b).Core().LastIndex()

	// Let b commit a current-term entry (mirroring proposeElectionNoOp),
	// then the identical request must now succeed.
	commitNoOpAndSettle(cl, b, []byte("noop"))
	if got := cl.Node(b).Core().LastIndex(); got == lenAfterRefusal {
		t.Fatal("test setup: expected b's log to grow from the no-op commit")
	}
	if err := cl.ProposeConfigChange(b, raft.RemoveServerChange, "remove-a", "a", ""); err != nil {
		t.Fatalf("ProposeConfigChange after b's current-term commit: %v", err)
	}
}

func otherThree(cl *Cluster, exclude raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != exclude {
			out = append(out, id)
		}
	}
	return out
}

// settleAmong advances ticks/delivery (relying on the simulator's own
// randomized election jitter to avoid a simultaneous split vote,
// unlike forcing every candidate's timeout at once) until exactly one
// node in ids reports itself Leader, or maxRounds elapses.
func settleAmong(cl *Cluster, ids []raft.NodeID, maxRounds int) raft.NodeID {
	for i := 0; i < maxRounds; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		var leader raft.NodeID
		count := 0
		for _, id := range ids {
			if cl.Node(id).Core().Role() == raft.Leader {
				count++
				leader = id
			}
		}
		if count == 1 {
			return leader
		}
	}
	return ""
}

// TestDM1_LeaderCrashDuringAddBeforeCommit regresses §5's "after
// proposal, before commit" boundary for a membership change (DM-1): a
// leader crashes after appending an AddLearner entry but before it
// reaches a majority; the cluster's post-recovery state must show the
// entry either fully committed (by whichever node becomes the new
// leader) or entirely absent — never partially adopted by only some
// nodes.
func TestDM1_LeaderCrashDuringAddBeforeCommit(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 1})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	leaders := cl.Leaders()
	a := leaders[0]
	commitNoOpAndSettle(cl, a, []byte("x")) // establish P1 for a

	// Isolate a fully before it can replicate the AddLearner entry to
	// anyone, then propose it (it will sit only in a's own log), then
	// crash a — modeling "proposed, replicated to nobody, then crashed."
	cl.IsolateNode(a)
	if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-x", "x", "x:0"); err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	cl.Crash(a)

	// The remaining two voters (a majority of the original 3) elect a
	// new leader, which never saw the AddLearner entry.
	others := otherThree(cl, a)
	if settleAmong(cl, others, 100) == "" {
		t.Fatal("remaining voters never settled on a leader")
	}

	for _, id := range others {
		cfg := cl.Node(id).Core().ActiveConfig()
		if cfg.IsMember("x") {
			t.Fatalf("node %s adopted the never-replicated AddLearner entry — it must be as if the proposal never happened", id)
		}
	}

	// Heal and restart a: its own divergent, uncommitted tail
	// (the abandoned AddLearner entry) is provably harmless as-is — a
	// quiescent cluster has no further reason to probe a follower's log
	// past what the leader's own heartbeats already cover, exactly like
	// any other uncommitted tail entry (docs/raft.md §3) — but it must
	// be repaired the moment the new leader has anything further to
	// replicate, via the ordinary conflict-hint mechanism, never
	// requiring special membership-specific handling.
	cl.HealAll()
	cl.Restart(a)
	newLeader := others[0]
	if cl.Node(others[0]).Core().Role() != raft.Leader {
		newLeader = others[1]
	}
	commitNoOpAndSettle(cl, newLeader, []byte("y"))
	for i := 0; i < 30; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cfg := cl.Node(a).Core().ActiveConfig(); cfg.IsMember("x") {
		t.Fatal("restarted node a still reports the abandoned AddLearner entry as active once the new leader had something further to replicate")
	}
}

// TestDM9_StaleRemovedNodeCannotDisruptCluster regresses §2.7's two
// rules together (DM-9): a node removed from the cluster, still able
// to send RequestVote traffic, must never disrupt the remaining
// cluster's stability — neither by being granted a vote (Rule 1: it is
// no longer a Voter in anyone's activeConfig) nor by forcing a step-down
// out of a current leader's term (Rule 2: leader-contact suppression).
func TestDM9_StaleRemovedNodeCannotDisruptCluster(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 9})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	leader := cl.Leaders()[0]
	commitNoOpAndSettle(cl, leader, []byte("x")) // P1

	var toRemove raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != leader {
			toRemove = id
			break
		}
	}
	if err := cl.ProposeConfigChange(leader, raft.RemoveServerChange, "remove-1", toRemove, ""); err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	for i := 0; i < 20; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	remainingCfg := cl.Node(leader).Core().ActiveConfig()
	if remainingCfg.IsMember(toRemove) {
		t.Fatalf("removal never committed: %s still a member", toRemove)
	}

	beforeTerm := cl.Node(leader).Core().CurrentTerm()

	// The removed node keeps campaigning at an ever-increasing term —
	// modeled directly by repeatedly forcing its own election timeout —
	// while remaining network-connected to the rest of the cluster.
	for i := 0; i < 10; i++ {
		cl.Node(toRemove).Step(raft.Input{Kind: raft.InputElectionTimeout})
		cl.DeliverEligible()
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}

	if got := cl.Node(leader).Core().CurrentTerm(); got != beforeTerm {
		t.Fatalf("legitimate leader's term changed from %d to %d due to a removed node's stale elections", beforeTerm, got)
	}
	if cl.Node(leader).Core().Role() != raft.Leader {
		t.Fatal("legitimate leader was disrupted by a removed node's stale elections")
	}
}
