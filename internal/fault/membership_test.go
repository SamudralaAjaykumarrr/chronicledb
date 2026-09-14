package fault

import (
	"fmt"
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

// TestDM2_SelfRemovalCommitsOnlyOnGenuineCNewMajorityThenLeaderMayCrash
// regresses DM-2 (§15, §4.2): a leader proposing its own removal must
// collect acknowledgements from a genuine majority of C_new — never
// counting itself — both to commit at all (the negative half below)
// and, once it does commit, the remaining voters must be able to make
// progress on their own even if the leader never survives to observe
// its own step-down.
func TestDM2_SelfRemovalCommitsOnlyOnGenuineCNewMajorityThenLeaderMayCrash(t *testing.T) {
	t.Run("does not commit without both remaining voters", func(t *testing.T) {
		cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 2})
		if !cl.SettleElection(50) {
			t.Fatal("no leader emerged")
		}
		a := cl.Leaders()[0]
		commitNoOpAndSettle(cl, a, []byte("x")) // P1
		preCommit := cl.Node(a).Core().CommitIndex()

		var b, c raft.NodeID
		for _, id := range cl.NodeIDs() {
			if id == a {
				continue
			}
			if b == "" {
				b = id
			} else {
				c = id
			}
		}

		// Isolate c before proposing, so only b can ever ack: a
		// majority of C_new = {b,c} requires both.
		cl.IsolateNode(c)
		if err := cl.ProposeConfigChange(a, raft.RemoveServerChange, "remove-a-1", a, ""); err != nil {
			t.Fatalf("ProposeConfigChange: %v", err)
		}
		entryIdx := cl.Node(a).Core().LastIndex()

		// Append-time-effective: a's own activeConfig already excludes
		// a, even though the entry cannot possibly commit yet.
		if cl.Node(a).Core().ActiveConfig().IsMember(a) {
			t.Fatal("a's activeConfig still includes a after proposing its own removal")
		}

		// Deliberately no AdvanceTicks here: c is isolated and must not
		// be allowed to time out into a spurious election of its own,
		// which would confound this negative assertion with an
		// unrelated leadership change. Delivery of b's already-eligible
		// ack needs no ticking.
		cl.DeliverEligible()
		cl.DeliverEligible()

		if got := cl.Node(a).Core().CommitIndex(); got != preCommit {
			t.Fatalf("self-removal entry committed on only one of C_new's two remaining voters: commitIndex went from %d to %d", preCommit, got)
		}
		if cl.Node(a).Core().Role() != raft.Leader {
			t.Fatal("leader stepped down before its self-removal entry actually committed")
		}

		// Healing c lets the genuine majority-of-two assemble, and the
		// entry commits, and only then does a step down. The original
		// AppendEntries to c is still queued (blocked, not dropped), so
		// no new tick is needed to re-trigger it.
		cl.HealAll()
		cl.DeliverEligible()
		cl.DeliverEligible()
		if got := cl.Node(a).Core().CommitIndex(); got < entryIdx {
			t.Fatalf("self-removal entry still not committed after both remaining voters could ack: commitIndex %d, want >= %d", got, entryIdx)
		}
		if cl.Node(a).Core().Role() != raft.Follower {
			t.Fatal("self-removing leader did not step down once its removal genuinely committed")
		}
	})

	t.Run("remaining voters elect among themselves even if the leader crashes before observing its own commit", func(t *testing.T) {
		cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 22})
		if !cl.SettleElection(50) {
			t.Fatal("no leader emerged")
		}
		a := cl.Leaders()[0]
		commitNoOpAndSettle(cl, a, []byte("x")) // P1

		var b, c raft.NodeID
		for _, id := range cl.NodeIDs() {
			if id == a {
				continue
			}
			if b == "" {
				b = id
			} else {
				c = id
			}
		}

		if err := cl.ProposeConfigChange(a, raft.RemoveServerChange, "remove-a-2", a, ""); err != nil {
			t.Fatalf("ProposeConfigChange: %v", err)
		}
		entryIdx := cl.Node(a).Core().LastIndex()

		// Force-deliver exactly the two outbound AppendEntries requests
		// (to b and to c) so both durably append the entry — but never
		// deliver their responses back to a, and never call
		// DeliverEligible, so a never processes an ack and never
		// computes its own commit/step-down. This is the "crashes
		// before self-observation" ordering: a majority of C_new has
		// the entry durably, but the only node that could have computed
		// that fact locally is gone before it gets the chance.
		for _, pm := range cl.Transport().Pending() {
			if pm.Message.To == b || pm.Message.To == c {
				cl.Deliver(pm.ID)
			}
		}
		if got := cl.Node(b).Core().LastIndex(); got != entryIdx {
			t.Fatalf("b never durably appended the self-removal entry: LastIndex %d, want %d", got, entryIdx)
		}
		if got := cl.Node(c).Core().LastIndex(); got != entryIdx {
			t.Fatalf("c never durably appended the self-removal entry: LastIndex %d, want %d", got, entryIdx)
		}
		if cl.Node(a).Core().CommitIndex() >= entryIdx {
			t.Fatal("test setup: a must not have observed commit yet")
		}

		cl.Crash(a)

		newLeader := settleAmong(cl, []raft.NodeID{b, c}, 100)
		if newLeader == "" {
			t.Fatal("remaining voters never elected a leader among themselves")
		}
		if cfg := cl.Node(newLeader).Core().ActiveConfig(); cfg.IsMember(a) {
			t.Fatal("new leader's activeConfig still includes the crashed, self-removed node")
		}

		// Progress continues: a further commit goes through on the
		// two-voter configuration alone, which also drags the
		// previously-uncertain self-removal entry to commit.
		commitNoOpAndSettle(cl, newLeader, []byte("after"))
		if got := cl.Node(newLeader).Core().CommitIndex(); got < entryIdx {
			t.Fatalf("cluster failed to make progress without the crashed self-removed leader: commitIndex %d, want >= %d", got, entryIdx)
		}
	})
}

// TestDM3_PartitionedLearnerDoesNotBlockVoterCommitAndCatchesUpOnceHealed
// regresses DM-3 (§15, LEARNER NON-INTERFERENCE): a learner isolated
// during catch-up must have zero effect on voter-side commit progress,
// and must fully catch up once healed.
func TestDM3_PartitionedLearnerDoesNotBlockVoterCommitAndCatchesUpOnceHealed(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 3})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	a := cl.Leaders()[0]
	commitNoOpAndSettle(cl, a, []byte("x")) // P1

	cl.AddNode("d")
	if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-d", "d", "d:0"); err != nil {
		t.Fatalf("ProposeConfigChange(AddLearner): %v", err)
	}
	for i := 0; i < 20; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if !cl.Node(a).Core().ActiveConfig().IsMember("d") {
		t.Fatal("AddLearner entry never committed against the old (pre-add) voter majority")
	}

	// Isolate the learner before it catches up, then drive ordinary
	// voter-side commits: LEARNER NON-INTERFERENCE means these must
	// proceed at full speed, unaffected by the isolated, lagging
	// learner.
	cl.IsolateNode("d")
	preLearnerIsolationIdx := cl.Node(a).Core().CommitIndex()
	for i := 0; i < 5; i++ {
		commitNoOpAndSettle(cl, a, []byte(fmt.Sprintf("voter-only-%d", i)))
	}
	if got := cl.Node(a).Core().CommitIndex(); got <= preLearnerIsolationIdx {
		t.Fatalf("voter-side commits made no progress while the learner was isolated: commitIndex stayed at %d", got)
	}
	if got := cl.Node("d").Core().LastIndex(); got >= cl.Node(a).Core().LastIndex() {
		t.Fatalf("test setup: learner d should not have caught up while isolated, got LastIndex %d vs leader's %d", got, cl.Node(a).Core().LastIndex())
	}

	// Heal: the learner must catch up fully.
	cl.HealAll()
	for i := 0; i < 40; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	leaderIdx := cl.Node(a).Core().LastIndex()
	if got := cl.Node("d").Core().LastIndex(); got != leaderIdx {
		t.Fatalf("learner d did not fully catch up once healed: LastIndex %d, want %d", got, leaderIdx)
	}
	if !cl.Node("d").Core().ActiveConfig().IsLearner("d") {
		t.Fatal("caught-up node d does not see itself as a learner in its own activeConfig")
	}
}

// TestDM4_RemovalOfIsolatedVoterCommitsWithoutItAndItRemainsHarmless
// regresses DM-4 (§15, §4.3): removing a voter that is currently
// unreachable must still commit via the remaining majority of C_new —
// the removed voter's own acknowledgement is never required — and the
// isolated node, once healed, remains harmless even though it still
// believes itself a member (§4.5/DM-9's guarantee, exercised here from
// the "still isolated at commit time" angle rather than DM-9's
// "already excluded before it ever campaigns" angle).
//
// This test deliberately does NOT assert that c, once healed, adopts
// its own removal: §1.8 references "§4.4's drain semantics" for a
// mechanism that would keep replicating to a departing voter until it
// acknowledges the very entry that removes it, but §4.4's own prose
// never actually specifies that mechanism, and the current
// implementation's replication fan-out (dynamic-membership plan §2.2's
// rows 4/5, `handleHeartbeatTimeout`/`appendLeaderEntry`) targets only
// `activeConfig.Voters`/`Learners` — which already excludes a removed
// member from the very entry that removes it. A removed voter that was
// offline/partitioned at commit time therefore has no way, at Core
// level, to ever learn of its removal via ordinary Raft traffic: it is
// permanently in the "never reconnects" branch of §4.4, which the plan
// itself documents as safe (Lemma 1 + W, §2.7's two rules) but not
// self-healing. This is a real gap between §4.4's prose and the
// implemented mechanism, left as-is here rather than freelanced,
// since closing it would mean changing safety-adjacent replication
// fan-out logic and the dial-address "drain" contract without the same
// multi-revision review the rest of this plan received.
func TestDM4_RemovalOfIsolatedVoterCommitsWithoutItAndItRemainsHarmless(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 30, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 4})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	a := cl.Leaders()[0]
	commitNoOpAndSettle(cl, a, []byte("x")) // P1

	var b, c raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id == a {
			continue
		}
		if b == "" {
			b = id
		} else {
			c = id
		}
	}

	// Isolate c (the target of the removal) before proposing, so it
	// can never acknowledge its own removal entry.
	cl.IsolateNode(c)
	if err := cl.ProposeConfigChange(a, raft.RemoveServerChange, "remove-c", c, ""); err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	entryIdx := cl.Node(a).Core().LastIndex()

	// §4.3: C_new = {a,b}, majority 2 — a and b alone are sufficient;
	// c's acknowledgement is never required.
	cl.DeliverEligible()
	cl.DeliverEligible()
	if got := cl.Node(a).Core().CommitIndex(); got < entryIdx {
		t.Fatalf("removal of an unreachable voter failed to commit via the remaining C_new majority: commitIndex %d, want >= %d", got, entryIdx)
	}
	if cl.Node(a).Core().ActiveConfig().IsMember(c) {
		t.Fatal("removed voter c still appears in the leader's activeConfig after commit")
	}

	// Heal and let c resume campaigning on its stale (pre-removal) view
	// of the configuration — it never receives the removal entry (see
	// the doc comment above), so it still believes itself a Voter and
	// will time out and campaign. Even so, it must remain harmless: no
	// disruption to the live cluster's term or leadership (§2.7 Rule 1:
	// a still-legitimate a/b now excludes c from activeConfig and
	// drops its votes outright).
	cl.HealAll()
	beforeTerm := cl.Node(a).Core().CurrentTerm()
	for i := 0; i < 15; i++ {
		cl.Node(c).Step(raft.Input{Kind: raft.InputElectionTimeout})
		cl.DeliverEligible()
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if got := cl.Node(a).Core().CurrentTerm(); got != beforeTerm {
		t.Fatalf("legitimate leader's term changed from %d to %d due to the removed-but-still-campaigning node c", beforeTerm, got)
	}
	if cl.Node(a).Core().Role() != raft.Leader {
		t.Fatal("legitimate leader was disrupted by the removed node's stale elections")
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
