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

// TestDM5_ElectionDuringInFlightConfigChange regresses DM-5 (§15, §5's
// "candidate election during change"/"after proposal, before commit"
// rows): an election occurring while a configuration change is
// in-flight (uncommitted) must be decided purely by LEADER COMPLETENESS
// exactly as for any other uncommitted entry — no membership-specific
// special case — in both directions: the entry survives if it reached
// a majority of C_new before the election, and is discarded (as if the
// proposal never happened) if it did not.
func TestDM5_ElectionDuringInFlightConfigChange(t *testing.T) {
	t.Run("entry that reached a majority of C_new survives the election and commits normally", func(t *testing.T) {
		cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 12, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 51})
		if !cl.SettleElection(50) {
			t.Fatal("no leader emerged")
		}
		a := cl.Leaders()[0]
		commitNoOpAndSettle(cl, a, []byte("x")) // P1
		others := otherThree(cl, a)

		if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-d", "d", "d:0"); err != nil {
			t.Fatalf("ProposeConfigChange: %v", err)
		}
		entryIdx := cl.Node(a).Core().LastIndex()

		// Let it replicate to (and be acked by) a majority of C_new's
		// voters (unchanged by an AddLearner: still {a,b,c}) before
		// crashing the leader — so the entry has genuinely reached a
		// majority and LEADER COMPLETENESS guarantees any future leader
		// must have it.
		cl.DeliverEligible()
		cl.DeliverEligible()
		for _, id := range others {
			if got := cl.Node(id).Core().LastIndex(); got != entryIdx {
				t.Fatalf("test setup: %s did not receive the entry before the crash: LastIndex %d, want %d", id, got, entryIdx)
			}
		}

		cl.Crash(a)
		newLeader := settleAmong(cl, others, 100)
		if newLeader == "" {
			t.Fatal("remaining voters never elected a leader among themselves")
		}
		if got := cl.Node(newLeader).Core().LastIndex(); got < entryIdx {
			t.Fatalf("new leader's log does not contain an entry that reached a majority of C_new — LEADER COMPLETENESS violated: LastIndex %d, want >= %d", got, entryIdx)
		}
		commitNoOpAndSettle(cl, newLeader, []byte("noop"))
		if got := cl.Node(newLeader).Core().CommitIndex(); got < entryIdx {
			t.Fatalf("entry that reached a majority of C_new failed to eventually commit under the new leader: commitIndex %d, want >= %d", got, entryIdx)
		}
		if !cl.Node(newLeader).Core().ActiveConfig().IsMember("d") {
			t.Fatal("new leader's activeConfig lost the survived AddLearner entry")
		}
	})

	t.Run("entry that never reached a majority is discarded as if it never happened", func(t *testing.T) {
		cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 12, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 52})
		if !cl.SettleElection(50) {
			t.Fatal("no leader emerged")
		}
		a := cl.Leaders()[0]
		commitNoOpAndSettle(cl, a, []byte("x")) // P1
		others := otherThree(cl, a)

		// Isolate a before proposing: the entry sits only in a's own
		// log, replicated to nobody.
		cl.IsolateNode(a)
		if err := cl.ProposeConfigChange(a, raft.RemoveServerChange, "remove-c", others[0], ""); err != nil {
			t.Fatalf("ProposeConfigChange: %v", err)
		}

		newLeader := settleAmong(cl, others, 100)
		if newLeader == "" {
			t.Fatal("remaining voters never elected a leader among themselves")
		}
		if cfg := cl.Node(newLeader).Core().ActiveConfig(); !cfg.IsMember(others[0]) {
			t.Fatal("new leader's activeConfig reflects an entry that never reached a majority — as-if-never-happened violated")
		}

		// The new leader can propose its own change normally — nothing
		// about the abandoned proposal lingers as a serialization
		// obstacle (SERIALIZED MEMBERSHIP CHANGE: at most one
		// outstanding change ever, and the abandoned one was never
		// really outstanding at all once superseded).
		commitNoOpAndSettle(cl, newLeader, []byte("noop"))
		if err := cl.ProposeConfigChange(newLeader, raft.RemoveServerChange, "remove-c-2", others[0], ""); err != nil {
			t.Fatalf("ProposeConfigChange after the abandoned proposal was superseded: %v", err)
		}
	})
}

// TestDM7_CrashRestartAtEveryReconfigurationBoundary regresses DM-7
// (§15, §5): one subtest per row of §5's boundary table that is not
// already covered by a dedicated scenario elsewhere — "before
// configuration proposal" and "after commit, before apply" here;
// "after proposal, before commit" is DM-1, "candidate election during
// change" is DM-5, "leader elected with an inherited uncommitted
// EntryConfig" is DM-12, and the two snapshot rows are DM-13.
func TestDM7_CrashRestartAtEveryReconfigurationBoundary(t *testing.T) {
	t.Run("before configuration proposal: as if the admin call never happened", func(t *testing.T) {
		cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 71})
		if !cl.SettleElection(50) {
			t.Fatal("no leader emerged")
		}
		a := cl.Leaders()[0]
		commitNoOpAndSettle(cl, a, []byte("x")) // P1
		preLen := cl.Node(a).Core().LastIndex()

		// Leader crashes before Core.ProposeConfigChange is ever called —
		// modeled directly by simply never calling it and crashing
		// instead: nothing was attempted, so nothing needs undoing.
		cl.Crash(a)
		others := otherThree(cl, a)
		newLeader := settleAmong(cl, others, 100)
		if newLeader == "" {
			t.Fatal("remaining voters never elected a leader among themselves")
		}
		if got := cl.Node(newLeader).Core().LastIndex(); got < preLen {
			t.Fatalf("new leader's log regressed: LastIndex %d, want >= %d", got, preLen)
		}
		// The operator's RequestID was never used; a fresh attempt
		// against the new leader must be accepted as a genuinely first
		// attempt (no idempotency-table row could possibly exist for
		// it — internal/fault has no FSM, so the only observable proxy
		// here is that ProposeConfigChange itself succeeds normally).
		commitNoOpAndSettle(cl, newLeader, []byte("noop"))
		if err := cl.ProposeConfigChange(newLeader, raft.AddLearnerChange, "never-attempted", "d", "d:0"); err != nil {
			t.Fatalf("ProposeConfigChange for a request that was never even attempted against the crashed leader: %v", err)
		}
	})

	t.Run("after commit, before apply: committed entry survives and is applied deterministically on the new leader", func(t *testing.T) {
		cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 72})
		if !cl.SettleElection(50) {
			t.Fatal("no leader emerged")
		}
		a := cl.Leaders()[0]
		commitNoOpAndSettle(cl, a, []byte("x")) // P1

		if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-d", "d", "d:0"); err != nil {
			t.Fatalf("ProposeConfigChange: %v", err)
		}
		entryIdx := cl.Node(a).Core().LastIndex()
		cl.DeliverEligible()
		cl.DeliverEligible()
		if got := cl.Node(a).Core().CommitIndex(); got < entryIdx {
			t.Fatalf("test setup: entry did not commit before the crash: commitIndex %d, want >= %d", got, entryIdx)
		}

		// Crash the (by construction, LEADER COMPLETENESS-eligible)
		// leader immediately after commit — before internal/fault's
		// equivalent of applyCommitted (there is none; the analogue
		// here is simply that this Core instance is discarded) ever
		// "observes" it locally again.
		cl.Crash(a)
		others := otherThree(cl, a)
		newLeader := settleAmong(cl, others, 100)
		if newLeader == "" {
			t.Fatal("remaining voters never elected a leader among themselves")
		}
		// LEADER COMPLETENESS: the new leader's log necessarily contains
		// the committed entry, and it is applied deterministically —
		// i.e. produces the identical resulting configuration every
		// time, regardless of which surviving node became leader.
		if got := cl.Node(newLeader).Core().LastIndex(); got < entryIdx {
			t.Fatalf("new leader's log lost a committed entry: LastIndex %d, want >= %d", got, entryIdx)
		}
		if !cl.Node(newLeader).Core().ActiveConfig().IsMember("d") {
			t.Fatal("new leader did not deterministically reconstruct the committed AddLearner entry's configuration")
		}

		// Restart the crashed original leader too: its own durable state
		// (the entry was persisted locally before it was ever
		// acknowledged, ordinary write-ahead discipline) reconstructs
		// the identical configuration on recovery.
		cl.Restart(a)
		if cfg := cl.Node(a).Core().ActiveConfig(); !cfg.IsMember("d") {
			t.Fatal("restarted original leader did not durably retain the committed AddLearner entry")
		}
	})
}

// TestDM13_SnapshotBoundaryCapturesConfigAtNotActiveConfig regresses
// DM-13 (§15, §23/C1's "T2 counterexample"): a snapshot boundary must
// capture ConfigAt(N) — the configuration effective strictly at the
// boundary — never activeConfig, which can reflect an uncommitted
// EntryConfig entry above the boundary that may yet be truncated away.
func TestDM13_SnapshotBoundaryCapturesConfigAtNotActiveConfig(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 13})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	a := cl.Leaders()[0]
	for i := 0; i < 4; i++ {
		commitNoOpAndSettle(cl, a, []byte("x"))
	}
	n := cl.Node(a).Core().LastIndex() // N: appliedIndex == commitIndex == N here
	configAtN := cl.Node(a).Core().ActiveConfig()
	if configAtN.IsMember("d") {
		t.Fatal("test setup: d must not be a member yet")
	}

	// Propose an EntryConfig at N+1; deliver it to nobody at all.
	if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-d", "d", "d:0"); err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	if got := cl.Node(a).Core().LastIndex(); got != n+1 {
		t.Fatalf("test setup: entry not appended at N+1: got %d, want %d", got, n+1)
	}
	if !cl.Node(a).Core().ActiveConfig().IsMember("d") {
		t.Fatal("test setup: append-time-effective activation did not fire")
	}

	// Trigger compaction at N (never at N+1: the pending entry is not
	// yet applied, and Compact refuses uptoIndex > appliedIndex).
	if !cl.Compact(a, n) {
		t.Fatal("Compact(N) refused")
	}

	// ConfigAt(snapshotIndex) must equal ConfigAt(N) as captured before
	// the pending entry — the boundary-agreement invariant (§6.3) — and
	// must explicitly differ from activeConfig, which still reflects
	// the pending, uncommitted d.
	if got, _ := cl.Node(a).Core().ConfigAt(n); !got.Equal(configAtN) {
		t.Fatalf("ConfigAt(snapshotIndex) after Compact = %+v, want %+v (the pre-pending-entry configuration)", got, configAtN)
	}
	if !cl.Node(a).Core().ActiveConfig().IsMember("d") {
		t.Fatal("Compact must not have altered activeConfig, which must still reflect the pending entry")
	}

	// Truncate N+1 away: isolate a, let b/c elect a new leader among
	// themselves (neither has the pending entry), and have that leader
	// commit different content at N+1.
	cl.IsolateNode(a)
	others := otherThree(cl, a)
	newLeader := settleAmong(cl, others, 200)
	if newLeader == "" {
		t.Fatal("remaining voters never elected a leader among themselves")
	}
	commitNoOpAndSettle(cl, newLeader, []byte("conflicting"))
	if got := cl.Node(newLeader).Core().LastIndex(); got < n+1 {
		t.Fatalf("test setup: new leader did not commit a conflicting entry at N+1: LastIndex %d", got)
	}

	// Heal: a must truncate its divergent tail and revert activeConfig
	// to ConfigAt(N) via the ordinary divergent-suffix repair path —
	// not a no-op, precisely because snapshotConfig was never
	// (incorrectly) set to the pending activeConfig.
	cl.HealAll()
	for i := 0; i < 40; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	revertedCfg := cl.Node(a).Core().ActiveConfig()
	if revertedCfg.IsMember("d") {
		t.Fatal("a's activeConfig still includes the truncated-away pending AddLearner entry after divergent-suffix repair")
	}
	if !revertedCfg.Equal(configAtN) {
		t.Fatalf("a's reverted activeConfig = %+v, want %+v", revertedCfg, configAtN)
	}

	// Restart from durable state alone: byte-identical to the reverted
	// state.
	cl.Crash(a)
	cl.Restart(a)
	if got := cl.Node(a).Core().ActiveConfig(); !got.Equal(revertedCfg) {
		t.Fatalf("restarted node's activeConfig = %+v, want byte-identical %+v", got, revertedCfg)
	}
}

// TestDM8_InstallSnapshotAdoptsConfigurationUnconditionally regresses
// DM-8 (§15, §7.2): installing a snapshot carrying a Configuration
// different from the receiver's own current one is adopted
// unconditionally — no merge, no backward scan, no attempt to
// reconcile with whatever the receiver believed before.
//
// This covers only Core.handleInstallSnapshotRequest's own contract
// (internal/raft), which is unconditional by construction (§7.2's
// comment: "adopted unconditionally... No merge, no backward scan").
// The companion assertion — that internal/node's driver-level
// msg.Configuration/msg.HasConfiguration vs. the installed file's own
// Meta pairwise check causes Node.fail on disagreement (§23/G8) — is
// already implemented (internal/node/node.go's handleInstallSnapshot)
// but is not covered here: internal/fault has no FSM/snapshot content
// of its own (this package's doc comment) and exercising that check
// safely requires driving a live internal/node.Node through its actual
// message-receipt path without racing its own event-loop goroutine,
// which is out of scope for this deterministic Core-only harness.
func TestDM8_InstallSnapshotAdoptsConfigurationUnconditionally(t *testing.T) {
	cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 8})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	a := cl.Leaders()[0]
	commitNoOpAndSettle(cl, a, []byte("x"))

	var b raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != a {
			b = id
			break
		}
	}
	staleCfg := cl.Node(b).Core().ActiveConfig()
	if staleCfg.IsZero() {
		t.Fatal("test setup: b must already have a real, non-zero configuration to overwrite")
	}

	// A configuration sharing no members at all with b's own stale one
	// — the sharpest possible demonstration that adoption is
	// unconditional replacement, never a union or a partial merge.
	freshCfg := raft.Configuration{Voters: []raft.Member{
		{ID: "x", Address: "x:0"}, {ID: "y", Address: "y:0"}, {ID: "z", Address: "z:0"},
	}}

	term := cl.Node(b).Core().CurrentTerm()
	lastIncluded := cl.Node(b).Core().LastIndex() + 10 // well beyond b's own log: never stale/duplicate
	cl.Node(b).Step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
		Type: raft.MsgInstallSnapshotRequest, From: a, To: b, Term: term,
		LastIncludedIndex: lastIncluded, LastIncludedTerm: term,
		HasConfiguration: true, Configuration: freshCfg,
	}})

	got := cl.Node(b).Core().ActiveConfig()
	if !got.Equal(freshCfg) {
		t.Fatalf("ActiveConfig after InstallSnapshot = %+v, want exactly the installed %+v (unconditional adoption)", got, freshCfg)
	}
	for _, stale := range staleCfg.Voters {
		if got.IsMember(stale.ID) {
			t.Fatalf("a member (%s) of b's pre-install stale configuration survived into the post-install configuration — a merge was attempted where none should occur", stale.ID)
		}
	}

	// HasConfiguration = false must adopt the bootstrap configuration,
	// never the just-discarded prior activeConfig and never freshCfg.
	term2 := cl.Node(b).Core().CurrentTerm()
	cl.Node(b).Step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
		Type: raft.MsgInstallSnapshotRequest, From: a, To: b, Term: term2,
		LastIncludedIndex: lastIncluded + 10, LastIncludedTerm: term2,
		HasConfiguration: false,
	}})
	got2 := cl.Node(b).Core().ActiveConfig()
	if got2.Equal(freshCfg) {
		t.Fatal("HasConfiguration=false install still shows the previous InstallSnapshot's configuration")
	}
	// b's Config.Bootstrap in this harness is the cluster's own original
	// 3-voter set (NewCluster seeds every node's Bootstrap from the
	// initial peer list) — a HasConfiguration=false install falls back
	// to exactly that, never to the discarded prior activeConfig.
	wantBootstrap := raft.Configuration{Voters: []raft.Member{
		{ID: "a", Address: "a:0"}, {ID: "b", Address: "b:0"}, {ID: "c", Address: "c:0"},
	}}
	if !got2.Equal(wantBootstrap) {
		t.Fatalf("HasConfiguration=false install's fallback = %+v, want the bootstrap configuration %+v", got2, wantBootstrap)
	}
}

// TestDM11_DiskFaultDuringEntryConfigAppend regresses DM-11 (§15): a
// genuine local persistence failure targeted specifically at a
// leader's own append of an EntryConfig entry must never let that
// node falsely believe the change succeeded, and the cluster must
// continue correctly afterward — mirroring
// TestChaos_DiskFaultDuringPersistence's existing pattern
// (docs/failure-model.md §1.8), but aimed precisely at a config-change
// entry rather than an ordinary one.
func TestDM11_DiskFaultDuringEntryConfigAppend(t *testing.T) {
	for seed := int64(1100); seed < 1105; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			cl := NewCluster([]raft.NodeID{"a", "b", "c"}, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: seed})
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: no leader emerged", seed)
			}
			a := cl.Leaders()[0]
			commitNoOpAndSettle(cl, a, []byte("x")) // P1

			// Target exactly the leader's own upcoming append of the
			// EntryConfig entry (the very next Append call on its
			// storage).
			cl.Node(a).Storage().FailNextAppends(1)
			if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-d", "d", "d:0"); err != nil {
				t.Fatalf("seed %d: ProposeConfigChange: %v", seed, err)
			}

			if !cl.Node(a).Failed() {
				t.Fatalf("seed %d: leader did not stop itself after its own EntryConfig append failed to persist", seed)
			}
			if cl.Node(a).FailErr() == nil {
				t.Fatalf("seed %d: Failed() true but FailErr() is nil", seed)
			}

			for i := 0; i < 30; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}

			// "Restart" the affected node (real-process crash+restart
			// modeling) and confirm the cluster converges to one
			// consistent, correct state — whichever it turned out to be
			// (the entry may have reached a majority of the other two
			// voters despite the leader's own local failure, or may not
			// have; both are legitimate, and either is fine, matching
			// TestChaos_DiskFaultDuringPersistence's convergence-only
			// assertion, not a fixed expected outcome).
			cl.Crash(a)
			cl.Restart(a)
			for i := 0; i < 60; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			if !cl.SettleElection(100) {
				t.Fatalf("seed %d: cluster never settled on a single leader after recovery", seed)
			}
			newLeader := cl.Leaders()[0]
			commitNoOpAndSettle(cl, newLeader, []byte("after-recovery"))
			finalIdx := cl.Node(newLeader).Core().CommitIndex()
			for _, id := range cl.NodeIDs() {
				if cl.Node(id).Crashed() {
					continue
				}
				for i := 0; i < 30 && cl.Node(id).Core().CommitIndex() < finalIdx; i++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
				}
				if got := cl.Node(id).Core().CommitIndex(); got < finalIdx {
					t.Fatalf("seed %d: node %s never caught up after recovery: commitIndex %d, want >= %d", seed, id, got, finalIdx)
				}
			}
			// Every live node must agree on the resulting configuration
			// lineage — no node stuck believing a divergent membership
			// state.
			want := cl.Node(newLeader).Core().ActiveConfig()
			for _, id := range cl.NodeIDs() {
				if got := cl.Node(id).Core().ActiveConfig(); !got.Equal(want) {
					t.Fatalf("seed %d: node %s's post-recovery configuration = %+v, want %+v (agreement with the leader)", seed, id, got, want)
				}
			}
		})
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
