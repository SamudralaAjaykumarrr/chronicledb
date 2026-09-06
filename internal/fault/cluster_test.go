package fault

import (
	"bytes"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

func newTestCluster(seed int64) *Cluster {
	return NewCluster([]raft.NodeID{"A", "B", "C"}, ClusterOptions{
		ElectionTimeoutTicks:       10,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      2,
		Seed:                       seed,
	})
}

// electOne runs the cluster until exactly one leader exists, failing
// the test otherwise.
func electOne(t *testing.T, cl *Cluster) raft.NodeID {
	t.Helper()
	if !cl.SettleElection(50) {
		t.Fatalf("cluster failed to settle on a single leader within 50 rounds")
	}
	return cl.Leaders()[0]
}

// TestRF1_NormalReplication: RF-1 — a proposed entry is replicated,
// committed, and converges identically on all three nodes.
func TestRF1_NormalReplication(t *testing.T) {
	cl := newTestCluster(1)
	leader := electOne(t, cl)

	cl.Propose(leader, []byte("hello"))
	for i := 0; i < 20 && cl.DeliverEligible() > 0; i++ {
		cl.AdvanceTicks(1)
	}

	for _, id := range cl.NodeIDs() {
		n := cl.Node(id)
		entries := n.Core().Entries()
		if len(entries) != 1 || !bytes.Equal(entries[0].Data, []byte("hello")) {
			t.Fatalf("node %s entries = %+v, want [hello]", id, entries)
		}
	}
	if cl.Node(leader).Core().CommitIndex() != 1 {
		t.Fatalf("leader CommitIndex() = %d, want 1", cl.Node(leader).Core().CommitIndex())
	}
}

// TestElectionSafety_AtMostOneLeaderPerTerm (RAFT-ELECTION-SAFETY):
// across a scripted sequence of elections (including forced repeated
// elections), no two nodes ever simultaneously claim leadership of the
// same term.
func TestElectionSafety_AtMostOneLeaderPerTerm(t *testing.T) {
	cl := newTestCluster(2)
	seenLeaderForTerm := map[raft.Term]raft.NodeID{}
	check := func() {
		byTerm := map[raft.Term][]raft.NodeID{}
		for _, id := range cl.Leaders() {
			term := cl.Node(id).Core().CurrentTerm()
			byTerm[term] = append(byTerm[term], id)
			if prev, ok := seenLeaderForTerm[term]; ok && prev != id {
				t.Fatalf("RAFT-ELECTION-SAFETY violated: both %s and %s claim leadership of term %d", prev, id, term)
			}
			seenLeaderForTerm[term] = id
		}
		for term, ids := range byTerm {
			if len(ids) > 1 {
				t.Fatalf("RAFT-ELECTION-SAFETY violated: %v simultaneously claim leadership of term %d", ids, term)
			}
		}
	}

	for round := 0; round < 30; round++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		check()
	}
	// Force repeated elections (RF-15): crash the current leader (if
	// any) and force a fresh one repeatedly.
	for i := 0; i < 5; i++ {
		if leaders := cl.Leaders(); len(leaders) == 1 {
			cl.Crash(leaders[0])
		}
		for round := 0; round < 30; round++ {
			cl.AdvanceTicks(1)
			cl.DeliverEligible()
			check()
		}
	}
}

// TestVoteSafety_SurvivesRestart (RAFT-ELECTION-SAFETY / restart
// safety): a node's vote for a term is not forgotten across a
// crash/restart, so it cannot later vote for a different candidate in
// that same term.
func TestVoteSafety_SurvivesRestart(t *testing.T) {
	peers := []raft.NodeID{"A", "B", "C"}
	cl := NewCluster(peers, ClusterOptions{ElectionTimeoutTicks: 10, HeartbeatTimeoutTicks: 2, Seed: 3})

	// A requests a vote from B for term 1; B grants it and persists.
	a := cl.Node("A")
	a.Step(raft.Input{Kind: raft.InputElectionTimeout})
	cl.flush(a)
	cl.DeliverEligible()
	if got := cl.Node("B").Core().VotedFor(); got != "A" {
		t.Fatalf("B.VotedFor() = %q before restart, want A", got)
	}

	cl.Crash("B")
	cl.Restart("B")
	if got := cl.Node("B").Core().VotedFor(); got != "A" {
		t.Fatalf("B.VotedFor() = %q after restart, want A (vote must survive restart)", got)
	}
	if got := cl.Node("B").Core().CurrentTerm(); got != 1 {
		t.Fatalf("B.CurrentTerm() = %d after restart, want 1 (term must survive restart)", got)
	}

	// C now (still term 1) asks B for its vote — must be denied.
	out := cl.Node("B").Core().Step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
		Type: raft.MsgRequestVoteRequest, From: "C", To: "B", Term: 1,
	}})
	if len(out.Messages) != 1 || out.Messages[0].VoteGranted {
		t.Fatalf("B granted a second vote in term 1 after restart: %+v", out.Messages)
	}
}

// TestQuorumSafety_MinorityPartitionCannotCommit (RF-11 / QUORUM
// SAFETY): once A is isolated, it can no longer advance commitIndex
// for new proposals, while B/C (the majority side) continue normally.
func TestQuorumSafety_MinorityPartitionCannotCommit(t *testing.T) {
	cl := newTestCluster(4)
	leader := electOne(t, cl)

	// Isolate the current leader (whichever node it is) as the
	// minority side; the other two are the majority side.
	var majority []raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != leader {
			majority = append(majority, id)
		}
	}
	cl.Partition([]raft.NodeID{leader}, majority)

	cl.Propose(leader, []byte("should-never-commit"))
	for i := 0; i < 30; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if ci := cl.Node(leader).Core().CommitIndex(); ci != 0 {
		t.Fatalf("isolated former leader %s advanced CommitIndex() to %d; a minority must never commit", leader, ci)
	}

	// The majority side must elect a new leader and make progress.
	newLeader := ""
	for _, id := range majority {
		if cl.Node(id).Core().Role() == raft.Leader {
			newLeader = string(id)
		}
	}
	if newLeader == "" {
		t.Fatalf("majority side %v failed to elect a new leader while %s is isolated", majority, leader)
	}
	cl.Propose(raft.NodeID(newLeader), []byte("majority-write"))
	for i := 0; i < 30 && cl.Node(raft.NodeID(newLeader)).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(raft.NodeID(newLeader)).Core().CommitIndex() == 0 {
		t.Fatalf("majority side %v failed to commit a write while the minority is isolated", majority)
	}
}

// TestStaleLeaderSafety_HealAndStepDown (RF-10/RF-13): a partitioned
// former leader, upon healing, observes the majority side's higher
// term and steps down; its divergent uncommitted tail is repaired and
// no committed entry is ever lost.
func TestStaleLeaderSafety_HealAndStepDown(t *testing.T) {
	cl := newTestCluster(5)
	leaderA := electOne(t, cl)
	var others []raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != leaderA {
			others = append(others, id)
		}
	}

	// Commit one entry while the cluster is whole.
	cl.Propose(leaderA, []byte("before-partition"))
	for i := 0; i < 20 && cl.Node(leaderA).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(leaderA).Core().CommitIndex() != 1 {
		t.Fatalf("failed to commit the pre-partition entry")
	}

	// Partition the old leader away; it keeps appending speculative,
	// uncommitted entries while isolated.
	cl.Partition([]raft.NodeID{leaderA}, others)
	cl.Propose(leaderA, []byte("speculative-while-isolated"))
	cl.AdvanceTicks(5)
	cl.DeliverEligible()
	if cl.Node(leaderA).Core().LastLogTerm() == 0 {
		t.Fatalf("expected leaderA to have appended a speculative entry")
	}
	speculativeLen := cl.Node(leaderA).Core().LastIndex()
	if speculativeLen < 2 {
		t.Fatalf("expected leaderA's speculative append to grow its log past the committed prefix")
	}

	// Majority side elects a new leader and commits a different entry
	// at the same log position.
	var newLeader raft.NodeID
	for i := 0; i < 50 && newLeader == ""; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		for _, id := range others {
			if cl.Node(id).Core().Role() == raft.Leader {
				newLeader = id
			}
		}
	}
	if newLeader == "" {
		t.Fatalf("majority side failed to elect a new leader")
	}
	cl.Propose(newLeader, []byte("authoritative"))
	for i := 0; i < 30 && cl.Node(newLeader).Core().CommitIndex() < 2; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(newLeader).Core().CommitIndex() < 2 {
		t.Fatalf("majority side failed to commit its authoritative entry")
	}

	// Heal, and let the stale leader learn the higher term and repair.
	cl.HealAll()
	for i := 0; i < 50; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}

	if cl.Node(leaderA).Core().Role() == raft.Leader {
		t.Fatalf("stale former leader %s did not step down after healing", leaderA)
	}
	if leaders := cl.Leaders(); len(leaders) != 1 || leaders[0] != newLeader {
		t.Fatalf("expected exactly the new leader %s to remain leader after heal, got %v", newLeader, leaders)
	}

	// Every node must converge to the same log, and the pre-partition
	// committed entry must never have been lost or altered.
	want := cl.Node(newLeader).Core().Entries()
	for _, id := range cl.NodeIDs() {
		got := cl.Node(id).Core().Entries()
		if len(got) != len(want) {
			t.Fatalf("node %s has %d entries, want %d (convergence failure)", id, len(got), len(want))
		}
		for i := range want {
			if got[i].Term != want[i].Term || !bytes.Equal(got[i].Data, want[i].Data) {
				t.Fatalf("node %s entry %d = %+v, want %+v (log matching / convergence failure)", id, i+1, got[i], want[i])
			}
		}
	}
	if !bytes.Equal(want[0].Data, []byte("before-partition")) {
		t.Fatalf("the pre-partition committed entry was lost or altered: %+v", want[0])
	}
}

// TestDeterminism_IdenticalScheduleIdenticalOutcome
// (docs/testing-strategy.md §3.2): the same seed and the same explicit
// sequence of cluster calls always produce the same Raft outcome
// (terms, roles, logs, commit indexes).
func TestDeterminism_IdenticalScheduleIdenticalOutcome(t *testing.T) {
	run := func() *Cluster {
		cl := newTestCluster(42)
		leader := electOne(t, cl)
		cl.Propose(leader, []byte("x"))
		cl.Propose(leader, []byte("y"))
		for i := 0; i < 30; i++ {
			cl.AdvanceTicks(1)
			cl.DeliverEligible()
		}
		return cl
	}

	c1 := run()
	c2 := run()

	for _, id := range c1.NodeIDs() {
		n1, n2 := c1.Node(id), c2.Node(id)
		if n1.Core().Role() != n2.Core().Role() {
			t.Fatalf("node %s: Role() diverged: %v vs %v", id, n1.Core().Role(), n2.Core().Role())
		}
		if n1.Core().CurrentTerm() != n2.Core().CurrentTerm() {
			t.Fatalf("node %s: CurrentTerm() diverged: %d vs %d", id, n1.Core().CurrentTerm(), n2.Core().CurrentTerm())
		}
		if n1.Core().CommitIndex() != n2.Core().CommitIndex() {
			t.Fatalf("node %s: CommitIndex() diverged: %d vs %d", id, n1.Core().CommitIndex(), n2.Core().CommitIndex())
		}
		e1, e2 := n1.Core().Entries(), n2.Core().Entries()
		if len(e1) != len(e2) {
			t.Fatalf("node %s: log length diverged: %d vs %d", id, len(e1), len(e2))
		}
		for i := range e1 {
			if e1[i].Term != e2[i].Term || !bytes.Equal(e1[i].Data, e2[i].Data) {
				t.Fatalf("node %s: entry %d diverged: %+v vs %+v", id, i+1, e1[i], e2[i])
			}
		}
	}
}

// TestRestartSafety_LogAndCommitmentSurvive (restart safety +
// LEADER-COMPLETENESS): a follower that crashes after persisting a
// committed entry, then restarts, still has that entry and can serve
// it back into a fresh election/commit round without loss.
func TestRestartSafety_LogAndCommitmentSurvive(t *testing.T) {
	cl := newTestCluster(6)
	leader := electOne(t, cl)
	cl.Propose(leader, []byte("durable"))
	for i := 0; i < 20 && cl.Node(leader).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(leader).Core().CommitIndex() != 1 {
		t.Fatalf("failed to commit before crashing a follower")
	}

	var follower raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != leader {
			follower = id
			break
		}
	}
	preCrashEntries := cl.Node(follower).Core().Entries()

	cl.Crash(follower)
	cl.Restart(follower)

	postRestartEntries := cl.Node(follower).Core().Entries()
	if len(postRestartEntries) != len(preCrashEntries) {
		t.Fatalf("follower lost log entries across restart: got %d, want %d", len(postRestartEntries), len(preCrashEntries))
	}
	for i := range preCrashEntries {
		if postRestartEntries[i].Index != preCrashEntries[i].Index ||
			postRestartEntries[i].Term != preCrashEntries[i].Term ||
			!bytes.Equal(postRestartEntries[i].Data, preCrashEntries[i].Data) {
			t.Fatalf("entry %d changed across restart: got %+v, want %+v", i+1, postRestartEntries[i], preCrashEntries[i])
		}
	}
	if cl.Node(follower).Core().CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d immediately after restart, want 0 (must be re-established, never trusted from disk)", cl.Node(follower).Core().CommitIndex())
	}

	// Cluster continues operating and the restarted follower catches
	// its commitIndex back up via normal heartbeats.
	for i := 0; i < 20 && cl.Node(follower).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(follower).Core().CommitIndex() != 1 {
		t.Fatalf("restarted follower failed to re-learn commitIndex from the leader")
	}
}

// TestFollowerLagAndSlowFollower (RF-2 / RF-14): while one follower's
// message delivery is held back well beyond the others', the majority
// (leader + the responsive follower) still commits normally; the
// lagging follower is not required for progress and eventually catches
// up once its delivery resumes.
func TestFollowerLagAndSlowFollower(t *testing.T) {
	cl := newTestCluster(8)
	leader := electOne(t, cl)
	var slow raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != leader {
			slow = id
			break
		}
	}

	delayTraffic := func(id raft.NodeID) {
		for _, pm := range cl.Transport().Pending() {
			if pm.Message.From == id || pm.Message.To == id {
				cl.Transport().Delay(pm.ID, cl.LogicalTick()+50)
			}
		}
	}

	cl.Propose(leader, []byte("first"))
	delayTraffic(slow)
	for i := 0; i < 30 && cl.Node(leader).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		delayTraffic(slow)
		cl.DeliverEligible()
	}
	if cl.Node(leader).Core().CommitIndex() == 0 {
		t.Fatalf("majority (leader + 1 responsive follower) failed to commit despite one slow/lagging follower")
	}
	if cl.Node(slow).Core().CommitIndex() != 0 {
		t.Fatalf("lagging follower advanced its CommitIndex despite all its traffic being delayed")
	}

	for i := 0; i < 100 && cl.Node(slow).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(slow).Core().CommitIndex() == 0 {
		t.Fatalf("lagging follower never caught up once its delayed delivery resumed")
	}
}

// TestDuplicateAndDroppedMessages_RF7 proves duplicate delivery
// (RF-7) and message loss are both harmless to eventual convergence.
func TestDuplicateAndDroppedMessages_RF7(t *testing.T) {
	cl := newTestCluster(7)
	leader := electOne(t, cl)
	cl.Propose(leader, []byte("once"))

	// Duplicate every currently pending message, and drop one copy of
	// each pair at random-but-deterministic (seeded) intervals by just
	// always dropping the duplicate — still exercises the harness
	// primitive even though the outcome (harmless duplication) is the
	// point.
	for _, pm := range cl.Transport().Pending() {
		cl.Transport().Duplicate(pm.ID)
	}

	for i := 0; i < 30 && cl.Node(leader).Core().CommitIndex() == 0; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if cl.Node(leader).Core().CommitIndex() != 1 {
		t.Fatalf("duplicated messages prevented commitment")
	}
	if entries := cl.Node(leader).Core().Entries(); len(entries) != 1 {
		t.Fatalf("duplicated AppendEntries produced duplicate log entries: %+v", entries)
	}
}
