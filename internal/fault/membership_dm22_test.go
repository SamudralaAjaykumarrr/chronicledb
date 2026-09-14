package fault

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file implements DM-22 (dynamic-membership plan §15): the
// configuration-lineage oracle's own calibration, proved in both
// directions per §23/F4 and §19 gate 3's discipline ("a regression test
// that cannot fail when the mechanism is removed is not a regression
// test," applied here to the oracle itself rather than to production
// code).
//
//   - TestDM22_ThreeLiveConfigurationsIsSafeAndOracleStaysQuiet drives
//     §2.3's own worked schedule to the exact three-live-configuration
//     snapshot it describes (C0 still on two voters, C1 still on the
//     partitioned pair, C2 live-but-uncommitted on the new leader) and
//     asserts BOTH committedOracle and configLineageOracle stay quiet —
//     proving W1/W2 (branch confinement), never the false "at most two
//     live configurations, adjacent" window revision 2 overstated, which
//     would raise a false alarm on exactly this safe, reachable state.
//   - TestDM22_SyntheticW1ViolationDetectedByLineageOracle drives a
//     genuine two-committed-sibling-configurations trace — reachable
//     only via DM-12's own test-only P1-disable hook, modified so BOTH
//     colliding branches commit an EntryConfig (not a plain no-op) at
//     the same index — and asserts the lineage oracle itself (not just
//     committedOracle, which DM-12 step 7 already proves detects this
//     class of divergence) reports the violation.

// TestDM22_ThreeLiveConfigurationsIsSafeAndOracleStaysQuiet regresses
// DM-22 steps 1-3 (§15, §2.3's worked schedule, §23/F4): three live
// configurations — C0, C1, C2 — is a safe, reachable state (safe by
// Lemma 3, not by any two-element window), and the independent
// configuration-lineage oracle must recognize it as safe rather than
// raising a false alarm.
func TestDM22_ThreeLiveConfigurationsIsSafeAndOracleStaysQuiet(t *testing.T) {
	peers := []raft.NodeID{"a", "b", "c", "d"}
	cl := NewCluster(peers, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 22})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	l1 := cl.Leaders()[0]
	commitNoOpAndSettle(cl, l1, []byte("warmup"))

	cl.AddNode("e")
	if err := cl.ProposeConfigChange(l1, raft.AddLearnerChange, "add-e", "e", "e:0"); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	for i := 0; i < 20; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if !cl.Node(l1).Core().ActiveConfig().IsMember("e") {
		t.Fatal("test setup: AddLearner did not commit")
	}
	commitNoOpAndSettle(cl, l1, []byte("more"))

	// Event 1 (§2.3's table): l1 appends idx=pendingIdx Promote(e) ->
	// C1={a,b,c,d,e}; delivered to e only, so it stays live-but-
	// uncommitted on {l1,e}.
	if err := cl.ProposeConfigChange(l1, raft.PromoteToVoterChange, "promote-e", "e", ""); err != nil {
		t.Fatalf("ProposeConfigChange Promote(e): %v", err)
	}
	pendingIdx := cl.Node(l1).Core().LastIndex()
	for _, pm := range cl.Transport().Pending() {
		if pm.Message.To == "e" {
			cl.Deliver(pm.ID)
		}
	}
	if got := cl.Node("e").Core().LastIndex(); got != pendingIdx {
		t.Fatalf("test setup: e did not receive the pending Promote(e) entry: got %d want %d", got, pendingIdx)
	}
	if cl.Node(l1).Core().CommitIndex() >= pendingIdx {
		t.Fatal("test setup: Promote(e) must not have committed")
	}

	// Event 2: {l1,e} partitioned from the other three original voters.
	var others []raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != l1 && id != "e" {
			others = append(others, id)
		}
	}
	if len(others) != 3 {
		t.Fatalf("test setup: expected 3 remaining original voters, got %v", others)
	}
	cl.Partition([]raft.NodeID{l1, "e"}, others)

	// Event 3: l2 wins term 2 among {others} (majority 3 of C0's 4
	// voters, all three still mutually connected) and commits its own
	// election no-op under C0 — P1 now legitimately holds on l2.
	l2 := settleAmong(cl, others, 200)
	if l2 == "" {
		t.Fatal("the other three voters never elected a leader among themselves")
	}
	if got := cl.Node(l2).Core().LastIndex(); got != pendingIdx-1 {
		t.Fatalf("test setup: new leader %s must not have the pending Promote(e) entry: LastIndex %d, want %d", l2, got, pendingIdx-1)
	}
	commitNoOpAndSettle(cl, l2, []byte("noop-term2"))
	if cl.Node(l2).Core().Role() != raft.Leader {
		t.Fatal("test setup: l2 unexpectedly lost leadership after its own no-op commit")
	}

	// Event 4: l2 appends Remove(l1) -> C2={others}, live only on l2 —
	// deliberately not yet delivered to cNode/dNode.
	if err := cl.ProposeConfigChange(l2, raft.RemoveServerChange, "remove-l1", l1, ""); err != nil {
		t.Fatalf("ProposeConfigChange Remove(l1): %v", err)
	}
	removeIdx := cl.Node(l2).Core().LastIndex()
	if cl.Node(l2).Core().CommitIndex() >= removeIdx {
		t.Fatal("test setup: Remove(l1) must not have committed yet (live only on l2)")
	}

	var cNode, dNode raft.NodeID
	for _, id := range others {
		if id == l2 {
			continue
		}
		if cNode == "" {
			cNode = id
		} else {
			dNode = id
		}
	}

	// Pin the exact three-live-configuration snapshot §2.3's table
	// describes: C0 still on cNode/dNode (l1 a voter, e still a mere
	// learner — the earlier AddLearner committed under the original
	// C0 majority before the partition, but Promote(e) did not), C1
	// still on {l1,e} (e now a voter), C2 live only on l2 (l1 absent
	// entirely).
	if got := cl.Node(cNode).Core().ActiveConfig(); !got.IsVoter(l1) || !got.IsLearner("e") {
		t.Fatalf("test setup: %s must still show C0 (l1 a voter, e a mere learner): %+v", cNode, got)
	}
	if got := cl.Node(dNode).Core().ActiveConfig(); !got.IsVoter(l1) || !got.IsLearner("e") {
		t.Fatalf("test setup: %s must still show C0 (l1 a voter, e a mere learner): %+v", dNode, got)
	}
	if !cl.Node(l1).Core().ActiveConfig().IsVoter("e") {
		t.Fatal("test setup: l1 must still show C1 (e present as a voter)")
	}
	if cl.Node(l2).Core().ActiveConfig().IsMember(l1) {
		t.Fatal("test setup: l2 must show C2 (l1 absent)")
	}

	// Step 2 (§15 DM-22): the oracle must stay quiet on this SAFE
	// three-configuration state — it does not raise the false alarm a
	// two-element-window oracle would.
	committedOrc := newCommittedOracle()
	committedOrc.observe(t, 22, cl)
	lineage := newConfigLineageOracle(peers)
	lineage.observe(t, 22, cl)

	// Step 3: l1's own candidacy under its stale C1 must never again
	// win an election — Lemma 3 observed directly, not merely assumed —
	// and the run must converge on a single surviving chain (C2).
	// Force l1 to campaign immediately upon healing, before Remove(l1)
	// has even reached cNode/dNode, to prove the rejection is by TERM
	// (cNode/dNode already carry l2's term-2 no-op) rather than by
	// l1's absence from a config those nodes have not yet learned about.
	cl.HealAll()
	cl.Node(l1).Step(raft.Input{Kind: raft.InputElectionTimeout})
	for i := 0; i < 100; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		if cl.Node(l1).Core().Role() == raft.Leader {
			t.Fatal("l1's stale C1 candidacy must never win: every C1 majority intersects a C0 majority that now holds a higher term (Lemma 1 + Lemma 3), so isLogUpToDate must reject it by term regardless of log length")
		}
		fake := &recordingFataler{}
		lineage.observe(fake, 22, cl)
		if fake.called {
			t.Fatalf("lineage oracle fired during ordinary post-heal convergence: %s", fake.message)
		}
	}

	// Drive further so the surviving majority (l2, cNode, dNode, and e
	// once its own divergent suffix is repaired) converges on the single
	// committed chain rooted at C2 — l1 itself is the DM-4-precedented
	// exception: a self-... a removed voter that never again receives
	// anything (§4.1/§4.4), so it is deliberately excluded from this
	// convergence check, exactly as DM-4's own test excludes it.
	for i := 0; i < 200; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if got := cl.Node(l2).Core().CommitIndex(); got < removeIdx {
		t.Fatalf("test setup: Remove(l1) never committed once healed: commitIndex %d, want >= %d", got, removeIdx)
	}
	want := cl.Node(l2).Core().ActiveConfig()
	if want.IsMember(l1) {
		t.Fatal("l2's converged configuration still includes the removed l1")
	}
	for _, id := range []raft.NodeID{cNode, dNode, "e"} {
		if got := cl.Node(id).Core().ActiveConfig(); !got.Equal(want) {
			t.Fatalf("node %s failed to converge on the single surviving chain: got %+v, want %+v", id, got, want)
		}
	}

	committedOrc.observe(t, 22, cl)
	lineage.observe(t, 22, cl)
}

// TestDM22_SyntheticW1ViolationDetectedByLineageOracle regresses DM-22
// step 4 (§15): "a synthetic schedule that genuinely violates W1 (two
// committed sibling configurations, reachable only via DM-12's test-only
// P1-disable hook) IS reported by the same oracle." This calibrates the
// lineage oracle specifically — TestDM12Step7 already proves
// committedOracle detects a divergence in this trace family, but that
// trace's two colliding values are an EntryNormal no-op versus a's own
// EntryConfig Promote(e), which committedOracle alone already fully
// catches without ever exercising the lineage oracle's own
// EntryConfig-specific W1 bookkeeping. This variant instead makes BOTH
// colliding branches commit a genuine EntryConfig at the same index —
// b's RemoveServer(a) and a's own Promote(e), both children of the same
// committed anchor C0 — so the lineage oracle's own W1 check (which
// only ever compares committed EntryConfig content, deliberately never
// EntryNormal) is what must fire.
func TestDM22_SyntheticW1ViolationDetectedByLineageOracle(t *testing.T) {
	peers := []raft.NodeID{"a", "b", "c", "d"}
	cl := NewCluster(peers, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 2210})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	a := cl.Leaders()[0]
	commitNoOpAndSettle(cl, a, []byte("warmup"))

	cl.AddNode("e")
	if err := cl.ProposeConfigChange(a, raft.AddLearnerChange, "add-e", "e", "e:0"); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	for i := 0; i < 20; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if !cl.Node(a).Core().ActiveConfig().IsMember("e") {
		t.Fatal("test setup: AddLearner did not commit")
	}
	commitNoOpAndSettle(cl, a, []byte("more"))

	// a proposes Promote(e) -> C1={a,b,c,d,e}; delivered to e only:
	// live-but-uncommitted on {a,e}.
	if err := cl.ProposeConfigChange(a, raft.PromoteToVoterChange, "promote-e", "e", ""); err != nil {
		t.Fatalf("ProposeConfigChange Promote(e): %v", err)
	}
	pendingIdx := cl.Node(a).Core().LastIndex()
	for _, pm := range cl.Transport().Pending() {
		if pm.Message.To == "e" {
			cl.Deliver(pm.ID)
		}
	}
	if got := cl.Node("e").Core().LastIndex(); got != pendingIdx {
		t.Fatalf("test setup: e did not receive the pending Promote(e) entry: got %d want %d", got, pendingIdx)
	}
	if cl.Node(a).Core().CommitIndex() >= pendingIdx {
		t.Fatal("test setup: Promote(e) must not have committed")
	}

	var others []raft.NodeID
	for _, id := range cl.NodeIDs() {
		if id != a && id != "e" {
			others = append(others, id)
		}
	}
	if len(others) != 3 {
		t.Fatalf("test setup: expected 3 remaining original voters, got %v", others)
	}
	cl.Partition([]raft.NodeID{a, "e"}, others)

	newLeaderB := settleAmong(cl, others, 200)
	if newLeaderB == "" {
		t.Fatal("the other three voters never elected a leader among themselves")
	}
	if got := cl.Node(newLeaderB).Core().LastIndex(); got != pendingIdx-1 {
		t.Fatalf("test setup: new leader %s must not have the pending entry: LastIndex %d, want %d", newLeaderB, got, pendingIdx-1)
	}

	// Split the two remaining followers into dNode (cut off from the new
	// leader for the upcoming commit) and cNode (stays connected) —
	// DM-12 step 7's own technique, so newLeaderB's proposal below
	// commits via the two-of-three still reachable to it.
	var dNode, cNode raft.NodeID
	for _, id := range others {
		if id == newLeaderB {
			continue
		}
		if dNode == "" {
			dNode = id
		} else {
			cNode = id
		}
	}
	cl.Transport().IsolateLink(newLeaderB, dNode)
	cl.Transport().IsolateLink(dNode, newLeaderB)
	cl.Transport().IsolateLink(cNode, dNode)
	cl.Transport().IsolateLink(dNode, cNode)

	// With P1 disabled (the sanctioned test-only hook DM-12 already
	// uses), newLeaderB proposes RemoveServer(a) directly — no preceding
	// no-op — so this single EntryConfig entry lands at exactly
	// pendingIdx, the SAME index as a's own Promote(e): a genuine
	// sibling of C0, not a different index.
	cl.Node(newLeaderB).Core().SetSkipP1GateForTest(true)
	if err := cl.ProposeConfigChange(newLeaderB, raft.RemoveServerChange, "remove-a-p1-disabled", a, ""); err != nil {
		t.Fatalf("ProposeConfigChange with P1 disabled: %v", err)
	}
	if got := cl.Node(newLeaderB).Core().LastIndex(); got != pendingIdx {
		t.Fatalf("test setup: newLeaderB's RemoveServer(a) must land at the same index as a's Promote(e): got %d, want %d", got, pendingIdx)
	}
	for i := 0; i < 30; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if got := cl.Node(newLeaderB).Core().CommitIndex(); got < pendingIdx {
		t.Fatalf("test setup: the P1-disabled branch must commit via its own two-of-three: commitIndex %d, want >= %d", got, pendingIdx)
	}

	// Record this branch's committed value in the oracle now, before the
	// second (conflicting) branch commits anything — mirroring
	// TestDM12Step7's own discipline: observing both branches in one
	// call would never exercise the oracle's actual contradiction path.
	lineage := newConfigLineageOracle(peers)
	lineage.observe(t, 2210, cl)

	// Reconnect {a,e} to dNode specifically — never to newLeaderB or
	// cNode — so the two branches never directly observe each other.
	cl.Transport().HealLink(a, dNode)
	cl.Transport().HealLink(dNode, a)
	cl.Transport().HealLink("e", dNode)
	cl.Transport().HealLink(dNode, "e")

	triple := []raft.NodeID{a, dNode, "e"}
	tripleLeader := settleAmong(cl, triple, 400)
	if tripleLeader == "" {
		t.Fatal("a, dNode, and e never settled on a leader among themselves")
	}
	// A marker under the regained leader's own new current term
	// transitively commits the old pending Promote(e) entry at
	// pendingIdx via majority-of-C1 — the same index newLeaderB's branch
	// already committed RemoveServer(a) at.
	cl.Propose(tripleLeader, []byte("term-N-marker"))
	for i := 0; i < 60; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if got := cl.Node(tripleLeader).Core().CommitIndex(); got < pendingIdx {
		t.Fatalf("test setup: the reconnected branch must also commit through pendingIdx: commitIndex %d, want >= %d", got, pendingIdx)
	}

	// The reconnected branch has now committed a DIFFERENT EntryConfig
	// at the very same index newLeaderB's branch already committed one
	// at: two committed sibling configurations of the common anchor C0.
	// This second observation, against the same oracle, must detect it.
	fake := &recordingFataler{}
	lineage.observe(fake, 2210, cl)
	if !fake.called {
		t.Fatal("configLineageOracle did not detect the two-committed-sibling-configurations divergence produced by disabling P1 — this negative control is not exercising the oracle's W1 check")
	}
	t.Logf("lineage oracle correctly detected: %s", fake.message)
}

// TestDM22_CorruptedOracleExpectationDetectedAgainstCorrectSystem is
// DM-22's converse calibration direction (perturbing the ORACLE's own
// expected lineage, not the system): a lineage oracle that never fires
// no matter what is not calibrated — it would pass
// TestDM22_ThreeLiveConfigurationsIsSafeAndOracleStaysQuiet trivially
// while providing no actual protection. This test runs a perfectly
// ordinary, correct membership change to completion, lets the oracle
// record it accurately, then directly corrupts the oracle's own
// recorded expectation (never the system, which remains untouched and
// correct throughout) and asserts the very next comparison against that
// same, still-correct system fails — proving the oracle's comparison
// mechanism is genuinely wired to its own recorded state, not a
// decoration that happens never to disagree.
func TestDM22_CorruptedOracleExpectationDetectedAgainstCorrectSystem(t *testing.T) {
	peers := []raft.NodeID{"a", "b", "c"}
	cl := NewCluster(peers, ClusterOptions{ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Seed: 2211})
	if !cl.SettleElection(50) {
		t.Fatal("no leader emerged")
	}
	leader := cl.Leaders()[0]
	commitNoOpAndSettle(cl, leader, []byte("warmup"))

	cl.AddNode("d")
	if err := cl.ProposeConfigChange(leader, raft.AddLearnerChange, "add-d", "d", "d:0"); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	addIdx := cl.Node(leader).Core().LastIndex()
	for i := 0; i < 20; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
	}
	if got := cl.Node(leader).Core().CommitIndex(); got < addIdx {
		t.Fatalf("test setup: AddLearner(d) never committed: commitIndex %d, want >= %d", got, addIdx)
	}

	lineage := newConfigLineageOracle(peers)
	lineage.observe(t, 2211, cl) // clean: records addIdx's real, correct value; must not fire

	rec, ok := lineage.committed[addIdx]
	if !ok {
		t.Fatalf("test setup: oracle did not record a committed value at index %d", addIdx)
	}
	// Corrupt the oracle's OWN recorded expectation — never the system —
	// by claiming a learner that was never added ("z") was the target
	// instead of "d". The system itself is completely unchanged and
	// remains correct.
	corrupted := rec
	corrupted.Cfg = raft.Configuration{
		Voters:   append([]raft.Member(nil), rec.Cfg.Voters...),
		Learners: []raft.Member{{ID: "z", Address: "z:0"}},
	}
	lineage.committed[addIdx] = corrupted

	fake := &recordingFataler{}
	lineage.observe(fake, 2211, cl)
	if !fake.called {
		t.Fatal("configLineageOracle stayed quiet after its own recorded expectation was corrupted, against an unchanged and correct system — the oracle is not actually calibrated, only ever permissive")
	}
	t.Logf("oracle correctly flagged its own corrupted expectation: %s", fake.message)
}
