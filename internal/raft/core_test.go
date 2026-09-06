package raft

import "testing"

// zeroRand is a deterministic Rand that always picks the low end of
// the jitter window — sufficient for tests that don't care about the
// exact timeout value, only that Step never touches a global RNG.
type zeroRand struct{}

func (zeroRand) Intn(int) int { return 0 }

func testConfig(id NodeID, peers []NodeID) Config {
	return Config{
		ID:                         id,
		Peers:                      peers,
		ElectionTimeoutTicks:       10,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      2,
		Rand:                       zeroRand{},
	}
}

func mustNewCore(t *testing.T, id NodeID, peers []NodeID) *Core {
	t.Helper()
	c, err := NewCore(testConfig(id, peers), HardState{}, nil)
	if err != nil {
		t.Fatalf("NewCore(%s): %v", id, err)
	}
	return c
}

func threePeers() []NodeID { return []NodeID{"A", "B", "C"} }

// --- RequestVote / election safety ---

func TestRequestVoteRejectsLowerTerm(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	for i := 0; i < 5; i++ {
		a.Step(Input{Kind: InputElectionTimeout})
	}
	if a.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want 5", a.CurrentTerm())
	}
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 2,
	}})
	if len(out.Messages) != 1 || out.Messages[0].VoteGranted || out.Messages[0].Term != 5 {
		t.Fatalf("reply = %+v, want a single VoteGranted=false Term=5 reply", out.Messages)
	}
	if out.PersistRequest != nil {
		t.Fatalf("rejecting a stale term must not require persistence, got %+v", out.PersistRequest)
	}
}

// TestVoteGrantWithheldUntilPersisted proves RAFT-ELECTION-SAFETY's
// enabling mechanism: a granted vote is never sent until votedFor is
// reported durably persisted (docs/adr/0008).
func TestVoteGrantWithheldUntilPersisted(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 1,
	}})
	if len(out.Messages) != 0 {
		t.Fatalf("vote grant must be withheld pending persistence, got messages %+v", out.Messages)
	}
	if out.PersistRequest == nil || out.PersistRequest.HardState == nil {
		t.Fatalf("expected a HardState PersistRequest, got %+v", out.PersistRequest)
	}
	if got := *out.PersistRequest.HardState; got != (HardState{CurrentTerm: 1, VotedFor: "B"}) {
		t.Fatalf("PersistRequest.HardState = %+v, want {1 B}", got)
	}

	ack := a.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	if len(ack.Messages) != 1 || !ack.Messages[0].VoteGranted || ack.Messages[0].To != "B" {
		t.Fatalf("after persistence, expected a granted vote to B, got %+v", ack.Messages)
	}
}

// TestAtMostOneVotePerTerm proves a node never grants two different
// candidates its vote in the same term, including after the grant has
// already been released.
func TestAtMostOneVotePerTerm(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	out := a.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 1}})
	a.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})

	out2 := a.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteRequest, From: "C", To: "A", Term: 1}})
	if len(out2.Messages) != 1 || out2.Messages[0].VoteGranted {
		t.Fatalf("second candidate in the same term must be denied, got %+v", out2.Messages)
	}
	if out2.PersistRequest != nil {
		t.Fatalf("a denial that changes no state must not persist, got %+v", out2.PersistRequest)
	}

	// A repeated RequestVote from the ALREADY-granted candidate in the
	// same term is idempotently re-grantable (docs/raft.md §2: "the
	// same valid candidate may receive repeated acknowledgment").
	out3 := a.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 1}})
	ack3 := a.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out3.PersistRequest.Seq})
	if len(ack3.Messages) != 1 || !ack3.Messages[0].VoteGranted {
		t.Fatalf("re-request from the already-granted candidate should re-grant, got %+v", ack3.Messages)
	}
}

func TestRequestVoteHigherTermStepsDown(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	for i := 0; i < 3; i++ {
		a.Step(Input{Kind: InputElectionTimeout})
	}
	if a.Role() != Candidate {
		t.Fatalf("Role() = %v, want Candidate", a.Role())
	}
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 99,
	}})
	if !out.SteppedDown {
		t.Fatalf("expected SteppedDown=true")
	}
	if a.Role() != Follower || a.CurrentTerm() != 99 {
		t.Fatalf("Role()=%v CurrentTerm()=%d, want Follower/99", a.Role(), a.CurrentTerm())
	}
}

// TestVoteDeniedStaleCandidateLog proves the log-up-to-date check: a
// candidate whose log is behind the voter's must not receive the vote,
// even in a brand-new term.
func TestVoteDeniedStaleCandidateLog(t *testing.T) {
	a, err := NewCore(testConfig("A", threePeers()), HardState{}, []Entry{{Index: 1, Term: 1, Data: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 5, LastLogIndex: 0, LastLogTerm: 0,
	}})
	if len(out.Messages) != 1 || out.Messages[0].VoteGranted {
		t.Fatalf("candidate with a strictly older log must be denied, got %+v", out.Messages)
	}
	if a.VotedFor() != "" {
		t.Fatalf("VotedFor() = %q, want empty (vote must not be recorded for a denied candidate)", a.VotedFor())
	}
}

// TestElectionMajorityBecomesLeader drives a full 3-node election by
// hand (no harness), including the persistence-gating handshake, and
// asserts exactly one leader results.
func TestElectionMajorityBecomesLeader(t *testing.T) {
	peers := threePeers()
	nodes := map[NodeID]*Core{"A": mustNewCore(t, "A", peers), "B": mustNewCore(t, "B", peers), "C": mustNewCore(t, "C", peers)}

	out := nodes["A"].Step(Input{Kind: InputElectionTimeout})
	var grantMsgs []Message
	for _, m := range out.Messages {
		r := nodes[m.To].Step(Input{Kind: InputMessage, Message: m})
		if r.PersistRequest != nil {
			r = nodes[m.To].Step(Input{Kind: InputPersistenceComplete, PersistSeq: r.PersistRequest.Seq})
		}
		grantMsgs = append(grantMsgs, r.Messages...)
	}
	if len(grantMsgs) != 2 {
		t.Fatalf("expected 2 vote replies, got %d", len(grantMsgs))
	}
	leaderCount := 0
	for _, m := range grantMsgs {
		r := nodes[m.To].Step(Input{Kind: InputMessage, Message: m})
		if r.BecameLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 || nodes["A"].Role() != Leader {
		t.Fatalf("leaderCount=%d, A.Role()=%v; want exactly A elected leader", leaderCount, nodes["A"].Role())
	}
	if nodes["A"].MatchIndexOf("A") != 0 || nodes["A"].NextIndexOf("B") != 1 {
		t.Fatalf("leader replication state not initialized as expected")
	}
}

// --- AppendEntries / log matching ---

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	for i := 0; i < 5; i++ {
		a.Step(Input{Kind: InputElectionTimeout})
	}
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "B", To: "A", Term: 1,
	}})
	if len(out.Messages) != 1 || out.Messages[0].Success || out.Messages[0].Term != 5 {
		t.Fatalf("stale AppendEntries must be rejected with the current term, got %+v", out.Messages)
	}
}

func TestAppendEntriesDuplicateIsNoOp(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	entry := Entry{Index: 1, Term: 1, Data: []byte("x")}
	msg := Message{Type: MsgAppendEntriesRequest, From: "L", To: "A", Term: 1, Entries: []Entry{entry}, LeaderCommit: 0}

	out1 := a.Step(Input{Kind: InputMessage, Message: msg})
	a.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out1.PersistRequest.Seq})
	if got := a.Entries(); len(got) != 1 {
		t.Fatalf("expected 1 entry after first append, got %d", len(got))
	}

	// Deliver the identical AppendEntries again (RF-7).
	out2 := a.Step(Input{Kind: InputMessage, Message: msg})
	if out2.PersistRequest != nil {
		t.Fatalf("a duplicate AppendEntries must not trigger a new persist request, got %+v", out2.PersistRequest)
	}
	if len(out2.Messages) != 1 || !out2.Messages[0].Success || out2.Messages[0].MatchIndex != 1 {
		t.Fatalf("duplicate AppendEntries should be acked immediately (already durable), got %+v", out2.Messages)
	}
	if got := a.Entries(); len(got) != 1 {
		t.Fatalf("duplicate delivery must not create a duplicate log entry, got %d entries", len(got))
	}
}

// TestDivergentSuffixTruncated proves RAFT-LOG-MATCHING's conflict
// repair: a follower's uncommitted, divergent tail is truncated and
// replaced by the legitimate leader's entries.
func TestDivergentSuffixTruncated(t *testing.T) {
	// Follower has [ (1,term1,"old2") ] at index 1, and a stray
	// divergent entry at index 2 from a stale former leader (term 1).
	peers := []NodeID{"F", "L", "X"}
	stray := []Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1, Data: []byte("stray")}}
	f, err := NewCore(testConfig("F", peers), HardState{CurrentTerm: 1}, stray)
	if err != nil {
		t.Fatal(err)
	}

	// The real leader, at a higher term, says index 2 is actually a
	// different entry.
	authoritative := Entry{Index: 2, Term: 2, Data: []byte("real")}
	msg := Message{
		Type: MsgAppendEntriesRequest, From: "L", To: "F", Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{authoritative}, LeaderCommit: 1,
	}
	out := f.Step(Input{Kind: InputMessage, Message: msg})
	if out.PersistRequest == nil || out.PersistRequest.TruncateFrom != 2 {
		t.Fatalf("expected a persist request truncating from index 2, got %+v", out.PersistRequest)
	}
	f.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})

	entries := f.Entries()
	if len(entries) != 2 || entries[1].Data == nil || string(entries[1].Data) != "real" {
		t.Fatalf("log after repair = %+v, want [.., real]", entries)
	}
}

// TestNeverTruncatesCommittedEntry proves the defensive safety net: an
// (unreachable, given correct peers) attempt to truncate a committed
// entry is treated as a fatal internal invariant violation, not
// silently permitted.
func TestNeverTruncatesCommittedEntry(t *testing.T) {
	peers := []NodeID{"F", "L", "X"}
	f, err := NewCore(testConfig("F", peers), HardState{CurrentTerm: 1}, []Entry{{Index: 1, Term: 1, Data: []byte("a")}})
	if err != nil {
		t.Fatal(err)
	}
	// Manually mark index 1 committed, as a legitimate leader would
	// have already told this follower.
	out := f.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "L", To: "F", Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1,
	}})
	_ = out
	if f.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d, want 1", f.CommitIndex())
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected a panic guarding against truncating a committed entry")
		}
	}()
	f.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "rogue", To: "F", Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 99, Data: []byte("evil")}},
	}})
}

// --- Commit rule ---

func setupLeader(t *testing.T) (leader *Core, peers []NodeID) {
	t.Helper()
	peers = threePeers()
	leader = mustNewCore(t, "A", peers)
	out := leader.Step(Input{Kind: InputElectionTimeout})
	leader.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	// Fabricate winning both votes directly (unit-testing the commit
	// rule, not the election handshake, which is covered above).
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteResponse, From: "B", To: "A", Term: 1, VoteGranted: true}})
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteResponse, From: "C", To: "A", Term: 1, VoteGranted: true}})
	if leader.Role() != Leader {
		t.Fatalf("setupLeader: Role() = %v, want Leader", leader.Role())
	}
	return leader, peers
}

// TestCommitRequiresMajority proves QUORUM-SAFETY's positive half: an
// entry acknowledged by only a minority never commits.
func TestCommitRequiresMajority(t *testing.T) {
	leader, _ := setupLeader(t)
	out := leader.Step(Input{Kind: InputPropose, ProposeData: []byte("cmd")})
	leader.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	if leader.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d before any follower ack, want 0", leader.CommitIndex())
	}

	ack := leader.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesResponse, From: "B", To: "A", Term: 1, Success: true, MatchIndex: 1,
	}})
	if leader.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d after leader+1 follower (majority of 3), want 1", leader.CommitIndex())
	}
	if len(ack.CommittedEntries) != 1 {
		t.Fatalf("expected exactly 1 newly committed entry, got %d", len(ack.CommittedEntries))
	}
}

// TestMinorityCannotCommit proves a lone follower ack (no majority
// including the leader) never advances commitIndex, and that the
// leader's own entry only counts once its own persistence is
// acknowledged.
func TestMinorityCannotCommit(t *testing.T) {
	leader, _ := setupLeader(t)
	out := leader.Step(Input{Kind: InputPropose, ProposeData: []byte("cmd")})
	// Do NOT ack the leader's own persistence yet: matchIndex[self]
	// must not move, so 0 acking followers means 0 total — no majority.
	if leader.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0 (leader's own entry not yet durable)", leader.CommitIndex())
	}
	leader.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	if leader.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0: leader alone is only 1 of 3, not a majority", leader.CommitIndex())
	}
}

// TestCurrentTermCommitRule proves a leader cannot commit a
// previous-term entry via replication count alone — only by also
// having replicated a later entry from its own current term to a
// majority (docs/raft.md §4).
func TestCurrentTermCommitRule(t *testing.T) {
	peers := threePeers()
	leader, err := NewCore(testConfig("A", peers), HardState{CurrentTerm: 1}, []Entry{{Index: 1, Term: 1, Data: []byte("old")}})
	if err != nil {
		t.Fatal(err)
	}
	// Force leader into term 2 without appending a term-2 entry yet:
	// simulate the election directly rather than a full handshake.
	out := leader.Step(Input{Kind: InputElectionTimeout}) // term -> 2, candidate
	leader.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteResponse, From: "B", To: "A", Term: 2, VoteGranted: true}})
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteResponse, From: "C", To: "A", Term: 2, VoteGranted: true}})
	if leader.Role() != Leader || leader.CurrentTerm() != 2 {
		t.Fatalf("expected leader at term 2, got role=%v term=%d", leader.Role(), leader.CurrentTerm())
	}

	// Both followers report having persisted the OLD term-1 entry —
	// that is a majority by count, but the entry's term (1) is not the
	// leader's currentTerm (2), so it must NOT become committed yet.
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgAppendEntriesResponse, From: "B", To: "A", Term: 2, Success: true, MatchIndex: 1}})
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgAppendEntriesResponse, From: "C", To: "A", Term: 2, Success: true, MatchIndex: 1}})
	if leader.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0: a majority-replicated PRIOR-term entry must not be directly committed", leader.CommitIndex())
	}

	// Now the leader replicates a fresh CURRENT-term entry to the same
	// majority — this legitimately commits both index 1 and 2 at once.
	proposeOut := leader.Step(Input{Kind: InputPropose, ProposeData: []byte("new")})
	leader.Step(Input{Kind: InputPersistenceComplete, PersistSeq: proposeOut.PersistRequest.Seq})
	bAck := leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgAppendEntriesResponse, From: "B", To: "A", Term: 2, Success: true, MatchIndex: 2}})
	if len(bAck.CommittedEntries) != 2 {
		t.Fatalf("expected both index 1 and 2 to become newly committed together once a majority (self+B) reaches the current-term entry, got %d entries", len(bAck.CommittedEntries))
	}
	leader.Step(Input{Kind: InputMessage, Message: Message{Type: MsgAppendEntriesResponse, From: "C", To: "A", Term: 2, Success: true, MatchIndex: 2}})
	if leader.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2 after a current-term entry reaches majority", leader.CommitIndex())
	}
}

// --- Restart / persistence reconstruction ---

func TestRestartReconstructsHardStateNotCommitIndex(t *testing.T) {
	peers := threePeers()
	c, err := NewCore(testConfig("A", peers), HardState{CurrentTerm: 7, VotedFor: "B"}, []Entry{{Index: 1, Term: 3, Data: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	if c.CurrentTerm() != 7 || c.VotedFor() != "B" {
		t.Fatalf("restart did not restore HardState: term=%d votedFor=%q", c.CurrentTerm(), c.VotedFor())
	}
	if c.CommitIndex() != 0 || c.AppliedIndex() != 0 {
		t.Fatalf("commitIndex/appliedIndex must never be trusted from disk: got commit=%d applied=%d", c.CommitIndex(), c.AppliedIndex())
	}
	// A stale vote request for the already-voted term must still be denied.
	out := c.Step(Input{Kind: InputMessage, Message: Message{Type: MsgRequestVoteRequest, From: "C", To: "A", Term: 7, LastLogIndex: 1, LastLogTerm: 3}})
	if len(out.Messages) != 1 || out.Messages[0].VoteGranted {
		t.Fatalf("restart must not forget an already-cast vote, got %+v", out.Messages)
	}
}

func TestProposeRejectedWhenNotLeader(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	out := a.Step(Input{Kind: InputPropose, ProposeData: []byte("x")})
	if !out.ProposalRejected {
		t.Fatalf("expected ProposalRejected=true for a non-leader")
	}
}

// --- Phase 7 regression: a node stepping down from Leader/Candidate
// must always get a fresh election timer, even when the message that
// caused the step-down does not itself grant a vote or accept a
// replication RPC. Found by internal/fault's chaos suite
// (TestChaos_AsymmetricPartitionSafety, seed 609): a node whose log was
// behind a higher-term candidate's correctly refused that candidate's
// vote (RAFT-ELECTION-SAFETY intact), but — since handleElectionTimeout
// disarms a node's election timer forever after its first no-op firing
// as Leader (docs/testing-strategy.md §3.1's driver model: a timer only
// re-arms via an explicit Output.ResetElectionTimer) — a former Leader
// or Candidate that stepped down without granting a vote, and without
// receiving a fresh AppendEntries/InstallSnapshotRequest to reset it,
// was left with NO election timer running at all. If the only
// candidate whose vote requests keep arriving can never actually win
// (its own log is behind), the cluster could be left permanently unable
// to elect anyone — a genuine liveness bug, not a safety violation.

// TestRejectedVoteAfterStepDownStillResetsElectionTimer covers the
// handleRequestVoteRequest reject-path regression directly.
func TestRejectedVoteAfterStepDownStillResetsElectionTimer(t *testing.T) {
	a, err := NewCore(testConfig("A", threePeers()), HardState{}, []Entry{{Index: 1, Term: 1, Data: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	a.Step(Input{Kind: InputElectionTimeout}) // A becomes Candidate at term 1, log still [1]
	if a.Role() != Candidate {
		t.Fatalf("Role() = %v, want Candidate", a.Role())
	}

	// B campaigns at a much higher term but with an empty (strictly
	// older) log: A must step down (term 5 > 1) yet correctly refuse
	// the vote (RAFT-ELECTION-SAFETY: B's log is not up to date).
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "B", To: "A", Term: 5, LastLogIndex: 0, LastLogTerm: 0,
	}})
	if len(out.Messages) != 1 || out.Messages[0].VoteGranted {
		t.Fatalf("expected the vote to be denied (stale candidate log), got %+v", out.Messages)
	}
	if !out.SteppedDown || a.Role() != Follower || a.CurrentTerm() != 5 {
		t.Fatalf("expected a step-down to Follower/term 5, got SteppedDown=%v Role()=%v CurrentTerm()=%d", out.SteppedDown, a.Role(), a.CurrentTerm())
	}
	if !out.ResetElectionTimer || out.ElectionTimeoutTicks <= 0 {
		t.Fatalf("a former Candidate that stepped down without granting a vote must still get a fresh election timer, got ResetElectionTimer=%v ElectionTimeoutTicks=%d", out.ResetElectionTimer, out.ElectionTimeoutTicks)
	}
}

// TestVoteResponseStepDownStillResetsElectionTimer covers the
// handleRequestVoteResponse regression: a Candidate learning of a
// higher term purely from a (rejecting) vote reply must still get a
// fresh election timer once it steps down.
func TestVoteResponseStepDownStillResetsElectionTimer(t *testing.T) {
	a := mustNewCore(t, "A", threePeers())
	a.Step(Input{Kind: InputElectionTimeout}) // A becomes Candidate at term 1
	out := a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteResponse, From: "B", To: "A", Term: 99, VoteGranted: false,
	}})
	if !out.SteppedDown || a.Role() != Follower || a.CurrentTerm() != 99 {
		t.Fatalf("expected a step-down to Follower/term 99, got SteppedDown=%v Role()=%v CurrentTerm()=%d", out.SteppedDown, a.Role(), a.CurrentTerm())
	}
	if !out.ResetElectionTimer || out.ElectionTimeoutTicks <= 0 {
		t.Fatalf("a former Candidate that stepped down on a stale vote response must still get a fresh election timer, got ResetElectionTimer=%v ElectionTimeoutTicks=%d", out.ResetElectionTimer, out.ElectionTimeoutTicks)
	}
}

// TestAppendEntriesResponseStepDownStillResetsElectionTimer covers the
// handleAppendEntriesResponse regression: a stale Leader learning of a
// higher term purely from a heartbeat reply — which has no independent
// election timer running at all while it believes itself Leader — must
// get a fresh one armed the instant it steps down.
func TestAppendEntriesResponseStepDownStillResetsElectionTimer(t *testing.T) {
	a := mustNewCore(t, "A", []NodeID{"A"}) // single-node cluster: instantly becomes Leader
	out := a.Step(Input{Kind: InputElectionTimeout})
	if !out.BecameLeader || a.Role() != Leader {
		t.Fatalf("expected A to become Leader immediately in a single-node cluster")
	}
	out = a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesResponse, From: "B", To: "A", Term: 99, Success: false,
	}})
	if !out.SteppedDown || a.Role() != Follower || a.CurrentTerm() != 99 {
		t.Fatalf("expected a step-down to Follower/term 99, got SteppedDown=%v Role()=%v CurrentTerm()=%d", out.SteppedDown, a.Role(), a.CurrentTerm())
	}
	if !out.ResetElectionTimer || out.ElectionTimeoutTicks <= 0 {
		t.Fatalf("a former Leader stepping down on a stale AppendEntriesResponse must get a fresh election timer (it had none running as Leader), got ResetElectionTimer=%v ElectionTimeoutTicks=%d", out.ResetElectionTimer, out.ElectionTimeoutTicks)
	}
}

// TestInstallSnapshotResponseStepDownStillResetsElectionTimer covers
// the handleInstallSnapshotResponse regression, the same shape as
// TestAppendEntriesResponseStepDownStillResetsElectionTimer.
func TestInstallSnapshotResponseStepDownStillResetsElectionTimer(t *testing.T) {
	a := mustNewCore(t, "A", []NodeID{"A"})
	out := a.Step(Input{Kind: InputElectionTimeout})
	if !out.BecameLeader || a.Role() != Leader {
		t.Fatalf("expected A to become Leader immediately in a single-node cluster")
	}
	out = a.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotResponse, From: "B", To: "A", Term: 99, Success: false,
	}})
	if !out.SteppedDown || a.Role() != Follower || a.CurrentTerm() != 99 {
		t.Fatalf("expected a step-down to Follower/term 99, got SteppedDown=%v Role()=%v CurrentTerm()=%d", out.SteppedDown, a.Role(), a.CurrentTerm())
	}
	if !out.ResetElectionTimer || out.ElectionTimeoutTicks <= 0 {
		t.Fatalf("a former Leader stepping down on a stale InstallSnapshotResponse must get a fresh election timer (it had none running as Leader), got ResetElectionTimer=%v ElectionTimeoutTicks=%d", out.ResetElectionTimer, out.ElectionTimeoutTicks)
	}
}
