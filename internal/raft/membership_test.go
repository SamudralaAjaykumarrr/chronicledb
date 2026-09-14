package raft

import (
	"errors"
	"testing"
)

func m(id NodeID) Member { return Member{ID: id, Address: string(id) + ":0"} }

// --- §2.6 four-shape structural validation ---

func TestClassifyTransitionFourShapes(t *testing.T) {
	base := Configuration{Voters: []Member{m("a"), m("b"), m("c")}, Learners: []Member{m("d")}}

	cases := []struct {
		name    string
		newCfg  Configuration
		wantErr bool
	}{
		{
			name:    "AddLearner",
			newCfg:  Configuration{Voters: cloneMembers(base.Voters), Learners: appendMember(base.Learners, m("e"))},
			wantErr: false,
		},
		{
			name:    "PromoteToVoter",
			newCfg:  Configuration{Voters: appendMember(base.Voters, m("d")), Learners: nil},
			wantErr: false,
		},
		{
			name:    "RemoveServer(voter)",
			newCfg:  Configuration{Voters: []Member{m("a"), m("b")}, Learners: cloneMembers(base.Learners)},
			wantErr: false,
		},
		{
			name:    "RemoveServer(learner)",
			newCfg:  Configuration{Voters: cloneMembers(base.Voters), Learners: nil},
			wantErr: false,
		},
		{
			name:    "two voters added at once",
			newCfg:  Configuration{Voters: append(cloneMembers(base.Voters), m("x"), m("y")), Learners: cloneMembers(base.Learners)},
			wantErr: true,
		},
		{
			name:    "voter and learner changed simultaneously",
			newCfg:  Configuration{Voters: append(cloneMembers(base.Voters), m("x")), Learners: nil},
			wantErr: true,
		},
		{
			name:    "identical configuration (no-op is not a legal transition)",
			newCfg:  Configuration{Voters: cloneMembers(base.Voters), Learners: cloneMembers(base.Learners)},
			wantErr: true,
		},
		{
			name:    "remove down to zero voters",
			newCfg:  Configuration{Voters: nil, Learners: cloneMembers(base.Learners)},
			wantErr: true,
		},
		{
			name:    "same id both voter and learner",
			newCfg:  Configuration{Voters: append(cloneMembers(base.Voters), m("d")), Learners: cloneMembers(base.Learners)},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyTransition(base, tc.newCfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("classifyTransition() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// --- ConfigAt invariants (DM-16) ---

func newBootstrapCore(t *testing.T, id NodeID, boot Configuration) *Core {
	t.Helper()
	cfg := Config{ID: id, Bootstrap: boot, ElectionTimeoutTicks: 10, ElectionTimeoutJitterTicks: 5, HeartbeatTimeoutTicks: 2, Rand: zeroRand{}}
	c, err := NewCore(cfg, HardState{}, nil)
	if err != nil {
		t.Fatalf("NewCore: %v", err)
	}
	return c
}

func TestConfigAtNeverSnapshottedFallsBackToBootstrap(t *testing.T) {
	boot := votersConfig([]NodeID{"a", "b", "c"})
	c := newBootstrapCore(t, "a", boot)

	// No EntryConfig entries at all: ConfigAt(0) must be the bootstrap
	// seed, not the zero Configuration (§23/C2's never-snapshotted case).
	got, idx := c.ConfigAt(0)
	if idx != 0 || !got.Equal(boot) {
		t.Fatalf("ConfigAt(0) = %+v, %d; want bootstrap config at index 0", got, idx)
	}
}

func TestConfigAtNeverJoinedIsZeroConfiguration(t *testing.T) {
	c := newBootstrapCore(t, "a", Configuration{})
	if !c.neverJoined() {
		t.Fatal("a Core with an empty Bootstrap and no log must report neverJoined()")
	}
	got, idx := c.ConfigAt(0)
	if idx != 0 || !got.IsZero() {
		t.Fatalf("ConfigAt(0) = %+v, %d; want the zero Configuration", got, idx)
	}
}

func TestConfigAtBoundaryAgreementAfterCompact(t *testing.T) {
	boot := votersConfig([]NodeID{"a", "b", "c"})
	c := newBootstrapCore(t, "a", boot)
	c.becomeLeader(&Output{})
	out, err := c.ProposeConfigChange(AddLearnerChange, "req1", "d", "d:1")
	if err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	applyPersist(t, c, out)
	// Commit the entry directly for test purposes.
	c.commitIndex = c.lastIndex()
	c.SetApplied(c.commitIndex)

	if !c.Compact(c.commitIndex) {
		t.Fatal("Compact failed")
	}
	got, idx := c.ConfigAt(c.SnapshotIndex())
	if idx != 0 {
		t.Fatalf("ConfigAt(snapshotIndex) index = %d, want 0 (boundary value)", idx)
	}
	if !got.Equal(c.activeConfig) {
		t.Fatalf("ConfigAt(snapshotIndex) = %+v, want the config captured at the boundary %+v", got, c.activeConfig)
	}
}

func applyPersist(t *testing.T, c *Core, out Output) {
	t.Helper()
	if out.PersistRequest == nil {
		return
	}
	c.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
}

// --- §2.2a P1/P2/P3 gates ---

func TestProposeConfigChangeRefusesWhenNotLeader(t *testing.T) {
	c := newBootstrapCore(t, "a", votersConfig([]NodeID{"a", "b", "c"}))
	_, err := c.ProposeConfigChange(AddLearnerChange, "r1", "d", "d:1")
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v, want ErrNotLeader", err)
	}
	if c.lastIndex() != 0 {
		t.Fatalf("log length changed on a refused proposal: lastIndex=%d", c.lastIndex())
	}
}

// TestProposeConfigChangeP2InheritedSuffix regresses DM-12's core
// premise: a newly elected leader with an inherited, possibly-uncommitted
// tail refuses to propose until P2 (and P1) are satisfied.
func TestProposeConfigChangeP2InheritedSuffix(t *testing.T) {
	boot := votersConfig([]NodeID{"a", "b", "c"})
	c := newBootstrapCore(t, "a", boot)
	// Simulate an inherited, uncommitted tail: append one entry as if it
	// arrived via AppendEntries, then become leader without committing
	// it.
	c.log = append(c.log, Entry{Index: 1, Term: 1, Data: []byte("x")})
	c.currentTerm = 2
	c.becomeLeader(&Output{})
	if c.commitIndex >= c.pendingConfIndex {
		t.Fatalf("test setup: expected commitIndex (%d) < pendingConfIndex (%d)", c.commitIndex, c.pendingConfIndex)
	}
	_, err := c.ProposeConfigChange(AddLearnerChange, "r1", "d", "d:1")
	if !errors.Is(err, ErrConfigChangeInheritedSuffixUncommitted) {
		t.Fatalf("got %v, want ErrConfigChangeInheritedSuffixUncommitted", err)
	}
	if c.lastIndex() != 1 {
		t.Fatalf("log length changed on a refused proposal: lastIndex=%d", c.lastIndex())
	}
}

// TestProposeConfigChangeP1NoCurrentTermCommit regresses DM-12 directly:
// even once P2 is satisfied (no inherited tail), a leader that has not
// committed anything in its own term must not propose a configuration
// change.
func TestProposeConfigChangeP1NoCurrentTermCommit(t *testing.T) {
	boot := votersConfig([]NodeID{"a", "b", "c"})
	c := newBootstrapCore(t, "a", boot)
	c.currentTerm = 1
	c.becomeLeader(&Output{}) // pendingConfIndex = lastIndex() = 0, so P2 holds trivially
	if c.termAt(c.commitIndex) == c.currentTerm {
		t.Fatal("test setup: expected no current-term commit yet")
	}
	_, err := c.ProposeConfigChange(AddLearnerChange, "r1", "d", "d:1")
	if !errors.Is(err, ErrConfigChangeNoCurrentTermCommit) {
		t.Fatalf("got %v, want ErrConfigChangeNoCurrentTermCommit", err)
	}
	if c.lastIndex() != 0 {
		t.Fatalf("log length changed on a refused proposal: lastIndex=%d", c.lastIndex())
	}
}

func TestProposeConfigChangeP3SerializationInProgress(t *testing.T) {
	c := leaderReadyForConfigChange(t, []NodeID{"a", "b", "c"})
	out, err := c.ProposeConfigChange(AddLearnerChange, "r1", "d", "d:1")
	if err != nil {
		t.Fatalf("first ProposeConfigChange: %v", err)
	}
	applyPersist(t, c, out)
	lenBefore := c.lastIndex()

	_, err = c.ProposeConfigChange(AddLearnerChange, "r2", "e", "e:1")
	if !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("got %v, want ErrConfigChangeInProgress", err)
	}
	if c.lastIndex() != lenBefore {
		t.Fatalf("log length changed on a refused proposal: lastIndex=%d, want %d", c.lastIndex(), lenBefore)
	}
}

// leaderReadyForConfigChange builds a Core that is Leader, has
// committed a current-term entry (satisfying P1), and has no inherited
// uncommitted tail (satisfying P2) — i.e. every proposal precondition
// except the target-specific ones holds.
func leaderReadyForConfigChange(t *testing.T, ids []NodeID) *Core {
	t.Helper()
	boot := votersConfig(ids)
	c := newBootstrapCore(t, ids[0], boot)
	c.currentTerm = 1
	c.becomeLeader(&Output{})
	// Append and immediately mark committed a current-term no-op,
	// mirroring proposeElectionNoOp's effect for test purposes.
	entry := Entry{Index: c.lastIndex() + 1, Term: c.currentTerm, Data: []byte("noop")}
	c.log = append(c.log, entry)
	c.matchIndex[c.cfg.ID] = entry.Index
	for _, v := range boot.Voters {
		c.matchIndex[v.ID] = entry.Index
	}
	c.commitIndex = entry.Index
	c.SetApplied(entry.Index)
	return c
}

func TestMinimumVoterInvariantRefusesLastVoterRemoval(t *testing.T) {
	c := leaderReadyForConfigChange(t, []NodeID{"a"})
	_, err := c.ProposeConfigChange(RemoveServerChange, "r1", "a", "")
	if !errors.Is(err, ErrLastVoterRemoval) {
		t.Fatalf("got %v, want ErrLastVoterRemoval", err)
	}
}

// --- §4.2/§4.2a self-removal exclusion ---

func TestSelfRemovingLeaderExcludedFromOwnCommitQuorum(t *testing.T) {
	c := leaderReadyForConfigChange(t, []NodeID{"a", "b", "c"})
	out, err := c.ProposeConfigChange(RemoveServerChange, "r1", "a", "")
	if err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	applyPersist(t, c, out)
	idx := c.activeConfigIndex
	if c.activeConfig.IsVoter("a") {
		t.Fatal("activeConfig must exclude the self-removing leader immediately on append")
	}

	// Only one of the two remaining voters acks: must NOT commit (needs
	// both — majority(2)=2).
	c.matchIndex["b"] = idx
	c.advanceLeaderCommit(&Output{})
	if c.commitIndex >= idx {
		t.Fatalf("commitIndex advanced to %d with only one of two remaining C_new voters acking", c.commitIndex)
	}
	if c.role != Leader {
		t.Fatal("must not step down before the removal entry commits")
	}

	// Second remaining voter acks: now both of C_new ({b,c}) have it —
	// must commit, and the leader must step down.
	c.matchIndex["c"] = idx
	var out2 Output
	c.advanceLeaderCommit(&out2)
	if c.commitIndex < idx {
		t.Fatalf("commitIndex = %d, want >= %d once both remaining C_new voters ack", c.commitIndex, idx)
	}
	if c.role != Follower || !out2.SteppedDown {
		t.Fatalf("leader must step down exactly at commit: role=%v steppedDown=%v", c.role, out2.SteppedDown)
	}
}

// --- §2.7 Rule 1 / Rule 2 ---

func TestRequestVoteDroppedFromNonMember(t *testing.T) {
	c := newBootstrapCore(t, "a", votersConfig([]NodeID{"a", "b", "c"}))
	beforeTerm := c.currentTerm
	out := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "stranger", To: "a", Term: 99,
	}})
	if c.currentTerm != beforeTerm {
		t.Fatalf("term bumped in response to a non-member's RequestVote: %d -> %d", beforeTerm, c.currentTerm)
	}
	if len(out.Messages) != 0 {
		t.Fatalf("expected no reply to a non-member's RequestVote, got %+v", out.Messages)
	}
}

func TestLearnerNeverGrantsVoteAndNeverCampaigns(t *testing.T) {
	cfg := Configuration{Voters: []Member{m("a"), m("b"), m("c")}, Learners: []Member{m("d")}}
	c := newBootstrapCore(t, "d", cfg)

	// Election timeout must be a no-op for a Learner.
	out := c.Step(Input{Kind: InputElectionTimeout})
	if c.role == Candidate || out.ResetElectionTimer {
		t.Fatalf("learner's election timeout must be a no-op, got role=%v resetTimer=%v", c.role, out.ResetElectionTimer)
	}

	// A Learner must never grant a vote even to an up-to-date candidate.
	voteOut := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "a", To: "d", Term: 1,
	}})
	applyPersist(t, c, voteOut)
	for _, msg := range voteOut.Messages {
		if msg.VoteGranted {
			t.Fatal("a learner must never grant a vote")
		}
	}
	if c.votedFor != noVote {
		t.Fatalf("a learner recorded a vote: votedFor=%q", c.votedFor)
	}
}

// TestNewLearnerFirstCatchUpBatchIncludingItsOwnAddEntry regresses a
// real bug found by real-process integration testing: a brand-new,
// never-joined node's very first AppendEntries batch can legitimately
// contain, among older ordinary entries, the very EntryConfig entry
// that adds it — activateFromAppendedEntries must not treat that as a
// structurally invalid transition merely because this receiver's own
// prior (zero) configuration gives it no independent basis to validate
// against.
func TestNewLearnerFirstCatchUpBatchIncludingItsOwnAddEntry(t *testing.T) {
	c := newBootstrapCore(t, "d", Configuration{}) // never joined: empty Bootstrap
	newCfg := Configuration{Voters: []Member{m("a"), m("b"), m("c")}, Learners: []Member{m("d")}}
	payload := encodeConfigChange(membershipKindAddLearner, "add-d", "d", "d:0", newCfg)
	batch := []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("x")},
		{Index: 2, Term: 1, Type: EntryNormal, Data: []byte("y")},
		{Index: 3, Term: 1, Type: EntryConfig, Data: payload},
	}
	out := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "a", To: "d", Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0, Entries: batch, LeaderCommit: 3,
	}})
	if out.PersistRequest == nil {
		t.Fatal("expected a PersistRequest for a fresh append")
	}
	if !c.activeConfig.Equal(newCfg) {
		t.Fatalf("activeConfig = %+v, want %+v", c.activeConfig, newCfg)
	}
	if c.activeConfigIndex != 3 {
		t.Fatalf("activeConfigIndex = %d, want 3", c.activeConfigIndex)
	}
}

// TestSelfRemovalDoesNotPanicOnLateArrivingResponse regresses a real
// bug found by real-process integration testing: advanceLeaderCommit
// can trigger an immediate self-removal step-down (nilling
// nextIndex/matchIndex) in the middle of handleAppendEntriesResponse/
// handleInstallSnapshotResponse, which must not then unconditionally
// keep indexing those now-nil maps in the same call.
func TestSelfRemovalDoesNotPanicOnLateArrivingResponse(t *testing.T) {
	c := leaderReadyForConfigChange(t, []NodeID{"a", "b", "c"})
	out, err := c.ProposeConfigChange(RemoveServerChange, "self-remove", "a", "")
	if err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}
	applyPersist(t, c, out)
	idx := c.activeConfigIndex

	// b acks first: not yet a majority of {b,c} (needs both).
	c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesResponse, From: "b", To: "a", Term: c.currentTerm,
		Success: true, MatchIndex: idx,
	}})
	if c.role != Leader {
		t.Fatal("must not step down before a genuine C_new majority acks")
	}

	// c's ack arrives, completing the majority and triggering step-down
	// mid-call; this must not panic.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("handling the completing AppendEntriesResponse panicked: %v", r)
			}
		}()
		c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgAppendEntriesResponse, From: "c", To: "a", Term: c.currentTerm,
			Success: true, MatchIndex: idx,
		}})
	}()
	if c.role != Follower {
		t.Fatalf("role after self-removal commit = %v, want Follower", c.role)
	}
}

func TestHeardFromLeaderSuppressesHigherTermRequestVote(t *testing.T) {
	c := newBootstrapCore(t, "a", votersConfig([]NodeID{"a", "b", "c"}))
	// Accept a leader AppendEntries at term 1: heardFromLeader becomes true.
	c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "b", To: "a", Term: 1,
	}})
	if !c.heardFromLeader {
		t.Fatal("accepting AppendEntries from the current leader must set heardFromLeader")
	}

	beforeTerm := c.currentTerm
	out := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgRequestVoteRequest, From: "c", To: "a", Term: beforeTerm + 5,
	}})
	if c.currentTerm != beforeTerm {
		t.Fatalf("term bumped despite fresh leader contact: %d -> %d", beforeTerm, c.currentTerm)
	}
	if len(out.Messages) != 0 || out.PersistRequest != nil {
		t.Fatalf("expected a silently ignored RequestVote while heardFromLeader is true, got %+v", out)
	}

	// Once the election timer fires, heardFromLeader clears and a
	// higher-term vote request is processed normally again.
	c.Step(Input{Kind: InputElectionTimeout})
	if c.heardFromLeader {
		t.Fatal("InputElectionTimeout must clear heardFromLeader")
	}
}
