package raft

import (
	"errors"
	"fmt"
	"math/rand"
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

// TestConfigAtInvariantsProperty regresses DM-16 (§15, §6.3): a
// randomized property test over §6.3's four stated invariants
// (determinism, monotone provenance, prefix stability, boundary
// agreement), across logs with and without a snapshot boundary and
// with and without a bootstrap seed.
func TestConfigAtInvariantsProperty(t *testing.T) {
	for seed := int64(1600); seed < 1640; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			withBootstrap := seed%3 != 0

			var boot Configuration
			if withBootstrap {
				boot = votersConfig([]NodeID{"a", "b", "c"})
			}
			c := newBootstrapCore(t, "a", boot)
			c.becomeLeader(&Output{})

			length := 6 + rnd.Intn(20)
			compactAtStep := -1
			if seed%2 == 0 && length > 3 {
				compactAtStep = 1 + rnd.Intn(length-2)
			}
			nextLearner := 0
			for i := 0; i < length; i++ {
				var out Output
				var err error
				if rnd.Intn(3) == 0 {
					id := NodeID(fmt.Sprintf("L%d", nextLearner))
					nextLearner++
					out, err = c.ProposeConfigChange(AddLearnerChange, fmt.Sprintf("req%d", i), id, string(id)+":0")
				} else {
					out = c.appendLeaderEntry(EntryNormal, []byte("x"))
				}
				if err != nil {
					continue // an occasionally-refused proposal is fine; just skip this step
				}
				applyPersist(t, c, out)
				c.commitIndex = c.lastIndex()
				c.SetApplied(c.commitIndex)
				if i == compactAtStep {
					if !c.Compact(c.commitIndex) {
						t.Fatalf("seed %d: Compact(%d) refused", seed, c.commitIndex)
					}
				}
			}

			// Determinism + monotone provenance, and cross-checked
			// against an independently reconstructed second Core built
			// from exactly the same durable state.
			c2, err := NewCoreFromSnapshot(c.cfg, HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor},
				c.snapshotIndex, c.snapshotTerm, c.snapshotConfig, c.snapshotHasConfig, c.Entries())
			if err != nil {
				t.Fatalf("seed %d: NewCoreFromSnapshot: %v", seed, err)
			}
			for i := c.SnapshotIndex(); i <= c.LastIndex(); i++ {
				cfg1, k1 := c.ConfigAt(i)
				cfg1b, k1b := c.ConfigAt(i)
				if !cfg1.Equal(cfg1b) || k1 != k1b {
					t.Fatalf("seed %d: ConfigAt(%d) not deterministic within one Core: (%+v,%d) vs (%+v,%d)", seed, i, cfg1, k1, cfg1b, k1b)
				}
				cfg2, k2 := c2.ConfigAt(i)
				if !cfg1.Equal(cfg2) || k1 != k2 {
					t.Fatalf("seed %d: ConfigAt(%d) disagreed between two Cores built from byte-identical durable state: (%+v,%d) vs (%+v,%d)", seed, i, cfg1, k1, cfg2, k2)
				}
				if k1 > 0 {
					if k1 > i {
						t.Fatalf("seed %d: monotone provenance violated: ConfigAt(%d) returned index %d > %d", seed, i, k1, i)
					}
					e, ok := c.EntryAt(k1)
					if !ok || e.Type != EntryConfig {
						t.Fatalf("seed %d: ConfigAt(%d)'s provenance index %d is not an EntryConfig entry: ok=%v type=%v", seed, i, k1, ok, e.Type)
					}
				}
			}

			// Prefix stability: truncating at any index > i must never
			// change ConfigAt(i). Modeled by building a fresh Core over
			// an actual prefix of the log and checking agreement for
			// every i within that prefix.
			if c.LastIndex() > c.SnapshotIndex() {
				m := c.SnapshotIndex() + 1 + Index(rnd.Intn(int(c.LastIndex()-c.SnapshotIndex())))
				var prefixEntries []Entry
				for _, e := range c.Entries() {
					if e.Index <= m {
						prefixEntries = append(prefixEntries, e)
					}
				}
				cTrunc, err := NewCoreFromSnapshot(c.cfg, HardState{CurrentTerm: c.currentTerm},
					c.snapshotIndex, c.snapshotTerm, c.snapshotConfig, c.snapshotHasConfig, prefixEntries)
				if err != nil {
					t.Fatalf("seed %d: NewCoreFromSnapshot (truncated prefix up to %d): %v", seed, m, err)
				}
				for i := c.SnapshotIndex(); i <= m; i++ {
					want, wantIdx := c.ConfigAt(i)
					got, gotIdx := cTrunc.ConfigAt(i)
					if !got.Equal(want) || gotIdx != wantIdx {
						t.Fatalf("seed %d: prefix stability violated: truncating past %d changed ConfigAt(%d) from (%+v,%d) to (%+v,%d)", seed, m, i, want, wantIdx, got, gotIdx)
					}
				}
			}

			// Boundary agreement.
			if c.snapshotHasConfig {
				got, idx := c.ConfigAt(c.SnapshotIndex())
				if idx != 0 || !got.Equal(c.snapshotConfig) {
					t.Fatalf("seed %d: boundary agreement violated: ConfigAt(snapshotIndex) = (%+v,%d), want (%+v,0) == snapshotConfig", seed, got, idx, c.snapshotConfig)
				}
			} else {
				got, idx := c.ConfigAt(c.SnapshotIndex())
				if idx != 0 || !got.Equal(c.bootstrapConfig) {
					t.Fatalf("seed %d: boundary agreement (no snapshot config) violated: ConfigAt(snapshotIndex) = (%+v,%d), want bootstrapConfig (%+v,0)", seed, got, idx, c.bootstrapConfig)
				}
			}
		})
	}
}

// TestConfigAtSkipsVoidedEntriesFallingBackToBootstrap regresses
// DM-16's explicit "never-snapshotted case that revision 1's two
// divergent algorithms disagreed on" (§23/C2): a log containing only
// Voided EntryConfig entries must yield Config.Bootstrap, never the
// zero Configuration, since a Voided entry establishes nothing and
// step 1's scan must skip it exactly as it skips an EntryNormal entry.
func TestConfigAtSkipsVoidedEntriesFallingBackToBootstrap(t *testing.T) {
	boot := votersConfig([]NodeID{"a", "b", "c"})
	c := newBootstrapCore(t, "a", boot)
	c.log = append(c.log,
		Entry{Index: 1, Term: 1, Type: EntryConfig, Data: EncodeVoidedEntryConfigPayload()},
		Entry{Index: 2, Term: 1, Type: EntryConfig, Data: EncodeVoidedEntryConfigPayload()},
		Entry{Index: 3, Term: 1, Type: EntryNormal, Data: []byte("x")},
	)
	for _, i := range []Index{0, 1, 2, 3} {
		got, idx := c.ConfigAt(i)
		if idx != 0 || !got.Equal(boot) {
			t.Fatalf("ConfigAt(%d) over a log of only Voided/EntryNormal entries = (%+v,%d), want (bootstrap,0)", i, got, idx)
		}
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

// TestRemovedMemberReceivesNothingViaReplyDrivenContinuation regresses a
// real defect found by DM-10's randomized combined schedule
// (dynamic-membership plan §15): §4.1 states, and
// TestDM4_RemovalOfIsolatedVoterCommitsWithoutItAndItRemainsHarmless
// (internal/fault) already pins, that "replication to the removed
// member stops at append, not at commit... every replication fan-out
// site... iterates activeConfig.Voters/.Learners, which by that instant
// already exclude the target." That claim is true of the three sites
// §4.1 actually names (appendLeaderEntry, handleHeartbeatTimeout,
// becomeLeader) but was false of three more: the reply-driven
// continuation in handleAppendEntriesResponse's success branch and its
// conflict-repair branch, and handleInstallSnapshotResponse's
// equivalent — none of which checked activeConfig membership before
// calling appendEntriesMessage(msg.From) again, because nextIndex/
// matchIndex bookkeeping for a removed member is never cleaned up.
// DM-4's own schedule never exercised the exact timing window that
// trips this: a message to the target already in flight (or a stale
// reply from one) at the instant its removal commits.
func TestRemovedMemberReceivesNothingViaReplyDrivenContinuation(t *testing.T) {
	assertNothingSentTo := func(t *testing.T, out Output, target NodeID, path string) {
		t.Helper()
		for _, msg := range out.Messages {
			if msg.To == target {
				t.Fatalf("%s: removed member %q was sent %+v — §4.1 requires replication to a removed member to stop at append, never resume via a reply-driven continuation", path, target, msg)
			}
		}
	}

	// setup builds a fresh leader, removes "b", commits the removal via
	// the new majority of C_new={a,c}, and appends one further entry
	// above the removal index — independently for each subtest below,
	// so each demonstrates its own reply-driven continuation site in
	// isolation rather than sharing nextIndex/matchIndex bookkeeping
	// state across subtests.
	setup := func(t *testing.T) (c *Core, removeIdx Index) {
		t.Helper()
		c = leaderReadyForConfigChange(t, []NodeID{"a", "b", "c"})
		out, err := c.ProposeConfigChange(RemoveServerChange, "r1", "b", "")
		if err != nil {
			t.Fatalf("ProposeConfigChange: %v", err)
		}
		applyPersist(t, c, out)
		removeIdx = c.activeConfigIndex

		c.matchIndex["c"] = removeIdx
		c.advanceLeaderCommit(&Output{})
		if c.commitIndex < removeIdx {
			t.Fatal("test setup: removal did not commit")
		}
		if c.activeConfig.IsMember("b") {
			t.Fatal("test setup: b still a member after the removal committed")
		}

		applyPersist(t, c, c.handlePropose([]byte("after-removal")))
		if c.lastIndex() <= removeIdx {
			t.Fatal("test setup: expected at least one entry above the removal index")
		}
		return c, removeIdx
	}

	t.Run("success branch", func(t *testing.T) {
		c, _ := setup(t)
		out := c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgAppendEntriesResponse, From: "b", To: "a", Term: c.currentTerm,
			Success: true, MatchIndex: c.matchIndex["b"],
		}})
		applyPersist(t, c, out)
		assertNothingSentTo(t, out, "b", "handleAppendEntriesResponse success branch")
	})

	t.Run("conflict-repair branch", func(t *testing.T) {
		c, _ := setup(t)
		out := c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgAppendEntriesResponse, From: "b", To: "a", Term: c.currentTerm,
			Success: false, ConflictIndex: 1, ConflictTerm: 0,
		}})
		applyPersist(t, c, out)
		assertNothingSentTo(t, out, "b", "handleAppendEntriesResponse conflict-repair branch")
	})

	t.Run("install-snapshot response", func(t *testing.T) {
		c, _ := setup(t)
		out := c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgInstallSnapshotResponse, From: "b", To: "a", Term: c.currentTerm,
			Success: true, MatchIndex: 1,
		}})
		applyPersist(t, c, out)
		assertNothingSentTo(t, out, "b", "handleInstallSnapshotResponse")
	})
}

// TestClassifyTransitionIgnoresAddressSpelling pins the unit-level half
// of the rule the v0.5.0 final correctness review found violated: §2.6's
// four shapes constrain MEMBERSHIP — which IDs are voters, which are
// learners, and that at most one moves per transition — and must never
// be decided by Member.Address, which is process-local at bootstrap
// (dynamic-membership plan §1.8) and therefore legitimately spelled
// differently on different replicas until the first EntryConfig
// replicates. The end-to-end proof, against real nodes whose -listen
// and -peers name the same endpoint differently, is
// internal/node's TestFirstConfigChangeToleratesBootstrapAddressSpellingDrift.
func TestClassifyTransitionIgnoresAddressSpelling(t *testing.T) {
	// "old" is one replica's own bootstrap view; "same endpoint, other
	// spelling" is what a peer's bootstrap (and so the leader's proposed
	// Configuration) carries for the identical node.
	old := Configuration{
		Voters:   []Member{{ID: "a", Address: "0.0.0.0:9000"}, m("b"), m("c")},
		Learners: []Member{m("d")},
	}
	otherSpelling := Member{ID: "a", Address: "10.0.0.1:9000"}

	cases := []struct {
		name   string
		newCfg Configuration
	}{
		{
			name:   "AddLearner",
			newCfg: Configuration{Voters: []Member{otherSpelling, m("b"), m("c")}, Learners: []Member{m("d"), m("e")}},
		},
		{
			name:   "PromoteToVoter",
			newCfg: Configuration{Voters: []Member{otherSpelling, m("b"), m("c"), m("d")}, Learners: nil},
		},
		{
			name:   "RemoveServer(voter)",
			newCfg: Configuration{Voters: []Member{otherSpelling, m("b")}, Learners: []Member{m("d")}},
		},
		{
			name:   "RemoveServer(learner)",
			newCfg: Configuration{Voters: []Member{otherSpelling, m("b"), m("c")}, Learners: nil},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := classifyTransition(old, tc.newCfg); err != nil {
				t.Fatalf("classifyTransition() = %v, want nil: a differently-spelled address for an otherwise untouched member must never make a legal single-server transition look structurally invalid", err)
			}
		})
	}

	// The shape rules themselves are unaffected: a differently-spelled
	// address does not license a second simultaneous membership change.
	twoAtOnce := Configuration{
		Voters:   []Member{otherSpelling, m("b"), m("c"), m("x")},
		Learners: []Member{m("d"), m("e")},
	}
	if err := classifyTransition(old, twoAtOnce); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("classifyTransition(two changes at once) = %v, want ErrInvalidTransition", err)
	}
}

// TestActivateFromAppendedEntriesAcceptsAddressSpellingDrift is the
// Core-level regression for the same defect: the accept-time
// defense-in-depth re-check runs inside activateFromAppendedEntries,
// which turns a classification failure into a panic — in Core.Step,
// before the entry is ever persisted, so a restart re-derives the same
// bootstrap and panics on the same entry again.
func TestActivateFromAppendedEntriesAcceptsAddressSpellingDrift(t *testing.T) {
	// This replica's bootstrap spells its own address the way its own
	// -listen does; the leader's Configuration spells it the way this
	// node's peers dial it.
	boot := Configuration{Voters: []Member{{ID: "a", Address: "0.0.0.0:9000"}, m("b"), m("c")}}
	c := newBootstrapCore(t, "a", boot)

	leaderView := Configuration{
		Voters:   []Member{{ID: "a", Address: "10.0.0.1:9000"}, m("b"), m("c")},
		Learners: []Member{m("d")},
	}
	entry := Entry{
		Index: 1, Term: 1, Type: EntryConfig,
		Data: encodeConfigChange(membershipKindAddLearner, "req1", "d", "d:0", leaderView),
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("appending the cluster's first EntryConfig panicked because this replica's bootstrap spells its own address differently: %v", r)
		}
	}()
	out := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "b", To: "a", Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{entry},
	}})
	if out.PersistRequest == nil || len(out.PersistRequest.Entries) != 1 {
		t.Fatalf("expected the EntryConfig entry to be accepted and persisted, got %+v", out)
	}
	if !c.activeConfig.Equal(leaderView) {
		t.Fatalf("activeConfig = %+v, want the leader's replicated view %+v (convergence is on what replicated, not on this node's own bootstrap spelling)", c.activeConfig, leaderView)
	}
}
