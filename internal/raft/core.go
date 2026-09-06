package raft

import "fmt"

// pendingKind identifies what a pendingItem resolves into once its
// PersistRequest.Seq is acknowledged complete (see Output's doc
// comment on persistence gating).
type pendingKind uint8

const (
	// pendingVoteGrant releases a granted RequestVoteResponse to
	// candidate, but only if this node's (term, votedFor) still match
	// what they were when the vote was decided.
	pendingVoteGrant pendingKind = iota
	// pendingAppendAck releases a successful AppendEntriesResponse to
	// to, reporting index as MatchIndex, but only if the log entry at
	// index still holds entryTerm (i.e. was not since truncated and
	// replaced by a conflicting leader).
	pendingAppendAck
	// pendingCommitAdvance advances this follower's own commitIndex up
	// to index, but only if the log entry at index still holds
	// entryTerm.
	pendingCommitAdvance
	// pendingSelfMatch records this node's own matchIndex (as Leader)
	// for index once persisted, and re-evaluates the commit rule, but
	// only if this node is still Leader in the same term and the log
	// entry at index still holds entryTerm.
	pendingSelfMatch
)

type pendingItem struct {
	kind pendingKind
	seq  uint64
	term Term // term context at creation

	candidate NodeID // pendingVoteGrant
	to        NodeID // pendingAppendAck

	index     Index // pendingAppendAck / pendingCommitAdvance / pendingSelfMatch
	entryTerm Term  // term recorded at c.log[index] at creation time
}

// Core is ChronicleDB's Raft consensus core (docs/raft.md §1): a
// deterministic, input/output-driven state machine with no direct
// network I/O, disk I/O, or wall-clock reads. All correctness-relevant
// decisions live in Step and its helpers; a Core is otherwise inert —
// nothing happens except in response to an explicit Input.
//
// Core is not safe for concurrent use; callers own serializing Step
// calls (docs/raft.md §1's pure input/output model is inherently
// single-threaded/event-loop in spirit — see docs/testing-strategy.md
// §3's single-threaded deterministic scheduler).
type Core struct {
	cfg Config

	role        Role
	currentTerm Term
	votedFor    NodeID
	leaderID    NodeID

	// log is 1-indexed; log[0] is a fixed sentinel {Index:0, Term:0}
	// entry representing "before the first real entry," so
	// prevLogIndex==0 checks are always trivially satisfied
	// (docs/raft.md §3).
	log []Entry

	commitIndex Index
	// appliedIndex is bookkeeping-only (docs/raft.md §3): Step never
	// reads it for any decision. It exists so a future driver
	// (internal/node, Phase 5) has a place to record "internal/fsm has
	// applied through here" without collapsing that concept into
	// commitIndex (docs/invariants.md APPLIED-PREFIX SAFETY).
	appliedIndex Index

	// Leader-only volatile replication state (docs/raft.md §3).
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// Candidate-only volatile state. Keyed by peer so a duplicate
	// RequestVoteResponse from the same peer is never counted twice
	// (docs/invariants.md RAFT-ELECTION-SAFETY proof obligation).
	votesReceived map[NodeID]bool

	persistSeq   uint64 // next PersistRequest.Seq to assign
	persistedSeq uint64 // highest Seq acknowledged via InputPersistenceComplete
	pending      []pendingItem
}

// NewCore constructs a Core from cfg and previously persisted state
// (hs, entries — both the zero value / nil for a brand-new node).
// entries must be exactly what Storage.Entries(1, Storage.LastIndex()+1)
// returns: a gap-free run starting at index 1, each entry's Index
// field matching its position. A restarted node reconstructs its Core
// this way every time (docs/raft.md §5.1): commitIndex/appliedIndex
// always start at 0 regardless of hs/entries and must be
// re-established via legitimate leader contact or this node's own
// election (docs/recovery.md §2) — NewCore never trusts a cached
// commitIndex from disk, because none is ever persisted (ADR-0008).
func NewCore(cfg Config, hs HardState, entries []Entry) (*Core, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	log := make([]Entry, 0, len(entries)+1)
	log = append(log, Entry{}) // sentinel: Index 0, Term 0
	for i, e := range entries {
		wantIdx := Index(i + 1)
		if e.Index != wantIdx {
			return nil, fmt.Errorf("%w: entries must be gap-free starting at 1 (entry %d has Index %d, want %d)", ErrInvalidConfig, i, e.Index, wantIdx)
		}
		log = append(log, e)
	}
	return &Core{
		cfg:           cfg,
		role:          Follower,
		currentTerm:   hs.CurrentTerm,
		votedFor:      hs.VotedFor,
		log:           log,
		nextIndex:     make(map[NodeID]Index),
		matchIndex:    make(map[NodeID]Index),
		votesReceived: make(map[NodeID]bool),
	}, nil
}

// --- Read-only accessors (observability, tests, harness bookkeeping) ---

func (c *Core) ID() NodeID          { return c.cfg.ID }
func (c *Core) Role() Role          { return c.role }
func (c *Core) CurrentTerm() Term   { return c.currentTerm }
func (c *Core) VotedFor() NodeID    { return c.votedFor }
func (c *Core) LeaderID() NodeID    { return c.leaderID }
func (c *Core) CommitIndex() Index  { return c.commitIndex }
func (c *Core) AppliedIndex() Index { return c.appliedIndex }
func (c *Core) LastIndex() Index    { return c.lastIndex() }
func (c *Core) LastLogTerm() Term   { return c.lastTerm() }

// SetApplied records that internal/fsm has applied through idx
// (bookkeeping only; never consulted by Step). A no-op if idx does not
// advance the watermark.
func (c *Core) SetApplied(idx Index) {
	if idx > c.appliedIndex {
		c.appliedIndex = idx
	}
}

// EntryAt returns the log entry at i (i must be >= 1), or ok=false if
// i is out of range.
func (c *Core) EntryAt(i Index) (e Entry, ok bool) {
	if i == 0 || int(i) >= len(c.log) {
		return Entry{}, false
	}
	return c.log[i], true
}

// Entries returns a copy of the entries at indices [1, LastIndex()].
func (c *Core) Entries() []Entry {
	out := make([]Entry, len(c.log)-1)
	copy(out, c.log[1:])
	return out
}

// MatchIndexOf and NextIndexOf return this node's (Leader-only) view
// of peer's replication progress; zero values if not currently Leader
// or if peer is unknown.
func (c *Core) MatchIndexOf(peer NodeID) Index { return c.matchIndex[peer] }
func (c *Core) NextIndexOf(peer NodeID) Index  { return c.nextIndex[peer] }

func (c *Core) lastIndex() Index { return Index(len(c.log) - 1) }
func (c *Core) lastTerm() Term   { return c.log[len(c.log)-1].Term }

func (c *Core) termAt(i Index) Term {
	if int(i) >= len(c.log) {
		return 0
	}
	return c.log[i].Term
}

func (c *Core) isLogUpToDate(otherLastIndex Index, otherLastTerm Term) bool {
	myTerm := c.lastTerm()
	if otherLastTerm != myTerm {
		return otherLastTerm > myTerm
	}
	return otherLastIndex >= c.lastIndex()
}

// NewElectionTimeout samples a fresh randomized election-timeout tick
// count using this Core's configured Rand, for a driver to arm a
// node's very first election timer at startup — before any Step call
// has produced an Output.ResetElectionTimer to arm it from
// (docs/testing-strategy.md §3.1).
func (c *Core) NewElectionTimeout() int { return c.cfg.electionTimeout() }

func (c *Core) nextPersistSeq() uint64 {
	c.persistSeq++
	return c.persistSeq
}

// stepDownTo unconditionally reverts to Follower at term, clearing
// votedFor and known leader (a fresh term is a clean slate —
// docs/raft.md §2). Returns true iff this node was Leader or Candidate
// immediately before the call, for Output.SteppedDown.
func (c *Core) stepDownTo(term Term) bool {
	wasLeaderOrCandidate := c.role == Leader || c.role == Candidate
	c.role = Follower
	c.currentTerm = term
	c.votedFor = noVote
	c.leaderID = ""
	c.votesReceived = nil
	return wasLeaderOrCandidate
}

// invalidatePendingFrom drops any pending index-tied item referring to
// index >= from: those log positions no longer hold what the pending
// item was created to confirm, because they were just truncated
// (docs/raft.md §3 divergent suffix repair).
func (c *Core) invalidatePendingFrom(from Index) {
	kept := c.pending[:0]
	for _, p := range c.pending {
		switch p.kind {
		case pendingAppendAck, pendingCommitAdvance, pendingSelfMatch:
			if p.index >= from {
				continue
			}
		}
		kept = append(kept, p)
	}
	c.pending = kept
}

// Step is Core's sole entry point (docs/raft.md §1). Every
// correctness-relevant state transition happens here or in a helper
// Step calls directly.
func (c *Core) Step(in Input) Output {
	switch in.Kind {
	case InputElectionTimeout:
		return c.handleElectionTimeout()
	case InputHeartbeatTimeout:
		return c.handleHeartbeatTimeout()
	case InputMessage:
		return c.handleMessage(in.Message)
	case InputPropose:
		return c.handlePropose(in.ProposeData)
	case InputPersistenceComplete:
		return c.handlePersistenceComplete(in.PersistSeq)
	default:
		return Output{}
	}
}

func (c *Core) handleElectionTimeout() Output {
	if c.role == Leader {
		return Output{}
	}

	c.role = Candidate
	c.currentTerm++
	c.votedFor = c.cfg.ID
	c.leaderID = ""
	c.votesReceived = map[NodeID]bool{c.cfg.ID: true}

	seq := c.nextPersistSeq()
	hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}

	out := Output{
		PersistRequest:       &PersistRequest{Seq: seq, HardState: &hs},
		ResetElectionTimer:   true,
		ElectionTimeoutTicks: c.cfg.electionTimeout(),
	}
	for _, p := range c.cfg.Peers {
		if p == c.cfg.ID {
			continue
		}
		out.Messages = append(out.Messages, Message{
			Type:         MsgRequestVoteRequest,
			From:         c.cfg.ID,
			To:           p,
			Term:         c.currentTerm,
			LastLogIndex: c.lastIndex(),
			LastLogTerm:  c.lastTerm(),
		})
	}
	if len(c.votesReceived) >= c.cfg.majority() {
		// Single-node cluster: a candidacy of one is already a majority.
		c.becomeLeader(&out)
	}
	return out
}

func (c *Core) handleHeartbeatTimeout() Output {
	if c.role != Leader {
		return Output{}
	}
	out := Output{ResetHeartbeatTimer: true, HeartbeatTimeoutTicks: c.cfg.HeartbeatTimeoutTicks}
	for _, p := range c.cfg.Peers {
		if p == c.cfg.ID {
			continue
		}
		out.Messages = append(out.Messages, c.appendEntriesMessage(p))
	}
	return out
}

func (c *Core) handlePropose(data []byte) Output {
	if c.role != Leader {
		return Output{ProposalRejected: true, LeaderHint: c.leaderID}
	}
	idx := c.lastIndex() + 1
	entry := Entry{Index: idx, Term: c.currentTerm, Data: append([]byte(nil), data...)}
	c.log = append(c.log, entry)
	c.matchIndex[c.cfg.ID] = idx // optimistic; confirmed via pendingSelfMatch below

	seq := c.nextPersistSeq()
	c.pending = append(c.pending, pendingItem{kind: pendingSelfMatch, seq: seq, term: c.currentTerm, index: idx, entryTerm: c.currentTerm})

	out := Output{PersistRequest: &PersistRequest{Seq: seq, Entries: []Entry{entry}}}
	for _, p := range c.cfg.Peers {
		if p == c.cfg.ID {
			continue
		}
		out.Messages = append(out.Messages, c.appendEntriesMessage(p))
	}
	return out
}

// appendEntriesMessage builds the next AppendEntriesRequest to send
// peer, per this Leader's current view of nextIndex[peer], and
// optimistically advances nextIndex[peer] past whatever it includes
// (corrected backward later on a rejection — docs/raft.md §3).
func (c *Core) appendEntriesMessage(peer NodeID) Message {
	next := c.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1
	prevTerm := c.termAt(prevIndex)

	var entries []Entry
	if next <= c.lastIndex() {
		entries = append([]Entry(nil), c.log[next:]...)
		c.nextIndex[peer] = c.lastIndex() + 1
	} else {
		c.nextIndex[peer] = next
	}
	return Message{
		Type:         MsgAppendEntriesRequest,
		From:         c.cfg.ID,
		To:           peer,
		Term:         c.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: c.commitIndex,
	}
}

// becomeLeader transitions this Candidate to Leader, initializes
// leader replication state, and appends immediate authority-asserting
// AppendEntries messages to out (docs/raft.md §"Leader initialization").
// No no-op entry is appended on election: the accepted architecture
// (docs/raft.md) does not specify one, and this phase does not invent
// one (see docs/raft.md's implementation note).
func (c *Core) becomeLeader(out *Output) {
	c.role = Leader
	c.leaderID = c.cfg.ID
	c.votesReceived = nil
	c.nextIndex = make(map[NodeID]Index, len(c.cfg.Peers))
	c.matchIndex = make(map[NodeID]Index, len(c.cfg.Peers))
	last := c.lastIndex()
	for _, p := range c.cfg.Peers {
		c.nextIndex[p] = last + 1
		c.matchIndex[p] = 0
	}
	c.matchIndex[c.cfg.ID] = last

	out.BecameLeader = true
	out.ResetHeartbeatTimer = true
	out.HeartbeatTimeoutTicks = c.cfg.HeartbeatTimeoutTicks
	for _, p := range c.cfg.Peers {
		if p == c.cfg.ID {
			continue
		}
		out.Messages = append(out.Messages, c.appendEntriesMessage(p))
	}
}

func (c *Core) handleMessage(msg Message) Output {
	switch msg.Type {
	case MsgRequestVoteRequest:
		return c.handleRequestVoteRequest(msg)
	case MsgRequestVoteResponse:
		return c.handleRequestVoteResponse(msg)
	case MsgAppendEntriesRequest:
		return c.handleAppendEntriesRequest(msg)
	case MsgAppendEntriesResponse:
		return c.handleAppendEntriesResponse(msg)
	default:
		return Output{}
	}
}

// handleRequestVoteRequest implements docs/raft.md §2's RequestVote
// safety rule exactly: reject a lower term outright; step down (and
// persist) on a higher term; grant at most one vote per term, only to
// a candidate whose log is at least as up to date as this node's own,
// and never send the grant until votedFor is durably persisted
// (docs/adr/0008, docs/invariants.md RAFT-ELECTION-SAFETY).
func (c *Core) handleRequestVoteRequest(msg Message) Output {
	var out Output

	if msg.Term < c.currentTerm {
		out.Messages = append(out.Messages, Message{
			Type: MsgRequestVoteResponse, From: c.cfg.ID, To: msg.From,
			Term: c.currentTerm, VoteGranted: false,
		})
		return out
	}

	steppedDown := false
	if msg.Term > c.currentTerm {
		if c.stepDownTo(msg.Term) {
			out.SteppedDown = true
		}
		steppedDown = true
	}

	canVote := c.votedFor == noVote || c.votedFor == msg.From
	logOK := c.isLogUpToDate(msg.LastLogIndex, msg.LastLogTerm)

	if canVote && logOK {
		c.votedFor = msg.From
		out.ResetElectionTimer = true
		out.ElectionTimeoutTicks = c.cfg.electionTimeout()

		seq := c.nextPersistSeq()
		hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}
		out.PersistRequest = &PersistRequest{Seq: seq, HardState: &hs}
		c.pending = append(c.pending, pendingItem{kind: pendingVoteGrant, seq: seq, term: c.currentTerm, candidate: msg.From})
		return out
	}

	out.Messages = append(out.Messages, Message{
		Type: MsgRequestVoteResponse, From: c.cfg.ID, To: msg.From,
		Term: c.currentTerm, VoteGranted: false,
	})
	if steppedDown {
		seq := c.nextPersistSeq()
		hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}
		out.PersistRequest = &PersistRequest{Seq: seq, HardState: &hs}
	}
	return out
}

func (c *Core) handleRequestVoteResponse(msg Message) Output {
	var out Output
	if msg.Term > c.currentTerm {
		if c.stepDownTo(msg.Term) {
			out.SteppedDown = true
		}
		seq := c.nextPersistSeq()
		hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}
		out.PersistRequest = &PersistRequest{Seq: seq, HardState: &hs}
		return out
	}
	if c.role != Candidate || msg.Term != c.currentTerm || !msg.VoteGranted {
		return out // stale, not a candidate anymore, or a rejection
	}
	c.votesReceived[msg.From] = true // map dedupes: a peer's vote is never counted twice
	if len(c.votesReceived) >= c.cfg.majority() {
		c.becomeLeader(&out)
	}
	return out
}

// handleAppendEntriesRequest implements docs/raft.md §3: term
// validation, leader recognition, election-timer reset, the
// prevLogIndex/prevLogTerm consistency check, duplicate-entry
// no-oping, divergent-suffix truncation, and follower commitIndex
// advancement — with the outgoing success acknowledgement withheld
// until the entries it confirms are durably persisted here
// (docs/adr/0008).
func (c *Core) handleAppendEntriesRequest(msg Message) Output {
	var out Output

	if msg.Term < c.currentTerm {
		out.Messages = append(out.Messages, Message{
			Type: MsgAppendEntriesResponse, From: c.cfg.ID, To: msg.From,
			Term: c.currentTerm, Success: false,
		})
		return out
	}

	stateChanged := false
	if msg.Term > c.currentTerm {
		if c.stepDownTo(msg.Term) {
			out.SteppedDown = true
		}
		stateChanged = true
	} else if c.role != Follower {
		// Same term: a legitimate leader has been elected and this
		// node (Candidate, or — should RAFT-ELECTION-SAFETY somehow be
		// violated upstream — Leader) must recognize it and revert.
		if c.stepDownTo(msg.Term) {
			out.SteppedDown = true
		}
	}

	c.leaderID = msg.From
	out.ResetElectionTimer = true
	out.ElectionTimeoutTicks = c.cfg.electionTimeout()

	if msg.PrevLogIndex > c.lastIndex() || c.termAt(msg.PrevLogIndex) != msg.PrevLogTerm {
		ci, ct := c.conflictHint(msg.PrevLogIndex)
		out.Messages = append(out.Messages, Message{
			Type: MsgAppendEntriesResponse, From: c.cfg.ID, To: msg.From,
			Term: c.currentTerm, Success: false, ConflictIndex: ci, ConflictTerm: ct,
		})
		if stateChanged {
			seq := c.nextPersistSeq()
			hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}
			out.PersistRequest = &PersistRequest{Seq: seq, HardState: &hs}
		}
		return out
	}

	truncateFrom := Index(0)
	appendStart := len(msg.Entries)
	for i, e := range msg.Entries {
		idx := msg.PrevLogIndex + 1 + Index(i)
		if idx <= c.lastIndex() && c.termAt(idx) == e.Term {
			continue // already present and identical (RF-7 duplicate)
		}
		if idx <= c.lastIndex() {
			if idx <= c.commitIndex {
				panic(fmt.Sprintf("raft: refusing to truncate committed entry at index %d (commitIndex=%d): this indicates a Raft safety violation upstream, not a recoverable local condition", idx, c.commitIndex))
			}
			truncateFrom = idx
			c.invalidatePendingFrom(idx)
			c.log = c.log[:idx]
		}
		appendStart = i
		break
	}
	var newEntries []Entry
	if appendStart < len(msg.Entries) {
		newEntries = append([]Entry(nil), msg.Entries[appendStart:]...)
		c.log = append(c.log, newEntries...)
	}

	lastNew := msg.PrevLogIndex + Index(len(msg.Entries))
	needPersist := stateChanged || truncateFrom != 0 || len(newEntries) != 0

	var persistSeq uint64
	if needPersist {
		persistSeq = c.nextPersistSeq()
		pr := &PersistRequest{Seq: persistSeq, TruncateFrom: truncateFrom, Entries: newEntries}
		if stateChanged {
			hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}
			pr.HardState = &hs
		}
		out.PersistRequest = pr
	}

	if msg.LeaderCommit > c.commitIndex {
		newCommit := msg.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > c.commitIndex {
			if needPersist {
				c.pending = append(c.pending, pendingItem{
					kind: pendingCommitAdvance, seq: persistSeq, term: c.currentTerm,
					index: newCommit, entryTerm: c.termAt(newCommit),
				})
			} else {
				c.advanceFollowerCommit(newCommit, &out)
			}
		}
	}

	if needPersist {
		c.pending = append(c.pending, pendingItem{
			kind: pendingAppendAck, seq: persistSeq, term: c.currentTerm,
			to: msg.From, index: lastNew, entryTerm: c.termAt(lastNew),
		})
	} else {
		out.Messages = append(out.Messages, Message{
			Type: MsgAppendEntriesResponse, From: c.cfg.ID, To: msg.From,
			Term: c.currentTerm, Success: true, MatchIndex: lastNew,
		})
	}
	return out
}

// conflictHint computes the (ConflictIndex, ConflictTerm) pair a
// rejecting AppendEntriesResponse reports for a mismatched prevIndex
// (docs/raft.md §3's log-matching conflict-repair optimization).
func (c *Core) conflictHint(prevIndex Index) (conflictIndex Index, conflictTerm Term) {
	if prevIndex > c.lastIndex() {
		return c.lastIndex() + 1, 0
	}
	t := c.termAt(prevIndex)
	i := prevIndex
	for i > 1 && c.termAt(i-1) == t {
		i--
	}
	return i, t
}

// advanceFollowerCommit advances this (non-Leader) node's commitIndex
// to newCommit — clamped to what is actually present in the local
// log — surfacing any newly committed entries.
func (c *Core) advanceFollowerCommit(newCommit Index, out *Output) {
	if newCommit > c.lastIndex() {
		newCommit = c.lastIndex()
	}
	if newCommit <= c.commitIndex {
		return
	}
	start := c.commitIndex + 1
	out.CommittedEntries = append(out.CommittedEntries, c.log[start:newCommit+1]...)
	c.commitIndex = newCommit
}

func (c *Core) handleAppendEntriesResponse(msg Message) Output {
	var out Output
	if msg.Term > c.currentTerm {
		if c.stepDownTo(msg.Term) {
			out.SteppedDown = true
		}
		seq := c.nextPersistSeq()
		hs := HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor}
		out.PersistRequest = &PersistRequest{Seq: seq, HardState: &hs}
		return out
	}
	if c.role != Leader || msg.Term != c.currentTerm {
		return out // stale reply
	}

	if msg.Success {
		if msg.MatchIndex > c.matchIndex[msg.From] {
			c.matchIndex[msg.From] = msg.MatchIndex
		}
		if msg.MatchIndex+1 > c.nextIndex[msg.From] {
			c.nextIndex[msg.From] = msg.MatchIndex + 1
		}
		c.advanceLeaderCommit(&out)
		if c.nextIndex[msg.From] <= c.lastIndex() {
			out.Messages = append(out.Messages, c.appendEntriesMessage(msg.From))
		}
		return out
	}

	next := msg.ConflictIndex
	if msg.ConflictTerm != 0 {
		found := Index(0)
		for i := c.lastIndex(); i > 0; i-- {
			if c.termAt(i) == msg.ConflictTerm {
				found = i + 1
				break
			}
		}
		if found != 0 {
			next = found
		}
	}
	if next < 1 {
		next = 1
	}
	if next < c.nextIndex[msg.From] {
		c.nextIndex[msg.From] = next
	}
	out.Messages = append(out.Messages, c.appendEntriesMessage(msg.From))
	return out
}

// advanceLeaderCommit implements the current-term commit rule exactly
// (docs/raft.md §4): the Leader may advance commitIndex to N only if a
// majority (including itself) has matchIndex >= N and the entry at N
// belongs to the Leader's own currentTerm.
func (c *Core) advanceLeaderCommit(out *Output) {
	for n := c.lastIndex(); n > c.commitIndex; n-- {
		if c.termAt(n) != c.currentTerm {
			continue
		}
		count := 0
		for _, p := range c.cfg.Peers {
			if c.matchIndex[p] >= n {
				count++
			}
		}
		if count >= c.cfg.majority() {
			start := c.commitIndex + 1
			out.CommittedEntries = append(out.CommittedEntries, c.log[start:n+1]...)
			c.commitIndex = n
			return
		}
	}
}

func (c *Core) handlePersistenceComplete(seq uint64) Output {
	if seq > c.persistedSeq {
		c.persistedSeq = seq
	}
	var out Output
	var remaining []pendingItem
	for _, p := range c.pending {
		if p.seq > c.persistedSeq {
			remaining = append(remaining, p)
			continue
		}
		switch p.kind {
		case pendingVoteGrant:
			if c.currentTerm == p.term && c.votedFor == p.candidate {
				out.Messages = append(out.Messages, Message{
					Type: MsgRequestVoteResponse, From: c.cfg.ID, To: p.candidate,
					Term: c.currentTerm, VoteGranted: true,
				})
			}
		case pendingAppendAck:
			if c.entryStillPresent(p.index, p.entryTerm) {
				out.Messages = append(out.Messages, Message{
					Type: MsgAppendEntriesResponse, From: c.cfg.ID, To: p.to,
					Term: c.currentTerm, Success: true, MatchIndex: p.index,
				})
			}
		case pendingCommitAdvance:
			if c.entryStillPresent(p.index, p.entryTerm) {
				c.advanceFollowerCommit(p.index, &out)
			}
		case pendingSelfMatch:
			if c.role == Leader && c.currentTerm == p.term && c.entryStillPresent(p.index, p.entryTerm) {
				if p.index > c.matchIndex[c.cfg.ID] {
					c.matchIndex[c.cfg.ID] = p.index
				}
				c.advanceLeaderCommit(&out)
			}
		}
	}
	c.pending = remaining
	return out
}

func (c *Core) entryStillPresent(index Index, term Term) bool {
	return int(index) < len(c.log) && c.log[index].Term == term
}
