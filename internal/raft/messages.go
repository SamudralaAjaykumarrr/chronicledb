package raft

// MessageType identifies which RPC (or RPC reply) a Message carries
// (docs/raft.md §2-3). The set is closed: internal/raft's wire model
// for Phase 4 is limited to the two RPC pairs Raft itself defines.
type MessageType uint8

const (
	MsgRequestVoteRequest MessageType = iota
	MsgRequestVoteResponse
	MsgAppendEntriesRequest
	MsgAppendEntriesResponse
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVoteRequest:
		return "RequestVoteRequest"
	case MsgRequestVoteResponse:
		return "RequestVoteResponse"
	case MsgAppendEntriesRequest:
		return "AppendEntriesRequest"
	case MsgAppendEntriesResponse:
		return "AppendEntriesResponse"
	default:
		return "Unknown"
	}
}

// Message is a single Raft protocol message, in either direction. It
// is a plain data value: internal/raft never sends or receives one
// itself (docs/raft.md §1) — a driver (internal/fault's deterministic
// transport today; internal/transport in Phase 5) is responsible for
// carrying Message values between nodes and feeding received ones back
// into the destination Core via Input{Kind: InputMessage}.
//
// Not every field is meaningful for every Type; see the per-type
// comments below. Unused fields for a given Type are left at their
// zero value.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term // sender's term, for every message type

	// RequestVote request fields.
	LastLogIndex Index
	LastLogTerm  Term

	// RequestVote response fields.
	VoteGranted bool

	// AppendEntries request fields.
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index

	// AppendEntries response fields.
	Success bool
	// MatchIndex is the highest index the follower now has, valid only
	// when Success is true (docs/raft.md §3 matchIndex).
	MatchIndex Index
	// ConflictIndex/ConflictTerm are the log-matching conflict-repair
	// hints (docs/raft.md §3 "divergent suffix repair"), valid only
	// when Success is false: ConflictTerm is the term of the
	// conflicting entry at PrevLogIndex (0 if the follower's log was
	// simply too short to contain PrevLogIndex at all), and
	// ConflictIndex is the first index of that term in the follower's
	// log (or, when ConflictTerm is 0, one past the follower's last log
	// index).
	ConflictIndex Index
	ConflictTerm  Term
}
