package raft

// NodeID identifies one member of a Raft cluster. ChronicleDB never
// interprets its bytes beyond equality comparison, so a plain string
// is sufficient (mirrors internal/fsm.RequestID's rationale).
type NodeID string

// Term is a monotonically increasing logical epoch number
// (docs/raft.md §2). At most one leader can be legitimately elected
// per term (RAFT-ELECTION-SAFETY, docs/invariants.md).
type Term uint64

// Index is a 1-based position in the Raft log. Index 0 is reserved as
// the sentinel "before the first entry" position, used to make
// prevLogIndex/prevLogTerm checks at the very start of the log
// trivially satisfied (docs/raft.md §3).
type Index uint64

// Role is one of the three Raft roles (docs/raft.md §2). Every node
// starts as Follower.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Entry is one entry of the Raft log: an (index, term) position
// carrying an opaque command payload. internal/raft has no opinion
// about Data's contents (docs/raft.md §6) — it is internal/fsm's
// command encoding once Phase 5 wires the two together.
type Entry struct {
	Index Index
	Term  Term
	Data  []byte
}

// HardState is the subset of Raft state that must survive a restart
// (docs/raft.md §5): currentTerm and votedFor, always persisted
// together so the pair is durably consistent (docs/adr/0008). A zero
// value HardState{} represents a brand-new node that has never voted
// and has never observed a term greater than 0.
type HardState struct {
	CurrentTerm Term
	VotedFor    NodeID // "" means no vote cast in CurrentTerm
}

// noVote is the sentinel VotedFor value meaning "no vote cast this
// term." NodeID's zero value already serves this purpose; the name
// exists only to make call sites self-documenting.
const noVote NodeID = ""
