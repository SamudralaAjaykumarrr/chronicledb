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

// EntryType discriminates an opaque FSM-command entry from a
// Core-native membership-configuration entry (dynamic-membership plan
// §2.2). EntryNormal is the zero value so every existing Entry{...}
// literal in every existing test/caller continues to mean exactly what
// it means today.
type EntryType uint8

const (
	// EntryNormal carries an opaque internal/fsm command payload. Core
	// never inspects Data for this type.
	EntryNormal EntryType = 0
	// EntryConfig carries a raft-native encoded Configuration (see
	// EncodeConfigChange/DecodeConfigChange) — decoded and acted upon
	// entirely inside Core, never via internal/fsm (dynamic-membership
	// plan §2.4).
	EntryConfig EntryType = 1
)

// Entry is one entry of the Raft log: an (index, term) position
// carrying an opaque command payload, or — when Type is EntryConfig —
// a Core-native membership-configuration payload. internal/raft has no
// opinion about an EntryNormal Data's contents (docs/raft.md §6) — it
// is internal/fsm's command encoding once Phase 5 wires the two
// together.
type Entry struct {
	Index Index
	Term  Term
	Type  EntryType
	Data  []byte
}

// Member pairs a NodeID with the dial address other nodes need to
// replicate to it (dynamic-membership plan §1.1). Core never dials
// Address itself (docs/raft.md §1: no network I/O in Core) — it
// carries it opaquely, exactly as it already carries
// Message.SnapshotData opaquely; only internal/node's transport layer
// ever reads it.
type Member struct {
	ID      NodeID
	Address string
}

// Configuration is the complete, self-contained description of who
// participates in this Raft group and how, at one point in the log
// (dynamic-membership plan §1.1). It is Core's own type — never passed
// through internal/fsm.
type Configuration struct {
	Voters   []Member // majority-counted; may vote, may be voted for, may become Leader
	Learners []Member // replicated to; never counted; never votes; never elected
}

// majority returns the smallest count that constitutes a majority of
// c.Voters.
func (c Configuration) majority() int { return len(c.Voters)/2 + 1 }

func (c Configuration) isVoter(id NodeID) bool {
	for _, m := range c.Voters {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (c Configuration) isLearner(id NodeID) bool {
	for _, m := range c.Learners {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (c Configuration) isMember(id NodeID) bool { return c.isVoter(id) || c.isLearner(id) }

// isZero reports whether c is the zero Configuration — no voters, no
// learners (dynamic-membership plan §3.1's "never joined" state).
func (c Configuration) isZero() bool { return len(c.Voters) == 0 && len(c.Learners) == 0 }

func cloneMembers(m []Member) []Member {
	if len(m) == 0 {
		return nil
	}
	return append([]Member(nil), m...)
}

// clone returns a deep copy of c, sharing no backing array with c
// (dynamic-membership plan §6.3a: ActiveConfig()/ConfigAt() must never
// alias Core's own mutable slices).
func (c Configuration) clone() Configuration {
	return Configuration{Voters: cloneMembers(c.Voters), Learners: cloneMembers(c.Learners)}
}

func membersEqual(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Equal reports whether c and o describe the same configuration,
// including member order (both are always produced deterministically
// from the same encode/decode or the same in-memory construction, so
// order-sensitive comparison is exact rather than approximate).
func (c Configuration) Equal(o Configuration) bool {
	return membersEqual(c.Voters, o.Voters) && membersEqual(c.Learners, o.Learners)
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
