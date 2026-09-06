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
	// MsgInstallSnapshotRequest asks a follower to install the snapshot
	// covering (LastIncludedIndex, LastIncludedTerm) — used when a
	// leader's own retained log no longer reaches back far enough to
	// bring a peer up to date with ordinary AppendEntries
	// (docs/snapshots.md §7). Core itself never carries the actual
	// snapshot bytes: it emits this message with SnapshotData left
	// empty, and the driver (internal/node) is responsible for filling
	// SnapshotData in from its local snapshot store before the message
	// is actually put on the wire, and for validating/durably installing
	// SnapshotData on the receiving side BEFORE ever feeding this
	// message back into the receiving Core (see Core.Step's doc comment
	// on this message type for the exact contract).
	MsgInstallSnapshotRequest
	// MsgInstallSnapshotResponse acknowledges a MsgInstallSnapshotRequest
	// once the receiver has durably installed it (Success=true,
	// MatchIndex=the installed LastIncludedIndex) or rejects it
	// (Success=false, e.g. failed local validation) — docs/snapshots.md
	// §5's "rejects it and requests re-transmission."
	MsgInstallSnapshotResponse
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
	case MsgInstallSnapshotRequest:
		return "InstallSnapshotRequest"
	case MsgInstallSnapshotResponse:
		return "InstallSnapshotResponse"
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

	// InstallSnapshot request fields (MsgInstallSnapshotRequest). Core
	// itself only ever reads/writes LastIncludedIndex/LastIncludedTerm;
	// SnapshotData is opaque cargo Core never inspects (see
	// MsgInstallSnapshotRequest's doc comment) — carried on Message
	// purely so it rides the same wire frame internal/transport already
	// carries every other Message type over, without a second RPC path.
	LastIncludedIndex Index
	LastIncludedTerm  Term
	SnapshotData      []byte

	// Seq is a driver-assigned (internal/node), Core-opaque correlation
	// token: Core itself never reads or writes it (always 0 on any
	// Message Core constructs) and it plays no role in any Raft safety
	// invariant. internal/node uses it to distinguish a Success
	// AppendEntriesResponse that genuinely answers a specific, freshly
	// sent AppendEntriesRequest from an older one merely processed
	// later — see Node.ackSeq's doc comment for why that distinction is
	// load-bearing for ADR-0010's ReadIndex freshness proof.
	Seq uint64
}
