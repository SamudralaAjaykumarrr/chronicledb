package raft

// InputKind identifies which of Core.Step's explicit input variants a
// given Input carries (docs/raft.md §1's Step(state, input) pseudocode).
type InputKind uint8

const (
	// InputElectionTimeout means the driver's election timer, most
	// recently armed per an earlier Output.ResetElectionTimer request,
	// has fired. Ignored by a Leader (defensive: a correct driver never
	// arms an election timer for a Leader in the first place).
	InputElectionTimeout InputKind = iota
	// InputHeartbeatTimeout means the driver's heartbeat timer has
	// fired. Meaningful only for a Leader; ignored otherwise.
	InputHeartbeatTimeout
	// InputMessage delivers one received Message (a request or a
	// reply) from another node.
	InputMessage
	// InputPropose asks a Leader to append ProposeData as a new log
	// entry. If this Core is not currently Leader, Step reports
	// rejection via Output.ProposalRejected / Output.LeaderHint rather
	// than performing any state change — this is the "small
	// compatibility hook" docs/raft.md §6 anticipates for a future
	// client-facing proposal path (Phase 5), not a client protocol
	// itself.
	InputPropose
	// InputPersistenceComplete reports that the PersistRequest with the
	// given Seq has been durably applied to Storage (docs/raft.md §1's
	// PersistenceComplete(...) input). A driver must deliver these in
	// non-decreasing Seq order — Storage writes themselves must also be
	// applied in that order (see ApplyPersistRequest) — since Step
	// tracks only the highest Seq acknowledged so far.
	InputPersistenceComplete
)

// Input is the single explicit input to one Core.Step call.
type Input struct {
	Kind InputKind

	Message     Message // InputMessage
	ProposeData []byte  // InputPropose
	PersistSeq  uint64  // InputPersistenceComplete
}

// PersistRequest describes durable-state work Step requires before the
// outputs gated on it (see Output's doc comment) may be released. A
// driver must, in Seq order:
//  1. If TruncateFrom != 0, call Storage.Truncate(TruncateFrom).
//  2. If len(Entries) != 0, call Storage.Append(Entries).
//  3. If HardState != nil, call Storage.SetHardState(*HardState).
//  4. Once all of the above return successfully, call
//     Core.Step(Input{Kind: InputPersistenceComplete, PersistSeq: Seq}).
//
// See ApplyPersistRequest, which performs exactly this sequence
// against a Storage implementation.
type PersistRequest struct {
	Seq          uint64
	HardState    *HardState
	TruncateFrom Index // 0 means "no truncation required"
	Entries      []Entry
}

// Output is everything one Core.Step call produces: explicit outbound
// messages, an optional durability requirement, timer-reset requests,
// and newly committed entries (docs/raft.md §1). internal/raft never
// hides time or network behavior inside Step itself — every side
// effect a driver must perform is named here.
//
// Messages that would let this node affect another node's persistent
// state before this node's own relevant state is itself durable (a
// granted vote; an AppendEntries success acknowledgement) are held
// internally and are not released into Messages until the
// corresponding PersistRequest.Seq is reported complete via
// InputPersistenceComplete (docs/adr/0008: "before they can affect
// other nodes' state"). They then appear in the Output of the Step
// call that delivers that InputPersistenceComplete, not in the
// Output of the Step call that first decided to send them.
type Output struct {
	Messages       []Message
	PersistRequest *PersistRequest // nil if this Step call requires no persistence

	ResetElectionTimer    bool
	ElectionTimeoutTicks  int // valid only if ResetElectionTimer
	ResetHeartbeatTimer   bool
	HeartbeatTimeoutTicks int // valid only if ResetHeartbeatTimer

	// CommittedEntries are newly committed entries (docs/raft.md §4),
	// in order, not yet reported as committed by any prior Output.
	// Turning these into applied database state is the caller's job
	// (internal/fsm, once Phase 5 wires the two together) — Core never
	// calls Apply itself and does not track appliedIndex; see
	// Core.AppliedIndex/Core.SetApplied for the bookkeeping-only
	// distinction Phase 4 keeps in place for Phase 5 to use.
	CommittedEntries []Entry

	// BecameLeader is true exactly on the Step call in which this node
	// transitioned to Leader. Convenience for observability/tests only;
	// no output field is load-bearing for correctness on its own.
	BecameLeader bool
	// SteppedDown is true exactly on the Step call in which this node
	// left Leader or Candidate role because of an observed higher term.
	SteppedDown bool

	// ProposalRejected is true when an InputPropose was rejected
	// because this Core is not currently Leader.
	ProposalRejected bool
	// LeaderHint is this Core's best current knowledge of the cluster
	// leader (possibly "" if unknown), valid when ProposalRejected.
	LeaderHint NodeID
}
