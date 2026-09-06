package fsm

import (
	"errors"
	"fmt"
)

// Status is a RequestID's terminal outcome status (docs/transactions.md
// §6, ADR-0006: RequestID -> COMMITTED(CommitSeq) | ABORTED(reason)).
type Status int

const (
	// StatusCommitted means every mutation in the command was applied
	// atomically at Outcome.CommitSeq.
	StatusCommitted Status = iota
	// StatusAborted means none of the command's mutations were applied
	// — either a Snapshot Isolation write-write conflict
	// (ConflictKey/ConflictLatestSeq describe it) or a malformed/
	// rejected command.
	StatusAborted
)

func (s Status) String() string {
	switch s {
	case StatusCommitted:
		return "committed"
	case StatusAborted:
		return "aborted"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// Outcome is a RequestID's durable, stable terminal result
// (docs/invariants.md REQUEST-OUTCOME-STABILITY): once Apply has
// produced one for a given RequestID, it never changes on retry,
// restart, or replay.
type Outcome struct {
	RequestID RequestID
	Status    Status

	// CommitSeq is set only when Status == StatusCommitted: the
	// logical sequence number (durable log index, pre-Raft; committed
	// Raft log index once Raft exists) assigned to this command's
	// mutations (docs/architecture.md §3).
	CommitSeq uint64

	// ConflictKey/ConflictLatestSeq are set only when Status ==
	// StatusAborted due to a write-write conflict: the first
	// conflicting key found (docs/mvcc.md §4 evaluation order) and the
	// committed CommitSeq that exceeded the command's StartSeq.
	ConflictKey       string
	ConflictLatestSeq uint64
}

var (
	// ErrRequestIDUnknown is returned by a read-only outcome lookup
	// (Precheck, GetOutcome) for a RequestID that has no recorded
	// terminal outcome. This is explicitly a "client-knowledge" signal
	// distinct from any database outcome (docs/transactions.md §7.1) —
	// callers must never interpret it as failure or success.
	ErrRequestIDUnknown = errors.New("fsm: RequestID has no recorded outcome")

	// ErrRequestIDPayloadMismatch is returned when a RequestID already
	// has a recorded outcome, but the command now presented under that
	// same RequestID has a different fingerprint (TxnID, StartSeq, or
	// Mutations differ from what was originally recorded). Per
	// docs/transactions.md §6 and this phase's safe-default policy,
	// ChronicleDB never guesses which submission "wins" — it rejects
	// the ambiguous reuse outright. The original RequestID's recorded
	// outcome is left completely unchanged.
	ErrRequestIDPayloadMismatch = errors.New("fsm: RequestID reused with a different command")

	// ErrMalformedCommand indicates a CommitTxn command's bytes could
	// not be decoded — truncated, or a length/count field inconsistent
	// with the bytes actually present. Never produced by a panic;
	// decoding always fails closed with this error instead
	// (docs/failure-model.md §6).
	ErrMalformedCommand = errors.New("fsm: malformed CommitTxn command")

	// ErrUnsupportedCommandVersion indicates a CommitTxn command's own
	// format-version byte does not match what this build understands.
	// Recovery refuses to guess at an unrecognized layout, mirroring
	// internal/wal's ErrUnsupportedVersion policy (docs/wal.md §6.3).
	ErrUnsupportedCommandVersion = errors.New("fsm: unsupported CommitTxn command version")
)
