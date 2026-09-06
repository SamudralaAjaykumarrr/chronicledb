package raft

import "fmt"

// Storage is the persistence contract a driver must satisfy to run a
// Core (docs/adr/0009). Core.Step itself never calls Storage — it
// requests durable work via Output.PersistRequest and learns of its
// completion via Input{Kind: InputPersistenceComplete} (docs/raft.md
// §1: "The Raft core never performs network I/O, disk I/O, or clock
// reads itself"). A driver applies each PersistRequest to a concrete
// Storage implementation and reports back once it durably lands.
//
// Production Storage is satisfied by internal/wal (Phase 5, not yet
// wired — see docs/roadmap.md Phase 4/5 boundary and the
// implementation note in docs/raft.md). internal/fault.MemoryStorage
// satisfies it today for the deterministic simulator
// (docs/testing-strategy.md §3.1).
type Storage interface {
	// InitialState returns the HardState most recently durably set (or
	// the zero HardState for a brand-new store), for use when
	// constructing a Core after restart.
	InitialState() (HardState, error)

	// SetHardState durably persists hs, replacing whatever was
	// previously stored.
	SetHardState(hs HardState) error

	// Entries returns entries in the half-open range [lo, hi). Both
	// bounds are 1-based log indices.
	Entries(lo, hi Index) ([]Entry, error)

	// LastIndex returns the index of the last durable log entry, or 0
	// if the log is empty.
	LastIndex() (Index, error)

	// Truncate durably discards every entry at index >= fromIndex. A
	// no-op if fromIndex > LastIndex(). Storage must never be asked (by
	// a correct driver) to truncate at or below any index it has
	// already reported as part of a committed Output.CommittedEntries
	// batch — Core enforces this on its own side (docs/raft.md §3:
	// "never truncate committed entries") before ever emitting such a
	// PersistRequest.
	Truncate(fromIndex Index) error

	// Append durably appends entries, which must extend Storage's
	// current log contiguously starting at LastIndex()+1. A driver
	// calls Truncate first if PersistRequest.TruncateFrom is nonzero.
	Append(entries []Entry) error
}

// ApplyPersistRequest performs the exact write sequence PersistRequest
// documents against s: truncate (if requested), then append, then set
// hard state. It is provided so every driver (test and, eventually,
// production) performs identically ordered writes rather than each
// re-deriving the ordering rule independently.
func ApplyPersistRequest(s Storage, pr *PersistRequest) error {
	if pr == nil {
		return nil
	}
	if pr.TruncateFrom != 0 {
		if err := s.Truncate(pr.TruncateFrom); err != nil {
			return fmt.Errorf("raft: truncate from %d: %w", pr.TruncateFrom, err)
		}
	}
	if len(pr.Entries) != 0 {
		if err := s.Append(pr.Entries); err != nil {
			return fmt.Errorf("raft: append %d entries: %w", len(pr.Entries), err)
		}
	}
	if pr.HardState != nil {
		if err := s.SetHardState(*pr.HardState); err != nil {
			return fmt.Errorf("raft: set hard state: %w", err)
		}
	}
	return nil
}
