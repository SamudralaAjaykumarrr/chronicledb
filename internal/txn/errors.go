package txn

import "errors"

var (
	// ErrAlreadyCommitted is returned by any operation (Read, Write,
	// Delete, Commit, Abort) attempted against a transaction that has
	// already committed.
	ErrAlreadyCommitted = errors.New("txn: transaction already committed")

	// ErrAlreadyAborted is returned by any operation attempted against a
	// transaction that has already aborted (explicitly, or implicitly via
	// a failed commit — see Manager.commit).
	ErrAlreadyAborted = errors.New("txn: transaction already aborted")

	// ErrInvalidState indicates a Txn was found in a state other than the
	// three defined lifecycle states. Unreachable given the package's own
	// state transitions; a defensive fallback, not an expected runtime
	// condition.
	ErrInvalidState = errors.New("txn: invalid transaction state")

	// ErrConflict indicates a write-write conflict under first-committer-
	// wins (docs/mvcc.md §4): the entire transaction aborts, none of its
	// mutations are applied. See internal/fsm.Outcome's ConflictKey/
	// ConflictLatestSeq for the deterministic detail.
	ErrConflict = errors.New("txn: write-write conflict (first-committer-wins)")
)
