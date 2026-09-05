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
	// mutations are applied.
	ErrConflict = errors.New("txn: write-write conflict (first-committer-wins)")

	// ErrMalformedRecord indicates a CommitTxn record's bytes could not
	// be decoded — truncated, or a length/count field inconsistent with
	// the bytes actually present. Never produced by a panic; decoding
	// always fails closed with this error instead (docs/failure-model.md
	// §6).
	ErrMalformedRecord = errors.New("txn: malformed CommitTxn record")

	// ErrUnsupportedRecordVersion indicates a CommitTxn record's own
	// format-version byte does not match what this build understands.
	// Recovery refuses to guess at an unrecognized layout, mirroring
	// internal/wal's ErrUnsupportedVersion policy (docs/wal.md §6.3).
	ErrUnsupportedRecordVersion = errors.New("txn: unsupported CommitTxn record version")
)
