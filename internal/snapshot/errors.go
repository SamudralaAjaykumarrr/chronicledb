package snapshot

import "errors"

var (
	// ErrCorrupt indicates a snapshot failed validation: bad magic, bad
	// checksum, a malformed length, or an internal-consistency violation
	// (docs/snapshots.md §5-6). A snapshot failing with this error is
	// never used — treated as if it does not exist at all.
	ErrCorrupt = errors.New("snapshot: corrupt or invalid snapshot")

	// ErrUnsupportedVersion indicates a snapshot's format version byte
	// does not match FormatVersion. Never guessed at (docs/wal.md §6.3's
	// sibling policy for snapshots).
	ErrUnsupportedVersion = errors.New("snapshot: unsupported snapshot format version")

	// ErrNoSnapshot is returned by Manager.Load when no snapshot is
	// available to load (pointerIndex is 0, or no file matches it) —
	// distinct from a validation failure: this is the expected,
	// non-error "nothing to restore from yet" case for a fresh node.
	ErrNoSnapshot = errors.New("snapshot: no snapshot available")
)
