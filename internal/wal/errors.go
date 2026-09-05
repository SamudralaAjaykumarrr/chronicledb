package wal

import "errors"

var (
	// ErrClosed is returned by any WAL method called after Close.
	ErrClosed = errors.New("wal: closed")

	// ErrUnsupportedVersion indicates a fully-framed record whose format
	// version does not match FormatVersion. Startup is refused
	// unconditionally rather than guessing at an unrecognized layout
	// (docs/wal.md §6.3).
	ErrUnsupportedVersion = errors.New("wal: unsupported record format version")

	// ErrRecordTooLarge indicates a fully-framed record whose declared
	// payload length exceeds MaxRecordPayloadSize. Never returned for a
	// record that is merely torn/incomplete.
	ErrRecordTooLarge = errors.New("wal: record payload exceeds maximum allowed size")

	// ErrCorrupt indicates durable-log corruption that must never be
	// silently repaired: a fully-framed record with a bad checksum
	// (anywhere in the log), an out-of-order/duplicate log index, an
	// unknown record type, or a torn tail found somewhere other than the
	// single open (last) segment. Startup refuses unconditionally
	// (docs/wal.md §6.2, docs/recovery.md §1 step 8).
	ErrCorrupt = errors.New("wal: durable log corruption detected; startup refused")

	// ErrNoMetadata indicates a non-empty durable log that never
	// contains a Metadata record, which should be impossible for any log
	// this package created.
	ErrNoMetadata = errors.New("wal: no metadata record found in non-empty durable log")
)

// errTornTail is an internal sentinel distinguishing "not enough bytes
// present to decode a complete frame" from every other decode failure. It
// is never returned to callers directly: Open() converts it into either an
// automatic truncation (last segment) or ErrCorrupt (any earlier
// segment), and Replay() converts it into a clean end-of-stream signal.
var errTornTail = errors.New("wal: torn tail (internal sentinel, not surfaced to callers)")
