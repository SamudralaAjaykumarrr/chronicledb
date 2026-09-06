// Package snapshot implements ChronicleDB's coordinated state-machine
// snapshot: encoding, checksum/version framing, crash-safe durable
// creation, validation, and installation, per docs/snapshots.md and
// ADR-0011. There is exactly one snapshot format here — it carries both
// the Raft consensus boundary (LastIncludedIndex/LastIncludedTerm) and
// the internal/fsm state it corresponds to together, in one file — never
// two independently-evolving "database backup" and "Raft snapshot"
// formats (docs/snapshots.md §1).
//
// This package depends on internal/fsm (to serialize/restore state) and
// internal/storage (durable, atomic file writes), per
// docs/architecture.md §5's component map. It has no knowledge of
// internal/raft or internal/node — Meta's LastIncludedIndex/
// LastIncludedTerm are plain uint64s here, not raft.Index/raft.Term,
// keeping this package reusable independently of any particular
// consensus core's types (the driver, internal/node, does that
// conversion at its boundary).
package snapshot

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
)

// FormatVersion is the current snapshot file format version, checked
// before any snapshot is ever trusted (docs/snapshots.md §5 point 1),
// mirroring internal/wal's own format-version discipline.
const FormatVersion uint8 = 1

// magic is a fixed 4-byte prefix identifying a ChronicleDB snapshot
// file, so a snapshot directory accidentally containing an unrelated
// file fails fast and explicitly rather than being misparsed.
var magic = [4]byte{'C', 'S', 'N', 'P'}

// Meta is the consensus boundary a snapshot corresponds to
// (docs/snapshots.md §2): the Raft log position up through which this
// snapshot's state-machine content is complete and authoritative.
type Meta struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// Snapshot is one fully-decoded, checksum-and-version-validated
// ChronicleDB snapshot: the consensus boundary plus a ready-to-use
// internal/fsm.FSM restored from its content (docs/snapshots.md §1's
// "one coordinated snapshot" model — there is nothing else to decode or
// separately validate once a Snapshot value exists).
type Snapshot struct {
	Meta Meta
	FSM  *fsm.FSM
}

// frame layout:
//
//	magic(4B) version(1B) lastIncludedIndex(8B) lastIncludedTerm(8B)
//	  fsmStateLen(8B) fsmState(fsmStateLen bytes) crc32(4B)
const (
	headerSize   = 4 + 1 + 8 + 8 + 8
	checksumSize = 4
)

// Encode serializes meta and f's current state into a single framed,
// checksummed byte slice (docs/snapshots.md §2) — pure, no I/O. f's
// state is captured via fsm.FSM.EncodeState, which itself guarantees a
// single atomic, consistent point-in-time view (see that method's doc
// comment).
func Encode(meta Meta, f *fsm.FSM) []byte {
	state := f.EncodeState()
	buf := make([]byte, headerSize+len(state)+checksumSize)
	off := 0
	off += copy(buf[off:], magic[:])
	buf[off] = FormatVersion
	off++
	binary.BigEndian.PutUint64(buf[off:], meta.LastIncludedIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], meta.LastIncludedTerm)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(len(state)))
	off += 8
	off += copy(buf[off:], state)
	crc := crc32.ChecksumIEEE(buf[:off])
	binary.BigEndian.PutUint32(buf[off:], crc)
	return buf
}

// Decode validates data's framing, format version, checksum, and
// internal consistency (docs/snapshots.md §5), then restores a ready-to
// -use FSM from it. It never trusts a length field beyond the bytes
// actually present and never panics on malformed or adversarial input
// (docs/failure-model.md §6) — every error path returns a plain error,
// and a non-nil error always means data must be treated as if it does
// not exist (docs/snapshots.md §6: "a snapshot that fails validation is
// never used").
//
// The internal-consistency spot-check (docs/snapshots.md §5 point 3)
// verifies that no decoded MVCC version or RequestID outcome references
// a CommitSeq beyond meta.LastIncludedIndex — a snapshot claiming a
// boundary it does not actually honor is corruption, not a legitimate
// variant.
func Decode(data []byte) (Snapshot, error) {
	if len(data) < headerSize+checksumSize {
		return Snapshot{}, fmt.Errorf("%w: snapshot too short (%d bytes)", ErrCorrupt, len(data))
	}
	off := 0
	if [4]byte(data[off:off+4]) != magic {
		return Snapshot{}, fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	off += 4
	version := data[off]
	off++
	if version != FormatVersion {
		return Snapshot{}, fmt.Errorf("%w: snapshot format version %d, expected %d", ErrUnsupportedVersion, version, FormatVersion)
	}
	meta := Meta{
		LastIncludedIndex: binary.BigEndian.Uint64(data[off:]),
	}
	off += 8
	meta.LastIncludedTerm = binary.BigEndian.Uint64(data[off:])
	off += 8
	stateLen := binary.BigEndian.Uint64(data[off:])
	off += 8

	if stateLen > uint64(len(data)-off-checksumSize) {
		return Snapshot{}, fmt.Errorf("%w: declared state length %d exceeds bytes present", ErrCorrupt, stateLen)
	}
	stateEnd := off + int(stateLen)
	if stateEnd+checksumSize != len(data) {
		return Snapshot{}, fmt.Errorf("%w: %d trailing/missing bytes after declared state length", ErrCorrupt, len(data)-(stateEnd+checksumSize))
	}
	wantCRC := binary.BigEndian.Uint32(data[stateEnd:])
	gotCRC := crc32.ChecksumIEEE(data[:stateEnd])
	if gotCRC != wantCRC {
		return Snapshot{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	state := data[off:stateEnd]
	f, maxSeq, err := fsm.DecodeState(state)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: decoding state: %v", ErrCorrupt, err)
	}
	if maxSeq > meta.LastIncludedIndex {
		return Snapshot{}, fmt.Errorf("%w: state references CommitSeq %d beyond claimed boundary %d", ErrCorrupt, maxSeq, meta.LastIncludedIndex)
	}
	return Snapshot{Meta: meta, FSM: f}, nil
}
