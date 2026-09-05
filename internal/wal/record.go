package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// RecordType identifies what kind of durable record a frame carries. The
// set is closed and shared by every durable history in ChronicleDB
// (docs/wal.md §2): internal/wal has no opinion about what a LogEntry's
// payload bytes mean.
type RecordType uint8

const (
	// RecordTypeLogEntry carries one entry of the durable ordered log
	// (a Raft log entry in the future, or the standalone commit history
	// in Phase 1-3). Its Index is the gap-free logical WAL/log index.
	RecordTypeLogEntry RecordType = 1
	// RecordTypeHardState carries Raft persistent state (currentTerm,
	// votedFor). Not written by any Phase-1 code path; reserved so the
	// on-disk format does not need to change when Raft (Phase 4) begins
	// using it. internal/wal treats its payload as opaque bytes, exactly
	// like RecordTypeLogEntry.
	RecordTypeHardState RecordType = 2
	// RecordTypeMetadata carries internal/wal's own node-local metadata
	// (docs/wal.md §8): node identity, format version, and a pointer to
	// the most recent snapshot (always "none" in Phase 1, since no
	// snapshot mechanism exists yet).
	RecordTypeMetadata RecordType = 3
)

func (t RecordType) valid() bool {
	switch t {
	case RecordTypeLogEntry, RecordTypeHardState, RecordTypeMetadata:
		return true
	default:
		return false
	}
}

// FormatVersion is the current WAL record format version. It is written
// into every record's version field and checked on read; an unrecognized
// version causes startup to refuse rather than guess at the layout
// (docs/wal.md §6, docs/failure-model.md §6).
const FormatVersion uint8 = 1

// MaxRecordPayloadSize bounds the payload length internal/wal will ever
// trust or allocate for, regardless of what a (possibly corrupt) length
// field on disk claims (docs/failure-model.md §6 "Security and Safety
// Expectations": bounded request sizes / bounded allocations).
const MaxRecordPayloadSize = 64 * 1024 * 1024 // 64 MiB

// Frame layout (docs/wal.md §3):
//
//	+----------+----------+-----------+-----------+------------------+----------+
//	| type (1B)| index(8B)| length(4B)| version(1B)| payload (length) | crc32(4B)|
//	+----------+----------+-----------+-----------+------------------+----------+
const (
	headerTypeOff    = 0
	headerIndexOff   = 1
	headerLengthOff  = 9
	headerVersionOff = 13
	headerSize       = 14
	checksumSize     = 4
)

// Record is one decoded, checksum-verified WAL record.
type Record struct {
	Type    RecordType
	Index   uint64
	Payload []byte
}

// encodeRecord frames rec as bytes ready to append to a segment.
func encodeRecord(rt RecordType, index uint64, payload []byte) []byte {
	frame := make([]byte, headerSize+len(payload)+checksumSize)
	frame[headerTypeOff] = byte(rt)
	binary.BigEndian.PutUint64(frame[headerIndexOff:], index)
	binary.BigEndian.PutUint32(frame[headerLengthOff:], uint32(len(payload)))
	frame[headerVersionOff] = FormatVersion
	copy(frame[headerSize:], payload)
	crc := crc32.ChecksumIEEE(frame[:headerSize+len(payload)])
	binary.BigEndian.PutUint32(frame[headerSize+len(payload):], crc)
	return frame
}

// decodeFrameBytes parses exactly one frame from the start of buf, which
// holds all bytes currently available from the record's start to the end
// of its segment (there may be nothing after it, or there may be more
// bytes than this one frame needs).
//
// It never allocates more than MaxRecordPayloadSize for a payload, and it
// never panics on malformed input (docs/failure-model.md §6). It returns:
//   - (rec, frameLen, nil) on a fully framed, checksum-valid record;
//   - (nil, 0, errTornTail) if buf is too short to contain a complete
//     frame — the signature of a crash during an in-progress append
//     (docs/wal.md §6.1);
//   - (nil, 0, err) wrapping ErrUnsupportedVersion, ErrRecordTooLarge, or
//     ErrCorrupt for a fully-framed record that is nonetheless invalid
//     (docs/wal.md §6.2) — these are never treated as torn tails.
func decodeFrameBytes(buf []byte) (rec *Record, frameLen int, err error) {
	if len(buf) < headerSize {
		return nil, 0, errTornTail
	}
	rt := RecordType(buf[headerTypeOff])
	index := binary.BigEndian.Uint64(buf[headerIndexOff:])
	length := binary.BigEndian.Uint32(buf[headerLengthOff:])
	version := buf[headerVersionOff]

	if version != FormatVersion {
		return nil, 0, fmt.Errorf("wal: %w: record version %d, expected %d: %w", ErrUnsupportedVersion, version, FormatVersion, ErrCorrupt)
	}

	need := headerSize + int64(length) + checksumSize
	if int64(len(buf)) < need {
		// Not enough bytes are physically present for this frame. This
		// covers both a torn header/payload and a length field so large
		// it could never legitimately fit; in either case we must not
		// allocate based on the claimed length, and truncation (if this
		// is indeed the final record) is the only safe automatic repair.
		return nil, 0, errTornTail
	}
	if length > MaxRecordPayloadSize {
		// Enough bytes ARE present (need <= len(buf)), so this is not a
		// torn write — it is a fully framed record claiming an oversized
		// payload, which must be rejected outright.
		return nil, 0, fmt.Errorf("wal: %w: record claims %d bytes, max %d", ErrRecordTooLarge, length, MaxRecordPayloadSize)
	}
	if !rt.valid() {
		return nil, 0, fmt.Errorf("wal: unknown record type %d: %w", rt, ErrCorrupt)
	}

	payloadEnd := headerSize + int64(length)
	wantCRC := binary.BigEndian.Uint32(buf[payloadEnd : payloadEnd+checksumSize])
	gotCRC := crc32.ChecksumIEEE(buf[:payloadEnd])
	if gotCRC != wantCRC {
		return nil, 0, fmt.Errorf("wal: %w: checksum mismatch (type=%d index=%d)", ErrCorrupt, rt, index)
	}

	payload := make([]byte, length)
	copy(payload, buf[headerSize:payloadEnd])
	return &Record{Type: rt, Index: index, Payload: payload}, int(need), nil
}
