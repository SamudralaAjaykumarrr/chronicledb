package txn

import (
	"encoding/binary"
	"fmt"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// commitTxnRecordVersion is the format version of the CommitTxn command
// payload defined by this file. It is a distinct version byte from
// internal/wal's own per-frame format version (docs/wal.md §3): the WAL
// frame's version governs the frame layout, while this one governs the
// layout of the opaque command bytes carried inside a RecordTypeLogEntry
// payload, which internal/wal has no opinion about (docs/wal.md §2).
const commitTxnRecordVersion uint8 = 1

// commitTxnCommand is the in-memory form of the single deterministic
// command a COMMIT submits (docs/transactions.md §3):
//
//	CommitTxn(TxnID, StartSeq, Mutations...)
//
// RequestID is deliberately absent: Phase 2 does not implement the
// RequestID idempotency table (that is Phase 3 scope, per
// docs/roadmap.md). Adding it later requires bumping
// commitTxnRecordVersion, not silently overloading this layout.
type commitTxnCommand struct {
	txnID     uint64
	startSeq  uint64
	mutations []mvcc.Mutation
}

// encodeCommitTxn serializes cmd into a CommitTxn record payload.
//
// Layout:
//
//	version(1B) txnID(8B) startSeq(8B) numMutations(4B)
//	  per mutation: keyLen(4B) key(keyLen) tombstone(1B) [valLen(4B) value(valLen)]
//
// (valLen/value are present only when tombstone == 0.)
func encodeCommitTxn(cmd commitTxnCommand) []byte {
	size := 1 + 8 + 8 + 4
	for _, m := range cmd.mutations {
		size += 4 + len(m.Key) + 1
		if !m.Tombstone {
			size += 4 + len(m.Value)
		}
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = commitTxnRecordVersion
	off++
	binary.BigEndian.PutUint64(buf[off:], cmd.txnID)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], cmd.startSeq)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(len(cmd.mutations)))
	off += 4
	for _, m := range cmd.mutations {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(m.Key)))
		off += 4
		off += copy(buf[off:], m.Key)
		if m.Tombstone {
			buf[off] = 1
		} else {
			buf[off] = 0
		}
		off++
		if !m.Tombstone {
			binary.BigEndian.PutUint32(buf[off:], uint32(len(m.Value)))
			off += 4
			off += copy(buf[off:], m.Value)
		}
	}
	return buf
}

// decodeCommitTxn parses a CommitTxn record payload. It never trusts a
// length or count field beyond the bytes actually remaining in b,
// mirroring internal/wal's decodeFrameBytes bounded-decoding discipline
// (docs/failure-model.md §6, "bounded request sizes / bounded
// allocations"): it returns an error, never panics, on any malformed or
// truncated input, and every allocation it makes is bounded by bytes
// already known to be present in b, not by an unvalidated count field.
func decodeCommitTxn(b []byte) (commitTxnCommand, error) {
	var cmd commitTxnCommand
	const fixedHeaderSize = 1 + 8 + 8 + 4
	if len(b) < fixedHeaderSize {
		return cmd, fmt.Errorf("%w: payload too short (%d bytes, need at least %d)", ErrMalformedRecord, len(b), fixedHeaderSize)
	}
	off := 0
	version := b[off]
	off++
	if version != commitTxnRecordVersion {
		return cmd, fmt.Errorf("%w: record version %d, expected %d", ErrUnsupportedRecordVersion, version, commitTxnRecordVersion)
	}
	cmd.txnID = binary.BigEndian.Uint64(b[off:])
	off += 8
	cmd.startSeq = binary.BigEndian.Uint64(b[off:])
	off += 8
	numMutations := binary.BigEndian.Uint32(b[off:])
	off += 4

	// Bound numMutations against the bytes actually remaining, not an
	// arbitrary constant: the smallest possible encoded mutation is 5
	// bytes (a zero-length key's 4-byte length prefix plus a 1-byte
	// tombstone flag), so no more than remaining/5 mutations can
	// legitimately be present. This prevents a small, corrupt or
	// adversarial payload from declaring an enormous mutation count and
	// forcing a large pre-allocation before any of its content is
	// validated.
	remaining := len(b) - off
	const minEncodedMutationSize = 4 + 1
	if numMutations > uint32(remaining/minEncodedMutationSize) {
		return cmd, fmt.Errorf("%w: declares %d mutations but only %d bytes remain", ErrMalformedRecord, numMutations, remaining)
	}

	mutations := make([]mvcc.Mutation, 0, numMutations)
	for i := uint32(0); i < numMutations; i++ {
		if len(b)-off < 4 {
			return cmd, fmt.Errorf("%w: truncated mutation %d (key length)", ErrMalformedRecord, i)
		}
		keyLen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if int64(keyLen) > int64(len(b)-off) {
			return cmd, fmt.Errorf("%w: truncated mutation %d (key: declared %d bytes, %d remain)", ErrMalformedRecord, i, keyLen, len(b)-off)
		}
		key := string(b[off : off+int(keyLen)])
		off += int(keyLen)

		if len(b)-off < 1 {
			return cmd, fmt.Errorf("%w: truncated mutation %d (tombstone flag)", ErrMalformedRecord, i)
		}
		tombstone := b[off] != 0
		off++

		var value []byte
		if !tombstone {
			if len(b)-off < 4 {
				return cmd, fmt.Errorf("%w: truncated mutation %d (value length)", ErrMalformedRecord, i)
			}
			valLen := binary.BigEndian.Uint32(b[off:])
			off += 4
			if int64(valLen) > int64(len(b)-off) {
				return cmd, fmt.Errorf("%w: truncated mutation %d (value: declared %d bytes, %d remain)", ErrMalformedRecord, i, valLen, len(b)-off)
			}
			value = make([]byte, valLen)
			copy(value, b[off:off+int(valLen)])
			off += int(valLen)
		}
		mutations = append(mutations, mvcc.Mutation{Key: key, Value: value, Tombstone: tombstone})
	}
	if off != len(b) {
		return cmd, fmt.Errorf("%w: %d trailing bytes after decoding %d mutations", ErrMalformedRecord, len(b)-off, numMutations)
	}
	cmd.mutations = mutations
	return cmd, nil
}
