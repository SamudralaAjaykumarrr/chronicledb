package wal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// Metadata is internal/wal's node-local metadata record (docs/wal.md §8):
// node identity (stable across restarts), the WAL format version it was
// written with, and a pointer to the most recent valid snapshot. Phase 1
// has no snapshot mechanism, so LatestSnapshotIndex is always 0 ("none").
type Metadata struct {
	NodeID              string
	FormatVersion       uint8
	LatestSnapshotIndex uint64
	// ClusterGeneration is the highest cluster-version generation this
	// node has durably observed finalized (docs/enterprise-v1-plan.md
	// §7, docs/upgrades.md) — a diagnostic mirror of
	// fsm.FSM.ClusterGeneration(), persisted here so Open can refuse to
	// start (WAL.CheckSupportedGeneration) before ever touching Raft/FSM
	// replay when a data directory has been finalized past what this
	// binary's internal/version.MaxSupportedGeneration understands.
	// Zero (the default, and the only value any pre-v0.4.0-written
	// metadata record has) means "never finalized."
	ClusterGeneration uint32
}

// encodeMetadata serializes m into a Metadata record payload. Like
// fsm.encodeState (internal/fsm/snapshot.go), ClusterGeneration is
// appended as a trailing 4-byte field ONLY when nonzero: a
// not-yet-finalized node writes byte-identical metadata to every prior
// release, so decodeMetadata's identical field-count logic below is
// what actually matters for compatibility — see its doc comment.
func encodeMetadata(m Metadata) []byte {
	idBytes := []byte(m.NodeID)
	size := 1 + 8 + 2 + len(idBytes)
	if m.ClusterGeneration > 0 {
		size += 4
	}
	buf := make([]byte, size)
	buf[0] = m.FormatVersion
	binary.BigEndian.PutUint64(buf[1:9], m.LatestSnapshotIndex)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(idBytes)))
	off := 11 + copy(buf[11:], idBytes)
	if m.ClusterGeneration > 0 {
		binary.BigEndian.PutUint32(buf[off:], m.ClusterGeneration)
	}
	return buf
}

// decodeMetadata parses a Metadata record payload. It never trusts the
// embedded node-id length beyond the bytes actually present.
//
// ClusterGeneration is read tolerantly: a record with nothing after
// the node-id bytes (every metadata record any binary before this
// field existed ever wrote, and every not-yet-finalized v0.4.0+ one —
// see encodeMetadata) decodes with ClusterGeneration left at its zero
// value; a record with exactly 4 trailing bytes decodes them as
// ClusterGeneration. This mirrors — deliberately, not coincidentally —
// this function's own PRE-EXISTING behavior of never validating that
// len(b) matches exactly 11+idLen: a pre-v0.4.0 binary's own unmodified
// decodeMetadata already silently ignores any trailing bytes it does
// not know about, so a v0.4.0+ node appending a nonzero ClusterGeneration
// (only possible once finalize has actually run — encodeMetadata again)
// remains readable, without any code change, by an old binary that
// never learns the value exists. Any length other than 11+idLen+{0,4}
// is genuinely malformed and rejected below.
func decodeMetadata(b []byte) (Metadata, error) {
	if len(b) < 11 {
		return Metadata{}, fmt.Errorf("wal: metadata payload too short (%d bytes)", len(b))
	}
	version := b[0]
	snap := binary.BigEndian.Uint64(b[1:9])
	idLen := binary.BigEndian.Uint16(b[9:11])
	if int(idLen) > len(b)-11 {
		return Metadata{}, fmt.Errorf("wal: metadata payload truncated node id (declared %d, have %d)", idLen, len(b)-11)
	}
	id := string(b[11 : 11+int(idLen)])
	off := 11 + int(idLen)
	var clusterGeneration uint32
	switch extra := len(b) - off; extra {
	case 0:
	case 4:
		clusterGeneration = binary.BigEndian.Uint32(b[off:])
	default:
		return Metadata{}, fmt.Errorf("wal: metadata payload has %d trailing bytes after node id, expected 0 or 4", extra)
	}
	return Metadata{NodeID: id, FormatVersion: version, LatestSnapshotIndex: snap, ClusterGeneration: clusterGeneration}, nil
}

// newNodeID generates a fresh, stable node identity for a brand-new
// durable log.
func newNodeID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not a recoverable condition; the caller
		// would be unable to safely proceed with node identity anyway.
		panic(fmt.Sprintf("wal: crypto/rand unavailable: %v", err))
	}
	return hex.EncodeToString(b[:])
}
