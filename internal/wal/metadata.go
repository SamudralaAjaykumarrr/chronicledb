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
}

// encodeMetadata serializes m into a Metadata record payload.
func encodeMetadata(m Metadata) []byte {
	idBytes := []byte(m.NodeID)
	buf := make([]byte, 1+8+2+len(idBytes))
	buf[0] = m.FormatVersion
	binary.BigEndian.PutUint64(buf[1:9], m.LatestSnapshotIndex)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(idBytes)))
	copy(buf[11:], idBytes)
	return buf
}

// decodeMetadata parses a Metadata record payload. It never trusts the
// embedded node-id length beyond the bytes actually present.
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
	return Metadata{NodeID: id, FormatVersion: version, LatestSnapshotIndex: snap}, nil
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
