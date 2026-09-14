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
// conversion at its boundary). Meta.Configuration is likewise a
// package-local mirror of raft.Configuration (dynamic-membership plan
// §7.1), for the identical reason.
package snapshot

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
)

// FormatVersion is the snapshot file format version this build WRITES,
// checked before any snapshot is ever trusted (docs/snapshots.md §5
// point 1), mirroring internal/wal's own format-version discipline.
//
// MinReadVersion is the oldest version this build READS. The two
// together replace v1's strict equality check with a bounded supported
// range (dynamic-membership plan §7.1): NO SILENT FORMAT
// MISINTERPRETATION is preserved (an unrecognized version, above
// FormatVersion or below MinReadVersion, is never guessed at) but the
// mechanism is a range rather than equality, because a v0.5.0 binary
// must still read its own pre-existing v1 snapshots and any v1 backup.
const (
	FormatVersion  uint8 = 2
	MinReadVersion uint8 = 1
)

// magic is a fixed 4-byte prefix identifying a ChronicleDB snapshot
// file, so a snapshot directory accidentally containing an unrelated
// file fails fast and explicitly rather than being misparsed.
var magic = [4]byte{'C', 'S', 'N', 'P'}

// Member mirrors raft.Member: a node identity paired with its dial
// address. This package has no dependency on internal/raft (see the
// package doc comment); internal/node converts at its own boundary,
// exactly as it already does for LastIncludedIndex/Term's
// raft.Index/raft.Term <-> uint64 conversion.
type Member struct {
	ID      string
	Address string
}

// Configuration mirrors raft.Configuration.
type Configuration struct {
	Voters   []Member
	Learners []Member
}

// Meta is the consensus boundary a snapshot corresponds to
// (docs/snapshots.md §2): the Raft log position up through which this
// snapshot's state-machine content is complete and authoritative, plus
// (v2+) the cluster membership configuration effective at that boundary.
type Meta struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64

	// HasConfiguration reports whether this snapshot carries a
	// configuration at all. It is a durable, explicitly encoded bit in
	// v2 — never inferred from Configuration being empty, which is a
	// legitimate value and a different statement (dynamic-membership
	// plan §7.1/§23 F7). Always false when decoded from a v1 file, and
	// deliberately false in a snapshot staged by internal/backup's
	// restore path (§7.6).
	HasConfiguration bool
	// Configuration is meaningful iff HasConfiguration is true;
	// otherwise it is the zero value and must not be read.
	Configuration Configuration
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
//	v1: magic(4B) version(1B)=1 lastIncludedIndex(8B) lastIncludedTerm(8B)
//	      fsmStateLen(8B) fsmState(...) crc32(4B)
//	v2: magic(4B) version(1B)=2 lastIncludedIndex(8B) lastIncludedTerm(8B)
//	      fsmStateLen(8B) fsmState(...)
//	      hasConfig(1B) configLen(8B) config(configLen bytes)
//	      crc32(4B)
const (
	headerSize   = 4 + 1 + 8 + 8 + 8
	checksumSize = 4
)

// Encode serializes meta and f's current state into a single framed,
// checksummed byte slice (docs/snapshots.md §2) — pure, no I/O. f's
// state is captured via fsm.FSM.EncodeState, which itself guarantees a
// single atomic, consistent point-in-time view (see that method's doc
// comment).
//
// writeVersion is the explicit write-version gate (dynamic-membership
// plan §7.1's "ROLLBACK BOUNDARY HONESTY"): a caller passes 1 until the
// cluster has finalized to generation >= 2, and 2 thereafter, rather
// than this package silently always writing its own newest
// FormatVersion — a pre-finalize v0.5.0 snapshot must be byte-identical
// to what v0.4.0 itself would produce. Passing a value outside
// [MinReadVersion, FormatVersion] panics: this is a programmer error
// (an internal caller passing a version this build cannot itself
// produce), never a runtime/input condition.
func Encode(meta Meta, f *fsm.FSM, writeVersion uint8) []byte {
	if writeVersion < MinReadVersion || writeVersion > FormatVersion {
		panic(fmt.Sprintf("snapshot: Encode: writeVersion %d outside supported range [%d, %d]", writeVersion, MinReadVersion, FormatVersion))
	}
	state := f.EncodeState()

	var configSection []byte
	if writeVersion >= 2 {
		configSection = encodeConfigSection(meta.HasConfiguration, meta.Configuration)
	}

	size := headerSize + len(state) + len(configSection) + checksumSize
	buf := make([]byte, size)
	off := 0
	off += copy(buf[off:], magic[:])
	buf[off] = writeVersion
	off++
	binary.BigEndian.PutUint64(buf[off:], meta.LastIncludedIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], meta.LastIncludedTerm)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(len(state)))
	off += 8
	off += copy(buf[off:], state)
	off += copy(buf[off:], configSection)
	crc := crc32.ChecksumIEEE(buf[:off])
	binary.BigEndian.PutUint32(buf[off:], crc)
	return buf
}

// encodeConfigSection encodes the v2 hasConfig/configLen/config trio.
// hasConfig and configLen are always both present, keeping the frame's
// shape fixed regardless of the flag (dynamic-membership plan §7.1).
func encodeConfigSection(hasConfig bool, cfg Configuration) []byte {
	var cfgBytes []byte
	if hasConfig {
		cfgBytes = encodeConfiguration(cfg)
	}
	buf := make([]byte, 1+8+len(cfgBytes))
	off := 0
	if hasConfig {
		buf[off] = 1
	}
	off++
	binary.BigEndian.PutUint64(buf[off:], uint64(len(cfgBytes)))
	off += 8
	off += copy(buf[off:], cfgBytes)
	return buf[:off]
}

func encodeConfiguration(cfg Configuration) []byte {
	size := 4
	for _, m := range cfg.Voters {
		size += 4 + len(m.ID) + 4 + len(m.Address)
	}
	size += 4
	for _, m := range cfg.Learners {
		size += 4 + len(m.ID) + 4 + len(m.Address)
	}
	buf := make([]byte, size)
	off := 0
	off = putMembers(buf, off, cfg.Voters)
	off = putMembers(buf, off, cfg.Learners)
	return buf[:off]
}

func putMembers(buf []byte, off int, members []Member) int {
	binary.BigEndian.PutUint32(buf[off:], uint32(len(members)))
	off += 4
	for _, m := range members {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(m.ID)))
		off += 4
		off += copy(buf[off:], m.ID)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(m.Address)))
		off += 4
		off += copy(buf[off:], m.Address)
	}
	return off
}

func getMembers(b []byte, off int) ([]Member, int, error) {
	if len(b)-off < 4 {
		return nil, off, fmt.Errorf("%w: truncated member count", ErrCorrupt)
	}
	count := binary.BigEndian.Uint32(b[off:])
	off += 4
	const minMemberSize = 4 + 4
	if remaining := len(b) - off; count > uint32(remaining/minMemberSize) {
		return nil, off, fmt.Errorf("%w: declares %d members but only %d bytes remain", ErrCorrupt, count, remaining)
	}
	members := make([]Member, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(b)-off < 4 {
			return nil, off, fmt.Errorf("%w: truncated member %d id length", ErrCorrupt, i)
		}
		idLen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if int64(idLen) > int64(len(b)-off) {
			return nil, off, fmt.Errorf("%w: truncated member %d id", ErrCorrupt, i)
		}
		id := string(b[off : off+int(idLen)])
		off += int(idLen)
		if len(b)-off < 4 {
			return nil, off, fmt.Errorf("%w: truncated member %d address length", ErrCorrupt, i)
		}
		addrLen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if int64(addrLen) > int64(len(b)-off) {
			return nil, off, fmt.Errorf("%w: truncated member %d address", ErrCorrupt, i)
		}
		addr := string(b[off : off+int(addrLen)])
		off += int(addrLen)
		members = append(members, Member{ID: id, Address: addr})
	}
	return members, off, nil
}

func decodeConfiguration(b []byte) (Configuration, error) {
	voters, off, err := getMembers(b, 0)
	if err != nil {
		return Configuration{}, err
	}
	learners, off2, err := getMembers(b, off)
	if err != nil {
		return Configuration{}, err
	}
	if off2 != len(b) {
		return Configuration{}, fmt.Errorf("%w: %d trailing bytes in configuration section", ErrCorrupt, len(b)-off2)
	}
	return Configuration{Voters: voters, Learners: learners}, nil
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
// version is checked against the bounded [MinReadVersion, FormatVersion]
// range (dynamic-membership plan §7.1), not strict equality: a version
// above FormatVersion or below MinReadVersion is ErrUnsupportedVersion,
// fail-closed, before any further byte is interpreted.
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
	switch {
	case version > FormatVersion:
		return Snapshot{}, fmt.Errorf("%w: snapshot format version %d, this build writes/reads at most %d", ErrUnsupportedVersion, version, FormatVersion)
	case version < MinReadVersion:
		return Snapshot{}, fmt.Errorf("%w: snapshot format version %d, this build reads at least %d", ErrUnsupportedVersion, version, MinReadVersion)
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
	state := data[off:stateEnd]
	off = stateEnd

	if version >= 2 {
		if len(data)-off-checksumSize < 1+8 {
			return Snapshot{}, fmt.Errorf("%w: truncated v2 configuration section header", ErrCorrupt)
		}
		hasConfigByte := data[off]
		off++
		configLen := binary.BigEndian.Uint64(data[off:])
		off += 8
		switch hasConfigByte {
		case 0x00:
			if configLen != 0 {
				return Snapshot{}, fmt.Errorf("%w: hasConfiguration=false but configLen=%d", ErrCorrupt, configLen)
			}
		case 0x01:
			if configLen == 0 {
				return Snapshot{}, fmt.Errorf("%w: hasConfiguration=true but configLen=0", ErrCorrupt)
			}
		default:
			return Snapshot{}, fmt.Errorf("%w: invalid hasConfiguration byte %d", ErrCorrupt, hasConfigByte)
		}
		if configLen > uint64(len(data)-off-checksumSize) {
			return Snapshot{}, fmt.Errorf("%w: declared configuration length %d exceeds bytes present", ErrCorrupt, configLen)
		}
		configEnd := off + int(configLen)
		if hasConfigByte == 0x01 {
			cfg, err := decodeConfiguration(data[off:configEnd])
			if err != nil {
				return Snapshot{}, err
			}
			meta.HasConfiguration = true
			meta.Configuration = cfg
		}
		off = configEnd
	}

	if off+checksumSize != len(data) {
		return Snapshot{}, fmt.Errorf("%w: %d trailing/missing bytes before checksum", ErrCorrupt, len(data)-(off+checksumSize))
	}
	wantCRC := binary.BigEndian.Uint32(data[off:])
	gotCRC := crc32.ChecksumIEEE(data[:off])
	if gotCRC != wantCRC {
		return Snapshot{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	f, maxSeq, err := fsm.DecodeState(state)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: decoding state: %v", ErrCorrupt, err)
	}
	if maxSeq > meta.LastIncludedIndex {
		return Snapshot{}, fmt.Errorf("%w: state references CommitSeq %d beyond claimed boundary %d", ErrCorrupt, maxSeq, meta.LastIncludedIndex)
	}
	return Snapshot{Meta: meta, FSM: f}, nil
}
