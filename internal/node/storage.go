// Package node implements ChronicleDB's process-level wiring
// (docs/architecture.md §5, Phase 5): it owns a raft.Core, a concrete
// internal/wal-backed persistent store, an internal/fsm state machine,
// a production internal/transport, and the client-facing proposal path
// that ties committed Raft entries to fsm.Apply — the "real replicated
// storage / quorum commits / leader failover" phase.
package node

import (
	"encoding/binary"
	"fmt"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// WALStorage is the production raft.Storage implementation
// (docs/raft.md §9.4, docs/roadmap.md Phase 5): it persists Raft's
// currentTerm/votedFor and log entries through internal/wal, the same
// durable log mechanism the rest of ChronicleDB uses
// (docs/invariants.md CONSISTENT-LOG-RESPONSIBILITY) — there is no
// second, Raft-only physical log.
//
// internal/wal treats both the HardState and LogEntry record payloads
// as opaque bytes (docs/architecture.md §5: internal/wal must not know
// Raft semantics), so WALStorage owns the small encode/decode step that
// turns a raft.HardState / (raft.Term, data) pair into the bytes
// internal/wal actually stores, and back.
//
// WALStorage keeps an in-memory mirror of the durable log's entries and
// hard state so raft.Storage's read methods (Entries, LastIndex,
// InitialState) are cheap and do not need to re-read the WAL's segment
// files on every Raft Step call; the WAL itself remains the durable
// source of truth (this mirror is reconstructed from it at Open,
// docs/recovery.md §9). WALStorage is not safe for concurrent use by
// multiple goroutines: it is designed to be owned by a single Raft
// event-loop goroutine (see node.go), mirroring raft.Core's own
// single-threaded-caller contract.
type WALStorage struct {
	w  *wal.WAL
	hs raft.HardState
	// baseIndex is the log index immediately before entries[0]: entries[i]
	// holds log index baseIndex+i+1. 0 for a node with no snapshot yet
	// (exactly Phase 5's original "entries[i] holds index i+1" convention
	// as the baseIndex==0 special case); otherwise the boundary of the
	// most recent snapshot this mirror has been rebased against (via
	// OpenWALStorage at restart, or Compact/InstallSnapshot live —
	// docs/snapshots.md §8), mirroring raft.Core's own snapshotIndex
	// field, which a correct driver always keeps in lockstep with this
	// one.
	baseIndex uint64
	entries   []raft.Entry
}

// OpenWALStorage constructs a WALStorage backed by w, reconstructing its
// in-memory mirror from whatever w's own recovery (wal.Open) already
// validated: the most recent HardState record and every LogEntry record
// from w.FirstIndex() forward (docs/raft.md §5.1, docs/recovery.md §9;
// w.FirstIndex() is 1 for a node with no snapshot yet, matching Phase
// 5's original behavior exactly as a special case). This does not
// itself determine commitIndex/appliedIndex — per ADR-0008, those are
// never trusted from disk and must be re-established by the caller via
// legitimate leader contact or this node's own election, and this does
// not itself restore state-machine content from a snapshot either — the
// caller (Open in node.go) is responsible for loading and validating
// any snapshot via internal/snapshot.Manager and reconciling its
// boundary against w.FirstIndex() before trusting this mirror's start
// point (see Open's doc comment).
func OpenWALStorage(w *wal.WAL) (*WALStorage, error) {
	s := &WALStorage{w: w, baseIndex: w.FirstIndex() - 1}

	if raw := w.LatestHardState(); raw != nil {
		hs, err := decodeHardState(raw)
		if err != nil {
			return nil, fmt.Errorf("node: decoding durable HardState: %w", err)
		}
		s.hs = hs
	}

	it, err := w.Replay(w.FirstIndex())
	if err != nil {
		return nil, fmt.Errorf("node: opening replay iterator: %w", err)
	}
	defer it.Close()
	for {
		rec, ok, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("node: replaying durable log: %w", err)
		}
		if !ok {
			break
		}
		term, typ, data, err := decodeEntryPayload(rec.Payload)
		if err != nil {
			return nil, fmt.Errorf("node: decoding log entry at index %d: %w", rec.Index, err)
		}
		s.entries = append(s.entries, raft.Entry{Index: raft.Index(rec.Index), Term: term, Type: typ, Data: data})
	}
	return s, nil
}

func (s *WALStorage) InitialState() (raft.HardState, error) {
	return s.hs, nil
}

// Reaffirm re-appends this WALStorage's currently known HardState as a
// fresh WAL record (mirroring SetHardState) purely so a copy of it lands
// in the WAL's *current* segment — required immediately before
// internal/wal.WAL.CompactBefore, which never deletes the current
// segment, so this guarantees compaction can never delete the only
// remaining durable copy of HardState (docs/wal.md §7, see
// WAL.CompactBefore's doc comment). Safe to call even when the current
// segment already holds a HardState record (last-one-wins makes a fresh
// append functionally identical); a no-op-safe call otherwise.
func (s *WALStorage) Reaffirm() error {
	return s.SetHardState(s.hs)
}

// Compact discards entries[i] for every index <= uptoIndex from this
// in-memory mirror and advances baseIndex to match — the storage-layer
// counterpart to raft.Core.Compact, always called immediately alongside
// it so the two stay in permanent agreement about which indices this
// node still holds (docs/snapshots.md §8). This does not touch durable
// state at all; internal/wal.WAL.CompactBefore (the durable half) is
// called separately by the same caller. A no-op if uptoIndex does not
// advance baseIndex.
func (s *WALStorage) Compact(uptoIndex raft.Index) {
	if uint64(uptoIndex) <= s.baseIndex {
		return
	}
	pos := uint64(uptoIndex) - s.baseIndex
	if pos > uint64(len(s.entries)) {
		pos = uint64(len(s.entries))
	}
	s.entries = append([]raft.Entry(nil), s.entries[pos:]...)
	s.baseIndex = uint64(uptoIndex)
}

// InstallSnapshot durably adopts a peer's snapshot boundary at
// uptoIndex (docs/snapshots.md §7 step 3-4): it records the new
// snapshot pointer (AppendMetadataSnapshot), re-affirms HardState into
// the current segment, then discards this node's entire durable and
// in-memory log — any entries beyond uptoIndex are necessarily either
// stale/divergent relative to the installed snapshot's origin or
// harmlessly re-derivable (the leader re-sends anything genuinely
// committed once ordinary replication resumes, mirroring
// raft.Core.handleInstallSnapshotRequest's identical "always discard
// the whole log" simplification — see that method's doc comment for why
// this is safe). Truncating to uptoIndex+1 durably resets the WAL's own
// next-index assignment counter too, so subsequent Append calls resume
// exactly at uptoIndex+1, matching the in-memory mirror this leaves
// behind (empty, rebased to uptoIndex). Finally, CompactBefore reclaims
// whole segment files now fully superseded by the new boundary.
//
// The caller (internal/node.Node) must call this — and every write it
// performs must land durably — strictly before ever handing the
// triggering MsgInstallSnapshotRequest to raft.Core.Step, per that
// message's documented driver contract (see raft.MsgInstallSnapshotRequest).
func (s *WALStorage) InstallSnapshot(uptoIndex raft.Index) error {
	if err := s.w.AppendMetadataSnapshot(uint64(uptoIndex)); err != nil {
		return fmt.Errorf("node: recording installed snapshot pointer: %w", err)
	}
	if err := s.Reaffirm(); err != nil {
		return fmt.Errorf("node: reaffirming hard state before compaction: %w", err)
	}
	if err := s.w.Truncate(uint64(uptoIndex) + 1); err != nil {
		return fmt.Errorf("node: discarding superseded log entries: %w", err)
	}
	if err := s.w.CompactBefore(uint64(uptoIndex)); err != nil {
		return fmt.Errorf("node: compacting log after snapshot install: %w", err)
	}
	s.entries = nil
	s.baseIndex = uint64(uptoIndex)
	return nil
}

// SetHardState durably appends and syncs a HardState record before
// returning (docs/raft.md §5: currentTerm/votedFor must survive
// restart before they can affect other nodes' state — ADR-0008), then
// updates the in-memory mirror.
func (s *WALStorage) SetHardState(hs raft.HardState) error {
	payload := encodeHardState(hs)
	if err := s.w.AppendHardState(payload); err != nil {
		return fmt.Errorf("node: appending HardState: %w", err)
	}
	if err := s.w.Sync(); err != nil {
		return fmt.Errorf("node: syncing HardState: %w", err)
	}
	s.hs = hs
	return nil
}

func (s *WALStorage) LastIndex() (raft.Index, error) {
	return raft.Index(s.baseIndex) + raft.Index(len(s.entries)), nil
}

// Entries returns entries in [lo, hi), same as raft.Storage documents,
// but never below baseIndex+1 — this mirror holds nothing at or before
// its own snapshot boundary (docs/snapshots.md §8), exactly as
// raft.Core's own log no longer does past its matching snapshotIndex.
func (s *WALStorage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	base := raft.Index(s.baseIndex)
	if lo < base+1 {
		lo = base + 1
	}
	maxHi := base + raft.Index(len(s.entries)) + 1
	if hi > maxHi {
		hi = maxHi
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]raft.Entry, hi-lo)
	copy(out, s.entries[lo-base-1:hi-base-1])
	return out, nil
}

// Truncate durably discards every entry at index >= fromIndex via
// internal/wal.WAL.Truncate (docs/wal.md's Phase 5 implementation
// note), then trims the in-memory mirror to match. A no-op if
// fromIndex is beyond the current log, matching raft.Storage's
// documented contract.
func (s *WALStorage) Truncate(fromIndex raft.Index) error {
	if err := s.w.Truncate(uint64(fromIndex)); err != nil {
		return fmt.Errorf("node: truncating durable log from %d: %w", fromIndex, err)
	}
	base := raft.Index(s.baseIndex)
	if pos := fromIndex - base - 1; pos < raft.Index(len(s.entries)) {
		if pos < 0 {
			pos = 0
		}
		s.entries = s.entries[:pos]
	}
	return nil
}

// Append durably appends and syncs entries (docs/raft.md §5: log
// entries must survive restart before a follower's acknowledgement or
// a leader's own matchIndex may be released — ADR-0008), then extends
// the in-memory mirror. entries must extend the log contiguously, as
// raft.Storage documents; internal/wal.AppendLogEntry's own index
// assignment additionally, independently enforces gap-free ordering.
func (s *WALStorage) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	want := raft.Index(s.baseIndex) + raft.Index(len(s.entries)) + 1
	for _, e := range entries {
		if e.Index != want {
			return fmt.Errorf("node: WALStorage.Append: non-contiguous append (got index %d, want %d)", e.Index, want)
		}
		want++
	}
	for _, e := range entries {
		idx, err := s.w.AppendLogEntry(encodeEntryPayload(e.Term, e.Type, e.Data))
		if err != nil {
			return fmt.Errorf("node: appending log entry %d: %w", e.Index, err)
		}
		if uint64(e.Index) != idx {
			return fmt.Errorf("node: WAL assigned index %d for raft log index %d (log responsibility mismatch)", idx, e.Index)
		}
	}
	if err := s.w.Sync(); err != nil {
		return fmt.Errorf("node: syncing appended log entries: %w", err)
	}
	s.entries = append(s.entries, entries...)
	return nil
}

// --- Opaque payload encodings (Raft-semantics-aware; deliberately kept
// outside internal/wal, docs/architecture.md §5) ---

// entryPayloadTypeSentinel is the self-describing typed-entry marker
// byte (dynamic-membership plan §6.1a): written at payload offset 8
// iff the entry's Type != raft.EntryNormal. It occupies a namespace
// that does not exist today at that offset — byte 8 is always the
// first byte of Entry.Data in every format that exists prior to this
// phase, which is either fsm.ControlCommandMarker (0xF0) or a small
// commitTxnCommandVersion integer (currently 2). An init() assertion
// below guards this non-collision structurally, mirroring
// fsm.ControlCommandMarker's own existing guard, one layer up at the
// payload-framing level.
const entryPayloadTypeSentinel byte = 0xFF

// knownEntryTypesMax is the highest raft.EntryType value this build
// recognizes; anything above it is ErrUnknownEntryType, fail-closed.
const knownEntryTypesMax = raft.EntryConfig

func init() {
	if entryPayloadTypeSentinel == fsm.ControlCommandMarker {
		panic(fmt.Sprintf("node: entryPayloadTypeSentinel (%d) collides with fsm.ControlCommandMarker (%d)", entryPayloadTypeSentinel, fsm.ControlCommandMarker))
	}
	// commitTxnCommandVersion is not exported, but it is documented (and
	// tested, fsm.TestEntryPayloadSentinelNeverCollides) to be a small,
	// sequentially-incrementing integer starting at 1 — realistically
	// never reaching 0xFF within CommitTxn's own version lineage.
}

// ErrUnknownEntryType indicates a log entry payload's typed-entry
// header names an EntryType this build does not recognize — a decode
// error, never a guess (dynamic-membership plan §6.1a,
// NO SILENT FORMAT MISINTERPRETATION).
var ErrUnknownEntryType = fmt.Errorf("node: unknown log entry type")

// ErrMalformedEntryPayload indicates a log entry payload's typed-entry
// header is truncated or otherwise structurally invalid.
var ErrMalformedEntryPayload = fmt.Errorf("node: malformed log entry payload")

// encodeEntryPayload wraps a raft.Entry's Term ahead of its opaque Data
// bytes, so a single internal/wal RecordTypeLogEntry payload carries
// both — the WAL frame's own Index field already carries the entry's
// log index (docs/wal.md §2's "(term, index, command bytes)": index
// comes from the frame, term+command bytes are this payload).
//
// The typed-entry header (entryPayloadTypeSentinel + a one-byte
// EntryType) is written iff typ != raft.EntryNormal — never gated on
// this node's durable cluster generation, which is updated at a point
// in processOutput's single event-loop pass that is systematically
// stale relative to when this function runs for the very same pass
// (dynamic-membership plan §6.1a/§23 F2): gating on the entry's own
// type is what makes MEMBERSHIP RECOVERY DETERMINISM hold across the
// generation-2 finalization boundary. An untyped payload
// (typ == raft.EntryNormal) is byte-identical to every payload this
// function has ever produced, in every release — the mechanism that
// keeps ROLLBACK BOUNDARY HONESTY structural rather than a runtime
// check: before generation 2, no EntryConfig entry can exist anywhere
// (§8.2's gate), so a pre-finalization node has nothing typed to write.
func encodeEntryPayload(term raft.Term, typ raft.EntryType, data []byte) []byte {
	if typ == raft.EntryNormal {
		buf := make([]byte, 8+len(data))
		binary.BigEndian.PutUint64(buf[0:8], uint64(term))
		copy(buf[8:], data)
		return buf
	}
	buf := make([]byte, 8+1+1+len(data))
	binary.BigEndian.PutUint64(buf[0:8], uint64(term))
	buf[8] = entryPayloadTypeSentinel
	buf[9] = byte(typ)
	copy(buf[10:], data)
	return buf
}

// decodeEntryPayload is unambiguous in both directions and needs no
// external context (dynamic-membership plan §6.1a): a payload whose
// 9th byte is entryPayloadTypeSentinel is the typed form; every other
// payload — including every payload ever written by v0.1.0-v0.4.0 — is
// EntryNormal, byte-identical to what those releases already produced.
func decodeEntryPayload(b []byte) (raft.Term, raft.EntryType, []byte, error) {
	if len(b) < 8 {
		return 0, raft.EntryNormal, nil, fmt.Errorf("%w: too short (%d bytes)", ErrMalformedEntryPayload, len(b))
	}
	term := raft.Term(binary.BigEndian.Uint64(b[0:8]))
	if len(b) >= 10 && b[8] == entryPayloadTypeSentinel {
		typ := raft.EntryType(b[9])
		if typ == raft.EntryNormal || typ > knownEntryTypesMax {
			return 0, raft.EntryNormal, nil, fmt.Errorf("%w: %d", ErrUnknownEntryType, b[9])
		}
		data := append([]byte(nil), b[10:]...)
		return term, typ, data, nil
	}
	data := append([]byte(nil), b[8:]...)
	return term, raft.EntryNormal, data, nil
}

// encodeHardState/decodeHardState serialize raft.HardState (currentTerm,
// votedFor) into the opaque bytes internal/wal's RecordTypeHardState
// record carries.
func encodeHardState(hs raft.HardState) []byte {
	idBytes := []byte(hs.VotedFor)
	buf := make([]byte, 8+4+len(idBytes))
	binary.BigEndian.PutUint64(buf[0:8], uint64(hs.CurrentTerm))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(idBytes)))
	copy(buf[12:], idBytes)
	return buf
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) < 12 {
		return raft.HardState{}, fmt.Errorf("node: HardState payload too short (%d bytes)", len(b))
	}
	term := binary.BigEndian.Uint64(b[0:8])
	idLen := binary.BigEndian.Uint32(b[8:12])
	if int64(idLen) > int64(len(b)-12) {
		return raft.HardState{}, fmt.Errorf("node: HardState payload truncated VotedFor (declared %d, have %d)", idLen, len(b)-12)
	}
	votedFor := raft.NodeID(b[12 : 12+int(idLen)])
	return raft.HardState{CurrentTerm: raft.Term(term), VotedFor: votedFor}, nil
}
