// Package wal implements ChronicleDB's durable, replayable ordered log
// (docs/wal.md), built on top of internal/storage's segment file
// primitives. It owns record framing, append/sync durability semantics,
// replay, and — most importantly — the exact rules for what happens when
// the durable log is found damaged at startup (docs/wal.md §6,
// docs/recovery.md).
package wal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// DefaultSegmentMaxSize is the segment-rotation size threshold used when
// Options.SegmentMaxSize is unset. Segment growth/rotation is an
// implementation detail explicitly deferred to Phase 1 by docs/storage.md
// §8; this is the smallest justified choice: large enough that ordinary
// use rotates rarely, small enough that tests can exercise rotation
// without writing gigabytes of data.
const DefaultSegmentMaxSize = 16 * 1024 * 1024 // 16 MiB

// Options configures Open.
type Options struct {
	// SegmentMaxSize is the approximate size, in bytes, at which the
	// current segment is rotated to a new one. A record is never split
	// across segments, so a single record larger than SegmentMaxSize is
	// still accepted into whatever segment is current at the time.
	SegmentMaxSize int64
}

// RecoveryReport summarizes what Open's recovery sequence observed and
// repaired, for tests and future observability to inspect. It is not used
// for any correctness decision by callers — only for evidence.
type RecoveryReport struct {
	SegmentsScanned   int
	LastLogIndex      uint64
	TornTailTruncated bool
	// TruncatedBytes is the number of bytes removed from the final
	// segment to repair a torn tail, or 0 if none was found.
	TruncatedBytes int64
	// FirstLogIndex is the oldest LogEntry index this WAL can still
	// serve (Metadata.LatestSnapshotIndex + 1) — 1 for a node that has
	// never compacted its log (docs/snapshots.md §8, docs/wal.md §7).
	FirstLogIndex uint64
}

// WAL is ChronicleDB's durable ordered log. A WAL serializes all Append,
// Sync, and Close calls internally (via an embedded mutex) so it is safe
// for concurrent use by multiple goroutines; Phase 1 makes no attempt at
// a group-commit optimization (docs/wal.md §4) — every Sync call issues
// its own fsync.
type WAL struct {
	mu sync.Mutex

	dir            string
	segmentMaxSize int64

	current  *storage.Segment
	metadata Metadata

	// nextLogIndex is the index that will be assigned to the next
	// RecordTypeLogEntry record appended.
	nextLogIndex uint64

	// firstLogIndex is the oldest LogEntry index this WAL can still
	// serve: metadata.LatestSnapshotIndex + 1 (docs/snapshots.md §8). A
	// fresh WAL with no snapshot yet has firstLogIndex == 1. Log entries
	// that may still be physically present at or before this boundary
	// (a compaction that ran partway, or hasn't run yet since the
	// pointer was last updated — see CompactBefore's doc comment) are
	// logically superseded and are skipped by Replay/Entries callers
	// exactly as if they were already physically gone.
	firstLogIndex uint64

	// latestHardState is the payload of the most recently appended (or,
	// at Open, most recently replayed) RecordTypeHardState record, or
	// nil if none has ever been written. internal/wal treats it as
	// opaque bytes (docs/architecture.md §5: internal/wal must not know
	// about Raft semantics) — encoding/decoding currentTerm/votedFor is
	// the Raft storage adapter's job (internal/node, Phase 5).
	latestHardState []byte

	closed bool
}

// Open performs the Phase-1 subset of the recovery sequence in
// docs/recovery.md §1 (steps 1-8; the Raft/snapshot-specific steps 2-4 and
// 9-14 do not apply until later phases) and returns a WAL ready to accept
// new appends, plus a report describing what recovery found.
//
// On a corrupted durable log (a fully framed record with a bad checksum,
// an out-of-order log index, an unsupported format version, or a torn
// tail anywhere but the final segment) Open returns a non-nil error and
// no usable WAL: the node must refuse to start, per
// docs/adr/0012-recovery-and-corruption-policy.md.
func Open(dir string, opts Options) (*WAL, *RecoveryReport, error) {
	segmentMaxSize := opts.SegmentMaxSize
	if segmentMaxSize <= 0 {
		segmentMaxSize = DefaultSegmentMaxSize
	}
	if err := storage.EnsureDir(dir); err != nil {
		return nil, nil, err
	}

	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		return nil, nil, err
	}

	w := &WAL{dir: dir, segmentMaxSize: segmentMaxSize, nextLogIndex: 1}
	report := &RecoveryReport{}

	if len(ids) == 0 {
		seg, err := storage.CreateSegment(dir, 1)
		if err != nil {
			return nil, nil, err
		}
		w.current = seg
		meta := Metadata{NodeID: newNodeID(), FormatVersion: FormatVersion}
		if err := w.appendLocked(RecordTypeMetadata, encodeMetadata(meta)); err != nil {
			seg.Close()
			return nil, nil, err
		}
		if err := w.current.Sync(); err != nil {
			seg.Close()
			return nil, nil, err
		}
		w.metadata = meta
		w.firstLogIndex = 1
		report.SegmentsScanned = 1
		report.FirstLogIndex = 1
		return w, report, nil
	}

	var (
		lastLogIndex      uint64
		haveMetadata      bool
		meta              Metadata
		latestHS          []byte
		seenAnyLogEntry   bool
		firstLogIndexSeen uint64
	)

	for i, id := range ids {
		isLastSegment := i == len(ids)-1
		seg, err := storage.OpenSegment(dir, id)
		if err != nil {
			return nil, nil, err
		}
		report.SegmentsScanned++

		var offset int64
		for {
			rec, frameLen, rerr := readFrame(seg, offset)
			if rerr == errTornTail {
				if !isLastSegment {
					seg.Close()
					return nil, nil, fmt.Errorf("wal: torn record in non-final segment %d: %w", id, ErrCorrupt)
				}
				report.TornTailTruncated = true
				report.TruncatedBytes = seg.Size() - offset
				if err := seg.Truncate(offset); err != nil {
					seg.Close()
					return nil, nil, err
				}
				break
			}
			if rerr != nil {
				seg.Close()
				return nil, nil, rerr
			}
			if rec == nil {
				break // clean end of this segment
			}

			switch rec.Type {
			case RecordTypeLogEntry:
				if !seenAnyLogEntry {
					// The very first LogEntry record physically
					// encountered may legitimately start at any index
					// (a not-yet-compacted or partially-compacted log
					// still holds superseded low-index entries; see
					// CompactBefore) — its absolute starting value is
					// validated once, after this scan, against the
					// winning Metadata record's LatestSnapshotIndex
					// (which may not be known yet at this point in the
					// scan). Every record after this one still must be
					// perfectly contiguous, exactly as before: physical
					// append order never has gaps regardless of
					// compaction, only a possibly-nonzero starting point.
					seenAnyLogEntry = true
					firstLogIndexSeen = rec.Index
				} else if rec.Index != lastLogIndex+1 {
					seg.Close()
					return nil, nil, fmt.Errorf("wal: out-of-order log index: got %d, want %d: %w", rec.Index, lastLogIndex+1, ErrCorrupt)
				}
				lastLogIndex = rec.Index
			case RecordTypeMetadata:
				m, derr := decodeMetadata(rec.Payload)
				if derr != nil {
					seg.Close()
					return nil, nil, fmt.Errorf("wal: %v: %w", derr, ErrCorrupt)
				}
				meta = m
				haveMetadata = true
			case RecordTypeHardState:
				// Opaque to internal/wal; frame integrity already
				// verified. "Most recent record of this type wins"
				// (docs/wal.md §2, §9) — a forward, in-order scan means
				// the last one encountered during recovery is correct.
				latestHS = append([]byte(nil), rec.Payload...)
			}
			offset += int64(frameLen)
		}

		if isLastSegment {
			w.current = seg
		} else {
			if err := seg.Close(); err != nil {
				return nil, nil, err
			}
		}
	}

	if !haveMetadata {
		w.current.Close()
		return nil, nil, fmt.Errorf("wal: %w", ErrNoMetadata)
	}
	if meta.FormatVersion != FormatVersion {
		w.current.Close()
		return nil, nil, fmt.Errorf("wal: %w: metadata format version %d, expected %d", ErrUnsupportedVersion, meta.FormatVersion, FormatVersion)
	}

	// The winning Metadata record (last one seen, per docs/wal.md §9) is
	// only fully known now that the whole scan has completed — validate
	// the first physically-encountered LogEntry's absolute index against
	// it (see the scan loop's comment above): it must never start
	// *strictly after* the snapshot boundary (that would mean genuine
	// missing history the snapshot does not cover — corruption), though
	// it may legitimately start at or before it (superseded, not-yet- or
	// only-partially-physically-compacted leftover entries, safe to
	// ignore — Replay/Entries already filter these by index).
	if seenAnyLogEntry && firstLogIndexSeen > meta.LatestSnapshotIndex+1 {
		w.current.Close()
		return nil, nil, fmt.Errorf("wal: durable log starts at index %d but snapshot pointer only covers up to %d (gap): %w", firstLogIndexSeen, meta.LatestSnapshotIndex, ErrCorrupt)
	}
	if !seenAnyLogEntry {
		// No LogEntry record survives at all — either a genuinely fresh
		// log (LatestSnapshotIndex == 0) or one fully compacted up to and
		// including its newest entry with nothing appended since; either
		// way, "the last known index" is exactly the snapshot boundary.
		lastLogIndex = meta.LatestSnapshotIndex
	}

	w.metadata = meta
	w.nextLogIndex = lastLogIndex + 1
	w.firstLogIndex = meta.LatestSnapshotIndex + 1
	w.latestHardState = latestHS
	report.LastLogIndex = lastLogIndex
	report.FirstLogIndex = w.firstLogIndex
	return w, report, nil
}

// Metadata returns the WAL's current node-local metadata.
func (w *WAL) Metadata() Metadata {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.metadata
}

// NextIndex returns the logical index that will be assigned to the next
// AppendLogEntry call.
func (w *WAL) NextIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextLogIndex
}

// FirstIndex returns the oldest LogEntry index this WAL can still serve
// (docs/snapshots.md §8): 1 for a node that has never compacted its
// log, or metadata.LatestSnapshotIndex+1 once it has. Replay/Entries
// callers never need to consult this directly — they already only ever
// see indices at or after it — but recovery (internal/node.Open) uses
// it to detect the one case that must refuse startup rather than
// silently starting from an incomplete history: FirstIndex() > 1 with
// no valid snapshot available to cover the gap (docs/recovery.md §4).
func (w *WAL) FirstIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstLogIndex
}

// AppendLogEntry appends payload as a new RecordTypeLogEntry record and
// returns the logical index assigned to it. This makes the record
// appended, not persisted (docs/architecture.md §4): callers that require
// a durability guarantee must call Sync and wait for it to return
// successfully before treating the write as durable/acknowledgeable
// (docs/wal.md §4).
func (w *WAL) AppendLogEntry(payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	idx := w.nextLogIndex
	if err := w.appendLocked(RecordTypeLogEntry, payload); err != nil {
		return 0, err
	}
	w.nextLogIndex++
	return idx, nil
}

// AppendHardState appends payload as a new RecordTypeHardState record.
// Like AppendLogEntry, this makes it appended, not persisted: a caller
// requiring the durability guarantee ADR-0008 requires before a vote or
// an AppendEntries acknowledgement may be released (docs/raft.md §5)
// must call Sync and wait for it to return successfully first. payload
// is opaque to internal/wal (docs/architecture.md §5) — encoding
// currentTerm/votedFor into it is the Raft storage adapter's job.
func (w *WAL) AppendHardState(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if err := w.appendLocked(RecordTypeHardState, payload); err != nil {
		return err
	}
	w.latestHardState = append([]byte(nil), payload...)
	return nil
}

// LatestHardState returns a copy of the payload of the most recently
// appended RecordTypeHardState record (whether appended earlier this
// process's lifetime or recovered from disk at Open), or nil if none
// has ever been written to this durable log.
func (w *WAL) LatestHardState() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.latestHardState...)
}

// Truncate durably discards every RecordTypeLogEntry record with
// index >= fromIndex, so a subsequent AppendLogEntry resumes exactly at
// fromIndex (docs/raft.md §3 "divergent suffix repair"; ADR-0008;
// Phase 5's raft.Storage.Truncate contract). It is a no-op if
// fromIndex >= the index that would be assigned to the next
// AppendLogEntry call (i.e. there is nothing at or after fromIndex to
// remove).
//
// Crash safety: Truncate first deletes every segment strictly newer
// than the one holding fromIndex, highest id first (each deletion is
// its own fsync'd directory operation), and only as its last step
// shrinks (and fsyncs) the segment that actually holds fromIndex. This
// ordering is deliberate and load-bearing — see the package-level
// truncation note below — because it guarantees that after every
// individual step, and therefore after a crash at any point during the
// call, the surviving on-disk segments still form a complete, gap-free,
// checksum-valid prefix of some legitimate prior state of the log
// (either the pre-Truncate log, the fully-truncated log, or a state in
// between where only some of the now-superseded tail has been removed
// so far). Recovery (Open) never sees a gap, and the operation is
// safely retryable: if Truncate is interrupted, Raft's own divergent-
// suffix-repair protocol re-issues an equivalent Truncate the next time
// this node exchanges AppendEntriesRPCs with a legitimate leader, so no
// operator action is required. Truncate never removes a segment at or
// before the one holding fromIndex, so it can never discard committed
// history: docs/raft.md's Storage.Truncate contract requires callers to
// never ask for a truncation at or below any index already reported via
// Output.CommittedEntries in the first place.
//
// Truncating the segment that holds fromIndex in place (rather than
// deleting it outright, even when fromIndex is its very first record)
// deliberately keeps any HardState/Metadata records physically
// preceding fromIndex's frame in that same segment intact.
func (w *WAL) Truncate(fromIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if fromIndex < 1 {
		return fmt.Errorf("wal: Truncate: fromIndex must be >= 1, got %d", fromIndex)
	}
	if fromIndex >= w.nextLogIndex {
		// Nothing physically present at or after fromIndex to remove,
		// but this method's own contract ("a subsequent AppendLogEntry
		// resumes exactly at fromIndex") still must hold — in
		// particular for internal/node.WALStorage.InstallSnapshot's use
		// of Truncate to jump this WAL's own next-index counter forward
		// past a gap it never physically held any entries for (the
		// ordinary case for a follower catching up via snapshot: its
		// log was behind, not merely diverged). Recovery (Open, above)
		// already derives exactly this value
		// (meta.LatestSnapshotIndex+1) from the durably-recorded
		// snapshot pointer when no LogEntry record survives, so this is
		// purely bringing this live, not-yet-restarted WAL's in-memory
		// counter into agreement with what a restart would already
		// compute — no additional durable write is required here.
		if fromIndex > w.nextLogIndex {
			w.nextLogIndex = fromIndex
		}
		return nil
	}

	ids, err := storage.ListSegmentIDs(w.dir)
	if err != nil {
		return err
	}

	targetID, targetOffset, found, err := locateLogIndex(w.dir, ids, fromIndex)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("wal: Truncate: log index %d (< nextLogIndex %d) not found in durable segments: %w", fromIndex, w.nextLogIndex, ErrCorrupt)
	}

	// Delete every segment strictly newer than targetID, highest id
	// first, so the remaining segment set is always a valid, gap-free
	// prefix at every intermediate step.
	currentID := w.current.ID()
	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		if id <= targetID {
			break
		}
		if id == currentID {
			if err := w.current.Close(); err != nil {
				return err
			}
		}
		if err := storage.RemoveSegment(w.dir, id); err != nil {
			return err
		}
	}

	var target *storage.Segment
	if targetID == currentID {
		target = w.current
	} else {
		seg, err := storage.OpenSegment(w.dir, targetID)
		if err != nil {
			return err
		}
		target = seg
	}
	if err := target.Truncate(targetOffset); err != nil {
		return err
	}
	w.current = target
	w.nextLogIndex = fromIndex
	return nil
}

// AppendMetadataSnapshot durably records that a valid, fully-persisted
// state-machine snapshot now covers history up to and including
// uptoIndex (docs/wal.md §8, docs/snapshots.md §3's restart-discovery
// pointer): it appends a fresh Metadata record (last-one-wins,
// docs/wal.md §9) — with the same NodeID/FormatVersion, an updated
// LatestSnapshotIndex — to the WAL's *current* segment, and syncs it.
//
// This must be called only after the snapshot file itself is already
// fully durable (fsynced and atomically renamed to its final name), and
// must be called before CompactBefore(uptoIndex) — ADR-0011's "snapshot
// durable before truncation" ordering requires the pointer recovery
// will trust to itself be durable before any segment it depends on is
// ever deleted. Writing this record to the *current* (never-deleted-by-
// compaction) segment is also what guarantees a compaction immediately
// following this call can never delete the only copy of it — see
// CompactBefore's doc comment for why this same reasoning additionally
// requires re-affirming HardState (internal/node.WALStorage.Reaffirm)
// before compacting.
//
// It also advances firstLogIndex/FirstIndex() to uptoIndex+1 in this
// live process, not only on a future restart's re-derivation from
// Metadata (Open's own w.firstLogIndex assignment) — a live caller that
// needs "the oldest index this WAL still logically serves" (e.g.
// internal/node.WALStorage discarding its own superseded log entries
// immediately after installing a peer's snapshot, without an
// intervening restart) must see this boundary move the moment it is
// durably adopted, exactly as a restart would compute it.
func (w *WAL) AppendMetadataSnapshot(uptoIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if uptoIndex < w.metadata.LatestSnapshotIndex {
		return fmt.Errorf("wal: AppendMetadataSnapshot: %d is behind current snapshot pointer %d", uptoIndex, w.metadata.LatestSnapshotIndex)
	}
	meta := w.metadata
	meta.LatestSnapshotIndex = uptoIndex
	if err := w.appendLocked(RecordTypeMetadata, encodeMetadata(meta)); err != nil {
		return err
	}
	if err := w.current.Sync(); err != nil {
		return err
	}
	w.metadata = meta
	w.firstLogIndex = uptoIndex + 1
	return nil
}

// CompactBefore durably deletes every whole WAL segment that holds only
// LogEntry records at or before uptoIndex (docs/wal.md §7,
// docs/snapshots.md §8) — the physical half of log compaction; the
// logical half (recovery/replay no longer trusting indices at or before
// the boundary) is what AppendMetadataSnapshot + the two-pass validation
// in Open already establish, independent of whether this method has run
// yet or how far it got.
//
// Eligibility is determined from segment ids alone (docs/storage.md §3:
// a segment's id is the first logical index it holds — the highest
// index segment ids[i] can possibly contain is therefore ids[i+1]-1),
// never by re-reading a segment's contents. The current (last,
// open-for-writing) segment is never deleted, which is exactly why
// AppendMetadataSnapshot (and internal/node.WALStorage.Reaffirm for
// HardState) must always target the current segment immediately before
// this is called — the invariant "whatever this compaction is about to
// leave behind still contains everything a future Open needs" holds
// only because of that ordering, not because of anything CompactBefore
// itself checks.
//
// CompactBefore is safe to call repeatedly (idempotent: a no-op once
// nothing more is eligible) and safe to interrupt at any point — each
// segment deletion is its own fsync'd directory operation
// (storage.RemoveSegment), so a crash mid-loop simply leaves some, not
// all, eligible segments removed. This is deliberately the same
// "extra history retained, nothing required ever lost" safe default
// docs/snapshots.md's compaction-failure handling calls for: a partially
// completed compaction is never a correctness problem, only a
// (temporary, self-healing on the next successful call) disk-space one.
func (w *WAL) CompactBefore(uptoIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	ids, err := storage.ListSegmentIDs(w.dir)
	if err != nil {
		return err
	}
	currentID := w.current.ID()
	for i, id := range ids {
		if id == currentID {
			break
		}
		var maxIndexInSegment uint64
		if i+1 < len(ids) {
			maxIndexInSegment = ids[i+1] - 1
		} else {
			// Unreachable in practice (the current segment is always
			// last), but treat conservatively as "unknown upper bound,"
			// never delete it, rather than guessing.
			break
		}
		if maxIndexInSegment > uptoIndex {
			break // this segment (and every later one) is still needed
		}
		if err := storage.RemoveSegment(w.dir, id); err != nil {
			return err
		}
	}
	return nil
}

// locateLogIndex scans segments (ascending id order, as returned by
// storage.ListSegmentIDs) for the RecordTypeLogEntry record whose Index
// equals target, returning the id of the segment holding it and the
// byte offset at which that record's frame begins (i.e. the offset that
// segment must be truncated to in order to remove that record and
// everything after it). ok is false if target is not present as a
// fully-framed record in any scanned segment.
func locateLogIndex(dir string, ids []uint64, target uint64) (segID uint64, offset int64, ok bool, err error) {
	for _, id := range ids {
		seg, err := storage.OpenSegment(dir, id)
		if err != nil {
			return 0, 0, false, err
		}
		var off int64
		for {
			rec, frameLen, rerr := readFrame(seg, off)
			if rerr == errTornTail || (rerr == nil && rec == nil) {
				break // clean end of this segment (or an in-progress tail)
			}
			if rerr != nil {
				seg.Close()
				return 0, 0, false, rerr
			}
			if rec.Type == RecordTypeLogEntry && rec.Index == target {
				seg.Close()
				return id, off, true, nil
			}
			off += int64(frameLen)
		}
		seg.Close()
	}
	return 0, 0, false, nil
}

// appendLocked frames and appends one record to the current segment,
// rotating segments first if the configured size threshold is exceeded.
// Callers must hold w.mu.
func (w *WAL) appendLocked(rt RecordType, payload []byte) error {
	if len(payload) > MaxRecordPayloadSize {
		return fmt.Errorf("wal: %w: %d bytes > max %d", ErrRecordTooLarge, len(payload), MaxRecordPayloadSize)
	}
	var index uint64
	if rt == RecordTypeLogEntry {
		index = w.nextLogIndex
	}
	frame := encodeRecord(rt, index, payload)

	if err := w.maybeRotateLocked(int64(len(frame))); err != nil {
		return err
	}
	if _, err := w.current.Append(frame); err != nil {
		return err
	}
	return nil
}

// maybeRotateLocked opens a new current segment if appending a frame of
// nextFrameSize bytes would push the current segment over the configured
// threshold. Rotation itself is a durable operation: the new segment is
// created and fsynced (internal/storage fsyncs the containing directory
// on creation) before the old segment is considered closed for writing
// (docs/storage.md §3). Callers must hold w.mu.
func (w *WAL) maybeRotateLocked(nextFrameSize int64) error {
	if w.current.Size() == 0 || w.current.Size()+nextFrameSize <= w.segmentMaxSize {
		return nil
	}
	newID := w.nextLogIndex
	if newID <= w.current.ID() {
		// No log-index progress has been made since the current segment
		// was created (e.g. only Metadata/HardState records have been
		// written so far): rotating now would collide with the current
		// segment's filename. Let this one frame land in the current
		// segment instead; rotation is a size heuristic, not a hard cap.
		return nil
	}
	newSeg, err := storage.CreateSegment(w.dir, newID)
	if err != nil {
		return err
	}
	if err := newSeg.Sync(); err != nil {
		newSeg.Close()
		return err
	}
	old := w.current
	w.current = newSeg
	return old.Close()
}

// Sync issues fsync on the current segment. A record is persisted only
// once a Sync call covering it has returned successfully
// (docs/wal.md §4).
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.current.Sync()
}

// Close closes the WAL's open segment. After Close, all other WAL methods
// return ErrClosed, including a repeated call to Close itself.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	return w.current.Close()
}

// readFrame reads and decodes exactly one frame starting at offset within
// seg. It returns (nil, 0, nil) at a clean end of the segment (nothing
// more to read), (nil, 0, errTornTail) if the bytes present are
// insufficient to form a complete frame, or (rec, frameLen, nil) on
// success. Any other error is an unconditional corruption failure.
func readFrame(seg *storage.Segment, offset int64) (*Record, int, error) {
	size := seg.Size()
	if offset >= size {
		return nil, 0, nil
	}
	avail := size - offset
	maxRead := int64(headerSize) + int64(MaxRecordPayloadSize) + int64(checksumSize)
	want := avail
	if want > maxRead {
		want = maxRead
	}
	buf := make([]byte, want)
	n, err := seg.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, storage.ErrShortRead) {
		return nil, 0, err
	}
	buf = buf[:n]

	rec, frameLen, derr := decodeFrameBytes(buf)
	if derr != nil {
		return nil, 0, derr
	}
	return rec, frameLen, nil
}
