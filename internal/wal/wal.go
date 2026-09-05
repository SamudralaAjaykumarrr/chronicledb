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
		report.SegmentsScanned = 1
		return w, report, nil
	}

	var (
		lastLogIndex uint64
		haveMetadata bool
		meta         Metadata
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
				if rec.Index != lastLogIndex+1 {
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
				// Opaque to Phase 1; frame integrity already verified.
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

	w.metadata = meta
	w.nextLogIndex = lastLogIndex + 1
	report.LastLogIndex = lastLogIndex
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
