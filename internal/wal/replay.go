package wal

import (
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// Iterator streams RecordTypeLogEntry records from a WAL's durable
// segments in append order, starting at a given logical index. It
// re-reads segments directly from disk (independent of any single Open
// call's cached state), as required for both startup recovery and future
// follower catch-up use (docs/wal.md §5).
//
// Iterator verifies every frame's checksum as it reads. It never invents,
// skips, or reorders records: it returns a record, signals a well-defined
// end of the readable log, or fails closed on unclassified corruption
// (docs/wal.md §5). A torn/incomplete trailing record is treated as a
// clean end of the readable stream, not an error — Iterator only ever
// reads records that Open has already validated or that were durably
// completed after Open, so a torn tail encountered here reflects an
// in-progress append racing the read, not damage to committed history.
type Iterator struct {
	dir       string
	ids       []uint64
	idx       int
	fromIndex uint64

	seg    *storage.Segment
	offset int64
}

// Replay returns an Iterator over RecordTypeLogEntry records with index
// >= fromIndex. It is safe to call concurrently with Append/Sync on the
// same WAL, but the resulting Iterator itself is not safe for concurrent
// use from multiple goroutines.
func (w *WAL) Replay(fromIndex uint64) (*Iterator, error) {
	w.mu.Lock()
	closed := w.closed
	dir := w.dir
	w.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}

	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		return nil, err
	}
	return &Iterator{dir: dir, ids: ids, fromIndex: fromIndex}, nil
}

// Next returns the next matching record. ok is false with a nil error at
// a clean end of the log; err is non-nil only for unclassified corruption
// (docs/wal.md §6.2), never for a torn trailing record.
func (it *Iterator) Next() (rec Record, ok bool, err error) {
	for {
		if it.seg == nil {
			if it.idx >= len(it.ids) {
				return Record{}, false, nil
			}
			id := it.ids[it.idx]
			it.idx++
			seg, oerr := storage.OpenSegment(it.dir, id)
			if oerr != nil {
				return Record{}, false, oerr
			}
			it.seg = seg
			it.offset = 0
		}

		r, frameLen, rerr := readFrame(it.seg, it.offset)
		if rerr == errTornTail || (rerr == nil && r == nil) {
			// Clean end of this segment (or an in-progress tail): move on.
			it.seg.Close()
			it.seg = nil
			continue
		}
		if rerr != nil {
			it.seg.Close()
			it.seg = nil
			return Record{}, false, rerr
		}

		it.offset += int64(frameLen)
		if r.Type != RecordTypeLogEntry || r.Index < it.fromIndex {
			continue
		}
		return *r, true, nil
	}
}

// Close releases the Iterator's open segment handle, if any.
func (it *Iterator) Close() error {
	if it.seg != nil {
		err := it.seg.Close()
		it.seg = nil
		return err
	}
	return nil
}
