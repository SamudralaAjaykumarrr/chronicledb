// Package storage implements ChronicleDB's durable append-only segment
// file primitives, as specified in docs/storage.md. It owns segment
// files: creating them, naming them, appending raw byte ranges to them,
// fsyncing them, reading byte ranges back, and enumerating/deleting whole
// segment files.
//
// This package has no opinion about what the appended bytes mean: record
// framing, checksums, and logical ordering are owned by internal/wal
// (see docs/storage.md §2).
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	segmentFileSuffix = ".seg"
	segmentIDWidth    = 16
)

// ErrShortRead is returned by Segment.ReadAt when fewer bytes remain in the
// segment than were requested. Callers use this to distinguish a torn/
// incomplete read from a genuine I/O error.
var ErrShortRead = errors.New("storage: short read (insufficient bytes remain in segment)")

// SegmentFileName returns the on-disk file name for the segment with the
// given id. The name encodes id as a fixed-width, zero-padded decimal
// number so segments sort lexically in the same order as numerically,
// and so startup can locate the correct segment for a logical index
// without reading every segment's contents (docs/storage.md §3).
func SegmentFileName(id uint64) string {
	return fmt.Sprintf("%0*d%s", segmentIDWidth, id, segmentFileSuffix)
}

// SegmentPath returns the full path to the segment file with the given id
// inside dir.
func SegmentPath(dir string, id uint64) string {
	return filepath.Join(dir, SegmentFileName(id))
}

// EnsureDir creates dir (and any missing parents) if it does not already
// exist, then fsyncs the parent directory so the new directory entry
// itself is durable.
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("storage: create dir %s: %w", dir, err)
	}
	return syncDir(filepath.Dir(dir))
}

// syncDir fsyncs the directory at path so that directory-entry changes
// (segment file creation/deletion) inside it are themselves durable.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("storage: open dir %s for sync: %w", path, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: sync dir %s: %w", path, err)
	}
	return nil
}

// ListSegmentIDs returns the ids of all segment files found directly in
// dir, sorted ascending. Files that do not match the segment naming
// convention are ignored.
func ListSegmentIDs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("storage: list dir %s: %w", dir, err)
	}
	var ids []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != segmentFileSuffix {
			continue
		}
		idStr := name[:len(name)-len(segmentFileSuffix)]
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// CreateSegment creates a brand-new, empty segment file with the given id.
// It fails if a segment with that id already exists. The containing
// directory is fsynced after creation so the new directory entry is
// itself durable before CreateSegment returns (docs/storage.md §3).
func CreateSegment(dir string, id uint64) (*Segment, error) {
	path := SegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: create segment %s: %w", path, err)
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{id: id, path: path, file: f, size: 0}, nil
}

// OpenSegment opens an existing segment file for reading and appending.
func OpenSegment(dir string, id uint64) (*Segment, error) {
	path := SegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: stat segment %s: %w", path, err)
	}
	return &Segment{id: id, path: path, file: f, size: info.Size()}, nil
}

// RemoveSegment deletes the segment file with the given id and fsyncs the
// containing directory, used by future log-compaction callers.
func RemoveSegment(dir string, id uint64) error {
	path := SegmentPath(dir, id)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("storage: remove segment %s: %w", path, err)
	}
	return syncDir(dir)
}

// Segment represents one durable, append-only segment file. A Segment is
// not safe for concurrent use by multiple goroutines; callers (internal/wal)
// are responsible for serializing access.
type Segment struct {
	id   uint64
	path string
	file *os.File
	size int64
}

// ID returns the segment's id.
func (s *Segment) ID() uint64 { return s.id }

// Size returns the segment's current logical size in bytes.
func (s *Segment) Size() int64 { return s.size }

// Path returns the segment's file path.
func (s *Segment) Path() string { return s.path }

// Append writes p to the end of the segment file, returning the offset at
// which it was written. This makes the bytes appended (visible to the OS
// page cache) but not persisted: only a subsequent successful Sync
// guarantees the bytes survive a crash (docs/architecture.md §4).
func (s *Segment) Append(p []byte) (offset int64, err error) {
	offset = s.size
	n, err := s.file.WriteAt(p, offset)
	s.size += int64(n)
	if err != nil {
		return offset, fmt.Errorf("storage: append to segment %s: %w", s.path, err)
	}
	if n != len(p) {
		return offset, fmt.Errorf("storage: short write to segment %s: wrote %d of %d bytes: %w", s.path, n, len(p), io.ErrShortWrite)
	}
	return offset, nil
}

// Sync issues fsync (or platform equivalent) on the segment file. Only
// after Sync returns successfully are the bytes appended since the
// previous successful Sync persisted (docs/storage.md §5).
func (s *Segment) Sync() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("storage: sync segment %s: %w", s.path, err)
	}
	return nil
}

// ReadAt reads up to len(p) bytes starting at offset, never reading beyond
// the segment's current logical size. If fewer bytes remain than len(p),
// ReadAt reads whatever is available, returns that count, and reports
// ErrShortRead so callers can distinguish a short/torn read from both a
// genuine I/O error and a fully successful read. It never trusts a
// requested length beyond the segment's known size (docs/storage.md §7).
func (s *Segment) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || offset > s.size {
		return 0, fmt.Errorf("storage: read offset %d out of range [0,%d]: %w", offset, s.size, ErrShortRead)
	}
	avail := s.size - offset
	want := int64(len(p))
	toRead := want
	if avail < toRead {
		toRead = avail
	}
	var n int
	var err error
	if toRead > 0 {
		n, err = s.file.ReadAt(p[:toRead], offset)
		if err != nil && err != io.EOF {
			return n, fmt.Errorf("storage: read segment %s at %d: %w", s.path, offset, err)
		}
	}
	if int64(n) < want {
		return n, ErrShortRead
	}
	return n, nil
}

// Truncate shrinks the segment file to size bytes and fsyncs the result.
// It exists exclusively to repair a torn final record left by a crash
// during an in-progress append (docs/wal.md §6.1).
func (s *Segment) Truncate(size int64) error {
	if err := s.file.Truncate(size); err != nil {
		return fmt.Errorf("storage: truncate segment %s to %d: %w", s.path, size, err)
	}
	s.size = size
	return s.Sync()
}

// Close closes the segment's underlying file handle.
func (s *Segment) Close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("storage: close segment %s: %w", s.path, err)
	}
	return nil
}
