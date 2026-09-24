package storage

import (
	"errors"
	"fmt"
	"os"
)

// ErrReadOnlySegment is returned by Append, Sync, and Truncate on a
// *Segment opened via OpenSegmentReadOnly (docs/v0.6.0-plan.md §21.2,
// SCRUB NON-DESTRUCTIVE, §27.10): scrub is the only production caller of
// OpenSegmentReadOnly, so this makes non-destructiveness a property of
// the file descriptor itself, not of reviewer discipline — there is no
// code path by which a *Segment constructed this way can ever mutate the
// file it reads.
var ErrReadOnlySegment = errors.New("storage: segment opened read-only")

// OpenSegmentReadOnly opens the existing segment with the given id in
// dir using os.O_RDONLY — never O_RDWR — so the resulting *Segment's
// Append/Sync/Truncate methods all fail closed with ErrReadOnlySegment
// rather than silently no-op-ing (a silent no-op could hide a genuine
// caller bug; a fixed sentinel error cannot). ReadAt, Size, ID, Path, and
// Close all work identically to a normal OpenSegment result.
func OpenSegmentReadOnly(dir string, id uint64) (*Segment, error) {
	path := SegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("storage: open segment %s read-only: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: stat segment %s: %w", path, err)
	}
	return &Segment{id: id, path: path, file: f, size: info.Size(), readOnly: true}, nil
}
