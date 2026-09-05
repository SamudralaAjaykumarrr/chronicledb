package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSegmentFileNaming(t *testing.T) {
	if got, want := SegmentFileName(1), "0000000000000001.seg"; got != want {
		t.Fatalf("SegmentFileName(1) = %q, want %q", got, want)
	}
	if got, want := SegmentFileName(482), "0000000000000482.seg"; got != want {
		t.Fatalf("SegmentFileName(482) = %q, want %q", got, want)
	}
}

func TestCreateAppendSyncReopen(t *testing.T) {
	dir := t.TempDir()

	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	off, err := seg.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off != 0 {
		t.Fatalf("first append offset = %d, want 0", off)
	}
	off2, err := seg.Append([]byte("world!"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off2 != 5 {
		t.Fatalf("second append offset = %d, want 5", off2)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenSegment(dir, 1)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer reopened.Close()
	if reopened.Size() != 11 {
		t.Fatalf("reopened size = %d, want 11", reopened.Size())
	}
	buf := make([]byte, 11)
	n, err := reopened.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 11 || string(buf) != "helloworld!" {
		t.Fatalf("ReadAt = %q (n=%d), want %q", buf, n, "helloworld!")
	}
}

func TestEnsureDirCreatesAndIsIdempotent(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "wal")

	if err := EnsureDir(dir); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("EnsureDir did not create a directory at %s: %v", dir, err)
	}
	if err := EnsureDir(dir); err != nil {
		t.Fatalf("EnsureDir on existing dir: %v", err)
	}
}

func TestSegmentAccessors(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 7)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()
	if seg.ID() != 7 {
		t.Fatalf("ID() = %d, want 7", seg.ID())
	}
	if seg.Path() != SegmentPath(dir, 7) {
		t.Fatalf("Path() = %q, want %q", seg.Path(), SegmentPath(dir, 7))
	}
}

func TestCreateSegmentAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	seg.Close()

	if _, err := CreateSegment(dir, 1); err == nil {
		t.Fatal("CreateSegment on existing id: expected error, got nil")
	}
}

func TestReadAtShortRead(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	if _, err := seg.Append([]byte("abc")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	buf := make([]byte, 10)
	n, err := seg.ReadAt(buf, 0)
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("ReadAt beyond size: err = %v, want ErrShortRead", err)
	}
	if n != 3 {
		t.Fatalf("ReadAt beyond size: n = %d, want 3", n)
	}
	if !bytes.Equal(buf[:3], []byte("abc")) {
		t.Fatalf("ReadAt beyond size: got %q, want prefix abc", buf[:3])
	}
}

func TestReadAtOffsetOutOfRange(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	buf := make([]byte, 4)
	if _, err := seg.ReadAt(buf, 100); !errors.Is(err, ErrShortRead) {
		t.Fatalf("ReadAt with out-of-range offset: err = %v, want ErrShortRead", err)
	}
	if _, err := seg.ReadAt(buf, -1); err == nil {
		t.Fatal("ReadAt with negative offset: expected error, got nil")
	}
}

func TestTruncate(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	if _, err := seg.Append([]byte("abcdefgh")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := seg.Truncate(4); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if seg.Size() != 4 {
		t.Fatalf("Size after truncate = %d, want 4", seg.Size())
	}

	info, err := os.Stat(filepath.Join(dir, SegmentFileName(1)))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 4 {
		t.Fatalf("on-disk size after truncate = %d, want 4", info.Size())
	}
}

func TestListSegmentIDs(t *testing.T) {
	dir := t.TempDir()
	for _, id := range []uint64{1, 5, 482} {
		seg, err := CreateSegment(dir, id)
		if err != nil {
			t.Fatalf("CreateSegment(%d): %v", id, err)
		}
		seg.Close()
	}
	// Non-segment file must be ignored.
	if err := os.WriteFile(filepath.Join(dir, "not-a-segment.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	ids, err := ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	want := []uint64{1, 5, 482}
	if len(ids) != len(want) {
		t.Fatalf("ListSegmentIDs = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ListSegmentIDs = %v, want %v", ids, want)
		}
	}
}

func TestRemoveSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	seg.Close()

	if err := RemoveSegment(dir, 1); err != nil {
		t.Fatalf("RemoveSegment: %v", err)
	}
	ids, err := ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ListSegmentIDs after remove = %v, want empty", ids)
	}
}

// TestConcurrentSegmentsRaceSafe exercises multiple independent Segment
// handles (as internal/wal would use across different segment files)
// concurrently to prove no shared mutable package-level state races.
func TestConcurrentSegmentsRaceSafe(t *testing.T) {
	dir := t.TempDir()
	var wg sync.WaitGroup
	for i := uint64(1); i <= 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			seg, err := CreateSegment(dir, i)
			if err != nil {
				t.Errorf("CreateSegment(%d): %v", i, err)
				return
			}
			defer seg.Close()
			if _, err := seg.Append([]byte("payload")); err != nil {
				t.Errorf("Append: %v", err)
			}
			if err := seg.Sync(); err != nil {
				t.Errorf("Sync: %v", err)
			}
		}()
	}
	wg.Wait()
}
