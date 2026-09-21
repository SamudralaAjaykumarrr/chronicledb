package storage_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/testfs"
)

// TestRealENOSPC_SegmentAppendAndWriteFileDurable is SL-14's storage-layer
// half (docs/v0.6.0-plan.md §19.3, §27.9 DISK-FULL EXPLICITNESS): on a
// real, genuinely-full small filesystem, both of internal/storage's
// write-shaped entry points classify the resulting syscall.ENOSPC as
// storage.ErrOutOfSpace rather than a generic I/O error.
func TestRealENOSPC_SegmentAppendAndWriteFileDurable(t *testing.T) {
	testfs.RunInNamespace(t, func(t *testing.T) {
		dir := t.TempDir()
		cleanup, err := testfs.MountTmpfs(dir, 1<<20) // 1 MiB
		if err != nil {
			t.Skipf("tmpfs unavailable: %v", err)
		}
		defer cleanup()

		segDir := filepath.Join(dir, "seg")
		if err := storage.EnsureDir(segDir); err != nil {
			t.Fatalf("EnsureDir: %v", err)
		}
		seg, err := storage.CreateSegment(segDir, 1)
		if err != nil {
			t.Fatalf("CreateSegment: %v", err)
		}
		defer seg.Close()

		buf := make([]byte, 64*1024)
		var appendErr error
		for i := 0; i < 1024; i++ { // far more than 1MiB/64KiB, guaranteed to exhaust it
			if _, appendErr = seg.Append(buf); appendErr != nil {
				break
			}
		}
		if appendErr == nil {
			t.Fatalf("expected Append to eventually fail with ENOSPC on a 1MiB tmpfs, never did")
		}
		if !errors.Is(appendErr, storage.ErrOutOfSpace) {
			t.Fatalf("Append error did not classify as storage.ErrOutOfSpace: %v", appendErr)
		}

		// WriteFileDurable's own independent write path (the snapshot
		// write path, SL-14's second required case).
		wfdDir := filepath.Join(dir, "wfd")
		if err := storage.EnsureDir(wfdDir); err != nil {
			t.Fatalf("EnsureDir: %v", err)
		}
		bigData := make([]byte, 2<<20) // 2 MiB into an already-near-full 1MiB fs
		wfdErr := storage.WriteFileDurable(wfdDir, filepath.Join(wfdDir, "final"), bigData)
		if wfdErr == nil {
			t.Fatalf("expected WriteFileDurable to fail with ENOSPC, it succeeded")
		}
		if !errors.Is(wfdErr, storage.ErrOutOfSpace) {
			t.Fatalf("WriteFileDurable error did not classify as storage.ErrOutOfSpace: %v", wfdErr)
		}
	})
}
