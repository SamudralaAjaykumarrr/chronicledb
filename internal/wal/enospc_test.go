package wal_test

import (
	"errors"
	"os"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/testfs"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// TestRealENOSPC_WALAppendLogEntry is SL-14's Raft-path half
// (docs/v0.6.0-plan.md §19.3, §27.9 DISK-FULL EXPLICITNESS): a real
// AppendLogEntry/Sync failure on a genuinely full small filesystem
// reaches the caller as wal.ErrOutOfSpace, not a generic I/O error —
// this is exactly the error internal/node.WALStorage.Append wraps with
// %w and hands to raft.ApplyPersistRequest on the durable Raft path.
func TestRealENOSPC_WALAppendLogEntry(t *testing.T) {
	testfs.RunInNamespace(t, func(t *testing.T) {
		dir := t.TempDir()
		cleanup, err := testfs.MountTmpfs(dir, 1<<20) // 1 MiB
		if err != nil {
			t.Skipf("tmpfs unavailable: %v", err)
		}
		defer cleanup()

		w, _, err := wal.Open(dir, wal.Options{})
		if err != nil {
			t.Fatalf("wal.Open: %v", err)
		}
		defer w.Close()

		payload := make([]byte, 64*1024)
		var appendErr error
		for i := 0; i < 1024; i++ {
			if _, appendErr = w.AppendLogEntry(payload); appendErr != nil {
				break
			}
			if appendErr = w.Sync(); appendErr != nil {
				break
			}
		}
		if appendErr == nil {
			t.Fatalf("expected AppendLogEntry/Sync to eventually fail with ENOSPC on a 1MiB tmpfs, never did")
		}
		if !errors.Is(appendErr, wal.ErrOutOfSpace) {
			t.Fatalf("error did not classify as wal.ErrOutOfSpace: %v", appendErr)
		}
		if !errors.Is(appendErr, storage.ErrOutOfSpace) {
			t.Fatalf("wal.ErrOutOfSpace lost the underlying storage.ErrOutOfSpace in its chain: %v", appendErr)
		}
	})
}

// TestRealENOSPC_RecoveryAfterSpaceFreed is SL-15 (docs/v0.6.0-plan.md
// §19.3 item 2): once ENOSPC has been hit, freeing space (here: removing
// an unrelated large file from the same small filesystem) lets the very
// next write succeed with no restart and no special recovery action —
// the WAL/storage layers hold no "stuck forever" latch of their own.
func TestRealENOSPC_RecoveryAfterSpaceFreed(t *testing.T) {
	testfs.RunInNamespace(t, func(t *testing.T) {
		dir := t.TempDir()
		cleanup, err := testfs.MountTmpfs(dir, 2<<20) // 2 MiB
		if err != nil {
			t.Skipf("tmpfs unavailable: %v", err)
		}
		defer cleanup()

		w, _, err := wal.Open(dir, wal.Options{})
		if err != nil {
			t.Fatalf("wal.Open: %v", err)
		}
		defer w.Close()

		// Consume the rest of the filesystem with an unrelated file
		// (never touched by internal/wal), leaving just enough that the
		// WAL's own next append hits ENOSPC.
		fillerPath := dir + "/ballast.bin"
		if err := os.WriteFile(fillerPath, make([]byte, (2<<20)-(64*1024)), 0o644); err != nil {
			t.Fatalf("writing ballast filler: %v", err)
		}

		payload := make([]byte, 128*1024)
		var appendErr error
		for i := 0; i < 64; i++ {
			if _, appendErr = w.AppendLogEntry(payload); appendErr != nil {
				break
			}
			if appendErr = w.Sync(); appendErr != nil {
				break
			}
		}
		if !errors.Is(appendErr, wal.ErrOutOfSpace) {
			t.Fatalf("expected wal.ErrOutOfSpace before space was freed, got: %v", appendErr)
		}

		if err := os.Remove(fillerPath); err != nil {
			t.Fatalf("removing ballast filler: %v", err)
		}

		if _, err := w.AppendLogEntry(payload); err != nil {
			t.Fatalf("AppendLogEntry after freeing space: expected success, got %v", err)
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("Sync after freeing space: expected success, got %v", err)
		}
	})
}
