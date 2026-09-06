package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// SyncDir fsyncs the directory at path, so that directory-entry changes
// (file creation/deletion/rename) inside it are themselves durable. It
// is the exported form of the same primitive internal/storage already
// uses internally for segment creation/deletion (docs/storage.md §3);
// exported so other durable-file owners (internal/snapshot) can reuse
// it instead of re-implementing directory fsync themselves.
func SyncDir(path string) error { return syncDir(path) }

// WriteFileDurable writes data to a fresh temporary file inside tmpDir,
// fsyncs it, atomically renames it to finalPath, and fsyncs finalPath's
// containing directory — the exact crash-safe sequence
// docs/snapshots.md §3 specifies for any durable, atomically-installed
// file (temp file -> fsync -> atomic rename -> directory fsync where
// required). It never leaves a partially written file visible at
// finalPath: a crash at any point either leaves finalPath absent/
// unchanged (temp file merely orphaned) or fully, correctly written.
//
// tmpDir and the directory containing finalPath are assumed to already
// exist (callers create them once via EnsureDir).
func WriteFileDurable(tmpDir, finalPath string, data []byte) error {
	tmp, err := os.CreateTemp(tmpDir, "tmp-*")
	if err != nil {
		return fmt.Errorf("storage: create temp file in %s: %w", tmpDir, err)
	}
	tmpPath := tmp.Name()
	// On any early return, remove the orphaned temp file — a best-effort
	// cleanup, not a correctness requirement (docs/snapshots.md §4: an
	// orphaned temp file left behind by a crash is simply ignored by a
	// future recovery scan, never trusted).
	succeeded := false
	defer func() {
		if !succeeded {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("storage: write temp file %s: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("storage: sync temp file %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: close temp file %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("storage: rename %s to %s: %w", tmpPath, finalPath, err)
	}
	succeeded = true
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return fmt.Errorf("storage: sync directory for %s: %w", finalPath, err)
	}
	return nil
}
