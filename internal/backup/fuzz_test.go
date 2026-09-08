package backup_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// FuzzRestoreManifest feeds arbitrary bytes as a backup's manifest.json
// and proves Restore never panics, regardless of how malformed the
// manifest is — mirroring internal/snapshot's FuzzDecode and
// internal/wal's FuzzDecodeFrameBytes discipline
// (docs/failure-model.md §6), applied here to the new backup manifest
// decode path (docs/enterprise-v1-plan.md §6 "Deterministic tests":
// "Manifest encode/decode round-trip and fuzz test"). A valid backup's
// own manifest bytes seed the corpus so the fuzzer starts from
// something realistic.
func FuzzRestoreManifest(f *testing.F) {
	dir := f.TempDir()
	srcDir := filepath.Join(dir, "src")
	backupDir := filepath.Join(dir, "backup")

	w, _, err := wal.Open(srcDir, wal.Options{})
	if err != nil {
		f.Fatalf("opening source WAL: %v", err)
	}
	cmd := fsm.CommitTxnCommand{RequestID: "r1", TxnID: 1, Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("v1")}}}
	if _, err := w.AppendLogEntry(fsm.EncodeCommitTxn(cmd)); err != nil {
		f.Fatalf("appending: %v", err)
	}
	if err := w.Sync(); err != nil {
		f.Fatalf("sync: %v", err)
	}
	fs := fsm.New(mvcc.NewStore())
	if _, err := fs.Apply(1, cmd); err != nil {
		f.Fatalf("apply: %v", err)
	}
	if _, err := backup.Export(backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: 1, LastIncludedTerm: 1},
		BaseFSM:  fs,
		WAL:      w,
	}, backupDir, backup.ExportOptions{UntilIndex: 1}); err != nil {
		f.Fatalf("Export: %v", err)
	}
	w.Close()

	seedManifest, err := os.ReadFile(filepath.Join(backupDir, "manifest.json"))
	if err != nil {
		f.Fatalf("reading seed manifest: %v", err)
	}
	f.Add(seedManifest)
	f.Add([]byte(""))
	f.Add([]byte("{}"))
	f.Add([]byte("not json at all\nffffffff\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzDir := t.TempDir()
		manifestDir := filepath.Join(fuzzDir, "backup")
		if err := os.MkdirAll(manifestDir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(manifestDir, "manifest.json"), data, 0o644); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		dataDir := filepath.Join(fuzzDir, "restored")
		// Never panics, regardless of input; a successful "restore" from
		// a directory with no valid snapshot/wal components alongside
		// the fuzzed manifest would itself be a bug, but readManifest's
		// own validation (checksum, format version) rejects almost every
		// fuzzed input before any component is even looked at.
		_, _ = backup.Restore(manifestDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest})
	})
}
