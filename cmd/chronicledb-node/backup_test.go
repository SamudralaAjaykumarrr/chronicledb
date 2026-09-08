package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// makeBackupDirForTest builds a minimal valid backup directory (n
// entries, no base snapshot) under t.TempDir(), for exercising the CLI
// restore preflight (runRestore/recordRestoreAudit) without spinning up
// a real node.Node.
func makeBackupDirForTest(t *testing.T, n int) string {
	t.Helper()
	srcDir := t.TempDir()
	w, _, err := wal.Open(srcDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening source WAL: %v", err)
	}
	defer w.Close()
	for i := 1; i <= n; i++ {
		cmd := fsm.CommitTxnCommand{RequestID: fsm.RequestID("r"), TxnID: uint64(i), Mutations: []mvcc.Mutation{{Key: "k", Value: []byte("v")}}}
		if _, err := w.AppendLogEntry(fsm.EncodeCommitTxn(cmd)); err != nil {
			t.Fatalf("appending entry %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	src := backup.Source{BaseMeta: snapshot.Meta{}, BaseFSM: fsm.New(mvcc.NewStore()), WAL: w}
	if _, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return backupDir
}

func TestRunRestore_DefaultUntilRestoresEverything(t *testing.T) {
	backupDir := makeBackupDirForTest(t, 5)
	dataDir := filepath.Join(t.TempDir(), "restored")
	res, err := runRestore(backupDir, dataDir, "", false)
	if err != nil {
		t.Fatalf("runRestore: %v", err)
	}
	if res.RestoredUntilIndex != 5 {
		t.Fatalf("RestoredUntilIndex = %d, want 5", res.RestoredUntilIndex)
	}
}

func TestRunRestore_ExplicitUntilIndex(t *testing.T) {
	backupDir := makeBackupDirForTest(t, 5)
	dataDir := filepath.Join(t.TempDir(), "restored")
	res, err := runRestore(backupDir, dataDir, "3", false)
	if err != nil {
		t.Fatalf("runRestore: %v", err)
	}
	if res.RestoredUntilIndex != 3 {
		t.Fatalf("RestoredUntilIndex = %d, want 3", res.RestoredUntilIndex)
	}
}

func TestRunRestore_InvalidUntilFlagRejected(t *testing.T) {
	backupDir := makeBackupDirForTest(t, 5)
	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := runRestore(backupDir, dataDir, "not-a-number", false); err == nil {
		t.Fatal("runRestore with a malformed -restore-until unexpectedly succeeded")
	}
}

func TestRunRestore_NonCleanTargetRequiresForce(t *testing.T) {
	backupDir := makeBackupDirForTest(t, 5)
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "existing"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seeding target: %v", err)
	}
	if _, err := runRestore(backupDir, dataDir, "", false); err == nil {
		t.Fatal("runRestore into a non-clean directory without force unexpectedly succeeded")
	}
	if _, err := runRestore(backupDir, dataDir, "", true); err != nil {
		t.Fatalf("forced runRestore: %v", err)
	}
}

func TestRecordRestoreAudit_WritesVerifiableEntry(t *testing.T) {
	backupDir := makeBackupDirForTest(t, 3)
	dataDir := filepath.Join(t.TempDir(), "restored")
	res, err := runRestore(backupDir, dataDir, "", false)
	if err != nil {
		t.Fatalf("runRestore: %v", err)
	}

	auditDir := t.TempDir()
	if err := recordRestoreAudit(auditDir, backupDir, dataDir, false, res); err != nil {
		t.Fatalf("recordRestoreAudit: %v", err)
	}

	entries, err := audit.ReadAll(auditDir)
	if err != nil {
		t.Fatalf("audit.ReadAll: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	if entries[0].Action != "restore" || entries[0].Principal != "cli-operator" || entries[0].Result != "allow" {
		t.Fatalf("unexpected audit entry: %+v", entries[0])
	}
	if err := audit.Verify(auditDir); err != nil {
		t.Fatalf("audit.Verify: %v", err)
	}
}
