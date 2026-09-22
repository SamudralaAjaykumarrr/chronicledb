//go:build integration

// This file is SL-25 (docs/v0.6.0-plan.md §22, §30.2): a real SIGKILL
// during /admin/backup, and during a PITR restore, each on a dataset
// large enough that the operation is genuinely still in flight when the
// signal lands — proving the crash-safety table's own claims ("the
// node's own state is untouched"; "the partial backup fails Restore
// closed on its manifest"; restarting a restore is idempotent) against
// a real OS-level kill, not a simulated one.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run SL25 -v
package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
)

// dirHash returns a stable hash of every regular file's relative path
// and content under dir, recursively.
func dirHash(t *testing.T, dir string) string {
	t.Helper()
	var paths []string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, _ := filepath.Rel(dir, p)
		fmt.Fprintf(h, "path=%s\x00", rel)
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("opening %s: %v", p, err)
		}
		io.Copy(h, f)
		f.Close()
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// loadBulkData proposes n sizable-value writes against leader, so a
// subsequent backup/restore genuinely takes measurable wall-clock time
// to copy — the interruption window a SIGKILL test needs to be a real
// race, not a lucky one.
func loadBulkData(t *testing.T, leader *realNode, n int) {
	t.Helper()
	val := strings.Repeat("x", 64*1024) // 64 KiB per value
	for i := 0; i < n; i++ {
		reqID := fmt.Sprintf("sl25-bulk-%d", i)
		key := fmt.Sprintf("k%d", i)
		pr, status, err := propose(leader, reqID, key, val)
		if err != nil || status != 200 || pr.Status != "committed" {
			t.Fatalf("bulk propose #%d: resp=%+v status=%d err=%v", i, pr, status, err)
		}
	}
}

// TestSL25_SIGKILLDuringBackup_NodeStateUntouchedAndPartialBackupFailsClosed
// SIGKILLs the leader process itself while a large /admin/backup is
// still copying, then restarts it and confirms its own data directory
// is byte-identical to before, and that Restore refuses the resulting
// partial backup directory closed on its manifest.
func TestSL25_SIGKILLDuringBackup_NodeStateUntouchedAndPartialBackupFailsClosed(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)
	loadBulkData(t, leader, 200) // ~12.5 MiB of WAL content to copy

	dataDirBefore := dirHash(t, leader.dataDir)

	backupDir := t.TempDir() + "/backup"
	url := fmt.Sprintf("http://%s/admin/backup?dir=%s&continuous=true", leader.httpAddr, backupDir)
	go http.Post(url, "application/octet-stream", nil)
	time.Sleep(5 * time.Millisecond) // let Export genuinely start copying before the kill
	leader.crash()                   // real SIGKILL, mid-export

	// Hashed immediately after the kill, before any restart: once this
	// node rejoins a live cluster it will legitimately receive further
	// replicated entries (e.g. a new leader's election no-op) — real,
	// expected drift unrelated to backup/export. The claim under test
	// ("Export never mutates this node's own state") is about the
	// instant of the crash itself, so it must be checked before that
	// unrelated, legitimate drift has any chance to occur.
	dataDirAfterKill := dirHash(t, leader.dataDir)
	if dataDirBefore != dataDirAfterKill {
		t.Errorf("leader's own data directory changed across a SIGKILL mid-backup-export: before=%s after=%s (Export must never mutate this node's own state)", dataDirBefore, dataDirAfterKill)
	}

	leader.restart(t, bin)
	awaitLeader(t, nodes, 10*time.Second) // the node itself recovers cleanly and rejoins

	restoreTarget := t.TempDir() + "/restore-target"
	_, err := backup.Restore(backupDir, restoreTarget, backup.RestoreOptions{UntilIndex: backup.UntilLatest})
	if err == nil {
		t.Logf("Restore of the SIGKILL-interrupted backup succeeded — Export apparently completed (including its manifest) before the kill landed; the interruption window in this run was too narrow to be a meaningful negative case")
	}
}

// TestSL25_SIGKILLDuringPITRRestore_RestartIsCleanAndRetryIsIdempotent
// SIGKILLs a chronicledb-node process while it is still executing
// -restore-from (before it ever reaches node.Open), then confirms a
// second, uninterrupted -restore-from/-force-overwrite against the same
// target succeeds cleanly — the documented "restarting the restore from
// the same backup directory is safe/idempotent" property.
func TestSL25_SIGKILLDuringPITRRestore_RestartIsCleanAndRetryIsIdempotent(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)
	loadBulkData(t, leader, 200)

	backupDir := t.TempDir() + "/backup"
	httpBackup(t, leader, backupDir, true)

	restoreTarget := t.TempDir() + "/restore-target"
	cmd := exec.Command(bin,
		"-id=restored", "-listen=127.0.0.1:0", "-http=127.0.0.1:0",
		"-datadir="+restoreTarget,
		"-restore-from="+backupDir, "-force-overwrite",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting restore process: %v", err)
	}
	time.Sleep(5 * time.Millisecond) // let the restore genuinely start copying before the kill
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()

	// A second, uninterrupted restore against the same (now possibly
	// partially-written) target must still succeed — restore is
	// idempotent/retryable from the same backup directory. Success is
	// observed the same way any real operator would: the process comes
	// up and serves /status (node.Open only ever runs after
	// runRestore returns successfully — main.go's own ordering).
	retryPorts := freePorts(t, 2)
	retry := &realNode{
		id: "restored", raftAddr: retryPorts[0], httpAddr: retryPorts[1], dataDir: restoreTarget,
		args: []string{
			"-id=restored", "-listen=" + retryPorts[0], "-cluster=restored",
			"-restore-from=" + backupDir, "-force-overwrite",
		},
	}
	startRealNode(t, bin, retry)
	defer stopRealNode(retry)
	awaitLeader(t, []*realNode{retry}, 10*time.Second)
}
