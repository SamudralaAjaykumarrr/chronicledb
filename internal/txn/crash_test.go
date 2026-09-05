package txn

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// reopen closes w and re-opens the same data directory, then rebuilds a
// Manager from scratch via recovery — exactly what a real restart does
// (docs/recovery.md). It does not use any in-memory shortcut: store is
// a brand-new, empty mvcc.Store, so any state the returned Manager
// exposes came entirely from replaying the durable log.
func reopen(t *testing.T, dir string) (*Manager, *wal.WAL) {
	t.Helper()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open (reopen): %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		w.Close()
		t.Fatalf("NewManager (reopen): %v", err)
	}
	return m, w
}

// TestRestart_CommittedSingleKeySurvives is a restart-recovery variant
// of LD-1/LD-2 for a transactional commit: a committed single-key
// transaction's value is present after a clean close + reopen.
func TestRestart_CommittedSingleKeySurvives(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	tx := m.Begin()
	if err := tx.Write("K", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wantSeq, err := tx.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	reader := m2.Begin()
	v, found, err := reader.Read("K")
	if err != nil || !found || string(v) != "v1" {
		t.Fatalf("Read(K) after restart = %q,%v,%v, want v1,true,nil", v, found, err)
	}
	if reader.StartSeq() < wantSeq {
		t.Fatalf("post-restart StartSeq %d < pre-restart CommitSeq %d", reader.StartSeq(), wantSeq)
	}
}

// TestRestart_CommittedMultiKeySurvivesAtomically: a multi-key
// transaction's entire mutation set is present after restart, never a
// subset (ATOMICITY through a real recovery path, not just an in-memory
// Apply call).
func TestRestart_CommittedMultiKeySurvivesAtomically(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	tx := m.Begin()
	if err := tx.Write("A", []byte("10")); err != nil {
		t.Fatalf("Write A: %v", err)
	}
	if err := tx.Write("B", []byte("20")); err != nil {
		t.Fatalf("Write B: %v", err)
	}
	if err := tx.Delete("C"); err != nil {
		t.Fatalf("Delete C: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	reader := m2.Begin()
	for _, tc := range []struct {
		key       string
		wantValue string
		wantFound bool
	}{
		{"A", "10", true},
		{"B", "20", true},
		{"C", "", false},
	} {
		v, found, err := reader.Read(tc.key)
		if err != nil || found != tc.wantFound || (found && string(v) != tc.wantValue) {
			t.Fatalf("Read(%s) after restart = %q,%v,%v, want %q,%v,nil", tc.key, v, found, err, tc.wantValue, tc.wantFound)
		}
	}
}

// TestRestart_AbortedTransactionDoesNotAppear: an explicitly aborted
// transaction leaves nothing in the durable log at all, so it cannot
// possibly reappear after restart (ABORT-SAFETY).
func TestRestart_AbortedTransactionDoesNotAppear(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	tx := m.Begin()
	if err := tx.Write("K", []byte("never-committed")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	before := w.NextIndex()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	if w2.NextIndex() != before {
		t.Fatalf("NextIndex after restart = %d, want unchanged %d (abort must not have appended anything)", w2.NextIndex(), before)
	}
	reader := m2.Begin()
	if _, found, err := reader.Read("K"); err != nil || found {
		t.Fatalf("Read(K) after restart: found=%v err=%v, want false,nil", found, err)
	}
}

// TestRestart_NeverCommittedWorkspaceDoesNotAppear: a transaction that
// began, wrote, but was never explicitly aborted or committed before
// the process ended (e.g. the connection just dropped) must not appear
// after restart either — its workspace was always purely in-memory
// (docs/transactions.md §2).
func TestRestart_NeverCommittedWorkspaceDoesNotAppear(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	tx := m.Begin()
	if err := tx.Write("K", []byte("orphaned-write")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Deliberately neither Commit nor Abort: simulates the client/session
	// simply vanishing. The transaction's write set is only ever
	// in-memory, so closing the WAL now must leave nothing durable.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	reader := m2.Begin()
	if _, found, err := reader.Read("K"); err != nil || found {
		t.Fatalf("Read(K) after restart: found=%v err=%v, want false,nil", found, err)
	}
}

// TestRestart_TombstoneSurvives.
func TestRestart_TombstoneSurvives(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	setup := m.Begin()
	if err := setup.Write("K", []byte("v0")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := setup.Commit(); err != nil {
		t.Fatalf("Commit setup: %v", err)
	}
	del := m.Begin()
	if err := del.Delete("K"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := del.Commit(); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	reader := m2.Begin()
	if _, found, err := reader.Read("K"); err != nil || found {
		t.Fatalf("Read(K) after restart: found=%v err=%v, want false,nil (tombstone must survive)", found, err)
	}
}

// TestRestart_VersionOrderingPreserved: several sequential commits'
// relative CommitSeq ordering (and hence which value each StartSeq
// should observe) must be identical before and after restart.
func TestRestart_VersionOrderingPreserved(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	var seqs []uint64
	for _, v := range []string{"v1", "v2", "v3"} {
		tx := m.Begin()
		if err := tx.Write("K", []byte(v)); err != nil {
			t.Fatalf("Write: %v", err)
		}
		seq, err := tx.Commit()
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
		seqs = append(seqs, seq)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	// A snapshot at each recorded CommitSeq must see exactly that
	// generation's value, both before and after restart.
	want := []string{"v1", "v2", "v3"}
	for i, seq := range seqs {
		v, found, err := m2.readAt("K", seq)
		if err != nil || !found || string(v) != want[i] {
			t.Fatalf("post-restart read at CommitSeq %d = %q,%v,%v, want %q,true,nil", seq, v, found, err, want[i])
		}
	}
}

// readAt is a test-only helper exposing a raw store read at an
// arbitrary StartSeq without going through a full Txn (useful for
// probing intermediate historical snapshots directly).
func (m *Manager) readAt(key string, startSeq uint64) ([]byte, bool, error) {
	v, ok := m.store.Visible(key, startSeq)
	return v, ok, nil
}

// TestRestart_NoUnackedRecordMeansNothingRecovered is the txn-level
// analogue of LD-3: Manager.commit always calls Sync synchronously
// before treating a transaction as committed, so there is no public-API
// path that leaves an "acked but not durable" record behind — this test
// confirms the baseline (a clean restart of an empty log recovers empty
// state), and TestSyncFailureDuringCommitDoesNotApply below covers the
// actual failure-injection case (Sync fails outright).
func TestRestart_NoUnackedRecordMeansNothingRecovered(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	if _, err := NewManager(w, mvcc.NewStore()); err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if w.NextIndex() != 1 {
		t.Fatalf("fresh WAL NextIndex = %d, want 1", w.NextIndex())
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()
	reader := m2.Begin()
	if reader.StartSeq() != 0 {
		t.Fatalf("StartSeq after restart of an empty log = %d, want 0", reader.StartSeq())
	}
}

// TestDurabilityFailureDuringCommitDoesNotApply injects a real (not
// mocked) durability failure — closing the WAL out from under an
// in-flight Commit, so AppendLogEntry/Sync fail exactly as
// internal/wal's own TestSyncFailurePropagatesNotTreatedAsSuccess
// injects one — and verifies Commit surfaces the error, marks the
// transaction terminal (Aborted), and never applies the mutation to the
// MVCC store (docs/failure-model.md §1.8: "must never happen: treating
// a failed Sync() as successful").
func TestDurabilityFailureDuringCommitDoesNotApply(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	tx := m.Begin()
	if err := tx.Write("K", []byte("should-never-be-visible")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Force every subsequent Append/Sync call to fail, the same way
	// internal/wal's own fault-injection test does.
	if err := w.Close(); err != nil {
		t.Fatalf("closing WAL for fault injection: %v", err)
	}

	if _, err := tx.Commit(); err == nil {
		t.Fatal("Commit with a closed underlying WAL: expected error, got nil")
	}
	if tx.State() != StateAborted {
		t.Fatalf("State() after a failed commit = %v, want aborted", tx.State())
	}

	// Reopen a fresh Manager/store over the same (untouched) directory
	// and confirm nothing was recorded.
	m2, w2 := reopen(t, dir)
	defer w2.Close()
	reader := m2.Begin()
	if _, found, err := reader.Read("K"); err != nil || found {
		t.Fatalf("Read(K) after a failed-durability commit: found=%v err=%v, want false,nil", found, err)
	}
}

const (
	crashHelperEnv    = "CHRONICLEDB_TXN_CRASH_HELPER"
	crashHelperDirEnv = "CHRONICLEDB_TXN_CRASH_DIR"
	crashHelperReady  = "CHRONICLEDB_TXN_HELPER_READY"
)

// TestCrashAfterCommitSurvivesKill is a real subprocess/crash-style test
// (docs/testing-strategy.md §"Crash/restart tests", reusing Phase 1's
// approach from internal/wal/crash_test.go): a child process opens a
// Manager, commits one transaction (which synchronously appends and
// Syncs before returning), announces readiness, and is SIGKILLed — an
// actual ungraceful process termination. The parent then reopens the
// same data directory in-process and verifies the committed value
// survived the real kill (DURABILITY).
func TestCrashAfterCommitSurvivesKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL-based crash simulation requires a POSIX signal model")
	}
	if os.Getenv(crashHelperEnv) == "1" {
		runTxnCrashHelper()
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashAfterCommitSurvivesKill$")
	cmd.Env = append(os.Environ(), crashHelperEnv+"=1", crashHelperDirEnv+"="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	readyCh := make(chan error, 1)
	scanner := bufio.NewScanner(stdout)
	go func() {
		for scanner.Scan() {
			if scanner.Text() == crashHelperReady {
				readyCh <- nil
				return
			}
		}
		readyCh <- fmt.Errorf("helper stdout ended without readiness marker: %w", scanner.Err())
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatalf("waiting for helper readiness: %v (stderr: %s)", err, stderr.String())
		}
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("timed out waiting for helper readiness (stderr: %s)", stderr.String())
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	_ = cmd.Wait() // expected to report termination by signal, not success

	m, w := reopen(t, dir)
	defer w.Close()

	reader := m.Begin()
	v, found, err := reader.Read("acked-key")
	if err != nil || !found || string(v) != "acked-before-kill" {
		t.Fatalf("Read(acked-key) after crash = %q,%v,%v, want \"acked-before-kill\",true,nil", v, found, err)
	}
}

// runTxnCrashHelper is the child-process body invoked when
// crashHelperEnv is set. It commits one transaction, announces
// readiness, then blocks forever so the parent's SIGKILL is the only
// way it ever stops.
func runTxnCrashHelper() {
	dir := os.Getenv(crashHelperDirEnv)
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper wal.Open:", err)
		os.Exit(1)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper NewManager:", err)
		os.Exit(1)
	}
	tx := m.Begin()
	if err := tx.Write("acked-key", []byte("acked-before-kill")); err != nil {
		fmt.Fprintln(os.Stderr, "helper Write:", err)
		os.Exit(1)
	}
	if _, err := tx.Commit(); err != nil {
		fmt.Fprintln(os.Stderr, "helper Commit:", err)
		os.Exit(1)
	}
	fmt.Println(crashHelperReady)
	select {}
}
