package wal

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

const (
	crashHelperEnv    = "CHRONICLEDB_WAL_CRASH_HELPER"
	crashHelperDirEnv = "CHRONICLEDB_WAL_CRASH_DIR"
	crashHelperReady  = "CHRONICLEDB_HELPER_READY"
)

// TestCrashAfterSyncSurvivesKill is a real subprocess/crash-style test
// (scenario LD-2): a child process opens a WAL, appends and syncs one
// record, announces it has done so, and is then sent SIGKILL — an actual
// ungraceful process termination, not a clean exit or an ordinary
// function return. The parent then reopens the same data directory
// in-process and verifies the persisted record survived the real kill.
func TestCrashAfterSyncSurvivesKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL-based crash simulation requires a POSIX signal model")
	}
	if os.Getenv(crashHelperEnv) == "1" {
		runCrashHelper()
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashAfterSyncSurvivesKill$")
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

	w, report, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer w.Close()
	if report.LastLogIndex != 1 {
		t.Fatalf("LastLogIndex after crash = %d, want 1", report.LastLogIndex)
	}

	it, err := w.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	rec, ok, err := it.Next()
	if err != nil || !ok || string(rec.Payload) != "acked-before-kill" {
		t.Fatalf("Next() = %+v ok=%v err=%v, want payload \"acked-before-kill\"", rec, ok, err)
	}
}

// runCrashHelper is the child-process body invoked when crashHelperEnv is
// set. It writes and syncs one record, announces readiness, then blocks
// forever so the parent's SIGKILL is the only way it ever stops — no
// deferred/graceful Close ever runs.
func runCrashHelper() {
	dir := os.Getenv(crashHelperDirEnv)
	w, _, err := Open(dir, Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper Open:", err)
		os.Exit(1)
	}
	if _, err := w.AppendLogEntry([]byte("acked-before-kill")); err != nil {
		fmt.Fprintln(os.Stderr, "helper AppendLogEntry:", err)
		os.Exit(1)
	}
	if err := w.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, "helper Sync:", err)
		os.Exit(1)
	}
	fmt.Println(crashHelperReady)
	select {}
}
