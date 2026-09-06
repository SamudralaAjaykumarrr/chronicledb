// This file tests the -version flag against a real built binary — no
// build tag, so it runs on every `go test ./...` unlike this package's
// `integration`-tagged real-cluster tests (main_test.go, chaos_test.go):
// it needs exactly one process and no cluster/ports at all.
package main

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionFlag_PrintsAndExitsCleanly(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "chronicledb-node")
	build := exec.Command("go", "build", "-o", bin, ".")
	var stderr bytes.Buffer
	build.Stderr = &stderr
	if err := build.Run(); err != nil {
		t.Fatalf("building chronicledb-node: %v\n%s", err, stderr.String())
	}

	cmd := exec.Command(bin, "-version")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running -version: %v", err)
	}

	got := strings.TrimSpace(string(out))
	if !strings.HasPrefix(got, "chronicledb-node ") {
		t.Fatalf("-version output = %q, want it to start with %q", got, "chronicledb-node ")
	}
	// -version must exit 0 and must not require -id/-listen/-http/etc
	// even though none were given — this is what the "usage" fallback
	// gate in main() must be checked *after*, not before.
	if cmd.ProcessState.ExitCode() != 0 {
		t.Fatalf("-version exit code = %d, want 0", cmd.ProcessState.ExitCode())
	}
}
