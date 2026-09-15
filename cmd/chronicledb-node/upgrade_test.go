// This file unit-tests /admin/upgrade/finalize's HTTP response
// directly against an in-process, single-node node.Node — no build
// tag, so it runs on every `go test ./...` unlike this package's
// `integration`-tagged real-OS-process mixed-version suite
// (mixed_version_test.go), mirroring control_test.go's own fast
// in-process style.
package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
)

// TestHandleUpgradeFinalize_ReportsTheActualAchievedGeneration is a
// regression test for a real bug this session's mixed-version
// integration suite surfaced (previously blocked from ever running by
// an unrelated go-build VCS-stamping failure, mixed_version_test.go's
// buildBinaryAtRef): handleUpgradeFinalize unconditionally reported
// NewGeneration as internal/version.MaxSupportedGeneration, on the
// stale assumption (from before dynamic-membership plan §8.1 bumped
// that constant 1 -> 2) that a single finalize call always jumps
// straight to it. Node.FinalizeUpgrade actually raises the cluster by
// exactly one generation per call (current+1,
// PrecheckResult.TargetGeneration's own doc comment) — so on a binary
// whose MaxSupportedGeneration is 2 or higher, a single finalize call
// against a fresh (generation-0) cluster only ever reaches generation
// 1, and the old code misreported it as MaxSupportedGeneration
// instead.
func TestHandleUpgradeFinalize_ReportsTheActualAchievedGeneration(t *testing.T) {
	if version.MaxSupportedGeneration < 2 {
		t.Skipf("this regression needs MaxSupportedGeneration >= 2 to distinguish the bug from the correct behavior; got %d", version.MaxSupportedGeneration)
	}
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/upgrade/finalize", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"newGeneration":1`) {
		t.Fatalf(`response body = %s, want "newGeneration":1 (one single-step finalize from a fresh cluster), not MaxSupportedGeneration (%d)`, body, version.MaxSupportedGeneration)
	}

	// The node's own durable/replicated state must agree with what the
	// HTTP response reported — not still at 0, and not already jumped to
	// MaxSupportedGeneration.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().ClusterGeneration == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := n.Status().ClusterGeneration; got != 1 {
		t.Fatalf("node's own ClusterGeneration = %d, want 1", got)
	}

	// A second finalize call must be needed (and must succeed) to reach
	// the true max — proving the first call genuinely only advanced by
	// one step rather than the response merely being mislabeled.
	if version.MaxSupportedGeneration > 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, _, err := n.FinalizeUpgrade(ctx); err != nil {
			t.Fatalf("second FinalizeUpgrade call: %v", err)
		}
		if got := n.Status().ClusterGeneration; got != version.MaxSupportedGeneration {
			t.Fatalf("ClusterGeneration after the second finalize = %d, want %d", got, version.MaxSupportedGeneration)
		}
	}
}
