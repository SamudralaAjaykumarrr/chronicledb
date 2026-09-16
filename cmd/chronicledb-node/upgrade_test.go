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

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
)

// orderedClusterGeneration reports n's cluster generation through
// Node.UpgradePrecheck — the event-loop-serialized read path — never
// through the cached Status() snapshot.
//
// The distinction is load-bearing, and reading the cache instead was a
// real intermittent failure here ("ClusterGeneration after the second
// finalize = 1, want 2"). Node.applyControlEntry sets the node's
// cluster generation and then releases the FinalizeUpgrade caller from
// resolveWaiter, both in the middle of one event-loop iteration, while
// Node.run refreshes the cached Status only at the END of that
// iteration (refreshStatusLocked, after the select arm returns). So
// between "FinalizeUpgrade returned" and "the cached Status reflects
// it" there is a genuine window with no happens-before edge, and an
// assertion reading Status() in that window legitimately observes the
// PREVIOUS generation. Nothing about the finalize itself is wrong: the
// entry is committed, applied, and durably adopted (adoptClusterGeneration)
// before the caller is ever released.
//
// UpgradePrecheck has no such window: it dispatches a request onto the
// same single-threaded event loop over an unbuffered channel, so it
// cannot be serviced until the iteration that released the caller has
// finished, and it then computes LocalClusterGeneration live from the
// node's own field rather than from any cached value. Sending on that
// channel is the ordering edge the assertion needs — which is why this
// is the right oracle rather than a sleep, a poll, or a weakened
// expectation. This mirrors internal/node's membershipCounts, which
// closes the identical gap for voter/learner counts.
func orderedClusterGeneration(t *testing.T, n *node.Node) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := n.UpgradePrecheck(ctx)
	if err != nil {
		t.Fatalf("UpgradePrecheck: %v", err)
	}
	return res.LocalClusterGeneration
}

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
	// MaxSupportedGeneration. handleUpgradeFinalize calls FinalizeUpgrade
	// synchronously, so by the time ServeHTTP has returned the generation
	// is committed and applied, and the ordered read below observes it at
	// once — no polling.
	if got := orderedClusterGeneration(t, n); got != 1 {
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
		if got := orderedClusterGeneration(t, n); got != version.MaxSupportedGeneration {
			t.Fatalf("ClusterGeneration after the second finalize = %d, want %d", got, version.MaxSupportedGeneration)
		}
	}
}

// TestClusterGenerationIsOrderedAfterFinalizeUpgrade pins the ordering
// property orderedClusterGeneration relies on, and that the cached
// Status() deliberately does not provide: once FinalizeUpgrade has
// returned successfully, the event-loop-serialized read must ALREADY
// report the generation that call reached, with no polling and no
// retry.
//
// The edge is structural, not timing: UpgradePrecheck dispatches onto
// Node.run's single-threaded event loop over an unbuffered channel, so
// it cannot be serviced until the iteration that released the caller
// has completed, and it computes LocalClusterGeneration live. Node.run
// refreshes its cached Status only at the end of each iteration, after
// applyControlEntry has already released the caller from resolveWaiter
// — which is why reading Status() immediately after a finalize was a
// real intermittent failure here (14/200 under deliberate CPU
// contention, "ClusterGeneration after the second finalize = 1,
// want 2").
//
// Every single-generation step is asserted immediately, so a
// regression that leaves the read unordered fails here rather than
// resurfacing as an intermittent generation mismatch somewhere
// downstream.
func TestClusterGenerationIsOrderedAfterFinalizeUpgrade(t *testing.T) {
	n := openSingleNodeForControlTest(t)

	if got := orderedClusterGeneration(t, n); got != 0 {
		t.Fatalf("a fresh single-node cluster's generation = %d, want 0", got)
	}

	// FinalizeUpgrade advances exactly one generation per call, so walk
	// every step from 0 to this binary's max, asserting each immediately.
	for want := uint32(1); want <= version.MaxSupportedGeneration; want++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, target, err := n.FinalizeUpgrade(ctx)
		cancel()
		if err != nil {
			t.Fatalf("FinalizeUpgrade to generation %d: %v", want, err)
		}
		if target != want {
			t.Fatalf("FinalizeUpgrade reported target generation %d, want %d", target, want)
		}
		if got := orderedClusterGeneration(t, n); got != want {
			t.Fatalf("after FinalizeUpgrade to %d returned, the ordered read reports generation %d — the read was not ordered after the finalize", want, got)
		}
	}

	if got := orderedClusterGeneration(t, n); got != version.MaxSupportedGeneration {
		t.Fatalf("final generation = %d, want %d", got, version.MaxSupportedGeneration)
	}
}
