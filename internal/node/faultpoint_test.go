//go:build faulttest

package node

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// awaitFired waits for ch to close (the injected hook decided to crash
// at its target point) within timeout, failing the test otherwise —
// slice 10a's own exit criterion is exactly this: "a smoke test proves
// each point is reachable," selected by index rather than timing.
func awaitFired(t *testing.T, ch <-chan struct{}, timeout time.Duration, point FaultPoint) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("FaultPoint %d was never reached within %s", point, timeout)
	}
}

// awaitNodeStopped confirms n's event-loop goroutine has actually
// exited (shutdown ran as part of the fault-triggered panic's unwind —
// see faultPointCrash's own doc comment) by observing that Stop()
// returns promptly rather than blocking on a still-running loop.
func awaitNodeStopped(t *testing.T, n *Node, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { n.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("node did not stop within %s after a simulated crash", timeout)
	}
}

// armOnce arms a fault hook that crashes exactly once, the first time
// target is reached, and reports that by closing the returned channel —
// a stronger signal than CrashOnceAtFaultPointForTest alone, since a
// caller needs to know reachability was actually exercised, not merely
// that the node eventually stopped for some unrelated reason.
//
// The hook must stop returning true after its first match, not just
// stop re-closing fired: a node restarted after the injected crash
// (SL-8/SL-26) can legitimately reach the very same FaultPoint again on
// a later, ordinary snapshot cycle (most easily for
// FaultAfterSnapshotCreate, literally the first line of maybeSnapshot),
// and the package-level fault hook stays armed across that restart
// (SetFaultPointForTest is not per-Node). An earlier version of this
// helper only guarded the channel close with sync.Once while still
// unconditionally returning true on every match, so the restarted node
// crashed again immediately, silently, on its very first post-restart
// snapshot cycle — observed as the restarted node getting permanently
// stuck reporting a stale pre-election Follower status forever, not as
// a visible second crash, since nothing in these tests re-observes
// armOnce's own fired channel a second time.
func armOnce(t *testing.T, target FaultPoint) <-chan struct{} {
	t.Helper()
	fired := make(chan struct{})
	var didFire atomic.Bool
	SetFaultPointForTest(func(p FaultPoint) bool {
		if p != target {
			return false
		}
		if !didFire.CompareAndSwap(false, true) {
			return false
		}
		close(fired)
		return true
	})
	t.Cleanup(func() { SetFaultPointForTest(nil) })
	return fired
}

// TestFaultPointSmoke_MaybeSnapshotSixPoints is slice 10a's own exit
// criterion for maybeSnapshot's half of the facility (docs/v0.6.0-plan.md
// §17.2, §33 slice 10a): each of the six named ordering points is
// reachable, deterministically, selected by index. A single-node
// cluster is the simplest fixture that still exercises the real
// snapMgr/walog/core/storage call sequence — SL-8 (slice 13) is what
// proves *safety* across a restart at each point; this only proves
// *reachability*.
func TestFaultPointSmoke_MaybeSnapshotSixPoints(t *testing.T) {
	points := []FaultPoint{
		FaultAfterSnapshotCreate,
		FaultAfterAppendMetadataSnapshot,
		FaultAfterCoreCompact,
		FaultAfterStorageCompact,
		FaultAfterReaffirm,
		FaultAfterCompactBefore,
	}
	for _, p := range points {
		p := p
		t.Run(fmt.Sprintf("point_%d", p), func(t *testing.T) {
			const threshold = 3
			tc := newTestClusterWithSnapshotThreshold(t, 1, threshold)
			leaderID := tc.awaitLeader(10 * time.Second)
			leader := tc.node(leaderID)

			fired := armOnce(t, p)

			for i := 0; i < 3*threshold; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := leader.Propose(ctx, cmd(fmt.Sprintf("smoke-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"))
				cancel()
				if err != nil {
					// The simulated crash may itself have interrupted
					// this very Propose call once the loop goroutine
					// stops mid-write — that observation is exactly
					// what this test is trying to produce, not a
					// failure.
					break
				}
			}

			awaitFired(t, fired, 5*time.Second, p)
			awaitNodeStopped(t, leader, 3*time.Second)
			delete(tc.nodes, leaderID) // already stopped; keep t.Cleanup's Stop() a no-op
		})
	}
}

// TestFaultPointSmoke_HandleInstallSnapshotThreePoints is slice 10a's
// exit criterion for handleInstallSnapshot's half (§22's three new
// rows): each point is reachable during a real learner catch-up via
// InstallSnapshot, selected by index. SL-26 (slice 13) proves safety
// across a restart at each point; this only proves reachability.
func TestFaultPointSmoke_HandleInstallSnapshotThreePoints(t *testing.T) {
	points := []FaultPoint{
		FaultBeforeInstallSnapshotStorage,
		FaultAfterInstallSnapshotBeforeFSMSwap,
		FaultAfterFSMSwapBeforeGenerationAdopt,
	}
	for _, p := range points {
		p := p
		t.Run(fmt.Sprintf("point_%d", p), func(t *testing.T) {
			const threshold = 5
			tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
			leaderID := tc.awaitLeader(10 * time.Second)
			leader := tc.node(leaderID)

			var follower raft.NodeID
			for _, id := range tc.ids {
				if id != leaderID {
					follower = id
					break
				}
			}

			tc.isolate(follower)
			for i := 0; i < 3*threshold; i++ {
				outcome, err := propose(t, leader, cmd(fmt.Sprintf("smoke-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
				if err != nil || outcome.Status != fsm.StatusCommitted {
					t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
				}
			}
			awaitCondition(t, 10*time.Second, "leader creates a real local snapshot", func() bool {
				return uint64(leader.Status().SnapshotIndex) > 0
			})

			followerNode := tc.node(follower)
			fired := armOnce(t, p)
			tc.heal(follower)

			awaitFired(t, fired, 10*time.Second, p)
			awaitNodeStopped(t, followerNode, 3*time.Second)
			delete(tc.nodes, follower)
		})
	}
}

// TestMaybeSnapshotSixStepCallOrder_SL24 is SL-24
// (docs/v0.6.0-plan.md §17.2, C4): a call-order assertion proving
// maybeSnapshot's six steps run in the exact documented order, so a
// future refactor cannot silently permute them. Reuses the slice 10a
// fault-point facility purely as an observation point (the hook always
// returns false — never crashes) rather than as a crash injector.
func TestMaybeSnapshotSixStepCallOrder_SL24(t *testing.T) {
	const threshold = 3
	tc := newTestClusterWithSnapshotThreshold(t, 1, threshold)
	leaderID := tc.awaitLeader(10 * time.Second)
	leader := tc.node(leaderID)

	var mu sync.Mutex
	var order []FaultPoint
	SetFaultPointForTest(func(p FaultPoint) bool {
		mu.Lock()
		order = append(order, p)
		mu.Unlock()
		return false
	})
	defer SetFaultPointForTest(nil)

	for i := 0; i < 3*threshold; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("order-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
	}
	awaitCondition(t, 5*time.Second, "leader creates a snapshot", func() bool {
		return uint64(leader.Status().SnapshotIndex) > 0
	})

	want := []FaultPoint{
		FaultAfterSnapshotCreate,
		FaultAfterAppendMetadataSnapshot,
		FaultAfterCoreCompact,
		FaultAfterStorageCompact,
		FaultAfterReaffirm,
		FaultAfterCompactBefore,
	}
	mu.Lock()
	got := append([]FaultPoint(nil), order...)
	mu.Unlock()
	if len(got) < len(want) {
		t.Fatalf("observed only %d fault-point events, want at least %d (one full maybeSnapshot cycle): %v", len(got), len(want), got)
	}
	first6 := got[:len(want)]
	for i, p := range want {
		if first6[i] != p {
			t.Fatalf("maybeSnapshot's six-step call order was permuted: got %v, want %v", first6, want)
		}
	}
}
