//go:build faulttest

package node

import (
	"context"
	"fmt"
	"sync"
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

// armOnce arms a fault hook that fires exactly once, at target, and
// reports firing by closing the returned channel — a stronger signal
// than CrashOnceAtFaultPointForTest alone, since this smoke test needs
// to know reachability was actually exercised, not merely that the
// node eventually stopped for some unrelated reason.
func armOnce(t *testing.T, target FaultPoint) <-chan struct{} {
	t.Helper()
	fired := make(chan struct{})
	var once sync.Once
	SetFaultPointForTest(func(p FaultPoint) bool {
		if p != target {
			return false
		}
		once.Do(func() { close(fired) })
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
