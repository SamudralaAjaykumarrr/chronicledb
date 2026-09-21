package node

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
)

// TestSnapshotServeMiss_SL11 is SL-11 (docs/v0.6.0-plan.md §18.1): force
// a real snapshot-serve miss — a snapshot pruned between Core deciding a
// follower needs index X and processOutput actually filling
// MsgInstallSnapshotRequest's bytes for X — and prove
// snapshot_serve_miss_total increments and the follower still converges
// via retry, exactly as §18.1 describes this race as self-healing.
//
// The forced prune uses SetPreSnapshotBytesFillHookForTest, which fires
// synchronously on the leader's own event-loop goroutine immediately
// before the real n.snapMgr.Bytes(index) call — the same goroutine that
// owns snapMgr in production, so calling snapMgr.Create reentrantly from
// inside the hook is exactly as safe as maybeSnapshot's own call to it,
// not a race. The phantom snapshot this creates is deliberately never
// recorded via AppendMetadataSnapshot, so Load never adopts it (Load's
// own doc comment: it never trusts a file above the durable pointer) —
// its only effect is pruning the file the fill was about to read,
// reproducing exactly the fill-step failure mode §18.1 describes without
// needing to win a real race by timing.
func TestSnapshotServeMiss_SL11(t *testing.T) {
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
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("sl11-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
	}
	awaitCondition(t, 10*time.Second, "leader creates a real local snapshot", func() bool {
		return uint64(leader.Status().SnapshotIndex) > 0
	})
	targetIndex := uint64(leader.Status().SnapshotIndex)

	var once sync.Once
	fired := make(chan struct{})
	leader.SetPreSnapshotBytesFillHookForTest(func(index uint64) {
		if index != targetIndex {
			return
		}
		once.Do(func() {
			// A phantom, never-adopted snapshot at a higher index, whose
			// sole purpose is to prune targetIndex's file away right
			// here — see the function doc comment above. Manager no
			// longer prunes automatically inside Create (that eager
			// prune was itself SL-8's crash-safety bug, fixed by moving
			// it to an explicit, caller-timed Prune call — see
			// Manager.Prune's own doc comment), so this test drives that
			// same explicit step directly to force the race
			// deterministically.
			phantom := snapshot.Meta{LastIncludedIndex: targetIndex + 1}
			if _, err := leader.snapMgr.Create(phantom, leader.fsmachine.Load(), leader.snapshotWriteVersion()); err != nil {
				t.Errorf("forcing the snapshot-serve-miss prune (Create): %v", err)
			}
			if err := leader.snapMgr.Prune(targetIndex + 1); err != nil {
				t.Errorf("forcing the snapshot-serve-miss prune (Prune): %v", err)
			}
			close(fired)
		})
	})
	defer leader.SetPreSnapshotBytesFillHookForTest(nil)

	before := leader.Metrics().SnapshotServeMissTotal
	tc.heal(follower)

	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatalf("SetPreSnapshotBytesFillHookForTest never fired for target index %d", targetIndex)
	}

	awaitCondition(t, 10*time.Second, "SnapshotServeMissTotal increments", func() bool {
		return leader.Metrics().SnapshotServeMissTotal > before
	})

	awaitCondition(t, 15*time.Second, "follower still converges via retry despite the forced serve miss", func() bool {
		return tc.node(follower).Status().AppliedIndex == leader.Status().AppliedIndex
	})
}
