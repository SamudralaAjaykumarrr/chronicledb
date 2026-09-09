package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
)

// TestUpgradePrecheckAndFinalize_RealCluster is this package's in-process
// (real WAL-backed storage, real TCP transport) proof of
// docs/enterprise-v1-plan.md §7's precheck/finalize mechanism end-to-end,
// complementary to cmd/chronicledb-node's real-mixed-binary integration
// test: every node here runs the identical current binary, so this test
// exercises the precheck-converges/finalize-commits/replicates-to-every-
// node/idempotent-retry/non-leader-rejection machinery, while the
// mixed-binary test is what actually proves old/new interoperation.
func TestUpgradePrecheckAndFinalize_RealCluster(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	// Every node runs this same binary, so once heartbeats have made a
	// round trip, precheck should converge to Ready — polled, not
	// slept-for, since heartbeat timing is not this test's concern.
	awaitCondition(t, 5*time.Second, "precheck reports Ready", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := leader.UpgradePrecheck(ctx)
		return err == nil && res.Ready && !res.AlreadyFinalized
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pre, err := leader.UpgradePrecheck(ctx)
	if err != nil {
		t.Fatalf("UpgradePrecheck: %v", err)
	}
	if pre.TargetGeneration != version.MaxSupportedGeneration {
		t.Fatalf("TargetGeneration = %d, want %d", pre.TargetGeneration, version.MaxSupportedGeneration)
	}
	for _, id := range tc.ids {
		if id == leaderID {
			continue
		}
		info, ok := pre.Peers[string(id)]
		if !ok || !info.Known || info.Generation != version.MaxSupportedGeneration {
			t.Fatalf("precheck peer info for %s = %+v (ok=%v), want Known=true Generation=%d", id, info, ok, version.MaxSupportedGeneration)
		}
	}

	// A follower cannot finalize directly — checked BEFORE the cluster is
	// actually finalized, since AlreadyFinalized would otherwise
	// short-circuit FinalizeUpgrade before it ever reaches the
	// leader-only check.
	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}
	follower := tc.node(followerID)
	_, ferr := follower.FinalizeUpgrade(ctx)
	var nle *NotLeaderError
	if !errors.As(ferr, &nle) {
		t.Fatalf("follower FinalizeUpgrade: err = %v, want *NotLeaderError", ferr)
	}

	outcome, err := leader.FinalizeUpgrade(ctx)
	if err != nil {
		t.Fatalf("FinalizeUpgrade: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("FinalizeUpgrade outcome = %+v, want Committed", outcome)
	}

	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" converges on ClusterGeneration=1", func() bool {
			return tc.node(id).Status().ClusterGeneration == version.MaxSupportedGeneration
		})
	}

	// Idempotent retry: finalize again once already at max.
	if _, err := leader.FinalizeUpgrade(ctx); !errors.Is(err, ErrAlreadyFinalized) {
		t.Fatalf("second FinalizeUpgrade: err = %v, want ErrAlreadyFinalized", err)
	}
}

// TestFinalizeUpgrade_RestartPersistsGeneration proves "restart/recovery
// across supported version transitions": a node that crashes and
// restarts after finalize must recover ClusterGeneration=1 purely from
// its own durable WAL/snapshot state (wal.Open's ErrUnsupportedGeneration
// check and fsm.DecodeState's generation-aware trailing field — see
// their own tests), not from live cluster traffic.
func TestFinalizeUpgrade_RestartPersistsGeneration(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	awaitCondition(t, 5*time.Second, "precheck reports Ready", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := leader.UpgradePrecheck(ctx)
		return err == nil && res.Ready
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := leader.FinalizeUpgrade(ctx); err != nil {
		t.Fatalf("FinalizeUpgrade: %v", err)
	}
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" converges on ClusterGeneration=1", func() bool {
			return tc.node(id).Status().ClusterGeneration == version.MaxSupportedGeneration
		})
	}

	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}
	tc.crash(followerID)
	restarted := tc.restart(followerID)
	// Open() itself only restores the last local snapshot boundary
	// (none exists in this test — well below the default snapshot
	// threshold); catching up to the committed finalize entry beyond
	// that happens asynchronously once its event loop starts replaying
	// the log, exactly like AppliedIndex catch-up after any other
	// restart — so this polls rather than reading Status() immediately.
	awaitCondition(t, 5*time.Second, "restarted node recovers ClusterGeneration=1 from local durable state", func() bool {
		return restarted.Status().ClusterGeneration == version.MaxSupportedGeneration
	})
}

// TestFinalizeUpgrade_FollowerAdoptsGenerationViaSnapshotInstall is a
// regression pin for a gap code review found in this same phase's own
// implementation: a follower that catches up via a real InstallSnapshot
// (because the leader has already compacted past the committed
// SetClusterVersionCommand log entry) never replays that entry itself
// via applyControlEntry — the only place the original implementation
// durably persisted the finalized generation to WAL metadata. Without
// Node.adoptClusterGeneration also being called from
// handleInstallSnapshot, such a follower's in-memory FSM would
// correctly report the finalized generation (it comes along for free
// inside the installed snapshot's own state, per
// fsm.EncodeState/DecodeState) while its WAL metadata silently stayed
// at generation 0 forever — defeating wal.Open's ErrUnsupportedGeneration
// rollback-refusal check for that specific node on a future restart
// with an older binary. This mirrors TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction's
// exact isolate/compact/heal shape, but checks fnode.walog.Metadata()
// directly (this package's own tests already have that access) rather
// than only Status(), which cannot distinguish "persisted to WAL
// metadata" from "merely reflected by the in-memory FSM."
func TestFinalizeUpgrade_FollowerAdoptsGenerationViaSnapshotInstall(t *testing.T) {
	const threshold = 3
	const numKeys = 9
	tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	// Isolate the follower BEFORE finalize is ever proposed, so it has
	// no way to ever learn the finalized generation via live replication
	// (applyControlEntry) at all — the installed snapshot below must be
	// the only path. (Precheck can still see this follower as
	// Known/ready: it exchanged RequestVote traffic with the leader
	// during the bootstrap election above, and PeerGenerationInfo has no
	// recency requirement — see UpgradePrecheck's own doc comment.)
	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			follower = id
			break
		}
	}
	tc.isolate(follower)

	awaitCondition(t, 5*time.Second, "precheck reports Ready despite the isolated follower", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := leader.UpgradePrecheck(ctx)
		return err == nil && res.Ready
	})
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := leader.FinalizeUpgrade(ctx); err != nil {
			t.Fatalf("FinalizeUpgrade: %v", err)
		}
	}
	awaitCondition(t, 5*time.Second, "leader converges on ClusterGeneration=1", func() bool {
		return leader.Status().ClusterGeneration == version.MaxSupportedGeneration
	})
	if got := tc.node(follower).Status().ClusterGeneration; got != 0 {
		t.Fatalf("isolated follower's ClusterGeneration = %d, want 0 (it must not have learned about finalize yet)", got)
	}

	outcomes := make([]fsm.Outcome, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k%d", i)
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("r%d", i), uint64(i+1), 0, key, "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
		outcomes[i] = outcome
	}
	last := outcomes[numKeys-1].CommitSeq
	for i := 0; uint64(leader.Status().SnapshotIndex) < last && i < threshold; i++ {
		if _, err := propose(t, leader, cmd(fmt.Sprintf("filler-r%d", i), uint64(numKeys+i+1000), 0, fmt.Sprintf("filler-k%d", i), "v"), 3*time.Second); err != nil {
			t.Fatalf("filler Propose #%d: %v", i, err)
		}
	}
	awaitCondition(t, 3*time.Second, "leader compacts its own log past every proposed key (and the finalize entry) while the follower is isolated", func() bool {
		return uint64(leader.Status().SnapshotIndex) >= last
	})
	snapIndex := uint64(leader.Status().SnapshotIndex)

	tc.heal(follower)
	awaitCondition(t, 5*time.Second, "isolated follower catches up via an installed snapshot", func() bool {
		st := tc.node(follower).Status()
		return uint64(st.SnapshotIndex) == snapIndex && uint64(st.AppliedIndex) >= snapIndex
	})

	fnode := tc.node(follower)
	if got := uint64(fnode.walog.FirstIndex()); got != snapIndex+1 {
		t.Fatalf("follower FirstIndex() after catch-up = %d, want %d — only a genuine InstallSnapshot install ever moves a follower's own boundary this way (confirming this follower never replayed the finalize entry itself)", got, snapIndex+1)
	}
	if got := fnode.Status().ClusterGeneration; got != version.MaxSupportedGeneration {
		t.Fatalf("follower's in-memory ClusterGeneration after snapshot catch-up = %d, want %d", got, version.MaxSupportedGeneration)
	}
	// The critical assertion: WAL metadata itself, not merely the
	// in-memory FSM, must reflect the adopted generation.
	if got := fnode.walog.Metadata().ClusterGeneration; got != version.MaxSupportedGeneration {
		t.Fatalf("follower's WAL metadata ClusterGeneration after snapshot catch-up = %d, want %d — handleInstallSnapshot must durably adopt the installed snapshot's own cluster generation", got, version.MaxSupportedGeneration)
	}
}
