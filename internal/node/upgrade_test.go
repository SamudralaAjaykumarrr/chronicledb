package node

import (
	"context"
	"errors"
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
