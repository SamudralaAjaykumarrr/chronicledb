package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	awaitPrecheckReady(t, leader, "every node runs this same binary and all are reachable")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pre, err := leader.UpgradePrecheck(ctx)
	if err != nil {
		t.Fatalf("UpgradePrecheck: %v", err)
	}
	if pre.TargetGeneration != pre.LocalClusterGeneration+1 {
		t.Fatalf("TargetGeneration = %d, want LocalClusterGeneration+1 = %d (single-step N/N+1-only policy)", pre.TargetGeneration, pre.LocalClusterGeneration+1)
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
	_, _, ferr := follower.FinalizeUpgrade(ctx)
	var nle *NotLeaderError
	if !errors.As(ferr, &nle) {
		t.Fatalf("follower FinalizeUpgrade: err = %v, want *NotLeaderError", ferr)
	}

	outcome, _, err := leader.FinalizeUpgrade(ctx)
	if err != nil {
		t.Fatalf("FinalizeUpgrade: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("FinalizeUpgrade outcome = %+v, want Committed", outcome)
	}

	// The N/N+1-only policy means one FinalizeUpgrade call advances the
	// cluster by exactly one generation; repeat until this binary's own
	// MaxSupportedGeneration is reached (2 calls today).
	finalizeToMax(t, leader, ctx)

	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" converges on ClusterGeneration=max", func() bool {
			return tc.node(id).Status().ClusterGeneration == version.MaxSupportedGeneration
		})
	}

	// Idempotent retry: finalize again once already at max.
	if _, _, err := leader.FinalizeUpgrade(ctx); !errors.Is(err, ErrAlreadyFinalized) {
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

	awaitPrecheckReady(t, leader, "every node runs this same binary and all are reachable")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finalizeToMax(t, leader, ctx)
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" converges on ClusterGeneration=max", func() bool {
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
	awaitCondition(t, 5*time.Second, "restarted node recovers ClusterGeneration=max from local durable state", func() bool {
		return restarted.Status().ClusterGeneration == version.MaxSupportedGeneration
	})
}

// describePrecheckPeers renders a PrecheckResult's per-peer
// Known/Generation view for a failure message, so a genuine
// failure-to-become-ready names the peer responsible instead of only
// reporting that a deadline passed.
func describePrecheckPeers(res PrecheckResult) string {
	ids := make([]string, 0, len(res.Peers))
	for id := range res.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var b strings.Builder
	fmt.Fprintf(&b, "target=%d local=%d peers=[", res.TargetGeneration, res.LocalClusterGeneration)
	for i, id := range ids {
		if i > 0 {
			b.WriteString(" ")
		}
		info := res.Peers[id]
		if info.Known {
			fmt.Fprintf(&b, "%s{gen=%d role=%s}", id, info.Generation, info.Role)
		} else {
			fmt.Fprintf(&b, "%s{UNKNOWN role=%s}", id, info.Role)
		}
	}
	b.WriteString("]")
	return b.String()
}

// precheckNow runs one UpgradePrecheck against leader. UpgradePrecheck
// dispatches through Node.run's event loop, so its answer is serialized
// against every other event-loop operation rather than read from cached
// state.
func precheckNow(t *testing.T, leader *Node) PrecheckResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := leader.UpgradePrecheck(ctx)
	if err != nil {
		t.Fatalf("UpgradePrecheck: %v", err)
	}
	return res
}

// awaitPrecheckReady polls until leader's precheck reports Ready, and
// names the peer that never became ready if it does not.
//
// What is actually being waited for is narrow and worth stating,
// because getting it wrong produced a real -race flake in this package:
// Ready requires the leader to have recorded a compatibility generation
// for every voter peer, and Node.recordPeerGeneration only ever learns
// one from a raft.Message this leader RECEIVES. A peer the leader has
// never heard from is Known=false, and UpgradePrecheck deliberately has
// no recency requirement in the other direction either — once recorded,
// a generation is never un-recorded.
//
// Both halves matter for any test that isolates a node:
//   - isolate a peer BEFORE the leader has received anything from it and
//     Ready is false PERMANENTLY, not slowly. No test deadline fixes
//     that, and raising one only converts a deterministic ordering bug
//     into a slower flake. Call this helper before isolating.
//   - isolate a peer AFTER Ready holds and Ready keeps holding
//     immediately, with no polling, because nothing un-records it. Use
//     requirePrecheckReadyNow for that assertion.
//
// The bound stays hard: this never retries forever and never sleeps in
// place of a condition.
func awaitPrecheckReady(t *testing.T, leader *Node, why string) {
	t.Helper()
	var last PrecheckResult
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		last = precheckNow(t, leader)
		if last.Ready && !last.AlreadyFinalized {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("precheck never reported Ready within 10s (%s): %s", why, describePrecheckPeers(last))
}

// requirePrecheckReadyNow asserts Ready on a single, immediate precheck
// — no polling. Used after isolating a peer whose generation the leader
// has already recorded: "Ready survives isolation" is a statement about
// PeerGenerationInfo having no recency requirement, so it must hold at
// once. Polling here would let a genuine regression pass by waiting for
// some unrelated later event.
func requirePrecheckReadyNow(t *testing.T, leader *Node, why string) {
	t.Helper()
	res := precheckNow(t, leader)
	if !res.Ready {
		t.Fatalf("precheck must be Ready immediately (%s): %s", why, describePrecheckPeers(res))
	}
}

// finalizeToMax repeatedly calls FinalizeUpgrade against leader until it
// reports ErrAlreadyFinalized, accommodating the N/N+1-only single-step
// policy (fsm.ApplySetClusterVersion) when this binary's
// MaxSupportedGeneration is more than one generation above a fresh
// cluster's starting generation 0.
func finalizeToMax(t *testing.T, leader *Node, ctx context.Context) {
	t.Helper()
	for i := 0; i < int(version.MaxSupportedGeneration)+1; i++ {
		_, _, err := leader.FinalizeUpgrade(ctx)
		if errors.Is(err, ErrAlreadyFinalized) {
			return
		}
		if err != nil {
			t.Fatalf("FinalizeUpgrade step %d: %v", i, err)
		}
	}
	t.Fatalf("finalizeToMax: did not reach ErrAlreadyFinalized within %d steps", version.MaxSupportedGeneration+1)
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

	// Wait for Ready BEFORE isolating anything. Ready means the leader
	// has recorded a generation for every voter peer, and a generation
	// is only ever learned from a message this leader RECEIVES — so
	// isolating a peer first can permanently strand it at Known=false
	// and make this wait unsatisfiable rather than merely slow (see
	// awaitPrecheckReady's doc comment; that ordering was a real -race
	// flake here).
	awaitPrecheckReady(t, leader, "before isolating any follower")

	// Isolate the follower BEFORE finalize is ever proposed, so it has
	// no way to ever learn the finalized generation via live replication
	// (applyControlEntry) at all — the installed snapshot below must be
	// the only path.
	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			follower = id
			break
		}
	}
	tc.isolate(follower)

	// Precheck still sees the now-unreachable follower as Known/ready,
	// and sees it IMMEDIATELY: PeerGenerationInfo has no recency
	// requirement, so nothing un-records a generation already recorded
	// (see UpgradePrecheck's own doc comment). Asserted without polling
	// precisely so a regression in that property fails here rather than
	// being waited out.
	requirePrecheckReadyNow(t, leader, "an isolated peer's already-recorded generation must not be un-recorded")
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		finalizeToMax(t, leader, ctx)
	}
	awaitCondition(t, 5*time.Second, "leader converges on ClusterGeneration=max", func() bool {
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

// TestUpgradePrecheckPeerGenerationIsLearnedOnlyFromReceivedMessages
// pins the two facts awaitPrecheckReady/requirePrecheckReadyNow depend
// on, so that a change to either (say, adding a recency expiry to
// PeerGenerationInfo, or inferring a generation from something other
// than a received message) fails here loudly instead of resurfacing as
// an intermittent -race flake in whichever test happens to isolate a
// node at the wrong moment.
//
// Both halves are asserted deterministically, with no reliance on how
// long anything takes.
func TestUpgradePrecheckPeerGenerationIsLearnedOnlyFromReceivedMessages(t *testing.T) {
	t.Run("a peer the leader has never received a message from stays Unknown", func(t *testing.T) {
		tc := newTestCluster(t, 3)
		// Isolated before any election completes. A three-node cluster
		// elects on two votes, so n1/n2 still form a leader and keep
		// replicating to each other without ever hearing from n3.
		tc.isolate("n3")

		var leader *Node
		awaitCondition(t, 10*time.Second, "a leader emerges among the two reachable nodes", func() bool {
			for _, id := range []raft.NodeID{"n1", "n2"} {
				if tc.node(id).Status().Role == raft.Leader {
					leader = tc.node(id)
					return true
				}
			}
			return false
		})

		// A committed proposal proves full replication rounds have
		// completed between the reachable nodes: if a peer's generation
		// were learnable from anything other than a message received
		// from that peer, n3's would be known by now.
		outcome, err := propose(t, leader, cmd("gen-probe", 1, 0, "k", "v"), 5*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose: outcome=%+v err=%v", outcome, err)
		}

		res := precheckNow(t, leader)
		if info, ok := res.Peers["n3"]; !ok || info.Known {
			t.Fatalf("n3 must be present and Known=false: %s", describePrecheckPeers(res))
		}
		if res.Ready {
			t.Fatalf("precheck must not be Ready while a voter peer's capability is unknown and unreachable: %s", describePrecheckPeers(res))
		}
		// This state is permanent, not slow — which is why every test
		// that isolates a node must reach Ready BEFORE doing so. The
		// reachable peer is Known, isolating the assertion to n3.
		if info, ok := res.Peers[string(leaderPeerOf(t, tc, leader))]; !ok || !info.Known {
			t.Fatalf("the reachable peer must be Known: %s", describePrecheckPeers(res))
		}
	})

	t.Run("an already-recorded generation survives that peer's isolation", func(t *testing.T) {
		tc := newTestCluster(t, 3)
		leaderID := tc.awaitLeader(5 * time.Second)
		leader := tc.node(leaderID)
		awaitPrecheckReady(t, leader, "all nodes reachable")

		var follower raft.NodeID
		for _, id := range tc.ids {
			if id != leaderID {
				follower = id
				break
			}
		}
		tc.isolate(follower)

		// Immediately, with no polling: PeerGenerationInfo has no
		// recency requirement, so isolation cannot un-record it.
		requirePrecheckReadyNow(t, leader, "isolation must not un-record a generation")
	})
}

// leaderPeerOf returns the one node that is neither leader nor the
// deliberately-isolated "n3", for the reachable-peer assertion above.
func leaderPeerOf(t *testing.T, tc *testCluster, leader *Node) raft.NodeID {
	t.Helper()
	for _, id := range tc.ids {
		if id != leader.cfg.ID && id != "n3" {
			return id
		}
	}
	t.Fatal("no reachable peer found")
	return ""
}
