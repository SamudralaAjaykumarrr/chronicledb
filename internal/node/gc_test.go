package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestGCOffByDefault_Gate9 is release gate 9 (docs/v0.6.0-plan.md §31):
// with GCInterval left at its zero value (Config.setDefaults
// deliberately never defaults it — see Config.GCInterval's own doc
// comment), the leader-GC-proposer must never fire at all: zero
// AdvanceGCWatermark proposals, and every replica's FSM.GCWatermark()
// stays at its v0.5.0-identical zero value, over a real write workload
// on a real 3-node cluster.
func TestGCOffByDefault_Gate9(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	for i := 0; i < 20; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("gate9-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil {
			t.Fatalf("Propose #%d: %v", i, err)
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome = %+v, want Committed", i, outcome)
		}
	}

	// Bounded observation window for an absence, not a correctness-
	// masking sleep: gcIntervalTicks==0 means tick() (10ms/tick here)
	// never even evaluates maybeProposeGC (see tick's own doc comment),
	// so 300ms/30 ticks is ample to catch a regression that made it fire
	// even once, while adding no flakiness of its own.
	time.Sleep(300 * time.Millisecond)

	for _, id := range tc.ids {
		n := tc.node(id)
		if got := n.Metrics().GCProposalsTotal; got != 0 {
			t.Fatalf("node %s: GCProposalsTotal = %d, want 0 (GC must be off by default)", id, got)
		}
		if got := n.Metrics().GCProposalsFailedTotal; got != 0 {
			t.Fatalf("node %s: GCProposalsFailedTotal = %d, want 0", id, got)
		}
		if w := n.FSM().GCWatermark(); w != 0 {
			t.Fatalf("node %s: FSM.GCWatermark() = %d, want 0 (v0.5.0-identical behavior with GC disabled)", id, w)
		}
		if s := n.FSM().Store().GCWatermark(); s != 0 {
			t.Fatalf("node %s: Store.GCWatermark() = %d, want 0", id, s)
		}
	}
}

// gcTuningOverride returns an admissionOverride that arms the leader-GC-
// proposer aggressively (short interval, no retain/advance slack, small
// per-pass budgets so a realistic write workload forces several
// continuation passes rather than reclaiming everything in one).
func gcTuningOverride(interval time.Duration) func(*Config) {
	return func(cfg *Config) {
		cfg.GCInterval = interval
		cfg.GCMinRetainSeqs = 0
		// GCMinAdvanceSeqs must stay strictly nonzero: every applied
		// AdvanceGCWatermark entry itself advances appliedIndex by 1
		// (it is a committed log entry like any other), so with 0 slack
		// "w > current+0" is retripped by the GC mechanism's own
		// footprint and it never quiesces — a real self-sustaining-loop
		// hazard the production default (256, cmd/chronicledb-node's
		// -gc-min-advance-seqs) already guards against; this is just a
		// much smaller nonzero margin so tests converge quickly.
		cfg.GCMinAdvanceSeqs = 10
		cfg.GCMaxVersionsPerPass = 4
		cfg.GCMaxKeysPerPass = 4
	}
}

// TestGCAdvancesAndReclaimsUnderLoad is the basic end-to-end proof that,
// once armed, the leader-GC-proposer actually drives FSM.GCWatermark
// forward and every replica converges on the identical value once its
// AdvanceGCWatermark entries replicate — the positive twin of gate 9's
// negative control, exercised through the real leader-proposer/tick/
// Propose/Apply path rather than fsm package's own synthetic pokes
// (internal/fsm/gc_test.go covers ApplyAdvanceGCWatermark's own
// mechanics in isolation; this proves the node-level wiring around it).
func TestGCAdvancesAndReclaimsUnderLoad(t *testing.T) {
	tc := newTestClusterWithAdmissionOverride(t, 3, gcTuningOverride(15*time.Millisecond))
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	// Many distinct keys, each with StartSeq=MaxUint64 (mustCommitKey's
	// own convention, internal/fsm/gc_test.go) so every write commits
	// regardless of concurrent commits to other keys — this test wants
	// applied-index churn to drive GC, not to exercise SI conflicts.
	const n = 60
	for i := 0; i < n; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("load-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil {
			t.Fatalf("Propose #%d: %v", i, err)
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome = %+v, want Committed", i, outcome)
		}
	}

	awaitCondition(t, 5*time.Second, "leader GC watermark advances past zero", func() bool {
		return leader.FSM().GCWatermark() > 0
	})
	if got := leader.Metrics().GCProposalsTotal; got == 0 {
		t.Fatalf("leader: GCProposalsTotal = 0, want > 0 once armed")
	}

	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" converges on the leader's GC watermark", func() bool {
			return tc.node(id).FSM().GCWatermark() == leader.FSM().GCWatermark()
		})
		n := tc.node(id)
		if got, want := n.FSM().Store().GCWatermark(), n.FSM().GCWatermark(); got != want {
			t.Fatalf("node %s: Store.GCWatermark()=%d != FSM.GCWatermark()=%d (docs/v0.6.0-plan.md §15.2b equality)", id, got, want)
		}
	}
}

// TestReadLeaseFloorsGCWatermark_SL7 is SL-7 (docs/v0.6.0-plan.md §30):
// a transaction's live read lease must floor the leader's proposed GC
// watermark below its own StartSeq for as long as it is held, and once
// released and the watermark has genuinely advanced past it, a fresh
// proposal presented with that now-stale StartSeq must be rejected with
// StatusAbortedStale identically on every replica (a deterministic Apply
// consequence of the replicated gcWatermark field, not a per-node
// heuristic).
func TestReadLeaseFloorsGCWatermark_SL7(t *testing.T) {
	tc := newTestClusterWithAdmissionOverride(t, 3, gcTuningOverride(15*time.Millisecond))
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	startSeq, lease, err := leader.BeginReadIndex(ctx)
	if err != nil {
		t.Fatalf("BeginReadIndex: %v", err)
	}
	if got := leader.Metrics().ReadLeasesActiveGauge; got < 1 {
		t.Fatalf("ReadLeasesActiveGauge = %d, want >= 1 while the lease is live", got)
	}

	// Drive enough applied-index churn, across many distinct keys, that
	// an unfloored watermark would certainly have overtaken startSeq —
	// far more than gcTuningOverride's per-pass budget of 4 so multiple
	// GC passes are forced to actually run and observe the live lease
	// repeatedly, not just once.
	const n = 120
	for i := 0; i < n; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("sl7-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil {
			t.Fatalf("Propose #%d: %v", i, err)
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome = %+v, want Committed", i, outcome)
		}
	}
	// Bounded window to let several GC ticks elapse while the lease
	// remains live; this is checking an absence (watermark must NOT
	// cross startSeq), so — like gate 9's own check — a sleep here is
	// the correct tool, not a substitute for a polled positive
	// assertion.
	time.Sleep(300 * time.Millisecond)
	if w := leader.FSM().GCWatermark(); w > startSeq {
		t.Fatalf("GC watermark %d advanced past the live lease's StartSeq %d — a snapshot this transaction may still read was reclaimed out from under it", w, startSeq)
	}

	lease.Release()
	awaitCondition(t, 5*time.Second, "GC watermark advances past the released lease's StartSeq", func() bool {
		return leader.FSM().GCWatermark() > startSeq
	})
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" observes the released lease in ReadLeasesActiveGauge", func() bool {
			return tc.node(id).Metrics().ReadLeasesActiveGauge == 0
		})
	}

	// A proposal presented with the now-stale startSeq must be refused,
	// deterministically, everywhere.
	staleID := fsm.RequestID("sl7-stale-probe")
	outcome, err := propose(t, leader, cmd(string(staleID), 999999, startSeq, "kstale", "v"), 3*time.Second)
	if err != nil {
		t.Fatalf("Propose(stale): %v", err)
	}
	if outcome.Status != fsm.StatusAbortedStale {
		t.Fatalf("leader outcome = %+v, want StatusAbortedStale", outcome)
	}
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, "node "+string(id)+" applies the identical AbortedStale outcome", func() bool {
			oc, ok := tc.node(id).FSM().GetOutcome(staleID)
			return ok && oc.Status == fsm.StatusAbortedStale
		})
	}
}

// TestGCStateSurvivesLogReplayAndInstallSnapshotIdentically is SL-13
// (docs/v0.6.0-plan.md §30): a follower that catches up on GC state by
// ordinary log replay (replaying committed AdvanceGCWatermark entries
// one at a time) and one that catches up via a peer's InstallSnapshot
// (the leader having already compacted those same entries away) must
// converge on byte-identical FSM.EncodeState() output — including
// gcWatermark/gcCursor/gcPasses/gcPassSeq, since generation-3's snapshot
// trailing block encodes all four (internal/fsm/snapshot.go) — and both
// must independently satisfy Store.GCWatermark() == FSM.gcWatermark
// (§15.2b).
func TestGCStateSurvivesLogReplayAndInstallSnapshotIdentically(t *testing.T) {
	// snapshotThreshold small enough that a modest write volume forces a
	// real local snapshot+compaction cycle on the leader, which is what
	// makes InstallSnapshot (rather than ordinary catch-up replication)
	// the only path left for a sufficiently-isolated follower.
	const threshold = 8
	tc := newTestClusterWithSnapshotThresholdAndAdmissionOverride(t, 3, threshold, gcTuningOverride(15*time.Millisecond))
	leaderID := tc.awaitLeader(10 * time.Second)
	leader := tc.node(leaderID)
	mustFinalizeToMax(t, tc, leader)

	var replayFollower, snapshotFollower raft.NodeID
	for _, id := range tc.ids {
		if id == leaderID {
			continue
		}
		if replayFollower == "" {
			replayFollower = id
		} else {
			snapshotFollower = id
		}
	}

	// Isolate snapshotFollower before driving any further writes, so
	// every entry and every local snapshot the leader creates from here
	// on is something it can only ever learn via a later InstallSnapshot
	// — never ordinary log replication — while replayFollower keeps
	// replaying the log normally the whole time.
	tc.isolate(snapshotFollower)

	const n = 3 * threshold
	for i := 0; i < n; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("sl13-%d", i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil {
			t.Fatalf("Propose #%d: %v", i, err)
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome = %+v, want Committed", i, outcome)
		}
	}

	awaitCondition(t, 10*time.Second, "leader GC watermark advances", func() bool {
		return leader.FSM().GCWatermark() > 0
	})
	awaitCondition(t, 10*time.Second, "leader creates a real local snapshot", func() bool {
		return uint64(leader.Status().SnapshotIndex) > 0
	})
	awaitCondition(t, 10*time.Second, "replay follower converges on the leader's GC watermark", func() bool {
		return tc.node(replayFollower).FSM().GCWatermark() == leader.FSM().GCWatermark()
	})

	tc.heal(snapshotFollower)
	awaitCondition(t, 10*time.Second, "isolated follower catches up via a real InstallSnapshot", func() bool {
		return uint64(tc.node(snapshotFollower).Status().SnapshotIndex) == uint64(leader.Status().SnapshotIndex) && leader.Status().SnapshotIndex > 0
	})
	if got := tc.node(snapshotFollower).Metrics().SnapshotsInstalledTotal; got == 0 {
		t.Fatalf("snapshotFollower: SnapshotsInstalledTotal = 0, want > 0 (test setup: it must have caught up via InstallSnapshot, not replay)")
	}
	awaitCondition(t, 10*time.Second, "isolated follower converges on the leader's GC watermark", func() bool {
		return tc.node(snapshotFollower).FSM().GCWatermark() == leader.FSM().GCWatermark()
	})

	// Let the cluster fully quiesce on one applied index before taking
	// the byte-comparison snapshot, so the assertion below compares
	// three settled states rather than racing an in-flight proposal.
	awaitCondition(t, 10*time.Second, "cluster quiesces on one applied index", func() bool {
		li := leader.Status().AppliedIndex
		return tc.node(replayFollower).Status().AppliedIndex == li && tc.node(snapshotFollower).Status().AppliedIndex == li
	})

	leaderState := leader.FSM().EncodeState()
	replayState := tc.node(replayFollower).FSM().EncodeState()
	snapshotState := tc.node(snapshotFollower).FSM().EncodeState()

	if string(replayState) != string(leaderState) {
		t.Fatalf("replay-follower FSM.EncodeState() diverges from leader's (len %d vs %d)", len(replayState), len(leaderState))
	}
	if string(snapshotState) != string(leaderState) {
		t.Fatalf("snapshot-follower FSM.EncodeState() diverges from leader's (len %d vs %d)", len(snapshotState), len(leaderState))
	}

	for _, id := range []raft.NodeID{leaderID, replayFollower, snapshotFollower} {
		n := tc.node(id)
		if got, want := n.FSM().Store().GCWatermark(), n.FSM().GCWatermark(); got != want {
			t.Fatalf("node %s: Store.GCWatermark()=%d != FSM.GCWatermark()=%d (docs/v0.6.0-plan.md §15.2b equality)", id, got, want)
		}
	}
}
