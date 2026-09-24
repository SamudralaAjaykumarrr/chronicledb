package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestSL12_CombinedChaosWithGCActive is SL-12 (docs/v0.6.0-plan.md
// §30.2, §29.3's testCluster-tier substitution for internal/fault's
// GC-less simulator): GC runs continuously, actively reclaiming, while
// a follower repeatedly crashes/restarts and a separate follower is
// repeatedly partitioned and healed, all against a bounded, heavily
// overwritten key set — GC SAFETY and GC DETERMINISM are checked after
// every disruptive action: every live node's FSM-encoded state and
// applied GC watermark converge to byte-identical/equal once the
// cluster quiesces, and §15.2b's Store.GCWatermark()==FSM.GCWatermark()
// equality holds on every node throughout.
func TestSL12_CombinedChaosWithGCActive(t *testing.T) {
	tc := newTestClusterWithAdmissionOverride(t, 3, gcTuningOverride(15*time.Millisecond))
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	var flapper, partitioned raft.NodeID
	for _, id := range tc.ids {
		if id == leader.cfg.ID {
			continue
		}
		if flapper == "" {
			flapper = id
		} else {
			partitioned = id
		}
	}

	// checkConverged polls the WHOLE comparison (applied index, encoded
	// state, and GC watermark, across every node) as one atomic
	// condition, retrying until a single instant sees them all agree —
	// gcTuningOverride's 15ms GC interval keeps the leader proposing in
	// the background even at rest, so a two-step "wait for applied-index
	// equality, THEN separately re-read state" check is a genuine TOCTOU
	// race (the leader can advance between the two steps); only a
	// same-iteration, all-fresh-reads comparison is race-free, and it
	// converges once GC and the earlier round's writes actually
	// quiesce (maybeProposeGC's own !advance && !cont guard stops
	// proposing once there is nothing left to do).
	checkConverged := func(label string) {
		t.Helper()
		var mismatch string
		ok := false
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			leader = tc.leaderNode(5 * time.Second)
			wantIndex := leader.Status().AppliedIndex
			wantState := string(leader.FSM().EncodeState())
			wantWatermark := leader.FSM().GCWatermark()
			mismatch = ""
			for _, id := range tc.ids {
				n := tc.node(id)
				if n.Status().AppliedIndex != wantIndex {
					mismatch = fmt.Sprintf("node %s applied index %d != leader %d", id, n.Status().AppliedIndex, wantIndex)
					break
				}
				if got := string(n.FSM().EncodeState()); got != wantState {
					mismatch = fmt.Sprintf("node %s FSM-encoded state diverges from leader at applied index %d (GC DETERMINISM)", id, wantIndex)
					break
				}
				if got := n.FSM().GCWatermark(); got != wantWatermark {
					mismatch = fmt.Sprintf("node %s GCWatermark=%d, want %d (leader's)", id, got, wantWatermark)
					break
				}
				if got, want := n.FSM().Store().GCWatermark(), n.FSM().GCWatermark(); got != want {
					mismatch = fmt.Sprintf("node %s Store.GCWatermark()=%d != FSM.GCWatermark()=%d (§15.2b)", id, got, want)
					break
				}
			}
			if mismatch == "" {
				ok = true
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("%s: cluster never reached a stable converged instant within 10s: %s", label, mismatch)
		}
	}

	const keys = 8
	round := 0
	overwrite := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			round++
			key := fmt.Sprintf("k%d", round%keys)
			reqID := fmt.Sprintf("sl12-%d", round)
			outcome, err := propose(t, leader, cmd(reqID, uint64(round), ^uint64(0), key, "v"), 3*time.Second)
			if err != nil {
				t.Fatalf("propose #%d: %v", round, err)
			}
			if outcome.Status != fsm.StatusCommitted && outcome.Status != fsm.StatusAborted {
				t.Fatalf("propose #%d: outcome = %+v, want Committed or Aborted (never AbortedStale here — no reader ever holds a lease across GC in this test)", round, outcome)
			}
		}
	}

	overwrite(40)
	checkConverged("after warmup")

	// Crash/restart a follower while GC keeps running and writes continue.
	tc.crash(flapper)
	overwrite(40)
	restarted := tc.restart(flapper)
	awaitCondition(t, 5*time.Second, "restarted follower catches up", func() bool {
		return restarted.Status().AppliedIndex >= leader.Status().AppliedIndex
	})
	checkConverged("after crash/restart")

	// Partition/heal a different follower while GC keeps running and
	// writes continue.
	tc.isolate(partitioned)
	overwrite(40)
	tc.heal(partitioned)
	awaitCondition(t, 5*time.Second, "healed follower catches up", func() bool {
		return tc.node(partitioned).Status().AppliedIndex >= leader.Status().AppliedIndex
	})
	checkConverged("after partition/heal")

	overwrite(40)
	checkConverged("final")

	if got := leader.Metrics().GCProposalsTotal; got == 0 {
		t.Fatal("GCProposalsTotal = 0 across the whole run, want > 0 — GC was never actually exercised")
	}
	if got := leader.FSM().GCPasses(); got == 0 {
		t.Fatal("GCPasses() = 0, want at least one completed full-keyspace walk")
	}
}
