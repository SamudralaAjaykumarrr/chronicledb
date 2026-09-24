//go:build faulttest

package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestCrashInjection_MaybeSnapshotSixPoints_SL8 is SL-8
// (docs/v0.6.0-plan.md §22, §30 proof matrix: `RECLAMATION BOUNDARY`):
// a real crash injected at each of maybeSnapshot's six ordering points
// (§17.2) must always be recoverable, with no committed entry ever
// lost, exactly as §22's table predicts for each point. A single-node
// cluster is sufficient — the property under test is this node's own
// local durable-state recovery, not cluster consensus.
func TestCrashInjection_MaybeSnapshotSixPoints_SL8(t *testing.T) {
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
			const threshold = 4
			tc := newTestClusterWithSnapshotThreshold(t, 1, threshold)
			leaderID := tc.awaitLeader(10 * time.Second)
			leader := tc.node(leaderID)

			// Commit a batch BEFORE arming the crash — these are the
			// entries RECLAMATION BOUNDARY says must never be lost,
			// regardless of what happens to the in-progress snapshot
			// cycle the crash below interrupts.
			type kv struct{ key, value string }
			var committed []kv
			for i := 0; i < threshold; i++ {
				key, value := fmt.Sprintf("pre-%d", i), fmt.Sprintf("v%d", i)
				outcome, err := propose(t, leader, cmd(fmt.Sprintf("sl8-pre-%d-%d", p, i), uint64(i), ^uint64(0), key, value), 3*time.Second)
				if err != nil || outcome.Status != fsm.StatusCommitted {
					t.Fatalf("pre-crash Propose #%d: outcome=%+v err=%v", i, outcome, err)
				}
				committed = append(committed, kv{key, value})
			}

			fired := armOnce(t, p)
			// Push appliedIndex-snapIdx past threshold again so
			// maybeSnapshot actually runs and reaches p.
			for i := 0; i < 3*threshold; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := leader.Propose(ctx, cmd(fmt.Sprintf("sl8-fill-%d-%d", p, i), uint64(1000+i), ^uint64(0), fmt.Sprintf("fill-%d", i), "v"))
				cancel()
				if err != nil {
					break // the crash may itself have interrupted this call
				}
			}
			awaitFired(t, fired, 5*time.Second, p)
			awaitNodeStopped(t, leader, 3*time.Second)
			delete(tc.nodes, leaderID)

			restarted := tc.restart(leaderID)
			if err := restarted.Err(); err != nil {
				t.Fatalf("restarted node reports a fatal error: %v", err)
			}
			newLeaderID := tc.awaitLeader(10 * time.Second)
			newLeader := tc.node(newLeaderID)

			store := newLeader.FSM().Store()
			for _, e := range committed {
				value, found, err := store.Visible(e.key, ^uint64(0))
				if err != nil {
					t.Fatalf("Visible(%q) after restart: %v", e.key, err)
				}
				if !found {
					t.Fatalf("RECLAMATION BOUNDARY violated: pre-crash committed key %q missing after a crash at FaultPoint %d and restart", e.key, p)
				}
				if string(value) != e.value {
					t.Fatalf("key %q = %q after restart, want %q (crash at FaultPoint %d)", e.key, value, e.value, p)
				}
			}

			// The node must still work normally afterward.
			outcome, err := propose(t, newLeader, cmd(fmt.Sprintf("sl8-post-%d", p), 999999, ^uint64(0), "post-key", "post-val"), 3*time.Second)
			if err != nil || outcome.Status != fsm.StatusCommitted {
				t.Fatalf("post-restart Propose: outcome=%+v err=%v", outcome, err)
			}
		})
	}
}

// TestCrashInjection_HandleInstallSnapshotThreePoints_SL26 is SL-26
// (docs/v0.6.0-plan.md §22, §30): a real crash injected at each of
// handleInstallSnapshot's three points, during a learner's real
// InstallSnapshot catch-up, must always be recoverable — the restarted
// node converges byte-identically with the rest of the cluster
// (FSM.EncodeState) and satisfies the §15.2b equality
// (Store.GCWatermark() == FSM.gcWatermark) at every restart. GC is
// armed (not left at its off-by-default zero value) so the §15.2b
// check is meaningful rather than a trivial 0==0.
func TestCrashInjection_HandleInstallSnapshotThreePoints_SL26(t *testing.T) {
	points := []FaultPoint{
		FaultBeforeInstallSnapshotStorage,
		FaultAfterInstallSnapshotBeforeFSMSwap,
		FaultAfterFSMSwapBeforeGenerationAdopt,
	}
	for _, p := range points {
		p := p
		t.Run(fmt.Sprintf("point_%d", p), func(t *testing.T) {
			const threshold = 5
			tc := newTestClusterWithSnapshotThresholdAndAdmissionOverride(t, 3, threshold, gcTuningOverride(15*time.Millisecond))
			leaderID := tc.awaitLeader(10 * time.Second)
			leader := tc.node(leaderID)
			mustFinalizeToMax(t, tc, leader)

			// Freeze every node's election clock for the rest of this
			// subtest: it drives a real, several-second sequence
			// (isolate, crash-inject, restart, re-converge) over real
			// disk/TCP, and this test's own assertions assume `leader`
			// stays leader throughout — a spontaneous re-election
			// elsewhere in that window (the same real-scheduling-under-
			// load hazard mustFinalizeToMax's own doc comment describes,
			// observed directly here too) would silently invalidate the
			// later "post-restart Propose against `leader`" step.
			// Heartbeat ticks keep running, so replication and the
			// restarted follower's catch-up are unaffected.
			tc.pauseTicking()
			defer tc.resumeTicking()

			var follower raft.NodeID
			for _, id := range tc.ids {
				if id != leaderID {
					follower = id
					break
				}
			}

			tc.isolate(follower)
			for i := 0; i < 4*threshold; i++ {
				outcome, err := propose(t, leader, cmd(fmt.Sprintf("sl26-%d-%d", p, i), uint64(i), ^uint64(0), fmt.Sprintf("k%d", i), "v"), 3*time.Second)
				if err != nil || outcome.Status != fsm.StatusCommitted {
					t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
				}
			}
			awaitCondition(t, 10*time.Second, "leader GC watermark advances", func() bool {
				return leader.FSM().GCWatermark() > 0
			})
			awaitCondition(t, 10*time.Second, "leader creates a real local snapshot", func() bool {
				return uint64(leader.Status().SnapshotIndex) > 0
			})

			followerNode := tc.node(follower)
			fired := armOnce(t, p)
			tc.heal(follower)

			awaitFired(t, fired, 10*time.Second, p)
			awaitNodeStopped(t, followerNode, 3*time.Second)
			delete(tc.nodes, follower)

			restarted := tc.restart(follower)
			if err := restarted.Err(); err != nil {
				t.Fatalf("restarted follower reports a fatal error: %v", err)
			}

			// Let the cluster fully re-converge before the byte-comparison
			// below. A two-step "wait for applied-index/GC-watermark
			// equality, THEN separately re-read EncodeState()" check is a
			// genuine TOCTOU race here: tc.pauseTicking() only freezes
			// election ticks (node.go's tick() gates the election
			// countdown behind electionTicksPaused, but calls
			// maybeProposeGC unconditionally on its own gcIntervalTicks
			// countdown), so gcTuningOverride's 15ms background GC
			// proposer keeps committing continuation passes throughout —
			// a pass can land on the leader between the equality polls
			// above succeeding and the sequential EncodeState() reads
			// below, before it replicates to the restarted follower.
			// Mirror sl12_gc_chaos_test.go's checkConverged pattern: only
			// a same-iteration, all-fresh-reads comparison is race-free.
			ok := false
			var mismatch string
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				wantIndex := leader.Status().AppliedIndex
				wantState := leader.FSM().EncodeState()
				mismatch = ""
				switch {
				case restarted.Status().AppliedIndex != wantIndex:
					mismatch = fmt.Sprintf("restarted follower applied index not yet settled to leader's %d", wantIndex)
				default:
					if got, want := restarted.FSM().Store().GCWatermark(), restarted.FSM().GCWatermark(); got != want {
						mismatch = fmt.Sprintf("restarted follower: Store.GCWatermark()=%d != FSM.GCWatermark()=%d (docs/v0.6.0-plan.md §15.2b)", got, want)
					} else if got := restarted.FSM().EncodeState(); string(got) != string(wantState) {
						mismatch = fmt.Sprintf("restarted follower's FSM.EncodeState() diverges from the leader's (len %d vs %d)", len(got), len(wantState))
					}
				}
				if mismatch == "" {
					ok = true
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !ok {
				t.Fatalf("cluster never reached a stable converged instant within 20s after a crash at FaultPoint %d: %s", p, mismatch)
			}

			// The node must still work normally afterward: a fresh
			// write must replicate to it.
			outcome, err := propose(t, leader, cmd(fmt.Sprintf("sl26-post-%d", p), 999999, ^uint64(0), "post-key", "post-val"), 3*time.Second)
			if err != nil || outcome.Status != fsm.StatusCommitted {
				t.Fatalf("post-restart Propose: outcome=%+v err=%v", outcome, err)
			}
			awaitCondition(t, 10*time.Second, "restarted follower applies the post-restart write", func() bool {
				return restarted.Status().AppliedIndex >= outcome.CommitSeq
			})
		})
	}
}

// TestCrashInjection_AfterSnapshotPrune is a regression test for the
// real crash-safety bug SL-8 found while this slice was being written:
// Manager used to prune old snapshot files eagerly, inside Create/
// Install themselves, before the caller had durably recorded the new
// snapshot's pointer — so a crash between those two steps deleted the
// still-pointer-named old file while never adopting the new one, an
// unrecoverable gap (docs/recovery.md §4). The fix moved pruning to an
// explicit Manager.Prune call the caller makes only after its own
// pointer-advance step has succeeded (see Prune's own doc comment) —
// this proves a crash AT that new, later point (FaultAfterSnapshotPrune)
// is safe, which is what makes the fix's own safety claim true rather
// than merely asserted.
func TestCrashInjection_AfterSnapshotPrune(t *testing.T) {
	const threshold = 4
	tc := newTestClusterWithSnapshotThreshold(t, 1, threshold)
	leaderID := tc.awaitLeader(10 * time.Second)
	leader := tc.node(leaderID)

	type kv struct{ key, value string }
	var committed []kv
	for i := 0; i < threshold; i++ {
		key, value := fmt.Sprintf("pre-%d", i), fmt.Sprintf("v%d", i)
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("prune-pre-%d", i), uint64(i), ^uint64(0), key, value), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("pre-crash Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
		committed = append(committed, kv{key, value})
	}

	fired := armOnce(t, FaultAfterSnapshotPrune)
	for i := 0; i < 3*threshold; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := leader.Propose(ctx, cmd(fmt.Sprintf("prune-fill-%d", i), uint64(1000+i), ^uint64(0), fmt.Sprintf("fill-%d", i), "v"))
		cancel()
		if err != nil {
			break
		}
	}
	awaitFired(t, fired, 5*time.Second, FaultAfterSnapshotPrune)
	awaitNodeStopped(t, leader, 3*time.Second)
	delete(tc.nodes, leaderID)

	restarted := tc.restart(leaderID)
	if err := restarted.Err(); err != nil {
		t.Fatalf("restarted node reports a fatal error: %v", err)
	}
	newLeaderID := tc.awaitLeader(10 * time.Second)
	newLeader := tc.node(newLeaderID)

	store := newLeader.FSM().Store()
	for _, e := range committed {
		value, found, err := store.Visible(e.key, ^uint64(0))
		if err != nil {
			t.Fatalf("Visible(%q) after restart: %v", e.key, err)
		}
		if !found {
			t.Fatalf("RECLAMATION BOUNDARY violated: pre-crash committed key %q missing after a crash at FaultAfterSnapshotPrune and restart", e.key)
		}
		if string(value) != e.value {
			t.Fatalf("key %q = %q after restart, want %q", e.key, value, e.value)
		}
	}

	outcome, err := propose(t, newLeader, cmd("prune-post", 999999, ^uint64(0), "post-key", "post-val"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("post-restart Propose: outcome=%+v err=%v", outcome, err)
	}
}
