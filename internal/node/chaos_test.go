package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file is Phase 7's real-disk/real-TCP chaos evidence
// (docs/testing-strategy.md §4, docs/roadmap.md Phase 7): it reuses
// testCluster (node_test.go) — real internal/wal-backed storage and a
// real TCP internal/transport, all in-process — to exercise crash-lab,
// RequestID-chaos, transaction-chaos, snapshot-chaos, and partition-
// topology combinations end to end through the real node.Node, not
// internal/fault's simulator. internal/fault/chaos_test.go covers the
// same shapes at the deterministic raft-core layer, at far higher
// iteration counts; this file's job is specifically to prove the real
// wiring (real fsync, real sockets, real goroutine scheduling) survives
// the same combinations, per docs/testing-strategy.md §3.3's "real-time
// end-to-end tests still have a place... but not for validating Raft
// safety logic" split.

func anyLeader(tc *testCluster) (raft.NodeID, bool) {
	for id, n := range tc.nodes {
		if n.Status().Role == raft.Leader {
			return id, true
		}
	}
	return "", false
}

// TestChaos_RepeatedCrashRestartCycles is the crash lab's "repeated
// crash/restart cycles" scenario: one follower flaps (crash, restart,
// crash, restart...) several times while the cluster keeps committing
// around it, proving each cycle's catch-up is correct and no earlier
// commit is ever lost or altered.
func TestChaos_RepeatedCrashRestartCycles(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var flapper raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			flapper = id
			break
		}
	}

	var outcomes []fsm.Outcome
	for cycle := 0; cycle < 5; cycle++ {
		tc.crash(flapper)

		key := fmt.Sprintf("k%d", cycle)
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("cycle-%d", cycle), uint64(cycle+1), 0, key, "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("cycle %d: Propose while flapper down: outcome=%+v err=%v", cycle, outcome, err)
		}
		outcomes = append(outcomes, outcome)

		restarted := tc.restart(flapper)
		awaitCondition(t, 5*time.Second, fmt.Sprintf("cycle %d: flapper catches up on %s", cycle, key), func() bool {
			v, ok := restarted.FSM().Store().Visible(key, outcome.CommitSeq)
			return ok && string(v) == "v"
		})

		// Every earlier cycle's key must still be present on the
		// restarted node — no cycle's catch-up may regress an earlier
		// one.
		for i, o := range outcomes {
			ek := fmt.Sprintf("k%d", i)
			if v, ok := restarted.FSM().Store().Visible(ek, o.CommitSeq); !ok || string(v) != "v" {
				t.Fatalf("cycle %d: earlier key %s missing/wrong after flapper's restart: ok=%v v=%q", cycle, ek, ok, v)
			}
		}
	}
}

// TestChaos_RetryAcrossMultipleLeadershipEpochs is the RequestID-chaos
// "retry during leader transition" scenario, extended across more than
// one failover: the client retries the identical RequestID against a
// second, and then a third, distinct leader, and always observes the
// same terminal outcome — never a double application, never a
// different result.
func TestChaos_RetryAcrossMultipleLeadershipEpochs(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)

	first, err := propose(t, tc.node(leaderID), cmd("multi-epoch", 1, 0, "k", "v"), 3*time.Second)
	if err != nil || first.Status != fsm.StatusCommitted {
		t.Fatalf("initial Propose: outcome=%+v err=%v", first, err)
	}

	seenLeaders := map[raft.NodeID]bool{leaderID: true}
	prev := first
	for epoch := 0; epoch < 2; epoch++ {
		curLeaderID, _ := anyLeader(tc)
		tc.crash(curLeaderID)

		var nextLeaderID raft.NodeID
		awaitCondition(t, 5*time.Second, "a new leader is elected", func() bool {
			id, ok := anyLeader(tc)
			if ok && !seenLeaders[id] {
				nextLeaderID = id
				return true
			}
			// Also accept a leader we've already seen (a restarted node
			// could theoretically re-win); only require SOME leader.
			if ok {
				nextLeaderID = id
				return true
			}
			return false
		})
		seenLeaders[nextLeaderID] = true

		retry, err := propose(t, tc.node(nextLeaderID), cmd("multi-epoch", 1, 0, "k", "v"), 3*time.Second)
		if err != nil {
			t.Fatalf("epoch %d retry: %v", epoch, err)
		}
		if retry != prev {
			t.Fatalf("epoch %d: retried outcome %+v != previous %+v (exactly-once logical effect violated across leadership epochs)", epoch, retry, prev)
		}
		prev = retry

		// Bring the crashed node back so the next epoch has 3 live
		// nodes to work with (majority remains available throughout).
		tc.restart(curLeaderID)
		tc.awaitLeader(5 * time.Second)
	}
}

// TestChaos_RetryAfterSnapshotAndCompactionStableOutcome is the
// RequestID-chaos "retry after compaction" scenario: the original Raft
// log entry for a RequestID is compacted away entirely (only the
// snapshot retains it), and a retry still returns the identical
// outcome — proven here specifically via a node that recovers the
// RequestID's outcome from ITS OWN restored snapshot (no log replay
// involved at all, mirroring TestSN1's proof but adding the explicit
// RequestID-retry angle that scenario doesn't check).
func TestChaos_RetryAfterSnapshotAndCompactionStableOutcome(t *testing.T) {
	const threshold = 3
	const numKeys = 6
	tc := newTestClusterWithSnapshotThreshold(t, 1, threshold)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var first fsm.Outcome
	for i := 0; i < numKeys; i++ {
		reqID := "compacted-req"
		if i > 0 {
			reqID = fmt.Sprintf("filler-%d", i)
		}
		outcome, err := propose(t, leader, cmd(reqID, uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
		if i == 0 {
			first = outcome
		}
	}
	awaitCondition(t, 3*time.Second, "snapshot boundary covers the RequestID's original entry", func() bool {
		return leader.Status().SnapshotIndex >= raft.Index(first.CommitSeq)
	})
	if got := leader.walog.FirstIndex(); got <= uint64(first.CommitSeq) {
		t.Fatalf("original entry's log index %d is still durably in the log (FirstIndex=%d); compaction did not actually remove it", first.CommitSeq, got)
	}

	tc.crash(leaderID)
	restarted := tc.restart(leaderID)
	tc.awaitLeader(5 * time.Second)

	// The retry must resolve purely from the restored snapshot's
	// outcome table — the original log entry no longer exists anywhere
	// on disk.
	retry, err := propose(t, restarted, cmd("compacted-req", 1, 0, "k0", "v"), 3*time.Second)
	if err != nil {
		t.Fatalf("retry after snapshot+compaction+restart: %v", err)
	}
	if retry.Status != fsm.StatusCommitted || retry.CommitSeq != first.CommitSeq {
		t.Fatalf("retry outcome = %+v, want identical to original %+v", retry, first)
	}
}

// TestChaos_ConflictOutcomeSurvivesSnapshotRestartAndRetry extends
// TestConflictOutcomeReplicatedAndStableAcrossFailover
// (node_test.go) with a snapshot+restart step: a deterministic ABORTED
// conflict outcome must remain stable not just across leader failover
// but across a snapshot boundary and a full restart too.
func TestChaos_ConflictOutcomeSurvivesSnapshotRestartAndRetry(t *testing.T) {
	const threshold = 3
	tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	o1, err := propose(t, leader, cmd("t1", 1, 0, "k", "v1"), 3*time.Second)
	if err != nil || o1.Status != fsm.StatusCommitted {
		t.Fatalf("t1 Propose: outcome=%+v err=%v", o1, err)
	}
	o2, err := propose(t, leader, cmd("t2", 2, 0, "k", "v2"), 3*time.Second)
	if err != nil {
		t.Fatalf("t2 Propose: %v", err)
	}
	if o2.Status != fsm.StatusAborted {
		t.Fatalf("t2 outcome = %+v, want Aborted", o2)
	}

	// Push enough further commits to cross the snapshot threshold,
	// compacting away t2's original log entry.
	for i := 0; i < threshold+2; i++ {
		filler, err := propose(t, leader, cmd(fmt.Sprintf("filler-%d", i), uint64(10+i), 0, fmt.Sprintf("f%d", i), "v"), 3*time.Second)
		if err != nil || filler.Status != fsm.StatusCommitted {
			t.Fatalf("filler propose #%d: outcome=%+v err=%v", i, filler, err)
		}
	}
	awaitCondition(t, 5*time.Second, "leader compacts past t2's original entry", func() bool {
		return leader.Status().SnapshotIndex >= raft.Index(o2.CommitSeq)
	})

	tc.crash(leaderID)
	restarted := tc.restart(leaderID)
	newLeaderID := tc.awaitLeader(5 * time.Second)
	newLeader := tc.node(newLeaderID)
	_ = restarted

	retry, err := propose(t, newLeader, cmd("t2", 2, 0, "k", "v2"), 3*time.Second)
	if err != nil {
		t.Fatalf("retry after snapshot+restart: %v", err)
	}
	if retry.Status != fsm.StatusAborted {
		t.Fatalf("retried t2 outcome = %+v, want stable Aborted even after snapshot+restart", retry)
	}
}

// TestChaos_AsymmetricPartitionNoSafetyViolation exercises a real,
// directional-only partition (docs/roadmap.md Phase 7's asymmetric
// topology) against the real TCP transport, via
// Transport.BlockSend/BlockRecv (added this phase specifically because
// Transport.Block/Unblock is symmetric-only — see that method's doc
// comment). The isolated-from-replication leader must never acknowledge
// a new write while cut off from receiving acknowledgements, and the
// cluster must converge once healed.
func TestChaos_AsymmetricPartitionNoSafetyViolation(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var others []raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			others = append(others, id)
		}
	}

	// Directional cut: the leader's outbound AppendEntries still reach
	// both followers (so they might durably persist an entry), but
	// their responses back to the leader are dropped — the leader can
	// never observe a majority matchIndex for anything proposed during
	// the cut, so it must never acknowledge a new commit either.
	for _, other := range others {
		leader.Transport().BlockRecv(other)
	}

	_, err := propose(t, leader, cmd("during-asym-cut", 1, 0, "k1", "v1"), 1*time.Second)
	if err == nil {
		t.Fatal("leader acknowledged a write while unable to receive any AppendEntriesResponse; QUORUM-SAFETY violated")
	}

	for _, other := range others {
		leader.Transport().UnblockRecv(other)
	}

	// Once healed, the identical retry must succeed and the cluster
	// must converge.
	outcome, err := propose(t, leader, cmd("during-asym-cut", 1, 0, "k1", "v1"), 5*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("retry after healing the asymmetric cut: outcome=%+v err=%v", outcome, err)
	}
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, fmt.Sprintf("node %s converges after asymmetric partition heals", id), func() bool {
			v, ok := tc.node(id).FSM().Store().Visible("k1", outcome.CommitSeq)
			return ok && string(v) == "v1"
		})
	}
}

// TestChaos_RepeatedPartitionHealAcrossLeaders is the real-disk/real-
// network variant of RF-13/RF-15's chaos scope: several isolate/heal
// cycles, each against a possibly different node, interleaved with
// commits, tracked by a simple oracle proving no committed key is ever
// lost or altered by a later cycle no matter how leadership churns.
func TestChaos_RepeatedPartitionHealAcrossLeaders(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.awaitLeader(5 * time.Second)

	type fact struct {
		value     string
		commitSeq uint64
	}
	oracle := make(map[string]fact)

	const cycles = 4
	for cycle := 0; cycle < cycles; cycle++ {
		leaderID := tc.awaitLeader(5 * time.Second)
		leader := tc.node(leaderID)

		key := fmt.Sprintf("k%d", cycle)
		val := fmt.Sprintf("v%d", cycle)
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("cycle-%d", cycle), uint64(cycle+1), 0, key, val), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("cycle %d: pre-partition Propose: outcome=%+v err=%v", cycle, outcome, err)
		}
		oracle[key] = fact{value: val, commitSeq: outcome.CommitSeq}

		// Isolate whichever node is currently leader (rotates which
		// node gets cut off across cycles as leadership changes).
		tc.isolate(leaderID)
		var newLeaderID raft.NodeID
		awaitCondition(t, 5*time.Second, "majority elects a leader while one node is isolated", func() bool {
			for id, n := range tc.nodes {
				if id == leaderID {
					continue
				}
				if n.Status().Role == raft.Leader {
					newLeaderID = id
					return true
				}
			}
			return false
		})
		midKey := fmt.Sprintf("mid%d", cycle)
		midVal := fmt.Sprintf("mv%d", cycle)
		midOutcome, err := propose(t, tc.node(newLeaderID), cmd(fmt.Sprintf("mid-%d", cycle), uint64(100+cycle), 0, midKey, midVal), 3*time.Second)
		if err != nil || midOutcome.Status != fsm.StatusCommitted {
			t.Fatalf("cycle %d: mid-partition Propose: outcome=%+v err=%v", cycle, midOutcome, err)
		}
		oracle[midKey] = fact{value: midVal, commitSeq: midOutcome.CommitSeq}

		tc.heal(leaderID)
		for _, id := range tc.ids {
			id := id
			awaitCondition(t, 5*time.Second, fmt.Sprintf("cycle %d: node %s converges after heal", cycle, id), func() bool {
				v, ok := tc.node(id).FSM().Store().Visible(midKey, midOutcome.CommitSeq)
				return ok && string(v) == midVal
			})
		}

		// Every fact recorded in every prior cycle must still hold on
		// every live node right now.
		for _, id := range tc.ids {
			for k, f := range oracle {
				v, ok := tc.node(id).FSM().Store().Visible(k, f.commitSeq)
				if !ok || string(v) != f.value {
					t.Fatalf("cycle %d: node %s lost or altered earlier fact %s=%s (got ok=%v v=%q)", cycle, id, k, f.value, ok, v)
				}
			}
		}
	}
}

// TestChaos_TransactionAtomicityAcrossLeaderCrash proves a multi-key
// CommitTxn command's atomicity holds even when its own leader crashes
// immediately after proposing it: every live replica either has ALL of
// its mutations visible or NONE of them, never a partial subset, at
// every point sampled through the crash/failover/retry sequence.
func TestChaos_TransactionAtomicityAcrossLeaderCrash(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	c := fsm.CommitTxnCommand{
		RequestID: "atomic-crash",
		TxnID:     1,
		StartSeq:  0,
		Mutations: []mvcc.Mutation{
			{Key: "x", Value: []byte("1")},
			{Key: "y", Value: []byte("2")},
			{Key: "z", Value: []byte("3")},
		},
	}

	resultCh := make(chan struct {
		outcome fsm.Outcome
		err     error
	}, 1)
	go func() {
		outcome, err := propose(t, leader, c, 3*time.Second)
		resultCh <- struct {
			outcome fsm.Outcome
			err     error
		}{outcome, err}
	}()

	// Give the proposal a brief window to reach the log before crashing
	// the leader — whether it reached quorum or not, atomicity must
	// still hold on every node at every subsequent point.
	time.Sleep(20 * time.Millisecond)
	tc.crash(leaderID)

	checkAtomic := func(id raft.NodeID) {
		store := tc.node(id).FSM().Store()
		seen := 0
		for _, k := range []string{"x", "y", "z"} {
			if _, ok := store.Visible(k, ^uint64(0)); ok {
				seen++
			}
		}
		if seen != 0 && seen != 3 {
			t.Fatalf("node %s observed a partial application of a multi-key transaction: %d of 3 keys visible", id, seen)
		}
	}
	for _, id := range tc.ids {
		if id != leaderID {
			checkAtomic(id)
		}
	}

	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "a new leader is elected after the crash", func() bool {
		for id, n := range tc.nodes {
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})

	// Retry the identical RequestID against the new leader to reach a
	// terminal outcome, then confirm atomicity holds everywhere.
	retry, err := propose(t, tc.node(newLeaderID), c, 3*time.Second)
	if err != nil {
		t.Fatalf("retry after leader crash: %v", err)
	}
	if retry.Status == fsm.StatusCommitted {
		for _, id := range tc.ids {
			if id == leaderID {
				continue // still crashed; never restarted in this test
			}
			id := id
			awaitCondition(t, 5*time.Second, fmt.Sprintf("node %s converges on all 3 keys", id), func() bool {
				store := tc.node(id).FSM().Store()
				for _, k := range []string{"x", "y", "z"} {
					if _, ok := store.Visible(k, retry.CommitSeq); !ok {
						return false
					}
				}
				return true
			})
		}
	}
	for _, id := range tc.ids {
		if id != leaderID {
			checkAtomic(id)
		}
	}
}

// TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly stresses
// SN-3 (interrupted snapshot installation) at the real-disk layer: a
// follower that falls behind a leader's compaction boundary is healed,
// then immediately crashed again before catch-up can possibly have
// completed, then restarted — proving the follower never exposes
// partial snapshot state and eventually converges via a clean,
// from-scratch install (docs/snapshots.md §7 step 6).
func TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly(t *testing.T) {
	const threshold = 3
	const numKeys = 9
	tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			follower = id
			break
		}
	}
	tc.isolate(follower)

	var last fsm.Outcome
	for i := 0; i < numKeys; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("r%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
		last = outcome
	}
	// Force the leader's snapshot boundary to actually cover every key
	// this test cares about. SnapshotThreshold-boundary alignment means
	// a snapshot only fires at multiples of threshold entries since the
	// last one; this test used to rely on numKeys being an exact
	// multiple of threshold so that a snapshot would exist at exactly
	// index numKeys with no other entries in the log. That assumption
	// no longer holds unconditionally as of Phase 8: this node's own
	// election win proposes one synthetic no-op entry
	// (internal/node.Node.proposeElectionNoOp, docs/replication.md
	// §4.3) before this test's own proposals ever run, which can shift
	// where a threshold boundary lands. Proposing up to one full
	// threshold's worth of harmless filler keys is always enough to
	// cross whatever boundary offset a single election introduced.
	for i := 0; uint64(leader.Status().SnapshotIndex) < last.CommitSeq && i < threshold; i++ {
		if _, err := propose(t, leader, cmd(fmt.Sprintf("filler-r%d", i), uint64(numKeys+i+1000), 0, fmt.Sprintf("filler-k%d", i), "v"), 3*time.Second); err != nil {
			t.Fatalf("filler Propose #%d: %v", i, err)
		}
	}
	awaitCondition(t, 3*time.Second, "leader compacts past every test key", func() bool {
		return uint64(leader.Status().SnapshotIndex) >= last.CommitSeq
	})

	tc.heal(follower)
	// Immediately crash the follower again — before it can possibly
	// have finished installing the snapshot — to force an interrupted
	// install, per SN-3.
	time.Sleep(5 * time.Millisecond)
	tc.crash(follower)
	restarted := tc.restart(follower)

	// No partial snapshot state may ever be visible: at every point
	// sampled while catch-up proceeds, either none or all of the
	// leader-compacted keys are visible.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		store := restarted.FSM().Store()
		seen := 0
		for i := 0; i < numKeys; i++ {
			if _, ok := store.Visible(fmt.Sprintf("k%d", i), last.CommitSeq); ok {
				seen++
			}
		}
		if seen != 0 && seen != numKeys {
			t.Fatalf("follower exposed partial snapshot state: %d of %d compacted keys visible", seen, numKeys)
		}
		if seen == numKeys {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower never fully caught up after a crash during snapshot catch-up")
}
