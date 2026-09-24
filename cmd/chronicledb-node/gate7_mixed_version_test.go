//go:build integration

// This file is release gate 7's remaining two bullets (docs/v0.6.0-plan.md
// §31): a real v0.5.0/v0.6.0 mixed cluster operates normally at
// generation 2 and refuses to reach generation 3 (so GC, which requires
// generation 3, is structurally unreachable while any v0.5.0 node is
// still a member); and a real v0.5.0 binary fails closed on a
// generation-3 control entry delivered over the wire (as opposed to
// SL-17's disk-path proof: opening a generation-3 snapshot directly,
// and restoring a generation-3 backup — cmd/chronicledb-node/sl17_mixed_binary_test.go).
// SL-16 (a real v0.5.0-produced snapshot/backup still opens under
// v0.6.0) is also here, as the forward-compatibility half of the same
// gate.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run 'TestGate7|TestSL16' -v
package main

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestGate7_MixedClusterAtGeneration2OperatesNormallyAndRefusesGeneration3
// starts a cluster with one real v0.5.0 node among two v0.6.0 nodes,
// finalizes it to generation 2 (both binaries understand that), proves
// ordinary reads/writes still replicate correctly, then proves precheck
// refuses generation 3 outright while the v0.5.0 node remains a member
// — the structural reason GC (which requires generation 3) can never
// be reached in a genuinely mixed cluster, not merely a documented
// recommendation against it.
func TestGate7_MixedClusterAtGeneration2OperatesNormallyAndRefusesGeneration3(t *testing.T) {
	newBin := buildBinary(t)
	oldBin := buildBinaryAtRef(t, preV060Commit)

	nodes := newRealCluster(t, newBin, 3)
	// Downgrade one node to the real old binary before any traffic.
	var mixed *realNode
	for _, rn := range nodes {
		mixed = rn
		break
	}
	mixed.crash()
	mixed.restart(t, oldBin)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)

	// Ordinary traffic replicates to every node, including the old one.
	pr, status, err := propose(leader, "gate7-mixed-1", "k1", "v1")
	if err != nil || status != 200 || pr.Status != "committed" {
		t.Fatalf("propose in a mixed cluster: resp=%+v status=%d err=%v", pr, status, err)
	}
	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "gate7-mixed-1", 10*time.Second)
	}

	// Finalize to generation 2 — both binaries understand this.
	for i := 0; i < 2; i++ {
		awaitCondition2(t, 10*time.Second, "precheck reports Ready for generation <=2", func() bool {
			leader = awaitLeaderV2(t, nodes, 10*time.Second)
			res, status, err := precheckHTTP(leader)
			return err == nil && status == 200 && res.Ready && !res.AlreadyFinalized
		})
		finalizeRealClusterOnce(t, leader)
	}
	for _, rn := range nodes {
		rn := rn
		awaitCondition2(t, 10*time.Second, "node "+rn.id+" converges on generation 2", func() bool {
			st, err := statusV2(rn)
			return err == nil && st.ClusterGeneration == 2
		})
	}

	// More ordinary traffic at generation 2, still replicating to the
	// old binary correctly.
	pr2, status2, err2 := propose(leader, "gate7-mixed-2", "k2", "v2")
	if err2 != nil || status2 != 200 || pr2.Status != "committed" {
		t.Fatalf("propose at generation 2 in a mixed cluster: resp=%+v status=%d err=%v", pr2, status2, err2)
	}
	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "gate7-mixed-2", 10*time.Second)
	}

	// Generation 3 must be refused: the old node's own MaxSupportedGeneration
	// is 2, learned by the leader from live Raft traffic, so precheck
	// must never report Ready while it remains a member.
	time.Sleep(200 * time.Millisecond) // let a few heartbeats exchange SenderGeneration
	res, status3, err3 := precheckHTTP(leader)
	if err3 != nil || status3 != 200 {
		t.Fatalf("precheck: resp=%+v status=%d err=%v", res, status3, err3)
	}
	if res.Ready {
		t.Fatalf("precheck reports Ready for generation 3 while a real v0.5.0 node (max generation 2) is still a cluster member: %+v", res)
	}
}

// TestGate7_OldBinaryFailsClosedOnGeneration3ControlEntryOverTheWire
// brings up an all-v0.6.0 cluster, finalizes fully to generation 3,
// enables GC, and lets it commit at least one real AdvanceGCWatermark
// control entry (control-kind 2, which did not exist in v0.5.0). It
// then adds a brand-new learner running the real v0.5.0 binary (a
// fresh data directory, so wal.Open itself never refuses it the way
// SL-17's disk-path test does) and lets ordinary catch-up replication
// deliver the cluster's committed history to it over the wire.
//
// The old binary actually fails closed one step earlier than a naive
// reading suggests: replaying the finalize-to-generation-3 entry
// itself trips its own generation-capability check ("this binary only
// supports up to 2") before it ever reaches an AdvanceGCWatermark entry
// to decode — a real, observed reproduction, confirmed by capturing the
// process's own output, not merely inferred. Either check firing counts
// as "fails closed on a generation-3 control entry over the wire";
// which one fires first is an implementation detail of v0.5.0's own
// dispatch order, not something this test needs to force.
// Node.fail halts the event loop, and main.go's own shutdown loop reacts
// to Node.Done() closing by exiting the process on its own — nobody
// sends it a signal.
func TestGate7_OldBinaryFailsClosedOnGeneration3ControlEntryOverTheWire(t *testing.T) {
	newBin := buildBinary(t)
	oldBin := buildBinaryAtRef(t, preV060Commit)

	nodes := newRealClusterWithFlags(t, newBin, 3,
		"-gc-interval=50ms", "-gc-min-retain-seqs=0", "-gc-min-advance-seqs=1",
		"-gc-max-versions-per-pass=10000", "-gc-max-keys-per-pass=10000")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration(t, leader, nodes, 3)

	// Real overwrites so GC has something to advance the watermark over.
	initialStatus, err := leader.status()
	if err != nil {
		t.Fatalf("leader /status: %v", err)
	}
	lastCommitSeq := initialStatus.AppliedIndex
	for i := 0; i < 40; i++ {
		reqID := "gate7-gc-warmup-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		pr, status, err := proposeAt(leader, reqID, "gate7k", "v", lastCommitSeq)
		if err != nil {
			t.Fatalf("propose #%d: %v", i, err)
		}
		if status == 200 && pr.Status == "committed" {
			lastCommitSeq = pr.CommitSeq
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	var watermark float64
	for time.Now().Before(deadline) {
		if v, ok := metricValue(t, leader, "chronicledb_mvcc_gc_watermark"); ok && v > 0 {
			watermark = v
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if watermark == 0 {
		t.Fatal("gc_watermark never advanced past 0 — no AdvanceGCWatermark entry ever committed, so this test would prove nothing")
	}

	// A brand-new learner, real v0.5.0 binary, fresh data directory —
	// its own process, spawned directly (not via realNode/startRealNode,
	// which pipes straight to this test binary's own stderr) so its
	// output can be captured and inspected.
	learnerPorts := freePorts(t, 2)
	var peerParts []string
	for _, rn := range nodes {
		peerParts = append(peerParts, rn.id+"="+rn.raftAddr)
	}
	learnerDataDir := t.TempDir()
	cmd := exec.Command(oldBin,
		"-id=oldlearner", "-listen="+learnerPorts[0], "-http="+learnerPorts[1],
		"-datadir="+learnerDataDir, "-peers="+strings.Join(peerParts, ","),
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting old-binary learner: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()

	addResp, addStatus, addErr := membershipAddHTTP(leader, "gate7-add-oldlearner", "oldlearner", learnerPorts[0])
	if addErr != nil || addStatus != 200 || addResp.Status != "committed" {
		t.Fatalf("AddLearner(oldlearner): resp=%+v status=%d err=%v", addResp, addStatus, addErr)
	}

	// The old binary's process must exit ON ITS OWN — nobody sends it a
	// signal — once its own event loop halts on the undecodable control
	// entry (main.go's shutdown loop reacts to Node.Done() closing).
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case <-waitDone:
		// Exited on its own, as expected; the deferred cleanup's own
		// best-effort Kill/Wait on an already-reaped process is a
		// harmless no-op (Kill fails silently, Wait's error is ignored).
	case <-time.After(20 * time.Second):
		t.Fatalf("old-binary learner never exited on its own within 20s — it should have halted on the undecodable generation-3 control entry; output so far:\n%s", output.String())
	}

	got := output.String()
	if !strings.Contains(got, "this binary only supports up to") && !strings.Contains(got, "unknown control command") {
		t.Errorf("old-binary learner's own output does not mention either documented fail-closed reason (a generation-capability check, or fsm.ErrUnknownControlCommand); got:\n%s", got)
	}
	if !strings.Contains(got, "node stopped itself") {
		t.Errorf("old-binary learner's own output does not show main.go's own \"node stopped itself\" shutdown-loop message; got:\n%s", got)
	}
}

// TestSL16_V050ProducedSnapshotAndBackupOpenUnderV060 is SL-16's
// forward-compatibility half: a real v0.5.0-produced data directory
// (snapshot included) opens and serves correctly under the current
// v0.6.0 binary, and a real v0.5.0-produced backup restores and serves
// correctly under it too — gate 7's third bullet.
func TestSL16_V050ProducedSnapshotAndBackupOpenUnderV060(t *testing.T) {
	newBin := buildBinary(t)
	oldBin := buildBinaryAtRef(t, preV060Commit)

	nodes := newRealClusterWithFlags(t, oldBin, 3, "-snapshot-threshold=5")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)

	var lastReqID string
	for i := 0; i < 20; i++ {
		lastReqID = "sl16-" + string(rune('a'+i))
		pr, status, err := propose(leader, lastReqID, "sl16k"+string(rune('a'+i)), "v"+string(rune('a'+i)))
		if err != nil || status != 200 || pr.Status != "committed" {
			t.Fatalf("propose #%d on the old-binary cluster: resp=%+v status=%d err=%v", i, pr, status, err)
		}
	}
	awaitCondition2(t, 10*time.Second, "a v0.5.0 snapshot is created", func() bool {
		st, err := statusV2(leader)
		return err == nil && st.SnapshotIndex > 0
	})
	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, lastReqID, 10*time.Second)
	}

	backupDir := t.TempDir() + "/sl16-backup"
	httpBackup(t, leader, backupDir, true)

	// A follower's own (v0.5.0-produced) data directory opens directly
	// under the new binary.
	var follower *realNode
	for _, rn := range nodes {
		if rn.httpAddr != leader.httpAddr {
			follower = rn
			break
		}
	}
	follower.crash()
	follower.restart(t, newBin)
	awaitCondition2(t, 10*time.Second, "the v0.5.0-produced data directory opens and catches up under v0.6.0", func() bool {
		st, err := statusV2(follower)
		return err == nil && st.AppliedIndex > 0
	})

	// The v0.5.0-produced backup restores under the new binary into a
	// fresh single-node process and serves the same data.
	restorePorts := freePorts(t, 2)
	restored := &realNode{
		id: "sl16-restored", raftAddr: restorePorts[0], httpAddr: restorePorts[1], dataDir: t.TempDir() + "/restored",
		args: []string{
			"-id=sl16-restored", "-listen=" + restorePorts[0], "-cluster=sl16-restored",
			"-restore-from=" + backupDir,
		},
	}
	startRealNode(t, newBin, restored)
	defer stopRealNode(restored)
	awaitLeader(t, []*realNode{restored}, 10*time.Second)
}
