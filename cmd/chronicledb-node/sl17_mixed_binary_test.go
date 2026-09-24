//go:build integration

// This file is SL-17 (docs/v0.6.0-plan.md §30.2): a real v0.5.0 binary
// (built from that release tag via a detached git worktree —
// mixed_version_test.go's buildBinaryAtRef, the same real-mixed-binary
// technique that file's own v0.3.0 proof already established) must fail
// closed, with the documented error, both opening a generation-3
// snapshot directly and restoring a generation-3 backup — never guess
// at content newer than it understands (NO SILENT FORMAT
// MISINTERPRETATION).
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run SL17 -v
package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// newRealClusterWithFlags is newRealCluster (main_test.go) plus extra
// CLI flags appended to every node's args — used here for
// -snapshot-threshold, which newRealCluster's own fixed arg set does
// not expose.
func newRealClusterWithFlags(t *testing.T, bin string, n int, extraFlags ...string) []*realNode {
	t.Helper()
	ids := make([]string, n)
	ports := freePorts(t, 2*n)
	raftAddrs := make(map[string]string, n)
	httpAddrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		raftAddrs[ids[i]] = ports[i]
		httpAddrs[ids[i]] = ports[n+i]
	}
	clusterFlag := strings.Join(ids, ",")

	nodes := make([]*realNode, n)
	for i, id := range ids {
		var peerParts []string
		for _, other := range ids {
			if other != id {
				peerParts = append(peerParts, other+"="+raftAddrs[other])
			}
		}
		args := []string{
			"-id=" + id,
			"-listen=" + raftAddrs[id],
			"-cluster=" + clusterFlag,
			"-peers=" + strings.Join(peerParts, ","),
		}
		args = append(args, extraFlags...)
		nodes[i] = &realNode{id: id, raftAddr: raftAddrs[id], httpAddr: httpAddrs[id], dataDir: t.TempDir(), args: args}
	}
	for _, rn := range nodes {
		startRealNode(t, bin, rn)
	}
	t.Cleanup(func() {
		for _, rn := range nodes {
			stopRealNode(rn)
		}
	})
	return nodes
}

// preV060Commit is v0.5.0 — the last release before this generation-3
// AdvanceGCWatermark control command existed at all.
const preV060Commit = "v0.5.0"

// finalizeRealClusterToGeneration finalizes leader/nodes up to target
// generation (each call raises it by exactly one), converging every
// node before returning — a straightforward generalization of
// finalizeRealClusterToGeneration2 (which is pinned at exactly 2 by its
// own name/callers and left alone rather than rewritten in place).
func finalizeRealClusterToGeneration(t *testing.T, leader *realNode, nodes []*realNode, target uint32) {
	t.Helper()
	for i := uint32(0); i < target; i++ {
		awaitCondition2(t, 10*time.Second, "precheck reports Ready", func() bool {
			res, status, err := precheckHTTP(leader)
			return err == nil && status == 200 && res.Ready && !res.AlreadyFinalized
		})
		finalizeRealClusterOnce(t, leader)
	}
	for _, rn := range nodes {
		rn := rn
		awaitCondition2(t, 10*time.Second, "node "+rn.id+" converges on generation "+string(rune('0'+target)), func() bool {
			st, err := statusV2(rn)
			return err == nil && st.ClusterGeneration == target
		})
	}
}

func TestSL17_OldBinaryFailsClosedOnGeneration3Snapshot(t *testing.T) {
	bin := buildBinary(t)
	oldBin := buildBinaryAtRef(t, preV060Commit)

	nodes := newRealClusterWithFlags(t, bin, 3, "-snapshot-threshold=1")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration(t, leader, nodes, 3)

	for i := 0; i < 3; i++ {
		pr, status, err := propose(leader, "sl17-warmup-"+string(rune('a'+i)), "k"+string(rune('a'+i)), "v")
		if err != nil || status != 200 || pr.Status != "committed" {
			t.Fatalf("warmup propose #%d: resp=%+v status=%d err=%v", i, pr, status, err)
		}
	}
	awaitCondition2(t, 10*time.Second, "a generation-3 local snapshot is created", func() bool {
		st, err := statusV2(leader)
		return err == nil && st.SnapshotIndex > 0
	})

	// Stop one follower and try to start the OLD binary directly against
	// its existing, generation-3 data directory.
	var follower *realNode
	for _, rn := range nodes {
		if rn.httpAddr != leader.httpAddr {
			follower = rn
			break
		}
	}
	follower.crash()

	cmd := exec.Command(oldBin, append([]string{}, follower.args...)...)
	cmd.Args = append(cmd.Args, "-http="+follower.httpAddr, "-datadir="+follower.dataDir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("old (v0.5.0) binary opened a generation-3 data directory without error; stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "generation") {
		t.Errorf("old binary's refusal did not mention \"generation\" (documented error expected): %s", stderr.String())
	}
}

func TestSL17_OldBinaryFailsClosedRestoringGeneration3Backup(t *testing.T) {
	bin := buildBinary(t)
	oldBin := buildBinaryAtRef(t, preV060Commit)

	nodes := newRealClusterWithFlags(t, bin, 3, "-snapshot-threshold=1")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration(t, leader, nodes, 3)

	for i := 0; i < 3; i++ {
		pr, status, err := propose(leader, "sl17b-warmup-"+string(rune('a'+i)), "k"+string(rune('a'+i)), "v")
		if err != nil || status != 200 || pr.Status != "committed" {
			t.Fatalf("warmup propose #%d: resp=%+v status=%d err=%v", i, pr, status, err)
		}
	}

	backupDir := t.TempDir() + "/backup"
	httpBackup(t, leader, backupDir, false)

	restoreTarget := t.TempDir() + "/restore-target"
	cmd := exec.Command(oldBin,
		"-id=restored", "-listen=127.0.0.1:0", "-http=127.0.0.1:0",
		"-datadir="+restoreTarget,
		"-restore-from="+backupDir, "-force-overwrite",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("old (v0.5.0) binary restored a generation-3 backup without error; stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "generation") {
		t.Errorf("old binary's restore refusal did not mention \"generation\" (documented error expected): %s", stderr.String())
	}
}
