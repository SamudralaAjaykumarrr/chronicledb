//go:build integration

// This file continues docs/dynamic-membership-plan.md §16's real-
// process proof: snapshot/compaction while a membership change is in
// flight, and the two required backup/restore real-process steps.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestRealMembership -v
package main

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRealMembership_SnapshotInFlightDuringConfigChange proves
// docs/dynamic-membership-plan.md §16's snapshot/compaction step: real
// writes cross a small snapshot threshold while a real membership
// change (add + promote a fourth node) is concurrently in flight, and
// a node restarted afterward recovers the correct, fully-converged
// Configuration from its snapshot plus WAL suffix.
func TestRealMembership_SnapshotInFlightDuringConfigChange(t *testing.T) {
	bin := buildBinary(t)
	const threshold = 5
	nodes := newRealClusterWithSnapshotThreshold(t, bin, 3, threshold)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration2(t, leader, nodes)

	writerStop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		i := 0
		for {
			select {
			case <-writerStop:
				return
			default:
			}
			i++
			cur := currentRealLeader(nodes)
			if cur == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			propose(cur, fmt.Sprintf("snap-write-%d", i), fmt.Sprintf("k%d", i), "v")
			time.Sleep(2 * time.Millisecond)
		}
	}()

	learnerAddr := freePorts(t, 1)[0]
	learner := &realNode{
		id:       "n4",
		raftAddr: learnerAddr,
		httpAddr: freePorts(t, 1)[0],
		dataDir:  t.TempDir(),
		args: []string{
			"-id=n4",
			"-listen=" + learnerAddr,
			"-peers=n1=" + nodes[0].raftAddr + ",n2=" + nodes[1].raftAddr + ",n3=" + nodes[2].raftAddr,
			"-snapshot-threshold=" + fmt.Sprint(threshold),
		},
	}
	startRealNode(t, bin, learner)
	defer stopRealNode(learner)

	addResp, addStatus, err := membershipAddHTTP(leader, "snap-add-n4", "n4", learnerAddr)
	if err != nil || addStatus != http.StatusOK || addResp.Status != "committed" {
		t.Fatalf("membership add n4: resp=%+v status=%d err=%v", addResp, addStatus, err)
	}
	awaitCondition2(t, 10*time.Second, "n4 catches up", func() bool {
		lst, lerr := statusV2(leader)
		nst, nerr := statusV2(learner)
		return lerr == nil && nerr == nil && nst.AppliedIndex >= lst.LastIndex
	})
	promoted := false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, status, err := membershipPromoteHTTP(leader, "snap-promote-n4", "n4")
		if err != nil {
			t.Fatalf("promote n4: %v", err)
		}
		if status == http.StatusOK && resp.Status == "committed" {
			promoted = true
			break
		}
		if status != http425TooEarly {
			t.Fatalf("promote n4: status=%d resp=%+v, want 200 or 425 only", status, resp)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !promoted {
		t.Fatal("promote n4 never succeeded within the deadline")
	}

	// Real writes concurrently cross the snapshot threshold on at
	// least one node while the above add/promote sequence was in
	// flight in real wall-clock time.
	awaitCondition2(t, 10*time.Second, "at least one node takes a real snapshot", func() bool {
		for _, rn := range nodes {
			st, err := statusV2(rn)
			if err == nil && st.SnapshotIndex > 0 {
				return true
			}
		}
		return false
	})

	close(writerStop)
	<-writerDone

	awaitCondition2(t, 10*time.Second, "cluster converges on 4 voters", func() bool {
		st, _, err := membershipStatusHTTP(leader)
		return err == nil && len(st.Voters) == 4
	})

	// Restart a follower and confirm it recovers the fully-converged
	// Configuration from its snapshot plus WAL suffix, not just the
	// pre-snapshot state.
	var follower *realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			follower = rn
			break
		}
	}
	follower.crash()
	follower.restart(t, bin)
	awaitCondition2(t, 10*time.Second, "restarted follower comes back up and converges on 4 voters", func() bool {
		st, status, err := membershipStatusHTTP(follower)
		return err == nil && status == http.StatusOK && len(st.Voters) == 4
	})
}

// TestRealMembership_BackupRestoreNewIdentityWithoutSnapshotFirst
// proves the first of §16's two required backup/restore steps: a real
// backup taken deliberately without forcing a snapshot first (so the
// membership entries live in the WAL suffix, §23/F1) is restored onto
// a new cluster with different NodeIDs/addresses, which must elect a
// leader and serve reads/writes, report exactly the operator-supplied
// membership, and carry no trace of the source's NodeIDs.
func TestRealMembership_BackupRestoreNewIdentityWithoutSnapshotFirst(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration2(t, leader, nodes)

	resp, status, err := propose(leader, "pre-backup-write", "kk", "vv")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("pre-backup write: resp=%+v status=%d err=%v", resp, status, err)
	}
	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "pre-backup-write", 10*time.Second)
	}

	learnerAddr := freePorts(t, 1)[0]
	addResp, addStatus, err := membershipAddHTTP(leader, "backup-add-n4", "n4", learnerAddr)
	if err != nil || addStatus != http.StatusOK || addResp.Status != "committed" {
		t.Fatalf("membership add n4: resp=%+v status=%d err=%v", addResp, addStatus, err)
	}

	backupDir := t.TempDir()
	httpBackup(t, leader, backupDir, true)

	// Restore onto a brand-new 3-node cluster with entirely different
	// NodeIDs and addresses.
	restoredIDs := []string{"r1", "r2", "r3"}
	restoredPorts := freePorts(t, 2*len(restoredIDs))
	restoredRaftAddrs := make(map[string]string, len(restoredIDs))
	restoredHTTPAddrs := make(map[string]string, len(restoredIDs))
	for i, id := range restoredIDs {
		restoredRaftAddrs[id] = restoredPorts[i]
		restoredHTTPAddrs[id] = restoredPorts[len(restoredIDs)+i]
	}
	restoredClusterFlag := strings.Join(restoredIDs, ",")
	restoredNodes := make([]*realNode, len(restoredIDs))
	for i, id := range restoredIDs {
		var peerParts []string
		for _, other := range restoredIDs {
			if other != id {
				peerParts = append(peerParts, other+"="+restoredRaftAddrs[other])
			}
		}
		// -restore-from is a preflight main.go itself runs before
		// node.Open, as part of ordinary startup (not a separate
		// standalone invocation) — -id/-listen/-http/-datadir are
		// still required alongside it, and normal startup continues
		// against the now-restored directory once it completes.
		// dataDir must not already exist: Restore's own final step
		// renames its staging directory into place at exactly this
		// path, which fails ("file exists") against a directory
		// t.TempDir() already created, even an empty one.
		restoredNodes[i] = &realNode{
			id:       id,
			raftAddr: restoredRaftAddrs[id],
			httpAddr: restoredHTTPAddrs[id],
			dataDir:  t.TempDir() + "/data",
			args: []string{
				"-id=" + id,
				"-listen=" + restoredRaftAddrs[id],
				"-cluster=" + restoredClusterFlag,
				"-peers=" + strings.Join(peerParts, ","),
				"-restore-from=" + backupDir,
			},
		}
	}
	for _, rn := range restoredNodes {
		startRealNode(t, bin, rn)
	}
	t.Cleanup(func() {
		for _, rn := range restoredNodes {
			stopRealNode(rn)
		}
	})

	restoredLeader := awaitLeader(t, restoredNodes, 10*time.Second)
	rResp, rStatus, err := propose(restoredLeader, "post-restore-write", "rk", "rv")
	if err != nil || rStatus != http.StatusOK || rResp.Status != "committed" {
		t.Fatalf("post-restore write: resp=%+v status=%d err=%v", rResp, rStatus, err)
	}

	stMember, _, err := membershipStatusHTTP(restoredLeader)
	if err != nil {
		t.Fatalf("membership status of restored cluster: %v", err)
	}
	if len(stMember.Voters) != 3 {
		t.Fatalf("restored cluster voters = %+v, want exactly the 3 operator-supplied voters", stMember.Voters)
	}
	for _, v := range stMember.Voters {
		if v.ID == "n1" || v.ID == "n2" || v.ID == "n3" || v.ID == "n4" {
			t.Fatalf("restored cluster retains a source NodeID: %+v", stMember.Voters)
		}
	}

	// The source's pre-backup ordinary write survives; its membership
	// RequestID (§7.6's scoped exception) is not expected to.
	for _, rn := range restoredNodes {
		awaitOutcomeCommitted(t, rn, "pre-backup-write", 10*time.Second)
	}
}

// TestRealMembership_BackupRestoreRealV040Binary proves the second of
// §16's two required backup/restore steps: a real backup produced by
// the actual, previously-released v0.4.0 binary (via the same git-
// worktree technique mixed_version_test.go's buildBinaryAtRef already
// uses, -buildvcs=false fix included) restores successfully under the
// current (v0.5.0) binary, and the resulting cluster's configuration
// comes from the restore's own operator-supplied flags — never a
// re-implemented old encoder, a genuine v0.4.0 artifact end to end
// (dynamic-membership plan §7.1, §7.6, §19 gate 6).
func TestRealMembership_BackupRestoreRealV040Binary(t *testing.T) {
	oldBin := buildBinaryAtRef(t, "v0.4.0")
	newBin := buildBinary(t)

	oldNodes := newRealCluster(t, oldBin, 3)
	oldLeader := awaitLeader(t, oldNodes, 10*time.Second)

	resp, status, err := propose(oldLeader, "v040-write", "ok", "ov")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("write against real v0.4.0 cluster: resp=%+v status=%d err=%v", resp, status, err)
	}
	for _, rn := range oldNodes {
		awaitOutcomeCommitted(t, rn, "v040-write", 10*time.Second)
	}

	backupDir := t.TempDir()
	httpBackup(t, oldLeader, backupDir, true)

	// Restore the real v0.4.0 backup under the current binary, onto a
	// single new node with a wholly different identity.
	restoredAddr := freePorts(t, 2)
	restoredID := "v5"
	restored := &realNode{
		id:       restoredID,
		raftAddr: restoredAddr[0],
		httpAddr: restoredAddr[1],
		dataDir:  t.TempDir() + "/data",
		args: []string{
			"-id=" + restoredID,
			"-listen=" + restoredAddr[0],
			"-cluster=" + restoredID,
			"-restore-from=" + backupDir,
		},
	}
	startRealNode(t, newBin, restored)
	t.Cleanup(func() { stopRealNode(restored) })

	awaitCondition2(t, 10*time.Second, "restored v0.4.0 backup elects a leader under v0.5.0", func() bool {
		st, err := statusV2(restored)
		return err == nil && st.Role == roleLeader
	})

	rResp, rStatus, err := propose(restored, "post-v040-restore-write", "rk2", "rv2")
	if err != nil || rStatus != http.StatusOK || rResp.Status != "committed" {
		t.Fatalf("write after v0.4.0 backup restore: resp=%+v status=%d err=%v", rResp, rStatus, err)
	}

	stMember, _, err := membershipStatusHTTP(restored)
	if err != nil {
		t.Fatalf("membership status after v0.4.0 backup restore: %v", err)
	}
	if len(stMember.Voters) != 1 || stMember.Voters[0].ID != restoredID {
		t.Fatalf("restored configuration = %+v, want exactly the operator-supplied sole voter %q", stMember.Voters, restoredID)
	}
	for _, v := range stMember.Voters {
		if v.ID == "n1" || v.ID == "n2" || v.ID == "n3" {
			t.Fatalf("restored configuration retains a source v0.4.0 NodeID: %+v", stMember.Voters)
		}
	}
}
