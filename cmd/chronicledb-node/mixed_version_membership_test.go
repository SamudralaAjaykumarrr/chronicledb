//go:build integration

// This file is dynamic-membership plan §16's mixed-old/new-binary
// variant: a real v0.4.0 baseline cluster, rolled node-by-node to the
// current (v0.5.0) binary — each node starting successfully against
// its own pre-existing, real FormatVersion 1 snapshot (§7.1's specific
// concern) — finalized to generation 2, then put through the add/
// promote/remove/restart sequence.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestMixedVersionMembership -v
package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestMixedVersionMembership_RollThenAddPromoteRemoveRestart proves
// §16's mixed-binary variant end to end.
func TestMixedVersionMembership_RollThenAddPromoteRemoveRestart(t *testing.T) {
	oldBin := buildBinaryAtRef(t, "v0.4.0")
	newBin := buildBinary(t)

	const threshold = 5
	nodes := newRealClusterWithSnapshotThreshold(t, oldBin, 3, threshold)
	leader := awaitLeader(t, nodes, 10*time.Second)

	// Force every node to take a real, genuine v0.4.0 (FormatVersion 1)
	// snapshot before any of them ever runs the new binary.
	for i := 0; i < threshold+2; i++ {
		reqID := fmt.Sprintf("v040-presnap-%d", i)
		resp, status, err := propose(leader, reqID, fmt.Sprintf("k%d", i), "v")
		if err != nil || status != http.StatusOK || resp.Status != "committed" {
			t.Fatalf("pre-upgrade write #%d: resp=%+v status=%d err=%v", i, resp, status, err)
		}
	}
	for _, rn := range nodes {
		rn := rn
		awaitCondition2(t, 10*time.Second, "node "+rn.id+" takes a real v0.4.0 snapshot", func() bool {
			st, err := statusV2(rn)
			return err == nil && st.SnapshotIndex > 0
		})
	}

	// Roll each node to the new binary one at a time, asserting each
	// one starts successfully against its own pre-existing snapshot
	// (no crash-loop, real catch-up) before moving to the next —
	// dynamic-membership plan §7.1's specific concern.
	for _, rn := range nodes {
		beforeCrash, err := statusV2(rn)
		if err != nil {
			t.Fatalf("status before rolling %s: %v", rn.id, err)
		}
		rn.crash()
		rn.restart(t, newBin)
		awaitCondition2(t, 10*time.Second, "node "+rn.id+" starts against its pre-existing v0.4.0 snapshot and catches up", func() bool {
			st, err := statusV2(rn)
			return err == nil && st.AppliedIndex >= beforeCrash.AppliedIndex
		})
	}

	leader = awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration2(t, leader, nodes)

	// Add, promote, remove, restart — dynamic-membership plan §16's
	// core sequence, now against a cluster whose durable state
	// originated on the old binary.
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
		},
	}
	startRealNode(t, newBin, learner)
	defer stopRealNode(learner)

	addResp, addStatus, err := membershipAddHTTP(leader, "mv-add-n4", "n4", learnerAddr)
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
		resp, status, err := membershipPromoteHTTP(leader, "mv-promote-n4", "n4")
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
	awaitCondition2(t, 10*time.Second, "cluster converges on 4 voters", func() bool {
		st, _, err := membershipStatusHTTP(leader)
		return err == nil && len(st.Voters) == 4
	})

	var removeTarget string
	for _, rn := range nodes {
		if rn.id != leader.id {
			removeTarget = rn.id
			break
		}
	}
	remResp, remStatus, err := membershipRemoveHTTP(leader, "mv-remove-1", removeTarget, 0)
	if err != nil || remStatus != http.StatusOK || remResp.Status != "committed" {
		t.Fatalf("remove %s: resp=%+v status=%d err=%v", removeTarget, remResp, remStatus, err)
	}
	awaitCondition2(t, 10*time.Second, "cluster converges on 3 voters after removal", func() bool {
		st, _, err := membershipStatusHTTP(leader)
		return err == nil && len(st.Voters) == 3
	})

	// Restart the leader (now running the new binary throughout) and
	// confirm it recovers the converged configuration exactly.
	beforeRestart, err := statusV2(leader)
	if err != nil {
		t.Fatalf("status before final restart: %v", err)
	}
	leader.crash()
	leader.restart(t, newBin)
	awaitCondition2(t, 10*time.Second, "leader restarts and recovers", func() bool {
		st, err := statusV2(leader)
		return err == nil && st.AppliedIndex >= beforeRestart.AppliedIndex
	})
	stMember, _, err := membershipStatusHTTP(leader)
	if err != nil {
		t.Fatalf("membership status after final restart: %v", err)
	}
	if len(stMember.Voters) != 3 {
		t.Fatalf("recovered configuration after final restart = %+v, want exactly 3 voters", stMember.Voters)
	}
}
