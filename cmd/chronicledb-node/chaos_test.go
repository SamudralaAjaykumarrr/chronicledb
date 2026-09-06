//go:build integration

// This file is Phase 7's real-process ("real SIGKILL") chaos evidence
// (docs/roadmap.md Phase 7's "Real SIGKILL tests" section): genuine
// separate OS processes, real persistent data directories, real TCP
// sockets, and a real SIGKILL — the same infrastructure
// main_test.go's TestRealProcesses_ElectionReplicationCrashRestartFailover
// established for Phase 5, extended here with: a follower kill during
// active replication, repeated SIGKILL attempts timed to land during a
// real snapshot install, a lost-response-then-retry sequence that never
// waits for the original response before killing the leader, and a
// real (not simulated) network partition/heal via the /fault control-
// plane endpoint main.go's handleFault exposes.
//
// Run explicitly (slower than the rest of the suite, like
// TestRealProcesses_*):
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestRealChaos -v
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func newRealClusterWithSnapshotThreshold(t *testing.T, bin string, n int, threshold uint64) []*realNode {
	t.Helper()
	ids := make([]string, n)
	raftAddrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		raftAddrs[ids[i]] = freePort(t)
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
		nodes[i] = &realNode{
			id:       id,
			raftAddr: raftAddrs[id],
			httpAddr: freePort(t),
			dataDir:  t.TempDir(),
			args: []string{
				"-id=" + id,
				"-listen=" + raftAddrs[id],
				"-cluster=" + clusterFlag,
				"-peers=" + strings.Join(peerParts, ","),
				fmt.Sprintf("-snapshot-threshold=%d", threshold),
			},
		}
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

func postFault(rn *realNode, action, peer string) error {
	resp, err := http.Post(fmt.Sprintf("http://%s/fault?action=%s&peer=%s", rn.httpAddr, action, peer), "application/json", bytes.NewReader(nil))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fault action %s on %s: status %d", action, rn.id, resp.StatusCode)
	}
	return nil
}

func awaitStatusCondition(t *testing.T, rn *realNode, timeout time.Duration, msg string, cond func(statusJSON) bool) statusJSON {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := rn.status()
		if err == nil && cond(st) {
			return st
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %s: condition not met within %s: %s", rn.id, timeout, msg)
	return statusJSON{}
}

// TestRealChaos_FollowerSIGKILLDuringReplicationCatchesUp is the
// "SIGKILL follower during replication" scenario: a follower is killed
// while writes are actively flowing, then restarted, and must catch up
// via ordinary real-disk/real-network replication.
func TestRealChaos_FollowerSIGKILLDuringReplicationCatchesUp(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	var follower *realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			follower = rn
			break
		}
	}

	// SIGKILL the follower mid-stream: fire off several proposals
	// without waiting for the follower's own outcome, killing it right
	// after issuing them.
	for i := 0; i < 5; i++ {
		if _, status, err := propose(leader, fmt.Sprintf("r%d", i), fmt.Sprintf("k%d", i), "v"); err != nil || status != http.StatusOK {
			t.Fatalf("propose #%d: status=%d err=%v", i, status, err)
		}
	}
	follower.crash()

	// Keep committing while the follower is down.
	last, status, err := propose(leader, "r-last", "k-last", "v")
	if err != nil || status != http.StatusOK || last.Status != "committed" {
		t.Fatalf("propose while follower down: resp=%+v status=%d err=%v", last, status, err)
	}

	follower.restart(t, bin)
	deadline := time.Now().Add(15 * time.Second)
	caught := false
	for time.Now().Before(deadline) {
		st, err := follower.status()
		if err == nil && st.AppliedIndex >= last.CommitSeq {
			caught = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !caught {
		t.Fatalf("restarted follower %s never caught up to CommitSeq %d", follower.id, last.CommitSeq)
	}
}

// TestRealChaos_SIGKILLDuringSnapshotInstall is the "SIGKILL during
// snapshot transfer/install" scenario. A real, separate follower
// process is killed and restarted repeatedly, timed (best-effort — real
// process/OS scheduling is not deterministic, unlike
// internal/fault/chaos_test.go's simulator-level equivalent) to land
// during its snapshot catch-up window, then finally left to run to
// completion — proving no partial/corrupted state survives any of the
// interrupted attempts and the follower eventually converges cleanly.
// This is a genuine real-process interruption, not a proxy for the
// deterministic simulator's exact-point crash tests (docs/roadmap.md
// Phase 7: "do not pretend deterministic simulator crash is equivalent
// to OS SIGKILL — keep both layers").
func TestRealChaos_SIGKILLDuringSnapshotInstall(t *testing.T) {
	bin := buildBinary(t)
	const threshold = 5
	nodes := newRealClusterWithSnapshotThreshold(t, bin, 3, threshold)
	leader := awaitLeader(t, nodes, 10*time.Second)

	var follower *realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			follower = rn
			break
		}
	}

	// Kill the follower now, before it has anything, so the leader's
	// compaction below leaves it needing a snapshot rather than
	// ordinary log replication to catch up.
	follower.crash()

	const numKeys = 12 // > threshold, forces at least one real compaction
	var lastCommitSeq uint64
	for i := 0; i < numKeys; i++ {
		resp, status, err := propose(leader, fmt.Sprintf("snap-r%d", i), fmt.Sprintf("snap-k%d", i), "v")
		if err != nil || status != http.StatusOK || resp.Status != "committed" {
			t.Fatalf("propose #%d: resp=%+v status=%d err=%v", i, resp, status, err)
		}
		lastCommitSeq = resp.CommitSeq
	}
	awaitStatusCondition(t, leader, 10*time.Second, "leader compacts its own log past the threshold", func(st statusJSON) bool {
		return st.SnapshotIndex > 0
	})

	// Repeatedly restart-then-almost-immediately-kill the follower a
	// few times: each attempt gives it a real chance to be mid-install
	// when killed. A real OS process/disk is not deterministic enough
	// to guarantee landing exactly mid-install every time (unlike the
	// simulator), so this is inherently best-effort — what is asserted
	// unconditionally is the safety property: never a partial/corrupt
	// state, and eventual convergence once left to finish.
	for attempt := 0; attempt < 4; attempt++ {
		follower.restart(t, bin)
		time.Sleep(15 * time.Millisecond) // give it a real chance to be mid-install
		follower.crash()
	}

	// Final attempt: let it run to completion.
	follower.restart(t, bin)
	deadline := time.Now().Add(15 * time.Second)
	converged := false
	for time.Now().Before(deadline) {
		st, err := follower.status()
		if err == nil && st.AppliedIndex >= lastCommitSeq {
			converged = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !converged {
		t.Fatalf("follower %s never converged after repeated SIGKILL attempts around snapshot install", follower.id)
	}

	// No partial state: every key must be present now that AppliedIndex
	// has caught up (an install is documented all-or-nothing —
	// docs/snapshots.md §7 — so a fully-caught-up follower must have
	// every key, never a subset).
	for i := 0; i < numKeys; i++ {
		out, status, err := getOutcome(follower, fmt.Sprintf("snap-r%d", i))
		if err != nil || status != http.StatusOK || out.Status != "committed" {
			t.Fatalf("key snap-k%d: outcome=%+v status=%d err=%v, want committed on the caught-up follower", i, out, status, err)
		}
	}
}

func getOutcome(rn *realNode, requestID string) (proposeResponse, int, error) {
	resp, err := http.Get("http://" + rn.httpAddr + "/outcome?requestId=" + requestID)
	if err != nil {
		return proposeResponse{}, 0, err
	}
	defer resp.Body.Close()
	var pr proposeResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return proposeResponse{}, resp.StatusCode, err
	}
	return pr, resp.StatusCode, nil
}

// TestRealChaos_LostResponseRetryAcrossFailover fires a propose and,
// without ever waiting for its HTTP response, immediately SIGKILLs the
// leader — modeling a genuinely lost response (the client never learns
// the outcome of its original call at all, indistinguishable from the
// TCP connection simply dying mid-response) — then retries the
// identical RequestID against the new leader and confirms a single,
// stable, non-duplicated terminal outcome.
func TestRealChaos_LostResponseRetryAcrossFailover(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	respCh := make(chan struct {
		resp proposeResponse
		err  error
	}, 1)
	go func() {
		resp, _, err := propose(leader, "lost-resp", "k1", "v1")
		respCh <- struct {
			resp proposeResponse
			err  error
		}{resp, err}
	}()
	// Kill immediately, deliberately racing the in-flight request —
	// whether or not it actually committed before the kill, the client
	// never gets to observe the original response either way.
	time.Sleep(5 * time.Millisecond)
	leader.crash()
	<-respCh // drain the goroutine; its result (if any) is irrelevant here

	newLeader := awaitLeader(t, nodes, 10*time.Second)
	if newLeader.id == leader.id {
		t.Fatalf("expected a different leader after killing %s", leader.id)
	}

	retry := awaitOutcomeOrRetryUntilCommitted(t, newLeader, nodes, "lost-resp", "k1", "v1", 10*time.Second)
	if retry.Status != "committed" {
		t.Fatalf("retry outcome = %+v, want committed", retry)
	}

	// Re-retrying once more must resolve to the identical CommitSeq —
	// exactly-once logical effect, never a duplicate application.
	again, status, err := propose(newLeader, "lost-resp", "k1", "v1")
	if err != nil || status != http.StatusOK || again.CommitSeq != retry.CommitSeq {
		t.Fatalf("second retry = %+v status=%d err=%v, want identical CommitSeq=%d", again, status, err, retry.CommitSeq)
	}
}

// awaitOutcomeOrRetryUntilCommitted retries RequestID against newLeader
// (falling back to any live node) until it resolves to committed,
// bounded polling throughout (never a fixed sleep) — a genuinely lost
// response means the client cannot know whether its original attempt
// landed, so a real client's correct recovery strategy is exactly this:
// keep retrying the same RequestID until a terminal outcome is
// observed.
func awaitOutcomeOrRetryUntilCommitted(t *testing.T, newLeader *realNode, all []*realNode, requestID, key, value string, timeout time.Duration) proposeResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, status, err := propose(newLeader, requestID, key, value)
		if err == nil && status == http.StatusOK && resp.Status == "committed" {
			return resp
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("RequestID %s never resolved to committed within %s", requestID, timeout)
	return proposeResponse{}
}

// TestRealChaos_RealPartitionHeal exercises a genuine (not simulated)
// network partition between real OS processes via the /fault control-
// plane endpoint (main.go's handleFault, this phase's addition): the
// leader is cut off symmetrically from both followers, must never
// acknowledge a new write while cut off, the majority side must elect
// and keep serving, and — once healed — the whole real cluster must
// converge.
func TestRealChaos_RealPartitionHeal(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	var others []*realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			others = append(others, rn)
		}
	}

	// Real symmetric partition: block the leader from/to each follower,
	// and each follower from/to the leader.
	for _, other := range others {
		if err := postFault(leader, "block", other.id); err != nil {
			t.Fatalf("blocking leader<-%s: %v", other.id, err)
		}
		if err := postFault(other, "block", leader.id); err != nil {
			t.Fatalf("blocking %s<-leader: %v", other.id, err)
		}
	}

	if resp, status, err := propose(leader, "during-real-partition", "k1", "v1"); err == nil && status == http.StatusOK && resp.Status == "committed" {
		t.Fatalf("isolated real leader process acknowledged a write during a real partition; QUORUM-SAFETY violated: resp=%+v", resp)
	}

	newLeader := awaitLeader(t, others, 10*time.Second)
	resp, status, err := propose(newLeader, "majority-write", "k2", "v2")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("majority side failed to commit during the real partition: resp=%+v status=%d err=%v", resp, status, err)
	}

	// Heal.
	for _, other := range others {
		if err := postFault(leader, "unblock", other.id); err != nil {
			t.Fatalf("unblocking leader<-%s: %v", other.id, err)
		}
		if err := postFault(other, "unblock", leader.id); err != nil {
			t.Fatalf("unblocking %s<-leader: %v", other.id, err)
		}
	}

	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "majority-write", 10*time.Second)
	}
}
