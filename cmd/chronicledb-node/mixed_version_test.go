//go:build integration

// This file is the "real mixed-binary cluster" proof
// docs/enterprise-v1-plan.md §7 requires: it builds two ACTUAL
// chronicledb-node binaries — one from the pre-v0.4.0 baseline commit
// (5637afa, "docs: correct v0.2.0 release status" — the last commit
// before any compatibility/rolling-upgrade code existed at all) and one
// from this working tree's own current source — and runs them together
// as real OS processes over real TCP/disk, exactly like
// main_test.go's existing single-binary real-process proof, but with
// two genuinely different binaries instead of one. It deliberately does
// NOT fake mixed-version behavior with one binary and different flags.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run MixedVersion -v
package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// buildBinaryAtRef builds chronicledb-node from git ref (a commit hash)
// rather than this working tree, via a temporary, detached git
// worktree — a real, independently-compiled binary reflecting exactly
// what that commit's source produced, not a synthetic stand-in. The
// worktree is created and torn down entirely outside the repository's
// actual working directory/index (git worktree add never touches
// either), and is removed in t.Cleanup regardless of test outcome.
func buildBinaryAtRef(t *testing.T, ref string) string {
	t.Helper()
	repoRoot := findRepoRoot(t)
	worktreeDir := t.TempDir()

	add := exec.Command("git", "worktree", "add", "--detach", worktreeDir, ref)
	add.Dir = repoRoot
	var addErr bytes.Buffer
	add.Stderr = &addErr
	if err := add.Run(); err != nil {
		t.Fatalf("git worktree add %s %s: %v\n%s", worktreeDir, ref, err, addErr.String())
	}
	t.Cleanup(func() {
		rm := exec.Command("git", "worktree", "remove", "--force", worktreeDir)
		rm.Dir = repoRoot
		rm.Run() // best-effort; a failure here does not invalidate the test that already ran
	})

	bin := worktreeDir + "/chronicledb-node-old"
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = worktreeDir + "/cmd/chronicledb-node"
	var buildErr bytes.Buffer
	build.Stderr = &buildErr
	if err := build.Run(); err != nil {
		t.Fatalf("building chronicledb-node at ref %s: %v\n%s", ref, err, buildErr.String())
	}
	return bin
}

// findRepoRoot locates the repository root via `git rev-parse
// --show-toplevel`, run from the current package directory (this test
// file's own directory, cmd/chronicledb-node, is always inside it).
func findRepoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("git rev-parse --show-toplevel: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// preV040Commit is the last real commit before any Compatibility /
// Rolling Upgrades code existed (docs/enterprise-v1-plan.md §7's
// baseline) — see this task's own baseline description. Building the
// "old" binary from here, rather than from a synthetic flag on the
// current binary, is exactly what "do not fake mixed-version proof
// using one binary with different flags" requires.
const preV040Commit = "5637afa"

// statusJSONv2 is statusJSON (main_test.go) extended with the two new
// fields this phase adds to node.Status — decoding an old binary's
// /status response (which never sends these fields) into this struct
// correctly leaves them at their zero value, exactly as
// raft.Message.SenderGeneration's own gob-based zero-value tolerance
// works at the wire layer (see that field's doc comment).
type statusJSONv2 struct {
	statusJSON
	ClusterGeneration      uint32 `json:"ClusterGeneration"`
	MaxSupportedGeneration uint32 `json:"MaxSupportedGeneration"`
}

func statusV2(rn *realNode) (statusJSONv2, error) {
	resp, err := http.Get("http://" + rn.httpAddr + "/status")
	if err != nil {
		return statusJSONv2{}, err
	}
	defer resp.Body.Close()
	var s statusJSONv2
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return statusJSONv2{}, err
	}
	return s, nil
}

func awaitLeaderV2(t *testing.T, nodes []*realNode, timeout time.Duration) *realNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *realNode
		count := 0
		for _, rn := range nodes {
			if rn.cmd == nil {
				continue
			}
			st, err := statusV2(rn)
			if err != nil {
				continue
			}
			if st.Role == roleLeader {
				leader = rn
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no single leader emerged among real mixed-version processes within timeout")
	return nil
}

type precheckPeerJSONt struct {
	Generation uint32 `json:"generation"`
	Known      bool   `json:"known"`
}
type precheckResponseT struct {
	LocalClusterGeneration      uint32                       `json:"localClusterGeneration"`
	LocalMaxSupportedGeneration uint32                       `json:"localMaxSupportedGeneration"`
	TargetGeneration            uint32                       `json:"targetGeneration"`
	Peers                       map[string]precheckPeerJSONt `json:"peers"`
	AlreadyFinalized            bool                         `json:"alreadyFinalized"`
	Ready                       bool                         `json:"ready"`
	Error                       string                       `json:"error,omitempty"`
}
type finalizeResponseT struct {
	Status        string `json:"status"`
	NewGeneration uint32 `json:"newGeneration,omitempty"`
	Error         string `json:"error,omitempty"`
	LeaderHint    string `json:"leaderHint,omitempty"`
}

func precheckHTTP(rn *realNode) (precheckResponseT, int, error) {
	resp, err := http.Get("http://" + rn.httpAddr + "/admin/upgrade/precheck")
	if err != nil {
		return precheckResponseT{}, 0, err
	}
	defer resp.Body.Close()
	var out precheckResponseT
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return precheckResponseT{}, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func finalizeHTTP(rn *realNode) (finalizeResponseT, int, error) {
	resp, err := http.Post("http://"+rn.httpAddr+"/admin/upgrade/finalize", "application/json", nil)
	if err != nil {
		return finalizeResponseT{}, 0, err
	}
	defer resp.Body.Close()
	var out finalizeResponseT
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return finalizeResponseT{}, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

// TestMixedVersion_CriticalUpgradeProofScenario runs exactly the
// numbered scenario the task requires:
//
//  1. Start a real cluster on the previous supported version.
//  2. Continue reads/writes.
//  3. Upgrade one node.
//  4. Continue traffic.
//  5. Upgrade another node.
//  6. Force leader failover while versions are mixed.
//  7. Continue traffic and retries.
//  8. Upgrade final node.
//  9. Finalize only when safe.
//  10. Restart the cluster.
//  11. Verify every acknowledged committed result, RequestID outcome,
//     and supported format boundary.
func TestMixedVersion_CriticalUpgradeProofScenario(t *testing.T) {
	oldBin := buildBinaryAtRef(t, preV040Commit)
	newBin := buildBinary(t)

	// --- 1. Start a real cluster on the previous supported version. ---
	nodes := newRealCluster(t, oldBin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	t.Logf("initial (all-old-binary) leader: %s", leader.id)

	var acked []string // RequestIDs whose committed outcome we must keep verifying
	mustCommit := func(rn *realNode, reqID, key, value string) proposeResponse {
		t.Helper()
		resp, status, err := propose(rn, reqID, key, value)
		if err != nil || status != http.StatusOK || resp.Status != "committed" {
			t.Fatalf("propose(%s): resp=%+v status=%d err=%v", reqID, resp, status, err)
		}
		acked = append(acked, reqID)
		return resp
	}
	verifyAllAcked := func(rn *realNode) {
		t.Helper()
		for _, id := range acked {
			awaitOutcomeCommitted(t, rn, id, 10*time.Second)
		}
	}

	// --- 2. Continue reads/writes. ---
	mustCommit(leader, "r1", "k1", "v1")
	verifyAllAcked(leader)

	// --- 3. Upgrade one node (a non-leader, the safe order an operator
	// actually follows). ---
	var firstUpgraded *realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			firstUpgraded = rn
			break
		}
	}
	firstUpgraded.crash()
	firstUpgraded.restart(t, newBin)
	awaitCondition2(t, 10*time.Second, "upgraded node "+firstUpgraded.id+" rejoins and catches up", func() bool {
		st, err := statusV2(firstUpgraded)
		return err == nil && st.AppliedIndex > 0
	})

	// --- 4. Continue traffic. ---
	leader = awaitLeaderV2(t, nodes, 10*time.Second) // may or may not have changed
	mustCommit(leader, "r2", "k2", "v2")
	verifyAllAcked(leader)

	// --- 5. Upgrade another node. ---
	var secondUpgraded *realNode
	for _, rn := range nodes {
		if rn.id != leader.id && rn.id != firstUpgraded.id {
			secondUpgraded = rn
			break
		}
	}
	secondUpgraded.crash()
	secondUpgraded.restart(t, newBin)
	awaitCondition2(t, 10*time.Second, "upgraded node "+secondUpgraded.id+" rejoins and catches up", func() bool {
		st, err := statusV2(secondUpgraded)
		return err == nil && st.AppliedIndex > 0
	})

	// Exactly two of three nodes are now on the new binary; the cluster
	// is genuinely mixed-version.
	leader = awaitLeaderV2(t, nodes, 10*time.Second)
	mustCommit(leader, "r3", "k3", "v3")
	verifyAllAcked(leader)

	// --- 6. Force leader failover while versions are mixed. ---
	oldLeaderID := leader.id
	leader.crash() // a real SIGKILL of whichever node is leader right now, old- or new-binary alike
	newLeader := awaitLeaderV2(t, nodes, 10*time.Second)
	if newLeader.id == oldLeaderID {
		t.Fatalf("expected a different leader after killing %s", oldLeaderID)
	}
	t.Logf("failover during mixed-version operation: %s -> %s", oldLeaderID, newLeader.id)

	// Restart the crashed node with WHICHEVER binary it was already
	// running before this failover (we are proving failover survives
	// mixed versions, not upgrading every node yet) — recover its own
	// current binary from firstUpgraded/secondUpgraded bookkeeping.
	crashedBin := oldBin
	if oldLeaderID == firstUpgraded.id || oldLeaderID == secondUpgraded.id {
		crashedBin = newBin
	}
	var crashed *realNode
	for _, rn := range nodes {
		if rn.id == oldLeaderID {
			crashed = rn
		}
	}
	crashed.restart(t, crashedBin)
	awaitCondition2(t, 10*time.Second, "failed-over node "+crashed.id+" rejoins", func() bool {
		st, err := statusV2(crashed)
		return err == nil && st.AppliedIndex > 0
	})

	// --- 7. Continue traffic and retries. ---
	leader = awaitLeaderV2(t, nodes, 10*time.Second)
	r4 := mustCommit(leader, "r4", "k4", "v4")
	verifyAllAcked(leader)
	// A retry of an already-committed RequestID against the (possibly
	// new) leader must return the identical CommitSeq, not re-evaluate.
	retry, status, err := propose(leader, "r4", "k4", "v4")
	if err != nil || status != http.StatusOK || retry.CommitSeq != r4.CommitSeq {
		t.Fatalf("retry of r4 against leader %s: retry=%+v status=%d err=%v, want CommitSeq=%d", leader.id, retry, status, err, r4.CommitSeq)
	}

	// --- 8. Upgrade final node. ---
	var lastOld *realNode
	for _, rn := range nodes {
		if rn.id != firstUpgraded.id && rn.id != secondUpgraded.id {
			lastOld = rn
		}
	}
	lastOld.crash()
	lastOld.restart(t, newBin)
	awaitCondition2(t, 10*time.Second, "final node "+lastOld.id+" upgraded and rejoined", func() bool {
		st, err := statusV2(lastOld)
		return err == nil && st.AppliedIndex > 0
	})

	leader = awaitLeaderV2(t, nodes, 10*time.Second)
	mustCommit(leader, "r5", "k5", "v5")
	verifyAllAcked(leader)

	// Every node is now on the new binary. Precheck must report Ready
	// once every peer's generation has been observed via live traffic.
	awaitCondition2(t, 10*time.Second, "precheck on leader reports Ready", func() bool {
		res, status, err := precheckHTTP(leader)
		return err == nil && status == http.StatusOK && res.Ready && !res.AlreadyFinalized
	})

	// --- 9. Finalize only when safe. ---
	finResp, finStatus, err := finalizeHTTP(leader)
	if err != nil || finStatus != http.StatusOK || finResp.Status != "committed" {
		t.Fatalf("finalize: resp=%+v status=%d err=%v", finResp, finStatus, err)
	}
	if finResp.NewGeneration != 1 {
		t.Fatalf("finalize NewGeneration = %d, want 1", finResp.NewGeneration)
	}
	for _, rn := range nodes {
		rn := rn
		awaitCondition2(t, 10*time.Second, "node "+rn.id+" converges on ClusterGeneration=1", func() bool {
			st, err := statusV2(rn)
			return err == nil && st.ClusterGeneration == 1
		})
	}

	mustCommit(leader, "r6", "k6", "v6")
	verifyAllAcked(leader)

	// --- 10. Restart the cluster. ---
	for _, rn := range nodes {
		rn.crash()
	}
	for _, rn := range nodes {
		rn.restart(t, newBin)
	}
	leader = awaitLeaderV2(t, nodes, 15*time.Second)

	// --- 11. Verify every acknowledged committed result, RequestID
	// outcome, and supported format boundary. ---
	for _, rn := range nodes {
		verifyAllAcked(rn)
		st, err := statusV2(rn)
		if err != nil {
			t.Fatalf("status after full-cluster restart: %v", err)
		}
		if st.ClusterGeneration != 1 {
			t.Fatalf("node %s ClusterGeneration after full-cluster restart = %d, want 1 (recovered purely from local durable state)", rn.id, st.ClusterGeneration)
		}
		if st.MaxSupportedGeneration != 1 {
			t.Fatalf("node %s MaxSupportedGeneration = %d, want 1", rn.id, st.MaxSupportedGeneration)
		}
	}
	// A write after the full restart still works normally.
	mustCommit(leader, "r7", "k7", "v7")
	verifyAllAcked(leader)

	t.Logf("critical mixed-version upgrade proof scenario completed: %d RequestIDs verified across every node after finalize + full-cluster restart", len(acked))
}

// awaitCondition2 is this file's local polling helper (main_test.go has
// no equivalent — its tests poll inline). Fails the test if cond never
// becomes true within timeout.
func awaitCondition2(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// TestMixedVersion_RollbackBeforeFinalizeIsSafe proves the documented
// rollback boundary's SAFE side: redeploying the old binary onto a node
// that was upgraded to the new binary, strictly before finalize, is
// safe — the cluster keeps operating normally and the rolled-back node
// rejoins and replicates correctly (docs/enterprise-v1-plan.md §7
// Acceptance criteria: "Rollback... is proven safe by real test").
func TestMixedVersion_RollbackBeforeFinalizeIsSafe(t *testing.T) {
	oldBin := buildBinaryAtRef(t, preV040Commit)
	newBin := buildBinary(t)

	nodes := newRealCluster(t, oldBin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)

	resp, status, err := propose(leader, "pre-upgrade", "k1", "v1")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("initial propose: resp=%+v status=%d err=%v", resp, status, err)
	}

	// Upgrade a follower to the new binary.
	var follower *realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			follower = rn
			break
		}
	}
	follower.crash()
	follower.restart(t, newBin)
	awaitCondition2(t, 10*time.Second, "upgraded follower rejoins", func() bool {
		st, err := statusV2(follower)
		return err == nil && st.AppliedIndex > 0
	})

	// Now roll it BACK to the old binary, strictly before any finalize
	// was ever attempted.
	follower.crash()
	follower.restart(t, oldBin)
	awaitCondition2(t, 10*time.Second, "rolled-back follower rejoins with the OLD binary again", func() bool {
		st, err := statusV2(follower)
		return err == nil && st.AppliedIndex > 0
	})

	// The cluster must still operate completely normally: a fresh write
	// through the current leader replicates to every node, including the
	// rolled-back one.
	leader = awaitLeaderV2(t, nodes, 10*time.Second)
	resp2, status2, err := propose(leader, "post-rollback", "k2", "v2")
	if err != nil || status2 != http.StatusOK || resp2.Status != "committed" {
		t.Fatalf("post-rollback propose: resp=%+v status=%d err=%v", resp2, status2, err)
	}
	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "pre-upgrade", 10*time.Second)
		awaitOutcomeCommitted(t, rn, "post-rollback", 10*time.Second)
	}
}

// TestMixedVersion_OldBinaryRejectedAfterFinalize proves the rollback
// boundary's REFUSAL side (docs/enterprise-v1-plan.md §7 Acceptance
// criteria: "rollback after finalize is proven to be correctly refused/
// documented as unsupported"): once a cluster has been finalized to
// generation 1, starting the pre-v0.4.0 binary against one of its
// (now generation-1) data directories must fail closed rather than
// silently misinterpret the replicated finalize command it cannot
// decode — proven here by capturing that process's own stderr output.
func TestMixedVersion_OldBinaryRejectedAfterFinalize(t *testing.T) {
	oldBin := buildBinaryAtRef(t, preV040Commit)
	newBin := buildBinary(t)

	// Bring up a real 3-node cluster entirely on the new binary, finalize
	// it, then shut it down cleanly — leaving one node's real, on-disk
	// data directory genuinely finalized to generation 1.
	nodes := newRealCluster(t, newBin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)

	awaitCondition2(t, 10*time.Second, "precheck reports Ready", func() bool {
		res, status, err := precheckHTTP(leader)
		return err == nil && status == http.StatusOK && res.Ready
	})
	finResp, finStatus, err := finalizeHTTP(leader)
	if err != nil || finStatus != http.StatusOK || finResp.Status != "committed" {
		t.Fatalf("finalize: resp=%+v status=%d err=%v", finResp, finStatus, err)
	}
	for _, rn := range nodes {
		rn := rn
		awaitCondition2(t, 10*time.Second, "node "+rn.id+" converges on ClusterGeneration=1", func() bool {
			st, err := statusV2(rn)
			return err == nil && st.ClusterGeneration == 1
		})
	}

	// Crash exactly ONE node and restart it with the OLD binary while the
	// other two remain alive, still on the new binary, still finalized —
	// a real rollback attempt against a live, already-finalized cluster,
	// not merely local replay of an already-durable tail: the rejoining
	// old binary must receive live AppendEntries traffic from the
	// current leader advancing its commitIndex past the finalize entry
	// (raft.NewCoreFromSnapshot resets commitIndex to the local snapshot
	// boundary on every restart — a restarted node never trusts its own
	// merely-durable log tail as committed until a leader reconfirms it,
	// per ordinary Raft safety), causing it to actually attempt applying
	// that entry and fail closed.
	target := nodes[0]
	target.crash()

	// Capture stderr instead of passing it through, so this test can
	// assert on the fail-closed diagnostic rather than merely on the
	// process not crashing the test harness.
	var stderr syncBuffer
	args := append([]string{}, target.args...)
	args = append(args, "-http="+target.httpAddr, "-datadir="+target.dataDir)
	cmd := exec.Command(oldBin, args...)
	cmd.Stdout = &stderr
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting old binary against finalized data directory: %v", err)
	}
	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		close(exited)
	}()
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		<-exited
	}()

	awaitCondition2(t, 15*time.Second, "old binary fails closed on the live-replicated finalize command", func() bool {
		s := stderr.String()
		return strings.Contains(s, "unsupported CommitTxn command version") ||
			strings.Contains(s, "fatal error") ||
			strings.Contains(s, "decoding committed entry")
	})
	t.Logf("old binary's captured stderr (fail-closed proof):\n%s", stderr.String())

	// The rejected node's own process must have actually stopped serving
	// (main.go returns once Node.Done() fires from a fatal error) —
	// proving this is a real, externally observable refusal, not merely
	// a log line with the process otherwise still silently running.
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("old binary's process never exited after failing closed")
	}
}

// syncBuffer is a concurrency-safe bytes.Buffer, needed because
// exec.Cmd writes to Stdout/Stderr from a separate goroutine than the
// one polling String() below.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
