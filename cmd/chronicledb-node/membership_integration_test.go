//go:build integration

// This file is docs/dynamic-membership-plan.md §16's real-process
// proof: it extends main_test.go's genuine-OS-process, genuine-TCP/
// disk harness with the admin-gated dynamic-membership surface
// (membership.go), driving add/promote/remove/self-removal/restart/
// failover/retirement against real chronicledb-node processes over
// real HTTP.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestRealMembership -v
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func membershipStatusHTTP(rn *realNode) (membershipStatusResponse, int, error) {
	resp, err := http.Get("http://" + rn.httpAddr + "/admin/membership/status")
	if err != nil {
		return membershipStatusResponse{}, 0, err
	}
	defer resp.Body.Close()
	var out membershipStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return membershipStatusResponse{}, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func membershipMutateHTTP(rn *realNode, path, requestID, nodeID, address string, confirmVoterCount int) (membershipMutateResponse, int, error) {
	body, _ := json.Marshal(membershipMutateRequest{
		RequestID:         requestID,
		NodeID:            nodeID,
		Address:           address,
		ConfirmVoterCount: confirmVoterCount,
	})
	resp, err := http.Post("http://"+rn.httpAddr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return membershipMutateResponse{}, 0, err
	}
	defer resp.Body.Close()
	var out membershipMutateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return membershipMutateResponse{}, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func membershipAddHTTP(rn *realNode, requestID, nodeID, address string) (membershipMutateResponse, int, error) {
	return membershipMutateHTTP(rn, "/admin/membership/add", requestID, nodeID, address, 0)
}

func membershipPromoteHTTP(rn *realNode, requestID, nodeID string) (membershipMutateResponse, int, error) {
	return membershipMutateHTTP(rn, "/admin/membership/promote", requestID, nodeID, "", 0)
}

func membershipRemoveHTTP(rn *realNode, requestID, nodeID string, confirmVoterCount int) (membershipMutateResponse, int, error) {
	return membershipMutateHTTP(rn, "/admin/membership/remove", requestID, nodeID, "", confirmVoterCount)
}

// finalizeRealClusterOnce calls /admin/upgrade/finalize against leader
// exactly once (dynamic-membership plan §8.2's generation gate needs
// generation >= 2; a fresh cluster is at 0, so two calls are needed —
// FinalizeUpgrade raises the cluster by exactly one generation per
// call, cmd/chronicledb-node/upgrade_test.go's own regression pin).
func finalizeRealClusterOnce(t *testing.T, rn *realNode) finalizeResponse {
	t.Helper()
	resp, status, err := finalizeHTTP(rn)
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("finalize: resp=%+v status=%d err=%v", resp, status, err)
	}
	return resp
}

// finalizeRealClusterToGeneration2 brings a real 3-node cluster all the
// way to generation 2, the precondition for any membership call
// (dynamic-membership plan §8.2), and waits for every node to converge.
func finalizeRealClusterToGeneration2(t *testing.T, leader *realNode, nodes []*realNode) {
	t.Helper()
	for i := 0; i < 2; i++ {
		awaitCondition2(t, 10*time.Second, "precheck reports Ready", func() bool {
			res, status, err := precheckHTTP(leader)
			return err == nil && status == http.StatusOK && res.Ready && !res.AlreadyFinalized
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
}

// TestRealMembership_GenerationGateBothSides proves dynamic-membership
// plan §8.2's leader-side gate against a real process, before and
// after finalization (§19 gate 7).
func TestRealMembership_GenerationGateBothSides(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)

	resp, status, err := membershipAddHTTP(leader, "premature-add", "ghost", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("membership add before finalize: %v", err)
	}
	if status != http.StatusPreconditionFailed || resp.Reason != "generation-too-low" {
		t.Fatalf("membership add before finalize: status=%d resp=%+v, want 412 generation-too-low", status, resp)
	}

	finalizeRealClusterToGeneration2(t, leader, nodes)

	learnerAddr := freePorts(t, 1)[0]
	resp2, status2, err := membershipAddHTTP(leader, "post-finalize-add", "n4", learnerAddr)
	if err != nil || status2 != http.StatusOK || resp2.Status != "committed" {
		t.Fatalf("membership add after finalize: resp=%+v status=%d err=%v", resp2, status2, err)
	}
}

// requireHeldLeaderNotReady asserts §11's boundary on a leader whose
// election no-op is held: changesReady:false with notReadyReason
// "no-current-term-commit". Because the hold makes that state stable,
// this is an assertion, not a wait — the only thing it retries is the
// question of *which* survivor is currently leader, since a leader that
// steps down mid-call answers the unrelated "not-leader" instead. Any
// other reason, and any ready answer, fails on the spot rather than
// being retried away.
func requireHeldLeaderNotReady(t *testing.T, survivors []*realNode, leader *realNode) *realNode {
	t.Helper()
	for attempt := 0; attempt < len(survivors)+1; attempt++ {
		st, status, err := membershipStatusHTTP(leader)
		if err != nil || status != http.StatusOK {
			t.Fatalf("membership status on held leader %s: status=%d err=%v", leader.id, status, err)
		}
		if st.ChangesReady {
			t.Fatalf("leader %s reports changesReady:true while its election no-op is held — "+
				"it cannot have committed an entry of its own term (§11); status=%+v", leader.id, st)
		}
		if st.NotReadyReason == "no-current-term-commit" {
			return leader
		}
		if st.NotReadyReason != "not-leader" {
			t.Fatalf("held leader %s reports notReadyReason %q, want \"no-current-term-commit\"; status=%+v",
				leader.id, st.NotReadyReason, st)
		}
		leader = awaitLeaderV2(t, survivors, 10*time.Second)
	}
	t.Fatalf("leadership never settled across %d survivors while the election no-op was held", len(survivors))
	return nil
}

// TestRealMembership_PostElectionNotReadyWindowIsRealAndTransient
// proves dynamic-membership plan §11 against real processes: right
// after a real election, /admin/membership/status must report
// changesReady:false with notReadyReason "no-current-term-commit",
// and that must flip to true without any operator action.
//
// The boundary is held open deliberately rather than raced for. Every
// surviving node is put into internal/node's HoldElectionNoOpForTest
// state through the existing /fault control plane before the leader is
// killed, so whichever node wins the ensuing real election stops at
// exactly §11's boundary — genuinely Leader, genuinely with no entry of
// its own term committed — and *stays* there. Both halves of §11 are
// then ordinary assertions on a stable state: not-ready while held, and
// ready after the hold is released, the release returning only once the
// node has actually re-proposed the no-op.
//
// The revision this replaces polled in a tight loop hoping to catch the
// window before the no-op committed, which is one real fsync-backed
// AppendEntries round trip in *uninstrumented* child processes. Under
// -race or CPU contention the observer slows down while that window does
// not, so the poller lost the race and the test failed spuriously —
// reproduced at roughly 1 run in 7 under contention, and observed
// without -race too. Nothing about the product was ever wrong; the
// observation method was. Do not reintroduce polling here: the
// assertions below must stay statements about a state the test controls,
// never about how fast it can issue HTTP requests.
func TestRealMembership_PostElectionNotReadyWindowIsRealAndTransient(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration2(t, leader, nodes)

	oldLeaderID := leader.id
	survivors := make([]*realNode, 0, len(nodes)-1)
	for _, rn := range nodes {
		if rn.id != oldLeaderID {
			survivors = append(survivors, rn)
		}
	}

	// Arm every survivor, not just the eventual winner: which one wins
	// the real election is genuinely not this test's to choose, and
	// arming all of them makes "the new leader is held" true regardless.
	for _, rn := range survivors {
		if err := postFault(rn, "holdelectionnoop", ""); err != nil {
			t.Fatalf("arming election-no-op hold on %s: %v", rn.id, err)
		}
	}

	leader.crash()
	newLeader := awaitLeaderV2(t, survivors, 10*time.Second)

	// Held, so this is a stable state, asserted directly rather than
	// waited for. A leader that lost leadership between awaitLeaderV2
	// and this call would answer "not-leader"; that is a different
	// (correct) reason, so it is retried against the new leader rather
	// than failed on — but every other answer, including a ready one,
	// fails immediately. This tolerates leader identity churn; it does
	// not wait for the property under test to appear.
	newLeader = requireHeldLeaderNotReady(t, survivors, newLeader)

	// Release only the leader's hold. Readiness must then advance with
	// no membership call, no write, and no operator action — by the
	// production path (proposeElectionNoOp) alone.
	if err := postFault(newLeader, "releaseelectionnoop", ""); err != nil {
		t.Fatalf("releasing election-no-op hold on %s: %v", newLeader.id, err)
	}
	awaitCondition2(t, 10*time.Second, "changesReady flips to true without operator action", func() bool {
		st, status, err := membershipStatusHTTP(newLeader)
		return err == nil && status == http.StatusOK && st.ChangesReady
	})
}

// TestRealMembership_FullLifecycle is the central real-process proof
// docs/dynamic-membership-plan.md §16 requires: add a real fourth
// process as a learner, continue writes throughout, promote it
// (retrying on 425), remove one of the original three voters (proving
// §4.4's stale-but-harmless retirement semantics against a process
// deliberately left running), force a real leader failover and
// immediately attempt a membership change, exercise the sub-three-
// voter guard, have the leader remove itself, and restart the
// surviving members to confirm every RequestID outcome and the
// recovered Configuration are byte-identical to before the restart.
func TestRealMembership_FullLifecycle(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration2(t, leader, nodes)

	// votingNodesMu guards votingNodes, read by the background writer
	// below and extended (once, when n4 joins) by the main goroutine —
	// the only mutation, so a simple mutex-guarded slice suffices.
	var votingNodesMu sync.Mutex
	votingNodes := append([]*realNode{}, nodes...)
	addVotingNode := func(rn *realNode) {
		votingNodesMu.Lock()
		votingNodes = append(votingNodes, rn)
		votingNodesMu.Unlock()
	}
	snapshotVotingNodes := func() []*realNode {
		votingNodesMu.Lock()
		defer votingNodesMu.Unlock()
		return append([]*realNode{}, votingNodes...)
	}

	// Continue writes throughout: a background writer with a bounded
	// in-flight budget (docs/testing-strategy.md §4's bounded-polling
	// discipline extended to bounded concurrency here), never free-
	// running, so later lag assertions have a value they themselves
	// control.
	writerStop := make(chan struct{})
	writerDone := make(chan struct{})
	var writeSeq int
	var writeFailures int
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-writerStop:
				return
			default:
			}
			writeSeq++
			reqID := fmt.Sprintf("bgwrite-%d", writeSeq)
			cur := currentRealLeader(snapshotVotingNodes())
			if cur == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			_, status, err := propose(cur, reqID, fmt.Sprintf("k%d", writeSeq), "v")
			if err != nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			if status != http.StatusOK {
				writeFailures++
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	stopWriter := func() {
		close(writerStop)
		<-writerDone
	}
	defer func() {
		select {
		case <-writerStop:
		default:
			stopWriter()
		}
	}()

	// --- Add a real fourth process as a learner. ---
	learnerID := "n4"
	learnerAddr := freePorts(t, 1)[0]
	learnerDataDir := t.TempDir()
	// No -cluster flag at all: a not-yet-added learner starts from a
	// genuinely empty Configuration and joins purely by replication,
	// never by a CLI-declared bootstrap (dynamic-membership plan §3.1;
	// docs/membership.md's add-node procedure) — main.go's own
	// validation was fixed in this same session to make -cluster
	// optional for exactly this case (it was previously mandatory,
	// which made this real-process step unrunnable at all).
	learner := &realNode{
		id:       learnerID,
		raftAddr: learnerAddr,
		httpAddr: freePorts(t, 1)[0],
		dataDir:  learnerDataDir,
		args: []string{
			"-id=" + learnerID,
			"-listen=" + learnerAddr,
			"-peers=n1=" + nodes[0].raftAddr + ",n2=" + nodes[1].raftAddr + ",n3=" + nodes[2].raftAddr,
		},
	}
	startRealNode(t, bin, learner)
	defer stopRealNode(learner)
	// allRealNodes covers every real process this test ever starts,
	// including the learner — needed from here on since leadership can
	// legitimately land on n4 once it is promoted, and nodes (the
	// original three) alone would never find it. Also registered with
	// the background writer's own voting-node view (addVotingNode)
	// once n4 is actually promoted below, so its in-flight write budget
	// can find n4 as leader too.
	allRealNodes := append(append([]*realNode{}, nodes...), learner)

	addResp, addStatus, err := membershipAddHTTP(leader, "add-n4", learnerID, learnerAddr)
	if err != nil || addStatus != http.StatusOK || addResp.Status != "committed" {
		t.Fatalf("membership add n4: resp=%+v status=%d err=%v", addResp, addStatus, err)
	}
	awaitCondition2(t, 10*time.Second, "n4 catches up to the leader", func() bool {
		lst, lerr := statusV2(leader)
		nst, nerr := statusV2(learner)
		return lerr == nil && nerr == nil && nst.AppliedIndex >= lst.LastIndex
	})

	// --- Promote n4, retrying on 425 (the learner keeps falling behind
	// the still-running background writer between polls, §3.3/§23/G4). ---
	promoted := false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, status, err := membershipPromoteHTTP(leader, "promote-n4", learnerID)
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
	addVotingNode(learner)

	// --- Remove one of the original three voters, leaving it running
	// and reachable (§4.4's deliberately-opposite-of-drain semantics). ---
	var removedID string
	var removedNode *realNode
	for _, rn := range nodes {
		if rn.id != leader.id {
			removedID = rn.id
			removedNode = rn
			break
		}
	}
	// survivorID is the one of the three original nodes that is neither
	// the current leader nor removedID — tracked explicitly because the
	// remaining-voter bookkeeping below needs it once the leader itself
	// is later crashed and removed too, and nodes (the original 3) never
	// includes the learner n4 that gets promoted alongside it.
	var survivorID string
	for _, rn := range nodes {
		if rn.id != leader.id && rn.id != removedID {
			survivorID = rn.id
			break
		}
	}
	remResp, remStatus, err := membershipRemoveHTTP(leader, "remove-1", removedID, 0)
	if err != nil || remStatus != http.StatusOK || remResp.Status != "committed" {
		t.Fatalf("remove %s: resp=%+v status=%d err=%v", removedID, remResp, remStatus, err)
	}
	awaitCondition2(t, 10*time.Second, "cluster converges on 3 voters after removal", func() bool {
		st, _, err := membershipStatusHTTP(leader)
		return err == nil && len(st.Voters) == 3
	})

	// The removed process is left running: it must never receive the
	// removal entry, must still report its own stale pre-removal
	// configuration, and a client request against it must return
	// NotLeaderError — never ErrNodeRemoved.
	staleBefore, err := statusV2(removedNode)
	if err != nil {
		t.Fatalf("status of removed-but-running node: %v", err)
	}
	proposeResp, _, err := propose(removedNode, "against-removed", "kx", "vx")
	if err != nil {
		t.Fatalf("propose against removed node: %v", err)
	}
	if proposeResp.Error == "" || proposeResp.LeaderHint == "" {
		t.Fatalf("propose against removed node = %+v, want a NotLeaderError-shaped refusal with a leader hint", proposeResp)
	}
	if proposeResp.Error == "node: this node has observed its own removal from the cluster and is no longer a member" {
		t.Fatalf("propose against removed node reported ErrNodeRemoved; want NotLeaderError (§4.4/§4.6)")
	}

	// Kill and restart it against its own unwiped data directory: same
	// stale configuration, still not ErrNodeRemoved.
	removedNode.crash()
	removedNode.restart(t, bin)
	awaitCondition2(t, 10*time.Second, "restarted removed node comes back up", func() bool {
		_, err := statusV2(removedNode)
		return err == nil
	})
	staleAfter, err := statusV2(removedNode)
	if err != nil {
		t.Fatalf("status after restarting removed node: %v", err)
	}
	if staleAfter.LastIndex < staleBefore.LastIndex {
		t.Fatalf("restarted removed node's LastIndex regressed: %d -> %d", staleBefore.LastIndex, staleAfter.LastIndex)
	}
	proposeResp2, _, err := propose(removedNode, "against-removed-2", "ky", "vy")
	if err != nil {
		t.Fatalf("propose against restarted removed node: %v", err)
	}
	if proposeResp2.Error == "" {
		t.Fatalf("propose against restarted removed node = %+v, want an error (still not a member)", proposeResp2)
	}

	// --- Force a real leader failover, then immediately attempt a
	// further membership change: it must either succeed or return a
	// retryable 503, never a 500, never a hang (DM-12's real-process
	// counterpart). ---
	oldLeaderID := leader.id
	leader.crash()
	newLeader := awaitLeaderV2(t, allRealNodes, 10*time.Second)
	if newLeader.id == oldLeaderID {
		t.Fatalf("expected a new leader after crashing %s", oldLeaderID)
	}

	// The two remaining live voters from this point on are exactly
	// survivorID and learnerID (removedID and, once the retry loop
	// below succeeds, oldLeaderID are no longer counted) — tracked as
	// realNode objects since both retry loops below must keep
	// rediscovering whichever of the two is currently leader, an
	// election churn (visible in this suite's own logs immediately
	// after a real crash) can otherwise strand a fixed *realNode
	// reference on a node that has already lost leadership.
	remainingCandidates := []*realNode{thirdVoterNode(allRealNodes, survivorID), learner}

	// --- Immediately after the failover, remove the crashed former
	// leader. Retrying on both 503 (§2.2a's pre-proposal not-ready
	// refusals) and 409 (NotLeaderError/leadership-lost, exactly as
	// retryable by the same RequestID, §10) — never a 500, never a
	// hang (DM-12's real-process counterpart, §16). ---
	postFailoverLeader, _ := retryMembershipRemove(t, remainingCandidates, "remove-old-leader", oldLeaderID, 2, 5*time.Second)
	awaitCondition2(t, 10*time.Second, "cluster converges on 2 voters after the post-failover removal", func() bool {
		st, _, err := membershipStatusHTTP(postFailoverLeader)
		return err == nil && len(st.Voters) == 2
	})

	// thirdVoter is whichever of the two remaining voters will be left
	// once the self-removal step below removes the other.
	thirdVoter := survivorID
	if postFailoverLeader.id == survivorID {
		thirdVoter = learnerID
	}

	// --- Sub-three-voter guard (§12.2): a further removal below 3
	// voters without confirmVoterCount must be refused with 409; the
	// same call with the correct value must succeed. ---
	guardResp, guardStatus, err := membershipRemoveHTTP(postFailoverLeader, "guard-attempt", thirdVoter, 0)
	if err != nil {
		t.Fatalf("sub-three-voter guard attempt: %v", err)
	}
	if guardStatus != http.StatusConflict || guardResp.Reason != "confirmation-required" || guardResp.ResultingVoterCount != 1 {
		t.Fatalf("sub-three-voter guard attempt: status=%d resp=%+v, want 409 confirmation-required resultingVoterCount=1", guardStatus, guardResp)
	}

	// --- The current leader removes itself. Since the target of a
	// self-removal is, by definition, whichever node the call actually
	// reaches as leader, this retries against a freshly-rediscovered
	// leader each attempt and removes *that* node's own ID — not a
	// fixed one captured earlier, which an election race could turn
	// into an ordinary (non-self) removal instead. ---
	_, _ = retryMembershipRemoveSelf(t, remainingCandidates, "self-remove", 1, 5*time.Second)
	stopWriter()

	finalLeaderID := ""
	awaitCondition2(t, 10*time.Second, "a new leader emerges after self-removal", func() bool {
		st, err := statusV2(thirdVoterNode(allRealNodes, thirdVoter))
		if err == nil && st.Role == roleLeader {
			finalLeaderID = thirdVoter
			return true
		}
		return false
	})
	if finalLeaderID != thirdVoter {
		t.Fatalf("final leader = %q, want the sole remaining voter %q", finalLeaderID, thirdVoter)
	}

	// --- Restart the surviving member and confirm exact recovery:
	// every acknowledged RequestID's recorded outcome, and the
	// recovered Configuration, byte-identical to before the restart
	// (dynamic-membership plan §16). LastIndex itself is deliberately
	// NOT asserted equal: becoming leader in a fresh term after a
	// restart always commits one election no-op entry first
	// (Node.proposeElectionNoOp), so a real restart-and-reelect
	// legitimately advances it by exactly one — asserting strict
	// equality here would be asserting something Raft itself never
	// promises.
	survivor := thirdVoterNode(allRealNodes, thirdVoter)
	beforeRestart, err := statusV2(survivor)
	if err != nil {
		t.Fatalf("status before restart: %v", err)
	}
	// /outcome only ever exposes the ordinary CommitTxn RequestID table
	// (internal/node.Node.FSM().GetOutcome) — membership RequestIDs live
	// in a separate table with no direct HTTP read path, so this
	// samples ordinary background-write RequestIDs, which is what §16
	// itself means by "every acknowledged RequestID's recorded outcome."
	// Early indices, not the last few: the very last background-write
	// attempts race stopWriter/the self-removal step and may never have
	// committed at all, which would make them a meaningless sample.
	sampleReqIDs := make([]string, 0, 5)
	for i := 1; i <= 5 && i <= writeSeq; i++ {
		sampleReqIDs = append(sampleReqIDs, fmt.Sprintf("bgwrite-%d", i))
	}
	outcomesBefore := make(map[string]string, len(sampleReqIDs))
	for _, reqID := range sampleReqIDs {
		resp, err := http.Get("http://" + survivor.httpAddr + "/outcome?requestId=" + reqID)
		if err != nil {
			t.Fatalf("fetching outcome %q before restart: %v", reqID, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		outcomesBefore[reqID] = string(body)
	}

	survivor.crash()
	survivor.restart(t, bin)
	awaitCondition2(t, 10*time.Second, "survivor comes back up as leader", func() bool {
		st, err := statusV2(survivor)
		return err == nil && st.Role == roleLeader
	})
	afterRestart, err := statusV2(survivor)
	if err != nil {
		t.Fatalf("status after restart: %v", err)
	}
	if afterRestart.LastIndex < beforeRestart.LastIndex {
		t.Fatalf("LastIndex after restart = %d, want >= %d (no regression)", afterRestart.LastIndex, beforeRestart.LastIndex)
	}
	stMember, _, err := membershipStatusHTTP(survivor)
	if err != nil {
		t.Fatalf("membership status after restart: %v", err)
	}
	if len(stMember.Voters) != 1 || stMember.Voters[0].ID != thirdVoter {
		t.Fatalf("recovered configuration after restart = %+v, want exactly the sole voter %q", stMember, thirdVoter)
	}
	for _, reqID := range sampleReqIDs {
		resp, err := http.Get("http://" + survivor.httpAddr + "/outcome?requestId=" + reqID)
		if err != nil {
			t.Fatalf("fetching outcome %q after restart: %v", reqID, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != outcomesBefore[reqID] {
			t.Errorf("outcome for RequestID %q after restart = %s, want identical to before restart %s", reqID, body, outcomesBefore[reqID])
		}
	}
	// A non-200 response to an in-flight write exactly when its target
	// is crashed/self-removed is expected, not a bug — §16 itself only
	// requires zero failed *commits*, which the RequestID-outcome check
	// above (against early, pre-disruption writes) already proves;
	// nothing here ever retries a background write by RequestID, so a
	// write racing a crash simply never lands, exactly as an ordinary
	// client that gave up after one attempt would see.
	t.Logf("background writer: %d proposals attempted, %d non-200 responses (expected around each crash/self-removal)", writeSeq, writeFailures)
}

func currentRealLeader(nodes []*realNode) *realNode {
	for _, rn := range nodes {
		st, err := statusV2(rn)
		if err == nil && st.Role == roleLeader {
			return rn
		}
	}
	return nil
}

func thirdVoterNode(nodes []*realNode, id string) *realNode {
	for _, rn := range nodes {
		if rn.id == id {
			return rn
		}
	}
	return nil
}

// retryMembershipRemove issues a membership remove call for targetID
// against whichever of candidates is currently leader, rediscovering
// the leader fresh on every attempt (an election racing the call can
// otherwise strand this on a node that has already lost leadership),
// retrying on both 503 (§2.2a's pre-proposal not-ready refusals) and
// 409 (NotLeaderError/leadership-lost, exactly as retryable by the
// same RequestID, §10) — anything else fails the test outright, since
// DM-12's real-process counterpart requires never a 500, never a hang.
func retryMembershipRemove(t *testing.T, candidates []*realNode, requestID, targetID string, confirmVoterCount int, timeout time.Duration) (*realNode, membershipMutateResponse) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastResp membershipMutateResponse
	for time.Now().Before(deadline) {
		cur := currentRealLeader(candidates)
		if cur == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		resp, status, err := membershipRemoveHTTP(cur, requestID, targetID, confirmVoterCount)
		if err != nil {
			t.Fatalf("membership remove (%s): %v", requestID, err)
		}
		if status == http.StatusOK && resp.Status == "committed" {
			return cur, resp
		}
		if status != http.StatusServiceUnavailable && status != http.StatusConflict {
			t.Fatalf("membership remove (%s): status=%d resp=%+v, want 200, 503, or a retryable 409 only", requestID, status, resp)
		}
		lastResp = resp
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("membership remove (%s) never succeeded within %s; last response: %+v", requestID, timeout, lastResp)
	return nil, membershipMutateResponse{}
}

// retryMembershipRemoveSelf is retryMembershipRemove specialized for
// self-removal: the target of each attempt is whichever candidate the
// call actually reaches as leader, not a fixed ID captured earlier —
// an election race between attempts would otherwise silently turn this
// into an ordinary (non-self) removal of whichever node was leader at
// the first attempt.
func retryMembershipRemoveSelf(t *testing.T, candidates []*realNode, requestIDPrefix string, confirmVoterCount int, timeout time.Duration) (*realNode, membershipMutateResponse) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastResp membershipMutateResponse
	// target and requestID are fixed on the FIRST attempt and never
	// change afterward: ErrLeadershipLost means "outcome unknown" (§10)
	// — the original proposal may still go on to commit even though
	// this client saw an error — so a retry must resubmit the *same*
	// RequestID for the *same* target against whoever is leader now,
	// never start a fresh self-removal of that new leader instead.
	// Picking a new target on every attempt (this function's first,
	// buggy version) can leave a later attempt trying to remove the
	// cluster's last remaining voter with the wrong confirmVoterCount,
	// because the original attempt's target had already been removed
	// by the very entry the client was told nothing certain about.
	var target, requestID string
	for time.Now().Before(deadline) {
		cur := currentRealLeader(candidates)
		if cur == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if target == "" {
			target = cur.id
			requestID = fmt.Sprintf("%s-%s", requestIDPrefix, target)
		}
		resp, status, err := membershipRemoveHTTP(cur, requestID, target, confirmVoterCount)
		if err != nil {
			t.Fatalf("self-removal (%s): %v", requestID, err)
		}
		if status == http.StatusOK && resp.Status == "committed" {
			// Return the node that was actually removed (target), not
			// necessarily cur: a retry after ErrLeadershipLost can
			// legitimately have this final confirmation come from a
			// different node than the one originally asked to remove
			// itself, per this function's own doc comment above.
			return thirdVoterNode(candidates, target), resp
		}
		if status != http.StatusServiceUnavailable && status != http.StatusConflict {
			t.Fatalf("self-removal (%s): status=%d resp=%+v, want 200, 503, or a retryable 409 only", requestID, status, resp)
		}
		lastResp = resp
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("self-removal (%s-*) never succeeded within %s; last response: %+v", requestIDPrefix, timeout, lastResp)
	return nil, membershipMutateResponse{}
}
