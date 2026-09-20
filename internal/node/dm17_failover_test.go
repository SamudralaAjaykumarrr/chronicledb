package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// awaitNewLeaderAmongExcluding polls the still-reachable members of tc
// (every id other than excluded) until exactly one of them reports
// Role==Leader in a term strictly greater than aboveTerm, and returns
// that node's ID. excluded is deliberately left out of the scan: it is
// isolated but not crashed, so it still locally believes itself
// Leader in its own (now stale) term, and including it here would
// make "exactly one leader" never hold — the whole point of this
// helper is to observe the *other* side of a real partition electing
// a genuine new leader while the isolated old leader has no way to
// find out yet (dynamic-membership plan §15 DM-17's "leader failover
// interleaved" repeat).
func awaitNewLeaderAmongExcluding(t *testing.T, tc *testCluster, excluded raft.NodeID, aboveTerm raft.Term, timeout time.Duration) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range tc.ids {
			if id == excluded {
				continue
			}
			st := tc.node(id).Status()
			if st.Role == raft.Leader && st.Term > aboveTerm {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no new leader emerged among the reachable members within %s", timeout)
	return ""
}

// failoverResult carries one blocking call's outcome back from the
// goroutine that issued it while the leader was isolated.
type failoverResult struct {
	err error
}

// runIsolatedAndAwaitFailover isolates leader from the rest of tc,
// starts a BeginReadIndex against it (registered while it is still
// legitimately Leader, exactly as DM-17's non-failover sub-cases
// require — "before the self-removal"/before any config change), then
// runs proposeFn (which appends the sub-case's membership-change entry
// append-time-effective on the isolated leader, per §2.2, but can
// never replicate or commit while isolated), waits for the remaining
// members to elect a genuine new leader in a higher term, heals the
// partition, and waits for the old leader to step down on hearing from
// the new one. It returns the read's and the propose's results
// (both must be ErrLeadershipLost, delivered together by the same
// out.SteppedDown handling in Node.processOutput) and the new leader's
// ID.
func runIsolatedAndAwaitFailover(t *testing.T, tc *testCluster, leader *Node, proposeFn func(ctx context.Context) error) (readErr, proposeErr error, newLeaderID raft.NodeID) {
	t.Helper()
	leaderID := leader.cfg.ID
	oldTerm := leader.Status().Term

	tc.isolate(leaderID)

	// Register the read via a direct, synchronous send on the leader's
	// own readIndexCh — not through the BeginReadIndex wrapper in a
	// goroutine — so that by the time this call returns, the read is
	// already queued on (and, since the event loop handles one case at
	// a time, effectively already registered by) the leader's single
	// event loop, strictly before proposeFn's own request is ever sent.
	// This matches DM-17's non-failover requirement that this sub-case's
	// read is "one whose term/requiredSeq were captured before the
	// self-removal" (§23/H3): without this ordering, the self-removal
	// sub-case races handleReadIndex's own selfRemoved() guard against
	// the append-time-effective removal and can spuriously refuse the
	// read with ErrNodeRemoved instead of ever registering it.
	readReq := readIndexReq{resultCh: make(chan readResult, 1)}
	select {
	case leader.readIndexCh <- readReq:
	case <-time.After(5 * time.Second):
		t.Fatal("could not register the read on the isolated leader")
	}

	proposeCh := make(chan failoverResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := proposeFn(ctx)
		proposeCh <- failoverResult{err: err}
	}()

	newLeaderID = awaitNewLeaderAmongExcluding(t, tc, leaderID, oldTerm, 10*time.Second)

	tc.heal(leaderID)

	awaitCondition(t, 5*time.Second, "the isolated leader steps down once healed", func() bool {
		return leader.Status().Role == raft.Follower
	})

	select {
	case res := <-readReq.resultCh:
		readErr = res.err
	case <-time.After(10 * time.Second):
		t.Fatal("BeginReadIndex on the failed-over leader never returned")
	}
	select {
	case res := <-proposeCh:
		proposeErr = res.err
	case <-time.After(10 * time.Second):
		t.Fatal("the pending membership proposal on the failed-over leader never returned")
	}
	return readErr, proposeErr, newLeaderID
}

// TestDM17Promote_WithLeaderFailover repeats DM-17's first sub-case
// (§15: "Repeat all three sub-cases with a leader failover
// interleaved") with a real leader failover, rather than the
// self-removal/commit boundary, interrupting both the pending read and
// the pending Promote: a learner is caught up and its promote is
// proposed on a leader that is then partitioned away before the entry
// can replicate; the remaining voters elect a genuine new leader in a
// higher term; once healed, the old leader must cleanly fail both the
// read and the promote with ErrLeadershipLost (never a phantom
// resolution, never a bare deadline expiry), and the promotion must
// still be achievable, from scratch, against the new leader.
func TestDM17Promote_WithLeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 4)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	learnerAddr := freeAddrs(t, 1)[0]
	learnerID := raft.NodeID("n5")
	peerAddrs := make(map[raft.NodeID]string, len(tc.ids))
	for _, id := range tc.ids {
		peerAddrs[id] = tc.addrs[id]
	}
	learner, err := Open(Config{
		ID:                         learnerID,
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 learnerAddr,
		DataDir:                    t.TempDir(),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening new learner process: %v", err)
	}
	defer learner.Stop()

	addCtx, addCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer addCancel()
	if _, err := leader.AddLearner(addCtx, "dm17fo-add", learnerID, learnerAddr); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	awaitCondition(t, 5*time.Second, "learner catches up before isolation", func() bool {
		return learner.Status().AppliedIndex >= uint64(leader.Status().LastIndex)
	})

	readErr, proposeErr, newLeaderID := runIsolatedAndAwaitFailover(t, tc, leader, func(ctx context.Context) error {
		_, err := leader.PromoteToVoter(ctx, "dm17fo-promote", learnerID, 0)
		return err
	})
	if readErr != ErrLeadershipLost {
		t.Fatalf("pending read's error = %v, want ErrLeadershipLost", readErr)
	}
	if proposeErr != ErrLeadershipLost {
		t.Fatalf("pending PromoteToVoter's error = %v, want ErrLeadershipLost", proposeErr)
	}

	newLeader := tc.node(newLeaderID)
	if got := membershipVoterCount(t, newLeader); got != 4 {
		t.Fatalf("new leader's VoterCount = %d, want 4 (the promote must not have committed)", got)
	}

	var promoted fsm.Outcome
	awaitCondition(t, 5*time.Second, "PromoteToVoter succeeds fresh against the new leader", func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		defer pcancel()
		o, err := newLeader.PromoteToVoter(pctx, "dm17fo-promote-retry", learnerID, 0)
		if err != nil {
			return false
		}
		promoted = o
		return true
	})
	if promoted.Status != fsm.StatusCommitted {
		t.Fatalf("PromoteToVoter outcome = %+v, want Committed", promoted)
	}
}

// TestDM17Remove_WithLeaderFailover repeats DM-17's second sub-case
// with a real leader failover: RemoveServer(D) is proposed on a leader
// that is then partitioned away before the entry can replicate, so the
// remaining three voters (including D itself, which stays reachable to
// the new leader throughout — only the old leader is isolated) elect a
// genuine new leader; the old leader must cleanly fail both the
// pending read and the pending removal, D must still be a voter
// afterward, and the new leader must be able to remove D itself.
func TestDM17Remove_WithLeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 4)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)
	leaderID := leader.cfg.ID

	var target raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			target = id
			break
		}
	}

	readErr, proposeErr, newLeaderID := runIsolatedAndAwaitFailover(t, tc, leader, func(ctx context.Context) error {
		_, err := leader.RemoveServer(ctx, "dm17fo-remove", target, 0)
		return err
	})
	if readErr != ErrLeadershipLost {
		t.Fatalf("pending read's error = %v, want ErrLeadershipLost", readErr)
	}
	if proposeErr != ErrLeadershipLost {
		t.Fatalf("pending RemoveServer's error = %v, want ErrLeadershipLost", proposeErr)
	}

	newLeader := tc.node(newLeaderID)
	if !liveConfig(t, newLeader).IsVoter(target) {
		t.Fatalf("target %s is no longer a voter on the new leader, want the removal to not have committed", target)
	}
	if got := membershipVoterCount(t, newLeader); got != 4 {
		t.Fatalf("new leader's VoterCount = %d, want 4 (the removal must not have committed)", got)
	}

	// Bounded-poll, retrying on the transient post-election not-ready
	// window (§11/§2.2a's P1 gate, raft.ErrConfigChangeNoCurrentTermCommit):
	// newLeaderID was only just observed to satisfy Role==Leader &&
	// Term>oldTerm, with no guarantee its own current-term no-op has
	// committed yet. Retrying here (rather than widening the timeout or
	// sleeping) mirrors this package's own documented remedy for this
	// exact window (docs/testing-strategy.md §4; see the DM-6 removal
	// test's identical retry and this test's own sibling,
	// TestDM17Promote_WithLeaderFailover's retry loop).
	var removed fsm.Outcome
	awaitCondition(t, 5*time.Second, "RemoveServer against the new leader eventually succeeds once it clears the P1 gate", func() bool {
		removeCtx, removeCancel := context.WithTimeout(context.Background(), time.Second)
		defer removeCancel()
		o, err := newLeader.RemoveServer(removeCtx, "dm17fo-remove-retry", target, 0)
		if err != nil {
			if errors.Is(err, raft.ErrConfigChangeNoCurrentTermCommit) || errors.Is(err, raft.ErrConfigChangeInheritedSuffixUncommitted) {
				return false // retryable, keep polling
			}
			t.Fatalf("RemoveServer against the new leader: %v", err)
		}
		removed = o
		return true
	})
	if removed.Status != fsm.StatusCommitted {
		t.Fatalf("RemoveServer outcome = %+v, want Committed", removed)
	}
}

// TestDM17SelfRemoval_WithLeaderFailover repeats DM-17's third
// sub-case with a real leader failover taking the place of the
// commit-then-step-down boundary the non-failover version exercises:
// the leader proposes its own removal, is then partitioned away before
// that entry can replicate to anyone, and the remaining three voters
// (which never saw the self-removal — it never left the old leader's
// own log) elect a genuine new leader among themselves under the
// still-4-voter configuration. Once healed, the old leader must cleanly
// fail both the pending read and its own pending self-removal, must
// still be a voter according to the new leader (the self-removal never
// committed), and the new leader must be able to remove it via an
// ordinary (non-self) removal.
func TestDM17SelfRemoval_WithLeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 4)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)
	leaderID := leader.cfg.ID

	readErr, proposeErr, newLeaderID := runIsolatedAndAwaitFailover(t, tc, leader, func(ctx context.Context) error {
		_, err := leader.RemoveServer(ctx, "dm17fo-self-remove", leaderID, 0)
		return err
	})
	if readErr != ErrLeadershipLost {
		t.Fatalf("pending read's error = %v, want ErrLeadershipLost", readErr)
	}
	if proposeErr != ErrLeadershipLost {
		t.Fatalf("pending self-removal's error = %v, want ErrLeadershipLost", proposeErr)
	}

	newLeader := tc.node(newLeaderID)
	if !liveConfig(t, newLeader).IsVoter(leaderID) {
		t.Fatalf("former leader %s is no longer a voter on the new leader, want the self-removal to not have committed", leaderID)
	}
	if got := membershipVoterCount(t, newLeader); got != 4 {
		t.Fatalf("new leader's VoterCount = %d, want 4 (the self-removal must not have committed)", got)
	}

	removeCtx, removeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer removeCancel()
	removed, err := newLeader.RemoveServer(removeCtx, "dm17fo-remove-former-leader", leaderID, 0)
	if err != nil {
		t.Fatalf("RemoveServer(former leader) against the new leader: %v", err)
	}
	if removed.Status != fsm.StatusCommitted {
		t.Fatalf("RemoveServer(former leader) outcome = %+v, want Committed", removed)
	}
}
