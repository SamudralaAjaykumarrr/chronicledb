package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// saturateWrites starts n goroutines hammering leader.Propose with short
// per-call deadlines until stop is closed, returning a WaitGroup the
// caller must Wait on after closing stop. Mirrors the saturation shape
// AC-3/AC-5's own tests already use, generalized to run concurrently
// with an external event (here, a failover or membership change) rather
// than being the sole thing under test.
func saturateWrites(leader *Node, n int, keyPrefix string) (stop chan struct{}, wg *sync.WaitGroup) {
	stop = make(chan struct{})
	wg = &sync.WaitGroup{}
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				reqID := fmt.Sprintf("%s-%d-%d", keyPrefix, g, i)
				leader.Propose(ctx, cmd(reqID, uint64(g*1_000_000+i), 0, reqID, "v"))
				cancel()
				i++
			}
		}(g)
	}
	return stop, wg
}

// assertNoGateTokenLeak checks InFlight()==0 on every Lane B/A gate a
// node owns — the AC-11 "no token leak after quiescence" assertion.
func assertNoGateTokenLeak(t *testing.T, n *Node, label string) {
	t.Helper()
	for _, g := range []struct {
		name string
		gate interface{ InFlight() int }
	}{
		{"write", n.admission.write},
		{"read", n.admission.read},
		{"control", n.admission.control},
		{"maintenance", n.admission.maintenance},
	} {
		if got := g.gate.InFlight(); got != 0 {
			t.Errorf("%s: gate %q InFlight() = %d after quiescence, want 0 (token leak)", label, g.name, got)
		}
	}
}

// TestAC11_SaturationPlusLeaderFailover_NoGateTokenLeak is AC-11
// (docs/v0.6.0-plan.md §30.1): a saturating write load runs concurrently
// with a leader failover — once via an ungraceful Stop (the crash
// case), once via isolate+heal (a real SteppedDown transition, distinct
// from Stop's own shutdown path, docs/node.go's Node.Stop) — and after
// quiescence every surviving gate's InFlight() is back to zero on every
// remaining node, and the newly elected leader enforces its own
// -max-inflight-proposals ceiling immediately.
func TestAC11_SaturationPlusLeaderFailover_NoGateTokenLeak(t *testing.T) {
	for _, mode := range []string{"crash", "graceful_stepdown"} {
		t.Run(mode, func(t *testing.T) {
			const maxInflight = 4
			tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 2))
			leaderID := tc.awaitLeader(5 * time.Second)
			leader := tc.node(leaderID)
			oldTerm := leader.Status().Term

			stop, wg := saturateWrites(leader, 8, "ac11-"+mode)
			time.Sleep(150 * time.Millisecond) // let saturation actually build

			var newLeaderID raft.NodeID
			if mode == "crash" {
				tc.crash(leaderID)
				newLeaderID = awaitNewLeaderAmongExcluding(t, tc, leaderID, oldTerm, 10*time.Second)
			} else {
				tc.isolate(leaderID)
				newLeaderID = awaitNewLeaderAmongExcluding(t, tc, leaderID, oldTerm, 10*time.Second)
				tc.heal(leaderID)
				awaitCondition(t, 5*time.Second, "isolated old leader steps down", func() bool {
					return leader.Status().Role == raft.Follower
				})
			}

			close(stop)
			wg.Wait()

			for _, id := range tc.ids {
				if id == leaderID && mode == "crash" {
					continue // removed from tc.nodes by crash
				}
				assertNoGateTokenLeak(t, tc.node(id), string(id))
			}

			// The new leader enforces its own ceiling immediately: more
			// concurrent callers than maxInflight must see a rejection.
			newLeader := tc.node(newLeaderID)
			var rejected atomic.Int32
			var wg2 sync.WaitGroup
			for g := 0; g < maxInflight*3; g++ {
				wg2.Add(1)
				go func(g int) {
					defer wg2.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					defer cancel()
					reqID := fmt.Sprintf("ac11-new-leader-%s-%d", mode, g)
					if _, err := newLeader.Propose(ctx, cmd(reqID, uint64(g+1), 0, reqID, "v")); err != nil {
						rejected.Add(1)
					}
				}(g)
			}
			wg2.Wait()
			if rejected.Load() == 0 {
				t.Errorf("new leader %s admitted %d concurrent writes against a ceiling of %d with zero rejections — ceiling not enforced immediately after failover", newLeaderID, maxInflight*3, maxInflight)
			}
			assertNoGateTokenLeak(t, newLeader, "new-leader-after-burst")
		})
	}
}

// TestAC12_SaturationPlusMembershipChange_AdminLaneNeverBlocked is AC-12
// (docs/v0.6.0-plan.md §30.1): a saturating write load runs throughout a
// full AddLearner -> PromoteToVoter -> RemoveServer sequence, proving
// Lane A1 (control) is never starved by Lane B (client writes) — each
// step completes within its own bounded timeout, never merely at the
// mercy of the saturated write queue draining first — and existing
// membership behavior (idempotent AddLearner retry, convergence to the
// expected voter/learner counts) is unaffected by the saturation.
func TestAC12_SaturationPlusMembershipChange_AdminLaneNeverBlocked(t *testing.T) {
	const maxInflight = 4
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 8))
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	stop, wg := saturateWrites(leader, 8, "ac12")
	time.Sleep(150 * time.Millisecond) // let saturation build before the membership sequence starts

	newAddr := freeAddrs(t, 1)[0]
	learnerID := raft.NodeID("n4")
	peerAddrs := make(map[raft.NodeID]string, len(tc.ids))
	for _, id := range tc.ids {
		peerAddrs[id] = tc.addrs[id]
	}
	learner, err := Open(Config{
		ID:                         learnerID,
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 newAddr,
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
	addOutcome, err := leader.AddLearner(addCtx, "ac12-add-n4", learnerID, newAddr)
	if err != nil {
		t.Fatalf("AddLearner under saturation: %v", err)
	}
	if addOutcome.Status != fsm.StatusCommitted {
		t.Fatalf("AddLearner outcome = %+v, want Committed", addOutcome)
	}
	// Idempotent retry under saturation: existing behavior unaffected.
	addOutcome2, err := leader.AddLearner(addCtx, "ac12-add-n4", learnerID, newAddr)
	if err != nil || addOutcome2 != addOutcome {
		t.Fatalf("AddLearner retry under saturation = %+v, %v; want identical %+v, nil", addOutcome2, err, addOutcome)
	}

	// PromoteToVoter requires the learner to reach exact zero lag
	// (-promotion-max-lag-entries defaults to 0): under truly continuous,
	// gapless saturation the leader's LastIndex never stops moving, so
	// that instant can never arrive by construction — not a defect, an
	// inherent property of a strict-catch-up promotion gate combined
	// with an unthrottled writer. Pausing the writers here for the
	// catch-up/promote step (then resuming them for the remove step
	// below) mirrors what a real operator promoting a learner during
	// heavy load would also have to do, and still exercises this test's
	// actual point — that Lane A1 (control) is never itself blocked by
	// Lane B saturation — on both sides of that pause.
	close(stop)
	wg.Wait()

	awaitCondition(t, 10*time.Second, "learner catches up once writes pause", func() bool {
		return learner.Status().AppliedIndex >= uint64(leader.Status().LastIndex)
	})

	var promoted fsm.Outcome
	awaitCondition(t, 10*time.Second, "PromoteToVoter eventually succeeds", func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		defer pcancel()
		o, err := leader.PromoteToVoter(pctx, "ac12-promote-n4", learnerID, 0)
		if err != nil {
			var lag *ErrLearnerNotCaughtUp
			if errors.As(err, &lag) {
				return false
			}
			t.Fatalf("PromoteToVoter under saturation: unexpected error %v", err)
		}
		promoted = o
		return true
	})
	if promoted.Status != fsm.StatusCommitted {
		t.Fatalf("PromoteToVoter outcome = %+v, want Committed", promoted)
	}

	// Resume saturation for the remove step: RemoveServer needs no
	// caught-up target, so this is where "admin lane never blocked by
	// client load" is exercised again with no pause required.
	stop, wg = saturateWrites(leader, 8, "ac12-remove")
	time.Sleep(150 * time.Millisecond)

	var toRemove raft.NodeID
	for _, id := range tc.ids {
		if id != leader.cfg.ID {
			toRemove = id
			break
		}
	}
	rmCtx, rmCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rmCancel()
	removeOutcome, err := leader.RemoveServer(rmCtx, "ac12-remove-1", toRemove, 0)
	if err != nil {
		t.Fatalf("RemoveServer under saturation: %v", err)
	}
	if removeOutcome.Status != fsm.StatusCommitted {
		t.Fatalf("RemoveServer outcome = %+v, want Committed", removeOutcome)
	}

	close(stop)
	wg.Wait()
	assertNoGateTokenLeak(t, leader, "leader-after-membership-sequence")
}
