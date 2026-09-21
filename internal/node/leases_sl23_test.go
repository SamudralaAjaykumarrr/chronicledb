package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// leaderLiveLeases reads a *Node's live-read-lease count through
// Metrics().ReadLeasesActiveGauge rather than leases.Len() directly:
// leaseRegistry, like n.waiters/n.pendingReads, is mutated exclusively
// on the event-loop goroutine (leases.go's own doc comment), so a test
// goroutine reading n.leases.Len() races that goroutine for real —
// exactly the same class of hazard node_test.go's liveConfig/
// membershipCounts helpers already document and route around for
// n.core/cached Status(). ReadLeasesActiveGauge is the sanctioned safe
// accessor: an atomic Gauge refreshed by refreshStatusLocked at the end
// of every single run() loop iteration (node.go's run/refreshStatusLocked),
// so it is never more than one iteration stale — fine-grained enough for
// polling-based assertions.
func leaderLiveLeases(n *Node) int64 { return n.Metrics().ReadLeasesActiveGauge }

// TestReadLeaseLifecycle_SL23 is SL-23 (docs/v0.6.0-plan.md §30, §15.3's
// lifecycle table): every documented way a read lease's life can end —
// a caller releasing it after a successful resolve, a caller releasing
// it twice, a ctx that is already canceled before BeginReadIndex is
// even called, a ctx canceled while the read is genuinely still
// pending, and this leader losing leadership while a read is pending —
// must leave leaseRegistry with no leaked entry. Run under `go test
// -race`.
func TestReadLeaseLifecycle_SL23(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	if got := leaderLiveLeases(leader); got != 0 {
		t.Fatalf("setup: leader has %d live leases before any test activity, want 0", got)
	}

	t.Run("release_after_resolve", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, lease, err := leader.BeginReadIndex(ctx)
		if err != nil {
			t.Fatalf("BeginReadIndex: %v", err)
		}
		if leaderLiveLeases(leader) == 0 {
			t.Fatalf("lease not registered after a successful BeginReadIndex")
		}
		lease.Release()
		awaitCondition(t, 3*time.Second, "lease released after resolve", func() bool {
			return leaderLiveLeases(leader) == 0
		})
	})

	// replicatedTxn.Commit and .Abort (internal/sql/engine.go) both
	// unconditionally `defer lease.Release()`; ReadLease.Release's own
	// atomic.Bool CompareAndSwap guard is what makes a caller invoking
	// both (or any other double-release) safe rather than a double-free
	// of the registry entry.
	t.Run("double_release_idempotent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, lease, err := leader.BeginReadIndex(ctx)
		if err != nil {
			t.Fatalf("BeginReadIndex: %v", err)
		}
		lease.Release()
		lease.Release()
		awaitCondition(t, 3*time.Second, "lease released exactly once, no double-free", func() bool {
			return leaderLiveLeases(leader) == 0
		})
	})

	// A pre-canceled ctx must never register a lease at all: the first
	// select in BeginReadIndex (dispatching req onto n.readIndexCh)
	// races ctx.Done() against the send, so a lease can only ever be
	// registered by handleReadIndex actually running — nothing to leak
	// by construction if that never happens.
	t.Run("ctx_canceled_before_dispatch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		before := leaderLiveLeases(leader)
		startSeq, lease, err := leader.BeginReadIndex(ctx)
		if err == nil {
			t.Fatalf("BeginReadIndex with an already-canceled ctx returned no error (startSeq=%d lease=%v)", startSeq, lease)
		}
		if lease != nil {
			t.Fatalf("BeginReadIndex returned a non-nil lease alongside an error")
		}
		if got := leaderLiveLeases(leader); got != before {
			t.Fatalf("live leases = %d, want unchanged %d (a pre-canceled ctx must never register a lease)", got, before)
		}
	})

	// A ctx canceled while the read is genuinely still pending (total
	// isolation means it can never resolve on its own via quorum acks):
	// the cancellation-path release (releaseLease) must reclaim the
	// lease even though the caller never received a *ReadLease to call
	// Release on itself.
	t.Run("ctx_canceled_while_pending", func(t *testing.T) {
		tc.isolate(leaderID) // no quorum reachable: this read can never resolve on its own
		defer tc.heal(leaderID)
		before := leaderLiveLeases(leader)
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		startSeq, lease, err := leader.BeginReadIndex(ctx)
		if err == nil {
			t.Fatalf("BeginReadIndex resolved despite total isolation from quorum (startSeq=%d lease=%v)", startSeq, lease)
		}
		if lease != nil {
			t.Fatalf("BeginReadIndex returned a non-nil lease alongside an error")
		}
		awaitCondition(t, 3*time.Second, "lease released after ctx cancellation while pending", func() bool {
			return leaderLiveLeases(leader) == before
		})
	})

	// Leadership lost while a read is pending: the two followers, cut
	// off from this (about-to-be-former) leader, elect a new leader
	// among themselves; healing the partition delivers that new
	// leader's higher-term messages to the old leader, forcing
	// SteppedDown. checkPendingReads' role/term-mismatch branch must
	// release the lease and resolve the caller with ErrLeadershipLost —
	// this leader's own Role() stays "Leader" locally for as long as it
	// is merely isolated (docs/replication.md §5; the same premise
	// admission_behavior_test.go's isolate(leaderID) tests already rely
	// on), so the read genuinely registers and stays pending rather
	// than failing immediately with NotLeaderError.
	t.Run("leadership_lost_while_pending", func(t *testing.T) {
		before := leaderLiveLeases(leader)
		resultCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, _, err := leader.BeginReadIndex(ctx)
			resultCh <- err
		}()
		awaitCondition(t, 2*time.Second, "read lease registered and pending", func() bool {
			return leaderLiveLeases(leader) > before
		})

		tc.isolate(leaderID)
		var others []raft.NodeID
		for _, id := range tc.ids {
			if id != leaderID {
				others = append(others, id)
			}
		}
		awaitCondition(t, 5*time.Second, "the two survivors elect a new leader", func() bool {
			for _, id := range others {
				if tc.node(id).Status().Role == raft.Leader {
					return true
				}
			}
			return false
		})
		tc.heal(leaderID)

		select {
		case err := <-resultCh:
			if !errors.Is(err, ErrLeadershipLost) {
				t.Fatalf("BeginReadIndex error = %v, want ErrLeadershipLost", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("BeginReadIndex never resolved after the old leader stepped down")
		}
		awaitCondition(t, 3*time.Second, "lease released after leadership loss", func() bool {
			return leaderLiveLeases(leader) == before
		})
	})
}
