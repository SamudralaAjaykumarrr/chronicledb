package sql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file is the substitute docs/dynamic-membership-plan.md §16's
// "background SQL reader" requirement gets in this codebase: §16 asks
// for a background goroutine issuing real SQL SELECTs against a
// changing voter set on real OS processes, but cmd/chronicledb-node
// has no SQL wire protocol at all (docs/sql.md §8, examples/sql-
// basics/main.go's own header: "There is no SQL CLI or wire protocol
// — this is the actual, currently-available way to run SQL against
// ChronicleDB: as a Go library"). Adding one purely to satisfy this
// proof would be an unscoped production change §21's slice list never
// asked for. Instead, this exercises the same substance — SQL SELECTs
// (each a real BeginReadIndex call, internal/sql/engine.go) racing a
// real membership-change sequence — at the tier this package's own
// docs/sql.md "Distributed Evidence" tests already use: real TCP
// (internal/transport), real disk (internal/wal), and unmodified
// internal/node.Node/internal/raft.Core, in one OS process.

// finalizeSQLClusterToGeneration2 mirrors
// internal/node's mustFinalizeToMax: brings leader (and, by
// replication, every node in c) to this binary's own
// MaxSupportedGeneration, the precondition for any membership call
// (dynamic-membership plan §8.2).
func finalizeSQLClusterToGeneration2(t *testing.T, c *sqlCluster, leader *node.Node) {
	t.Helper()
	// Finalization is a multi-round-trip, leader-only sequence driven
	// against a leader reference this helper already holds. A
	// spontaneous re-election anywhere inside it deposes that leader
	// and surfaces here as "not leader (leader unknown)" —
	// indistinguishable, from this helper, from a real defect. This is
	// the same root cause internal/node's mustFinalizeToMax documents
	// (82397b4): configFor's election budget (5+jitter 5 ticks at
	// 10ms, 50-100ms) is ample for ordinary fsync latency but not for a
	// follower descheduled on a loaded host. Freeze the election clock
	// across the sequence instead of widening that budget; heartbeat
	// ticks keep running so replication and catch-up are unaffected.
	for _, n := range c.nodes {
		n.PauseTicksForTest()
	}
	defer func() {
		for _, n := range c.nodes {
			n.ResumeTicksForTest()
		}
	}()

	awaitConditionSQL(t, 5*time.Second, "precheck reports Ready", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := leader.UpgradePrecheck(ctx)
		return err == nil && res.Ready
	})
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err := leader.FinalizeUpgrade(ctx)
		cancel()
		if errors.Is(err, node.ErrAlreadyFinalized) {
			break
		}
		if err != nil {
			t.Fatalf("FinalizeUpgrade: %v", err)
		}
	}
	for _, id := range c.ids {
		id := id
		awaitConditionSQL(t, 5*time.Second, "node "+string(id)+" converges on max generation", func() bool {
			return c.nodes[id].Status().ClusterGeneration == leader.Status().MaxSupportedGeneration
		})
	}
}

// currentSQLLeader scans c's nodes for whichever one currently
// believes itself Leader, or nil if none does at this instant (a real,
// momentary possibility during an election). Reading Status() this way
// concurrently with the cluster's own event-loop goroutines is safe —
// Status() is exactly the synchronized accessor internal/node.Node
// exposes for this purpose. mu guards every access to c.nodes from this
// test (c.nodes itself carries no synchronization of its own — the
// tests in distributed_test.go never mutate it concurrently with a
// reader, but this test's background SQL reader goroutine runs
// concurrently with the main goroutine's own AddLearner/crash-driven
// mutations, so it needs one).
func currentSQLLeader(c *sqlCluster, mu *sync.Mutex) *node.Node {
	mu.Lock()
	defer mu.Unlock()
	for _, n := range c.nodes {
		if n.Status().Role == raft.Leader {
			return n
		}
	}
	return nil
}

// isCleanSQLReadFailure reports whether err is one of the two clean,
// documented failure classes a SQL SELECT's underlying BeginReadIndex
// call may return while racing a membership change against an
// otherwise-healthy, fully reachable cluster (dynamic-membership plan
// §4.2a rows 1-2; §16's own scoping — this test never partitions a
// remaining voter away from the leader, so row 3's legitimate blocked-
// read/deadline outcome does not apply here, exactly as §16 itself
// excludes it from its "never a timeout" assertion): *node.NotLeaderError
// (the background reader raced Status() against a node that had
// already stepped down, or not yet become leader, by the time its own
// SELECT reached Begin) and node.ErrLeadershipLost (a read that was
// already registered when its leader stepped down mid-flight, resolved
// exactly as DM-17 proves deterministically).
func isCleanSQLReadFailure(err error) bool {
	var nle *node.NotLeaderError
	if errors.As(err, &nle) {
		return true
	}
	// ErrNodeStopped is this test's own real crash() (a genuine
	// Node.Stop, this tier's SIGKILL-equivalent) landing on a read that
	// reached the doomed node's BeginReadIndex at almost the same
	// instant — as clean and documented a class as ErrLeadershipLost,
	// just one this in-process crash mechanism can produce that a
	// separate-OS-process kill cannot (there, the read would instead see
	// a connection failure at the transport level). ErrNodeRemoved is
	// handleReadIndex's own append-time-effective refusal (§2.2, §4.6)
	// when this reader's currentSQLLeader scan happens to pick the
	// self-removing leader itself for a brand-new read after it has
	// already observed its own removal but before it has stepped down —
	// distinct from, but exactly as clean and documented as,
	// ErrLeadershipLost's coverage of a read already pending when that
	// happens (DM-17's own case).
	return errors.Is(err, node.ErrLeadershipLost) || errors.Is(err, node.ErrNodeStopped) || errors.Is(err, node.ErrNodeRemoved)
}

// isRetryableConfigChangeRefusal reports whether err is one of §2.2a's
// two admin-layer-retryable pre-proposal refusals (dynamic-membership
// plan §2.6a, mirroring cmd/chronicledb-node/membership.go's own 503
// mapping) — the only two membership-change refusals this test's
// "immediately after failover" step may legitimately see.
func isRetryableConfigChangeRefusal(err error) bool {
	return errors.Is(err, raft.ErrConfigChangeInheritedSuffixUncommitted) || errors.Is(err, raft.ErrConfigChangeNoCurrentTermCommit)
}

// TestDynamicMembershipWithConcurrentSQLReads interleaves a background
// SQL reader against whichever node currently holds leadership with a
// full add/promote/remove/self-removal/failover membership-change
// sequence (dynamic-membership plan §16), asserting throughout that
// every read either succeeds or fails with one of the two clean,
// documented classes — never a bare context deadline, since this test
// (matching §16's own scoping) never partitions a remaining voter away
// from the leader.
func TestDynamicMembershipWithConcurrentSQLReads(t *testing.T) {
	c := newSQLCluster(t, 3)
	leaderID := c.awaitLeader(5 * time.Second)
	leader := c.nodes[leaderID]

	s := NewSession(NewReplicatedEngine(leader))
	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", "create")

	finalizeSQLClusterToGeneration2(t, c, leader)

	var clusterMu sync.Mutex

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopAndWait := func() {
		stopOnce.Do(func() { close(stop) })
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var readOK int
	var readErrs []error

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			cur := currentSQLLeader(c, &clusterMu)
			if cur == nil {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			sess := NewSession(NewReplicatedEngine(cur))
			_, err := sess.Execute(ctx, "SELECT id FROM t", fmt.Sprintf("bgread-%d", i))
			cancel()
			mu.Lock()
			if err != nil {
				readErrs = append(readErrs, err)
			} else {
				readOK++
			}
			mu.Unlock()
		}
	}()
	defer func() {
		stopAndWait()
		wg.Wait()
	}()

	// Add a fourth node as a learner over genuine TCP/disk, catch it up,
	// then promote it — retrying on the learner-not-caught-up refusal
	// exactly like §16's own promote step, since the background reader
	// above keeps every node's LastIndex moving throughout.
	learnerID := raft.NodeID("n4")
	learnerAddr := freeAddrForSQLTest(t)
	peerAddrs := make(map[raft.NodeID]string, len(c.ids))
	for _, id := range c.ids {
		peerAddrs[id] = c.addrs[id]
	}
	learner, err := node.Open(node.Config{
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
		t.Fatalf("opening learner n4: %v", err)
	}
	// Track n4 in c.nodes/c.ids/c.addrs too, so this harness's own
	// awaitLeader/currentSQLLeader (both of which only scan c.nodes) can
	// see it once it is promoted and can itself become leader — c's own
	// t.Cleanup already stops every node in c.nodes, so no separate
	// defer is needed here. Guarded by clusterMu: the background reader
	// goroutine is already running and reads c.nodes concurrently.
	clusterMu.Lock()
	c.nodes[learnerID] = learner
	c.ids = append(c.ids, learnerID)
	c.addrs[learnerID] = learnerAddr
	clusterMu.Unlock()

	// Same structural staleness risk as every membership call below:
	// re-fetch whichever node currently holds leadership on each
	// attempt rather than assuming the leader captured at the top of
	// this test is still it.
	var addErr error
	awaitConditionSQL(t, 5*time.Second, "AddLearner(n4) eventually succeeds", func() bool {
		cur := currentSQLLeader(c, &clusterMu)
		if cur == nil {
			return false
		}
		actx, acancel := context.WithTimeout(context.Background(), time.Second)
		defer acancel()
		_, err := cur.AddLearner(actx, "dm16-add-n4", learnerID, learnerAddr)
		if err != nil {
			var nle *node.NotLeaderError
			if errors.As(err, &nle) || errors.Is(err, node.ErrLeadershipLost) || isRetryableConfigChangeRefusal(err) {
				return false
			}
			addErr = err
			return true
		}
		return true
	})
	if addErr != nil {
		t.Fatalf("AddLearner: %v", addErr)
	}
	awaitConditionSQL(t, 5*time.Second, "n4 catches up", func() bool {
		return learner.Status().AppliedIndex >= uint64(leader.Status().LastIndex)
	})

	// Re-fetch the current leader on every attempt, exactly like the
	// background reader above (currentSQLLeader) — the background
	// reader keeps every node ticking under real timers throughout this
	// test, so the original leader can genuinely, transiently lose
	// leadership to a spontaneous election before this promote is ever
	// issued (a real CI-timing race, not specific to any deliberate
	// failover later in this test). Retrying PromoteToVoter against the
	// stale, no-longer-leader node would only ever reproduce
	// *node.NotLeaderError forever; retrying against whichever node
	// currently holds leadership is this file's own established pattern
	// for that class of staleness (isCleanSQLReadFailure, above).
	// isRetryableConfigChangeRefusal's two errors are retried for the
	// same reason a fresh leader may not yet be able to accept a config
	// change (§2.2a/ADR-0014's election no-op gate) — the identical
	// class the post-failover RemoveServer loop below already retries
	// on. Any other error is unexpected and fails the test immediately,
	// exactly as before.
	var promoteErr error
	awaitConditionSQL(t, 5*time.Second, "PromoteToVoter(n4) eventually succeeds", func() bool {
		cur := currentSQLLeader(c, &clusterMu)
		if cur == nil {
			return false
		}
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		defer pcancel()
		_, err := cur.PromoteToVoter(pctx, "dm16-promote-n4", learnerID, 0)
		if err != nil {
			var lag *node.ErrLearnerNotCaughtUp
			var nle *node.NotLeaderError
			// ErrLeadershipLost ("outcome unknown, retry by RequestID
			// against the current leader") is that sentinel's own
			// documented recovery instruction — exactly what re-fetching
			// cur and reusing the same RequestID on the next attempt
			// does.
			if errors.As(err, &lag) || errors.As(err, &nle) || errors.Is(err, node.ErrLeadershipLost) || isRetryableConfigChangeRefusal(err) {
				return false
			}
			promoteErr = err
			return true
		}
		return true
	})
	if promoteErr != nil {
		t.Fatalf("PromoteToVoter(n4): %v", promoteErr)
	}
	awaitConditionSQL(t, 5*time.Second, "cluster converges on 4 voters", func() bool {
		return leader.Status().VoterCount == 4
	})

	// Remove one of the three original voters (not the current leader),
	// leaving 3 voters — no confirmation required. Same structural
	// staleness risk as the PromoteToVoter/self-removal calls above (a
	// captured leader variable, real timers, the background reader
	// still driving real traffic): re-fetch whichever node currently
	// holds leadership on each attempt. removeTarget itself is
	// independent of who currently leads, so it is computed once.
	var removeTarget raft.NodeID
	for _, id := range c.ids {
		if id != leaderID && id != learnerID {
			removeTarget = id
			break
		}
	}
	var removeErr error
	awaitConditionSQL(t, 5*time.Second, "RemoveServer(original voter) eventually succeeds", func() bool {
		cur := currentSQLLeader(c, &clusterMu)
		if cur == nil {
			return false
		}
		rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
		defer rcancel()
		_, err := cur.RemoveServer(rctx, "dm16-remove-1", removeTarget, 0)
		if err != nil {
			var nle *node.NotLeaderError
			if errors.As(err, &nle) || errors.Is(err, node.ErrLeadershipLost) || isRetryableConfigChangeRefusal(err) {
				return false
			}
			removeErr = err
			return true
		}
		return true
	})
	if removeErr != nil {
		t.Fatalf("RemoveServer(%s): %v", removeTarget, removeErr)
	}
	awaitConditionSQL(t, 5*time.Second, "cluster converges on 3 voters after removal", func() bool {
		return leader.Status().VoterCount == 3
	})

	// Force a real leader failover (a real SIGKILL-equivalent: Node.Stop),
	// then immediately attempt a further membership change against the
	// new leader — it must either succeed or return one of §2.2a's two
	// retryable pre-proposal refusals, never anything else (this is
	// DM-12's real-process counterpart, §16). The change removes the
	// now-crashed former leader itself — the operationally sensible
	// thing to do right after a real failover, and the only choice that
	// leaves both of this cluster's remaining voters (keptOriginal,
	// learnerID) alive for the self-removal step below.
	clusterMu.Lock()
	c.crash(leaderID)
	clusterMu.Unlock()
	newLeaderID := c.awaitLeader(10 * time.Second)
	if newLeaderID == leaderID {
		t.Fatalf("expected a new leader after crashing %s, got the same node", leaderID)
	}
	newLeader := c.nodes[newLeaderID]

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	succeeded := false
	for time.Now().Before(deadline) {
		mctx, mcancel := context.WithTimeout(context.Background(), time.Second)
		_, err := newLeader.RemoveServer(mctx, "dm16-remove-crashed-leader", leaderID, 2)
		mcancel()
		if err == nil {
			succeeded = true
			break
		}
		if !isRetryableConfigChangeRefusal(err) {
			t.Fatalf("membership change immediately after failover returned a non-retryable error: %v", err)
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	if !succeeded {
		t.Fatalf("membership change immediately after failover never succeeded; last retryable refusal: %v", lastErr)
	}
	awaitConditionSQL(t, 5*time.Second, "cluster converges on 2 voters after the post-failover removal", func() bool {
		return newLeader.Status().VoterCount == 2
	})

	// The new leader removes itself (self-removal, §4.2/§4.2a) with the
	// background SQL reader still running, against a healthy, fully
	// reachable remaining voter. Structurally identical staleness risk
	// to the PromoteToVoter loop above (a captured leader variable,
	// real timers, the background reader still driving real traffic).
	// But unlike those calls, the proposer is not interchangeable here:
	// a different leader removing newLeaderID is an ordinary follower
	// removal, and would pass every assertion below without ever
	// exercising self-removal. So only newLeader itself may propose,
	// until newLeader has returned ErrLeadershipLost for this exact
	// RequestID — its self-removal proposal's outcome is then unknown,
	// and retrying the SAME RequestID against whichever node now leads
	// is that sentinel's own documented recovery (it deduplicates to
	// the original outcome). If newLeader merely stops leading without
	// that, keep waiting for it to lead again; the outer bound fails
	// closed if the self-removal path is never exercised.
	var selfRemoveOutcome fsm.Outcome
	var selfRemoveErr error
	selfRemoveOutcomeUnknown := false
	awaitConditionSQL(t, 5*time.Second, "self-removal of the new leader eventually succeeds", func() bool {
		cur := currentSQLLeader(c, &clusterMu)
		if cur == nil || (cur != newLeader && !selfRemoveOutcomeUnknown) {
			return false
		}
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		defer pcancel()
		o, err := cur.RemoveServer(pctx, "dm16-self-remove", newLeaderID, 1)
		if cur == newLeader && errors.Is(err, node.ErrLeadershipLost) {
			selfRemoveOutcomeUnknown = true
		}
		if err != nil {
			var nle *node.NotLeaderError
			if errors.As(err, &nle) || errors.Is(err, node.ErrLeadershipLost) || isRetryableConfigChangeRefusal(err) {
				return false
			}
			selfRemoveErr = err
			return true
		}
		selfRemoveOutcome = o
		return true
	})
	if selfRemoveErr != nil {
		t.Fatalf("self-removal: %v", selfRemoveErr)
	}
	if selfRemoveOutcome.Status.String() != "committed" {
		t.Fatalf("self-removal outcome = %+v, want committed", selfRemoveOutcome)
	}
	awaitConditionSQL(t, 5*time.Second, "the self-removed node steps down", func() bool {
		return newLeader.Status().Role != raft.Leader
	})
	finalLeaderID := c.awaitLeader(5 * time.Second)
	if finalLeaderID == newLeaderID {
		t.Fatal("the self-removed node is still reporting itself as leader")
	}

	stopAndWait()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if readOK == 0 {
		t.Fatal("the background SQL reader never completed a single successful read")
	}
	for _, e := range readErrs {
		if !isCleanSQLReadFailure(e) {
			t.Fatalf("background SQL reader saw a non-clean failure (want NotLeaderError or ErrLeadershipLost only): %v", e)
		}
	}
	t.Logf("background SQL reader: %d successful reads, %d clean failures", readOK, len(readErrs))
}
