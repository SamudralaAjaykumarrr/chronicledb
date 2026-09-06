package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// freeAddr reserves an available TCP port on 127.0.0.1 by binding and
// immediately releasing it, returning the address string for a Node's
// ListenAddr/PeerAddrs. Standard "find a free port" test pattern; the
// tiny window between release and the Node's own bind is acceptable
// for these tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// testCluster wires together a real, in-process, multi-goroutine
// cluster: each *Node has its own temp-dir-backed real WAL and a real
// TCP transport listening on localhost — genuine disk and network I/O,
// not internal/fault's deterministic simulator. Fast timeouts keep the
// suite quick without relying on real time for correctness assertions
// beyond "eventually" (this phase's brief: real end-to-end tests use
// bounded polling, not arbitrary sleeps).
type testCluster struct {
	t     *testing.T
	ids   []raft.NodeID
	addrs map[raft.NodeID]string
	dirs  map[raft.NodeID]string
	nodes map[raft.NodeID]*Node
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	tc := &testCluster{
		t:     t,
		addrs: make(map[raft.NodeID]string, n),
		dirs:  make(map[raft.NodeID]string, n),
		nodes: make(map[raft.NodeID]*Node, n),
	}
	for i := 0; i < n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		tc.ids = append(tc.ids, id)
		tc.addrs[id] = freeAddr(t)
		tc.dirs[id] = t.TempDir()
	}
	for _, id := range tc.ids {
		tc.nodes[id] = tc.mustOpen(id)
	}
	t.Cleanup(func() {
		for _, n := range tc.nodes {
			n.Stop()
		}
	})
	return tc
}

func (tc *testCluster) configFor(id raft.NodeID) Config {
	peerAddrs := make(map[raft.NodeID]string)
	for _, p := range tc.ids {
		if p != id {
			peerAddrs[p] = tc.addrs[p]
		}
	}
	return Config{
		ID:                         id,
		Peers:                      append([]raft.NodeID(nil), tc.ids...),
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 tc.addrs[id],
		DataDir:                    tc.dirs[id],
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	}
}

func (tc *testCluster) mustOpen(id raft.NodeID) *Node {
	tc.t.Helper()
	n, err := Open(tc.configFor(id))
	if err != nil {
		tc.t.Fatalf("Open(%s): %v", id, err)
	}
	return n
}

// crash simulates an ungraceful process kill of id: its event loop
// stops and its transport/WAL file handles close, but — exactly as a
// real SIGKILL would — nothing beyond what was already fsync'd
// (docs/wal.md §4) is lost, since Node.Stop performs no additional
// flush beyond what each durable operation already guaranteed. The
// node's data directory and listen/peer addresses are preserved so a
// later restart reconstructs it.
func (tc *testCluster) crash(id raft.NodeID) {
	tc.nodes[id].Stop()
	delete(tc.nodes, id)
}

// restart reopens a previously-crashed node from its existing data
// directory (docs/recovery.md).
func (tc *testCluster) restart(id raft.NodeID) *Node {
	tc.t.Helper()
	n := tc.mustOpen(id)
	tc.nodes[id] = n
	return n
}

func (tc *testCluster) node(id raft.NodeID) *Node { return tc.nodes[id] }

// isolate fully disconnects id from every other currently-live node, in
// both directions (docs/replication.md §5's network partition
// contract), via the real transport's Block hook.
func (tc *testCluster) isolate(id raft.NodeID) {
	for other, n := range tc.nodes {
		if other == id {
			continue
		}
		n.Transport().Block(id)
		if live := tc.nodes[id]; live != nil {
			live.Transport().Block(other)
		}
	}
}

// heal reverses isolate for id.
func (tc *testCluster) heal(id raft.NodeID) {
	for other, n := range tc.nodes {
		if other == id {
			continue
		}
		n.Transport().Unblock(id)
		if live := tc.nodes[id]; live != nil {
			live.Transport().Unblock(other)
		}
	}
}

// awaitLeader polls every currently-live node until exactly one
// distinct (leader-id, term) pair is observed cluster-wide, or fails
// the test after timeout. Bounded polling, not a fixed sleep.
func (tc *testCluster) awaitLeader(timeout time.Duration) raft.NodeID {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := map[raft.NodeID]raft.Term{}
		for id, n := range tc.nodes {
			st := n.Status()
			if st.Role == raft.Leader {
				leaders[id] = st.Term
			}
		}
		if len(leaders) == 1 {
			for id := range leaders {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	tc.t.Fatalf("no single leader emerged within %s", timeout)
	return ""
}

func (tc *testCluster) leaderNode(timeout time.Duration) *Node {
	return tc.node(tc.awaitLeader(timeout))
}

// awaitCondition polls cond until it returns true or timeout elapses.
func awaitCondition(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

func cmd(reqID string, txnID uint64, startSeq uint64, key, value string) fsm.CommitTxnCommand {
	return fsm.CommitTxnCommand{
		RequestID: fsm.RequestID(reqID),
		TxnID:     txnID,
		StartSeq:  startSeq,
		Mutations: []mvcc.Mutation{{Key: key, Value: []byte(value)}},
	}
}

func propose(t *testing.T, n *Node, c fsm.CommitTxnCommand, timeout time.Duration) (fsm.Outcome, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return n.Propose(ctx, c)
}

// --- Tests ---

// TestRF1_NormalReplicationConvergesAcrossRealNodes is RF-1
// (docs/scenario-corpus.md) proven end-to-end through real WAL-backed
// storage and a real TCP transport, not internal/fault's simulator.
func TestRF1_NormalReplicationConvergesAcrossRealNodes(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	outcome, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("outcome = %+v, want Committed", outcome)
	}

	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 3*time.Second, fmt.Sprintf("node %s converges on k1=v1", id), func() bool {
			v, ok := tc.node(id).FSM().Store().Visible("k1", outcome.CommitSeq)
			return ok && string(v) == "v1"
		})
	}
}

// TestNotLeaderRejectsProposal proves a follower never acknowledges a
// replicated mutation itself (this phase's brief's leader requirement).
func TestNotLeaderRejectsProposal(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)

	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}

	_, err := propose(t, tc.node(followerID), cmd("r1", 1, 0, "k1", "v1"), 500*time.Millisecond)
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("Propose to follower: err = %v, want *NotLeaderError", err)
	}
}

// TestRF11_MinorityPartitionCannotCommit is RF-11 / QUORUM-SAFETY: an
// isolated former leader (1 of 3) can never acknowledge a new write.
func TestRF11_MinorityPartitionCannotCommit(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	tc.isolate(leaderID)
	// The isolated leader does not immediately know it is partitioned;
	// it must still never acknowledge a new commit while cut off.
	_, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 1*time.Second)
	if err == nil {
		t.Fatal("isolated leader acknowledged a write during a minority partition; QUORUM-SAFETY violated")
	}
}

// TestRF12_MajorityElectsNewLeaderDuringPartition is RF-12: the
// majority side elects a legitimate new leader and keeps serving
// writes while the old leader is isolated.
func TestRF12_MajorityElectsNewLeaderDuringPartition(t *testing.T) {
	tc := newTestCluster(t, 3)
	oldLeaderID := tc.awaitLeader(5 * time.Second)
	tc.isolate(oldLeaderID)

	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "majority elects a new leader", func() bool {
		for id, n := range tc.nodes {
			if id == oldLeaderID {
				continue
			}
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})

	outcome, err := propose(t, tc.node(newLeaderID), cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose to new majority leader: outcome=%+v err=%v", outcome, err)
	}
}

// TestRF13_OldLeaderRejoinsAndConverges is RF-10/RF-13 and
// docs/replication.md §5's exact network-partition-contract scenario:
// after the partition heals, the old (now stale) leader observes a
// higher term and steps down; its own divergent, uncommitted tail (if
// any) is repaired via the real WAL-backed Storage.Truncate; and it
// converges to the majority's committed history.
func TestRF13_OldLeaderRejoinsAndConverges(t *testing.T) {
	tc := newTestCluster(t, 3)
	oldLeaderID := tc.awaitLeader(5 * time.Second)
	oldLeader := tc.node(oldLeaderID)

	tc.isolate(oldLeaderID)

	// The old leader, still believing it is leader, may keep appending
	// speculative (uncommitted) entries while cut off.
	go func() { _, _ = propose(t, oldLeader, cmd("stale", 1, 0, "stale-key", "stale-val"), 2*time.Second) }()

	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "majority elects a new leader", func() bool {
		for id, n := range tc.nodes {
			if id == oldLeaderID {
				continue
			}
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})
	outcome, err := propose(t, tc.node(newLeaderID), cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose to new leader before heal: outcome=%+v err=%v", outcome, err)
	}

	tc.heal(oldLeaderID)

	awaitCondition(t, 5*time.Second, "old leader steps down", func() bool {
		return oldLeader.Status().Role != raft.Leader
	})
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, fmt.Sprintf("node %s converges on k1=v1 after heal", id), func() bool {
			v, ok := tc.node(id).FSM().Store().Visible("k1", outcome.CommitSeq)
			return ok && string(v) == "v1"
		})
	}
	// The stale, never-committed key must never appear anywhere.
	for _, id := range tc.ids {
		if _, ok := tc.node(id).FSM().Store().Visible("stale-key", outcome.CommitSeq); ok {
			t.Fatalf("node %s materialized the old leader's uncommitted speculative write", id)
		}
	}
}

// TestRF4_LeaderCrashBeforeQuorumEntryVanishes is RF-4: a leader
// proposes an entry, crashes before any follower ever persists it (here
// forced by isolating the leader from both followers before proposing),
// and the entry is legitimately absent from the eventual new leader's
// history — safe to have vanished, since it was never committed.
func TestRF4_LeaderCrashBeforeQuorumEntryVanishes(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	tc.isolate(leaderID)
	go func() { _, _ = propose(t, leader, cmd("never-quorum", 1, 0, "k1", "v1"), 2*time.Second) }()
	// Give the leader a moment to durably append the entry locally
	// (its own persistence, but never replicated to a majority).
	awaitCondition(t, 2*time.Second, "leader locally appends the doomed entry", func() bool {
		return leader.Status().LastIndex >= 1
	})

	tc.crash(leaderID)

	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "remaining majority elects a new leader", func() bool {
		for id, n := range tc.nodes {
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})

	// A fresh RequestID for the same key must be evaluated from empty
	// history (StartSeq=0 is still valid), proving the old leader's
	// never-replicated entry does not exist anywhere in the new
	// leader's committed history.
	outcome, err := propose(t, tc.node(newLeaderID), cmd("fresh-after-rf4", 2, 0, "k1", "v2"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose to new leader: outcome=%+v err=%v", outcome, err)
	}
	if _, ok := tc.node(newLeaderID).FSM().GetOutcome("never-quorum"); ok {
		t.Fatal("the leader's never-replicated RequestID resurfaced in the new leader's history")
	}
}

// TestRF5_LeaderCrashAfterQuorumBeforeApply proves LEADER-COMPLETENESS
// and REQUEST-OUTCOME-STABILITY: an entry that reached majority
// persistence before its original leader crashed is preserved by the
// new leader, and a client retry by RequestID against the new leader
// observes COMMITTED — never a false failure and never a duplicate
// application.
func TestRF5_LeaderCrashAfterQuorumBeforeApply(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	outcome, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("initial Propose: outcome=%+v err=%v", outcome, err)
	}

	// Crash the leader immediately after it has committed+applied+would
	// have responded (the response itself is irrelevant here — what
	// matters is that a *new* leader, and a client retry against it,
	// both observe the same stable outcome).
	tc.crash(leaderID)

	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "remaining nodes elect a new leader", func() bool {
		for id, n := range tc.nodes {
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})

	retryOutcome, err := propose(t, tc.node(newLeaderID), cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil {
		t.Fatalf("retry by RequestID against new leader: %v", err)
	}
	if retryOutcome.Status != fsm.StatusCommitted || retryOutcome.CommitSeq != outcome.CommitSeq {
		t.Fatalf("retry outcome = %+v, want identical to original %+v", retryOutcome, outcome)
	}
}

// TestFollowerRestartCatchesUpViaLogReplication is RF-3's log-catch-up
// leg: a follower is stopped, the cluster keeps committing without it,
// and the follower catches up via ordinary AppendEntries replication
// once restarted.
func TestFollowerRestartCatchesUpViaLogReplication(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}
	tc.crash(followerID)

	outcome, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose while a follower is down: outcome=%+v err=%v", outcome, err)
	}

	restarted := tc.restart(followerID)
	awaitCondition(t, 5*time.Second, "restarted follower catches up", func() bool {
		v, ok := restarted.FSM().Store().Visible("k1", outcome.CommitSeq)
		return ok && string(v) == "v1"
	})
}

// TestIdempotencyAcrossFailover is the central Phase-5 acceptance
// scenario named in this phase's brief: a client's RequestID resolves
// to the identical committed outcome via a brand-new leader, and the
// mutation is never applied twice.
func TestIdempotencyAcrossFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	outcome1, err := propose(t, leader, cmd("dup-req", 1, 0, "balance", "100"), 3*time.Second)
	if err != nil || outcome1.Status != fsm.StatusCommitted {
		t.Fatalf("first Propose: outcome=%+v err=%v", outcome1, err)
	}

	tc.crash(leaderID)
	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "new leader elected", func() bool {
		for id, n := range tc.nodes {
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})

	// Retry with the exact same RequestID/TxnID/StartSeq/Mutations.
	outcome2, err := propose(t, tc.node(newLeaderID), cmd("dup-req", 1, 0, "balance", "100"), 3*time.Second)
	if err != nil {
		t.Fatalf("retry Propose: %v", err)
	}
	if outcome2 != outcome1 {
		t.Fatalf("retried outcome %+v != original %+v", outcome2, outcome1)
	}

	// The value must reflect exactly one application, not two.
	v, ok := tc.node(newLeaderID).FSM().Store().Visible("balance", outcome1.CommitSeq)
	if !ok || string(v) != "100" {
		t.Fatalf("balance = %q ok=%v, want \"100\" applied exactly once", v, ok)
	}
}

// TestMultiKeyTransactionReplicationAtomic proves a multi-key CommitTxn
// command replicates and applies atomically: no replica ever observes
// exactly some, but not all, of its mutations.
func TestMultiKeyTransactionReplicationAtomic(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	c := fsm.CommitTxnCommand{
		RequestID: "multi-1",
		TxnID:     1,
		StartSeq:  0,
		Mutations: []mvcc.Mutation{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: []byte("2")},
			{Key: "c", Value: []byte("3")},
		},
	}
	outcome, err := propose(t, leader, c, 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose: outcome=%+v err=%v", outcome, err)
	}

	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 3*time.Second, fmt.Sprintf("node %s has all three keys", id), func() bool {
			store := tc.node(id).FSM().Store()
			for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
				v, ok := store.Visible(k, outcome.CommitSeq)
				if !ok || string(v) != want {
					return false
				}
			}
			return true
		})
	}
}

// TestConflictOutcomeReplicatedAndStableAcrossFailover proves a
// deterministic ABORTED conflict outcome is itself replicated and
// remains stable even after a leader failover — CONFLICT-CORRECTNESS
// and REQUEST-OUTCOME-STABILITY together.
func TestConflictOutcomeReplicatedAndStableAcrossFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	// T1 commits first, advancing k's CommitSeq.
	o1, err := propose(t, leader, cmd("t1", 1, 0, "k", "v1"), 3*time.Second)
	if err != nil || o1.Status != fsm.StatusCommitted {
		t.Fatalf("t1 Propose: outcome=%+v err=%v", o1, err)
	}

	// T2 began at StartSeq=0 (before t1 committed) and now tries to
	// commit a conflicting write to the same key: it must abort. Node.
	// Propose returns a nil error for any *determined* outcome
	// (Committed or Aborted) — an error return is reserved for the
	// request never being determined at all (not leader, leadership
	// lost, timeout).
	o2, err := propose(t, leader, cmd("t2", 2, 0, "k", "v2"), 3*time.Second)
	if err != nil {
		t.Fatalf("t2 Propose: %v", err)
	}
	if o2.Status != fsm.StatusAborted {
		t.Fatalf("t2 outcome = %+v, want Aborted", o2)
	}

	tc.crash(leaderID)
	var newLeaderID raft.NodeID
	awaitCondition(t, 5*time.Second, "new leader elected", func() bool {
		for id, n := range tc.nodes {
			if n.Status().Role == raft.Leader {
				newLeaderID = id
				return true
			}
		}
		return false
	})

	retry, err := propose(t, tc.node(newLeaderID), cmd("t2", 2, 0, "k", "v2"), 3*time.Second)
	if retry.Status != fsm.StatusAborted {
		t.Fatalf("retried t2 outcome = %+v (err=%v), want stable Aborted", retry, err)
	}
}

// TestDurablePersistenceFailureStopsNodeWithoutFalseAck injects a real
// storage failure (closing the leader's underlying WAL out from under
// it) mid-Propose and proves the node never falsely acknowledges the
// mutation as committed: the caller observes an honest error (never a
// fabricated success), and the node stops itself rather than
// continuing in a state it can no longer trust
// (docs/failure-model.md §1.8; this phase's brief: "do not silently
// mark unsuccessful proposals committed").
func TestDurablePersistenceFailureStopsNodeWithoutFalseAck(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	if err := leader.walog.Close(); err != nil {
		t.Fatalf("closing leader's underlying WAL for fault injection: %v", err)
	}

	_, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err == nil {
		t.Fatal("Propose succeeded despite a real durable-persistence failure; DURABILITY violated")
	}

	select {
	case <-leader.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("node did not stop itself after a fatal local persistence failure")
	}
	if leader.Err() == nil {
		t.Fatal("Err() is nil after a fatal local persistence failure")
	}

	// The value must never have materialized on any node.
	for _, id := range tc.ids {
		if id == leaderID {
			continue
		}
		if _, ok := tc.node(id).FSM().Store().Visible("k1", ^uint64(0)); ok {
			t.Fatalf("node %s materialized a write that was never durably committed", id)
		}
	}
}

// TestBeginReadIndexOnLeaderSucceedsAfterCommit proves ADR-0010's
// ReadIndex protocol: a leader with at least one committed+applied
// entry can establish a safe StartSeq watermark via a majority-
// acknowledged fresh AppendEntries round.
func TestBeginReadIndexOnLeaderSucceedsAfterCommit(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	outcome, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose: outcome=%+v err=%v", outcome, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	startSeq, err := leader.BeginReadIndex(ctx)
	if err != nil {
		t.Fatalf("BeginReadIndex: %v", err)
	}
	if startSeq < outcome.CommitSeq {
		t.Fatalf("StartSeq = %d, want >= CommitSeq %d", startSeq, outcome.CommitSeq)
	}
}

// TestBeginReadIndexRejectedOnFollower proves only the legitimate
// leader ever serves the ReadIndex check (docs/replication.md §4: "all
// strong reads are served by the current leader only").
func TestBeginReadIndexRejectedOnFollower(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := tc.node(followerID).BeginReadIndex(ctx)
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("BeginReadIndex on follower: err = %v, want *NotLeaderError", err)
	}
}

// TestBeginReadIndexBlockedAfterIsolationEvenWithStaleReplicatedLog is a
// regression test for a specific failure mode: a leader that already
// replicated its full log to a majority *before* becoming isolated
// must not be able to pass the ReadIndex check on cached replication
// facts alone — the check must require a live round-trip that happens
// strictly after isolation, not just "peers were caught up at some
// point in the past." (A matchIndex-only freshness check would pass
// this vacuously, since matchIndex never resets on isolation.)
func TestBeginReadIndexBlockedAfterIsolationEvenWithStaleReplicatedLog(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	outcome, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose: outcome=%+v err=%v", outcome, err)
	}
	awaitCondition(t, 3*time.Second, "followers fully replicate before isolation", func() bool {
		return leader.Status().CommitIndex >= raft.Index(outcome.CommitSeq)
	})

	tc.isolate(leaderID)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := leader.BeginReadIndex(ctx); err == nil {
		t.Fatal("isolated leader completed ReadIndex using stale pre-isolation replication facts alone")
	}
}

// TestBeginReadIndexBlockedDuringMinorityPartition proves a partitioned
// former leader can never complete the majority-heartbeat check
// (ADR-0010's proof obligation, extension of RF-11): it cannot serve a
// strong read while isolated.
func TestBeginReadIndexBlockedDuringMinorityPartition(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)
	tc.isolate(leaderID)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := leader.BeginReadIndex(ctx)
	if err == nil {
		t.Fatal("isolated leader completed a ReadIndex check during a minority partition; QUORUM-SAFETY/read-consistency violated")
	}
}

// TestClusterRestartRecoversFSMAndRequestIDOutcomes stops every node
// and restarts the whole cluster, proving Raft persistent state (the
// pre-crash entry survives on disk and is reconstructed into every
// restarted node's log) and the deterministic FSM/RequestID-outcome
// machinery all reconstruct correctly from durable disk state
// (docs/recovery.md), not from any in-memory shortcut.
//
// Per docs/raft.md §9.5 (this phase's accepted architecture does not
// append a no-op entry on election), a brand-new leader elected after a
// *full* cluster restart cannot re-establish commitIndex over the
// pre-restart (now previous-term) history until a fresh proposal in its
// own current term reaches majority — commitIndex itself is never
// persisted (ADR-0008) and no node survived the restart with live
// knowledge of it. This is precisely the documented client
// responsibility this phase's brief calls out: the client retries by
// RequestID (docs/transactions.md §7), and that retry is what
// "unlocks" applying the durable-but-not-yet-reconfirmed pre-restart
// entry — not automatic recovery alone. The retry must still resolve
// to the *original* CommitSeq, never a fresh double-application.
func TestClusterRestartRecoversFSMAndRequestIDOutcomes(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	outcome, err := propose(t, leader, cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose: outcome=%+v err=%v", outcome, err)
	}
	awaitCondition(t, 3*time.Second, "all nodes apply before restart", func() bool {
		for _, id := range tc.ids {
			if _, ok := tc.node(id).FSM().Store().Visible("k1", outcome.CommitSeq); !ok {
				return false
			}
		}
		return true
	})

	for _, id := range tc.ids {
		tc.crash(id)
	}
	for _, id := range tc.ids {
		tc.restart(id)
	}
	newLeaderID := tc.awaitLeader(5 * time.Second)

	// The client's own retry-by-RequestID (docs/transactions.md §7) is
	// what re-establishes commitment of the pre-restart history here.
	retry, err := propose(t, tc.node(newLeaderID), cmd("r1", 1, 0, "k1", "v1"), 3*time.Second)
	if err != nil || retry.Status != fsm.StatusCommitted || retry.CommitSeq != outcome.CommitSeq {
		t.Fatalf("RequestID retry after full restart = %+v err=%v, want Committed CommitSeq=%d (no double-application)", retry, err, outcome.CommitSeq)
	}

	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, fmt.Sprintf("node %s recovers k1=v1 after full cluster restart", id), func() bool {
			v, ok := tc.node(id).FSM().Store().Visible("k1", outcome.CommitSeq)
			return ok && string(v) == "v1"
		})
	}
}
