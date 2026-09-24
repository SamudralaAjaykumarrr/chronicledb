package sql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file proves docs/sql.md §Distributed Evidence against a real,
// genuine three-node ChronicleDB cluster: real TCP (internal/transport),
// real disk (internal/wal), and the unmodified, already-proven
// internal/node.Node/internal/raft.Core — the SQL frontend adds nothing
// to this path except translating a parsed statement into a
// fsm.CommitTxnCommand via ReplicatedEngine (engine.go). It deliberately
// mirrors internal/node/node_test.go's own testCluster helper pattern
// (fast tick intervals, bounded polling instead of fixed sleeps) rather
// than importing it, since that helper is unexported from package node.

func freeAddrForSQLTest(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

type sqlCluster struct {
	t                 *testing.T
	ids               []raft.NodeID
	addrs             map[raft.NodeID]string
	dirs              map[raft.NodeID]string
	nodes             map[raft.NodeID]*node.Node
	snapshotThreshold uint64
}

func newSQLCluster(t *testing.T, n int) *sqlCluster {
	return newSQLClusterWithSnapshotThreshold(t, n, 0)
}

// newSQLClusterWithSnapshotThreshold mirrors
// internal/node/node_test.go's newTestClusterWithSnapshotThreshold: a
// non-zero threshold makes nodes perform real synchronous fsync-heavy
// snapshot creation inside their single event-loop goroutine, which
// needs proportionally larger election/heartbeat timeouts to avoid a
// spurious election stealing leadership mid-test (a test-timing
// concern, not a correctness one — see configFor below).
func newSQLClusterWithSnapshotThreshold(t *testing.T, n int, snapshotThreshold uint64) *sqlCluster {
	t.Helper()
	c := &sqlCluster{
		t:                 t,
		addrs:             make(map[raft.NodeID]string, n),
		dirs:              make(map[raft.NodeID]string, n),
		nodes:             make(map[raft.NodeID]*node.Node, n),
		snapshotThreshold: snapshotThreshold,
	}
	for i := 0; i < n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddrForSQLTest(t)
		c.dirs[id] = t.TempDir()
	}
	for _, id := range c.ids {
		c.nodes[id] = c.mustOpen(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *sqlCluster) configFor(id raft.NodeID) node.Config {
	peerAddrs := make(map[raft.NodeID]string)
	for _, p := range c.ids {
		if p != id {
			peerAddrs[p] = c.addrs[p]
		}
	}
	electionTicks, heartbeatTicks := 5, 1
	if c.snapshotThreshold != 0 {
		electionTicks, heartbeatTicks = 30, 3
	}
	return node.Config{
		ID:                         id,
		Peers:                      append([]raft.NodeID(nil), c.ids...),
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 c.addrs[id],
		DataDir:                    c.dirs[id],
		ElectionTimeoutTicks:       electionTicks,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      heartbeatTicks,
		TickInterval:               10 * time.Millisecond,
		SnapshotThreshold:          c.snapshotThreshold,
		Logger:                     debugLogger(id),
	}
}

func debugLogger(id raft.NodeID) *log.Logger {
	if os.Getenv("CHRONICLEDB_SQL_TEST_DEBUG") == "" {
		return nil
	}
	return log.New(os.Stderr, string(id)+": ", log.LstdFlags|log.Lmicroseconds)
}

func (c *sqlCluster) mustOpen(id raft.NodeID) *node.Node {
	c.t.Helper()
	n, err := node.Open(c.configFor(id))
	if err != nil {
		c.t.Fatalf("node.Open(%s): %v", id, err)
	}
	return n
}

func (c *sqlCluster) crash(id raft.NodeID) {
	c.nodes[id].Stop()
	delete(c.nodes, id)
}

func (c *sqlCluster) restart(id raft.NodeID) *node.Node {
	c.t.Helper()
	n := c.mustOpen(id)
	c.nodes[id] = n
	return n
}

func (c *sqlCluster) awaitLeader(timeout time.Duration) raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := map[raft.NodeID]raft.Term{}
		for id, n := range c.nodes {
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
	c.t.Fatalf("no single leader emerged within %s", timeout)
	return ""
}

func (c *sqlCluster) anyOtherNode(id raft.NodeID) raft.NodeID {
	for _, other := range c.ids {
		if other != id {
			return other
		}
	}
	c.t.Fatalf("cluster has no other node besides %s", id)
	return ""
}

func awaitConditionSQL(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
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

// mustExecRetryingLeadershipLoss executes sqlText/requestID against
// whichever node in c currently reports itself leader, re-fetching that
// node and retrying with the identical requestID on the two documented-
// safe-to-retry classes (node.ErrLeadershipLost's own doc comment:
// "outcome unknown, retry by RequestID against the current leader"; a
// bare *node.NotLeaderError, the same node not yet — or no longer —
// leader when this attempt's own Execute reached its admission check) —
// dynamic_membership_test.go's currentSQLLeader/awaitConditionSQL
// pattern already established for this identical error class, applied
// here to plain writes instead of membership calls. Any other error is
// a real, unexpected failure and fails the test immediately, exactly
// like mustExec.
func mustExecRetryingLeadershipLoss(t *testing.T, c *sqlCluster, mu *sync.Mutex, sqlText, requestID string) Result {
	t.Helper()
	var res Result
	var execErr error
	awaitConditionSQL(t, 5*time.Second, fmt.Sprintf("Execute(%q) eventually succeeds against the current leader", sqlText), func() bool {
		cur := currentSQLLeader(c, mu)
		if cur == nil {
			return false
		}
		s := NewSession(NewReplicatedEngine(cur))
		r, err := s.Execute(context.Background(), sqlText, requestID)
		if err != nil {
			var nle *node.NotLeaderError
			if errors.As(err, &nle) || errors.Is(err, node.ErrLeadershipLost) {
				return false
			}
			execErr = fmt.Errorf("Execute(%q): unexpected error: %w", sqlText, err)
			return true
		}
		res = r
		return true
	})
	if execErr != nil {
		t.Fatal(execErr)
	}
	return res
}

// TestDistributedSQLInsertReplicates proves the headline Phase 8
// distributed scenario: SQL INSERT -> Raft commit -> replicated state
// -> every node (not just the leader) ends up with the identical
// applied row, and SELECT against the leader returns it.
func TestDistributedSQLInsertReplicates(t *testing.T) {
	c := newSQLCluster(t, 3)
	ctx := context.Background()
	leaderID := c.awaitLeader(5 * time.Second)
	leader := c.nodes[leaderID]

	s := NewSession(NewReplicatedEngine(leader))
	mustExec(t, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "create-1")
	mustExec(t, s, "INSERT INTO users VALUES (1, 'alice')", "ins-1")

	key := rowKey("users", intValue(1))
	for _, id := range c.ids {
		n := c.nodes[id]
		awaitConditionSQL(t, 5*time.Second, fmt.Sprintf("node %s applies the INSERT", id), func() bool {
			_, ok, _ := n.FSM().Store().Visible(key, n.Status().AppliedIndex)
			return ok
		})
	}

	res, err := s.Execute(ctx, "SELECT id, name FROM users WHERE id = 1", "sel-1")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 1 || res.Rows[0][1].Text != "alice" {
		t.Fatalf("got %+v, want [[1 alice]]", res.Rows)
	}
}

// TestDistributedSQLLeaderFailoverRetry proves docs/sql.md's retry-
// safety requirement against a real leader crash: a client resubmits
// the identical INSERT statement under the identical RequestID against
// the newly elected leader after the original leader crashes, and gets
// back the identical outcome — no duplicate row, no error, the same
// CommitSeq — mirroring the repository's own Phase 5 central acceptance
// scenario (docs/roadmap.md), now exercised through the SQL frontend.
func TestDistributedSQLLeaderFailoverRetry(t *testing.T) {
	c := newSQLCluster(t, 3)
	ctx := context.Background()
	leaderID := c.awaitLeader(5 * time.Second)
	leader := c.nodes[leaderID]

	s := NewSession(NewReplicatedEngine(leader))
	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", "create")

	res1 := mustExec(t, s, "INSERT INTO t VALUES (1, 'v1')", "req-failover-1")

	c.crash(leaderID)
	newLeaderID := c.awaitLeader(5 * time.Second)
	if newLeaderID == leaderID {
		t.Fatalf("expected a new leader after crashing %s, got the same node", leaderID)
	}
	newLeader := c.nodes[newLeaderID]

	s2 := NewSession(NewReplicatedEngine(newLeader))
	res2, err := s2.Execute(ctx, "INSERT INTO t VALUES (1, 'v1')", "req-failover-1")
	if err != nil {
		t.Fatalf("retry against new leader: %v", err)
	}
	if res2.CommitSeq != res1.CommitSeq {
		t.Errorf("retry CommitSeq = %d, want identical to original %d", res2.CommitSeq, res1.CommitSeq)
	}

	sel := mustExec(t, s2, "SELECT id FROM t", "sel-check")
	if len(sel.Rows) != 1 {
		t.Errorf("got %d rows after failover+retry, want exactly 1 (no duplicate)", len(sel.Rows))
	}
}

// TestDistributedSQLSnapshotCompactionSurvivesRestart proves
// docs/sql.md §Recovery's snapshot/compaction leg against a real
// cluster: enough SQL INSERTs to cross a small SnapshotThreshold
// trigger a real internal/snapshot creation + internal/wal compaction
// (docs/snapshots.md), and a node crashed and restarted after that
// point recovers every row via snapshot restore, not full log replay.
func TestDistributedSQLSnapshotCompactionSurvivesRestart(t *testing.T) {
	const rows = 20
	c := newSQLClusterWithSnapshotThreshold(t, 3, 5)
	leaderID := c.awaitLeader(10 * time.Second)

	// This test holds leader across a real multi-round-trip sequence
	// (CREATE TABLE + 20 INSERTs) that deliberately crosses
	// SnapshotThreshold=5 several times over — each crossing triggers a
	// real, synchronous, fsync-heavy internal/snapshot creation on the
	// event loop (docs/snapshots.md), which is exactly the class of
	// operation documented (internal/node's mustFinalizeToMax, 82397b4;
	// internal/sql's own finalizeSQLClusterToGeneration2) as able to
	// occasionally blow even configFor's snapshot-scaled election
	// budget (30 ticks/10ms, ample for ordinary fsync latency but not
	// for a follower descheduled on a loaded host) and trigger a
	// legitimate re-election — which would depose leader mid-loop and
	// surface here as "not leader (leader unknown)," indistinguishable
	// from this test's own point of view from a real defect. Freeze the
	// election clock across the whole sequence instead of widening that
	// budget, exactly as those two precedents do; heartbeat ticks keep
	// running so the follower's own replication/catch-up/snapshot-
	// creation and the later crash/restart/catch-up phase are
	// unaffected.
	for _, n := range c.nodes {
		n.PauseTicksForTest()
	}
	defer func() {
		for _, n := range c.nodes {
			n.ResumeTicksForTest()
		}
	}()

	// mustExecRetryingLeadershipLoss (not plain mustExec): freezing
	// ticks above closes the self-initiated-re-election window, but not
	// a step-down from a higher-term message already in flight when
	// leaderID was first observed (leader is a momentary Role==Leader
	// snapshot, not proof of settled leadership) — the same class
	// TestPeerMTLS_ThreeNodeClusterReplicatesEndToEnd and
	// TestAC6_ControlPlaneNonStarvationUnderSaturatedClientLoad were
	// both independently reproduced hitting under induced contention
	// during this investigation, confirming it is generic to real-
	// cluster startup, not specific to this test's SnapshotThreshold.
	// node.ErrLeadershipLost's own doc comment is "retry by RequestID
	// against the current leader" — this re-fetches the current leader
	// and retries with the identical RequestID, exactly that documented
	// contract, exactly as dynamic_membership_test.go's own established
	// pattern (currentSQLLeader + awaitConditionSQL) already does for
	// this identical error class.
	var mu sync.Mutex
	mustExecRetryingLeadershipLoss(t, c, &mu, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", "create")
	for i := 0; i < rows; i++ {
		mustExecRetryingLeadershipLoss(t, c, &mu, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", i, i), fmt.Sprintf("ins-%d", i))
	}

	followerID := c.anyOtherNode(leaderID)
	awaitConditionSQL(t, 10*time.Second, "follower creates a snapshot", func() bool {
		return uint64(c.nodes[followerID].Status().SnapshotIndex) > 0
	})

	c.crash(followerID)
	restarted := c.restart(followerID)
	awaitConditionSQL(t, 10*time.Second, "restarted node catches back up", func() bool {
		return restarted.Status().AppliedIndex >= rows+1 // +1 for CREATE TABLE
	})

	for i := 0; i < rows; i++ {
		key := rowKey("t", intValue(int64(i)))
		if _, ok, _ := restarted.FSM().Store().Visible(key, restarted.Status().AppliedIndex); !ok {
			t.Errorf("row id=%d missing on restarted node after snapshot restore", i)
		}
	}
}
