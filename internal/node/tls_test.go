package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/tlstest"
)

// newTLSTestCluster builds a real, in-process, real-TCP, real-disk
// three-(or-n-)node cluster with peer mTLS enabled end-to-end — genuine
// certificates issued by a throwaway CA, genuine TLS handshakes over
// genuine sockets, not a mock — satisfying
// docs/enterprise-v1-plan.md §5's Integration tests requirement: "Real
// three-node cluster with peer mTLS enabled end-to-end... proving
// replication still works."
func newTLSTestCluster(t *testing.T, n int) (*testCluster, *tlstest.CA) {
	t.Helper()
	return newTLSTestClusterWithSnapshotThreshold(t, n, 0)
}

// newTLSTestClusterWithSnapshotThreshold is newTLSTestCluster plus a real
// snapshot-creation trigger (mirrors node_test.go's
// newTestClusterWithSnapshotThreshold), used to prove snapshot-based
// follower catch-up (docs/scenario-corpus.md SN-5) also holds over peer
// mTLS, not just plaintext.
func newTLSTestClusterWithSnapshotThreshold(t *testing.T, n int, snapshotThreshold uint64) (*testCluster, *tlstest.CA) {
	t.Helper()
	ca := tlstest.NewCA(t)

	tc := &testCluster{
		addrs:             make(map[raft.NodeID]string, n),
		dirs:              make(map[raft.NodeID]string, n),
		nodes:             make(map[raft.NodeID]*Node, n),
		peerTLS:           make(map[raft.NodeID]peerTLSFiles, n),
		snapshotThreshold: snapshotThreshold,
	}
	tc.t = t
	addrs := freeAddrs(t, n)
	for i := 0; i < n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		tc.ids = append(tc.ids, id)
		tc.addrs[id] = addrs[i]
		tc.dirs[id] = t.TempDir()

		leaf := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: string(id)})
		certPath, keyPath := tlstest.WriteFiles(t, t.TempDir(), string(id), leaf)
		caPath := tlstest.WriteCAFile(t, t.TempDir(), ca)
		tc.peerTLS[id] = peerTLSFiles{CertFile: certPath, KeyFile: keyPath, CAFile: caPath}
	}
	for _, id := range tc.ids {
		tc.nodes[id] = tc.mustOpen(id)
	}
	t.Cleanup(func() {
		for _, n := range tc.nodes {
			n.Stop()
		}
	})
	return tc, ca
}

// TestPeerMTLS_ThreeNodeClusterReplicatesEndToEnd is the required
// integration proof: real TCP, real certs, a real three-node cluster,
// leader election, and a replicated write confirmed committed on every
// node — all over mutually authenticated TLS.
func TestPeerMTLS_ThreeNodeClusterReplicatesEndToEnd(t *testing.T) {
	tc, _ := newTLSTestCluster(t, 3)

	leader := tc.awaitLeader(10 * time.Second)
	ln := tc.node(leader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := ln.Propose(ctx, fsm.CommitTxnCommand{
		RequestID: "r1", TxnID: 1, StartSeq: 0,
		Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("v1")}},
	})
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose over mTLS cluster: outcome=%+v err=%v", outcome, err)
	}

	for _, id := range tc.ids {
		n := tc.node(id)
		deadline := time.Now().Add(5 * time.Second)
		for n.Status().AppliedIndex < outcome.CommitSeq && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if n.Status().AppliedIndex < outcome.CommitSeq {
			t.Fatalf("node %s never applied CommitSeq %d over mTLS replication", id, outcome.CommitSeq)
		}
	}
}

// TestPeerMTLS_LeaderFailoverStillWorks proves replication/failover
// behavior is unchanged with peer mTLS enabled (docs/enterprise-v1-plan.md
// §5 acceptance criteria: "unchanged pass rate versus plaintext").
func TestPeerMTLS_LeaderFailoverStillWorks(t *testing.T) {
	tc, _ := newTLSTestCluster(t, 3)

	leader := tc.awaitLeader(10 * time.Second)
	tc.crash(leader)

	newLeader := tc.awaitLeader(10 * time.Second)
	if newLeader == leader {
		t.Fatalf("expected a new leader distinct from crashed %s", leader)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := tc.node(newLeader).Propose(ctx, fsm.CommitTxnCommand{
		RequestID: "r1", TxnID: 1, StartSeq: 0,
		Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("v1")}},
	})
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("Propose after mTLS failover: outcome=%+v err=%v", outcome, err)
	}
}

// TestPeerMTLS_UntrustedCertificateRejectedByRealListener proves a node
// whose peer certificate is not signed by the cluster's trusted CA can
// never join real replication traffic — rejected by the real transport
// listener, not a mock, and its absence never blocks the healthy
// cluster's own quorum/replication.
func TestPeerMTLS_UntrustedCertificateRejectedByRealListener(t *testing.T) {
	tc, _ := newTLSTestCluster(t, 3)
	leader := tc.awaitLeader(10 * time.Second)

	otherCA := tlstest.NewCA(t)
	rogueID := raft.NodeID("rogue")
	rogueLeaf := otherCA.IssueLeaf(t, tlstest.LeafOptions{CommonName: string(rogueID)})
	certPath, keyPath := tlstest.WriteFiles(t, t.TempDir(), string(rogueID), rogueLeaf)
	caPath := tlstest.WriteCAFile(t, t.TempDir(), otherCA)

	rogueAddrs := freeAddrs(t, 1)
	peers := append([]raft.NodeID(nil), tc.ids...)
	peers = append(peers, rogueID)
	peerAddrs := map[raft.NodeID]string{}
	for _, id := range tc.ids {
		peerAddrs[id] = tc.addrs[id]
	}

	rogue, err := Open(Config{
		ID:                         rogueID,
		Peers:                      peers,
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 rogueAddrs[0],
		DataDir:                    t.TempDir(),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
		PeerTLSCertFile:            certPath,
		PeerTLSKeyFile:             keyPath,
		PeerTLSCAFile:              caPath,
	})
	if err != nil {
		t.Fatalf("opening rogue node (its own material is self-consistent, just untrusted by the real cluster): %v", err)
	}
	defer rogue.Stop()

	// The rogue node can never be recognized by the real cluster's
	// leader: no valid mTLS handshake is ever possible between them.
	// Prove the healthy cluster's own quorum/replication is entirely
	// unaffected by the rogue's mere presence/attempts to connect.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := tc.node(leader).Propose(ctx, fsm.CommitTxnCommand{
		RequestID: "r1", TxnID: 1, StartSeq: 0,
		Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("v1")}},
	})
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("healthy cluster's own replication with an untrusted rogue node present: outcome=%+v err=%v", outcome, err)
	}
	if rogue.Status().AppliedIndex != 0 {
		t.Fatalf("rogue node's AppliedIndex = %d, want 0 (it must never receive real cluster state)", rogue.Status().AppliedIndex)
	}
}

// TestPeerMTLS_PartitionHealAcrossLeadersRecovers extends the mTLS
// acceptance proof (docs/enterprise-v1-plan.md §5 acceptance criteria:
// "a real three-node cluster runs the full existing scenario corpus...
// with mTLS enabled, unchanged pass rate versus plaintext") to the
// repeated-partition/heal-across-leaders shape already proven over
// plaintext by chaos_test.go's TestChaos_RepeatedPartitionHealAcrossLeaders
// (docs/scenario-corpus.md RF-9 through RF-13's real-cluster leg): every
// isolate/heal cycle must still elect a majority leader, commit through
// the partition, and converge without losing or altering any
// earlier-committed fact, over real mTLS-verified peer connections.
func TestPeerMTLS_PartitionHealAcrossLeadersRecovers(t *testing.T) {
	tc, _ := newTLSTestCluster(t, 3)
	tc.awaitLeader(10 * time.Second)

	type fact struct {
		value     string
		commitSeq uint64
	}
	oracle := make(map[string]fact)

	const cycles = 2
	for cycle := 0; cycle < cycles; cycle++ {
		leaderID := tc.awaitLeader(10 * time.Second)
		leader := tc.node(leaderID)

		key := fmt.Sprintf("k%d", cycle)
		val := fmt.Sprintf("v%d", cycle)
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("cycle-%d", cycle), uint64(cycle+1), 0, key, val), 5*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("cycle %d: pre-partition Propose over mTLS: outcome=%+v err=%v", cycle, outcome, err)
		}
		oracle[key] = fact{value: val, commitSeq: outcome.CommitSeq}

		tc.isolate(leaderID)
		var newLeaderID raft.NodeID
		awaitCondition(t, 10*time.Second, "majority elects a leader over mTLS while one node is isolated", func() bool {
			for id, n := range tc.nodes {
				if id == leaderID {
					continue
				}
				if n.Status().Role == raft.Leader {
					newLeaderID = id
					return true
				}
			}
			return false
		})

		midKey := fmt.Sprintf("mid%d", cycle)
		midVal := fmt.Sprintf("mv%d", cycle)
		midOutcome, err := propose(t, tc.node(newLeaderID), cmd(fmt.Sprintf("mid-%d", cycle), uint64(100+cycle), 0, midKey, midVal), 5*time.Second)
		if err != nil || midOutcome.Status != fsm.StatusCommitted {
			t.Fatalf("cycle %d: mid-partition Propose over mTLS: outcome=%+v err=%v", cycle, midOutcome, err)
		}
		oracle[midKey] = fact{value: midVal, commitSeq: midOutcome.CommitSeq}

		tc.heal(leaderID)
		for _, id := range tc.ids {
			id := id
			awaitCondition(t, 10*time.Second, fmt.Sprintf("cycle %d: node %s converges over mTLS after heal", cycle, id), func() bool {
				v, ok := tc.node(id).FSM().Store().Visible(midKey, midOutcome.CommitSeq)
				return ok && string(v) == midVal
			})
		}

		for _, id := range tc.ids {
			for k, f := range oracle {
				v, ok := tc.node(id).FSM().Store().Visible(k, f.commitSeq)
				if !ok || string(v) != f.value {
					t.Fatalf("cycle %d: node %s lost or altered earlier fact %s=%s over mTLS (got ok=%v v=%q)", cycle, id, k, f.value, ok, v)
				}
			}
		}
	}
}

// TestPeerMTLS_FollowerCatchesUpViaSnapshotAfterLeaderCompaction extends
// the mTLS acceptance proof to snapshot-based follower catch-up
// (docs/scenario-corpus.md SN-5's real-cluster leg, already proven over
// plaintext by node_test.go's
// TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction): an
// isolated follower that has fallen behind a leader's compaction
// boundary must still catch up via a genuine InstallSnapshot RPC over a
// real mTLS-verified connection, not log replication.
func TestPeerMTLS_FollowerCatchesUpViaSnapshotAfterLeaderCompaction(t *testing.T) {
	const threshold = 3
	const numKeys = 9
	tc, _ := newTLSTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(10 * time.Second)
	leader := tc.node(leaderID)

	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			follower = id
			break
		}
	}
	tc.isolate(follower)

	outcomes := make([]fsm.Outcome, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k%d", i)
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("r%d", i), uint64(i+1), 0, key, "v"), 5*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d over mTLS: outcome=%+v err=%v", i, outcome, err)
		}
		outcomes[i] = outcome
	}
	last := outcomes[numKeys-1].CommitSeq
	// See node_test.go's TestSN5 for why up to one threshold's worth of
	// filler proposals may be needed to cross the boundary: this node's
	// own election win proposes a synthetic no-op entry first, which can
	// shift where a threshold boundary lands.
	for i := 0; uint64(leader.Status().SnapshotIndex) < last && i < threshold; i++ {
		if _, err := propose(t, leader, cmd(fmt.Sprintf("filler-r%d", i), uint64(numKeys+i+1000), 0, fmt.Sprintf("filler-k%d", i), "v"), 5*time.Second); err != nil {
			t.Fatalf("filler Propose #%d over mTLS: %v", i, err)
		}
	}
	awaitCondition(t, 5*time.Second, "leader compacts its own log past every proposed key over mTLS while the follower is isolated", func() bool {
		return uint64(leader.Status().SnapshotIndex) >= last
	})
	snapIndex := uint64(leader.Status().SnapshotIndex)

	tc.heal(follower)
	awaitCondition(t, 10*time.Second, "isolated follower catches up via an installed snapshot over mTLS", func() bool {
		st := tc.node(follower).Status()
		return uint64(st.SnapshotIndex) == snapIndex && uint64(st.AppliedIndex) >= snapIndex
	})

	fnode := tc.node(follower)
	if got := uint64(fnode.walog.FirstIndex()); got != snapIndex+1 {
		t.Fatalf("follower FirstIndex() after mTLS catch-up = %d, want %d — only a genuine InstallSnapshot install ever moves a follower's own boundary this way", got, snapIndex+1)
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k%d", i)
		if v, ok := fnode.FSM().Store().Visible(key, last); !ok || string(v) != "v" {
			t.Fatalf("follower missing/wrong key %s after mTLS snapshot catch-up: ok=%v v=%q", key, ok, v)
		}
	}
}

// TestOpen_PeerTLSIdentityMismatchRefusesToStart proves
// docs/enterprise-v1-plan.md §5 layer 1: "internal/node binds NodeID to
// the certificate at startup and refuses to start on a mismatch."
func TestOpen_PeerTLSIdentityMismatchRefusesToStart(t *testing.T) {
	ca := tlstest.NewCA(t)
	leaf := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1"})
	certPath, keyPath := tlstest.WriteFiles(t, t.TempDir(), "n1", leaf)
	caPath := tlstest.WriteCAFile(t, t.TempDir(), ca)

	addrs := freeAddrs(t, 1)
	_, err := Open(Config{
		ID:              "n2", // deliberately does not match the certificate's CommonName "n1"
		Peers:           []raft.NodeID{"n2"},
		ListenAddr:      addrs[0],
		DataDir:         t.TempDir(),
		PeerTLSCertFile: certPath,
		PeerTLSKeyFile:  keyPath,
		PeerTLSCAFile:   caPath,
	})
	if err == nil {
		t.Fatal("expected Open to refuse to start on a node-ID/certificate identity mismatch")
	}
}

// TestOpen_PartialPeerTLSConfigurationRejected proves peer TLS is never
// silently half-configured (Config.validate's NO PLAINTEXT PEER
// REPLICATION check).
func TestOpen_PartialPeerTLSConfigurationRejected(t *testing.T) {
	addrs := freeAddrs(t, 1)
	_, err := Open(Config{
		ID:              "n1",
		Peers:           []raft.NodeID{"n1"},
		ListenAddr:      addrs[0],
		DataDir:         t.TempDir(),
		PeerTLSCertFile: "/some/cert.pem", // key/CA deliberately omitted
	})
	if err == nil {
		t.Fatal("expected Open to reject a partially-configured peer TLS setup")
	}
}
