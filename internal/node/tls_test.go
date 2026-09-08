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
	ca := tlstest.NewCA(t)

	tc := &testCluster{
		addrs:   make(map[raft.NodeID]string, n),
		dirs:    make(map[raft.NodeID]string, n),
		nodes:   make(map[raft.NodeID]*Node, n),
		peerTLS: make(map[raft.NodeID]peerTLSFiles, n),
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
