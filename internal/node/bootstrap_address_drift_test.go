package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestFirstConfigChangeToleratesBootstrapAddressSpellingDrift regresses a
// real defect found by the v0.5.0 final correctness review: every node
// seeds raft.Config.Bootstrap from its OWN -listen value for itself and
// from -peers for everyone else (bootstrapConfiguration), so two nodes
// legitimately hold different SPELLINGS of the same endpoint —
// -listen=0.0.0.0:9000 against a routable -peers address, localhost
// against 127.0.0.1, hostname against IP. The dynamic-membership plan
// §1.8 says so outright: "-listen and -http ... are process-local
// configuration, never part of replicated Configuration."
//
// Before the fix, raft's accept-time four-shape re-check compared whole
// Member values (ID AND Address), so the very first EntryConfig a
// correct leader proposed was classified as structurally invalid on any
// node whose bootstrap spelled its own address differently — and
// activateFromAppendedEntries turns that into a panic, in Core.Step,
// BEFORE the entry is persisted. A restart re-derived the same bootstrap
// and panicked on the same entry again: a permanent crash loop, and on
// a three-voter cluster with two such nodes, unrecoverable loss of
// quorum on the first membership change ever attempted.
//
// This test is deliberately end-to-end rather than a classifyTransition
// unit test: the defect was not that the comparison was wrong in
// isolation, but that production wiring feeds it two legitimately
// different spellings. See TestClassifyTransitionIgnoresAddressSpelling
// in internal/raft for the unit-level pin of the same rule.
func TestFirstConfigChangeToleratesBootstrapAddressSpellingDrift(t *testing.T) {
	addrs := freeAddrs(t, 3)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	dirs := make(map[raft.NodeID]string, len(ids))
	addr := make(map[raft.NodeID]string, len(ids))
	for i, id := range ids {
		dirs[id] = t.TempDir()
		addr[id] = addrs[i]
	}
	// n1's own -listen names the same endpoint every peer dials, spelled
	// differently. This is the whole fault being injected.
	listenOf := func(id raft.NodeID) string {
		if id == "n1" {
			return strings.Replace(addr[id], "127.0.0.1:", "localhost:", 1)
		}
		return addr[id]
	}

	nodes := make(map[raft.NodeID]*Node, len(ids))
	for _, id := range ids {
		peerAddrs := make(map[raft.NodeID]string, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peerAddrs[p] = addr[p]
			}
		}
		n, err := Open(Config{
			ID:                         id,
			Peers:                      append([]raft.NodeID(nil), ids...),
			PeerAddrs:                  peerAddrs,
			ListenAddr:                 listenOf(id),
			DataDir:                    dirs[id],
			ElectionTimeoutTicks:       5,
			ElectionTimeoutJitterTicks: 5,
			HeartbeatTimeoutTicks:      1,
			TickInterval:               10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Open(%s): %v", id, err)
		}
		nodes[id] = n
		defer n.Stop()
	}

	var leader *Node
	awaitCondition(t, 10*time.Second, "a leader emerges", func() bool {
		for _, id := range ids {
			if nodes[id].Status().Role == raft.Leader {
				leader = nodes[id]
				return true
			}
		}
		return false
	})

	// Precondition: the nodes really do disagree about n1's address, so
	// this test is exercising the fault it claims to.
	spellings := make(map[string]bool)
	for _, id := range ids {
		for _, v := range liveConfig(t, nodes[id]).Voters {
			if v.ID == "n1" {
				spellings[v.Address] = true
			}
		}
	}
	if len(spellings) < 2 {
		t.Fatalf("test setup: every node agrees on n1's bootstrap address (%v); this test must inject a genuine spelling divergence", spellings)
	}

	// Finalization is this test's precondition, not its subject: it is a
	// multi-round-trip, leader-only sequence, and a spontaneous
	// re-election or a not-yet-converged precheck anywhere inside it
	// fails the test for reasons that have nothing to do with address
	// spelling. Both guards below mirror what every other finalize call
	// site in this package already does (mustFinalizeToMax); this one
	// was the single site 88868ae's sweep missed, and it is why this
	// test failed roughly 2 runs in 11 under `-race -tags=integration`
	// with the host loaded.
	//
	// Freeze the election clock first, so the leader reference stays
	// valid across the sequence (heartbeats keep flowing — see
	// testCluster.pauseTicking / Node.electionTicksPaused for why this
	// is the right remedy rather than a wider timeout), then wait for
	// precheck to actually report Ready: FinalizeUpgrade requires the
	// leader to have recorded a generation for every voter peer, which
	// it only ever learns from a message it RECEIVES, so calling it
	// straight after the election is a race, not a slow path.
	for _, id := range ids {
		nodes[id].PauseTicksForTest()
	}
	defer func() {
		for _, id := range ids {
			nodes[id].ResumeTicksForTest()
		}
	}()
	awaitPrecheckReady(t, leader, "every node runs this same binary and all are reachable")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	finalizeToMax(t, leader, ctx)
	awaitCondition(t, 10*time.Second, "every node reaches generation 2", func() bool {
		for _, id := range ids {
			if nodes[id].Status().ClusterGeneration < 2 {
				return false
			}
		}
		return true
	})

	// The first EntryConfig this cluster ever replicates. Before the fix
	// this panicked n1's event loop as it appended the entry.
	addCtx, addCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer addCancel()
	outcome, err := leader.AddLearner(addCtx, "add-n4", "n4", "127.0.0.1:59999")
	if err != nil {
		t.Fatalf("AddLearner across bootstrap address-spelling drift: %v", err)
	}
	if outcome.Status.String() != "committed" {
		t.Fatalf("AddLearner outcome = %+v, want committed", outcome)
	}

	want := liveConfig(t, leader)
	for _, id := range ids {
		id := id
		awaitCondition(t, 10*time.Second, "node "+string(id)+" adopts the replicated configuration", func() bool {
			return liveConfig(t, nodes[id]).Equal(want)
		})
		if err := nodes[id].Err(); err != nil {
			t.Fatalf("node %s stopped with a fatal error: %v", id, err)
		}
	}
	// Convergence is on the leader's spelling, replicated — not on each
	// node's own bootstrap view.
	got := liveConfig(t, nodes["n1"])
	if len(got.Learners) != 1 || got.Learners[0].ID != "n4" {
		t.Fatalf("n1's configuration after the add = %+v, want exactly one learner n4", got)
	}
}
