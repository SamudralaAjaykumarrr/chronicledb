// Benchmark for the Raft proposal/replication path (docs/roadmap.md
// Phase 9 §Raft "Raft proposal/replication path where meaningful"),
// measured against the deterministic simulator this package already
// provides for correctness testing: real, unmodified raft.Core
// instances, driven by explicit logical-time advancement rather than
// real sockets/disk/wall-clock sleeps. internal/node's own end-to-end
// benchmarks (BenchmarkThreeNodeReplicatedWrite) measure the identical
// path against genuine TCP/WAL/fsync — this benchmark isolates Raft's
// own logical replication/commit-rule cost from that I/O, exactly the
// same separation of concerns docs/testing-strategy.md §3.3 draws
// between simulator-based Raft-safety testing and real-transport
// end-to-end testing.
//
// Run: go test ./internal/fault/... -run '^$' -bench . -benchmem
package fault

import (
	"fmt"
	"testing"
)

// BenchmarkRaftProposalReplication measures one full propose -> quorum
// replicate -> commit cycle: Cluster.Propose, then driving logical
// ticks/message delivery until the entry is committed on a majority.
// Each iteration proposes a distinct payload and verifies the leader's
// own CommitIndex actually advanced, so this is never benchmarking a
// no-op (docs/roadmap.md §Benchmark correctness).
func BenchmarkRaftProposalReplication(b *testing.B) {
	cl := newTestCluster(1)
	if !cl.SettleElection(50) {
		b.Fatalf("cluster failed to settle on a single leader")
	}
	leader := cl.Leaders()[0]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := cl.Node(leader).Core().CommitIndex()
		cl.Propose(leader, []byte(fmt.Sprintf("payload-%d", i)))
		for round := 0; round < 50; round++ {
			cl.AdvanceTicks(1)
			if cl.DeliverEligible() == 0 && cl.Node(leader).Core().CommitIndex() > before {
				break
			}
		}
		if cl.Node(leader).Core().CommitIndex() <= before {
			b.Fatalf("iteration %d: leader CommitIndex did not advance past %d", i, before)
		}
	}
}
