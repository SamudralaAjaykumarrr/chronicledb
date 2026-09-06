// End-to-end/macro benchmarks for internal/node (docs/roadmap.md Phase
// 9 §Macro/end-to-end benchmarks): a single-node durable write, a
// three-node quorum-committed replicated write, a primary-key read via
// the real ReadIndex protocol, and a documented mixed read/write
// workload. Every benchmark here runs against real temp-dir-backed
// WAL/snapshot storage and real TCP transport (docs/roadmap.md's "do
// not cheat benchmarks: ... benchmarking mocked persistence while
// labeling it durable") — the same infrastructure node_test.go's
// correctness tests use, duplicated here in a *testing.B-compatible
// form rather than generalizing testCluster (node_test.go) to avoid
// touching that already-proven, heavily-used test harness for a
// benchmark-only need.
//
// Run: go test ./internal/node/... -run '^$' -bench . -benchmem
package node

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/benchutil"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

func freeAddrB(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("reserving free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// benchCluster is bench_test.go's own minimal analog of node_test.go's
// testCluster (see this file's package doc comment for why it is not
// shared directly).
type benchCluster struct {
	ids   []raft.NodeID
	nodes map[raft.NodeID]*Node
}

func newBenchCluster(b *testing.B, n int, snapshotThreshold uint64) *benchCluster {
	b.Helper()
	ids := make([]raft.NodeID, n)
	addrs := make(map[raft.NodeID]string, n)
	dirs := make(map[raft.NodeID]string, n)
	for i := 0; i < n; i++ {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
		addrs[ids[i]] = freeAddrB(b)
		dirs[ids[i]] = b.TempDir()
	}
	nodes := make(map[raft.NodeID]*Node, n)
	for _, id := range ids {
		peerAddrs := make(map[raft.NodeID]string)
		for _, p := range ids {
			if p != id {
				peerAddrs[p] = addrs[p]
			}
		}
		node, err := Open(Config{
			ID:                         id,
			Peers:                      append([]raft.NodeID(nil), ids...),
			PeerAddrs:                  peerAddrs,
			ListenAddr:                 addrs[id],
			DataDir:                    dirs[id],
			ElectionTimeoutTicks:       5,
			ElectionTimeoutJitterTicks: 5,
			HeartbeatTimeoutTicks:      1,
			TickInterval:               10 * time.Millisecond,
			SnapshotThreshold:          snapshotThreshold,
		})
		if err != nil {
			b.Fatalf("Open(%s): %v", id, err)
		}
		nodes[id] = node
	}
	bc := &benchCluster{ids: ids, nodes: nodes}
	b.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return bc
}

func (bc *benchCluster) awaitLeader(b *testing.B, timeout time.Duration) *Node {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range bc.ids {
			if bc.nodes[id].Status().Role == raft.Leader {
				return bc.nodes[id]
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("no leader emerged within %s", timeout)
	return nil
}

// BenchmarkSingleNodeDurableWrite measures the full client-to-durable-
// acknowledgement path for a single-node deployment (docs/roadmap.md
// §Macro benchmarks "Single-node durable write"): Propose -> Raft
// (trivial one-node quorum) -> real WAL append+fsync -> fsm.Apply ->
// committed outcome returned to the caller. This is Raft-shaped
// (a one-node cluster is still a Raft group of one, per
// docs/architecture.md §1) rather than internal/txn.Manager's own
// standalone path, which internal/txn's own BenchmarkCommitSingleKey
// already measures separately.
func BenchmarkSingleNodeDurableWrite(b *testing.B) {
	bc := newBenchCluster(b, 1, 0)
	leader := bc.awaitLeader(b, 2*time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		outcome, err := leader.Propose(ctx, cmd(fmt.Sprintf("bench-solo-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"))
		cancel()
		if err != nil {
			b.Fatalf("Propose: %v", err)
		}
		if outcome.Status.String() != "committed" {
			b.Fatalf("outcome = %+v, want committed", outcome)
		}
	}
}

// BenchmarkThreeNodeReplicatedWrite measures the full quorum-committed
// replicated write path (docs/roadmap.md §Macro benchmarks "Three-node
// replicated write"): leader -> Raft replication -> majority durable
// persistence -> commit -> apply -> result, over genuine TCP and real
// per-node WAL/fsync (not internal/fault's simulator, which
// BenchmarkRaftProposalReplication in internal/fault measures
// separately).
func BenchmarkThreeNodeReplicatedWrite(b *testing.B) {
	bc := newBenchCluster(b, 3, 0)
	leader := bc.awaitLeader(b, 2*time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		outcome, err := leader.Propose(ctx, cmd(fmt.Sprintf("bench-3n-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"))
		cancel()
		if err != nil {
			b.Fatalf("Propose: %v", err)
		}
		if outcome.Status.String() != "committed" {
			b.Fatalf("outcome = %+v, want committed", outcome)
		}
	}
}

// BenchmarkPrimaryKeyRead measures a strong, leader-local read via the
// real ReadIndex protocol (docs/roadmap.md §Macro benchmarks "Primary-
// key reads"; docs/replication.md §4, ADR-0010): BeginReadIndex's
// majority-freshness round trip, then a single-key MVCC visibility
// read at the resulting StartSeq.
func BenchmarkPrimaryKeyRead(b *testing.B) {
	bc := newBenchCluster(b, 3, 0)
	leader := bc.awaitLeader(b, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	outcome, err := leader.Propose(ctx, cmd("bench-read-seed", 1, 0, "k", "v"))
	cancel()
	if err != nil || outcome.Status.String() != "committed" {
		b.Fatalf("seed Propose: outcome=%+v err=%v", outcome, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		startSeq, err := leader.BeginReadIndex(ctx)
		cancel()
		if err != nil {
			b.Fatalf("BeginReadIndex: %v", err)
		}
		if _, found := leader.FSM().Store().Visible("k", startSeq); !found {
			b.Fatalf("key not visible at StartSeq %d", startSeq)
		}
	}
}

// BenchmarkMixedWorkload80Read20Write measures a fixed, explicitly
// named 80% read / 20% write mix (docs/roadmap.md §Macro benchmarks
// "Mixed workload... Name them explicitly. Do not invent workload
// ratios silently.") against the three-node replicated cluster: every
// 5th operation is a quorum-committed write to a fresh key, the other
// 4 are ReadIndex-backed reads of the most recently written key.
func BenchmarkMixedWorkload80Read20Write(b *testing.B) {
	bc := newBenchCluster(b, 3, 0)
	leader := bc.awaitLeader(b, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	outcome, err := leader.Propose(ctx, cmd("bench-mixed-seed", 1, 0, "mixed-key", "v0"))
	cancel()
	if err != nil || outcome.Status.String() != "committed" {
		b.Fatalf("seed Propose: outcome=%+v err=%v", outcome, err)
	}
	// lastSeq tracks "mixed-key"'s own latest CommitSeq so each write
	// below carries a StartSeq that is not stale relative to the
	// previous write to the same key (docs/mvcc.md §4): a write mix
	// against a single hot key, unlike BenchmarkThreeNodeReplicatedWrite's
	// distinct-key-per-iteration write mix.
	lastSeq := outcome.CommitSeq

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			outcome, err := leader.Propose(ctx, cmd(fmt.Sprintf("bench-mixed-w-%d", i), uint64(i+2), lastSeq, "mixed-key", fmt.Sprintf("v%d", i)))
			cancel()
			if err != nil {
				b.Fatalf("write Propose: %v", err)
			}
			if outcome.Status.String() != "committed" {
				b.Fatalf("write outcome = %+v, want committed", outcome)
			}
			lastSeq = outcome.CommitSeq
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		startSeq, err := leader.BeginReadIndex(ctx)
		cancel()
		if err != nil {
			b.Fatalf("BeginReadIndex: %v", err)
		}
		if _, found := leader.FSM().Store().Visible("mixed-key", startSeq); !found {
			b.Fatalf("mixed-key not visible at StartSeq %d", startSeq)
		}
	}
}

// BenchmarkThreeNodeReplicatedWriteLatencyDistribution runs the same
// operation as BenchmarkThreeNodeReplicatedWrite but additionally
// records each operation's own latency and reports p50/p95/p99/max via
// internal/benchutil (docs/roadmap.md §Latency distributions), rather
// than relying on ns/op alone, which is a per-b.N-loop average, not a
// percentile.
func BenchmarkThreeNodeReplicatedWriteLatencyDistribution(b *testing.B) {
	bc := newBenchCluster(b, 3, 0)
	leader := bc.awaitLeader(b, 2*time.Second)
	rec := benchutil.NewRecorder(b.N)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		outcome, err := leader.Propose(ctx, cmd(fmt.Sprintf("bench-3n-lat-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"))
		cancel()
		rec.Record(int64(time.Since(start)))
		if err != nil {
			b.Fatalf("Propose: %v", err)
		}
		if outcome.Status.String() != "committed" {
			b.Fatalf("outcome = %+v, want committed", outcome)
		}
	}
	b.StopTimer()
	rec.Summarize().Report(b, "commit-latency")
}

// BenchmarkNodeRestartRecovery measures node.Open's own recovery cost
// (docs/roadmap.md §Compaction/recovery performance: "recovery from
// snapshot + retained suffix", "WAL replay before compaction") for a
// single-node deployment (no election/network time involved — Open
// itself is synchronous up to starting the event-loop goroutine, so
// this isolates recovery from any leader-election latency). Each
// sub-benchmark builds a fixed on-disk state once (not timed), then
// times repeated Open+Stop cycles against that same, unchanging
// on-disk directory.
func BenchmarkNodeRestartRecovery(b *testing.B) {
	b.Run("fromLogOnly/entries=1000", func(b *testing.B) {
		cfg := buildRecoveryFixture(b, 1000, 0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			n, err := Open(cfg)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			b.StopTimer()
			n.Stop()
			b.StartTimer()
		}
	})
	b.Run("fromSnapshotPlusSuffix/base=1000,suffix=50", func(b *testing.B) {
		cfg := buildRecoveryFixtureWithSnapshot(b, 1000, 50, 200)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			n, err := Open(cfg)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			b.StopTimer()
			n.Stop()
			b.StartTimer()
		}
	})
}

// buildRecoveryFixture opens a single-node deployment, commits n
// entries, stops it cleanly, and returns the Config a benchmark can
// reopen the same on-disk directory with — an unchanging fixture, built
// once outside any timed loop.
func buildRecoveryFixture(b *testing.B, n int, snapshotThreshold uint64) Config {
	b.Helper()
	cfg := Config{
		ID:                         raft.NodeID("solo"),
		Peers:                      []raft.NodeID{raft.NodeID("solo")},
		DataDir:                    b.TempDir(),
		ListenAddr:                 freeAddrB(b),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
		SnapshotThreshold:          snapshotThreshold,
	}
	node, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && node.Status().Role != raft.Leader {
		time.Sleep(2 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		outcome, err := node.Propose(ctx, cmd(fmt.Sprintf("fixture-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"))
		cancel()
		if err != nil || outcome.Status.String() != "committed" {
			b.Fatalf("fixture Propose %d: outcome=%+v err=%v", i, outcome, err)
		}
	}
	node.Stop()
	return cfg
}

// buildRecoveryFixtureWithSnapshot builds a fixture with a small
// SnapshotThreshold so a snapshot is created covering roughly base
// entries, then commits suffix more entries on top of it (the retained,
// not-yet-compacted-into-a-new-snapshot log tail a real restart must
// still replay after restoring the snapshot) — total commits is base+
// suffix, and threshold controls roughly how many snapshots occur.
func buildRecoveryFixtureWithSnapshot(b *testing.B, base, suffix int, threshold uint64) Config {
	b.Helper()
	cfg := Config{
		ID:                         raft.NodeID("solo"),
		Peers:                      []raft.NodeID{raft.NodeID("solo")},
		DataDir:                    b.TempDir(),
		ListenAddr:                 freeAddrB(b),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
		SnapshotThreshold:          threshold,
	}
	node, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && node.Status().Role != raft.Leader {
		time.Sleep(2 * time.Millisecond)
	}
	total := base + suffix
	for i := 0; i < total; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		outcome, err := node.Propose(ctx, cmd(fmt.Sprintf("fixture-%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"))
		cancel()
		if err != nil || outcome.Status.String() != "committed" {
			b.Fatalf("fixture Propose %d: outcome=%+v err=%v", i, outcome, err)
		}
	}
	if node.Metrics().SnapshotsCreatedTotal == 0 {
		b.Fatalf("fixture never created a snapshot (SnapshotsCreatedTotal=0) — threshold=%d not reached within %d commits", threshold, total)
	}
	node.Stop()
	return cfg
}
