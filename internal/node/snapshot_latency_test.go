// TestSnapshotLatencyImpact is a controlled latency experiment
// (docs/roadmap.md Phase 9 §Snapshot performance), not a `go test
// -bench` benchmark: it needs exact control over precisely when the
// snapshot threshold is crossed (propose exactly N entries, then
// observe the very next operations), which a Benchmark's free-running
// b.N loop cannot guarantee. It reuses testCluster (node_test.go) — the
// same real WAL/TCP-backed three-node harness node_test.go's own
// snapshot scenarios (TestSN1, TestSN5) already exercise — and reports
// real, measured per-operation commit latencies via
// internal/benchutil, split into three phases: normal operation well
// below the snapshot threshold, the operation that crosses the
// threshold and triggers snapshot creation, and post-snapshot
// operation. Per this phase's brief, the result is reported honestly
// (see docs/benchmarks.md §Snapshot performance) — this test does not
// assert a specific latency bound on the snapshot-triggering
// operation, since docs/snapshots.md already documents synchronous
// fsync-in-the-event-loop as a known V1 limitation; it only asserts
// that the experiment actually exercised the mechanism it claims to
// (SnapshotsCreatedTotal advanced).
package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/benchutil"
)

func TestSnapshotLatencyImpact(t *testing.T) {
	const (
		threshold  = 20
		belowCount = 15 // phase A: well below threshold
		atCount    = 10 // phase B: crosses the threshold partway through
		afterCount = 15 // phase C: steady state after compaction
	)
	tc := newTestClusterWithSnapshotThreshold(t, 3, threshold)
	leaderID := tc.awaitLeader(3 * time.Second)
	leader := tc.node(leaderID)

	runPhase := func(label string, n int, keyOffset int) benchutil.Summary {
		t.Helper()
		rec := benchutil.NewRecorder(n)
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("k%d", keyOffset+i)
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			outcome, err := leader.Propose(ctx, cmd(fmt.Sprintf("snaplat-%s-%d", label, i), uint64(keyOffset+i+1), 0, key, "v"))
			cancel()
			rec.Record(int64(time.Since(start)))
			if err != nil {
				t.Fatalf("phase %s op %d: Propose: %v", label, i, err)
			}
			if outcome.Status.String() != "committed" {
				t.Fatalf("phase %s op %d: outcome = %+v, want committed", label, i, outcome)
			}
		}
		return rec.Summarize()
	}

	before := leader.Metrics().SnapshotsCreatedTotal

	baseline := runPhase("baseline", belowCount, 0)
	crossing := runPhase("crossing", atCount, belowCount)
	after := runPhase("after", afterCount, belowCount+atCount)

	awaitCondition(t, 3*time.Second, "leader creates a snapshot after crossing the threshold", func() bool {
		return leader.Metrics().SnapshotsCreatedTotal > before
	})

	t.Logf("phase=baseline (n=%d, well below threshold=%d):  p50=%v p95=%v p99=%v max=%v",
		baseline.Count, threshold, time.Duration(baseline.P50), time.Duration(baseline.P95), time.Duration(baseline.P99), time.Duration(baseline.Max))
	t.Logf("phase=crossing (n=%d, crosses threshold=%d):      p50=%v p95=%v p99=%v max=%v",
		crossing.Count, threshold, time.Duration(crossing.P50), time.Duration(crossing.P95), time.Duration(crossing.P99), time.Duration(crossing.Max))
	t.Logf("phase=after (n=%d, post-snapshot steady state):   p50=%v p95=%v p99=%v max=%v",
		after.Count, time.Duration(after.P50), time.Duration(after.P95), time.Duration(after.P99), time.Duration(after.Max))
	if crossing.Max > baseline.P99 {
		t.Logf("observed latency spike: phase=crossing max (%v) exceeds phase=baseline p99 (%v) — consistent with docs/snapshots.md's documented synchronous-fsync-in-event-loop V1 limitation, not a regression this test fails on", time.Duration(crossing.Max), time.Duration(baseline.P99))
	}
}
