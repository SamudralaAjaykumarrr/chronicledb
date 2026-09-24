package node

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/metrics"
)

// ac10Duration returns how long TestAC10 sustains saturation: a short
// default (fast enough for every push, mirroring chaosSeeds's own
// discipline in internal/fault/chaos_test.go), or the full 60s
// docs/v0.6.0-plan.md §30.1 specifies when CHRONICLEDB_AC10_DURATION is
// set (the release-gate invocation — e.g. CHRONICLEDB_AC10_DURATION=60s).
func ac10Duration() time.Duration {
	if v := os.Getenv("CHRONICLEDB_AC10_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 2 * time.Second
}

// histogramDeltaP99 estimates the 99th percentile of the samples added
// between before and after (same histogram, two snapshots), by bucket
// boundary — the standard fixed-bucket-histogram percentile
// approximation: the upper bound of the first bucket whose cumulative
// share of the delta reaches 99%. +Inf if no bucket does (e.g. zero
// samples in the window).
func histogramDeltaP99(before, after metrics.HistogramSnapshot) float64 {
	if len(before.BucketCounts) != len(after.BucketCounts) {
		return math.Inf(1)
	}
	delta := make([]uint64, len(after.BucketCounts))
	var total uint64
	for i := range delta {
		d := after.BucketCounts[i] - before.BucketCounts[i]
		delta[i] = d
		total += d
	}
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(total) * 0.99))
	var cumulative uint64
	for i, c := range delta {
		cumulative += c
		if cumulative >= target {
			if i < len(after.Bounds) {
				return after.Bounds[i]
			}
			return math.Inf(1)
		}
	}
	return math.Inf(1)
}

// readRSSBytes reads this test process's current resident set size from
// /proc/self/status (Linux — docs/support-matrix.md: only Linux amd64
// is actually tested/supported). Skips the RSS assertion entirely on
// any platform/sandbox where this file is unavailable, rather than
// failing a portability gap this test does not exist to prove.
func readRSSBytes(t *testing.T) (uint64, bool) {
	t.Helper()
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// TestAC10_SustainedSaturation_ZeroElectionsBoundedLatencyAndRSS is
// AC-10 (docs/v0.6.0-plan.md §30.1): sustained saturating client-write
// load against a real three-node testCluster produces zero elections,
// keeps raft_message_process_seconds' p99 within a generous multiple of
// its unloaded baseline, and does not grow this process's RSS without
// bound. Default duration is short (see ac10Duration); the documented
// 60s release-gate run sets CHRONICLEDB_AC10_DURATION=60s.
func TestAC10_SustainedSaturation_ZeroElectionsBoundedLatencyAndRSS(t *testing.T) {
	const maxInflight = 4
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 8))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	electionsAt := func() uint64 {
		var total uint64
		for _, n := range tc.nodes {
			total += n.Metrics().ElectionsTotal
		}
		return total
	}
	electionsBefore := electionsAt()

	// A brief unloaded warm-up (heartbeats only) establishes the
	// baseline window for the p99 comparison below.
	time.Sleep(300 * time.Millisecond)
	baseline := leader.Metrics().RaftMessageProcessSeconds

	rssBefore, haveRSS := readRSSBytes(t)

	stop, wg := saturateWrites(leader, 8, "ac10")
	time.Sleep(ac10Duration())

	loaded := leader.Metrics().RaftMessageProcessSeconds
	close(stop)
	wg.Wait()

	electionsAfter := electionsAt()
	if electionsAfter != electionsBefore {
		t.Fatalf("elections occurred under sustained saturation (before=%d after=%d) — CONTROL-PLANE NON-STARVATION requires zero", electionsBefore, electionsAfter)
	}
	tc.awaitLeader(2 * time.Second) // leadership must still be stable

	baselineP99 := histogramDeltaP99(metrics.HistogramSnapshot{Bounds: baseline.Bounds, BucketCounts: zeroCounts(baseline)}, baseline)
	loadedP99 := histogramDeltaP99(baseline, loaded)
	// A generous multiple, not a tight bound: this proves Lane K service
	// time does not blow up under saturation (the actual property under
	// test), not that it is literally unaffected — some slowdown sharing
	// one OS/CPU with 8 saturating goroutines is expected and acceptable.
	const maxRegressionMultiple = 10
	if baselineP99 > 0 && loadedP99 > baselineP99*maxRegressionMultiple {
		t.Errorf("raft_message_process_seconds p99 regressed from %.6fs (baseline) to %.6fs (loaded) — more than %dx", baselineP99, loadedP99, maxRegressionMultiple)
	}

	if haveRSS {
		rssAfter, ok := readRSSBytes(t)
		if ok {
			const maxRSSGrowthBytes = 500 << 20 // 500 MiB: a leak-detection heuristic, not a tight production budget
			if rssAfter > rssBefore && rssAfter-rssBefore > maxRSSGrowthBytes {
				t.Errorf("RSS grew by %d bytes (%d -> %d) during sustained saturation — want < %d bytes", rssAfter-rssBefore, rssBefore, rssAfter, maxRSSGrowthBytes)
			}
		}
	}
}

// zeroCounts returns a zero-valued BucketCounts slice the same length as
// snap's, so histogramDeltaP99 can be reused to compute an absolute
// (not delta) p99 for the baseline window from a single snapshot taken
// after a brief unloaded warm-up.
func zeroCounts(snap metrics.HistogramSnapshot) []uint64 {
	return make([]uint64, len(snap.BucketCounts))
}
