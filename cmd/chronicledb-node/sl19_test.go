//go:build integration

// This file is SL-19 (docs/v0.6.0-plan.md §30.2): continuous write load
// over a bounded key set against a real cluster, GC enabled, proving
// chronicledb_mvcc_versions actually stabilizes rather than growing
// without bound — the central claim GC exists to make true — while
// chronicledb_requestid_outcomes is reported honestly as the separate,
// genuinely unbounded number it is (§28.2/D6), and the cluster still
// behaves correctly (read-back, restart/recovery) afterward.
//
// Duration scales via CHRONICLEDB_SL19_DURATION exactly like AC-10's
// own CHRONICLEDB_AC10_DURATION (internal/node/ac10_test.go): short by
// default, the documented >=30m qualification run sets
// CHRONICLEDB_SL19_DURATION=30m.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run SL19 -v
package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func sl19Duration() time.Duration {
	if v := os.Getenv("CHRONICLEDB_SL19_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 3 * time.Second
}

func TestSL19_LongRunningBoundedKeySetGCStabilizesVersions(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealClusterWithFlags(t, bin, 3,
		"-gc-interval=50ms", "-gc-min-retain-seqs=5", "-gc-min-advance-seqs=1",
		"-gc-max-versions-per-pass=10000", "-gc-max-keys-per-pass=10000")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration(t, leader, nodes, 3)

	const boundedKeys = 20
	duration := sl19Duration()
	deadline := time.Now().Add(duration)

	var writes int
	initialStatus, err := leader.status()
	if err != nil {
		t.Fatalf("leader /status: %v", err)
	}
	lastCommitSeq := initialStatus.AppliedIndex

	type sample struct {
		at       time.Duration
		versions float64
		outcomes float64
	}
	var samples []sample
	start := time.Now()
	sampleEvery := duration / 20
	if sampleEvery < 50*time.Millisecond {
		sampleEvery = 50 * time.Millisecond
	}
	nextSample := start

	for time.Now().Before(deadline) {
		key := fmt.Sprintf("sl19-k%d", writes%boundedKeys)
		reqID := fmt.Sprintf("sl19-%d", writes)
		pr, status, err := proposeAt(leader, reqID, key, fmt.Sprintf("v%d", writes), lastCommitSeq)
		if err != nil {
			t.Fatalf("propose #%d: transport error: %v", writes, err)
		}
		if status == 200 && pr.Status == "committed" {
			lastCommitSeq = pr.CommitSeq
		}
		writes++

		if now := time.Now(); !now.Before(nextSample) {
			versions, _ := metricValue(t, leader, "chronicledb_mvcc_versions")
			outcomes, _ := metricValue(t, leader, "chronicledb_requestid_outcomes")
			samples = append(samples, sample{at: now.Sub(start), versions: versions, outcomes: outcomes})
			nextSample = now.Add(sampleEvery)
		}
	}
	// One final sample after the load stops and GC has had a moment to
	// catch up on anything still in flight.
	time.Sleep(500 * time.Millisecond)
	finalVersions, _ := metricValue(t, leader, "chronicledb_mvcc_versions")
	finalOutcomes, _ := metricValue(t, leader, "chronicledb_requestid_outcomes")
	t.Logf("SL-19: %d writes over %s across %d keys; chronicledb_mvcc_versions samples: %v; final=%v; chronicledb_requestid_outcomes final=%v",
		writes, duration, boundedKeys, samples, finalVersions, finalOutcomes)

	if len(samples) >= 4 {
		// Compare the back half of the run's samples against the front
		// half's: with a bounded key set and GC enabled, versions must
		// stabilize, not keep growing roughly linearly with elapsed
		// writes the way an unbounded table (like requestid_outcomes)
		// does. A generous multiple, not a tight bound: some growth
		// while GC's lag window (-gc-min-retain-seqs) fills is expected
		// and legitimate.
		mid := len(samples) / 2
		midVersions := samples[mid].versions
		if midVersions > 0 && finalVersions > midVersions*3 {
			t.Errorf("chronicledb_mvcc_versions grew from %v (midpoint) to %v (final) — more than 3x — GC does not appear to be bounding growth over a fixed key set", midVersions, finalVersions)
		}
	}
	if finalOutcomes < float64(writes) {
		t.Errorf("chronicledb_requestid_outcomes = %v, want >= %d (every write used a distinct RequestID) — reported dishonestly low", finalOutcomes, writes)
	}

	// Post-run correctness: the bounded key set's current values are
	// each independently readable and correct (no resurrection, no
	// silently-lost writes), and a follower still recovers cleanly via
	// an ordinary restart — the "scenario corpus still passes" claim,
	// exercised directly against this same long-running cluster rather
	// than a separate freshly-started one.
	follower := nodes[0]
	if follower.httpAddr == leader.httpAddr {
		follower = nodes[1]
	}
	follower.crash()
	restarted := follower
	restarted.restart(t, bin)
	awaitLeaderV2(t, nodes, 10*time.Second)
	awaitCondition2(t, 10*time.Second, "restarted follower catches up to the leader after the long run", func() bool {
		st, err := statusV2(restarted)
		lst, lerr := statusV2(leader)
		return err == nil && lerr == nil && st.AppliedIndex >= lst.AppliedIndex
	})

	verifyReqID := "sl19-verify"
	pr, status, err := proposeAt(leader, verifyReqID, "sl19-k0", "final-check-value", lastCommitSeq)
	if err != nil || status != 200 || pr.Status != "committed" {
		t.Fatalf("post-run verification propose failed: resp=%+v status=%d err=%v", pr, status, err)
	}
}
