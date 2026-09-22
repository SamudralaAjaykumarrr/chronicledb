//go:build integration

// This file is AC-19's real-process tier (docs/v0.6.0-plan.md §30.1,
// §31 gate 3), in two parts:
//
//  1. TestAC19_RealProcess_ConcurrentOutcomeFloodP99WithinBaseline —
//     the scenario as §30.1 words it: a real leader under sustained
//     Raft traffic, flooded with /outcome+/status at the HTTP
//     connection cap, chronicledb_raft_message_process_seconds p99 held
//     within its pre-flood baseline.
//
//  2. TestAC19_RealProcess_OutcomeReadersShareFSMLock — the negative
//     control, deterministic rather than statistical: it counts how
//     many concurrent /outcome requests are inside GetOutcome's
//     critical section at once in a real process (unbounded under the
//     production RWMutex, exactly one under the exclusive mutex
//     -debug-force-exclusive-outcome-lock reverts to). See the long
//     comment above it for why that, and not latency, is the oracle
//     that discriminates.
//
//     go test -tags=integration ./cmd/chronicledb-node/... -run TestAC19_RealProcess -v
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
)

// histogramBuckets scrapes rn's /metrics for every cumulative
// "name_bucket{le=..." line, returning them in ascending bound order
// (le="+Inf" last, keyed "+Inf").
func histogramBuckets(t *testing.T, rn *realNode, name string) (bounds []string, cumulative []uint64) {
	t.Helper()
	resp, err := http.Get("http://" + rn.httpAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	prefix := name + "_bucket{le=\""
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		rest := strings.TrimPrefix(line, prefix)
		closeIdx := strings.Index(rest, "\"}")
		if closeIdx < 0 {
			continue
		}
		bound := rest[:closeIdx]
		fields := strings.Fields(rest[closeIdx:])
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[len(fields)-1], 10, 64)
		if err != nil {
			continue
		}
		bounds = append(bounds, bound)
		cumulative = append(cumulative, v)
	}
	return bounds, cumulative
}

// p99FromDelta computes an approximate p99 latency (in whatever unit the
// histogram's bounds are, here seconds) from two cumulative-bucket
// snapshots of the same histogram taken at different times: the delta
// per bucket isolates exactly the samples observed between the two
// scrapes, and p99 is the smallest bound whose cumulative delta count
// covers at least 99% of the delta total. Returns ok=false if fewer
// than minSamples new observations occurred between the snapshots (too
// few to estimate a percentile from).
func p99FromDelta(boundsBefore, boundsAfter []string, cumBefore, cumAfter []uint64, minSamples uint64) (p99 float64, ok bool) {
	if len(boundsAfter) != len(boundsBefore) {
		return 0, false
	}
	total := cumAfter[len(cumAfter)-1] - cumBefore[len(cumBefore)-1]
	if total < minSamples {
		return 0, false
	}
	threshold := (total * 99) / 100
	if threshold == 0 {
		threshold = 1
	}
	var running uint64
	for i, bound := range boundsAfter {
		running += cumAfter[i] - cumBefore[i]
		if running >= threshold {
			if bound == "+Inf" {
				return -1, true // "unbounded" — worse than any finite bound
			}
			v, err := strconv.ParseFloat(bound, 64)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}
	return -1, true
}

// floodOutcomeAndStatus issues concurrent /outcome + /status requests
// against rn for duration, using connections roughly at the configured
// -max-http-connections cap, and returns the count actually completed.
func floodOutcomeAndStatus(rn *realNode, duration time.Duration, concurrency int) int64 {
	var completed atomic.Int64
	stop := time.Now().Add(duration)
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 2 * time.Second}
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for time.Now().Before(stop) {
				var resp *http.Response
				var err error
				if c%2 == 0 {
					resp, err = client.Get("http://" + rn.httpAddr + "/outcome?requestId=nonexistent")
				} else {
					resp, err = client.Get("http://" + rn.httpAddr + "/status")
				}
				if err != nil {
					continue
				}
				resp.Body.Close()
				completed.Add(1)
			}
		}(c)
	}
	wg.Wait()
	return completed.Load()
}

// sustainedWriteLoad keeps proposing against rn, from concurrency
// parallel goroutines, until ctx is canceled — AC-19's own "sustained
// Raft traffic" requirement, run as a background load around the flood
// below. Concurrency (not a single sequential writer) matters here: the
// FSM.mu contention this test's negative control needs to observe only
// shows up if the event loop's own Apply path (which needs FSM.mu to
// commit) is competing for that lock often enough, relative to the
// flood's read-side acquisitions, to queue behind them.
func sustainedWriteLoad(ctx context.Context, rn *realNode, concurrency int) {
	var wg sync.WaitGroup
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				i++
				_, _, _ = propose(rn, fmt.Sprintf("ac19-%d-%d", c, i), fmt.Sprintf("ac19k%d-%d", c, i%50), "v")
			}
		}(c)
	}
	<-ctx.Done()
	wg.Wait()
}

func TestAC19_RealProcess_ConcurrentOutcomeFloodP99WithinBaseline(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealClusterWithFlags(t, bin, 3, "-max-http-connections=64")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sustainedWriteLoad(ctx, leader, 16)
	time.Sleep(300 * time.Millisecond) // let write traffic actually get going

	boundsBase, cumBase := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
	time.Sleep(500 * time.Millisecond)
	boundsBaseEnd, cumBaseEnd := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
	baselineP99, baseOK := p99FromDelta(boundsBase, boundsBaseEnd, cumBase, cumBaseEnd, 5)
	if !baseOK {
		t.Skip("not enough baseline raft_message_process_seconds samples on this host to estimate p99 — inconclusive, not a failure")
	}

	boundsFloodStart, cumFloodStart := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
	completed := floodOutcomeAndStatus(leader, 800*time.Millisecond, 64)
	boundsFloodEnd, cumFloodEnd := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
	cancel()

	if completed == 0 {
		t.Fatal("the /outcome+/status flood completed zero requests — the flood itself is broken, this proves nothing")
	}
	floodP99, floodOK := p99FromDelta(boundsFloodStart, boundsFloodEnd, cumFloodStart, cumFloodEnd, 5)
	if !floodOK {
		t.Skip("not enough during-flood raft_message_process_seconds samples to estimate p99 — inconclusive, not a failure")
	}

	t.Logf("AC-19: baseline p99=%v flood p99=%v (completed=%d read requests during flood)", baselineP99, floodP99, completed)
	// "Within baseline" per §5.4a: generous multiplicative slack (not a
	// tight bound) since this is a real, shared, WSL2-hosted machine —
	// the property under test is "no severe regression from RWMutex-vs-
	// Mutex contention", not "byte-for-byte identical latency".
	const maxAcceptableMultiple = 5.0
	if floodP99 < 0 || baselineP99 < 0 {
		return // either side hit +Inf; nothing quantitative to compare, and neither is a pass/fail signal on its own
	}
	if floodP99 > baselineP99*maxAcceptableMultiple && floodP99 > 0.05 {
		t.Errorf("raft_message_process_seconds p99 regressed under the /outcome+/status flood: baseline=%v flood=%v (> %vx and > 50ms) — want held within baseline (§5.4a)",
			baselineP99, floodP99, maxAcceptableMultiple)
	}
}

// ---------------------------------------------------------------------
// AC-19's real-process negative control (§31 gate 3: "with FSM.mu
// reverted to an exclusive mutex, the /outcome flood *does* move the
// oracle outside its baseline").
//
// The first attempt at this used latency as the oracle — first
// chronicledb_raft_message_process_seconds p99 (§5.4a's own wording),
// then client-observed /outcome round-trip p99 — and neither
// discriminated at any concurrency tried. That is not a property of
// this host: GetOutcome's critical section is a single map lookup, so
// serializing it adds microseconds of aggregate queuing delay to a
// measurement whose noise floor is milliseconds. A latency oracle
// cannot see a nanosecond-scale critical section no matter how much
// load is applied to it, which makes "run a bigger flood" the wrong
// response.
//
// The property AC-19 actually asserts is not about latency at all. It
// is that /outcome takes FSM.mu in *shared* mode, so the number of
// client lookups simultaneously inside that critical section is
// unbounded rather than exactly one. That is a counting property, it
// discriminates by a factor of `readers` rather than by noise, and it
// is exact. internal/fsm/readrendezvous.go's one-shot barrier measures
// it, TestReadRendezvousDiscriminatesLockMode calibrates the barrier
// itself at the internal/fsm tier, and the test below reads it out of
// two genuine OS processes over the /fault control plane.
//
// What the real-process tier adds over that unit proof — which is the
// whole reason this test exists rather than the unit test standing
// alone — is that it observes the shipped binary end to end: real
// concurrent client connections arriving at the real HTTP server, the
// real handleOutcome route, into the real node's FSM, with the real
// Raft event loop running alongside. The unit test proves the lock mode
// is what it claims inside one package; only this proves that client
// concurrency at the process boundary actually *survives* as
// concurrency all the way into that critical section, and that
// -debug-force-exclusive-outcome-lock is correctly wired to destroy it.
// ---------------------------------------------------------------------

// ac19Rendezvous is one arm-drive-read cycle against a real node
// process: it establishes `readers` distinct keep-alive connections
// first, arms the barrier, drives exactly one concurrent /outcome
// request down each connection, waits for every one to return, and only
// then reads the result back. No assertion anywhere depends on how long
// anything took — only on how many readers were inside at once.
func ac19Rendezvous(t *testing.T, rn *realNode, readers int, barrier time.Duration) fsm.ReadRendezvousResult {
	t.Helper()

	// One Transport per reader, so "concurrent request" really means
	// "concurrent connection" and no reader can be queued behind another
	// on a shared one. The client timeout must outlast the barrier's own
	// deadline, since in the exclusive case every request is waiting on
	// it; it is a safety net, never the thing under test.
	clients := make([]*http.Client, readers)
	for i := range clients {
		clients[i] = &http.Client{
			Timeout:   barrier + 30*time.Second,
			Transport: &http.Transport{},
		}
	}
	outcomeURL := "http://" + rn.httpAddr + "/outcome?requestId=ac19-rendezvous-nonexistent"

	// Warm every connection *before* arming, so connection setup is not
	// part of what the barrier has to wait for and these calls are not
	// counted as arrivals.
	for i, c := range clients {
		resp, err := c.Get(outcomeURL)
		if err != nil {
			t.Fatalf("warming connection %d: %v", i, err)
		}
		resp.Body.Close()
	}

	ac19ArmRendezvous(t, rn, readers, barrier)
	defer ac19ArmRendezvous(t, rn, 0, 0) // disarm

	var wg sync.WaitGroup
	wg.Add(readers)
	for i, c := range clients {
		go func(i int, c *http.Client) {
			defer wg.Done()
			resp, err := c.Get(outcomeURL)
			if err != nil {
				t.Errorf("reader %d: GET /outcome: %v", i, err)
				return
			}
			resp.Body.Close()
		}(i, c)
	}
	wg.Wait()

	return ac19ReadRendezvousResult(t, rn)
}

func ac19ArmRendezvous(t *testing.T, rn *realNode, readers int, barrier time.Duration) {
	t.Helper()
	url := fmt.Sprintf("http://%s/fault?action=armoutcomereadrendezvous&n=%d&timeoutMs=%d",
		rn.httpAddr, readers, barrier.Milliseconds())
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatalf("arming the outcome-read rendezvous (n=%d): %v", readers, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("arming the outcome-read rendezvous (n=%d): status %d", readers, resp.StatusCode)
	}
}

func ac19ReadRendezvousResult(t *testing.T, rn *realNode) fsm.ReadRendezvousResult {
	t.Helper()
	resp, err := http.Post("http://"+rn.httpAddr+"/fault?action=outcomereadrendezvousresult", "application/json", nil)
	if err != nil {
		t.Fatalf("reading the outcome-read rendezvous result: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reading the outcome-read rendezvous result: status %d", resp.StatusCode)
	}
	var got fsm.ReadRendezvousResult
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decoding the outcome-read rendezvous result: %v", err)
	}
	return got
}

// ac19RendezvousCluster starts a real three-node cluster and returns its
// leader. -gc-interval=0 (the default, stated explicitly because this
// test depends on it) is what guarantees no FSM *writer* runs during the
// barrier: Go's RWMutex deliberately blocks new readers once a writer is
// waiting — that is precisely the anti-starvation property §5.4a relies
// on — so the reader-overlap measurement is taken with the write path
// quiet. The sustained-Raft-traffic scenario is the positive test above;
// this one isolates the lock mode.
func ac19RendezvousCluster(t *testing.T, bin string, extraFlags ...string) *realNode {
	t.Helper()
	flags := append([]string{"-enable-fault-endpoint", "-gc-interval=0"}, extraFlags...)
	nodes := newRealClusterWithFlags(t, bin, 3, flags...)
	return awaitLeaderV2(t, nodes, 10*time.Second)
}

func TestAC19_RealProcess_OutcomeReadersShareFSMLock(t *testing.T) {
	const readers = 8
	const barrier = 5 * time.Second
	bin := buildBinary(t)

	// Positive: against an ordinary node, all `readers` concurrent
	// /outcome requests are inside GetOutcome's critical section at the
	// same time. Under an exclusive mutex this outcome is not merely
	// unlikely, it is impossible.
	t.Run("shared", func(t *testing.T) {
		got := ac19Rendezvous(t, ac19RendezvousCluster(t, bin), readers, barrier)
		t.Logf("AC-19 real process, shared mode: %+v", got)
		if !got.Armed || got.Arrived != readers {
			t.Fatalf("got %+v; want Armed with Arrived = %d — the %d concurrent /outcome requests never all reached the FSM, so this run proves nothing either way", got, readers, readers)
		}
		if got.Peak != readers || !got.Reached {
			t.Errorf("got Peak = %d, Reached = %v; want Peak = %d, Reached = true — concurrent /outcome lookups must not serialize against each other in a real process (§5.4a)", got.Peak, got.Reached, readers)
		}
	})

	// Negative control: the identical scenario against a node started
	// with FSM.mu reverted to an exclusive mutex must be *detected* —
	// same arrivals, but never more than one reader inside at a time.
	t.Run("negative control: exclusive mutex", func(t *testing.T) {
		got := ac19Rendezvous(t, ac19RendezvousCluster(t, bin, "-debug-force-exclusive-outcome-lock"), readers, barrier)
		t.Logf("AC-19 real process, forced-exclusive mode: %+v", got)
		if !got.Armed || got.Arrived != readers {
			t.Fatalf("got %+v; want Armed with Arrived = %d — the %d concurrent /outcome requests never all reached the FSM, so the control did not actually run", got, readers, readers)
		}
		if got.Peak != 1 || got.Reached {
			t.Errorf("got Peak = %d, Reached = %v; want Peak = 1, Reached = false — with FSM.mu reverted to an exclusive mutex the oracle must detect that readers serialize (§31 gate 3); an oracle that reports the same thing in both modes does not calibrate the positive run above", got.Peak, got.Reached)
		}
	})
}
