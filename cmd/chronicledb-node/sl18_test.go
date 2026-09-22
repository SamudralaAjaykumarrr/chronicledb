//go:build integration

// This file is SL-18 (docs/v0.6.0-plan.md §30.2): a generation-3 backup
// taken while GC is live and has already advanced its watermark, then
// restored — both fully and with -restore-until targeting an earlier
// point in the same backup's WAL suffix — with the restored watermark
// carried over exactly (no ongoing GC on the restored node itself, so
// its exposed value is never anything but what restore actually wrote)
// and the horizon guard immediately and correctly enforced: a stale
// StartSeq is refused, a fresh one is not.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run SL18 -v
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// metricValue scrapes rn's /metrics for the first non-comment line
// beginning with exactly "name ", returning its trailing numeric value.
func metricValue(t *testing.T, rn *realNode, name string) (float64, bool) {
	t.Helper()
	resp, err := http.Get("http://" + rn.httpAddr + "/metrics")
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			v, err := strconv.ParseFloat(fields[1], 64)
			return v, err == nil
		}
	}
	return 0, false
}

func TestSL18_BackupRestoreCarriesGCStateAndHorizonHoldsImmediately(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealClusterWithFlags(t, bin, 3,
		"-gc-interval=100ms", "-gc-min-retain-seqs=0", "-gc-min-advance-seqs=0",
		"-gc-max-versions-per-pass=10000", "-gc-max-keys-per-pass=10000")
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration(t, leader, nodes, 3)

	// Repeatedly overwrite a small key set so GC has real chains to
	// reclaim and a horizon to advance past. Each proposal's StartSeq
	// tracks the highest CommitSeq observed so far (a real client's own
	// optimistic-read pattern) rather than a fixed 0: once GC (enabled
	// above with essentially no retain lag, to make this test fast)
	// starts advancing the watermark, a fixed StartSeq=0 would itself
	// immediately start reading as stale — the exact property under
	// test, but not one this warmup phase is trying to exercise yet.
	const keys = 5
	const overwritesPerKey = 40
	initialStatus, err := leader.status()
	if err != nil {
		t.Fatalf("leader /status: %v", err)
	}
	lastCommitSeq := initialStatus.AppliedIndex
	for round := 0; round < overwritesPerKey; round++ {
		for k := 0; k < keys; k++ {
			reqID := fmt.Sprintf("sl18-%d-%d", round, k)
			pr, status, err := proposeAt(leader, reqID, fmt.Sprintf("gk%d", k), fmt.Sprintf("v%d", round), lastCommitSeq)
			if err != nil || status != 200 || pr.Status != "committed" {
				t.Fatalf("propose round=%d key=%d: resp=%+v status=%d err=%v", round, k, pr, status, err)
			}
			lastCommitSeq = pr.CommitSeq
		}
	}

	var sourceWatermark float64
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := metricValue(t, leader, "chronicledb_mvcc_gc_watermark"); ok && v > 0 {
			sourceWatermark = v
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if sourceWatermark == 0 {
		t.Fatal("gc_watermark never advanced past 0 within 15s")
	}
	if passes, ok := metricValue(t, leader, "chronicledb_mvcc_gc_passes_total"); !ok || passes == 0 {
		t.Errorf("gc_passes_total = %v, ok=%v — want at least one completed pass (proves continuation passes actually run)", passes, ok)
	}

	backupDir := t.TempDir() + "/backup"
	br := httpBackup(t, leader, backupDir, true)

	checkRestored := func(name string, extraArgs ...string) {
		ports := freePorts(t, 2)
		rn := &realNode{
			id: name, raftAddr: ports[0], httpAddr: ports[1], dataDir: t.TempDir() + "/" + name,
			args: append([]string{"-id=" + name, "-listen=" + ports[0], "-cluster=" + name, "-restore-from=" + backupDir}, extraArgs...),
		}
		startRealNode(t, bin, rn)
		defer stopRealNode(rn)
		awaitLeader(t, []*realNode{rn}, 10*time.Second)

		watermark, ok := metricValue(t, rn, "chronicledb_mvcc_gc_watermark")
		if !ok {
			t.Fatalf("%s: gc_watermark not reported", name)
		}
		if watermark == 0 {
			t.Errorf("%s: restored gc_watermark = 0, want the reclaimed horizon carried over from the backup", name)
		}

		// Stale: a StartSeq of 1 is below any watermark that ever
		// advanced past the very first few writes.
		staleReqID := "sl18-stale-check-" + name
		pr, status, err := proposeAt(rn, staleReqID, "gk0", "stale-value", 1)
		if err != nil || status != 200 {
			t.Fatalf("%s: stale propose transport error: resp=%+v status=%d err=%v", name, pr, status, err)
		}
		if pr.Status != "aborted_stale" {
			t.Errorf("%s: stale propose (StartSeq=1) status = %q, want aborted_stale — the restored horizon guard did not fire", name, pr.Status)
		}

		// Fresh: a brand-new key with a genuinely current StartSeq (this
		// node's own current applied index) must commit normally — the
		// horizon must not fire spuriously.
		st, err := rn.status()
		if err != nil {
			t.Fatalf("%s: /status: %v", name, err)
		}
		freshReqID := "sl18-fresh-check-" + name
		pr2, status2, err2 := proposeAt(rn, freshReqID, "gk-fresh-"+name, "v", st.AppliedIndex)
		if err2 != nil || status2 != 200 || pr2.Status != "committed" {
			t.Errorf("%s: fresh propose on a new key = %+v status=%d err=%v, want committed", name, pr2, status2, err2)
		}
	}

	checkRestored("sl18-full") // UntilLatest (no -restore-until)

	// An earlier PITR boundary, still within the backup's own WAL
	// suffix and past its snapshot base.
	until := br.Manifest.LastIncludedIndex
	if until < br.Manifest.WALUntilIndex {
		until = (br.Manifest.LastIncludedIndex + br.Manifest.WALUntilIndex) / 2
	}
	checkRestored("sl18-until", fmt.Sprintf("-restore-until=%d", until))

}

// proposeAt is propose (main_test.go) with an explicit StartSeq — needed
// for the stale-horizon check above, which propose's own fixed
// StartSeq=0 cannot exercise once real commits have moved the log past
// the very first few entries (StartSeq=0 is itself already "stale" in
// the ordinary sense, but only ever produces a ConflictKey abort, not a
// StatusAbortedStale, unless it is also below gcWatermark — spelled out
// explicitly here via a real StartSeq argument rather than overloading
// propose's own fixed shape).
func proposeAt(rn *realNode, requestID, key, value string, startSeq uint64) (proposeResponse, int, error) {
	body := fmt.Sprintf(`{"requestId":%q,"txnId":1,"startSeq":%d,"mutations":[{"key":%q,"value":%q}]}`, requestID, startSeq, key, value)
	resp, err := http.Post("http://"+rn.httpAddr+"/propose", "application/json", strings.NewReader(body))
	if err != nil {
		return proposeResponse{}, 0, err
	}
	defer resp.Body.Close()
	var pr proposeResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return proposeResponse{}, resp.StatusCode, err
	}
	return pr, resp.StatusCode, nil
}
