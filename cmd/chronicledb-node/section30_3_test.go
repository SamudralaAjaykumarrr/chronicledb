//go:build integration

// This file is docs/v0.6.0-plan.md §30.3's real-process suite. The
// mechanisms it combines are each already independently proven by this
// package's other integration tests (TestSL17/18/19/25) and by
// internal/node's TestAC10/11/12/SL12: this file's own job is
// specifically the property none of those exercise in isolation —
// §30.3 step 3's requirement that "the backup and the scrub must be
// overlapping and still running when the membership calls are issued,
// so Lane A2 saturation is actually exercised against Lane A1... a
// sequential arrangement would pass without proving anything" — plus a
// basic combined-load-and-shedding sanity check (step 1-2) covering
// every read endpoint answering throughout.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run Section30_3 -v
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func httpScrub(t *testing.T, rn *realNode) (scrubResponse, int, error) {
	t.Helper()
	resp, err := http.Post("http://"+rn.httpAddr+"/admin/storage/scrub", "application/octet-stream", nil)
	if err != nil {
		return scrubResponse{}, 0, err
	}
	defer resp.Body.Close()
	var sr scrubResponse
	decodeErr := json.NewDecoder(resp.Body).Decode(&sr)
	return sr, resp.StatusCode, decodeErr
}

func TestSection30_3_LaneA2OverlapAndShedding(t *testing.T) {
	bin := buildBinary(t)
	const maxInflight = 4
	nodes := newRealClusterWithFlags(t, bin, 3,
		fmt.Sprintf("-max-inflight-proposals=%d", maxInflight),
		"-admission-queue-depth=2", "-admission-max-wait=100ms",
		"-max-maintenance-concurrency=2",
	)
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	finalizeRealClusterToGeneration(t, leader, nodes, 3)
	loadBulkData(t, leader, 150) // ~9.4 MiB, enough that backup+scrub both take real time

	electionsAt := func() float64 {
		var total float64
		for _, rn := range nodes {
			v, _ := metricValue(t, rn, "chronicledb_raft_elections_total")
			total += v
		}
		return total
	}
	electionsBefore := electionsAt()

	// Step 1-2: sustained /propose load at ~4x the admission limit from
	// multiple concurrent "clients", overlapping everything below.
	stopLoad := make(chan struct{})
	var admitted, shed atomic.Int64
	var loadWG sync.WaitGroup
	for c := 0; c < maxInflight*4; c++ {
		loadWG.Add(1)
		go func(c int) {
			defer loadWG.Done()
			i := 0
			for {
				select {
				case <-stopLoad:
					return
				default:
				}
				reqID := fmt.Sprintf("s303-%d-%d", c, i)
				pr, status, err := propose(leader, reqID, fmt.Sprintf("s303k%d", c), "v")
				i++
				if err != nil {
					continue
				}
				if status == http.StatusServiceUnavailable {
					shed.Add(1)
					continue
				}
				if status == http.StatusOK && pr.Status == "committed" {
					admitted.Add(1)
				}
			}
		}(c)
	}

	// Step 3: backup and scrub launched together and left running —
	// neither is awaited before the membership call below fires.
	backupDone := make(chan error, 1)
	go func() {
		backupDir := t.TempDir() + "/s303-backup"
		url := fmt.Sprintf("http://%s/admin/backup?dir=%s&continuous=true", leader.httpAddr, backupDir)
		resp, err := http.Post(url, "application/octet-stream", nil)
		if err != nil {
			backupDone <- err
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			backupDone <- fmt.Errorf("backup status %d", resp.StatusCode)
			return
		}
		backupDone <- nil
	}()
	scrubDone := make(chan error, 1)
	go func() {
		sr, status, err := httpScrub(t, leader)
		if err != nil {
			scrubDone <- err
			return
		}
		if status != http.StatusOK {
			scrubDone <- fmt.Errorf("scrub status %d body %+v", status, sr)
			return
		}
		scrubDone <- nil
	}()

	// Give backup/scrub a moment to genuinely be mid-flight (not just
	// queued) before the membership call — both read several MiB of
	// real WAL content, which is not instantaneous. Verified, not
	// assumed: both channels must still be empty right before the
	// membership call fires, or this test would pass without proving
	// the one property §30.3 step 3 actually requires (a sequential
	// arrangement, by construction, cannot fail this check).
	time.Sleep(5 * time.Millisecond)
	select {
	case err := <-backupDone:
		t.Fatalf("backup already completed (err=%v) before the membership call was issued — not a genuine overlap; shorten the sleep or enlarge loadBulkData", err)
	default:
	}
	select {
	case err := <-scrubDone:
		t.Fatalf("scrub already completed (err=%v) before the membership call was issued — not a genuine overlap; shorten the sleep or enlarge loadBulkData", err)
	default:
	}

	precheckRes, precheckStatus, precheckErr := precheckHTTP(leader)
	if precheckErr != nil || precheckStatus != http.StatusOK {
		t.Errorf("/admin/upgrade/precheck while backup+scrub in flight: resp=%+v status=%d err=%v", precheckRes, precheckStatus, precheckErr)
	}

	addResp, addStatus, addErr := membershipAddHTTP(leader, "s303-add", "s303learner", "127.0.0.1:1")
	if addErr != nil || addStatus != http.StatusOK || addResp.Status != "committed" {
		t.Errorf("/admin/membership/add while backup+scrub in flight: resp=%+v status=%d err=%v — Lane A1 must never be blocked by Lane A2 saturation", addResp, addStatus, addErr)
	}

	if err := <-backupDone; err != nil {
		t.Errorf("concurrent backup failed: %v", err)
	}
	if err := <-scrubDone; err != nil {
		t.Errorf("concurrent scrub failed: %v", err)
	}

	// Read endpoints must answer throughout, checked now while load is
	// still running.
	for _, path := range []string{"/health", "/status", "/metrics", "/outcome?requestId=nonexistent"} {
		resp, err := http.Get("http://" + leader.httpAddr + path)
		if err != nil {
			t.Errorf("GET %s while under load: %v", path, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			t.Errorf("GET %s while under load: status %d, want a normal response (never 5xx for a read endpoint)", path, resp.StatusCode)
		}
	}

	close(stopLoad)
	loadWG.Wait()

	if admitted.Load() == 0 {
		t.Error("zero admitted proposals under 4x load — the whole load-generation phase produced nothing usable")
	}
	if shed.Load() == 0 {
		t.Error("zero 503s under 4x the configured admission limit — shedding did not occur")
	}
	t.Logf("section 30.3: admitted=%d shed=%d", admitted.Load(), shed.Load())

	electionsAfter := electionsAt()
	if electionsAfter != electionsBefore {
		t.Errorf("elections occurred during combined overload+backup+scrub+membership (before=%v after=%v), want zero", electionsBefore, electionsAfter)
	}
}
