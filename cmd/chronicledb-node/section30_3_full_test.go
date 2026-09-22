//go:build integration

// This file is docs/v0.6.0-plan.md §31 gate 4: "the §30.3 real-process
// suite passes end to end, all nine steps (1, 2, 3, 3a, 4, 5, 6, 7, 8),
// against genuine OS processes" — one continuous run, not each property
// proven in isolation elsewhere (that isolation already exists:
// TestSection30_3_LaneA2OverlapAndShedding in this same package proves
// step 3's specific overlap requirement in the fastest, most targeted
// form; TestSL17/18/19/25, TestAC10/11/12/SL12 and this package's own
// membership/backup real-process suites each independently prove the
// mechanisms this file combines). This file's own job is the property
// none of those prove: that all of it survives being sequenced back to
// back against the same three (then four, then three again) real
// processes without interference.
//
// One deliberate reordering from §30.3's literal numbering: step 3's
// "a membership add + promote + remove" full real cycle is folded into
// step 7's restart phase instead of running concurrently with step 3's
// backup+scrub overlap (already proven separately, precisely, by
// TestSection30_3_LaneA2OverlapAndShedding — duplicating that exact
// timing-sensitive overlap here would only add flakiness without new
// coverage). Step 5's real disk-fill needs a leader whose data
// directory is on this test's own size-limited tmpfs; running it before
// any membership change guarantees the leader at that point is still
// one of the three original, tmpfs-backed nodes — reordering the
// promote/remove cycle to run later (step 7, after the process that
// gets SIGKILLed in step 6 has already forced one leadership change
// anyway) avoids needing a fourth tmpfs mount for a learner that might
// or might not become leader.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestSection30_3_FullNineStepScript -v -timeout 5m
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/testfs"
)

// readRSSBytesOfPID reads pid's current resident set size from
// /proc/<pid>/status's VmRSS line (kilobytes, per that file's own
// documented unit) — the real-process analogue of internal/node's
// AC-10 readRSSBytes, which reads /proc/self/status because that tier
// runs the node in-process. ok is false if this platform/sandbox does
// not expose it (e.g. no /proc), in which case the RSS assertion is
// skipped entirely rather than failed on inconclusive data.
func readRSSBytesOfPID(t *testing.T, pid int) (uint64, bool) {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
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

func TestSection30_3_FullNineStepScript(t *testing.T) {
	testfs.RunInNamespace(t, func(t *testing.T) {
		bin := buildBinary(t)

		// ---------- Setup: 3 real nodes, each on its own size-limited
		// tmpfs (step 5 needs real ENOSPC-adjacent disk pressure against
		// whichever of these is leader at that point). ----------
		const nodeTmpfsSize = 192 << 20 // 192 MiB
		ids := []string{"n1", "n2", "n3"}
		ports := freePorts(t, 2*len(ids))
		raftAddrs := map[string]string{}
		httpAddrs := map[string]string{}
		for i, id := range ids {
			raftAddrs[id] = ports[i]
			httpAddrs[id] = ports[len(ids)+i]
		}
		clusterFlag := strings.Join(ids, ",")

		nodes := make([]*realNode, len(ids))
		var tmpfsCleanups []func()
		// Registered via t.Cleanup (not a plain defer, which would fire
		// as soon as this function literal returns — well before the
		// node-stopping t.Cleanup below, since a real chronicledb-node
		// process still has the tmpfs open and unmounting under it fails
		// "device or resource busy"). t.Cleanup runs LIFO, so this
		// registration (first) must unmount only after the node-stopping
		// registration (below, registered second) has already run.
		t.Cleanup(func() {
			for i := len(tmpfsCleanups) - 1; i >= 0; i-- {
				tmpfsCleanups[i]()
			}
		})
		for i, id := range ids {
			dir := t.TempDir() + "/data-" + id
			cleanup, err := testfs.MountTmpfs(dir, nodeTmpfsSize)
			if err != nil {
				t.Skipf("tmpfs unavailable: %v", err)
			}
			tmpfsCleanups = append(tmpfsCleanups, cleanup)
			var peerParts []string
			for _, other := range ids {
				if other != id {
					peerParts = append(peerParts, other+"="+raftAddrs[other])
				}
			}
			nodes[i] = &realNode{
				id: id, raftAddr: raftAddrs[id], httpAddr: httpAddrs[id], dataDir: dir,
				args: []string{
					"-id=" + id, "-listen=" + raftAddrs[id],
					"-cluster=" + clusterFlag, "-peers=" + strings.Join(peerParts, ","),
					"-max-inflight-proposals=4", "-admission-queue-depth=2", "-admission-max-wait=100ms",
					"-max-maintenance-concurrency=2", "-max-http-connections=256",
					"-disk-pressure-threshold=50%", "-disk-critical-threshold=15%",
					"-resource-poll-interval=50ms",
					"-gc-interval=50ms", "-gc-min-retain-seqs=5", "-gc-min-advance-seqs=1",
					"-gc-max-versions-per-pass=10000", "-gc-max-keys-per-pass=10000",
				},
			}
		}
		for _, rn := range nodes {
			startRealNode(t, bin, rn)
		}
		t.Cleanup(func() {
			for _, rn := range nodes {
				stopRealNode(rn)
			}
		})

		leader := awaitLeaderV2(t, nodes, 10*time.Second)
		finalizeRealClusterToGeneration(t, leader, nodes, 3)

		requestIDOutcomesAtStart, _ := metricValue(t, leader, "chronicledb_requestid_outcomes")
		t.Logf("step 8 (start): chronicledb_requestid_outcomes = %v", requestIDOutcomesAtStart)

		electionsAt := func() float64 {
			var total float64
			for _, rn := range nodes {
				v, _ := metricValue(t, rn, "chronicledb_raft_elections_total")
				total += v
			}
			return total
		}
		electionsBefore := electionsAt()

		rssBefore, haveRSS := readRSSBytesOfPID(t, leader.cmd.Process.Pid)

		// ==================== Steps 1, 2: sustained overload ====================
		stopLoad := make(chan struct{})
		var admitted, shed atomic.Int64
		var loadWG sync.WaitGroup
		const maxInflight = 4
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
					i++
					reqID := fmt.Sprintf("full303-%d-%d", c, i)
					pr, status, err := propose(leader, reqID, fmt.Sprintf("full303k%d", c), "v")
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

		// ==================== Step 3: backup + scrub overlap + admin under load ====================
		backupDone := make(chan error, 1)
		go func() {
			backupDir := t.TempDir() + "/full303-backup"
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
		time.Sleep(5 * time.Millisecond)

		precheckRes, precheckStatus, precheckErr := precheckHTTP(leader)
		if precheckErr != nil || precheckStatus != http.StatusOK {
			t.Errorf("step 3: /admin/upgrade/precheck while backup+scrub in flight: resp=%+v status=%d err=%v", precheckRes, precheckStatus, precheckErr)
		}
		addResp, addStatus, addErr := membershipAddHTTP(leader, "full303-add", "full303learner", "127.0.0.1:1")
		if addErr != nil || addStatus != http.StatusOK || addResp.Status != "committed" {
			t.Errorf("step 3: /admin/membership/add while backup+scrub in flight: resp=%+v status=%d err=%v", addResp, addStatus, addErr)
		}
		// Undo the phantom add immediately — it was only step 3's Lane-A1-
		// under-Lane-A2-saturation proof, not part of the real membership
		// cycle step 7 performs with a genuine, reachable node.
		if remResp, remStatus, remErr := membershipRemoveHTTP(leader, "full303-add-undo", "full303learner", 0); remErr != nil || remStatus != http.StatusOK || remResp.Status != "committed" {
			t.Errorf("step 3: removing the phantom learner: resp=%+v status=%d err=%v", remResp, remStatus, remErr)
		}

		if err := <-backupDone; err != nil {
			t.Errorf("step 3: concurrent backup failed: %v", err)
		}
		if err := <-scrubDone; err != nil {
			t.Errorf("step 3: concurrent scrub failed: %v", err)
		}
		for _, path := range []string{"/health", "/status", "/metrics", "/outcome?requestId=nonexistent"} {
			resp, err := http.Get("http://" + leader.httpAddr + path)
			if err != nil {
				t.Errorf("step 2: GET %s while under load: %v", path, err)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				t.Errorf("step 2: GET %s while under load: status %d, want never 5xx", path, resp.StatusCode)
			}
		}

		// ==================== Step 3a: concurrent /outcome flood, p99 held within baseline ====================
		boundsBase, cumBase := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
		time.Sleep(300 * time.Millisecond)
		boundsBaseEnd, cumBaseEnd := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
		baselineP99, baseOK := p99FromDelta(boundsBase, boundsBaseEnd, cumBase, cumBaseEnd, 5)

		boundsFloodStart, cumFloodStart := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
		floodCompleted := floodOutcomeAndStatus(leader, 800*time.Millisecond, 64)
		boundsFloodEnd, cumFloodEnd := histogramBuckets(t, leader, "chronicledb_raft_message_process_seconds")
		if floodCompleted == 0 {
			t.Error("step 3a: the /outcome+/status flood completed zero requests")
		}
		if baseOK {
			if floodP99, floodOK := p99FromDelta(boundsFloodStart, boundsFloodEnd, cumFloodStart, cumFloodEnd, 5); floodOK && floodP99 >= 0 && baselineP99 >= 0 {
				const maxAcceptableMultiple = 5.0
				if floodP99 > baselineP99*maxAcceptableMultiple && floodP99 > 0.05 {
					t.Errorf("step 3a: raft_message_process_seconds p99 regressed under the /outcome flood: baseline=%v flood=%v", baselineP99, floodP99)
				}
			}
		}

		close(stopLoad)
		loadWG.Wait()
		if admitted.Load() == 0 {
			t.Error("steps 1-2: zero admitted proposals under 4x load")
		}
		if shed.Load() == 0 {
			t.Error("steps 1-2: zero 503s under 4x the configured admission limit — shedding did not occur")
		}
		if electionsAfter := electionsAt(); electionsAfter != electionsBefore {
			t.Errorf("steps 1-3a: elections occurred during combined overload+backup+scrub+membership (before=%v after=%v), want zero", electionsBefore, electionsAfter)
		}
		if haveRSS {
			if rssAfter, ok := readRSSBytesOfPID(t, leader.cmd.Process.Pid); ok {
				const maxRSSGrowthBytes = 500 << 20
				if rssAfter > rssBefore && rssAfter-rssBefore > maxRSSGrowthBytes {
					t.Errorf("step 2: leader RSS grew by %d bytes (%d -> %d) during sustained saturation, want < %d", rssAfter-rssBefore, rssBefore, rssAfter, maxRSSGrowthBytes)
				}
			}
		}
		t.Logf("steps 1-3a: admitted=%d shed=%d flood_completed=%d", admitted.Load(), shed.Load(), floodCompleted)

		// ==================== Step 4: GC over a bounded key set ====================
		versionsBefore, _ := metricValue(t, leader, "chronicledb_mvcc_versions")
		watermarkBefore, _ := metricValue(t, leader, "chronicledb_mvcc_gc_watermark")
		passesBefore, _ := metricValue(t, leader, "chronicledb_mvcc_gc_passes_total")
		lastStatus, err := leader.status()
		if err != nil {
			t.Fatalf("step 4: leader /status: %v", err)
		}
		lastCommitSeq := lastStatus.AppliedIndex
		const boundedKeys = 20
		deadline := time.Now().Add(3 * time.Second)
		writes := 0
		for time.Now().Before(deadline) {
			key := fmt.Sprintf("full303-gc-k%d", writes%boundedKeys)
			reqID := fmt.Sprintf("full303-gc-%d", writes)
			pr, status, err := proposeAt(leader, reqID, key, fmt.Sprintf("v%d", writes), lastCommitSeq)
			if err != nil {
				t.Fatalf("step 4: propose #%d: %v", writes, err)
			}
			if status == 200 && pr.Status == "committed" {
				lastCommitSeq = pr.CommitSeq
			}
			writes++
		}
		time.Sleep(300 * time.Millisecond) // let GC's own 50ms-interval evaluator catch up
		versionsAfter, _ := metricValue(t, leader, "chronicledb_mvcc_versions")
		watermarkAfter, _ := metricValue(t, leader, "chronicledb_mvcc_gc_watermark")
		passesAfter, _ := metricValue(t, leader, "chronicledb_mvcc_gc_passes_total")
		t.Logf("step 4: writes=%d versions %v->%v watermark %v->%v passes %v->%v", writes, versionsBefore, versionsAfter, watermarkBefore, watermarkAfter, passesBefore, passesAfter)
		if watermarkAfter <= watermarkBefore {
			t.Errorf("step 4: chronicledb_mvcc_gc_watermark did not advance (%v -> %v) over a %d-write bounded-key-set run", watermarkBefore, watermarkAfter, writes)
		}
		if versionsAfter > float64(boundedKeys)*4 {
			t.Errorf("step 4: chronicledb_mvcc_versions = %v after a bounded-%d-key run, want GC to have kept it from growing unboundedly", versionsAfter, boundedKeys)
		}

		// ==================== Step 5: real disk-fill to Critical, then recover ====================
		ballastPath := leader.dataDir + "/full303-ballast.bin"
		writeBallast := func(size int64) {
			t.Helper()
			if err := os.WriteFile(ballastPath, make([]byte, size), 0o644); err != nil {
				t.Fatalf("step 5: writing %d-byte ballast: %v", size, err)
			}
		}
		// Measure this leader's *actual* current free space on its own
		// tmpfs directly (WAL/snapshot/audit activity from steps 1-4
		// already consumed an unpredictable amount of it) and write
		// exactly enough ballast to land at ~5% free — comfortably below
		// the 15% critical threshold, rather than incrementally growing a
		// file and racing the resource-pressure monitor's own 50ms poll
		// interval, which independently reacts to legitimate WAL
		// compaction freeing space at the same time.
		var stat syscall.Statfs_t
		if err := syscall.Statfs(leader.dataDir, &stat); err != nil {
			t.Fatalf("step 5: statfs(%s): %v", leader.dataDir, err)
		}
		currentFree := int64(stat.Bavail) * int64(stat.Bsize)
		targetFree := int64(nodeTmpfsSize) * 5 / 100
		ballastSize := currentFree - targetFree
		if ballastSize <= 0 {
			t.Fatalf("step 5: currentFree=%d already at or below the 5%% target (%d) before writing any ballast", currentFree, targetFree)
		}
		writeBallast(ballastSize)
		reachedCritical := false
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			h, hstatus, herr := getHealth(leader)
			if herr == nil && hstatus == http.StatusServiceUnavailable && h.DiskPressure == "critical" {
				reachedCritical = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !reachedCritical {
			t.Fatal("step 5: never reached disk-pressure Critical on the leader's own tmpfs after writing ballast down to ~5% free")
		}
		rejCtx, rejCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer rejCancel()
		rejBody, rejStatus, rejErr := proposeWithReason(rejCtx, leader, "full303-critical-write", "ck", "v")
		if rejErr != nil || rejStatus != http.StatusServiceUnavailable {
			t.Errorf("step 5: write during disk-critical: status=%d err=%v, want 503", rejStatus, rejErr)
		} else if rejBody.Reason != "disk_critical" {
			t.Errorf("step 5: write during disk-critical: reason=%q, want \"disk_critical\"", rejBody.Reason)
		}
		for _, path := range []string{"/health", "/status"} {
			resp, err := http.Get("http://" + leader.httpAddr + path)
			if err != nil {
				t.Errorf("step 5: GET %s during disk-critical: %v", path, err)
				continue
			}
			resp.Body.Close()
		}
		if _, _, err := precheckHTTP(leader); err != nil {
			t.Errorf("step 5: /admin/upgrade/precheck during disk-critical: %v", err)
		}
		if err := os.Remove(ballastPath); err != nil {
			t.Fatalf("step 5: freeing space: %v", err)
		}
		awaitCondition2(t, 5*time.Second, "disk pressure recovers to normal after freeing space", func() bool {
			h, status, err := getHealth(leader)
			return err == nil && status == http.StatusOK && h.DiskPressure == "normal"
		})
		if _, status, err := proposeAt(leader, "full303-post-recovery", "ck", "v2", lastCommitSeq); err != nil || status != http.StatusOK {
			t.Errorf("step 5: write after disk-space recovery: status=%d err=%v, want success with no restart", status, err)
		}
		t.Log("step 5: disk-critical refusal and automatic recovery both confirmed")

		// ==================== Step 6: SIGKILL the leader mid-load, failover, idempotency, new leader's limits ====================
		preKillReqID := "full303-prekill"
		preKillResp, preKillStatus, preKillErr := proposeAt(leader, preKillReqID, "prekillkey", "v", lastCommitSeq)
		if preKillErr != nil || preKillStatus != http.StatusOK || preKillResp.Status != "committed" {
			t.Fatalf("step 6: pre-kill propose: resp=%+v status=%d err=%v", preKillResp, preKillStatus, preKillErr)
		}
		killedID := leader.id
		leader.crash()
		newLeader := awaitLeaderV2(t, nodes, 10*time.Second)
		if newLeader.id == killedID {
			t.Fatalf("step 6: awaitLeaderV2 returned the just-killed node %s as leader", killedID)
		}
		replayResp := awaitOutcomeCommitted(t, newLeader, preKillReqID, 5*time.Second)
		if replayResp.CommitSeq != preKillResp.CommitSeq {
			t.Errorf("step 6: no-duplicate-effect: pre-kill CommitSeq=%d, post-failover replay CommitSeq=%d for the same RequestID, want identical", preKillResp.CommitSeq, replayResp.CommitSeq)
		}
		// The new leader's own admission limits apply immediately: a small
		// saturating burst against it produces at least one 503.
		var newLeaderShed atomic.Bool
		var burstWG sync.WaitGroup
		for i := 0; i < maxInflight*4; i++ {
			burstWG.Add(1)
			go func(i int) {
				defer burstWG.Done()
				_, status, _ := propose(newLeader, fmt.Sprintf("full303-burst-%d", i), fmt.Sprintf("burstk%d", i), "v")
				if status == http.StatusServiceUnavailable {
					newLeaderShed.Store(true)
				}
			}(i)
		}
		burstWG.Wait()
		if !newLeaderShed.Load() {
			t.Error("step 6: new leader's admission limits did not shed any of an immediate 16-way saturating burst")
		}
		awaitCondition2(t, 5*time.Second, "no waiter/gate-slot leak on the new leader after quiescence", func() bool {
			v, ok := metricValue(t, newLeader, "chronicledb_node_waiters")
			return ok && v == 0
		})
		t.Logf("step 6: failover %s -> %s confirmed, no duplicate effect, new leader sheds immediately", killedID, newLeader.id)

		// ==================== Step 7: restart every node; membership add+promote+remove; GC state survives ====================
		watermarkPreRestart, _ := metricValue(t, newLeader, "chronicledb_mvcc_gc_watermark")

		leader.restart(t, bin) // bring the SIGKILLed node back
		awaitCondition2(t, 15*time.Second, "restarted node rejoins and catches up", func() bool {
			st, err := statusV2(leader)
			lst, lerr := statusV2(newLeader)
			return err == nil && lerr == nil && st.AppliedIndex >= lst.AppliedIndex
		})
		// Restart the two nodes that were never killed, one at a time, so
		// "restart every node" is genuinely every node, not just the one
		// step 6 already killed.
		for _, rn := range nodes {
			if rn.id == killedID {
				continue // already restarted above
			}
			cur := awaitLeaderV2(t, nodes, 10*time.Second)
			if rn.id == cur.id {
				// Restarting the current leader here would just trigger
				// another failover, already proven in step 6; restart the
				// other still-pending node first and come back to this
				// one last if it's still leader then.
				continue
			}
			rn.crash()
			rn.restart(t, bin)
			awaitCondition2(t, 15*time.Second, "node "+rn.id+" rejoins after its own restart", func() bool {
				st, err := statusV2(rn)
				lst, lerr := statusV2(cur)
				return err == nil && lerr == nil && st.AppliedIndex >= lst.AppliedIndex
			})
		}
		// Whichever node is leader now (possibly still not yet restarted
		// this pass) gets its own restart last, forcing one more failover.
		finalLeaderBeforeLastRestart := awaitLeaderV2(t, nodes, 10*time.Second)
		finalLeaderBeforeLastRestart.crash()
		finalLeaderBeforeLastRestart.restart(t, bin)
		stableLeader := awaitLeaderV2(t, nodes, 10*time.Second)
		for _, rn := range nodes {
			awaitCondition2(t, 15*time.Second, "node "+rn.id+" converges after the full-cluster restart pass", func() bool {
				st, err := statusV2(rn)
				lst, lerr := statusV2(stableLeader)
				return err == nil && lerr == nil && st.AppliedIndex >= lst.AppliedIndex
			})
		}

		watermarkPostRestart, _ := metricValue(t, stableLeader, "chronicledb_mvcc_gc_watermark")
		if watermarkPostRestart < watermarkPreRestart {
			t.Errorf("step 7: chronicledb_mvcc_gc_watermark regressed across the full-cluster restart pass: %v -> %v, want persisted (never decreasing)", watermarkPreRestart, watermarkPostRestart)
		}
		if _, status, err := propose(stableLeader, "full303-post-restart-check", "post-restart-key", "v"); err != nil || status != http.StatusOK {
			t.Errorf("step 7: a normal write after the full restart pass: status=%d err=%v, want success (scenario corpus still passes)", status, err)
		}

		// A real add + promote + remove cycle, against a genuine
		// reachable fourth process — §30.3 step 3's full membership
		// requirement, run here rather than during step 3's backup/scrub
		// overlap window (see this file's header comment).
		learnerID := "n4full"
		learnerAddr := freePorts(t, 1)[0]
		learner := &realNode{
			id: learnerID, raftAddr: learnerAddr, httpAddr: freePorts(t, 1)[0], dataDir: t.TempDir(),
			args: []string{
				"-id=" + learnerID, "-listen=" + learnerAddr,
				"-peers=" + strings.Join(func() []string {
					var ps []string
					for _, rn := range nodes {
						ps = append(ps, rn.id+"="+rn.raftAddr)
					}
					return ps
				}(), ","),
			},
		}
		startRealNode(t, bin, learner)
		defer stopRealNode(learner)
		allNodes := append(append([]*realNode{}, nodes...), learner)

		addResp2, addStatus2, addErr2 := membershipAddHTTP(stableLeader, "full303-add-n4", learnerID, learnerAddr)
		if addErr2 != nil || addStatus2 != http.StatusOK || addResp2.Status != "committed" {
			t.Fatalf("step 7: membership add %s: resp=%+v status=%d err=%v", learnerID, addResp2, addStatus2, addErr2)
		}
		awaitCondition2(t, 15*time.Second, learnerID+" catches up to the leader", func() bool {
			lst, lerr := statusV2(stableLeader)
			nst, nerr := statusV2(learner)
			return lerr == nil && nerr == nil && nst.AppliedIndex >= lst.LastIndex
		})
		promoted := false
		promoDeadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(promoDeadline) {
			resp, status, err := membershipPromoteHTTP(stableLeader, "full303-promote-n4", learnerID)
			if err != nil {
				t.Fatalf("step 7: promote %s: %v", learnerID, err)
			}
			if status == http.StatusOK && resp.Status == "committed" {
				promoted = true
				break
			}
			if status != http425TooEarly {
				t.Fatalf("step 7: promote %s: status=%d resp=%+v, want 200 or 425 only", learnerID, status, resp)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !promoted {
			t.Fatal("step 7: promote never succeeded within the deadline")
		}
		awaitCondition2(t, 15*time.Second, "cluster converges on 4 voters", func() bool {
			st, _, err := membershipStatusHTTP(stableLeader)
			return err == nil && len(st.Voters) == 4
		})

		var removeTargetID string
		for _, rn := range nodes {
			if rn.id != stableLeader.id {
				removeTargetID = rn.id
				break
			}
		}
		remResp2, remStatus2, remErr2 := membershipRemoveHTTP(stableLeader, "full303-remove-1", removeTargetID, 0)
		if remErr2 != nil || remStatus2 != http.StatusOK || remResp2.Status != "committed" {
			t.Fatalf("step 7: remove %s: resp=%+v status=%d err=%v", removeTargetID, remResp2, remStatus2, remErr2)
		}
		awaitCondition2(t, 15*time.Second, "cluster converges on 3 voters after removal", func() bool {
			st, _, err := membershipStatusHTTP(stableLeader)
			return err == nil && len(st.Voters) == 3
		})
		_ = allNodes
		t.Log("step 7: full-cluster restart, GC-state persistence, and a real add+promote+remove cycle all confirmed")

		// ==================== Step 8: report chronicledb_requestid_outcomes honestly ====================
		finalLeader := awaitLeaderV2(t, nodes, 10*time.Second)
		requestIDOutcomesAtEnd, _ := metricValue(t, finalLeader, "chronicledb_requestid_outcomes")
		t.Logf("step 8 (end): chronicledb_requestid_outcomes = %v (start was %v) — v0.6.0 makes NO globally-bounded-disk claim (§28.2, gate 14): this table grows with every distinct RequestID regardless of GC",
			requestIDOutcomesAtEnd, requestIDOutcomesAtStart)
		if requestIDOutcomesAtEnd < requestIDOutcomesAtStart {
			t.Errorf("step 8: chronicledb_requestid_outcomes decreased (%v -> %v) — this table must never shrink", requestIDOutcomesAtStart, requestIDOutcomesAtEnd)
		}
	})
}

// getHealth is a small local decode helper for /health's response
// shape (main.go's healthResponse), used by step 5's disk-pressure
// polling.
func getHealth(rn *realNode) (healthResponse, int, error) {
	resp, err := http.Get("http://" + rn.httpAddr + "/health")
	if err != nil {
		return healthResponse{}, 0, err
	}
	defer resp.Body.Close()
	var h healthResponse
	if decErr := json.NewDecoder(resp.Body).Decode(&h); decErr != nil {
		return healthResponse{}, resp.StatusCode, decErr
	}
	return h, resp.StatusCode, nil
}

// proposeWithReason is propose (main_test.go) but decoding the 503
// admission-rejection body shape (errors.go's admissionRejectedResponse)
// instead of proposeResponse, so the caller can inspect Reason.
func proposeWithReason(ctx context.Context, rn *realNode, requestID, key, value string) (admissionRejectedResponse, int, error) {
	body := fmt.Sprintf(`{"requestId":%q,"mutations":[{"key":%q,"value":%q}]}`, requestID, key, value)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+rn.httpAddr+"/propose", strings.NewReader(body))
	if err != nil {
		return admissionRejectedResponse{}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return admissionRejectedResponse{}, 0, err
	}
	defer resp.Body.Close()
	var out admissionRejectedResponse
	if decErr := json.NewDecoder(resp.Body).Decode(&out); decErr != nil {
		return admissionRejectedResponse{}, resp.StatusCode, decErr
	}
	return out, resp.StatusCode, nil
}
