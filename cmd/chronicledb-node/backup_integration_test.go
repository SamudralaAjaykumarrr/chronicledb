//go:build integration

// This file is the real-OS-process evidence docs/enterprise-v1-plan.md
// §6 requires for Backup / Disaster Recovery / PITR, extending
// main_test.go's realNode harness (genuine processes, genuine TCP,
// genuine persistent data directories, a real SIGKILL): a real
// /admin/backup HTTP call against a live three-node cluster under
// concurrent write load, followed by the destructive drill explicitly
// named in the plan — kill the source cluster entirely, delete every
// node's data directory, and restore a brand-new cluster from the
// backup alone via the real -restore-from CLI flag on real process
// invocations, then prove it is a genuinely live, functioning cluster
// (a further write commits normally).
//
// Run explicitly:
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestRealBackup -v
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func httpBackup(t *testing.T, rn *realNode, dir string, continuous bool) backupResponse {
	t.Helper()
	url := fmt.Sprintf("http://%s/admin/backup?dir=%s&continuous=%v", rn.httpAddr, dir, continuous)
	resp, err := http.Post(url, "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("POST /admin/backup: %v", err)
	}
	defer resp.Body.Close()
	var br backupResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		t.Fatalf("decoding /admin/backup response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/admin/backup returned %d: %s", resp.StatusCode, br.Error)
	}
	return br
}

// TestRealBackup_LiveClusterUnderConcurrentWriteLoad proves a real
// /admin/backup HTTP call against a real, live three-node process
// cluster succeeds while proposals keep flowing against it, and that
// ongoing replication is unaffected afterward.
func TestRealBackup_LiveClusterUnderConcurrentWriteLoad(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			propose(leader, fmt.Sprintf("load-%d", i), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(200 * time.Millisecond)

	backupDir := filepath.Join(t.TempDir(), "backup")
	br := httpBackup(t, leader, backupDir, true)

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	if br.Manifest.WALUntilIndex == 0 {
		t.Fatalf("backup captured no state: %+v", br.Manifest)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "manifest.json")); err != nil {
		t.Fatalf("backup manifest not on disk: %v", err)
	}

	// The cluster must still be healthy and accepting writes after a
	// backup taken under load.
	resp, status, err := propose(leader, "after-backup", "k-after", "v-after")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("propose after backup: resp=%+v status=%d err=%v", resp, status, err)
	}
}

// TestRealBackup_DestructiveDisasterRecoveryDrill is the real-OS-process
// destructive disaster-recovery proof: back up a live three-node
// cluster's leader, SIGKILL and delete every node's data directory
// (simulating total loss), restore three brand-new data directories
// from the backup alone via real `-restore-from`/-force-overwrite`
// process invocations, start a brand-new cluster of real processes
// against them, and confirm every pre-loss commit survived and the
// restored cluster accepts new writes normally.
func TestRealBackup_DestructiveDisasterRecoveryDrill(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	const n = 10
	var lastCommitSeq uint64
	for i := 1; i <= n; i++ {
		reqID := fmt.Sprintf("drill-%d", i)
		resp, status, err := propose(leader, reqID, fmt.Sprintf("drill-k%d", i), fmt.Sprintf("v%d", i))
		if err != nil || status != http.StatusOK || resp.Status != "committed" {
			t.Fatalf("seed propose %d: resp=%+v status=%d err=%v", i, resp, status, err)
		}
		lastCommitSeq = resp.CommitSeq
	}
	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "drill-10", 10*time.Second)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	br := httpBackup(t, leader, backupDir, true)
	if br.Manifest.WALUntilIndex < lastCommitSeq {
		t.Fatalf("backup captured up to %d, want at least %d", br.Manifest.WALUntilIndex, lastCommitSeq)
	}

	// TOTAL LOSS: real SIGKILL every node, then delete every data
	// directory outright.
	for _, rn := range nodes {
		rn.crash()
	}
	for _, rn := range nodes {
		if err := os.RemoveAll(rn.dataDir); err != nil {
			t.Fatalf("simulating total loss of %s's data directory: %v", rn.id, err)
		}
	}

	// Restore three brand-new data directories from the backup alone,
	// each via a real process invocation of the actual CLI flag
	// (-restore-from), then start a brand-new cluster of real processes
	// against them — never touching the original (deleted) directories.
	newDirs := make(map[string]string, len(nodes))
	for _, rn := range nodes {
		newDirs[rn.id] = filepath.Join(t.TempDir(), rn.id)
	}
	ids := make([]string, len(nodes))
	raftAddrs := make(map[string]string, len(nodes))
	for i, rn := range nodes {
		ids[i] = rn.id
		raftAddrs[rn.id] = rn.raftAddr
	}
	clusterFlag := strings.Join(ids, ",")

	httpPorts := freePorts(t, len(nodes))
	restored := make([]*realNode, len(nodes))
	for i, rn := range nodes {
		var peerParts []string
		for _, other := range ids {
			if other != rn.id {
				peerParts = append(peerParts, other+"="+raftAddrs[other])
			}
		}
		restored[i] = &realNode{
			id:       rn.id,
			raftAddr: rn.raftAddr,
			httpAddr: httpPorts[i],
			dataDir:  newDirs[rn.id],
			args: []string{
				"-id=" + rn.id,
				"-listen=" + raftAddrs[rn.id],
				"-cluster=" + clusterFlag,
				"-peers=" + strings.Join(peerParts, ","),
				"-restore-from=" + backupDir,
			},
		}
	}
	for _, rn := range restored {
		startRealNode(t, bin, rn)
	}
	t.Cleanup(func() {
		for _, rn := range restored {
			stopRealNode(rn)
		}
	})

	newLeader := awaitLeader(t, restored, 15*time.Second)
	for i := 1; i <= n; i++ {
		awaitOutcomeCommitted(t, newLeader, fmt.Sprintf("drill-%d", i), 15*time.Second)
	}

	// Genuinely live, not merely an inert restored directory: a fresh
	// write after disaster recovery commits normally.
	resp, status, err := propose(newLeader, "after-recovery", "after-key", "after-val")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("post-recovery propose: resp=%+v status=%d err=%v", resp, status, err)
	}

	// Criterion: the restored cluster itself survives a further real
	// restart/recovery cycle — a restored data directory must be a
	// fully valid, durable directory that node.Open's ordinary recovery
	// path can reopen after a real SIGKILL, indistinguishable from one
	// that grew organically, not merely openable the one time Restore
	// itself produced it.
	var victim *realNode
	for _, rn := range restored {
		if rn.id != newLeader.id {
			victim = rn
			break
		}
	}
	victim.crash()
	// A real restart after the initial disaster recovery does not
	// re-pass -restore-from: the data directory is now this node's own
	// ordinary, already-populated directory, and node.Open's normal
	// recovery path is what must reopen it. Re-passing -restore-from
	// here would (correctly) be refused by DESTRUCTIVE RESTORE
	// ISOLATION, since the directory is no longer clean/empty — that is
	// not what this criterion is testing.
	restartArgs := make([]string, 0, len(victim.args))
	for _, a := range victim.args {
		if !strings.HasPrefix(a, "-restore-from=") {
			restartArgs = append(restartArgs, a)
		}
	}
	victim.args = restartArgs
	victim.restart(t, bin)
	deadline := time.Now().Add(15 * time.Second)
	caught := false
	for time.Now().Before(deadline) {
		st, err := victim.status()
		if err == nil && st.AppliedIndex >= uint64(n)+1 {
			caught = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !caught {
		t.Fatalf("restarted restored node %s never caught back up after a real SIGKILL+restart post-recovery", victim.id)
	}

	// The restored-then-restarted cluster still accepts new writes.
	postRestartLeader := awaitLeader(t, restored, 15*time.Second)
	resp2, status2, err2 := propose(postRestartLeader, "after-restart-of-restored", "after-restart-key", "after-restart-val")
	if err2 != nil || status2 != http.StatusOK || resp2.Status != "committed" {
		t.Fatalf("propose after restarting a restored node: resp=%+v status=%d err=%v", resp2, status2, err2)
	}
}
