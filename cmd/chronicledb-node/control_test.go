// This file unit-tests controlServer's Phase 9 observability endpoints
// (/metrics, /health) directly against an in-process node.Node — no
// build tag, so it runs on every `go test ./...` unlike this package's
// `integration`-tagged real-OS-process tests (main_test.go,
// chaos_test.go), mirroring internal/node's own fast in-process test
// style for a single-node cluster.
package main

import (
	"encoding/json"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

func freeControlTestAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func openSingleNodeForControlTest(t *testing.T) *node.Node {
	t.Helper()
	id := raft.NodeID("solo")
	n, err := node.Open(node.Config{
		ID:                         id,
		Peers:                      []raft.NodeID{id},
		DataDir:                    t.TempDir(),
		ListenAddr:                 freeControlTestAddr(t),
		ElectionTimeoutTicks:       3,
		ElectionTimeoutJitterTicks: 2,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("node.Open: %v", err)
	}
	t.Cleanup(n.Stop)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().Role == raft.Leader {
			return n
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("solo node never became leader")
	return nil
}

func TestControlServerMetricsExposesExpectedNames(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"chronicledb_raft_role",
		"chronicledb_raft_term",
		"chronicledb_raft_commit_index",
		"chronicledb_raft_applied_index",
		"chronicledb_raft_last_log_index",
		"chronicledb_raft_snapshot_index",
		"chronicledb_raft_elections_total",
		"chronicledb_raft_leader_changes_total",
		"chronicledb_proposals_total",
		"chronicledb_proposals_committed_total",
		"chronicledb_requestid_duplicates_total",
		"chronicledb_snapshots_created_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics output missing %q", want)
		}
	}
	// This solo node necessarily elected itself leader at least once.
	if !strings.Contains(body, "chronicledb_raft_leader_changes_total 1") {
		t.Errorf("/metrics output = %q, want chronicledb_raft_leader_changes_total 1", body)
	}
}

func TestControlServerHealthNeverClaimsQuorumForFollower(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/health", nil)
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding /health response: %v", err)
	}
	if !resp.Alive || !resp.NodeStarted || !resp.RaftInitialized || !resp.StorageOpened {
		t.Errorf("health = %+v, want all baseline booleans true", resp)
	}
	if resp.Role != "Leader" {
		t.Errorf("Role = %q, want Leader (this test's solo node always wins its own election)", resp.Role)
	}
	if !resp.Ready {
		t.Errorf("Ready = false on a healthy node, want true")
	}
	if resp.DiskPressure != "unsupported" && resp.DiskPressure != "normal" {
		t.Errorf("DiskPressure = %q on a healthy node with no thresholds configured, want \"unsupported\" or \"normal\"", resp.DiskPressure)
	}
	if !resp.StorageHealthy {
		t.Errorf("StorageHealthy = false on a healthy node, want true")
	}
}

// TestControlServerHealthReports503WhenStorageUnhealthy is §11.3's
// readiness rule: a node that is storage-unhealthy (§20.2) reports 503
// from /health while remaining alive — the Kubernetes-shaped
// live-but-not-ready distinction — and clears back to 200/ready the
// moment storage-unhealthy itself clears, with no restart.
func TestControlServerHealthReports503WhenStorageUnhealthy(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	getHealth := func() (int, healthResponse) {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
		var resp healthResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("decoding /health response: %v", err)
		}
		return rec.Code, resp
	}

	n.SetStorageUnhealthyForTest(true)
	deadline := time.Now().Add(2 * time.Second)
	var code int
	var resp healthResponse
	for time.Now().Before(deadline) {
		code, resp = getHealth()
		if !resp.Ready {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if resp.Ready {
		t.Fatalf("Ready never became false after SetStorageUnhealthyForTest(true)")
	}
	if code != 503 {
		t.Errorf("status = %d while not-ready, want 503", code)
	}
	if resp.StorageHealthy {
		t.Errorf("StorageHealthy = true while not-ready, want false")
	}
	if resp.Alive != true || resp.NodeStarted != true {
		t.Errorf("health = %+v: a not-ready node must still report Alive/NodeStarted true (live but not ready, never dead)", resp)
	}

	n.SetStorageUnhealthyForTest(false)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		code, resp = getHealth()
		if resp.Ready {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !resp.Ready || code != 200 {
		t.Fatalf("after clearing storage-unhealthy: code=%d ready=%v, want 200/true (recovery requires no restart)", code, resp.Ready)
	}
}
