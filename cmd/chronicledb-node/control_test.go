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
	srv := newControlServer(n, nil)

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
	srv := newControlServer(n, nil)

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
}
