package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleMembershipAdd_RefusedBeforeGeneration2 pins §8.2's leader
// -side gate and its HTTP mapping (412, reason "generation-too-low") —
// a single-node cluster never finalizes in this test, so generation
// stays at 0.
func TestHandleMembershipAdd_RefusedBeforeGeneration2(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	body := `{"requestId":"add1","nodeId":"n2","address":"127.0.0.1:1"}`
	req := httptest.NewRequest("POST", "/admin/membership/add", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != 412 {
		t.Fatalf("status = %d, want 412", rec.Code)
	}
	var resp membershipMutateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Reason != "generation-too-low" {
		t.Fatalf("reason = %q, want %q", resp.Reason, "generation-too-low")
	}
}

// TestHandleMembershipAdd_RejectsMalformedAddress pins §9's "an
// operator typo produces an immediate 400, never a proposed-then-
// rejected log entry."
func TestHandleMembershipAdd_RejectsMalformedAddress(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	body := `{"requestId":"add1","nodeId":"n2","address":"not-a-host-port"}`
	req := httptest.NewRequest("POST", "/admin/membership/add", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != 400 {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// TestHandleMembershipStatus_ReflectsSingleNodeConfiguration is a
// read-only smoke test of the JSON wiring end to end.
func TestHandleMembershipStatus_ReflectsSingleNodeConfiguration(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	req := httptest.NewRequest("GET", "/admin/membership/status", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var resp membershipStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if len(resp.Voters) != 1 || resp.Voters[0].ID != "solo" {
		t.Fatalf("Voters = %+v, want exactly [solo]", resp.Voters)
	}
	if len(resp.Learners) != 0 {
		t.Fatalf("Learners = %+v, want none", resp.Learners)
	}
}

// TestHandleMembershipRemove_RejectsGET pins "POST required" for every
// mutating endpoint.
func TestHandleMembershipRemove_RejectsGET(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	req := httptest.NewRequest("GET", "/admin/membership/remove", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 405 {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
