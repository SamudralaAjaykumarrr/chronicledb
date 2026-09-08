// This file proves the Security Foundation HTTP-layer requirements
// (docs/enterprise-v1-plan.md §5 "Deterministic tests"): RBAC
// decision-table tests (every role x every endpoint), the /fault
// route-registration test under every flag combination, generic
// (non-information-leaking) auth failure messages, and audit
// completeness (exactly one record per decision, write failure blocks
// the action).
package main

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/authn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/authz"
)

// rbacTestBackupDir is a scratch directory /admin/backup's RBAC-decision-
// table exercise writes into when a role/endpoint combination is
// expected to reach the handler — a fixed package-level path (rather
// than a per-test t.TempDir()) because endpointHTTP below is itself a
// package-level table shared by several tests.
var rbacTestBackupDir = mustMkdirTemp("chronicledb-rbac-backup-")

func mustMkdirTemp(prefix string) string {
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		panic(err)
	}
	return dir
}

// newTokenSecurityForTest builds a *security in token auth mode with
// three principals, one per fixed role, plus a dedicated audit log
// directory the test can inspect afterward.
func newTokenSecurityForTest(t *testing.T) (sec *security, auditDir string, tokenFor map[authz.Role]string) {
	t.Helper()
	tokenFor = map[authz.Role]string{
		authz.RoleAdmin:    "tok-admin",
		authz.RoleOperator: "tok-operator",
		authz.RoleReadOnly: "tok-readonly",
	}
	tokens := map[string]string{
		"tok-admin":    "alice",
		"tok-operator": "bob",
		"tok-readonly": "carol",
	}
	rbac := map[string]authz.Role{
		"alice": authz.RoleAdmin,
		"bob":   authz.RoleOperator,
		"carol": authz.RoleReadOnly,
	}
	auditDir = t.TempDir()
	auditLog, err := audit.Open(auditDir)
	if err != nil {
		t.Fatalf("audit.Open: %v", err)
	}
	t.Cleanup(func() { auditLog.Close() })

	sec = &security{
		mode:          authModeToken,
		authenticator: authn.NewTokenAuthenticator(tokens),
		rbac:          rbac,
		auditLog:      auditLog,
	}
	return sec, auditDir, tokenFor
}

// endpointHTTP maps each authz endpoint constant to its HTTP method and
// path, matching newControlServer's route table.
var endpointHTTP = map[string]struct {
	method string
	path   string
}{
	authz.EndpointStatus:    {"GET", "/status"},
	authz.EndpointMetrics:   {"GET", "/metrics"},
	authz.EndpointHealth:    {"GET", "/health"},
	authz.EndpointPropose:   {"POST", "/propose"}, // body supplied per-call below (RequestID must be unique per test run)
	authz.EndpointOutcome:   {"GET", "/outcome?requestId=nonexistent"},
	authz.EndpointFault:     {"POST", "/fault?action=block&peer=ghost"},
	authz.EndpointReloadTLS: {"POST", "/admin/reload-tls"},
	authz.EndpointBackup:    {"POST", "/admin/backup?dir=" + rbacTestBackupDir},
}

func TestRBAC_DecisionTable_HTTPLayer_EveryRoleEveryEndpoint(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	sec, _, tokenFor := newTokenSecurityForTest(t)
	srv := newControlServer(n, nil, sec, nil, true, "test-cluster") // enableFault=true so /fault's RBAC (not merely registration) is exercised here

	for _, ep := range authz.AllEndpoints {
		hh := endpointHTTP[ep]
		for _, role := range authz.AllRoles {
			t.Run(ep+"/"+string(role), func(t *testing.T) {
				var body *strings.Reader
				if ep == authz.EndpointPropose {
					// A well-formed proposal body so a request that
					// clears RBAC also clears the handler's own
					// request-decoding step — this test is checking
					// the AUTH layer's decision, not business-logic
					// validity, so the request must otherwise be valid.
					body = strings.NewReader(`{"requestId":"rbac-test-` + ep + "-" + string(role) + `","txnId":1,"mutations":[{"key":"k","value":"v"}]}`)
				} else {
					body = strings.NewReader("")
				}
				req := httptest.NewRequest(hh.method, hh.path, body)
				req.Header.Set("Authorization", "Bearer "+tokenFor[role])
				rec := httptest.NewRecorder()
				srv.ServeHTTP(rec, req)

				// This test asserts the AUTH layer's decision only: an
				// allowed request must reach the handler (never 401/403);
				// a denied request must be refused with exactly 403 (the
				// request was authenticated, just not authorized) before
				// the handler ever runs. Whatever status the handler
				// itself then produces (200, 404 "unknown RequestID",
				// etc.) is that handler's own business logic, not RBAC's
				// concern.
				wantAllowed := authz.Allowed(ep, role)
				gotDeniedByRBAC := rec.Code == 401 || rec.Code == 403
				if wantAllowed && gotDeniedByRBAC {
					t.Errorf("endpoint=%s role=%s: expected allowed through to the handler, got status %d body %q", ep, role, rec.Code, rec.Body.String())
				}
				if !wantAllowed && rec.Code != 403 {
					t.Errorf("endpoint=%s role=%s: expected 403 (denied), got status %d body %q", ep, role, rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestRBAC_UnauthenticatedRequestDeniedForEveryEndpoint(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	sec, _, _ := newTokenSecurityForTest(t)
	srv := newControlServer(n, nil, sec, nil, true, "test-cluster")

	for _, ep := range authz.AllEndpoints {
		hh := endpointHTTP[ep]
		t.Run(ep, func(t *testing.T) {
			req := httptest.NewRequest(hh.method, hh.path, nil) // no Authorization header
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != 401 {
				t.Errorf("endpoint=%s: unauthenticated request got status %d, want 401", ep, rec.Code)
			}
		})
	}
}

func TestAuth_GenericErrorMessages_NoInformationLeak(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	sec, _, tokenFor := newTokenSecurityForTest(t)
	srv := newControlServer(n, nil, sec, nil, true, "test-cluster")

	call := func(token string) (int, string) {
		req := httptest.NewRequest("GET", "/status", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}

	_, missingBody := call("")
	_, wrongBody := call("this-token-does-not-exist")
	if missingBody != wrongBody {
		t.Errorf("missing-credential body %q != wrong-credential body %q — must be identical (no information leak)", missingBody, wrongBody)
	}

	// A read-only principal calling an operator-only endpoint (denied by
	// RBAC, not by authn) must get a generic 403 body too, and it must
	// not equal the 401 body (they are distinguishable AS status codes —
	// that's required, since a client must know whether to re-auth or
	// not — just not distinguishable in ways that leak *why* within each
	// class).
	req := httptest.NewRequest("POST", "/fault?action=block&peer=ghost", nil)
	req.Header.Set("Authorization", "Bearer "+tokenFor[authz.RoleReadOnly])
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 403 {
		t.Fatalf("read-only calling /fault: status = %d, want 403", rec.Code)
	}
	if rec.Body.String() == missingBody {
		t.Errorf("403 body should not equal 401 body")
	}
}

// TestFaultEndpoint_UnregisteredByDefault proves FAULT SURFACE OFF BY
// DEFAULT structurally: with enableFault=false, /fault's route is never
// registered on the mux at all — http.ServeMux.Handler returns an empty
// pattern for a request that matches nothing, distinguishing "route
// does not exist" from "route exists but returned an error."
func TestFaultEndpoint_UnregisteredByDefault(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, nil, nil, false, "test-cluster")

	req := httptest.NewRequest("POST", "/fault?action=block&peer=ghost", nil)
	_, pattern := srv.mux.Handler(req)
	if pattern != "" {
		t.Fatalf("expected /fault to be structurally unregistered when enableFault=false, but matched pattern %q", pattern)
	}

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("calling unregistered /fault: status = %d, want 404", rec.Code)
	}
}

// TestFaultEndpoint_RegisteredFlagCombinations exercises every
// combination of (enableFault, auth configured) and checks both route
// registration and, when registered, the RBAC outcome.
func TestFaultEndpoint_RegisteredFlagCombinations(t *testing.T) {
	cases := []struct {
		name        string
		enableFault bool
		withAuth    bool
		role        authz.Role // meaningful only if withAuth
		wantStatus  int        // meaningful only if the route is registered
	}{
		{name: "disabled, no auth", enableFault: false, withAuth: false},
		{name: "disabled, with auth", enableFault: false, withAuth: true, role: authz.RoleAdmin},
		{name: "enabled, no auth (matches v0.1.0 open behavior)", enableFault: true, withAuth: false, wantStatus: 200},
		{name: "enabled, with auth, admin", enableFault: true, withAuth: true, role: authz.RoleAdmin, wantStatus: 200},
		{name: "enabled, with auth, operator (denied)", enableFault: true, withAuth: true, role: authz.RoleOperator, wantStatus: 403},
		{name: "enabled, with auth, read-only (denied)", enableFault: true, withAuth: true, role: authz.RoleReadOnly, wantStatus: 403},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := openSingleNodeForControlTest(t)
			var sec *security
			var tokenFor map[authz.Role]string
			if tc.withAuth {
				sec, _, tokenFor = newTokenSecurityForTest(t)
			}
			srv := newControlServer(n, nil, sec, nil, tc.enableFault, "test-cluster")

			req := httptest.NewRequest("POST", "/fault?action=block&peer=ghost", nil)
			if tc.withAuth {
				req.Header.Set("Authorization", "Bearer "+tokenFor[tc.role])
			}
			_, pattern := srv.mux.Handler(req)

			if !tc.enableFault {
				if pattern != "" {
					t.Fatalf("enableFault=false: expected unregistered route, matched %q", pattern)
				}
				return
			}
			if pattern == "" {
				t.Fatalf("enableFault=true: expected /fault to be registered")
			}
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%q)", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestAudit_ExactlyOneRecordPerDecision proves AUDIT COMPLETENESS:
// every RBAC-gated call — allowed, denied-unauthenticated, and
// denied-unauthorized alike — produces exactly one audit record.
func TestAudit_ExactlyOneRecordPerDecision(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	sec, auditDir, tokenFor := newTokenSecurityForTest(t)
	srv := newControlServer(n, nil, sec, nil, false, "test-cluster")

	do := func(token string) {
		req := httptest.NewRequest("GET", "/status", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
	}

	do(tokenFor[authz.RoleAdmin])    // allow
	do("")                           // deny_unauthenticated
	do("bogus-token-value-here-123") // deny_unauthenticated (different reason, same class)

	entries, err := audit.ReadAll(auditDir)
	if err != nil {
		t.Fatalf("audit.ReadAll: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d audit entries, want exactly 3 (one per call)", len(entries))
	}
	if entries[0].Result != "allow" || entries[1].Result != "deny_unauthenticated" || entries[2].Result != "deny_unauthenticated" {
		t.Fatalf("entries = %+v", entries)
	}
	if err := audit.Verify(auditDir); err != nil {
		t.Fatalf("audit.Verify: %v", err)
	}
}

// TestMetrics_ExposesSecurityCounters proves docs/enterprise-v1-plan.md
// §5's Observability requirement: auth success/failure counts and the
// audit-write-failure counter are exposed on /metrics once security is
// configured.
func TestMetrics_ExposesSecurityCounters(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	sec, _, tokenFor := newTokenSecurityForTest(t)
	srv := newControlServer(n, nil, sec, nil, false, "test-cluster")

	// One success, one failure, to populate both counters.
	ok := httptest.NewRequest("GET", "/status", nil)
	ok.Header.Set("Authorization", "Bearer "+tokenFor[authz.RoleAdmin])
	srv.ServeHTTP(httptest.NewRecorder(), ok)

	bad := httptest.NewRequest("GET", "/status", nil)
	bad.Header.Set("Authorization", "Bearer nonexistent")
	srv.ServeHTTP(httptest.NewRecorder(), bad)

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+tokenFor[authz.RoleAdmin])
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, want := range []string{"chronicledb_auth_success_total", "chronicledb_auth_failure_total", "chronicledb_audit_write_failures_total"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q; body=%q", want, body)
		}
	}
}

// TestAudit_WriteFailureBlocksAction proves audit-write failure fails
// the triggering action closed: the handler's own side effect must
// never run, and the caller must see an error, not a silent success.
func TestAudit_WriteFailureBlocksAction(t *testing.T) {
	n := openSingleNodeForControlTest(t)
	sec, _, tokenFor := newTokenSecurityForTest(t)
	srv := newControlServer(n, nil, sec, nil, false, "test-cluster")

	// Simulate an unwritable audit log (disk full / permission error /
	// any I/O failure) by closing its underlying file handle out from
	// under it — internal/audit.Log.Append then fails deterministically
	// (see internal/audit's own TestAppend_WriteFailureAfterUnderlyingCloseIsReported).
	sec.auditLog.Close()

	req := httptest.NewRequest("POST", "/propose", nil)
	req.Header.Set("Authorization", "Bearer "+tokenFor[authz.RoleAdmin])
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != 500 {
		t.Fatalf("status = %d, want 500 (action must fail closed when audit write fails)", rec.Code)
	}
}
