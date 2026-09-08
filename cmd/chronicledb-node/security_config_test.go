// This file closes a test-coverage gap identified during acceptance
// review of commit f0b7754 (Security Foundation): validateSecurityFlags
// itself had no direct unit test, and the documented default-insecure
// startup contract (docs/enterprise-v1-plan.md §5's "secure-by-
// configuration, not yet secure-by-default" — v0.2.0 ships with
// -auth-mode=none/no-TLS as the flag default, matching v0.1.0 exactly,
// with a loud startup warning) had no test pinning it either. Neither
// gap affected plan compliance (the enforcement code itself was already
// correct and exercised indirectly by auth_test.go/
// security_integration_test.go) — this file adds the missing direct
// coverage without changing any production behavior.
package main

import (
	"bytes"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestValidateSecurityFlags_RejectionPaths covers every documented
// rejection path in validateSecurityFlags (docs/security.md §4):
// invalid -auth-mode, token/mtls modes missing their required
// companion flags, and partial (neither-all-nor-none) peer-TLS flag
// sets.
func TestValidateSecurityFlags_RejectionPaths(t *testing.T) {
	const (
		cert = "/tmp/cert.pem"
		key  = "/tmp/key.pem"
		ca   = "/tmp/ca.pem"
		tok  = "/tmp/tokens"
		rbac = "/tmp/rbac.json"
	)

	cases := []struct {
		name string
		f    securityFlags
	}{
		{
			name: "invalid auth-mode string",
			f:    securityFlags{authModeFlag: "bogus"},
		},
		{
			name: "token auth without token file",
			f:    securityFlags{authModeFlag: "token", rbacMappingFile: rbac},
		},
		{
			name: "token auth without RBAC mapping",
			f:    securityFlags{authModeFlag: "token", authTokenFile: tok},
		},
		{
			name: "token auth without token file or RBAC mapping",
			f:    securityFlags{authModeFlag: "token"},
		},
		{
			name: "mtls auth without client TLS cert/key",
			f:    securityFlags{authModeFlag: "mtls", tlsCAFile: ca, rbacMappingFile: rbac},
		},
		{
			name: "mtls auth without client TLS cert only (key present)",
			f:    securityFlags{authModeFlag: "mtls", tlsKeyFile: key, tlsCAFile: ca, rbacMappingFile: rbac},
		},
		{
			name: "mtls auth without client TLS key only (cert present)",
			f:    securityFlags{authModeFlag: "mtls", tlsCertFile: cert, tlsCAFile: ca, rbacMappingFile: rbac},
		},
		{
			name: "mtls auth without TLS CA",
			f:    securityFlags{authModeFlag: "mtls", tlsCertFile: cert, tlsKeyFile: key, rbacMappingFile: rbac},
		},
		{
			name: "mtls auth without RBAC mapping",
			f:    securityFlags{authModeFlag: "mtls", tlsCertFile: cert, tlsKeyFile: key, tlsCAFile: ca},
		},
		{
			name: "mtls auth with nothing configured",
			f:    securityFlags{authModeFlag: "mtls"},
		},
		{
			name: "partial peer-TLS: cert only",
			f:    securityFlags{authModeFlag: "none", peerTLSCertFile: cert},
		},
		{
			name: "partial peer-TLS: cert+key, no CA",
			f:    securityFlags{authModeFlag: "none", peerTLSCertFile: cert, peerTLSKeyFile: key},
		},
		{
			name: "partial peer-TLS: CA only",
			f:    securityFlags{authModeFlag: "none", peerTLSCAFile: ca},
		},
		{
			name: "partial peer-TLS: key+CA, no cert",
			f:    securityFlags{authModeFlag: "none", peerTLSKeyFile: key, peerTLSCAFile: ca},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mode, err := validateSecurityFlags(tc.f)
			if err == nil {
				t.Fatalf("validateSecurityFlags(%+v) = (%q, nil), want a rejection error", tc.f, mode)
			}
		})
	}
}

// TestValidateSecurityFlags_AcceptedCombinations is the positive
// counterpart to the rejection-path table: every one of these must be
// accepted, so the rejection test above is provably about the specific
// missing field, not an overly strict validator rejecting everything.
func TestValidateSecurityFlags_AcceptedCombinations(t *testing.T) {
	const (
		cert = "/tmp/cert.pem"
		key  = "/tmp/key.pem"
		ca   = "/tmp/ca.pem"
		tok  = "/tmp/tokens"
		rbac = "/tmp/rbac.json"
	)

	cases := []struct {
		name     string
		f        securityFlags
		wantMode authMode
	}{
		{
			name:     "default: auth-mode=none, nothing else set",
			f:        securityFlags{authModeFlag: "none"},
			wantMode: authModeNone,
		},
		{
			name:     "auth-mode=none with peer-TLS fully configured",
			f:        securityFlags{authModeFlag: "none", peerTLSCertFile: cert, peerTLSKeyFile: key, peerTLSCAFile: ca},
			wantMode: authModeNone,
		},
		{
			name:     "token auth fully configured",
			f:        securityFlags{authModeFlag: "token", authTokenFile: tok, rbacMappingFile: rbac},
			wantMode: authModeToken,
		},
		{
			name:     "mtls auth fully configured",
			f:        securityFlags{authModeFlag: "mtls", tlsCertFile: cert, tlsKeyFile: key, tlsCAFile: ca, rbacMappingFile: rbac},
			wantMode: authModeMTLS,
		},
		{
			name:     "token auth with peer-TLS and enable-fault also set",
			f:        securityFlags{authModeFlag: "token", authTokenFile: tok, rbacMappingFile: rbac, peerTLSCertFile: cert, peerTLSKeyFile: key, peerTLSCAFile: ca, enableFault: true},
			wantMode: authModeToken,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mode, err := validateSecurityFlags(tc.f)
			if err != nil {
				t.Fatalf("validateSecurityFlags(%+v): unexpected error: %v", tc.f, err)
			}
			if mode != tc.wantMode {
				t.Fatalf("validateSecurityFlags(%+v) mode = %q, want %q", tc.f, mode, tc.wantMode)
			}
		})
	}
}

// TestValidateSecurityFlags_DefaultFlagsAreInsecureAndValid pins the
// documented v0.2.0 contract (docs/enterprise-v1-plan.md §5
// "Compatibility implications", docs/security.md §1): a securityFlags
// value built from exactly the CLI flag package's own defaults
// (auth-mode "none", every other flag "") validates successfully, and
// the resulting mode/TLS-file state is the same "insecure" condition
// main.go computes to decide whether to warn — reproduced here
// structurally from the validated, public outputs (mode, tlsCertFile)
// rather than by depending on main.go's unexported local variable,
// which cannot be referenced across a `go test` build directly.
func TestValidateSecurityFlags_DefaultFlagsAreInsecureAndValid(t *testing.T) {
	defaults := securityFlags{
		// Mirrors flag.String("auth-mode", string(authModeNone), ...)
		// and every other Security Foundation flag's "" default in
		// main.go's flag declarations exactly.
		authModeFlag: string(authModeNone),
	}
	mode, err := validateSecurityFlags(defaults)
	if err != nil {
		t.Fatalf("default flags must always validate cleanly: %v", err)
	}
	if mode != authModeNone {
		t.Fatalf("default auth mode = %q, want %q", mode, authModeNone)
	}
	// This is exactly main.go's own "insecure" condition
	// (tlsCertFile == "" || authModeValue == authModeNone) evaluated
	// against the default flag set — true, as documented.
	insecure := defaults.tlsCertFile == "" || mode == authModeNone
	if !insecure {
		t.Fatal("default flag set must compute as insecure (no TLS, no auth) per the documented v0.2.0 default")
	}
	if defaults.enableFault {
		t.Fatal("default securityFlags.enableFault must be false")
	}
}

// TestLogInsecureWarning_EmitsRecognizableWarning proves the "loud,
// repeated warning" docs/enterprise-v1-plan.md §5 requires is actually
// emitted — deterministically, via a single direct call against a
// buffer-backed logger, with no sleep or timing dependency (the
// periodic re-warning in warnInsecurePeriodically is a 30s-ticker
// wrapper around this same function and is not independently
// re-tested here, since asserting its cadence would require either a
// brittle real-time wait or a production-code change to make the
// interval injectable — out of scope for a test-only change).
func TestLogInsecureWarning_EmitsRecognizableWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	logInsecureWarning(logger)

	out := buf.String()
	for _, want := range []string{"WARNING", "TLS", "auth", "docs/security.md"} {
		if !strings.Contains(out, want) {
			t.Errorf("insecure warning output missing %q; got:\n%s", want, out)
		}
	}
}

// TestDefaultInsecureStartup_PlaintextModeFunctional proves the other
// half of the v0.2.0 compatibility contract: not just that the default
// is insecure, but that it is still fully FUNCTIONAL — a client can
// call /status and /propose with no Authorization header at all and
// succeed, exactly as in v0.1.0 (newSecurity(f, authModeNone, ...)
// returns (nil, nil), and security.wrap is a documented no-op when sec
// is nil). Also confirms /fault stays structurally unregistered under
// this same default configuration, tying the earlier
// TestFaultEndpoint_UnregisteredByDefault property specifically to the
// validated *default* securityFlags value rather than a hand-built
// enableFault=false literal.
func TestDefaultInsecureStartup_PlaintextModeFunctional(t *testing.T) {
	defaults := securityFlags{authModeFlag: string(authModeNone)}
	mode, err := validateSecurityFlags(defaults)
	if err != nil {
		t.Fatalf("validateSecurityFlags(defaults): %v", err)
	}
	sec, err := newSecurity(defaults, mode, nil)
	if err != nil {
		t.Fatalf("newSecurity(defaults): %v", err)
	}
	if sec != nil {
		t.Fatalf("newSecurity under the default (auth-mode=none) config must return a nil *security, got %+v", sec)
	}

	n := openSingleNodeForControlTest(t)
	srv := newControlServer(n, nil, sec, nil, defaults.enableFault, "test-cluster")

	// GET /status with no credential at all must succeed.
	statusReq := httptest.NewRequest("GET", "/status", nil)
	statusRec := httptest.NewRecorder()
	srv.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != 200 {
		t.Fatalf("default-mode GET /status without credentials: status = %d, want 200 (plaintext mode must remain fully functional)", statusRec.Code)
	}

	// POST /propose with no credential at all must succeed and commit —
	// the actual v0.1.0-era client-facing write path, unauthenticated.
	body := strings.NewReader(`{"requestId":"default-mode-r1","txnId":1,"mutations":[{"key":"k","value":"v"}]}`)
	proposeReq := httptest.NewRequest("POST", "/propose", body)
	proposeRec := httptest.NewRecorder()
	srv.ServeHTTP(proposeRec, proposeReq)
	if proposeRec.Code != 200 {
		t.Fatalf("default-mode POST /propose without credentials: status = %d body = %q, want 200", proposeRec.Code, proposeRec.Body.String())
	}
	if !strings.Contains(proposeRec.Body.String(), `"committed"`) {
		t.Fatalf("default-mode POST /propose response = %q, want a committed outcome", proposeRec.Body.String())
	}

	// /fault must still be structurally absent under this exact
	// default configuration — proven via mux.Handler, not merely a
	// status-code check, matching TestFaultEndpoint_UnregisteredByDefault's
	// discipline (docs/enterprise-v1-plan.md §5's FAULT SURFACE OFF BY
	// DEFAULT invariant).
	faultReq := httptest.NewRequest("POST", "/fault?action=block&peer=ghost", nil)
	_, pattern := srv.mux.Handler(faultReq)
	if pattern != "" {
		t.Fatalf("default configuration must never register /fault, matched pattern %q", pattern)
	}
	faultRec := httptest.NewRecorder()
	srv.ServeHTTP(faultRec, faultReq)
	if faultRec.Code != 404 {
		t.Fatalf("default-mode /fault: status = %d, want 404 (structurally unregistered)", faultRec.Code)
	}
}
