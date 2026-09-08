// This file wires ChronicleDB's Security Foundation
// (docs/enterprise-v1-plan.md §5) into cmd/chronicledb-node's HTTP
// control plane: the middleware chain (TLS termination -> authn ->
// authz -> audit -> handler), conditional /fault registration, and the
// /admin/reload-tls endpoint.
package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/authn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/authz"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/identity"
)

// authMode is the -auth-mode flag's value (docs/enterprise-v1-plan.md
// §5 "Formats/APIs affected"): "none" (default, matches v0.1.0 exactly),
// "token" (static bearer token), or "mtls" (client-certificate
// identity).
type authMode string

const (
	authModeNone  authMode = "none"
	authModeToken authMode = "token"
	authModeMTLS  authMode = "mtls"
)

// securityFlags collects every Security Foundation CLI flag
// (docs/enterprise-v1-plan.md §5 "Formats/APIs affected") in one place,
// separate from cmd/chronicledb-node/main.go's raft/storage flags.
type securityFlags struct {
	tlsCertFile     string
	tlsKeyFile      string
	tlsCAFile       string
	peerTLSCertFile string
	peerTLSKeyFile  string
	peerTLSCAFile   string
	authModeFlag    string
	authTokenFile   string
	rbacMappingFile string
	auditLogDir     string
	enableFault     bool
}

// security holds everything the HTTP control plane needs to authorize
// and audit a request once flags are validated and loaded. A nil
// *security (or one with mode == authModeNone) means "insecure,
// v0.1.0-compatible mode": wrap returns handlers unmodified, and no
// audit record is ever written — see docs/enterprise-v1-plan.md §5
// "Compatibility implications": "auth-mode none... matching v0.1.0
// behavior exactly."
type security struct {
	mode          authMode
	authenticator authn.Authenticator
	rbac          map[string]authz.Role
	auditLog      *audit.Log
	logger        *log.Logger

	// SecurityMetrics (docs/enterprise-v1-plan.md §5 Observability:
	// "auth success/failure counts... audit-write-failure counter", no
	// credential value in any label). Read via /metrics; never a
	// correctness dependency.
	authSuccessTotal        atomic.Int64
	authFailureTotal        atomic.Int64
	auditWriteFailuresTotal atomic.Int64
}

func validateSecurityFlags(f securityFlags) (authMode, error) {
	mode := authMode(f.authModeFlag)
	switch mode {
	case authModeNone, authModeToken, authModeMTLS:
	default:
		return "", fmt.Errorf("invalid -auth-mode %q (must be one of %q, %q, %q)", f.authModeFlag, authModeNone, authModeToken, authModeMTLS)
	}

	peerTLSCount := countNonEmpty(f.peerTLSCertFile, f.peerTLSKeyFile, f.peerTLSCAFile)
	if peerTLSCount != 0 && peerTLSCount != 3 {
		return "", fmt.Errorf("-peer-tls-cert, -peer-tls-key, -peer-tls-ca must all be set together or all left empty")
	}

	if mode == authModeToken {
		if f.authTokenFile == "" {
			return "", fmt.Errorf("-auth-mode=token requires -auth-token-file")
		}
		if f.rbacMappingFile == "" {
			return "", fmt.Errorf("-auth-mode=token requires -rbac-mapping-file")
		}
	}
	if mode == authModeMTLS {
		if f.tlsCertFile == "" || f.tlsKeyFile == "" {
			return "", fmt.Errorf("-auth-mode=mtls requires client TLS to be enabled (-tls-cert/-tls-key)")
		}
		if f.tlsCAFile == "" {
			return "", fmt.Errorf("-auth-mode=mtls requires -tls-ca (to verify presented client certificates)")
		}
		if f.rbacMappingFile == "" {
			return "", fmt.Errorf("-auth-mode=mtls requires -rbac-mapping-file")
		}
	}
	if f.enableFault && mode == authModeNone {
		// Not an error — mirrors v0.1.0's always-open /fault when the
		// operator has not opted into auth at all — but startup already
		// prints a loud warning about running without auth, and this is
		// an additional, specific reason to.
	}
	return mode, nil
}

func countNonEmpty(vals ...string) int {
	n := 0
	for _, v := range vals {
		if v != "" {
			n++
		}
	}
	return n
}

// newSecurity builds a *security from validated flags. mode ==
// authModeNone returns (nil, nil): the caller wires routes with no
// middleware at all, byte-for-byte the same as v0.1.0.
func newSecurity(f securityFlags, mode authMode, logger *log.Logger) (*security, error) {
	if mode == authModeNone {
		return nil, nil
	}

	var authenticator authn.Authenticator
	switch mode {
	case authModeToken:
		tokens, err := authz.LoadTokenFile(f.authTokenFile)
		if err != nil {
			return nil, err
		}
		authenticator = authn.NewTokenAuthenticator(tokens)
	case authModeMTLS:
		authenticator = authn.NewMTLSAuthenticator()
	}

	rbac, err := authz.LoadMappingFile(f.rbacMappingFile)
	if err != nil {
		return nil, err
	}

	auditDir := f.auditLogDir
	auditLog, err := audit.Open(auditDir)
	if err != nil {
		return nil, fmt.Errorf("opening audit log at %s: %w", auditDir, err)
	}

	return &security{mode: mode, authenticator: authenticator, rbac: rbac, auditLog: auditLog, logger: logger}, nil
}

func (s *security) Close() error {
	if s == nil || s.auditLog == nil {
		return nil
	}
	return s.auditLog.Close()
}

// genericUnauthenticatedMessage/genericUnauthorizedMessage are the only
// two messages ever returned for an authn/authz failure
// (docs/enterprise-v1-plan.md §5 "Failure semantics": "a generic
// message (no information leak distinguishing 'wrong token' from
// 'unknown user' from 'disabled account')").
const (
	genericUnauthenticatedMessage = "unauthorized"
	genericUnauthorizedMessage    = "forbidden"
)

// wrap implements the middleware chain "TLS termination -> authn ->
// authz -> audit -> handler" for one logical administrative endpoint.
// TLS termination itself happens earlier, at the net/http server level
// (main.go); wrap starts at authn.
//
// AUDIT COMPLETENESS (docs/enterprise-v1-plan.md §5): exactly one audit
// record is written per call, for every outcome (allowed, denied
// unauthenticated, denied unauthorized) — written BEFORE next runs, so
// an audit-write failure blocks the action outright (fails closed) even
// if authn/authz would otherwise have allowed it.
//
// NO UNAUTHENTICATED ADMIN ACTION: next is never invoked before authn
// and authz have both succeeded and the audit record for that success
// has itself been durably written.
func (s *security) wrap(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	if s == nil || s.mode == authModeNone {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		principal, authErr := s.authenticator.Authenticate(r)
		if authErr != nil {
			s.authFailureTotal.Add(1)
		} else {
			s.authSuccessTotal.Add(1)
		}

		var (
			result string
			role   authz.Role
		)
		switch {
		case authErr != nil:
			result = "deny_unauthenticated"
		default:
			var ok bool
			role, ok = s.rbac[principal.Name]
			if !ok || !authz.Allowed(endpoint, role) {
				result = "deny_unauthorized"
			} else {
				result = "allow"
			}
		}

		entry := audit.Entry{
			Timestamp: time.Now().UnixNano(),
			Principal: principal.Name,
			Role:      string(role),
			Action:    endpoint,
			Endpoint:  r.URL.Path,
			Result:    result,
			Detail:    r.URL.RawQuery,
		}
		if err := s.auditLog.Append(entry); err != nil {
			// AUDIT COMPLETENESS: a write failure blocks the action —
			// even one that authn/authz would have allowed — rather
			// than silently proceeding without a record.
			s.auditWriteFailuresTotal.Add(1)
			if s.logger != nil {
				s.logger.Printf("audit log write failed, rejecting action on endpoint %s: %v", endpoint, err)
			}
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		switch result {
		case "deny_unauthenticated":
			http.Error(w, genericUnauthenticatedMessage, http.StatusUnauthorized)
			return
		case "deny_unauthorized":
			http.Error(w, genericUnauthorizedMessage, http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// buildTLSConfig builds a hot-reloadable *tls.Config for the
// control-plane HTTP server (docs/enterprise-v1-plan.md §5 layer 3/7):
// GetConfigForClient reads holder.Current() fresh on every handshake,
// so /admin/reload-tls or SIGHUP rotates certificate/CA material
// without a restart or dropping any already-established connection —
// identical mechanism to internal/transport/tls.go's peer-mTLS reload.
func buildTLSConfig(holder *identity.Holder, clientAuth tls.ClientAuthType) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			m := holder.Current()
			return &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: []tls.Certificate{m.Certificate},
				ClientCAs:    m.CAPool,
				ClientAuth:   clientAuth,
			}, nil
		},
	}
}
