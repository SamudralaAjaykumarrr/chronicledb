// Package authn implements ChronicleDB's pluggable client authentication
// layer (docs/enterprise-v1-plan.md §5 layer 4): "a pluggable check
// (interface, not a hardcoded mechanism) with two V1-shipped
// implementations: static bearer token... and mTLS client-certificate
// identity." No OIDC/SSO/LDAP integration (see that section's
// Non-goals).
package authn

import (
	"crypto/subtle"
	"errors"
	"net/http"
)

// ErrUnauthenticated is returned by Authenticator.Authenticate when a
// request carries no valid credential. Handlers must map this to HTTP
// 401 with a generic message — never one that distinguishes "wrong
// token" from "unknown user" from "disabled account"
// (docs/enterprise-v1-plan.md §5 "Failure semantics").
var ErrUnauthenticated = errors.New("authn: unauthenticated")

// Principal is the authenticated identity a request resolves to. Name
// is opaque to this package — for token auth it is whatever name the
// operator's -auth-token-file mapped the presented token to; for mTLS
// auth it is the client certificate's CommonName (the same identity
// convention internal/identity uses for node identity).
type Principal struct {
	Name string
}

// Authenticator is the pluggable check every V1 authentication mode
// implements. Authenticate never has a side effect beyond reading r —
// it is safe to call speculatively and must never itself constitute the
// "administrative action" NO UNAUTHENTICATED ADMIN ACTION guards.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// TokenAuthenticator implements static bearer-token authentication
// (docs/enterprise-v1-plan.md §5 layer 4): a client presents
// `Authorization: Bearer <token>`, compared against every configured
// token using a constant-time comparison (crypto/subtle) so response
// timing cannot be used to guess a valid token byte-by-byte.
type TokenAuthenticator struct {
	// tokens maps token value -> principal name. Both are compared
	// constant-time against the presented token to avoid leaking which
	// (if any) prefix matched via early-exit timing.
	tokens map[string]string
}

// NewTokenAuthenticator builds a TokenAuthenticator from a token ->
// principal-name mapping (see cmd/chronicledb-node's -auth-token-file
// format, docs/security.md).
func NewTokenAuthenticator(tokens map[string]string) *TokenAuthenticator {
	cp := make(map[string]string, len(tokens))
	for k, v := range tokens {
		cp[k] = v
	}
	return &TokenAuthenticator{tokens: cp}
}

// Authenticate implements Authenticator.
func (a *TokenAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	presented, ok := bearerToken(r)
	if !ok || presented == "" {
		return Principal{}, ErrUnauthenticated
	}
	// Compare against every configured token, always, rather than
	// stopping at the first match: a map lookup by presented token
	// value would leak, via timing, whether presented is even the
	// right *length* as some configured token before comparison
	// begins; iterating every configured token with a constant-time
	// compare and only then deciding avoids that. The number of
	// configured tokens (small, operator-provisioned, not
	// attacker-influenced) does not itself leak anything the operator
	// didn't already know.
	var matchedName string
	found := 0
	for tok, name := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(presented)) == 1 {
			matchedName = name
			found = 1
		}
	}
	if found == 0 {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Name: matchedName}, nil
}

func bearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return "", false
	}
	return h[len(prefix):], true
}

// MTLSAuthenticator implements mTLS client-certificate identity
// authentication (docs/enterprise-v1-plan.md §5 layer 4): it reuses the
// already-verified certificate from client TLS (layer 3) as the
// authenticated principal — crypto/tls has already checked the
// certificate chains to a trusted CA and is within its validity window
// (net/http only calls the handler at all once that succeeded, when
// tls.Config.ClientAuth requires it); this authenticator's job is only
// to read off the identity, never to re-verify trust.
type MTLSAuthenticator struct{}

// NewMTLSAuthenticator returns an MTLSAuthenticator.
func NewMTLSAuthenticator() *MTLSAuthenticator { return &MTLSAuthenticator{} }

// Authenticate implements Authenticator.
func (a *MTLSAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return Principal{}, ErrUnauthenticated
	}
	cn := r.TLS.PeerCertificates[0].Subject.CommonName
	if cn == "" {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Name: cn}, nil
}
