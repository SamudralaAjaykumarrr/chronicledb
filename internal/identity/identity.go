// Package identity implements node-identity binding for ChronicleDB's
// Security Foundation phase (docs/enterprise-v1-plan.md §5 "Node
// identity"): a node's identity becomes its TLS certificate's subject,
// not just the free-text -id flag. V1 does not build a built-in CA/PKI
// service (see that section's Non-goals) — operators bring their own
// CA, and this package only loads and verifies material issued by it.
package identity

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
)

// Material is one loaded (cert, key, trusted-CA-pool) triple, ready to
// build a *tls.Config from. Loading is a pure file-read + parse step —
// no network calls, no filesystem watching (that is Reloader's job).
type Material struct {
	// Certificate is this node's own leaf certificate (with private
	// key attached), presented both when accepting inbound connections
	// and when dialing outbound ones.
	Certificate tls.Certificate
	// Leaf is the parsed leaf certificate — tls.Certificate.Leaf is not
	// always populated by tls.LoadX509KeyPair, so Load parses it
	// explicitly for identity-binding checks (BindNodeIdentity below).
	Leaf *x509.Certificate
	// CAPool is the trusted CA pool used to verify a peer's presented
	// certificate. Nil if no CA file was configured (verification then
	// relies entirely on the caller's own tls.Config.RootCAs/ClientCAs
	// default, which is almost never what a peer-mTLS deployment wants
	// — see docs/security.md).
	CAPool *x509.CertPool
}

// Load reads and parses a certificate/key pair and an optional trusted
// CA bundle from disk. caFile may be empty, in which case CAPool is nil.
func Load(certFile, keyFile, caFile string) (Material, error) {
	if certFile == "" || keyFile == "" {
		return Material{}, fmt.Errorf("identity: both a certificate and key file are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return Material{}, fmt.Errorf("identity: loading certificate/key pair (%s, %s): %w", certFile, keyFile, err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return Material{}, fmt.Errorf("identity: parsing leaf certificate %s: %w", certFile, err)
	}
	cert.Leaf = leaf

	var pool *x509.CertPool
	if caFile != "" {
		pool, err = loadCAPool(caFile)
		if err != nil {
			return Material{}, err
		}
	}
	return Material{Certificate: cert, Leaf: leaf, CAPool: pool}, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("identity: reading CA file %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("identity: no valid PEM certificates found in CA file %s", caFile)
	}
	return pool, nil
}

// ErrIdentityMismatch is returned by BindNodeIdentity when a node's
// configured -id does not match its own certificate's subject.
type ErrIdentityMismatch struct {
	NodeID       string
	CertSubject  string
	CertDNSNames []string
}

func (e *ErrIdentityMismatch) Error() string {
	return fmt.Sprintf("identity: configured node ID %q does not match certificate identity (CommonName=%q, DNSNames=%v)",
		e.NodeID, e.CertSubject, e.CertDNSNames)
}

// BindNodeIdentity implements docs/enterprise-v1-plan.md §5 layer 1: "a
// node's identity becomes its TLS certificate's subject... internal/node
// binds NodeID to the certificate at startup and refuses to start on a
// mismatch." A node's configured ID is accepted only if it equals the
// certificate's CommonName, or appears among its DNSNames (SAN) — both
// are legitimate ways an operator-managed CA might encode node identity.
func BindNodeIdentity(nodeID string, leaf *x509.Certificate) error {
	if leaf.Subject.CommonName == nodeID {
		return nil
	}
	for _, name := range leaf.DNSNames {
		if name == nodeID {
			return nil
		}
	}
	return &ErrIdentityMismatch{NodeID: nodeID, CertSubject: leaf.Subject.CommonName, CertDNSNames: leaf.DNSNames}
}

// VerifyPeerIdentity checks that a chain of peer-presented certificates
// (already chain-verified against a trusted CA pool by crypto/tls
// itself) actually identifies expectedID — i.e. that a holder of *any*
// CA-issued certificate cannot impersonate a *specific* other cluster
// member merely by possessing some valid certificate. Used by
// internal/transport for peer mTLS (docs/enterprise-v1-plan.md §5 layer
// 2) once a specific expected peer is known (an outbound dial always
// knows which peer it intended to reach).
func VerifyPeerIdentity(expectedID string, certs []*x509.Certificate) error {
	if len(certs) == 0 {
		return fmt.Errorf("identity: no peer certificate presented")
	}
	leaf := certs[0]
	if err := BindNodeIdentity(expectedID, leaf); err != nil {
		return err
	}
	return nil
}

// Holder is a hot-reloadable holder of Material (docs/enterprise-v1-plan.md
// §5 layer 7, certificate rotation): a *tls.Config built from GetCertificate/
// GetConfigForClient callbacks that read Holder.Current() always observes the
// most recently Reload()-ed material, while a connection whose handshake
// already completed under older material is entirely unaffected (Go's
// crypto/tls never re-runs a handshake on an established connection) —
// this is what gives rotation its "no live connection is forcibly
// dropped" property without any extra bookkeeping.
type Holder struct {
	certFile, keyFile, caFile string
	current                   atomic.Pointer[Material]
}

// NewHolder loads the initial material and returns a Holder serving it.
func NewHolder(certFile, keyFile, caFile string) (*Holder, error) {
	m, err := Load(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	h := &Holder{certFile: certFile, keyFile: keyFile, caFile: caFile}
	h.current.Store(&m)
	return h, nil
}

// Current returns the most recently loaded Material. Safe for
// concurrent use.
func (h *Holder) Current() Material { return *h.current.Load() }

// Reload re-reads certificate/key/CA material from the same paths
// NewHolder was given and atomically swaps it in. It validates the new
// material fully before swapping — a malformed replacement (e.g. a
// half-written file mid-rotation) never replaces a working
// configuration with a broken one.
func (h *Holder) Reload() error {
	m, err := Load(h.certFile, h.keyFile, h.caFile)
	if err != nil {
		return fmt.Errorf("identity: reload failed, keeping existing material: %w", err)
	}
	h.current.Store(&m)
	return nil
}
