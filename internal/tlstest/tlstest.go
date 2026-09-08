// Package tlstest generates throwaway CA and leaf certificates entirely
// in memory (crypto/x509 + crypto/ecdsa, stdlib only, per
// docs/dependencies.md's zero-external-dependency policy) for use by
// this repository's own test suites (internal/identity,
// internal/transport, internal/authn, cmd/chronicledb-node, and any
// internal/node/cmd integration test exercising Security Foundation
// (docs/enterprise-v1-plan.md §5) TLS/mTLS fixtures.
//
// It is a test-support package: nothing under internal/ or
// cmd/chronicledb-node's non-test code imports it, and it must never be
// imported from production code. It exists so every package's own test
// suite does not each reinvent certificate generation, and so every
// fixture (valid, expired, wrong-CA, self-signed, mismatched-identity)
// is built the same, deterministic-enough way.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// loopbackIPs is included as IP SANs on every issued leaf certificate
// (in addition to its DNS SANs) so a test dialing "127.0.0.1:port"
// (this repository's standard local-cluster address form throughout
// internal/node/cmd/chronicledb-node) verifies successfully: Go's TLS
// client checks the dialed host against IP SANs specifically when the
// host is an IP literal rather than a hostname, never falling back to
// a DNSName match for it.
var loopbackIPs = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}

// CA is an in-memory certificate authority a test can issue leaf
// certificates from.
type CA struct {
	Cert    *x509.Certificate
	CertPEM []byte
	key     *ecdsa.PrivateKey
}

// NewCA creates a fresh, throwaway CA.
func NewCA(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "chronicledb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("tlstest: create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlstest: parse CA cert: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &CA{Cert: cert, CertPEM: pemBytes, key: key}
}

// LeafOptions configures IssueLeaf.
type LeafOptions struct {
	// CommonName becomes the leaf's Subject.CommonName — the identity
	// internal/identity binds a node's -id flag to, and the identity
	// internal/authn's mTLS authenticator reads as the authenticated
	// principal.
	CommonName string
	NotBefore  time.Time
	NotAfter   time.Time
	// ServerAuth/ClientAuth control ExtKeyUsage; both true by default
	// when neither is set, since ChronicleDB peer connections act as
	// both a TLS server (accepting) and a TLS client (dialing) using
	// the same certificate.
	ServerAuth bool
	ClientAuth bool
}

// IssueLeaf issues a leaf certificate signed by ca, returning both the
// parsed certificate/key and a ready-to-use tls.Certificate.
func (ca *CA) IssueLeaf(t testing.TB, opts LeafOptions) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: generate leaf key: %v", err)
	}
	notBefore := opts.NotBefore
	if notBefore.IsZero() {
		notBefore = time.Now().Add(-time.Hour)
	}
	notAfter := opts.NotAfter
	if notAfter.IsZero() {
		notAfter = time.Now().Add(24 * time.Hour)
	}
	var extKeyUsage []x509.ExtKeyUsage
	if opts.ServerAuth || !opts.ClientAuth {
		extKeyUsage = append(extKeyUsage, x509.ExtKeyUsageServerAuth)
	}
	if opts.ClientAuth || !opts.ServerAuth {
		extKeyUsage = append(extKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: opts.CommonName},
		DNSNames:     []string{opts.CommonName, "localhost"},
		IPAddresses:  loopbackIPs,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  extKeyUsage,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("tlstest: create leaf cert for %q: %v", opts.CommonName, err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlstest: marshal leaf key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("tlstest: build tls.Certificate for %q: %v", opts.CommonName, err)
	}
	return cert
}

// SelfSigned issues a leaf certificate signed by itself, not by any CA
// — used to test that a self-signed certificate is rejected when a
// specific CA is required.
func SelfSigned(t testing.TB, commonName string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: generate self-signed key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		DNSNames:     []string{commonName, "localhost"},
		IPAddresses:  loopbackIPs,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("tlstest: create self-signed cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlstest: marshal self-signed key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("tlstest: build tls.Certificate: %v", err)
	}
	return cert
}

// WriteFiles writes cert/key to PEM files under dir and returns their
// paths — for tests that exercise ChronicleDB's file-path-based
// (-tls-cert/-tls-key/-tls-ca style) configuration surface rather than
// in-memory tls.Certificate values directly.
func WriteFiles(t testing.TB, dir, prefix string, cert tls.Certificate) (certPath, keyPath string) {
	t.Helper()
	certPath = filepath.Join(dir, prefix+"-cert.pem")
	keyPath = filepath.Join(dir, prefix+"-key.pem")
	var certPEM []byte
	for _, der := range cert.Certificate {
		certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("tlstest: write cert file: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(cert.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatalf("tlstest: marshal key for file: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("tlstest: write key file: %v", err)
	}
	return certPath, keyPath
}

// WriteCAFile writes ca's certificate PEM to dir and returns its path.
func WriteCAFile(t testing.TB, dir string, ca *CA) string {
	t.Helper()
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, ca.CertPEM, 0o600); err != nil {
		t.Fatalf("tlstest: write CA file: %v", err)
	}
	return path
}

// WriteCAFileMulti concatenates several CAs' PEM certificates into one
// file — used to build an "overlap window" trust bundle for certificate
// rotation tests (docs/enterprise-v1-plan.md §5 "Certificate rotation"
// failure semantics: old and new peer certificates both validate during
// a rotation window).
func WriteCAFileMulti(t testing.TB, dir string, cas ...*CA) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("ca-bundle-%d.pem", time.Now().UnixNano()))
	var all []byte
	for _, ca := range cas {
		all = append(all, ca.CertPEM...)
	}
	if err := os.WriteFile(path, all, 0o600); err != nil {
		t.Fatalf("tlstest: write CA bundle file: %v", err)
	}
	return path
}
