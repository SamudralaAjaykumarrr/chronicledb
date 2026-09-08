// This file extends internal/transport with peer mTLS
// (docs/enterprise-v1-plan.md §5 layer 2): "internal/transport requires
// and verifies a client certificate on every inbound peer connection and
// presents one on every outbound dial; connections without a valid peer
// certificate are rejected before any Raft message is read. No plaintext
// fallback: a misconfigured peer fails closed (connection refused),
// never silently downgrades."
package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/identity"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TLSMaterialSource supplies the current TLS certificate/CA material for
// peer mTLS, re-read on every call so a Transport always dials/accepts
// using whatever internal/identity.Holder.Reload most recently loaded
// (docs/enterprise-v1-plan.md §5 layer 7: certificate rotation without a
// process restart). internal/identity.Holder itself implements this
// interface (its Current method has the matching signature).
type TLSMaterialSource interface {
	Current() identity.Material
}

// serverTLSConfig builds a *tls.Config for accepting inbound peer
// connections: it requires and verifies a client certificate
// (RequireAndVerifyClientCert) against src's current CA pool — read
// fresh on every handshake via GetConfigForClient, so a certificate
// rotation takes effect for every new inbound connection without
// restarting the listener, while a connection whose handshake already
// completed is entirely unaffected (crypto/tls never revisits a
// completed handshake).
func serverTLSConfig(src TLSMaterialSource) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			m := src.Current()
			return &tls.Config{
				MinVersion:   tls.VersionTLS12,
				ClientAuth:   tls.RequireAndVerifyClientCert,
				Certificates: []tls.Certificate{m.Certificate},
				ClientCAs:    m.CAPool,
			}, nil
		},
	}
}

// clientTLSConfigFor builds a *tls.Config for dialing a specific,
// already-known peer (expectedPeerID). Peer certificates in
// ChronicleDB's operator-managed-CA model are not required to carry a
// DNS SAN matching the dial address (docs/security.md documents
// CommonName-as-identity as the supported convention), so standard
// hostname-based verification is not the right tool here: this
// disables crypto/tls's built-in hostname check
// (InsecureSkipVerify=true) but replaces it with an explicit
// VerifyPeerCertificate callback that (a) verifies the presented chain
// against src's current trusted CA pool exactly as crypto/tls's default
// verifier would, and (b) additionally confirms the leaf certificate's
// identity (CommonName/SAN) actually matches expectedPeerID — i.e.
// strictly more verification than the default, not less: chain trust
// AND a specific-peer identity check, rather than chain trust alone.
func clientTLSConfigFor(src TLSMaterialSource, expectedPeerID raft.NodeID) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			m := src.Current()
			return &m.Certificate, nil
		},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			m := src.Current()
			return verifyChainAndIdentity(rawCerts, m.CAPool, string(expectedPeerID))
		},
	}
}

// verifyChainAndIdentity parses rawCerts (as presented on the wire),
// verifies the leaf chains to a certificate in caPool, and verifies the
// leaf's identity matches expectedID (docs/enterprise-v1-plan.md §5
// layer 1/2: node identity is the certificate subject, checked on every
// peer connection, not just at the node's own startup).
func verifyChainAndIdentity(rawCerts [][]byte, caPool *x509.CertPool, expectedID string) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("transport: no peer certificate presented")
	}
	certs := make([]*x509.Certificate, len(rawCerts))
	for i, raw := range rawCerts {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("transport: parsing peer certificate: %w", err)
		}
		certs[i] = c
	}
	if caPool == nil {
		return fmt.Errorf("transport: no trusted CA pool configured, refusing to trust any peer certificate")
	}
	intermediates := x509.NewCertPool()
	for _, c := range certs[1:] {
		intermediates.AddCert(c)
	}
	opts := x509.VerifyOptions{
		Roots:         caPool,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}
	if _, err := certs[0].Verify(opts); err != nil {
		return fmt.Errorf("transport: peer certificate chain verification failed: %w", err)
	}
	if expectedID != "" {
		if err := identity.VerifyPeerIdentity(expectedID, certs); err != nil {
			return fmt.Errorf("transport: %w", err)
		}
	}
	return nil
}

// NewTLS creates a Transport exactly like New, except every inbound
// connection must present, and every outbound connection presents, a
// certificate validated per this file's doc comment — never falling
// back to plaintext. src is consulted fresh on every handshake, so
// internal/identity.Holder.Reload (triggered by SIGHUP or
// /admin/reload-tls) rotates this Transport's material without
// restarting it or dropping any live connection.
func NewTLS(id raft.NodeID, listenAddr string, peerAddrs map[raft.NodeID]string, src TLSMaterialSource) (*Transport, error) {
	rawLn, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen on %s: %w", listenAddr, err)
	}
	ln := tls.NewListener(rawLn, serverTLSConfig(src))
	t := newTransport(id, ln, peerAddrs)
	t.tlsSrc = src
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}
