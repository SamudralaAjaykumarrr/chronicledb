package identity

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/tlstest"
)

func TestBindNodeIdentity_CommonNameMatch(t *testing.T) {
	ca := tlstest.NewCA(t)
	cert := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1"})
	leaf := cert.Leaf
	if leaf == nil {
		t.Fatalf("tlstest.IssueLeaf did not populate Leaf")
	}
	if err := BindNodeIdentity("n1", leaf); err != nil {
		t.Fatalf("BindNodeIdentity(n1): %v", err)
	}
}

func TestBindNodeIdentity_Mismatch(t *testing.T) {
	ca := tlstest.NewCA(t)
	cert := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1"})
	err := BindNodeIdentity("n2", cert.Leaf)
	if err == nil {
		t.Fatal("expected mismatch error for node ID not matching certificate identity")
	}
	var mismatch *ErrIdentityMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected *ErrIdentityMismatch, got %T: %v", err, err)
	}
}

func TestBindNodeIdentity_DNSNameMatch(t *testing.T) {
	ca := tlstest.NewCA(t)
	// tlstest.IssueLeaf always sets DNSNames = [CommonName, "localhost"];
	// "localhost" is therefore a legitimate SAN-based identity match
	// even though it is not the CommonName.
	cert := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1"})
	if err := BindNodeIdentity("localhost", cert.Leaf); err != nil {
		t.Fatalf("BindNodeIdentity(localhost) via SAN: %v", err)
	}
}

func TestLoad_ValidMaterial(t *testing.T) {
	dir := t.TempDir()
	ca := tlstest.NewCA(t)
	leaf := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1"})
	certPath, keyPath := tlstest.WriteFiles(t, dir, "n1", leaf)
	caPath := tlstest.WriteCAFile(t, dir, ca)

	m, err := Load(certPath, keyPath, caPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.Leaf.Subject.CommonName != "n1" {
		t.Fatalf("Leaf.Subject.CommonName = %q, want n1", m.Leaf.Subject.CommonName)
	}
	if m.CAPool == nil {
		t.Fatal("CAPool is nil, want a populated pool")
	}
}

func TestLoad_MissingFiles(t *testing.T) {
	if _, err := Load("/nonexistent/cert.pem", "/nonexistent/key.pem", ""); err == nil {
		t.Fatal("expected error loading nonexistent cert/key files")
	}
}

func TestHolder_ReloadPicksUpNewMaterial(t *testing.T) {
	dir := t.TempDir()
	ca := tlstest.NewCA(t)
	leaf1 := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1", NotAfter: time.Now().Add(time.Hour)})
	certPath, keyPath := tlstest.WriteFiles(t, dir, "n1", leaf1)
	caPath := tlstest.WriteCAFile(t, dir, ca)

	h, err := NewHolder(certPath, keyPath, caPath)
	if err != nil {
		t.Fatalf("NewHolder: %v", err)
	}
	first := h.Current().Leaf.SerialNumber.String()

	// Overwrite with a freshly issued leaf (different serial number).
	leaf2 := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1", NotAfter: time.Now().Add(2 * time.Hour)})
	tlstest.WriteFiles(t, dir, "n1", leaf2)

	if err := h.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	second := h.Current().Leaf.SerialNumber.String()
	if first == second {
		t.Fatal("Reload did not pick up new certificate material")
	}
}

func TestHolder_ReloadRejectsMalformedMaterialKeepsOld(t *testing.T) {
	dir := t.TempDir()
	ca := tlstest.NewCA(t)
	leaf := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "n1"})
	certPath, keyPath := tlstest.WriteFiles(t, dir, "n1", leaf)
	caPath := tlstest.WriteCAFile(t, dir, ca)

	h, err := NewHolder(certPath, keyPath, caPath)
	if err != nil {
		t.Fatalf("NewHolder: %v", err)
	}
	before := h.Current().Leaf.SerialNumber.String()

	// Corrupt the cert file in place.
	writeFileT(t, certPath, []byte("not a certificate"))

	if err := h.Reload(); err == nil {
		t.Fatal("expected Reload to fail on malformed certificate material")
	}
	after := h.Current().Leaf.SerialNumber.String()
	if before != after {
		t.Fatal("Reload replaced valid material with a broken reload attempt")
	}
}

func writeFileT(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
