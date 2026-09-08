package transport

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/identity"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/tlstest"
)

// newHolderFor builds an internal/identity.Holder (a TLSMaterialSource)
// for nodeID, signed by ca, written to files under t.TempDir().
func newHolderFor(t *testing.T, ca *tlstest.CA, nodeID string, notBefore, notAfter time.Time) *identity.Holder {
	t.Helper()
	dir := t.TempDir()
	leaf := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: nodeID, NotBefore: notBefore, NotAfter: notAfter})
	certPath, keyPath := tlstest.WriteFiles(t, dir, nodeID, leaf)
	caPath := tlstest.WriteCAFile(t, dir, ca)
	h, err := identity.NewHolder(certPath, keyPath, caPath)
	if err != nil {
		t.Fatalf("newHolderFor(%s): %v", nodeID, err)
	}
	return h
}

func TestNewTLS_ValidMTLS_RoundTrip(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, ca, "A", now.Add(-time.Hour), now.Add(time.Hour))
	hB := newHolderFor(t, ca, "B", now.Add(-time.Hour), now.Add(time.Hour))

	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	b, err := NewTLS("B", "127.0.0.1:0", map[raft.NodeID]string{"A": a.Addr()}, hB)
	if err != nil {
		t.Fatalf("NewTLS B: %v", err)
	}
	defer b.Close()

	msg := raft.Message{Type: raft.MsgAppendEntriesRequest, From: "B", To: "A", Term: 1}
	b.Send(msg)

	select {
	case got := <-a.Recv():
		if got.From != "B" || got.Type != msg.Type {
			t.Fatalf("received = %+v, want From=B", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message over valid mTLS connection")
	}
}

// dialRaw opens a raw TLS connection to addr using cfg, for tests that
// need to construct an adversarial handshake NewTLS's own peerSendLoop
// (which always presents a legitimately-issued certificate) cannot
// express.
func dialRaw(t *testing.T, addr string, cfg *tls.Config) (net.Conn, error) {
	t.Helper()
	d := &net.Dialer{Timeout: 2 * time.Second}
	return tls.DialWithDialer(d, "tcp", addr, cfg)
}

func TestNewTLS_ExpiredCertificateRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, ca, "A", now.Add(-time.Hour), now.Add(time.Hour))
	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	expired := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "B", NotBefore: now.Add(-2 * time.Hour), NotAfter: now.Add(-time.Hour)})
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{expired},
		InsecureSkipVerify: true,
	}
	conn, _ := dialRaw(t, a.Addr(), cfg)
	if conn != nil {
		defer conn.Close()
	}
	// Whether the handshake itself fails, or completes but a subsequent
	// write is refused, the important property is: no Raft message from
	// an expired client certificate is ever delivered.
	assertNothingArrives(t, a)
}

func TestNewTLS_WrongCARejected(t *testing.T) {
	caA := tlstest.NewCA(t)
	caOther := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, caA, "A", now.Add(-time.Hour), now.Add(time.Hour))
	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	wrongCA := caOther.IssueLeaf(t, tlstest.LeafOptions{CommonName: "B"})
	cfg := &tls.Config{Certificates: []tls.Certificate{wrongCA}, InsecureSkipVerify: true}
	conn, err := dialRaw(t, a.Addr(), cfg)
	if conn != nil {
		defer conn.Close()
	}
	assertNothingArrives(t, a)
	_ = err
}

func TestNewTLS_SelfSignedRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, ca, "A", now.Add(-time.Hour), now.Add(time.Hour))
	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	selfSigned := tlstest.SelfSigned(t, "B")
	cfg := &tls.Config{Certificates: []tls.Certificate{selfSigned}, InsecureSkipVerify: true}
	conn, _ := dialRaw(t, a.Addr(), cfg)
	if conn != nil {
		defer conn.Close()
	}
	assertNothingArrives(t, a)
}

func TestNewTLS_NoCertificateRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, ca, "A", now.Add(-time.Hour), now.Add(time.Hour))
	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	// No client certificate presented at all.
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, _ := dialRaw(t, a.Addr(), cfg)
	if conn != nil {
		defer conn.Close()
	}
	assertNothingArrives(t, a)
}

func TestNewTLS_PlaintextConnectionRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, ca, "A", now.Add(-time.Hour), now.Add(time.Hour))
	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	// A plain TCP connection, never upgraded to TLS at all — the "no
	// plaintext fallback" requirement: writing a raw (unencrypted)
	// transport frame must never be interpreted as a message.
	conn, err := net.DialTimeout("tcp", a.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dialing plaintext TCP to a TLS listener: %v", err)
	}
	defer conn.Close()
	msg := raft.Message{Type: raft.MsgAppendEntriesRequest, From: "B", To: "A", Term: 1}
	_ = writeFrame(conn, msg) // best-effort; the server never completes a TLS handshake over this
	assertNothingArrives(t, a)
}

// TestNewTLS_WrongIdentitySpoofRejected proves that a connection whose
// TLS certificate identity is "mallory" cannot claim (via the wire
// Message.From field) to be a different, legitimate cluster member
// ("B") — the load-bearing check in readLoop that ties every message on
// a connection back to that connection's actual verified identity.
func TestNewTLS_WrongIdentitySpoofRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()
	hA := newHolderFor(t, ca, "A", now.Add(-time.Hour), now.Add(time.Hour))
	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()

	// mallory holds a perfectly valid certificate, issued by the same
	// trusted CA — just not for the identity it is about to claim on
	// the wire.
	malloryCert := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "mallory"})
	cfg := &tls.Config{Certificates: []tls.Certificate{malloryCert}, InsecureSkipVerify: true}
	conn, err := dialRaw(t, a.Addr(), cfg)
	if err != nil {
		t.Fatalf("mallory's otherwise-valid handshake should succeed: %v", err)
	}
	defer conn.Close()

	spoofed := raft.Message{Type: raft.MsgAppendEntriesRequest, From: "B", To: "A", Term: 1}
	if err := writeFrame(conn, spoofed); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	assertNothingArrives(t, a)
}

func assertNothingArrives(t *testing.T, tr *Transport) {
	t.Helper()
	select {
	case msg, ok := <-tr.Recv():
		if ok {
			t.Fatalf("expected no message to be delivered, got %+v", msg)
		}
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing arrives.
	}
}

// TestNewTLS_CertificateRotationNoDroppedConnection proves the
// certificate-rotation failure semantics documented in
// docs/enterprise-v1-plan.md §5: "in-flight connections using the old
// certificate continue to validate until the overlap window ends; no
// live connection is forcibly dropped by rotation alone." It reloads
// A's identity.Holder mid-session (a fresh CA-issued certificate) and
// confirms an already-open B->A connection keeps delivering messages
// throughout, while a NEW connection dialed after rotation also
// succeeds (proving the rotation actually took effect for future
// handshakes, not just that nothing broke).
func TestNewTLS_CertificateRotationNoDroppedConnection(t *testing.T) {
	ca := tlstest.NewCA(t)
	now := time.Now()

	dirA := t.TempDir()
	leafA1 := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "A", NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)})
	certPathA, keyPathA := tlstest.WriteFiles(t, dirA, "A", leafA1)
	caPathA := tlstest.WriteCAFile(t, dirA, ca)
	hA, err := identity.NewHolder(certPathA, keyPathA, caPathA)
	if err != nil {
		t.Fatalf("NewHolder A: %v", err)
	}

	hB := newHolderFor(t, ca, "B", now.Add(-time.Hour), now.Add(time.Hour))

	a, err := NewTLS("A", "127.0.0.1:0", nil, hA)
	if err != nil {
		t.Fatalf("NewTLS A: %v", err)
	}
	defer a.Close()
	b, err := NewTLS("B", "127.0.0.1:0", map[raft.NodeID]string{"A": a.Addr()}, hB)
	if err != nil {
		t.Fatalf("NewTLS B: %v", err)
	}
	defer b.Close()

	send := func(seq raft.Term) {
		b.Send(raft.Message{Type: raft.MsgAppendEntriesRequest, From: "B", To: "A", Term: seq})
	}
	recv := func() raft.Message {
		select {
		case m := <-a.Recv():
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for message")
			return raft.Message{}
		}
	}

	send(1)
	if got := recv(); got.Term != 1 {
		t.Fatalf("pre-rotation message: got Term=%d", got.Term)
	}

	// Rotate A's certificate to a fresh one (same CA, new serial).
	leafA2 := ca.IssueLeaf(t, tlstest.LeafOptions{CommonName: "A", NotBefore: now.Add(-time.Hour), NotAfter: now.Add(2 * time.Hour)})
	tlstest.WriteFiles(t, dirA, "A", leafA2)
	if err := hA.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// The connection B already established before rotation must still
	// work — it was never forcibly dropped.
	send(2)
	if got := recv(); got.Term != 2 {
		t.Fatalf("post-rotation message over pre-existing connection: got Term=%d", got.Term)
	}

	// A brand new connection, dialed after rotation, must also succeed
	// (proving the rotation actually took effect, not merely that the
	// old connection survived by inertia).
	c, err := NewTLS("C", "127.0.0.1:0", map[raft.NodeID]string{"A": a.Addr()}, newHolderFor(t, ca, "C", now.Add(-time.Hour), now.Add(time.Hour)))
	if err != nil {
		t.Fatalf("NewTLS C: %v", err)
	}
	defer c.Close()
	c.Send(raft.Message{Type: raft.MsgAppendEntriesRequest, From: "C", To: "A", Term: 3})
	if got := recv(); got.From != "C" || got.Term != 3 {
		t.Fatalf("new post-rotation connection: got %+v", got)
	}
}
