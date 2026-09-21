package transport

import (
	"net"
	"testing"
	"time"
)

// TestMaxPeerConnectionsRejectsBeyondLimit is AC-16
// (docs/v0.6.0-plan.md §30.1): a connection beyond -max-peer-
// connections is refused/closed, and PeerConnections() never exceeds
// the configured limit.
func TestMaxPeerConnectionsRejectsBeyondLimit(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
	a.SetMaxPeerConnections(2)

	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	dial := func() net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", a.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conns = append(conns, c)
		return c
	}

	dial()
	dial()
	// Give acceptLoop a moment to register both connections before the
	// third, which must be refused.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && a.PeerConnections() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := a.PeerConnections(); got != 2 {
		t.Fatalf("PeerConnections() = %d, want 2 before the third dial", got)
	}

	before := a.PeerConnectionsRejectedTotal()
	third := dial()

	// The third connection must be closed by the server side shortly
	// after connecting — proven by a read returning EOF/error, not by
	// PeerConnections() alone (which only counts admitted connections).
	third.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := third.Read(buf); err == nil {
		t.Fatal("expected the connection beyond -max-peer-connections to be closed by the server, got a successful read")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && a.PeerConnectionsRejectedTotal() == before {
		time.Sleep(5 * time.Millisecond)
	}
	if got := a.PeerConnectionsRejectedTotal(); got <= before {
		t.Fatalf("PeerConnectionsRejectedTotal() = %d, want > %d", got, before)
	}
	if got := a.PeerConnections(); got != 2 {
		t.Fatalf("PeerConnections() = %d after the rejected third dial, want still 2", got)
	}
}

// TestMaxPeerConnectionsZeroMeansUnlimited proves the default (0) is
// exactly v0.5.0 behavior: no cap at all.
func TestMaxPeerConnectionsZeroMeansUnlimited(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
	// SetMaxPeerConnections deliberately not called: default is 0.

	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < 10; i++ {
		c, err := net.DialTimeout("tcp", a.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && a.PeerConnections() < 10 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := a.PeerConnections(); got != 10 {
		t.Fatalf("PeerConnections() = %d, want 10 (unlimited)", got)
	}
}

// TestPeerIdleTimeoutClosesSilentConnection is AC-16's idle-timeout
// half: an inbound connection that never sends a frame is closed after
// -peer-idle-timeout, and its slot in PeerConnections() is freed —
// "a dead TCP connection no longer retains its readLoop goroutine
// forever" (docs/v0.6.0-plan.md §2.2 item 4).
func TestPeerIdleTimeoutClosesSilentConnection(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
	a.SetPeerIdleTimeout(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", a.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && a.PeerConnections() != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := a.PeerConnections(); got != 1 {
		t.Fatalf("PeerConnections() = %d, want 1 before the idle timeout elapses", got)
	}

	// Send nothing; wait for the idle timeout to close the server side.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && a.PeerConnections() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := a.PeerConnections(); got != 0 {
		t.Fatalf("PeerConnections() = %d after the idle timeout should have elapsed, want 0", got)
	}
}
