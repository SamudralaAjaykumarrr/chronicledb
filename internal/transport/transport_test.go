package transport

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

func waitRecv(t *testing.T, tr *Transport, timeout time.Duration) raft.Message {
	t.Helper()
	select {
	case m := <-tr.Recv():
		return m
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message")
		return raft.Message{}
	}
}

func TestSendReceiveRoundTrip(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	defer a.Close()

	b, err := New("B", "127.0.0.1:0", map[raft.NodeID]string{"A": a.Addr()})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	defer b.Close()

	msg := raft.Message{
		Type: raft.MsgAppendEntriesRequest,
		From: "B", To: "A", Term: 5,
		PrevLogIndex: 3, PrevLogTerm: 2,
		Entries:      []raft.Entry{{Index: 4, Term: 2, Data: []byte("hello")}},
		LeaderCommit: 3,
	}
	b.Send(msg)

	got := waitRecv(t, a, 5*time.Second)
	if got.Type != msg.Type || got.From != msg.From || got.To != msg.To || got.Term != msg.Term {
		t.Fatalf("received = %+v, want %+v", got, msg)
	}
	if len(got.Entries) != 1 || string(got.Entries[0].Data) != "hello" {
		t.Fatalf("received entries = %+v, want one entry with data hello", got.Entries)
	}
}

func TestSendToUnknownPeerIsSilentlyDropped(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
	// No peer address registered for "ghost": Send must not block or panic.
	a.Send(raft.Message{From: "A", To: "ghost"})
}

func TestSendToDeadPeerDoesNotBlockOrPanic(t *testing.T) {
	// Bind a listener and then close it immediately, so the address is
	// (almost certainly) refusing connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	a, err := New("A", "127.0.0.1:0", map[raft.NodeID]string{"dead": deadAddr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()

	done := make(chan struct{})
	go func() {
		a.Send(raft.Message{From: "A", To: "dead"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send to a dead peer blocked the caller")
	}
}

// TestMultipleMessagesPreserveIndependentFrames proves several messages
// sent back-to-back over the same persistent connection are each
// decoded correctly (framing does not corrupt or merge payloads).
func TestMultipleMessagesPreserveIndependentFrames(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	defer a.Close()
	b, err := New("B", "127.0.0.1:0", map[raft.NodeID]string{"A": a.Addr()})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	defer b.Close()

	for i := 0; i < 20; i++ {
		b.Send(raft.Message{Type: raft.MsgRequestVoteRequest, From: "B", To: "A", Term: raft.Term(i)})
	}
	seen := make(map[raft.Term]bool)
	for i := 0; i < 20; i++ {
		m := waitRecv(t, a, 5*time.Second)
		seen[m.Term] = true
	}
	if len(seen) != 20 {
		t.Fatalf("saw %d distinct terms, want 20 (framing corrupted a message)", len(seen))
	}
}

// TestMalformedFrameDoesNotPanicOrAffectOtherConnections sends a raw,
// adversarial frame (a header claiming an oversized/garbage payload)
// directly over a socket and confirms the receiver survives and keeps
// serving other, well-formed connections.
func TestMalformedFrameDoesNotPanicOrAffectOtherConnections(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	defer a.Close()

	// Connection 1: claims an oversized payload length.
	conn1, err := net.Dial("tcp", a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	header := make([]byte, frameHeaderSize)
	header[0] = WireVersion
	binary.BigEndian.PutUint32(header[1:], MaxMessageSize+1)
	if _, err := conn1.Write(header); err != nil {
		t.Fatalf("Write oversized header: %v", err)
	}
	conn1.Close()

	// Connection 2: wrong wire version.
	conn2, err := net.Dial("tcp", a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	badVersionHeader := make([]byte, frameHeaderSize)
	badVersionHeader[0] = WireVersion + 1
	binary.BigEndian.PutUint32(badVersionHeader[1:], 0)
	if _, err := conn2.Write(badVersionHeader); err != nil {
		t.Fatalf("Write bad-version header: %v", err)
	}
	conn2.Close()

	// A legitimate transport must still work afterward.
	b, err := New("B", "127.0.0.1:0", map[raft.NodeID]string{"A": a.Addr()})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	defer b.Close()
	b.Send(raft.Message{Type: raft.MsgRequestVoteRequest, From: "B", To: "A", Term: 42})
	got := waitRecv(t, a, 5*time.Second)
	if got.Term != 42 {
		t.Fatalf("got term %d after malformed frames, want 42 (receiver did not recover)", got.Term)
	}
}

func TestCloseStopsDeliveryAndClosesRecv(t *testing.T) {
	a, err := New("A", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A second Close must not panic or hang.
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// Send after Close must be a safe no-op.
	a.Send(raft.Message{From: "A", To: "anyone"})

	_, ok := <-a.Recv()
	if ok {
		t.Fatal("Recv channel not closed after Close")
	}
}
