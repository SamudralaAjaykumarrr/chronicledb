// Package transport implements ChronicleDB's production network
// transport (docs/architecture.md §5 "internal/transport": "production
// network implementation of the transport interface internal/raft
// defines"; docs/roadmap.md Phase 5). It carries internal/raft.Message
// values between real OS processes over TCP.
//
// internal/raft itself never imports this package (docs/architecture.md
// §5's dependency rules: "internal/raft must not import
// internal/transport"): Transport is a driver-side adapter, used by
// internal/node, that feeds received messages back into a raft.Core via
// Input{Kind: InputMessage} and sends a Core's outbound Output.Messages
// out over the wire.
//
// Design deliberately stays as small as correctness requires (per this
// phase's brief: "prefer standard library... do not build a custom RPC
// framework unnecessarily"): a persistent, lazily-(re)dialed TCP
// connection per peer, explicit length-prefixed framing with a bounded
// maximum message size, a one-byte wire-format version, and
// encoding/gob for the message body (a Message is a plain data value
// with no interfaces/cyclic references, well within gob's comfortable
// range). Every decode path is bounded and panic-safe (no message can
// crash a node merely by being malformed) per docs/failure-model.md §6.
package transport

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// WireVersion is the transport frame format version. A peer speaking a
// different version is rejected explicitly rather than misparsed,
// mirroring internal/wal's own format-version discipline
// (docs/wal.md §6).
const WireVersion uint8 = 1

// MaxMessageSize bounds the size of a single encoded Message this
// transport will ever read or allocate for, regardless of what a
// (possibly malformed or adversarial) length prefix claims
// (docs/failure-model.md §6: bounded request sizes / bounded
// allocations). Large enough for any realistic batch of AppendEntries
// log entries at V1 scale; small enough to bound a single connection's
// worst-case allocation.
const MaxMessageSize = 64 * 1024 * 1024 // 64 MiB

// frame layout: version(1B) length(4B) payload(length bytes, gob-encoded Message)
const frameHeaderSize = 1 + 4

// dialTimeout bounds how long a single outbound connection attempt may
// block a peer's dedicated sender goroutine (see peerSender) before
// giving up on that attempt.
const dialTimeout = 2 * time.Second

// sendQueueSize bounds each peer's outbound queue. Send never blocks
// the caller: once a peer's queue is full, further sends to it are
// dropped, exactly as a congested/lossy real network would drop
// packets (raft.Core's protocol already tolerates this).
const sendQueueSize = 256

// peerSender owns exactly one outbound TCP connection to one peer and
// is the only goroutine that ever writes to it — this is what makes
// concurrent Send calls to the same peer safe without an explicit
// per-write lock: net.Conn.Write is not safe against multiple
// concurrent writers interleaving partial writes of two different
// frames, so serializing all of a peer's writes through a single
// goroutine (rather than one goroutine per Send call sharing the
// connection) is load-bearing for correctness, not just style.
type peerSender struct {
	ch chan raft.Message
}

// Transport is a production, real-socket implementation of the message
// delivery internal/raft's driver (internal/node) needs. It is safe
// for concurrent use by multiple goroutines.
type Transport struct {
	id   raft.NodeID
	ln   net.Listener
	recv chan raft.Message

	mu      sync.Mutex
	addrs   map[raft.NodeID]string
	senders map[raft.NodeID]*peerSender
	blocked map[raft.NodeID]bool
	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup

	// inbound tracks every accepted (server-side) connection so Close
	// can force them all shut — an accepted connection is otherwise not
	// closed merely by closing the listener, which would leave its
	// readLoop goroutine blocked in a read forever and deadlock Close's
	// wg.Wait.
	inbound map[net.Conn]struct{}
}

// New creates a Transport for node id, listening on listenAddr, with
// peerAddrs giving every other cluster member's dial address (id ->
// "host:port"). New starts accepting inbound connections immediately;
// received messages are delivered via Recv.
func New(id raft.NodeID, listenAddr string, peerAddrs map[raft.NodeID]string) (*Transport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen on %s: %w", listenAddr, err)
	}
	t := &Transport{
		id:      id,
		ln:      ln,
		recv:    make(chan raft.Message, 256),
		addrs:   make(map[raft.NodeID]string, len(peerAddrs)),
		senders: make(map[raft.NodeID]*peerSender),
		blocked: make(map[raft.NodeID]bool),
		closeCh: make(chan struct{}),
		inbound: make(map[net.Conn]struct{}),
	}
	for peer, addr := range peerAddrs {
		t.addrs[peer] = addr
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Addr returns the transport's actual listen address (useful when
// listenAddr was ":0", letting the OS choose a port, e.g. in tests).
func (t *Transport) Addr() string { return t.ln.Addr().String() }

// Recv returns the channel on which received messages arrive. The
// channel is closed after Close.
func (t *Transport) Recv() <-chan raft.Message { return t.recv }

// Send transmits msg to msg.To asynchronously and never blocks the
// caller on network I/O: it enqueues onto that peer's dedicated sender
// goroutine (dialing lazily and reconnecting on failure) and simply
// drops the message if the queue is full or delivery cannot be
// completed, exactly as a lossy/congested network would
// (docs/raft.md's protocol is designed to tolerate dropped messages;
// see docs/testing-strategy.md §3's simulator, which models the
// identical possibility deterministically).
func (t *Transport) Send(msg raft.Message) {
	t.mu.Lock()
	if t.closed || t.blocked[msg.To] {
		t.mu.Unlock()
		return
	}
	addr, ok := t.addrs[msg.To]
	if !ok {
		t.mu.Unlock()
		return // unknown peer: nothing we can do, message is simply lost
	}
	ps, exists := t.senders[msg.To]
	if !exists {
		ps = &peerSender{ch: make(chan raft.Message, sendQueueSize)}
		t.senders[msg.To] = ps
		t.wg.Add(1)
		go t.peerSendLoop(msg.To, addr, ps.ch)
	}
	t.mu.Unlock()

	select {
	case ps.ch <- msg:
	default:
		// Queue full: drop, as a congested real network would.
	}
}

// peerSendLoop is the single goroutine responsible for every byte ever
// written to peer's connection (see peerSender's doc comment). It owns
// the connection exclusively: dialing lazily on first use, redialing
// after a write failure, and exiting only when the transport closes.
func (t *Transport) peerSendLoop(peer raft.NodeID, addr string, ch chan raft.Message) {
	defer t.wg.Done()
	var conn net.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		select {
		case msg := <-ch:
			if conn == nil {
				c, err := net.DialTimeout("tcp", addr, dialTimeout)
				if err != nil {
					continue // drop this message; a later one may succeed once the peer is reachable
				}
				conn = c
			}
			if err := writeFrame(conn, msg); err != nil {
				conn.Close()
				conn = nil
			}
		case <-t.closeCh:
			return
		}
	}
}

// acceptLoop accepts inbound connections until Close, handling each on
// its own goroutine.
func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return // listener closed
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			continue
		}
		t.inbound[conn] = struct{}{}
		t.mu.Unlock()
		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

// readLoop reads frames from a single inbound connection until it
// fails or the transport is closed, delivering each successfully
// decoded Message to recv. A malformed frame (bad version, oversized
// length, truncated payload, or a value gob cannot decode) closes just
// this connection — it never panics and never affects any other
// connection or peer (docs/failure-model.md §6).
func (t *Transport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()
	defer func() {
		t.mu.Lock()
		delete(t.inbound, conn)
		t.mu.Unlock()
	}()
	r := bufio.NewReader(conn)
	for {
		msg, err := readFrame(r)
		if err != nil {
			return
		}
		t.mu.Lock()
		blocked := t.blocked[msg.From]
		t.mu.Unlock()
		if blocked {
			continue // simulated partition: silently drop, as a real dropped packet would be
		}
		select {
		case t.recv <- msg:
		case <-t.closeCh:
			return
		}
	}
}

// writeFrame encodes and writes one Message as version(1B) + length(4B)
// + gob(Message) to w.
func writeFrame(w io.Writer, msg raft.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("transport: panic encoding message: %v", r)
		}
	}()
	var buf bytes.Buffer
	if encErr := gob.NewEncoder(&buf).Encode(msg); encErr != nil {
		return fmt.Errorf("transport: encode message: %w", encErr)
	}
	if buf.Len() > MaxMessageSize {
		return fmt.Errorf("transport: encoded message %d bytes exceeds max %d", buf.Len(), MaxMessageSize)
	}
	header := make([]byte, frameHeaderSize)
	header[0] = WireVersion
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("transport: write frame header: %w", err)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("transport: write frame payload: %w", err)
	}
	return nil
}

// readFrame reads and decodes exactly one frame from r. It never
// allocates more than MaxMessageSize for a payload and never panics on
// malformed input: a gob-decode panic (possible only on deeply
// malformed adversarial input, since Message contains no interfaces)
// is recovered and converted into an error.
func readFrame(r io.Reader) (msg raft.Message, err error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return raft.Message{}, err
	}
	version := header[0]
	if version != WireVersion {
		return raft.Message{}, fmt.Errorf("transport: unsupported wire version %d, expected %d", version, WireVersion)
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length > MaxMessageSize {
		return raft.Message{}, fmt.Errorf("transport: frame claims %d bytes, max %d", length, MaxMessageSize)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return raft.Message{}, err
	}

	defer func() {
		if rec := recover(); rec != nil {
			msg = raft.Message{}
			err = fmt.Errorf("transport: panic decoding message: %v", rec)
		}
	}()
	if derr := gob.NewDecoder(bytes.NewReader(payload)).Decode(&msg); derr != nil {
		return raft.Message{}, fmt.Errorf("transport: decode message: %w", derr)
	}
	return msg, nil
}

// Block makes this Transport silently drop every message to or from
// peer, in both directions, as of the next Send/receive — a test-only
// fault-injection hook (this phase's brief: partition/heal scenarios
// must be exercised "end-to-end through the real node/replication
// path," not only in internal/fault's simulator) modeling a real
// network partition without needing actual OS-level network control.
// Safe for concurrent use; takes effect immediately for new sends and
// for messages not yet delivered to Recv.
func (t *Transport) Block(peer raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[peer] = true
}

// Unblock reverses a prior Block, healing the simulated partition with
// peer.
func (t *Transport) Unblock(peer raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, peer)
}

// Close shuts down the listener and every open connection, and stops
// all transport goroutines (docs Phase 5 brief's "cancellation/lifecycle
// support"). After Close, Recv's channel is closed and Send is a no-op.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	conns := make([]net.Conn, 0, len(t.inbound))
	for c := range t.inbound {
		conns = append(conns, c)
	}
	t.mu.Unlock()

	err := t.ln.Close()
	for _, c := range conns {
		c.Close()
	}
	t.wg.Wait()
	close(t.recv)
	return err
}
