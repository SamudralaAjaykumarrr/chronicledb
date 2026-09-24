// This file implements -max-http-connections (docs/v0.6.0-plan.md
// §10.1): a bounded accept on the control-plane HTTP listener,
// reimplemented locally (a small net.Listener wrapper) rather than
// importing golang.org/x/net/netutil's LimitListener — ChronicleDB has
// zero external Go module dependencies (docs/dependencies.md).
package main

import (
	"net"
	"sync/atomic"
)

// limitListener wraps a net.Listener to bound the number of
// concurrently open accepted connections at max. Beyond that, a newly
// accepted connection is closed immediately rather than left to queue
// in Accept (docs/v0.6.0-plan.md §10.1's other bounded-accept surface,
// internal/transport's own -max-peer-connections, takes the identical
// accept-then-reject-explicitly shape) — this is what makes the
// rejection an explicit, countable application-level event
// (chronicledb_http_connections_rejected_total) rather than an
// invisible OS-level accept-backlog overflow.
type limitListener struct {
	net.Listener
	max           int
	current       atomic.Int64
	rejectedTotal atomic.Uint64
}

// newLimitListener wraps l with a cap of max concurrently open
// connections. max <= 0 means unlimited (l is returned unwrapped).
func newLimitListener(l net.Listener, max int) net.Listener {
	if max <= 0 {
		return l
	}
	return &limitListener{Listener: l, max: max}
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if int(l.current.Add(1)) <= l.max {
			return &limitListenerConn{Conn: c, l: l}, nil
		}
		// Over the cap: refuse this one and keep accepting — a
		// misbehaving/flooding client must not be able to starve every
		// other client's ability to even attempt a connection.
		l.current.Add(-1)
		l.rejectedTotal.Add(1)
		c.Close()
	}
}

// CurrentConnections returns the number of connections currently
// admitted through this listener (chronicledb_http_connections).
func (l *limitListener) CurrentConnections() int64 { return l.current.Load() }

// RejectedTotal returns the count of connections refused for exceeding
// the cap (chronicledb_http_connections_rejected_total).
func (l *limitListener) RejectedTotal() uint64 { return l.rejectedTotal.Load() }

// limitListenerConn decrements the listener's live-connection count
// exactly once when closed, however that happens (explicit Close, or
// http.Server's own cleanup on shutdown/idle-timeout/etc.).
type limitListenerConn struct {
	net.Conn
	l        *limitListener
	closeErr error
	closed   atomic.Bool
}

func (c *limitListenerConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.l.current.Add(-1)
		c.closeErr = c.Conn.Close()
	}
	return c.closeErr
}
