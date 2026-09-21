package node

import (
	"container/heap"
	"sync/atomic"
)

// leaseHeapEntry is one (possibly stale) entry in leaseRegistry's
// min-heap.
type leaseHeapEntry struct {
	startSeq uint64
	id       uint64
}

// leaseMinHeap implements container/heap.Interface over StartSeq,
// ascending.
type leaseMinHeap []leaseHeapEntry

func (h leaseMinHeap) Len() int            { return len(h) }
func (h leaseMinHeap) Less(i, j int) bool  { return h[i].startSeq < h[j].startSeq }
func (h leaseMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *leaseMinHeap) Push(x interface{}) { *h = append(*h, x.(leaseHeapEntry)) }
func (h *leaseMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// leaseRegistry is the live-read-lease set (docs/v0.6.0-plan.md §9.1a,
// §15.3): every currently-open read transaction's StartSeq, read and
// written exclusively on the event-loop goroutine (exactly like
// n.waiters/n.pendingReads). It keeps StartSeqs in a min-heap with
// lazy deletion — a released lease is removed from the live set
// immediately but its heap entry is popped lazily, on the next call
// that would otherwise return it — so Min() is amortized O(1) (each
// heap entry is popped at most once across its entire lifetime,
// regardless of how many Min() calls happen in between) and
// Register/Release are O(log n), never a function scanned in full on
// every GC tick (fact 8b's sibling concern for reads).
type leaseRegistry struct {
	live map[uint64]uint64 // leaseID -> StartSeq; the authoritative live set
	heap leaseMinHeap
}

func newLeaseRegistry() *leaseRegistry {
	return &leaseRegistry{live: make(map[uint64]uint64)}
}

// Register adds a new live lease at startSeq under id. id is allocated
// by the caller (BeginReadIndex, via Node.nextLeaseID) before the
// request is even sent to the event loop — not generated here — so
// the caller's own cancellation path can name it without having
// received a result (docs/v0.6.0-plan.md §15.3's lifecycle table).
func (r *leaseRegistry) Register(id, startSeq uint64) {
	r.live[id] = startSeq
	heap.Push(&r.heap, leaseHeapEntry{startSeq: startSeq, id: id})
}

// Release removes id from the live set. A no-op if id is not live
// (already released, or never registered) — matching ReadLease.Release's
// own idempotency contract.
func (r *leaseRegistry) Release(id uint64) {
	delete(r.live, id)
}

// Len returns the number of currently live leases.
func (r *leaseRegistry) Len() int { return len(r.live) }

// Min returns the minimum StartSeq among currently live leases, or
// (0, false) if none are live.
func (r *leaseRegistry) Min() (uint64, bool) {
	for r.heap.Len() > 0 {
		top := r.heap[0]
		if sq, ok := r.live[top.id]; ok && sq == top.startSeq {
			return top.startSeq, true
		}
		heap.Pop(&r.heap) // stale: already released
	}
	return 0, false
}

// ReadLease represents one live read transaction's claim on a StartSeq
// boundary (docs/v0.6.0-plan.md §15.3): registered at capture (inside
// handleReadIndex, in the same statement sequence that captures
// target, before the pendingRead is even appended — see handleReadIndex's
// own comment), and held for the lifetime of the transaction that owns
// it — a fundamentally different lifetime than the readGate slot,
// which is released the moment BeginReadIndex returns (§5.4). Every
// open transaction holding a ReadLease contributes to minLease, which
// floors the leader's proposed GC watermark (§13.4/§14.2) so this
// transaction's snapshot can never be reclaimed out from under it
// while this leader remains leader.
//
// A leaked lease (a caller that forgets to Release) stalls GC; it
// never makes GC unsafe — the fail-safe direction (§15.3) — and
// -read-lease-max-age force-expires an abandoned one (a liveness
// mechanism only, C2).
type ReadLease struct {
	n        *Node
	id       uint64
	startSeq uint64
	released atomic.Bool
}

// StartSeq is this lease's fixed StartSeq boundary.
func (l *ReadLease) StartSeq() uint64 { return l.startSeq }

// Release is idempotent and safe to call from any goroutine, any
// number of times, including after the Node has stopped (a no-op in
// that case — the registry it would have updated no longer matters).
func (l *ReadLease) Release() {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	select {
	case l.n.releaseLeaseCh <- l.id:
	case <-l.n.doneCh:
	}
}

// releaseLease is BeginReadIndex's own cancellation-path release
// (docs/v0.6.0-plan.md §15.3's lifecycle table: "the caller sends its
// leaseID on releaseLeaseCh before returning") — used when a lease was
// registered but the caller's ctx was canceled/the node stopped before
// the read resolved, so the caller never received a *ReadLease to call
// Release on itself. id is allocated by handleReadIndex before the
// request is sent, precisely so this path can name it without having
// received a result.
//
// Deliberately not select-guarded on the caller's own ctx: ctx being
// done is exactly why this call exists, so gating the send on it would
// intermittently skip the send (Go's select picks pseudo-randomly among
// already-ready cases) — n.doneCh is the only legitimate reason to give
// up, since a stopped node has no registry left to update.
func releaseLease(n *Node, id uint64) {
	select {
	case n.releaseLeaseCh <- id:
	case <-n.doneCh:
	}
}
