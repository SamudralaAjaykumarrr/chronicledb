package fault

import (
	"sync"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// PendingMessage is a snapshot of one message currently queued in a
// Transport, for test inspection.
type PendingMessage struct {
	ID          uint64
	Message     raft.Message
	ReadyAtTick int64 // 0 means "ready as soon as its link is reachable"
}

type linkKey struct{ From, To raft.NodeID }

// Transport is ChronicleDB's deterministic, in-memory message
// transport for the simulator (docs/testing-strategy.md §3.1): an
// explicitly ordered, explicitly controllable queue of in-flight
// messages, with first-class support for dropping, duplicating,
// delaying, reordering, and partitioning delivery. Nothing in Transport
// depends on real time or a real network; a test drives every delivery
// decision explicitly (or via Cluster's convenience helpers).
type Transport struct {
	mu     sync.Mutex
	nextID uint64
	// pending and order together give a stable, explicit send order
	// (docs/testing-strategy.md §3.1's "in-memory, explicitly ordered
	// queue per node-pair") while still letting a test select and
	// deliver any pending message out of order (reordering control).
	pending map[uint64]*PendingMessage
	order   []uint64

	isolated map[raft.NodeID]bool
	blocked  map[linkKey]bool

	tick int64
}

// NewTransport returns an empty, fully-connected Transport.
func NewTransport() *Transport {
	return &Transport{
		pending:  make(map[uint64]*PendingMessage),
		isolated: make(map[raft.NodeID]bool),
		blocked:  make(map[linkKey]bool),
	}
}

func (t *Transport) advanceTick(tick int64) {
	t.mu.Lock()
	t.tick = tick
	t.mu.Unlock()
}

// Send enqueues msg for later delivery and returns its pending id.
func (t *Transport) Send(msg raft.Message) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	t.pending[id] = &PendingMessage{ID: id, Message: msg}
	t.order = append(t.order, id)
	return id
}

func (t *Transport) reachableLocked(from, to raft.NodeID) bool {
	if from == to {
		return true
	}
	if t.isolated[from] || t.isolated[to] {
		return false
	}
	return !t.blocked[linkKey{from, to}]
}

// Pending returns every currently queued message, in send order.
func (t *Transport) Pending() []PendingMessage {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PendingMessage, 0, len(t.order))
	for _, id := range t.order {
		if pm, ok := t.pending[id]; ok {
			out = append(out, *pm)
		}
	}
	return out
}

// Take force-delivers (removes and returns) the pending message with
// the given id, regardless of the current link/partition/delay state —
// an explicit test override for constructing precise scenarios (e.g. a
// stale message delivered well after a partition heals).
func (t *Transport) Take(id uint64) (raft.Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pm, ok := t.pending[id]
	if !ok {
		return raft.Message{}, false
	}
	delete(t.pending, id)
	return pm.Message, true
}

// TakeEligible removes and returns, in send order, every pending
// message whose link is currently reachable and whose delay (if any)
// has elapsed. Messages that are not yet eligible remain queued.
func (t *Transport) TakeEligible() []raft.Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []raft.Message
	remaining := t.order[:0]
	for _, id := range t.order {
		pm, ok := t.pending[id]
		if !ok {
			continue
		}
		if pm.ReadyAtTick > t.tick || !t.reachableLocked(pm.Message.From, pm.Message.To) {
			remaining = append(remaining, id)
			continue
		}
		out = append(out, pm.Message)
		delete(t.pending, id)
	}
	t.order = append([]uint64(nil), remaining...)
	return out
}

// Drop permanently discards a pending message without delivering it.
func (t *Transport) Drop(id uint64) bool {
	_, ok := t.Take(id)
	return ok
}

// Duplicate re-enqueues a copy of the still-pending message id (a new,
// independent pending entry with its own id), modeling a network that
// delivers a packet twice.
func (t *Transport) Duplicate(id uint64) (uint64, bool) {
	t.mu.Lock()
	pm, ok := t.pending[id]
	var msg raft.Message
	if ok {
		msg = pm.Message
	}
	t.mu.Unlock()
	if !ok {
		return 0, false
	}
	return t.Send(msg), true
}

// Delay postpones delivery of the pending message id until the
// Cluster's logical tick reaches readyAtTick.
func (t *Transport) Delay(id uint64, readyAtTick int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	pm, ok := t.pending[id]
	if !ok {
		return false
	}
	pm.ReadyAtTick = readyAtTick
	return true
}

// IsolateNode fully disconnects id from every other node, in both
// directions, until HealNode or HealAll.
func (t *Transport) IsolateNode(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolated[id] = true
}

// HealNode reconnects a node previously isolated by IsolateNode.
func (t *Transport) HealNode(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.isolated, id)
}

// IsolateLink blocks delivery in the from->to direction only.
func (t *Transport) IsolateLink(from, to raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[linkKey{from, to}] = true
}

// HealLink reconnects a single direction previously blocked by
// IsolateLink (or by Partition).
func (t *Transport) HealLink(from, to raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, linkKey{from, to})
}

// Partition blocks every link between groupA and groupB, in both
// directions, leaving links within each group untouched — the
// canonical network-partition contract scenario
// (docs/replication.md §5).
func (t *Transport) Partition(groupA, groupB []raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			t.blocked[linkKey{a, b}] = true
			t.blocked[linkKey{b, a}] = true
		}
	}
}

// HealAll clears every isolation and partition control, fully
// reconnecting the simulated network.
func (t *Transport) HealAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolated = make(map[raft.NodeID]bool)
	t.blocked = make(map[linkKey]bool)
}
