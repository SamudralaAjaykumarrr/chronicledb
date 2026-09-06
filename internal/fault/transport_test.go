package fault

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestTransport_DeliversInSendOrder proves DeliverEligible's ordering
// guarantee (docs/testing-strategy.md §3.1's "explicitly ordered
// queue"): messages become eligible in the order they were sent,
// absent any delay/partition control saying otherwise.
func TestTransport_DeliversInSendOrder(t *testing.T) {
	tr := NewTransport()
	tr.Send(raft.Message{From: "A", To: "B", Term: 1})
	tr.Send(raft.Message{From: "A", To: "B", Term: 2})
	tr.Send(raft.Message{From: "A", To: "B", Term: 3})

	got := tr.TakeEligible()
	if len(got) != 3 || got[0].Term != 1 || got[1].Term != 2 || got[2].Term != 3 {
		t.Fatalf("TakeEligible() = %+v, want terms [1 2 3] in order", got)
	}
	if more := tr.TakeEligible(); len(more) != 0 {
		t.Fatalf("expected no further eligible messages, got %+v", more)
	}
}

// TestTransport_IsolateNodeBlocksBothDirections proves IsolateNode
// blocks a node from both sending to and receiving from every peer.
func TestTransport_IsolateNodeBlocksBothDirections(t *testing.T) {
	tr := NewTransport()
	tr.IsolateNode("B")
	tr.Send(raft.Message{From: "A", To: "B"})
	tr.Send(raft.Message{From: "B", To: "A"})
	tr.Send(raft.Message{From: "A", To: "C"})

	got := tr.TakeEligible()
	if len(got) != 1 || got[0].To != "C" {
		t.Fatalf("TakeEligible() = %+v, want only the A->C message while B is isolated", got)
	}
	pending := tr.Pending()
	if len(pending) != 2 {
		t.Fatalf("expected the 2 messages touching B to remain pending, got %d", len(pending))
	}

	tr.HealNode("B")
	got2 := tr.TakeEligible()
	if len(got2) != 2 {
		t.Fatalf("expected both B-involving messages to become eligible after HealNode, got %d", len(got2))
	}
}

// TestTransport_PartitionIsDirectionAgnosticAndScoped proves Partition
// blocks both directions between the two groups while leaving
// intra-group links untouched.
func TestTransport_PartitionIsDirectionAgnosticAndScoped(t *testing.T) {
	tr := NewTransport()
	tr.Partition([]raft.NodeID{"A"}, []raft.NodeID{"B", "C"})

	tr.Send(raft.Message{From: "A", To: "B"})
	tr.Send(raft.Message{From: "B", To: "A"})
	tr.Send(raft.Message{From: "B", To: "C"}) // intra-majority-group: unaffected

	got := tr.TakeEligible()
	if len(got) != 1 || got[0].From != "B" || got[0].To != "C" {
		t.Fatalf("TakeEligible() = %+v, want only the intra-group B->C message", got)
	}

	tr.HealAll()
	rest := tr.TakeEligible()
	if len(rest) != 2 {
		t.Fatalf("expected both cross-partition messages eligible after HealAll, got %d", len(rest))
	}
}

// TestTransport_DropRemovesPermanently proves a dropped message is
// never delivered, even by a later HealAll.
func TestTransport_DropRemovesPermanently(t *testing.T) {
	tr := NewTransport()
	id := tr.Send(raft.Message{From: "A", To: "B"})
	if !tr.Drop(id) {
		t.Fatalf("Drop(%d) = false, want true", id)
	}
	tr.HealAll()
	if got := tr.TakeEligible(); len(got) != 0 {
		t.Fatalf("dropped message was delivered: %+v", got)
	}
	if _, ok := tr.Take(id); ok {
		t.Fatalf("Take() found a message that was already dropped")
	}
}

// TestTransport_DuplicateProducesIndependentCopy proves Duplicate
// creates a second, independently deliverable/droppable pending entry.
func TestTransport_DuplicateProducesIndependentCopy(t *testing.T) {
	tr := NewTransport()
	id := tr.Send(raft.Message{From: "A", To: "B", Term: 7})
	dupID, ok := tr.Duplicate(id)
	if !ok || dupID == id {
		t.Fatalf("Duplicate(%d) = (%d, %v), want a distinct new id", id, dupID, ok)
	}
	if !tr.Drop(id) {
		t.Fatalf("Drop of original failed")
	}
	got := tr.TakeEligible()
	if len(got) != 1 || got[0].Term != 7 {
		t.Fatalf("expected the duplicate to still be independently deliverable, got %+v", got)
	}
}

// TestTransport_DelayWithholdsUntilTick proves Delay withholds a
// message from DeliverEligible until the target logical tick.
func TestTransport_DelayWithholdsUntilTick(t *testing.T) {
	tr := NewTransport()
	id := tr.Send(raft.Message{From: "A", To: "B"})
	if !tr.Delay(id, 5) {
		t.Fatalf("Delay failed")
	}
	tr.advanceTick(4)
	if got := tr.TakeEligible(); len(got) != 0 {
		t.Fatalf("message delivered before its delay elapsed: %+v", got)
	}
	tr.advanceTick(5)
	if got := tr.TakeEligible(); len(got) != 1 {
		t.Fatalf("message not delivered once its delay elapsed")
	}
}

// TestTransport_TakeForcesDeliveryDuringPartition proves Take (unlike
// TakeEligible) is an explicit override that ignores partition state —
// used to construct precise stale-message scenarios (RF-8/RF-10).
func TestTransport_TakeForcesDeliveryDuringPartition(t *testing.T) {
	tr := NewTransport()
	tr.IsolateNode("B")
	id := tr.Send(raft.Message{From: "A", To: "B", Term: 3})
	msg, ok := tr.Take(id)
	if !ok || msg.Term != 3 {
		t.Fatalf("Take() = (%+v, %v), want the message despite isolation", msg, ok)
	}
}

// TestTransport_ReorderViaExplicitSelection proves a test can deliver
// pending messages out of send order by selecting ids explicitly.
func TestTransport_ReorderViaExplicitSelection(t *testing.T) {
	tr := NewTransport()
	id1 := tr.Send(raft.Message{From: "A", To: "B", Term: 1})
	id2 := tr.Send(raft.Message{From: "A", To: "B", Term: 2})

	msg2, ok := tr.Take(id2)
	if !ok || msg2.Term != 2 {
		t.Fatalf("expected to select and deliver the second message first")
	}
	msg1, ok := tr.Take(id1)
	if !ok || msg1.Term != 1 {
		t.Fatalf("expected the first message still deliverable afterward")
	}
}
