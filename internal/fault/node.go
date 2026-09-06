package fault

import (
	"fmt"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// Node wraps one real, unmodified raft.Core together with its
// MemoryStorage and the logical-timer bookkeeping a production driver
// (internal/node, Phase 5) would otherwise own, plus the crash/restart
// lifecycle docs/testing-strategy.md §3.1 requires: "the scheduler can
// stop a node (discarding its in-memory state, retaining its simulated
// durable state) and later restart it, driving it through the real
// recovery path."
//
// Node applies each Output.PersistRequest to its MemoryStorage
// synchronously, in the same call that produced it, then immediately
// feeds the resulting InputPersistenceComplete back into Core — Phase
// 4 does not model asynchronous disk latency or disk-fault injection
// (that is Phase 6/7 scope); what Phase 4 proves is Core's protocol
// logic under the documented persistence-gating contract, exercised
// against a real (if synchronous) Storage implementation, not a mock.
type Node struct {
	id  raft.NodeID
	cfg raft.Config

	storage *MemoryStorage
	core    *raft.Core

	electionArmed      bool
	electionTicksLeft  int
	heartbeatArmed     bool
	heartbeatTicksLeft int

	outbox    []raft.Message
	committed []raft.Entry

	crashed bool
}

func newNode(cfg raft.Config) *Node {
	n := &Node{id: cfg.ID, cfg: cfg, storage: NewMemoryStorage()}
	n.startFresh()
	return n
}

func (n *Node) startFresh() {
	core, err := raft.NewCore(n.cfg, raft.HardState{}, nil)
	if err != nil {
		panic(fmt.Sprintf("fault: NewCore: %v", err))
	}
	n.core = core
	n.armInitialElectionTimer()
}

func (n *Node) armInitialElectionTimer() {
	n.electionArmed = true
	n.electionTicksLeft = n.core.NewElectionTimeout()
	n.heartbeatArmed = false
}

// ID returns this node's identity.
func (n *Node) ID() raft.NodeID { return n.id }

// Core returns the node's current live Core, or nil if the node is
// currently crashed (see Crashed).
func (n *Node) Core() *raft.Core { return n.core }

// Storage returns the node's durable store. It remains valid and
// unaffected across Crash/Restart cycles.
func (n *Node) Storage() *MemoryStorage { return n.storage }

// Crashed reports whether this node is currently down (after Crash,
// before the matching Restart).
func (n *Node) Crashed() bool { return n.crashed }

// Crash discards this node's volatile state — its live Core (role,
// term cache, timers, undelivered outbox) — while leaving Storage,
// its durable state, completely untouched (docs/raft.md §5's
// persistent/volatile split). A crashed node ignores Step and Tick
// until Restart.
func (n *Node) Crash() {
	n.crashed = true
	n.core = nil
	n.outbox = nil
	n.electionArmed = false
	n.heartbeatArmed = false
}

// Restart reconstructs this node's Core from exactly what Storage
// durably holds (docs/recovery.md, docs/raft.md §5.1): currentTerm,
// votedFor, and the log are restored; commitIndex/appliedIndex are
// not — they start at 0 and must be re-established via a legitimate
// leader's AppendEntriesRPC or this node's own successful election,
// never trusted from a pre-crash cache.
func (n *Node) Restart() {
	hs, err := n.storage.InitialState()
	if err != nil {
		panic(fmt.Sprintf("fault: InitialState: %v", err))
	}
	last, err := n.storage.LastIndex()
	if err != nil {
		panic(fmt.Sprintf("fault: LastIndex: %v", err))
	}
	entries, err := n.storage.Entries(1, last+1)
	if err != nil {
		panic(fmt.Sprintf("fault: Entries: %v", err))
	}
	core, err := raft.NewCore(n.cfg, hs, entries)
	if err != nil {
		panic(fmt.Sprintf("fault: NewCore on restart: %v", err))
	}
	n.core = core
	n.crashed = false
	n.armInitialElectionTimer()
}

// CommittedEntries returns every entry this node has, over its
// lifetime (including across restarts), reported as newly committed.
// A re-derived commit of an already-recorded index reports the
// identical Entry value (STATE MACHINE SAFETY), so this list may
// contain benign duplicates after a restart — tests comparing content
// per index, not list length, are the intended usage.
func (n *Node) CommittedEntries() []raft.Entry {
	out := make([]raft.Entry, len(n.committed))
	copy(out, n.committed)
	return out
}

// Step delivers one Input to this node's Core (a no-op if the node is
// currently crashed) and applies the resulting Output, including
// synchronously satisfying any PersistRequest.
func (n *Node) Step(in raft.Input) {
	if n.crashed {
		return
	}
	n.applyOutput(n.core.Step(in))
}

func (n *Node) applyOutput(out raft.Output) {
	if out.ResetElectionTimer {
		n.electionArmed = true
		n.electionTicksLeft = out.ElectionTimeoutTicks
	}
	if out.ResetHeartbeatTimer {
		n.heartbeatArmed = true
		n.heartbeatTicksLeft = out.HeartbeatTimeoutTicks
	}
	n.outbox = append(n.outbox, out.Messages...)
	if len(out.CommittedEntries) > 0 {
		n.committed = append(n.committed, out.CommittedEntries...)
	}
	if out.PersistRequest != nil {
		if err := raft.ApplyPersistRequest(n.storage, out.PersistRequest); err != nil {
			panic(fmt.Sprintf("fault: MemoryStorage rejected a PersistRequest: %v", err))
		}
		ack := n.core.Step(raft.Input{Kind: raft.InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
		n.applyOutput(ack)
	}
}

// Tick advances this node's logical timers by one step
// (docs/testing-strategy.md §3.1's logical clock), firing
// InputElectionTimeout / InputHeartbeatTimeout exactly when their
// independently-tracked countdowns reach zero. A no-op while crashed.
func (n *Node) Tick() {
	if n.crashed {
		return
	}
	if n.electionArmed {
		n.electionTicksLeft--
		if n.electionTicksLeft <= 0 {
			n.electionArmed = false
			n.Step(raft.Input{Kind: raft.InputElectionTimeout})
		}
	}
	if n.heartbeatArmed {
		n.heartbeatTicksLeft--
		if n.heartbeatTicksLeft <= 0 {
			n.heartbeatArmed = false
			n.Step(raft.Input{Kind: raft.InputHeartbeatTimeout})
		}
	}
}

// DrainOutbox removes and returns every message queued for sending
// since the last DrainOutbox call.
func (n *Node) DrainOutbox() []raft.Message {
	out := n.outbox
	n.outbox = nil
	return out
}
