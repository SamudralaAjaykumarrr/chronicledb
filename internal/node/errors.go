package node

import (
	"errors"
	"fmt"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// NotLeaderError is returned by Propose/BeginReadIndex when this node
// is not currently the legitimate leader (docs/architecture.md §1:
// "a single leader per term accepts all writes"; this phase's brief:
// "Replicated mutation requests must only be accepted through the
// legitimate leader path"). Leader carries this node's best current
// knowledge of the cluster leader, if any — a hint only, never a
// guaranteed-current fact (the hinted node could itself have since lost
// leadership).
type NotLeaderError struct {
	Leader raft.NodeID
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "node: not leader (leader unknown)"
	}
	return fmt.Sprintf("node: not leader (leader hint: %s)", e.Leader)
}

var (
	// ErrLeadershipLost is returned to a pending Propose/BeginReadIndex
	// waiter when this node observes a higher term (steps down) while
	// the caller's request was still outstanding. This is an honest
	// "unknown, retry by RequestID" signal (docs/transactions.md §7),
	// never a false claim that the mutation failed: the underlying Raft
	// proposal may or may not still go on to commit through whichever
	// node is now the legitimate leader.
	ErrLeadershipLost = errors.New("node: leadership lost while request was pending; outcome unknown, retry by RequestID against the current leader")

	// ErrProposalSuperseded is returned to a pending Propose waiter
	// whose proposed log index was, before it could commit, overwritten
	// by a different leader's entry (Raft divergent-suffix repair,
	// docs/raft.md §3). Like ErrLeadershipLost, this is an honest
	// "unknown, retry by RequestID" signal, not a false failure — the
	// original request was never committed, so it is simply as if it
	// never happened; a retry is safe and will be evaluated fresh.
	ErrProposalSuperseded = errors.New("node: proposed entry was superseded by a different leader before it committed; retry by RequestID")

	// ErrNodeStopped is returned by Propose/BeginReadIndex once this
	// Node's event loop has shut down (Stop called, or a fatal local
	// error occurred), and to any request still pending at that moment.
	ErrNodeStopped = errors.New("node: node is stopped")
)
