package node

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/transport"
)

// zeroRand is a deterministic raft.Rand for tests that don't care about
// the exact jitter value, only that Step never touches a global RNG
// (mirrors internal/raft's own unexported test helper of the same
// name/shape, which this package cannot import).
type zeroRand struct{}

func (zeroRand) Intn(int) int { return 0 }

// newSingleLeaderNodeForTest builds a *Node whose raft.Core has already
// won an election as "A" in a 3-node cluster ("A","B","C") — via the
// same raw Core.Step handshake internal/raft's own tests use
// (core_test.go's setupLeader) — but WITHOUT ever calling Open or
// starting Node.run in a background goroutine. Every field the
// production code paths under test (step/handleReadIndex/
// checkPendingReads/processOutput, for a heartbeat-only Output with no
// PersistRequest/CommittedEntries/BecameLeader) actually touches is
// populated; everything else is left at its zero value, since it is
// provably unreached for this scenario.
//
// The whole point of building the Node this way, rather than through a
// real 3-node testCluster (as every other test in this package does),
// is determinism: with no run() goroutine and no real peers, this
// test's single goroutine is the only thing ever calling into Node, so
// it can construct an exact, reproducible interleaving of "process a
// stale AppendEntriesResponse" and "BeginReadIndex captures its
// freshness baseline" — the race that produced the flake in
// TestBeginReadIndexBlockedDuringMinorityPartition — instead of relying
// on real transport/scheduling timing to (rarely) land in that
// interleaving by chance.
func newSingleLeaderNodeForTest(t *testing.T) *Node {
	t.Helper()
	peers := []raft.NodeID{"A", "B", "C"}
	rcfg := raft.Config{
		ID:                         "A",
		Peers:                      peers,
		ElectionTimeoutTicks:       10,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      2,
		Rand:                       zeroRand{},
	}
	core, err := raft.NewCore(rcfg, raft.HardState{}, nil)
	if err != nil {
		t.Fatalf("raft.NewCore: %v", err)
	}
	// Win the election via raw Core.Step calls, exactly like
	// internal/raft's own setupLeader test helper — deliberately
	// bypassing Node.step/processOutput for this part, since a real
	// election's PersistRequest/BecameLeader side effects (WAL
	// persistence, proposeElectionNoOp) are irrelevant to, and would
	// otherwise require standing up, the ReadIndex freshness mechanism
	// this test actually targets.
	out := core.Step(raft.Input{Kind: raft.InputElectionTimeout})
	if out.PersistRequest == nil {
		t.Fatalf("election: no PersistRequest for vote persistence")
	}
	core.Step(raft.Input{Kind: raft.InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	core.Step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{Type: raft.MsgRequestVoteResponse, From: "B", To: "A", Term: 1, VoteGranted: true}})
	core.Step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{Type: raft.MsgRequestVoteResponse, From: "C", To: "A", Term: 1, VoteGranted: true}})
	if core.Role() != raft.Leader {
		t.Fatalf("setup: Role() = %v, want Leader", core.Role())
	}

	// A real Transport is required (processOutput's send loop calls
	// n.tr.Send unconditionally), but it never needs to reach a real
	// peer: Send's documented lossy-network behavior (internal/transport
	// transport.go) is to silently drop a message it cannot dial, which
	// is exactly "harmless" for a test that only cares about this
	// node's own bookkeeping, never about anything actually arriving at
	// "B" or "C".
	tr, err := transport.New("A", freeAddr(t), map[raft.NodeID]string{
		"B": freeAddr(t),
		"C": freeAddr(t),
	})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { tr.Close() })

	return &Node{
		// SnapshotThreshold must be non-zero: applyCommitted
		// unconditionally calls maybeSnapshot, and the zero value
		// ("use the package default," normally filled in by
		// Config.setDefaults during Open, which this helper
		// deliberately bypasses) would make maybeSnapshot's own
		// `appliedIndex-snapIdx < SnapshotThreshold` guard (0 < 0,
		// false) fall through into a real Manager.Create call against
		// this test's nil snapMgr.
		cfg:    Config{ID: "A", Peers: peers, SnapshotThreshold: 1},
		core:   core,
		tr:     tr,
		ackSeq: make(map[raft.NodeID]uint64, len(peers)),
	}
}

// resultNow does a non-blocking read of a readIndexReq's resultCh —
// appropriate here because every call in this test that could produce
// a result runs synchronously to completion on this test's own single
// goroutine (checkPendingReads either sends a result or it does not;
// there is no concurrent producer to wait for).
func resultNow(t *testing.T, ch chan readResult) (readResult, bool) {
	t.Helper()
	select {
	case res := <-ch:
		return res, true
	default:
		return readResult{}, false
	}
}

// TestReadIndexRejectsStaleAckProcessedAfterBaseline is a deterministic
// regression test for the exact hazard behind
// TestBeginReadIndexBlockedDuringMinorityPartition's rare (~1-in-10,
// only-under-heavy-parallel-load) failure: BeginReadIndex's freshness
// proof must reject a Success AppendEntriesResponse that merely
// *arrives at/is processed by* Node.step after a read's baseline was
// captured, unless it actually *answers a request sent* no earlier
// than that baseline. Before the fix (raft.Message.Seq plus
// Node.sentSeqCounter/ackSeq, replacing a purely local
// Node.ackCounter/lastAck "was any qualifying ack processed" counter),
// this test fails: a stale ack — one whose Seq corresponds to a
// heartbeat sent *before* handleReadIndex captured requiredSeq —
// satisfied the old counter-based check simply by being processed
// late, exactly reproducing an isolated/partitioned leader completing
// a ReadIndex it must not (docs/replication.md §4.1, ADR-0010).
//
// This is deliberately independent of real transport timing: it drives
// Node.step/handleReadIndex/checkPendingReads directly, in an exact,
// reproducible order, rather than hoping real scheduling/network
// timing lands in the hazardous interleaving.
func TestReadIndexRejectsStaleAckProcessedAfterBaseline(t *testing.T) {
	n := newSingleLeaderNodeForTest(t)

	// Round 1: an ordinary heartbeat to B and C. n.sentSeqCounter is now
	// 2 (Seq=1 to whichever of B/C processOutput iterates first, Seq=2
	// to the other — iteration order over cfg.Peers is irrelevant to
	// this test, only the two distinct assigned values are used below).
	n.step(raft.Input{Kind: raft.InputHeartbeatTimeout})
	if n.sentSeqCounter != 2 {
		t.Fatalf("after round 1: sentSeqCounter = %d, want 2", n.sentSeqCounter)
	}

	// B answers round 1 promptly, in order, the way a real network
	// almost always delivers it — establishing a legitimate ackSeq
	// baseline for B before any read is even issued.
	n.step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
		Type: raft.MsgAppendEntriesResponse, From: "B", To: "A", Term: 1, Success: true, Seq: 1,
	}})
	if n.ackSeq["B"] != 1 {
		t.Fatalf("ackSeq[B] = %d, want 1", n.ackSeq["B"])
	}

	// C's round-1 response (Seq=2) is the one that goes "missing" for
	// now — modeling exactly the real hazard: a Success ack that was
	// already sent/in flight before a partition/isolation began, but
	// whose processing by this node's single event-loop goroutine is
	// delayed until afterward (backlog, GC pause, scheduling
	// contention — internal/transport's own recv channel is a 256-deep
	// buffer, so "already received, not yet processed" is an entirely
	// ordinary, expected state, not a contrived one).

	// BeginReadIndex is issued now (before C's round-1 ack has been
	// processed): baseline requiredSeq = 2 (sentSeqCounter's value right
	// now), and this call forces round 2 (Seq=3 to one peer, Seq=4 to
	// the other), so sentSeqCounter becomes 4.
	req := readIndexReq{resultCh: make(chan readResult, 1)}
	n.handleReadIndex(req)
	if got, want := n.sentSeqCounter, uint64(4); got != want {
		t.Fatalf("after handleReadIndex: sentSeqCounter = %d, want %d", got, want)
	}
	if len(n.pendingReads) != 1 || n.pendingReads[0].requiredSeq != 2 {
		t.Fatalf("pendingReads = %+v, want one entry with requiredSeq=2", n.pendingReads)
	}
	if _, done := resultNow(t, req.resultCh); done {
		t.Fatalf("BeginReadIndex resolved before any round-2 ack arrived")
	}

	// C's round-1 ack (Seq=2) now finally gets processed — strictly
	// after requiredSeq(=2) was captured, exactly the hazardous
	// ordering. It must NOT satisfy the freshness proof: Seq=2 answers
	// a request sent before this read was issued, not after.
	n.step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
		Type: raft.MsgAppendEntriesResponse, From: "C", To: "A", Term: 1, Success: true, Seq: 2,
	}})
	if res, done := resultNow(t, req.resultCh); done {
		t.Fatalf("QUORUM-SAFETY/read-consistency violated: BeginReadIndex resolved (result=%+v) using a stale (Seq=2 <= requiredSeq=2) ack merely processed after the read was issued", res)
	}
	if len(n.pendingReads) != 1 {
		t.Fatalf("pendingReads = %+v, want the read to remain pending", n.pendingReads)
	}

	// A genuinely fresh ack for round 2 (Seq=4, whichever peer that
	// round assigned it to — B already supplied one qualifying vote of
	// its own for round 1 above, so this is the second, majority-
	// completing vote) must, unlike the stale one, resolve the read.
	n.step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
		Type: raft.MsgAppendEntriesResponse, From: "C", To: "A", Term: 1, Success: true, Seq: 4,
	}})
	res, done := resultNow(t, req.resultCh)
	if !done {
		t.Fatalf("BeginReadIndex did not resolve after a genuinely fresh (Seq=4 > requiredSeq=2) majority-completing ack")
	}
	if res.err != nil || res.startSeq != 0 {
		t.Fatalf("BeginReadIndex result = %+v, want startSeq=0 err=nil", res)
	}
}
