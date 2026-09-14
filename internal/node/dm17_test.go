package node

import (
	"context"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/transport"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// newDeterministicLeaderForDM17 builds a real, already-elected *Node
// leading voters (whichever raft.NodeID set is passed, always including
// "A" as leader), driven directly via Node.step/Node.core.Step with no
// run() goroutine — the same determinism-over-realism trade
// readindex_seq_test.go's newSingleLeaderNodeForTest makes, extended
// with real durable storage and a real FSM so DM-17's scenarios (which
// need genuine raft.Core.ProposeConfigChange commits, not just
// heartbeats) can actually persist and apply entries. A single test
// goroutine is the only thing ever calling into n, so every
// read/promote/remove/ack interleaving below is exact and reproducible
// rather than dependent on real transport/scheduling timing.
func newDeterministicLeaderForDM17(t *testing.T, voters []raft.NodeID) *Node {
	t.Helper()
	members := make([]raft.Member, len(voters))
	peerAddrs := make(map[raft.NodeID]string, len(voters))
	for i, p := range voters {
		members[i] = raft.Member{ID: p, Address: string(p) + ":0"}
		if p != "A" {
			peerAddrs[p] = freeAddr(t)
		}
	}
	rcfg := raft.Config{
		ID:                         "A",
		Bootstrap:                  raft.Configuration{Voters: members},
		ElectionTimeoutTicks:       10,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      2,
		Rand:                       zeroRand{},
	}
	core, err := raft.NewCore(rcfg, raft.HardState{}, nil)
	if err != nil {
		t.Fatalf("raft.NewCore: %v", err)
	}
	out := core.Step(raft.Input{Kind: raft.InputElectionTimeout})
	if out.PersistRequest == nil {
		t.Fatalf("election: no PersistRequest for vote persistence")
	}
	core.Step(raft.Input{Kind: raft.InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
	for _, p := range voters {
		if p == "A" {
			continue
		}
		core.Step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{Type: raft.MsgRequestVoteResponse, From: p, To: "A", Term: 1, VoteGranted: true}})
	}
	if core.Role() != raft.Leader {
		t.Fatalf("setup: Role() = %v, want Leader", core.Role())
	}

	tr, err := transport.New("A", freeAddr(t), peerAddrs)
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { tr.Close() })

	w, _, err := wal.Open(t.TempDir(), wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	st, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}

	n := &Node{
		// A very large SnapshotThreshold keeps applyCommitted's
		// unconditional maybeSnapshot call inside its own early-return
		// guard for every commit this test performs, so the nil snapMgr
		// below is never actually touched (mirrors
		// newSingleLeaderNodeForTest's own documented reasoning).
		cfg:               Config{ID: "A", Peers: voters, SnapshotThreshold: 1_000_000},
		core:              core,
		storage:           st,
		tr:                tr,
		ackSeq:            make(map[raft.NodeID]uint64, len(voters)+1),
		waiters:           make(map[raft.Index]waiter),
		peerGenerations:   make(map[raft.NodeID]uint32),
		clusterGeneration: 2, // membership changes require generation >= 2 (§8.2); DM-18 covers that gate itself
		readIndexCh:       make(chan readIndexReq),
		doneCh:            make(chan struct{}),
	}
	n.fsmachine.Store(fsm.New(mvcc.NewStore()))
	return n
}

// dm17NoOp commits one ordinary current-term entry so P1 (§2.2a) lets
// ProposeConfigChange run at all, then commits it via a genuine
// majority ack from the given voters (which must exclude "A"; A's own
// vote is implicit).
func dm17NoOp(t *testing.T, n *Node, ackFrom []raft.NodeID) {
	t.Helper()
	out := n.core.Step(raft.Input{Kind: raft.InputPropose, ProposeData: fsm.EncodeCommitTxn(fsm.CommitTxnCommand{
		RequestID: fsm.RequestID("dm17-noop"),
		TxnID:     1,
	})})
	if out.ProposalRejected {
		t.Fatalf("dm17NoOp: proposal rejected: %+v", out)
	}
	n.processOutput(out)
	dm17Ack(t, n, ackFrom)
}

// dm17Ack delivers a fresh, commit-and-freshness-bearing
// AppendEntriesResponse from each of froms, simulating that peer being
// fully caught up to n's current LastIndex — the single mechanism this
// file uses both to advance matchIndex (so commitIndex/step-down can
// follow, per raft.Core's own commit rule) and to satisfy the ReadIndex
// freshness proof (Message.Seq strictly greater than any requiredSeq
// captured so far, since Node.sentSeqCounter only ever increases).
func dm17Ack(t *testing.T, n *Node, froms []raft.NodeID) {
	t.Helper()
	for _, p := range froms {
		// n.sentSeqCounter itself (never +1): a real ack can only ever
		// echo a Seq this node has actually assigned to some already-
		// sent outbound request, so this is the freshest value that
		// remains realistic — anything higher would fabricate an ack
		// for a message never sent, artificially satisfying a later
		// read's freshness proof for free.
		n.step(raft.Input{Kind: raft.InputMessage, Message: raft.Message{
			Type: raft.MsgAppendEntriesResponse, From: p, To: "A", Term: n.core.CurrentTerm(),
			Success: true, MatchIndex: n.core.LastIndex(), Seq: n.sentSeqCounter,
		}})
	}
}

// dm17ProposeConfigChange proposes and appends (not necessarily
// commits — checkPendingReads reads Core.ActiveConfig(), which is
// already the append-time configuration, §2.2) one membership change,
// and runs it through processOutput so persistence and
// checkPendingReads both fire exactly as they would in production.
func dm17ProposeConfigChange(t *testing.T, n *Node, kind raft.MembershipChangeKind, requestID string, targetID raft.NodeID, targetAddr string) raft.Index {
	t.Helper()
	out, err := n.core.ProposeConfigChange(kind, requestID, targetID, targetAddr)
	if err != nil {
		t.Fatalf("ProposeConfigChange(%v, %s): %v", kind, targetID, err)
	}
	idx := out.PersistRequest.Entries[0].Index
	n.processOutput(out)
	return idx
}

// TestDM17Promote_NewVoterWithZeroAckSeqDoesNotCount regresses DM-17's
// first sub-case (§15, §4.2a row 1): a learner promoted to voter joins
// the quorum denominator immediately (append-time, §2.2) but must not
// be phantom-counted in the numerator merely for existing — its
// ackSeq is (and stays) 0 until it actually answers a fresh round.
func TestDM17Promote_NewVoterWithZeroAckSeqDoesNotCount(t *testing.T) {
	n := newDeterministicLeaderForDM17(t, []raft.NodeID{"A", "B", "C"})
	dm17NoOp(t, n, []raft.NodeID{"B", "C"}) // P1

	dm17ProposeConfigChange(t, n, raft.AddLearnerChange, "dm17-add", "D", "D:0")
	dm17Ack(t, n, []raft.NodeID{"B", "C"}) // let the learner-add itself commit
	dm17ProposeConfigChange(t, n, raft.PromoteToVoterChange, "dm17-promote", "D", "")

	if cfg := n.core.ActiveConfig(); !cfg.IsVoter("D") || cfg.Majority() != 3 {
		t.Fatalf("test setup: activeConfig = %+v, want D a voter and Majority()==3", cfg)
	}

	req := readIndexReq{resultCh: make(chan readResult, 1)}
	n.handleReadIndex(req)

	// Only B acks. Under the OLD 3-voter majority (2), self+B would
	// already satisfy it — the fact it does not here is the direct
	// proof that D (ackSeq still 0) is not being phantom-counted.
	dm17Ack(t, n, []raft.NodeID{"B"})
	if _, done := resultNow(t, req.resultCh); done {
		t.Fatal("read resolved on self+B alone once D became a voter — D's zero ackSeq must not have counted, but something did")
	}
	if len(n.pendingReads) != 1 {
		t.Fatalf("pendingReads = %+v, want the read to remain pending", n.pendingReads)
	}

	// C acks too: self+B+C=3 >= Majority()==3, a genuine majority that
	// does not rely on D at all.
	dm17Ack(t, n, []raft.NodeID{"C"})
	res, done := resultNow(t, req.resultCh)
	if !done {
		t.Fatal("read did not resolve once self+B+C reached the new 4-voter majority")
	}
	if res.err != nil {
		t.Fatalf("read result = %+v, want no error", res)
	}
}

// TestDM17Remove_RemovedVoterStopsCounting regresses DM-17's second
// sub-case (§15, §4.2a row 1): a voter whose fresh ack was already
// being counted toward a pending read's quorum must stop being counted
// the instant it is removed — checkPendingReads recomputes acked from
// the current activeConfig.Voters on every pass, never from a
// once-true snapshot, so a stale (already-fresh) ackSeq for a
// since-removed voter must not go on satisfying anything.
func TestDM17Remove_RemovedVoterStopsCounting(t *testing.T) {
	n := newDeterministicLeaderForDM17(t, []raft.NodeID{"A", "B", "C", "D"})
	dm17NoOp(t, n, []raft.NodeID{"B", "C"}) // P1; majority(4)=3, self+2

	if cfg := n.core.ActiveConfig(); cfg.Majority() != 3 {
		t.Fatalf("test setup: Majority() = %d, want 3", cfg.Majority())
	}

	req := readIndexReq{resultCh: make(chan readResult, 1)}
	n.handleReadIndex(req)

	// C acks fresh: self+C=2 < 3, correctly still pending.
	dm17Ack(t, n, []raft.NodeID{"C"})
	if _, done := resultNow(t, req.resultCh); done {
		t.Fatal("read resolved on self+C alone under a 4-voter majority of 3")
	}

	// Remove C. Majority drops to 2 (3 voters left: A,B,D).
	dm17ProposeConfigChange(t, n, raft.RemoveServerChange, "dm17-remove-c", "C", "")
	if cfg := n.core.ActiveConfig(); cfg.IsMember("C") || cfg.Majority() != 2 {
		t.Fatalf("test setup: activeConfig = %+v, want C removed and Majority()==2", cfg)
	}

	// If C's already-fresh ack still counted, self+C=2 >= 2 would
	// resolve the read right here, with neither B nor D ever
	// contacted. It must not.
	if _, done := resultNow(t, req.resultCh); done {
		t.Fatal("read resolved via a removed voter's stale-but-still-fresh ack — removal must stop it from counting")
	}
	if len(n.pendingReads) != 1 {
		t.Fatalf("pendingReads = %+v, want the read to remain pending", n.pendingReads)
	}

	// B, a still-current voter, acks: self+B=2 >= 2, a genuine majority
	// of the post-removal configuration.
	dm17Ack(t, n, []raft.NodeID{"B"})
	res, done := resultNow(t, req.resultCh)
	if !done {
		t.Fatal("read did not resolve once self+B reached the post-removal majority")
	}
	if res.err != nil {
		t.Fatalf("read result = %+v, want no error", res)
	}
}

// TestDM17SelfRemoval_TwoPhasesAgainstOneReadPlusPositive regresses
// DM-17's third sub-case (§15, §4.2a row 2/3, §23/G2/H3): a single
// BeginReadIndex call, issued while its leader is mid self-removal,
// observes two different things at two different layers without
// contradiction — the caller's own deadline expires first (row 3,
// "blocks to the deadline," never a phantom resolution), and the
// registered read underneath it — never re-issued — later fails
// cleanly with ErrLeadershipLost once the self-removal genuinely
// commits and this node steps down (row 2). A third, independent
// read against a fresh C_new-shaped leader demonstrates row 1 (a
// healthy read resolves, majority excluding the leader).
func TestDM17SelfRemoval_TwoPhasesAgainstOneReadPlusPositive(t *testing.T) {
	n := newDeterministicLeaderForDM17(t, []raft.NodeID{"A", "B", "C"})
	dm17NoOp(t, n, []raft.NodeID{"B", "C"}) // P1

	// The read must be issued and registered *before* the self-removal
	// is even proposed: handleReadIndex's own first check refuses any
	// new read once selfRemoved() is true (append-time effective, §2.2)
	// — which is exactly why this sub-case's read is "one whose
	// term/requiredSeq were captured before the self-removal" (§23/H3).
	// A read already registered in n.pendingReads is unaffected by that
	// gate, which only guards handleReadIndex's own entry point.
	//
	// Driven through the real channel-based BeginReadIndex (exercising
	// its actual ctx-based select, not handleReadIndex directly), with
	// this test's own goroutine standing in for run()'s event loop for
	// exactly one request — the only concurrency in this test, and
	// fully synchronized via the unbuffered readIndexCh handoff below.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	beginDone := make(chan struct {
		seq uint64
		err error
	}, 1)
	go func() {
		seq, err := n.BeginReadIndex(ctx)
		beginDone <- struct {
			seq uint64
			err error
		}{seq, err}
	}()
	req := <-n.readIndexCh
	n.handleReadIndex(req)
	preRemovalTarget := n.pendingReads[0].target
	if len(n.pendingReads) != 1 {
		t.Fatalf("pendingReads = %+v, want exactly one entry registered before the self-removal", n.pendingReads)
	}

	// Self-removal: append-time effective immediately (§2.2) — A is no
	// longer a Voter in its own activeConfig, though it remains Leader
	// (commit, not append, is the step-down boundary — §4.2).
	removeIdx := dm17ProposeConfigChange(t, n, raft.RemoveServerChange, "dm17-self-remove", "A", "")
	if cfg := n.core.ActiveConfig(); cfg.IsMember("A") {
		t.Fatal("A's activeConfig still includes A immediately after proposing its own removal")
	}
	if n.core.Role() != raft.Leader {
		t.Fatal("A stepped down before its own removal committed")
	}
	if len(n.pendingReads) != 1 || n.pendingReads[0].target != preRemovalTarget {
		t.Fatalf("pendingReads = %+v, want the pre-removal read still registered, untouched, target=%d", n.pendingReads, preRemovalTarget)
	}

	// Exactly one of the two remaining voters (C_new={B,C}) acks: the
	// removal entry has only one of the two matchIndex acks it needs
	// (§4.2's true-majority-of-C_new rule) and — independently — the
	// read has only self-excluded+B=1 < majority(C_new)==2, so it
	// cannot resolve either. Neither B is given a chance: this must
	// stay blocked all the way to Phase A's deadline.
	dm17Ack(t, n, []raft.NodeID{"B"})
	if n.core.CommitIndex() >= removeIdx {
		t.Fatal("test setup: the self-removal must not have committed yet (only one of C_new's two voters acked)")
	}
	if _, done := resultNow(t, req.resultCh); done {
		t.Fatal("the pending read resolved with only one of C_new's two voters acking — a phantom majority")
	}

	res := <-beginDone
	if res.err == nil {
		t.Fatalf("BeginReadIndex returned (seq=%d, err=%v), want ctx.Err() from its own deadline", res.seq, res.err)
	}
	if ctxErr := ctx.Err(); res.err != ctxErr {
		t.Fatalf("BeginReadIndex err = %v, want exactly ctx.Err() = %v", res.err, ctxErr)
	}
	// The read itself is untouched by the caller's deadline expiring:
	// still registered, still pending, same resultCh.
	if len(n.pendingReads) != 1 || n.pendingReads[0].resultCh != req.resultCh {
		t.Fatalf("pendingReads = %+v, want the original read still registered after only the caller's deadline expired", n.pendingReads)
	}

	// --- Phase B: the read's own clean failure, on that same read. ---
	// C now also acks: both of C_new's voters have matchIndex>=removeIdx,
	// a genuine majority of C_new not counting the leader — the removal
	// commits and A steps down exactly there.
	dm17Ack(t, n, []raft.NodeID{"C"})
	if got := n.core.CommitIndex(); got < removeIdx {
		t.Fatalf("self-removal did not commit once both of C_new's voters acked: commitIndex=%d, want >= %d", got, removeIdx)
	}
	if n.core.Role() != raft.Follower {
		t.Fatal("A did not step down exactly at its own removal's commit")
	}
	res2, done := resultNow(t, req.resultCh)
	if !done {
		t.Fatal("the original pending read never resolved once its leader stepped down")
	}
	if res2.err != ErrLeadershipLost {
		t.Fatalf("original pending read's result = %+v, want ErrLeadershipLost", res2)
	}

	// --- Positive phase: §4.2a row 1 against a healthy two-voter
	// leader shaped exactly like C_new after a self-removal (a fresh,
	// independent construction — this synthetic harness has no real
	// B/C node objects to continue the schedule onto; the leader ID
	// "A" here is just this harness's fixed convention, not a claim
	// that it is the same node — what matters is a two-voter majority
	// resolving via the other voter alone). ---
	positive := newDeterministicLeaderForDM17(t, []raft.NodeID{"A", "B"})
	dm17NoOp(t, positive, []raft.NodeID{"B"}) // P1
	preq := readIndexReq{resultCh: make(chan readResult, 1)}
	positive.handleReadIndex(preq)
	dm17Ack(t, positive, []raft.NodeID{"B"})
	pres, done := resultNow(t, preq.resultCh)
	if !done {
		t.Fatal("positive-phase read did not resolve once C_new's other voter acked")
	}
	if pres.err != nil {
		t.Fatalf("positive-phase read result = %+v, want no error", pres)
	}
}
