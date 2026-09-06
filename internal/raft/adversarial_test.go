package raft

import "testing"

// This file is Phase 10's targeted adversarial Raft-core testing
// (docs/roadmap.md Phase 10 "ADVERSARIAL RAFT TESTING": "InstallSnapshot
// followed by stale AppendEntries"). handleAppendEntriesRequest's
// msg.PrevLogIndex < c.snapshotIndex branch (core.go) already documents
// exactly this scenario in its own comment — a stale in-flight
// AppendEntries sent just before this node installed a snapshot — but
// had no direct unit test before this phase; the cluster-level version
// of the same scenario is internal/fault/adversarial_test.go's
// TestChaos_StaleAppendEntriesAfterSnapshotInstall.

// TestStaleAppendEntriesBelowSnapshotBoundaryReportsMatchIndexWithoutMutation
// proves a follower receiving an AppendEntriesRequest whose
// PrevLogIndex is strictly below its own snapshot boundary (a message
// that predates a snapshot this node has since installed) neither
// panics nor mutates any state: it reports Success with
// MatchIndex=SnapshotIndex so the leader can advance past it, per
// SNAPSHOT-SAFETY and RECOVERY-NON-INVENTION.
func TestStaleAppendEntriesBelowSnapshotBoundaryReportsMatchIndexWithoutMutation(t *testing.T) {
	c, err := NewCoreFromSnapshot(testConfig("A", threePeers()), HardState{CurrentTerm: 5}, 10, 3, []Entry{
		{Index: 11, Term: 5, Data: []byte("x")},
	})
	if err != nil {
		t.Fatalf("NewCoreFromSnapshot: %v", err)
	}
	beforeLast := c.LastIndex()
	beforeCommit := c.CommitIndex()
	beforeSnap := c.SnapshotIndex()

	// A leader's AppendEntries built from a view of this follower that
	// predates the snapshot it has since installed: PrevLogIndex=3 is
	// well below the current SnapshotIndex=10.
	out := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "B", To: "A", Term: 5,
		PrevLogIndex: 3, PrevLogTerm: 2,
		Entries:      []Entry{{Index: 4, Term: 2, Data: []byte("stale")}},
		LeaderCommit: 3,
	}})

	if c.LastIndex() != beforeLast || c.CommitIndex() != beforeCommit || c.SnapshotIndex() != beforeSnap {
		t.Fatalf("stale AppendEntries below the snapshot boundary must never mutate state: last=%d(want %d) commit=%d(want %d) snap=%d(want %d)",
			c.LastIndex(), beforeLast, c.CommitIndex(), beforeCommit, c.SnapshotIndex(), beforeSnap)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("got %d response messages, want exactly 1", len(out.Messages))
	}
	resp := out.Messages[0]
	if resp.Type != MsgAppendEntriesResponse || !resp.Success || resp.MatchIndex != c.SnapshotIndex() {
		t.Fatalf("response = %+v, want Success=true MatchIndex=%d", resp, c.SnapshotIndex())
	}
	if out.PersistRequest != nil {
		t.Fatalf("a state-unchanged stale message must not request any durable persistence, got %+v", out.PersistRequest)
	}

	// The node must continue operating normally afterward: a legitimate
	// AppendEntries starting exactly at the snapshot boundary is
	// accepted and does mutate state.
	out2 := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "B", To: "A", Term: 5,
		PrevLogIndex: 10, PrevLogTerm: 3,
		Entries:      []Entry{{Index: 11, Term: 5, Data: []byte("x")}, {Index: 12, Term: 5, Data: []byte("y")}},
		LeaderCommit: 11,
	}})
	if c.LastIndex() != 12 {
		t.Fatalf("legitimate AppendEntries after the stale one was rejected: LastIndex=%d, want 12", c.LastIndex())
	}
	foundAck := false
	for _, m := range out2.Messages {
		if m.Type == MsgAppendEntriesResponse && m.Success {
			foundAck = true
		}
	}
	if !foundAck && out2.PersistRequest == nil {
		t.Fatalf("expected either an immediate ack or a pending persist request for the legitimate AppendEntries, got %+v", out2)
	}
}

// TestStaleAppendEntriesExactlyAtSnapshotBoundaryIsAccepted covers the
// boundary condition itself (PrevLogIndex == SnapshotIndex, not
// strictly below it) is handled by the ordinary matching path, not the
// stale-snapshot short-circuit — both must agree the entry is
// accepted.
func TestStaleAppendEntriesExactlyAtSnapshotBoundaryIsAccepted(t *testing.T) {
	c, err := NewCoreFromSnapshot(testConfig("A", threePeers()), HardState{CurrentTerm: 2}, 5, 1, nil)
	if err != nil {
		t.Fatalf("NewCoreFromSnapshot: %v", err)
	}
	out := c.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgAppendEntriesRequest, From: "B", To: "A", Term: 2,
		PrevLogIndex: 5, PrevLogTerm: 1,
		Entries:      []Entry{{Index: 6, Term: 2, Data: []byte("z")}},
		LeaderCommit: 6,
	}})
	if c.LastIndex() != 6 {
		t.Fatalf("LastIndex = %d, want 6", c.LastIndex())
	}
	if len(out.Messages) == 0 && out.PersistRequest == nil {
		t.Fatalf("expected a response or a pending persist request, got neither: %+v", out)
	}
}
