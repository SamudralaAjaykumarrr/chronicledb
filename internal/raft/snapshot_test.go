package raft

import "testing"

// --- NewCoreFromSnapshot ---

func TestNewCoreFromSnapshotStartsCommitAndAppliedAtBoundary(t *testing.T) {
	c, err := NewCoreFromSnapshot(testConfig("A", threePeers()), HardState{CurrentTerm: 2}, 5, 1, []Entry{{Index: 6, Term: 2, Data: []byte("x")}})
	if err != nil {
		t.Fatalf("NewCoreFromSnapshot: %v", err)
	}
	if c.CommitIndex() != 5 || c.AppliedIndex() != 5 {
		t.Fatalf("CommitIndex/AppliedIndex = %d/%d, want 5/5", c.CommitIndex(), c.AppliedIndex())
	}
	if c.SnapshotIndex() != 5 || c.SnapshotTerm() != 1 {
		t.Fatalf("SnapshotIndex/Term = %d/%d, want 5/1", c.SnapshotIndex(), c.SnapshotTerm())
	}
	if c.LastIndex() != 6 || c.LastLogTerm() != 2 {
		t.Fatalf("LastIndex/LastLogTerm = %d/%d, want 6/2", c.LastIndex(), c.LastLogTerm())
	}
	if _, ok := c.EntryAt(5); ok {
		t.Fatal("EntryAt(5) (the snapshot boundary itself) must not be reported as a live entry")
	}
	e, ok := c.EntryAt(6)
	if !ok || e.Term != 2 || string(e.Data) != "x" {
		t.Fatalf("EntryAt(6) = %+v ok=%v, want the real entry", e, ok)
	}
}

func TestNewCoreFromSnapshotRejectsNonContiguousEntries(t *testing.T) {
	if _, err := NewCoreFromSnapshot(testConfig("A", threePeers()), HardState{}, 5, 1, []Entry{{Index: 7, Term: 1}}); err == nil {
		t.Fatal("expected an error for entries not starting at snapshotIndex+1")
	}
}

func TestNewCoreZeroSnapshotMatchesOriginalNewCore(t *testing.T) {
	c, err := NewCoreFromSnapshot(testConfig("A", threePeers()), HardState{}, 0, 0, nil)
	if err != nil {
		t.Fatalf("NewCoreFromSnapshot: %v", err)
	}
	if c.CommitIndex() != 0 || c.AppliedIndex() != 0 || c.SnapshotIndex() != 0 || c.LastIndex() != 0 {
		t.Fatalf("a zero-snapshot Core should be indistinguishable from NewCore's own zero state, got commit=%d applied=%d snap=%d last=%d",
			c.CommitIndex(), c.AppliedIndex(), c.SnapshotIndex(), c.LastIndex())
	}
}

// --- Core.Compact (self-initiated local compaction, docs/snapshots.md §8) ---

func TestCompactAdvancesSnapshotBoundaryAndTrimsLog(t *testing.T) {
	c, err := NewCore(testConfig("A", threePeers()), HardState{CurrentTerm: 1}, []Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.commitIndex = 3
	c.appliedIndex = 3

	if !c.Compact(2) {
		t.Fatal("Compact(2) = false, want true")
	}
	if c.SnapshotIndex() != 2 || c.SnapshotTerm() != 1 {
		t.Fatalf("SnapshotIndex/Term = %d/%d, want 2/1", c.SnapshotIndex(), c.SnapshotTerm())
	}
	if _, ok := c.EntryAt(1); ok {
		t.Fatal("EntryAt(1) should be gone after compacting past it")
	}
	if _, ok := c.EntryAt(2); ok {
		t.Fatal("EntryAt(2) (the new boundary itself) must not be reported as a live entry")
	}
	e, ok := c.EntryAt(3)
	if !ok || e.Term != 2 || string(e.Data) != "c" {
		t.Fatalf("EntryAt(3) after compaction = %+v ok=%v, want the original entry 3 intact", e, ok)
	}
	if c.LastIndex() != 3 {
		t.Fatalf("LastIndex() = %d, want 3 (unaffected by compaction)", c.LastIndex())
	}
}

func TestCompactRejectsBeyondAppliedOrAtOrBehindBoundary(t *testing.T) {
	c, err := NewCore(testConfig("A", threePeers()), HardState{CurrentTerm: 1}, []Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.commitIndex = 2
	c.appliedIndex = 2

	if c.Compact(3) {
		t.Fatal("Compact beyond appliedIndex must be rejected")
	}
	if c.Compact(0) {
		t.Fatal("Compact at or behind the current snapshotIndex (0) must be rejected")
	}
	if !c.Compact(2) {
		t.Fatal("Compact(2) at appliedIndex should succeed")
	}
	if c.Compact(2) {
		t.Fatal("repeating Compact at the same boundary must be a no-op returning false")
	}
}

// TestAppendEntriesMessageSendsInstallSnapshotWhenPeerBehindSnapshot proves
// SN-5's leader-side trigger: once a peer's nextIndex has fallen to or
// below this leader's own (locally compacted) snapshotIndex, the next
// replication message is an InstallSnapshotRequest rather than an
// AppendEntriesRequest the leader can no longer honor.
func TestAppendEntriesMessageSendsInstallSnapshotWhenPeerBehindSnapshot(t *testing.T) {
	peers := threePeers()
	c, err := NewCore(testConfig("A", peers), HardState{CurrentTerm: 1}, []Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.role = Leader
	c.commitIndex = 3
	c.appliedIndex = 3
	if !c.Compact(2) {
		t.Fatal("Compact(2) should succeed")
	}
	c.nextIndex["B"] = 1 // B still thinks it needs entries this leader has already compacted away

	msg := c.appendEntriesMessage("B")
	if msg.Type != MsgInstallSnapshotRequest {
		t.Fatalf("appendEntriesMessage = %+v, want MsgInstallSnapshotRequest", msg)
	}
	if msg.LastIncludedIndex != 2 || msg.LastIncludedTerm != 1 {
		t.Fatalf("InstallSnapshotRequest boundary = %d/%d, want 2/1", msg.LastIncludedIndex, msg.LastIncludedTerm)
	}
	if len(msg.SnapshotData) != 0 {
		t.Fatalf("Core must never carry snapshot bytes itself, got %d bytes", len(msg.SnapshotData))
	}
}

// --- handleInstallSnapshotRequest (follower side, docs/snapshots.md §7) ---

func TestHandleInstallSnapshotRequestInstallsAndAcks(t *testing.T) {
	f := mustNewCore(t, "F", []NodeID{"F", "L", "X"})
	out := f.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotRequest, From: "L", To: "F", Term: 1,
		LastIncludedIndex: 5, LastIncludedTerm: 1, SnapshotData: []byte("opaque"),
	}})
	if out.PersistRequest == nil {
		t.Fatal("expected a PersistRequest for the new term's HardState")
	}
	f.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})

	if f.SnapshotIndex() != 5 || f.SnapshotTerm() != 1 {
		t.Fatalf("SnapshotIndex/Term = %d/%d, want 5/1", f.SnapshotIndex(), f.SnapshotTerm())
	}
	if f.CommitIndex() != 5 || f.AppliedIndex() != 5 {
		t.Fatalf("CommitIndex/AppliedIndex = %d/%d, want both 5", f.CommitIndex(), f.AppliedIndex())
	}
	if f.LastIndex() != 5 {
		t.Fatalf("LastIndex() = %d, want 5 (empty log past the new boundary)", f.LastIndex())
	}
	if len(out.Messages) != 1 {
		t.Fatalf("expected exactly one response message, got %+v", out.Messages)
	}
	resp := out.Messages[0]
	if resp.Type != MsgInstallSnapshotResponse || !resp.Success || resp.MatchIndex != 5 {
		t.Fatalf("response = %+v, want Success MatchIndex=5", resp)
	}
}

func TestHandleInstallSnapshotRequestStaleTermRejected(t *testing.T) {
	f := mustNewCore(t, "F", []NodeID{"F", "L", "X"})
	for i := 0; i < 5; i++ {
		f.Step(Input{Kind: InputElectionTimeout})
	}
	currentTerm := f.CurrentTerm()

	out := f.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotRequest, From: "L", To: "F", Term: currentTerm - 1,
		LastIncludedIndex: 5, LastIncludedTerm: 1,
	}})
	if len(out.Messages) != 1 || out.Messages[0].Success || out.Messages[0].Term != currentTerm {
		t.Fatalf("stale-term InstallSnapshot must be rejected with the current term, got %+v", out.Messages)
	}
	if f.SnapshotIndex() != 0 {
		t.Fatalf("SnapshotIndex must remain unchanged on stale-term rejection, got %d", f.SnapshotIndex())
	}
}

// TestHandleInstallSnapshotRequestAlreadyCoveredAcksIdempotently proves the
// "stale or duplicate" branch of docs/snapshots.md §7: a boundary at or
// behind what this node already has is acknowledged idempotently
// (reporting this node's own boundary, not the stale request's) without
// disturbing existing state.
func TestHandleInstallSnapshotRequestAlreadyCoveredAcksIdempotently(t *testing.T) {
	f := mustNewCore(t, "F", []NodeID{"F", "L", "X"})
	out := f.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotRequest, From: "L", To: "F", Term: 1, LastIncludedIndex: 5, LastIncludedTerm: 1,
	}})
	f.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})

	out2 := f.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotRequest, From: "L", To: "F", Term: 1, LastIncludedIndex: 3, LastIncludedTerm: 1,
	}})
	if out2.PersistRequest != nil {
		t.Fatalf("an already-covered InstallSnapshot must not require a fresh persist, got %+v", out2.PersistRequest)
	}
	if len(out2.Messages) != 1 || !out2.Messages[0].Success || out2.Messages[0].MatchIndex != 5 {
		t.Fatalf("response = %+v, want idempotent Success MatchIndex=5 (this node's own boundary)", out2.Messages)
	}
	if f.SnapshotIndex() != 5 {
		t.Fatalf("SnapshotIndex must remain at 5, got %d", f.SnapshotIndex())
	}
}

// --- handleInstallSnapshotResponse (leader side) ---

func TestHandleInstallSnapshotResponseAdvancesReplicationAndCommit(t *testing.T) {
	peers := threePeers()
	l, err := NewCore(testConfig("A", peers), HardState{CurrentTerm: 1}, []Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	l.role = Leader
	l.appliedIndex = 3
	l.commitIndex = 3
	if !l.Compact(2) {
		t.Fatal("Compact(2) should succeed")
	}
	l.nextIndex["B"] = 1

	out := l.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotResponse, From: "B", To: "A", Term: 1, Success: true, MatchIndex: 2,
	}})
	if l.MatchIndexOf("B") != 2 {
		t.Fatalf("MatchIndexOf(B) = %d, want 2", l.MatchIndexOf("B"))
	}
	if len(out.Messages) != 1 || out.Messages[0].Type != MsgAppendEntriesRequest || out.Messages[0].To != "B" {
		t.Fatalf("expected a follow-up AppendEntriesRequest to B, got %+v", out.Messages)
	}
	// appendEntriesMessage optimistically advances nextIndex past what it
	// just sent (entry 3), to lastIndex()+1.
	if l.NextIndexOf("B") != 4 {
		t.Fatalf("NextIndexOf(B) = %d, want 4", l.NextIndexOf("B"))
	}
}

func TestHandleInstallSnapshotResponseRejectionRetriedSeparately(t *testing.T) {
	peers := threePeers()
	l, err := NewCore(testConfig("A", peers), HardState{CurrentTerm: 1}, []Entry{{Index: 1, Term: 1}})
	if err != nil {
		t.Fatal(err)
	}
	l.role = Leader
	l.appliedIndex = 1
	l.commitIndex = 1
	l.Compact(1)
	l.nextIndex["B"] = 1

	out := l.Step(Input{Kind: InputMessage, Message: Message{
		Type: MsgInstallSnapshotResponse, From: "B", To: "A", Term: 1, Success: false,
	}})
	if len(out.Messages) != 0 {
		t.Fatalf("a rejected InstallSnapshotResponse should produce no immediate follow-up (the driver retries on its own next round), got %+v", out.Messages)
	}
	if l.MatchIndexOf("B") != 0 || l.NextIndexOf("B") != 1 {
		t.Fatalf("a rejection must not advance matchIndex/nextIndex, got match=%d next=%d", l.MatchIndexOf("B"), l.NextIndexOf("B"))
	}
}
