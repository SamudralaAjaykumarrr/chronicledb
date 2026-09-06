package raft

import "testing"

// FuzzAppendEntriesReconciliation feeds arbitrary (including
// malformed-by-a-real-leader's-standards) AppendEntriesRequest shapes
// at a freshly constructed follower Core and asserts Step never panics
// except via the one documented, intentional safety-net panic guarding
// against truncating an already-committed entry — which a single
// isolated call from a fresh Core (commitIndex always 0 beforehand)
// can never legitimately trigger. This targets docs/raft.md §3's log
// conflict repair, the area most exposed to off-by-one errors.
func FuzzAppendEntriesReconciliation(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint8(0), uint64(1), uint64(1))
	f.Add(uint64(3), uint64(2), uint8(4), uint64(5), uint64(1))
	f.Add(uint64(10), uint64(1), uint8(1), uint64(1), uint64(9))

	f.Fuzz(func(t *testing.T, prevIndex, prevTerm uint64, numEntries uint8, term, entryTerm uint64) {
		peers := []NodeID{"F", "L", "X"}
		c, err := NewCore(testConfig("F", peers), HardState{}, nil)
		if err != nil {
			t.Fatal(err)
		}

		n := int(numEntries % 8)
		entries := make([]Entry, n)
		for i := 0; i < n; i++ {
			entries[i] = Entry{Index: Index(prevIndex) + 1 + Index(i), Term: Term(entryTerm)}
		}

		out := c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgAppendEntriesRequest, From: "L", To: "F",
			Term: Term(term), PrevLogIndex: Index(prevIndex), PrevLogTerm: Term(prevTerm),
			Entries: entries,
		}})
		if out.PersistRequest != nil {
			c.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
		}
	})
}

// FuzzRequestVoteHandling feeds arbitrary RequestVoteRequest shapes at
// a freshly constructed voter Core and asserts Step never panics and
// never grants a vote twice within the fuzzed sequence for the same
// term to two different candidates (RAFT-ELECTION-SAFETY's enabling
// rule, re-checked under adversarial input rather than a scripted
// scenario).
func FuzzRequestVoteHandling(f *testing.F) {
	f.Add(uint64(1), "B", uint64(0), uint64(0))
	f.Add(uint64(5), "C", uint64(3), uint64(2))

	f.Fuzz(func(t *testing.T, term uint64, candidate string, lastLogIndex, lastLogTerm uint64) {
		if candidate == "" || candidate == "F" {
			candidate = "B" // must be a real, distinct peer to be meaningful
		}
		peers := []NodeID{"F", "B", "C"}
		c, err := NewCore(testConfig("F", peers), HardState{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgRequestVoteRequest, From: NodeID(candidate), To: "F",
			Term: Term(term), LastLogIndex: Index(lastLogIndex), LastLogTerm: Term(lastLogTerm),
		}})
		if out.PersistRequest != nil {
			ack := c.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
			for _, m := range ack.Messages {
				if m.VoteGranted && c.VotedFor() != m.To {
					t.Fatalf("released a vote grant to %s but VotedFor() = %q", m.To, c.VotedFor())
				}
			}
		}
		// A second, different candidate must never also be granted the
		// same term's vote.
		other := NodeID("C")
		if NodeID(candidate) == other {
			other = "B"
		}
		out2 := c.Step(Input{Kind: InputMessage, Message: Message{
			Type: MsgRequestVoteRequest, From: other, To: "F", Term: Term(term), LastLogIndex: Index(lastLogIndex), LastLogTerm: Term(lastLogTerm),
		}})
		if out2.PersistRequest != nil {
			ack2 := c.Step(Input{Kind: InputPersistenceComplete, PersistSeq: out2.PersistRequest.Seq})
			for _, m := range ack2.Messages {
				if m.VoteGranted && c.VotedFor() == NodeID(candidate) {
					t.Fatalf("granted votes to both %s and %s in term %d", candidate, other, term)
				}
			}
		}
	})
}
