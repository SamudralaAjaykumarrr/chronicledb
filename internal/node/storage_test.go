package node

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

func openTestWAL(t *testing.T, dir string) *wal.WAL {
	t.Helper()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	return w
}

func TestWALStorageInitialStateEmpty(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	defer w.Close()

	s, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	hs, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if hs != (raft.HardState{}) {
		t.Fatalf("InitialState on fresh storage = %+v, want zero value", hs)
	}
	last, err := s.LastIndex()
	if err != nil || last != 0 {
		t.Fatalf("LastIndex = %d err=%v, want 0", last, err)
	}
}

func TestWALStorageAppendEntriesAndReadBack(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	defer w.Close()

	s, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}

	entries := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	}
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	last, err := s.LastIndex()
	if err != nil || last != 3 {
		t.Fatalf("LastIndex = %d err=%v, want 3", last, err)
	}
	got, err := s.Entries(1, 4)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Entries returned %d, want 3", len(got))
	}
	for i, e := range got {
		if e.Index != entries[i].Index || e.Term != entries[i].Term || string(e.Data) != string(entries[i].Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, e, entries[i])
		}
	}

	// Partial range.
	partial, err := s.Entries(2, 3)
	if err != nil || len(partial) != 1 || partial[0].Index != 2 {
		t.Fatalf("Entries(2,3) = %+v err=%v, want single entry index 2", partial, err)
	}
}

func TestWALStorageSetHardStateRoundTrips(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	defer w.Close()

	s, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	hs := raft.HardState{CurrentTerm: 7, VotedFor: raft.NodeID("nodeB")}
	if err := s.SetHardState(hs); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	got, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if got != hs {
		t.Fatalf("InitialState after SetHardState = %+v, want %+v", got, hs)
	}
}

// TestWALStorageSurvivesRestart proves the full Raft persistent-state
// contract (docs/raft.md §5): currentTerm, votedFor, and log entries
// all reconstruct correctly from a fresh WALStorage built over a
// reopened WAL, exactly as docs/recovery.md requires.
func TestWALStorageSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	s, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	if err := s.SetHardState(raft.HardState{CurrentTerm: 3, VotedFor: "nodeA"}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	entries := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("x")},
		{Index: 2, Term: 2, Data: []byte("y")},
	}
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := openTestWAL(t, dir)
	defer w2.Close()
	s2, err := OpenWALStorage(w2)
	if err != nil {
		t.Fatalf("OpenWALStorage after restart: %v", err)
	}
	hs, err := s2.InitialState()
	if err != nil {
		t.Fatalf("InitialState after restart: %v", err)
	}
	if hs.CurrentTerm != 3 || hs.VotedFor != "nodeA" {
		t.Fatalf("InitialState after restart = %+v, want term=3 votedFor=nodeA", hs)
	}
	last, err := s2.LastIndex()
	if err != nil || last != 2 {
		t.Fatalf("LastIndex after restart = %d err=%v, want 2", last, err)
	}
	got, err := s2.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries after restart: %v", err)
	}
	for i, e := range got {
		if e.Index != entries[i].Index || e.Term != entries[i].Term || string(e.Data) != string(entries[i].Data) {
			t.Fatalf("entry %d after restart = %+v, want %+v", i, e, entries[i])
		}
	}
}

// TestWALStorageAppendFailurePropagatesNotTreatedAsSuccess injects a
// real (not mocked) durability failure by closing the underlying WAL
// out from under WALStorage, then proves Append/SetHardState report the
// failure as an error — never silently as success — and never update
// the in-memory mirror on a failed write (docs/failure-model.md §1.8;
// this phase's brief's "storage append failure" / "fsync failure"
// obligations). The underlying internal/wal.WAL.Sync/Append failure-
// propagation contract itself is already proven directly by
// internal/wal's own TestSyncFailurePropagatesNotTreatedAsSuccess; this
// test is specifically about the WALStorage adapter not swallowing or
// masking that failure.
func TestWALStorageAppendFailurePropagatesNotTreatedAsSuccess(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	s, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	if err := s.Append([]raft.Entry{{Index: 1, Term: 1, Data: []byte("a")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("closing underlying WAL for fault injection: %v", err)
	}

	if err := s.Append([]raft.Entry{{Index: 2, Term: 1, Data: []byte("b")}}); err == nil {
		t.Fatal("Append after underlying WAL closed: expected error, got nil")
	}
	last, err := s.LastIndex()
	if err != nil || last != 1 {
		t.Fatalf("LastIndex after failed Append = %d err=%v, want unchanged at 1", last, err)
	}

	if err := s.SetHardState(raft.HardState{CurrentTerm: 5, VotedFor: "x"}); err == nil {
		t.Fatal("SetHardState after underlying WAL closed: expected error, got nil")
	}
	hs, err := s.InitialState()
	if err != nil || hs != (raft.HardState{}) {
		t.Fatalf("InitialState after failed SetHardState = %+v err=%v, want unchanged zero value", hs, err)
	}
}

// TestWALStorageTruncateThenAppendMirrorsRaftCore proves WALStorage
// correctly implements the exact Truncate-then-Append sequence
// raft.ApplyPersistRequest performs during divergent-suffix repair
// (docs/raft.md §3), and that the result survives a restart.
func TestWALStorageTruncateThenAppendMirrorsRaftCore(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	s, err := OpenWALStorage(w)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	if err := s.Append([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b-stale")},
		{Index: 3, Term: 1, Data: []byte("c-stale")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	pr := &raft.PersistRequest{
		TruncateFrom: 2,
		Entries: []raft.Entry{
			{Index: 2, Term: 2, Data: []byte("b-new")},
		},
	}
	if err := raft.ApplyPersistRequest(s, pr); err != nil {
		t.Fatalf("ApplyPersistRequest: %v", err)
	}

	last, err := s.LastIndex()
	if err != nil || last != 2 {
		t.Fatalf("LastIndex after repair = %d err=%v, want 2", last, err)
	}
	got, err := s.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 2 || string(got[1].Data) != "b-new" || got[1].Term != 2 {
		t.Fatalf("entries after repair = %+v, want index2/term2/b-new", got)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w2 := openTestWAL(t, dir)
	defer w2.Close()
	s2, err := OpenWALStorage(w2)
	if err != nil {
		t.Fatalf("OpenWALStorage after restart: %v", err)
	}
	last2, err := s2.LastIndex()
	if err != nil || last2 != 2 {
		t.Fatalf("LastIndex after restart = %d err=%v, want 2 (stale entries must not reappear)", last2, err)
	}
	got2, err := s2.Entries(1, 3)
	if err != nil || len(got2) != 2 || string(got2[1].Data) != "b-new" {
		t.Fatalf("entries after restart = %+v err=%v, want index2=b-new", got2, err)
	}
}
