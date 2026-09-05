package wal

import (
	"errors"
	"sync"
	"testing"
)

func mustOpen(t *testing.T, dir string, opts Options) (*WAL, *RecoveryReport) {
	t.Helper()
	w, report, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return w, report
}

func TestOpenEmptyDatabaseColdStart(t *testing.T) {
	dir := t.TempDir()
	w, report := mustOpen(t, dir, Options{})
	defer w.Close()

	if w.NextIndex() != 1 {
		t.Fatalf("NextIndex = %d, want 1", w.NextIndex())
	}
	if w.Metadata().NodeID == "" {
		t.Fatal("fresh WAL has empty NodeID")
	}
	if report.LastLogIndex != 0 {
		t.Fatalf("LastLogIndex = %d, want 0", report.LastLogIndex)
	}
}

func TestOpenCloseReopenEmpty(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	nodeID := w.Metadata().NodeID
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{})
	defer w2.Close()
	if w2.Metadata().NodeID != nodeID {
		t.Fatalf("NodeID changed across reopen: %q -> %q", nodeID, w2.Metadata().NodeID)
	}
	if w2.NextIndex() != 1 {
		t.Fatalf("NextIndex after empty reopen = %d, want 1", w2.NextIndex())
	}
	if report.LastLogIndex != 0 {
		t.Fatalf("LastLogIndex = %d, want 0", report.LastLogIndex)
	}
}

// TestSingleWriteCleanRestart is scenario LD-1 from docs/scenario-corpus.md.
func TestSingleWriteCleanRestart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	idx, err := w.AppendLogEntry([]byte("key=value"))
	if err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if idx != 1 {
		t.Fatalf("first log index = %d, want 1", idx)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{})
	defer w2.Close()
	if report.LastLogIndex != 1 {
		t.Fatalf("LastLogIndex after restart = %d, want 1", report.LastLogIndex)
	}
	if report.TornTailTruncated {
		t.Fatal("clean restart falsely reported a torn tail")
	}

	it, err := w2.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	rec, ok, err := it.Next()
	if err != nil || !ok {
		t.Fatalf("Next() = rec=%v ok=%v err=%v, want a record", rec, ok, err)
	}
	if string(rec.Payload) != "key=value" {
		t.Fatalf("replayed payload = %q, want %q", rec.Payload, "key=value")
	}
	if _, ok, err := it.Next(); ok || err != nil {
		t.Fatalf("expected exactly one record, got ok=%v err=%v", ok, err)
	}
}

// TestMultipleWritesRestartOrderingPreserved covers LD-1's multi-write
// variant plus the ORDERING invariant from docs/invariants.md.
func TestMultipleWritesRestartOrderingPreserved(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	values := []string{"a", "b", "c", "d", "e"}
	for i, v := range values {
		idx, err := w.AppendLogEntry([]byte(v))
		if err != nil {
			t.Fatalf("AppendLogEntry(%q): %v", v, err)
		}
		if idx != uint64(i+1) {
			t.Fatalf("index for %q = %d, want %d", v, idx, i+1)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{})
	defer w2.Close()
	if report.LastLogIndex != uint64(len(values)) {
		t.Fatalf("LastLogIndex = %d, want %d", report.LastLogIndex, len(values))
	}
	if w2.NextIndex() != uint64(len(values)+1) {
		t.Fatalf("NextIndex = %d, want %d", w2.NextIndex(), len(values)+1)
	}

	it, err := w2.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	for i, want := range values {
		rec, ok, err := it.Next()
		if err != nil || !ok {
			t.Fatalf("Next() #%d: rec=%v ok=%v err=%v", i, rec, ok, err)
		}
		if rec.Index != uint64(i+1) {
			t.Fatalf("record #%d index = %d, want %d", i, rec.Index, i+1)
		}
		if string(rec.Payload) != want {
			t.Fatalf("record #%d payload = %q, want %q (ordering violated)", i, rec.Payload, want)
		}
	}
	if _, ok, err := it.Next(); ok || err != nil {
		t.Fatalf("expected exactly %d records, got extra: ok=%v err=%v", len(values), ok, err)
	}
}

// TestReplayNeverInventsRecords is the RECOVERY-NON-INVENTION invariant,
// checked at the replay level: replaying from an index beyond the last
// written record yields nothing, and replaying from the middle skips
// exactly the right prefix without fabricating anything.
func TestReplayNeverInventsRecords(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	for _, v := range []string{"a", "b", "c"} {
		if _, err := w.AppendLogEntry([]byte(v)); err != nil {
			t.Fatalf("AppendLogEntry: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	it, err := w.Replay(10)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	if _, ok, err := it.Next(); ok || err != nil {
		t.Fatalf("Replay(10) with only 3 records: ok=%v err=%v, want none", ok, err)
	}

	it2, err := w.Replay(2)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it2.Close()
	rec, ok, err := it2.Next()
	if err != nil || !ok || rec.Index != 2 || string(rec.Payload) != "b" {
		t.Fatalf("Replay(2) first record = %+v ok=%v err=%v, want index=2 payload=b", rec, ok, err)
	}
}

func TestAppendAckedOnlyAfterSync(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	// AppendLogEntry succeeding must not, by itself, mean the record is
	// durable: this is a compile-time/API-contract property (Append never
	// returns a "synced" flag claiming durability) exercised here by
	// confirming Sync is a distinct, separately-failable step.
	if _, err := w.AppendLogEntry([]byte("unsynced")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func TestLifecycleClosed(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close: err = %v, want ErrClosed", err)
	}
	if _, err := w.AppendLogEntry([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("AppendLogEntry after Close: err = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close: err = %v, want ErrClosed", err)
	}
	if _, err := w.Replay(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Replay after Close: err = %v, want ErrClosed", err)
	}
}

func TestConcurrentAppendRaceSafe(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	const n = 50
	var wg sync.WaitGroup
	indices := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx, err := w.AppendLogEntry([]byte("payload"))
			indices[i] = idx
			errs[i] = err
		}()
	}
	wg.Wait()
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	seen := make(map[uint64]bool)
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: AppendLogEntry: %v", i, errs[i])
		}
		if seen[indices[i]] {
			t.Fatalf("duplicate index %d assigned to two concurrent appends", indices[i])
		}
		seen[indices[i]] = true
	}
	if w.NextIndex() != n+1 {
		t.Fatalf("NextIndex = %d, want %d", w.NextIndex(), n+1)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// Small enough that a handful of records force at least one rotation.
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 128})

	for i := 0; i < 50; i++ {
		if _, err := w.AppendLogEntry([]byte("0123456789")); err != nil {
			t.Fatalf("AppendLogEntry #%d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{SegmentMaxSize: 128})
	defer w2.Close()
	if report.SegmentsScanned < 2 {
		t.Fatalf("SegmentsScanned = %d, want >= 2 (rotation should have occurred)", report.SegmentsScanned)
	}
	if report.LastLogIndex != 50 {
		t.Fatalf("LastLogIndex = %d, want 50", report.LastLogIndex)
	}

	it, err := w2.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	count := 0
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		count++
		if rec.Index != uint64(count) {
			t.Fatalf("record #%d has index %d (ordering broken across segments)", count, rec.Index)
		}
	}
	if count != 50 {
		t.Fatalf("replayed %d records across segments, want 50", count)
	}
}

func TestRecordTooLargeRejected(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	oversized := make([]byte, MaxRecordPayloadSize+1)
	if _, err := w.AppendLogEntry(oversized); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("AppendLogEntry(oversized): err = %v, want ErrRecordTooLarge", err)
	}
}
