package wal

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// TestTruncateInterruptedMidwayStillOpensToValidPrefix simulates a crash
// partway through Truncate's two-phase algorithm (docs/wal.md's
// implementation note): the newest segment(s) strictly beyond the
// target have already been deleted, but the target segment itself has
// not yet been shrunk. This is the exact intermediate state the
// implementation's deletion order (highest segment id first, target
// segment last) is designed to make safe. Open must still succeed
// (no corruption, no invented history), and finishing the interrupted
// Truncate afterward must converge to the same result a single,
// uninterrupted call would have produced.
func TestTruncateInterruptedMidwayStillOpensToValidPrefix(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	appendN(t, w, 40)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 3 {
		t.Fatalf("expected several segments, got %d", len(ids))
	}

	const from = 10
	targetID, _, found, err := locateLogIndex(dir, ids, from)
	if err != nil || !found {
		t.Fatalf("locateLogIndex(%d): found=%v err=%v", from, found, err)
	}

	// Manually perform only the first phase of Truncate: delete every
	// segment strictly newer than targetID, but leave the target segment
	// itself untouched — the exact state a crash right before the final
	// shrink step would leave on disk.
	deleted := 0
	for _, id := range ids {
		if id > targetID {
			if err := storage.RemoveSegment(dir, id); err != nil {
				t.Fatalf("RemoveSegment(%d): %v", id, err)
			}
			deleted++
		}
	}
	if deleted == 0 {
		t.Fatal("test setup did not actually delete any segment; from/SegmentMaxSize need adjusting")
	}

	// Open must succeed: the surviving segments are still a complete,
	// gap-free, checksum-valid prefix (just not yet fully truncated).
	w2, report, err := Open(dir, Options{SegmentMaxSize: 64})
	if err != nil {
		t.Fatalf("Open after interrupted truncate: %v", err)
	}
	if report.LastLogIndex < from-1 {
		t.Fatalf("LastLogIndex after interrupted truncate = %d, want >= %d (target segment not yet shrunk)", report.LastLogIndex, from-1)
	}

	// Finishing the truncation now must converge to the fully-truncated
	// result, proving the interrupted state was safely resumable.
	if err := w2.Truncate(from); err != nil {
		t.Fatalf("Truncate to finish interrupted operation: %v", err)
	}
	if w2.NextIndex() != from {
		t.Fatalf("NextIndex after completing interrupted truncate = %d, want %d", w2.NextIndex(), from)
	}
	recs := replayAll(t, w2)
	if len(recs) != from-1 {
		t.Fatalf("replayed %d records, want %d", len(recs), from-1)
	}
	for i, r := range recs {
		if r.Index != uint64(i+1) {
			t.Fatalf("record %d has index %d, want %d", i, r.Index, i+1)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// appendN appends n log entries with predictable payloads "e1".."eN" and
// syncs, returning their assigned indices (always 1..n from a fresh WAL,
// but callers should not assume that in general).
func appendN(t *testing.T, w *WAL, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := w.AppendLogEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("AppendLogEntry #%d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func replayAll(t *testing.T, w *WAL) []Record {
	t.Helper()
	it, err := w.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	var out []Record
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		out = append(out, rec)
	}
	return out
}

// TestTruncateBasic covers the core suffix-truncation contract: entries
// at and after fromIndex disappear; entries before it survive untouched.
func TestTruncateBasic(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	appendN(t, w, 5) // indices 1..5

	if err := w.Truncate(3); err != nil {
		t.Fatalf("Truncate(3): %v", err)
	}
	if w.NextIndex() != 3 {
		t.Fatalf("NextIndex after Truncate(3) = %d, want 3", w.NextIndex())
	}

	recs := replayAll(t, w)
	if len(recs) != 2 {
		t.Fatalf("replayed %d records after truncate, want 2", len(recs))
	}
	if recs[0].Index != 1 || string(recs[0].Payload) != "a" {
		t.Fatalf("record 0 = %+v, want index=1 payload=a", recs[0])
	}
	if recs[1].Index != 2 || string(recs[1].Payload) != "b" {
		t.Fatalf("record 1 = %+v, want index=2 payload=b", recs[1])
	}
}

// TestTruncateThenAppendResumesAtFromIndex proves a new AppendLogEntry
// after Truncate(fromIndex) is assigned exactly fromIndex, and that the
// new entry's content — not the discarded one — is what survives.
func TestTruncateThenAppendResumesAtFromIndex(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	appendN(t, w, 5)
	if err := w.Truncate(3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	idx, err := w.AppendLogEntry([]byte("new-c"))
	if err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if idx != 3 {
		t.Fatalf("index after truncate+append = %d, want 3", idx)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	recs := replayAll(t, w)
	if len(recs) != 3 {
		t.Fatalf("replayed %d records, want 3", len(recs))
	}
	if string(recs[2].Payload) != "new-c" {
		t.Fatalf("record 2 payload = %q, want new-c (divergent entry not overwritten)", recs[2].Payload)
	}
}

// TestTruncateSurvivesRestart proves the truncation itself is durable:
// reopening the WAL after a clean close reflects the truncated log, not
// the original one.
func TestTruncateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	appendN(t, w, 5)
	if err := w.Truncate(3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := w.AppendLogEntry([]byte("new-c")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{})
	defer w2.Close()
	if report.LastLogIndex != 3 {
		t.Fatalf("LastLogIndex after restart = %d, want 3", report.LastLogIndex)
	}
	recs := replayAll(t, w2)
	if len(recs) != 3 || string(recs[2].Payload) != "new-c" {
		t.Fatalf("replayed after restart = %+v, want 3 records ending in new-c", recs)
	}
}

// TestTruncateAcrossSegmentBoundary forces the truncation point to fall
// in an older, already-rotated-away segment, so Truncate must delete
// one or more newer whole segment files before shrinking the older one.
func TestTruncateAcrossSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	// Small segment size so a handful of single-byte-payload entries
	// force multiple rotations.
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	defer w.Close()

	appendN(t, w, 40)

	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 3 {
		t.Fatalf("expected several segments from rotation, got %d", len(ids))
	}

	// Truncate well before the tail, guaranteed to land in an early
	// segment while later segments still exist as whole files.
	const from = 10
	if err := w.Truncate(from); err != nil {
		t.Fatalf("Truncate(%d): %v", from, err)
	}

	idsAfter, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs after truncate: %v", err)
	}
	if len(idsAfter) >= len(ids) {
		t.Fatalf("expected segment files to be removed by truncation across a boundary: before=%d after=%d", len(ids), len(idsAfter))
	}

	recs := replayAll(t, w)
	if len(recs) != from-1 {
		t.Fatalf("replayed %d records, want %d", len(recs), from-1)
	}
	for i, r := range recs {
		if r.Index != uint64(i+1) {
			t.Fatalf("record %d has index %d, want %d", i, r.Index, i+1)
		}
	}

	// Append past the truncation point and confirm durability + ordering
	// survive a restart too.
	if _, err := w.AppendLogEntry([]byte("resumed")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	defer w2.Close()
	if report.LastLogIndex != uint64(from) {
		t.Fatalf("LastLogIndex after restart = %d, want %d", report.LastLogIndex, from)
	}
}

// TestRepeatedTruncation proves Truncate can be applied more than once
// in succession (each call further shortening the log), as real
// divergent-suffix repair may require across successive leader changes.
func TestRepeatedTruncation(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	appendN(t, w, 10)
	if err := w.Truncate(7); err != nil {
		t.Fatalf("Truncate(7): %v", err)
	}
	if err := w.Truncate(4); err != nil {
		t.Fatalf("Truncate(4): %v", err)
	}
	if w.NextIndex() != 4 {
		t.Fatalf("NextIndex = %d, want 4", w.NextIndex())
	}
	recs := replayAll(t, w)
	if len(recs) != 3 {
		t.Fatalf("replayed %d records, want 3", len(recs))
	}
}

// TestTruncateNoOpBeyondLog proves Truncate at or beyond the current
// tail never mutates anything (raft.Storage's documented no-op case).
func TestTruncateNoOpBeyondLog(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	appendN(t, w, 5)
	if err := w.Truncate(6); err != nil {
		t.Fatalf("Truncate(6): %v", err)
	}
	if w.NextIndex() != 6 {
		t.Fatalf("NextIndex changed by an out-of-range Truncate: got %d, want 6", w.NextIndex())
	}
	recs := replayAll(t, w)
	if len(recs) != 5 {
		t.Fatalf("replayed %d records, want 5 (no-op truncate mutated the log)", len(recs))
	}
}

// TestTruncateRejectsZero proves Truncate validates its argument rather
// than silently doing something unspecified for index 0.
func TestTruncateRejectsZero(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()
	appendN(t, w, 3)
	if err := w.Truncate(0); err == nil {
		t.Fatal("Truncate(0) succeeded, want an error")
	}
}

// TestTruncateOnClosedWAL proves the closed-WAL contract extends to
// Truncate like every other method.
func TestTruncateOnClosedWAL(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	appendN(t, w, 2)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Truncate(1); err == nil {
		t.Fatal("Truncate after Close succeeded, want ErrClosed")
	}
}

// TestHardStateRoundTripsAndSurvivesRestart proves AppendHardState +
// LatestHardState behave per docs/wal.md §2/§9 ("tracked by most recent
// record of this type, not by index") both live and after a restart.
func TestHardStateRoundTripsAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	if got := w.LatestHardState(); got != nil {
		t.Fatalf("LatestHardState on fresh WAL = %v, want nil", got)
	}

	if err := w.AppendHardState([]byte("term=1,vote=A")); err != nil {
		t.Fatalf("AppendHardState: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := string(w.LatestHardState()); got != "term=1,vote=A" {
		t.Fatalf("LatestHardState = %q, want term=1,vote=A", got)
	}

	// A second HardState record supersedes the first (docs/wal.md §9:
	// "the last one seen wins").
	if err := w.AppendHardState([]byte("term=2,vote=B")); err != nil {
		t.Fatalf("AppendHardState: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, _ := mustOpen(t, dir, Options{})
	defer w2.Close()
	if got := string(w2.LatestHardState()); got != "term=2,vote=B" {
		t.Fatalf("LatestHardState after restart = %q, want term=2,vote=B", got)
	}
}

// TestHardStateInterleavedWithLogEntriesSurvivesTruncation proves that
// truncating log entries never disturbs a HardState record physically
// preceding the truncation point in the same segment.
func TestHardStateInterleavedWithLogEntriesSurvivesTruncation(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	appendN(t, w, 2)
	if err := w.AppendHardState([]byte("term=5,vote=X")); err != nil {
		t.Fatalf("AppendHardState: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	appendN(t, w, 3) // indices 3,4,5

	if err := w.Truncate(3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if got := string(w.LatestHardState()); got != "term=5,vote=X" {
		t.Fatalf("LatestHardState after truncating later log entries = %q, want term=5,vote=X", got)
	}
	recs := replayAll(t, w)
	if len(recs) != 2 {
		t.Fatalf("replayed %d records, want 2", len(recs))
	}
}
