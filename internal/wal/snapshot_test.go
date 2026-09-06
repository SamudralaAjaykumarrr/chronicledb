package wal

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// TestFirstIndexTracksSnapshotPointerLive proves AppendMetadataSnapshot
// advances FirstIndex() immediately in this live process, not only after
// a future restart re-derives it from Metadata (docs/snapshots.md §8).
func TestFirstIndexTracksSnapshotPointerLive(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	if w.FirstIndex() != 1 {
		t.Fatalf("FirstIndex() on a fresh WAL = %d, want 1", w.FirstIndex())
	}
	for i := 0; i < 5; i++ {
		if _, err := w.AppendLogEntry([]byte("x")); err != nil {
			t.Fatalf("AppendLogEntry: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.AppendMetadataSnapshot(3); err != nil {
		t.Fatalf("AppendMetadataSnapshot: %v", err)
	}
	if w.FirstIndex() != 4 {
		t.Fatalf("FirstIndex() after AppendMetadataSnapshot(3) = %d, want 4", w.FirstIndex())
	}
}

func TestAppendMetadataSnapshotRejectsGoingBackward(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	defer w.Close()

	if _, err := w.AppendLogEntry([]byte("a")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.AppendMetadataSnapshot(1); err != nil {
		t.Fatalf("AppendMetadataSnapshot(1): %v", err)
	}
	if err := w.AppendMetadataSnapshot(0); err == nil {
		t.Fatal("expected an error moving the snapshot pointer backward")
	}
	if w.Metadata().LatestSnapshotIndex != 1 {
		t.Fatalf("a rejected backward move must not change the pointer, got %d", w.Metadata().LatestSnapshotIndex)
	}
}

// TestCompactBeforeDeletesOnlySegmentsFullyAtOrBeforeBoundary proves the
// physical half of SN-6: only whole segments entirely at or before the
// snapshot boundary are removed, and everything after it remains fully
// readable via Replay.
func TestCompactBeforeDeletesOnlySegmentsFullyAtOrBeforeBoundary(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	defer w.Close()

	for i := 0; i < 30; i++ {
		if _, err := w.AppendLogEntry([]byte("0123456789")); err != nil {
			t.Fatalf("AppendLogEntry #%d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	idsBefore, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(idsBefore) < 3 {
		t.Fatalf("test needs >= 3 segments to be meaningful, got %d", len(idsBefore))
	}

	if err := w.AppendMetadataSnapshot(15); err != nil {
		t.Fatalf("AppendMetadataSnapshot: %v", err)
	}
	if err := w.CompactBefore(15); err != nil {
		t.Fatalf("CompactBefore: %v", err)
	}

	idsAfter, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs after compaction: %v", err)
	}
	if len(idsAfter) >= len(idsBefore) {
		t.Fatalf("expected fewer segments after compaction: before=%d after=%d", len(idsBefore), len(idsAfter))
	}

	it, err := w.Replay(w.FirstIndex())
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
		if rec.Index != uint64(15+count) {
			t.Fatalf("record #%d has index %d, want %d", count, rec.Index, 15+count)
		}
	}
	if count != 15 {
		t.Fatalf("replayed %d entries after compaction, want 15 (indices 16..30)", count)
	}
}

// TestCompactBeforeNeverDeletesCurrentSegment proves the current
// (open-for-writing) segment always survives compaction, however large
// uptoIndex is — so subsequent appends keep working correctly afterward.
func TestCompactBeforeNeverDeletesCurrentSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	defer w.Close()

	for i := 0; i < 10; i++ {
		if _, err := w.AppendLogEntry([]byte("0123456789")); err != nil {
			t.Fatalf("AppendLogEntry #%d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.AppendMetadataSnapshot(10); err != nil {
		t.Fatalf("AppendMetadataSnapshot: %v", err)
	}
	if err := w.CompactBefore(1_000_000); err != nil {
		t.Fatalf("CompactBefore: %v", err)
	}

	idx, err := w.AppendLogEntry([]byte("new"))
	if err != nil {
		t.Fatalf("AppendLogEntry after compaction: %v", err)
	}
	if idx != 11 {
		t.Fatalf("AppendLogEntry after compaction assigned index %d, want 11", idx)
	}
}

// TestReopenAfterSnapshotAndCompactionReplaysOnlyRemainingEntries is
// SN-6's restart leg: "recovery after a crash immediately following
// truncation still succeeds using the snapshot" — a fresh Open correctly
// re-derives FirstIndex from durable Metadata and replays only the
// entries genuinely beyond the snapshot boundary.
func TestReopenAfterSnapshotAndCompactionReplaysOnlyRemainingEntries(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 64})

	for i := 0; i < 20; i++ {
		if _, err := w.AppendLogEntry([]byte("0123456789")); err != nil {
			t.Fatalf("AppendLogEntry #%d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.AppendMetadataSnapshot(12); err != nil {
		t.Fatalf("AppendMetadataSnapshot: %v", err)
	}
	if err := w.CompactBefore(12); err != nil {
		t.Fatalf("CompactBefore: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	defer w2.Close()
	if report.FirstLogIndex != 13 {
		t.Fatalf("report.FirstLogIndex = %d, want 13", report.FirstLogIndex)
	}
	if w2.FirstIndex() != 13 {
		t.Fatalf("FirstIndex() after reopen = %d, want 13", w2.FirstIndex())
	}
	if report.LastLogIndex != 20 {
		t.Fatalf("report.LastLogIndex = %d, want 20", report.LastLogIndex)
	}
	if w2.Metadata().LatestSnapshotIndex != 12 {
		t.Fatalf("Metadata().LatestSnapshotIndex = %d, want 12", w2.Metadata().LatestSnapshotIndex)
	}

	it, err := w2.Replay(w2.FirstIndex())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	count := 0
	for {
		_, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}
	if count != 8 {
		t.Fatalf("replayed %d entries after reopen, want 8 (indices 13..20)", count)
	}
}

// TestOpenRefusesWhenLogGapExceedsSnapshotPointer proves the recovery
// side of SN-6/docs/recovery.md §4: if the durable log's physically
// oldest surviving entry starts strictly after where the durable
// snapshot pointer says it should (a gap — e.g. an operator or disk
// fault removed a segment no legitimate compaction would have touched,
// since CompactBefore only ever deletes what AppendMetadataSnapshot has
// already sanctioned), Open refuses to start rather than silently
// skipping the missing history.
func TestOpenRefusesWhenLogGapExceedsSnapshotPointer(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 64})
	for i := 0; i < 20; i++ {
		if _, err := w.AppendLogEntry([]byte("0123456789")); err != nil {
			t.Fatalf("AppendLogEntry #%d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// Re-affirm Metadata (still LatestSnapshotIndex=0) into the *current*
	// (last) segment, so the oldest segment — created at the very first
	// Open and holding the WAL's original sole Metadata record — is no
	// longer the only copy of it, and can be removed below without also
	// destroying the Metadata record itself (last-one-wins,
	// docs/wal.md §9). This isolates the case under test: a physical gap
	// with an otherwise perfectly valid, unrelated Metadata record.
	if err := w.AppendMetadataSnapshot(0); err != nil {
		t.Fatalf("AppendMetadataSnapshot(0): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("test needs >= 2 segments, got %d", len(ids))
	}
	// Remove the oldest segment directly, bypassing CompactBefore's own
	// metadata-pointer discipline entirely (no snapshot was ever taken) —
	// simulating disk-level data loss or an operator mistake, not a
	// legitimate compaction.
	if err := storage.RemoveSegment(dir, ids[0]); err != nil {
		t.Fatalf("RemoveSegment: %v", err)
	}

	if _, _, err := Open(dir, Options{SegmentMaxSize: 64}); err == nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with a physical gap and no snapshot pointer covering it: err=%v, want ErrCorrupt", err)
	}
}
