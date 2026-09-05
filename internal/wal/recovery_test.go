package wal

import (
	"errors"
	"os"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// lastSegmentPath returns the path of the highest-numbered (current)
// segment file in dir, for tests that need to directly manipulate raw
// on-disk bytes to simulate exactly what a crash at a given byte offset
// would leave behind.
func lastSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("no segments found")
	}
	return storage.SegmentPath(dir, ids[len(ids)-1])
}

// TestTornFinalRecordTruncatedAutomatically is scenario LD-4: a crash
// mid-append of the last record leaves a torn tail on disk. Directly
// truncating a real, previously-fsynced segment file to simulate exactly
// the bytes a crash-during-write would leave is the standard, correct way
// to reproduce this deterministically (the alternative — an actual power
// failure — is why real databases use this same technique).
func TestTornFinalRecordTruncatedAutomatically(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	for _, v := range []string{"one", "two", "three"} {
		if _, err := w.AppendLogEntry([]byte(v)); err != nil {
			t.Fatalf("AppendLogEntry: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	fullSize := w.current.Size()

	// Simulate a crash mid-append of a 4th record: some bytes of its
	// frame reached disk, but not all of them.
	if _, err := w.AppendLogEntry([]byte("four-torn")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	tornSize := w.current.Size()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := lastSegmentPath(t, dir)
	// Chop off the last several bytes of the 4th record's frame, leaving
	// an incomplete (torn) frame at the tail — exactly what an
	// interrupted append looks like on disk.
	if err := os.Truncate(path, tornSize-5); err != nil {
		t.Fatalf("os.Truncate: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{})
	defer w2.Close()
	if !report.TornTailTruncated {
		t.Fatal("expected TornTailTruncated=true for a torn final record")
	}
	if report.LastLogIndex != 3 {
		t.Fatalf("LastLogIndex = %d, want 3 (torn 4th record must not count)", report.LastLogIndex)
	}
	if w2.current.Size() != fullSize {
		t.Fatalf("segment size after repair = %d, want %d (truncate to last valid record boundary)", w2.current.Size(), fullSize)
	}

	it, err := w2.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	var got []string
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(rec.Payload))
	}
	want := []string{"one", "two", "three"}
	if len(got) != len(want) {
		t.Fatalf("replayed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replayed %v, want %v", got, want)
		}
	}

	// The repaired WAL must still be writable.
	if _, err := w2.AppendLogEntry([]byte("four-real")); err != nil {
		t.Fatalf("AppendLogEntry after repair: %v", err)
	}
	if err := w2.Sync(); err != nil {
		t.Fatalf("Sync after repair: %v", err)
	}
}

// TestCorruptCompleteFinalRecordRefusesStartup is scenario LD-5: a fully
// framed record's checksum bytes are flipped (bit rot), simulating
// corruption rather than a torn write. Startup must refuse unconditionally
// even though it is the last record.
func TestCorruptCompleteFinalRecordRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	for _, v := range []string{"one", "two"} {
		if _, err := w.AppendLogEntry([]byte(v)); err != nil {
			t.Fatalf("AppendLogEntry: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	size := w.current.Size()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := lastSegmentPath(t, dir)
	flipLastChecksumByte(t, path, size)

	_, _, err := Open(dir, Options{})
	if err == nil {
		t.Fatal("Open with a corrupt final record: expected error, got nil")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with a corrupt final record: err = %v, want ErrCorrupt", err)
	}
}

// TestMidLogCorruptionRefusesStartup is scenario LD-6: a record strictly
// before the last one is corrupted, with valid-looking records after it.
// Startup must refuse regardless of the apparent validity of what follows.
func TestMidLogCorruptionRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	for _, v := range []string{"one", "two", "three"} {
		if _, err := w.AppendLogEntry([]byte(v)); err != nil {
			t.Fatalf("AppendLogEntry: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := lastSegmentPath(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Flip a payload byte inside the FIRST record ("one"), which is
	// followed by two more otherwise-valid-looking records.
	firstPayloadOffset := headerSize
	data[firstPayloadOffset] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, err = Open(dir, Options{})
	if err == nil {
		t.Fatal("Open with mid-log corruption: expected error, got nil")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with mid-log corruption: err = %v, want ErrCorrupt", err)
	}
}

// TestTornTailInNonFinalSegmentRefusesStartup: a torn frame appearing in
// an earlier (closed, supposedly immutable) segment is itself a form of
// corruption — only the single open/last segment may ever have a torn
// tail — and must refuse startup rather than being auto-repaired.
func TestTornTailInNonFinalSegmentRefusesStartup(t *testing.T) {
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
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("expected rotation to produce >= 2 segments, got %d", len(ids))
	}
	// Truncate an EARLIER segment (not the last one) mid-frame.
	earlier := storage.SegmentPath(dir, ids[0])
	info, err := os.Stat(earlier)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(earlier, info.Size()-2); err != nil {
		t.Fatalf("os.Truncate: %v", err)
	}

	_, _, err = Open(dir, Options{SegmentMaxSize: 64})
	if err == nil {
		t.Fatal("Open with torn tail in a non-final segment: expected error, got nil")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with torn tail in a non-final segment: err = %v, want ErrCorrupt", err)
	}
}

// TestUnsupportedFormatVersionRefusesStartup covers the "unknown/
// unsupported record version" row of docs/wal.md §6.3.
func TestUnsupportedFormatVersionRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	if _, err := w.AppendLogEntry([]byte("one")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := lastSegmentPath(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// The Metadata record is written first; corrupt the LAST record's
	// (the "one" LogEntry's) version byte instead of the metadata one.
	// Find the second frame's header start by decoding the first frame's
	// length.
	firstLen := int(uint32(data[9])<<24 | uint32(data[10])<<16 | uint32(data[11])<<8 | uint32(data[12]))
	secondHeaderStart := headerSize + firstLen + checksumSize
	data[secondHeaderStart+headerVersionOff] = 0xFF // unrecognized version
	// The checksum was computed over the original version byte, so this
	// also breaks the checksum — but per docs, an unsupported version is
	// still its own distinct, always-refuse category regardless.
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, err = Open(dir, Options{})
	if err == nil {
		t.Fatal("Open with unsupported version: expected error, got nil")
	}
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Open with unsupported version: err = %v, want ErrUnsupportedVersion", err)
	}
}

// TestCrashBeforeSyncMayLoseUnackedWrite is scenario LD-3: Append
// succeeds but the process is lost before the corresponding Sync ever
// returns, so no acknowledgment was ever given for that record. Losing it
// on restart is a PERMITTED outcome (not a bug), because it was never
// acknowledged; what must never happen is treating it as durable. We
// reproduce the disk-level effect of such a crash the same way real WAL
// test suites do: truncate the file back to exactly the bytes that
// existed before the unacknowledged Append.
func TestCrashBeforeSyncMayLoseUnackedWrite(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	if _, err := w.AppendLogEntry([]byte("acked")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	preAppendSize := w.current.Size()

	if _, err := w.AppendLogEntry([]byte("never-acked")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := lastSegmentPath(t, dir)
	if err := os.Truncate(path, preAppendSize); err != nil {
		t.Fatalf("os.Truncate: %v", err)
	}

	w2, report := mustOpen(t, dir, Options{})
	defer w2.Close()
	if report.LastLogIndex != 1 {
		t.Fatalf("LastLogIndex = %d, want 1 (unacked record correctly absent)", report.LastLogIndex)
	}
}

// TestSyncFailurePropagatesNotTreatedAsSuccess injects a real (not
// mocked) fsync-style failure by closing the underlying segment file out
// from under the WAL, then verifies a failed Sync is reported as an
// error rather than silently treated as success, and that a failed
// Append does not advance the log index (docs/failure-model.md §1.8).
func TestSyncFailurePropagatesNotTreatedAsSuccess(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})

	if _, err := w.AppendLogEntry([]byte("one")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if err := w.current.Close(); err != nil {
		t.Fatalf("closing underlying segment for fault injection: %v", err)
	}

	if err := w.Sync(); err == nil {
		t.Fatal("Sync on a closed underlying file: expected error, got nil (a failed Sync must never be treated as success)")
	}

	beforeIdx := w.NextIndex()
	if _, err := w.AppendLogEntry([]byte("two")); err == nil {
		t.Fatal("AppendLogEntry on a closed underlying file: expected error, got nil")
	}
	if w.NextIndex() != beforeIdx {
		t.Fatalf("NextIndex advanced after a failed append: got %d, want unchanged %d", w.NextIndex(), beforeIdx)
	}
}

// TestMetadataFormatVersionMismatchRefusesStartup covers the case where a
// Metadata record's own embedded FormatVersion field disagrees with the
// reader's supported version even though the frame itself is intact and
// checksum-valid — a distinct version-skew signal from the per-frame
// version byte checked during decode.
func TestMetadataFormatVersionMismatchRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	if err := storage.EnsureDir(dir); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	seg, err := storage.CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	badMeta := encodeMetadata(Metadata{NodeID: "node-x", FormatVersion: FormatVersion + 1})
	frame := encodeRecord(RecordTypeMetadata, 0, badMeta)
	if _, err := seg.Append(frame); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, _, err = Open(dir, Options{})
	if err == nil {
		t.Fatal("Open with mismatched metadata format version: expected error, got nil")
	}
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Open with mismatched metadata format version: err = %v, want ErrUnsupportedVersion", err)
	}
}

// TestNoMetadataRecordRefusesStartup covers a non-empty durable log that
// never contains a Metadata record — impossible for any log this package
// created, but a robustness check for a foreign/malformed data directory.
func TestNoMetadataRecordRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	if err := storage.EnsureDir(dir); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	seg, err := storage.CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	frame := encodeRecord(RecordTypeLogEntry, 1, []byte("orphan"))
	if _, err := seg.Append(frame); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, _, err = Open(dir, Options{})
	if !errors.Is(err, ErrNoMetadata) {
		t.Fatalf("Open with no metadata record: err = %v, want ErrNoMetadata", err)
	}
}

func flipLastChecksumByte(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], size-1); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], size-1); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}
