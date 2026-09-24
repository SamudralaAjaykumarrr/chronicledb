package wal_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// dirHash returns a stable hash of every regular file's path and content
// under dir, recursively — used to prove scrub never modifies anything
// it reads (SL-10, SCRUB NON-DESTRUCTIVE §27.10).
func dirHash(t *testing.T, dir string) string {
	t.Helper()
	var paths []string
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, _ := filepath.Rel(dir, p)
		fmt.Fprintf(h, "path=%s\x00", rel)
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("opening %s: %v", p, err)
		}
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			t.Fatalf("hashing %s: %v", p, err)
		}
		f.Close()
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// writeMultiSegmentWAL writes n small log entries with a tiny
// SegmentMaxSize, forcing several segment rotations, and returns the dir
// plus the ascending segment ids actually created.
func writeMultiSegmentWAL(t *testing.T, n int) (dir string, ids []uint64) {
	t.Helper()
	dir = t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{SegmentMaxSize: 200})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := w.AppendLogEntry([]byte("payload-of-some-length-to-force-rotation")); err != nil {
			t.Fatalf("AppendLogEntry %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ids, err = storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 3 {
		t.Fatalf("test setup needs >= 3 segments to exercise \"earlier vs current\", got %d", len(ids))
	}
	return dir, ids
}

func TestScrub_CleanTreeZeroFindings(t *testing.T) {
	dir, _ := writeMultiSegmentWAL(t, 30)
	findings, stats, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("clean tree: findings = %+v, want none", findings)
	}
	if stats.SegmentsChecked < 3 || stats.FramesChecked == 0 {
		t.Fatalf("stats = %+v, want several segments and frames checked", stats)
	}
}

func TestScrub_TornTailInCurrentSegmentIsNotAFinding(t *testing.T) {
	dir, ids := writeMultiSegmentWAL(t, 30)
	current := ids[len(ids)-1]
	path := storage.SegmentPath(dir, current)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil { // chop off the last few bytes: a torn final frame
		t.Fatalf("truncate: %v", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("torn tail in current segment: findings = %+v, want none (legal per docs/wal.md §6)", findings)
	}
}

func TestScrub_TornTailInEarlierSegmentIsAFinding(t *testing.T) {
	dir, ids := writeMultiSegmentWAL(t, 30)
	earlier := ids[0]
	path := storage.SegmentPath(dir, earlier)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	// A torn tail in a non-open segment is corruption in its own right
	// (bad_framing); the entries that truncation discarded also produce
	// a real, honest index gap once scanning resumes in the next
	// segment (index_out_of_order) — both are legitimate, independent
	// symptoms of the same injected corruption, so this only asserts
	// the primary one is present, not that it is the sole finding.
	found := false
	for _, f := range findings {
		if f.File == path && f.Kind == scrub.FindingBadFraming {
			found = true
		}
	}
	if !found {
		t.Fatalf("torn tail in an earlier (non-open) segment: findings = %+v, want a bad_framing finding for %s", findings, path)
	}
}

func TestScrub_ChecksumCorruptionDetected(t *testing.T) {
	dir, ids := writeMultiSegmentWAL(t, 30)
	earlier := ids[0]
	path := storage.SegmentPath(dir, earlier)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip a byte well inside the first frame's payload region (past the
	// 14-byte header), leaving the frame's own declared length/version
	// intact so this trips the checksum, not framing.
	data[20] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 1 || findings[0].Kind != scrub.FindingBadChecksum {
		t.Fatalf("checksum corruption: findings = %+v, want exactly one bad_checksum", findings)
	}
}

func TestScrub_DirectoryUnchangedBeforeAndAfter(t *testing.T) {
	dir, _ := writeMultiSegmentWAL(t, 30)
	before := dirHash(t, dir)
	if _, _, err := wal.Scrub(dir, nil); err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	after := dirHash(t, dir)
	if before != after {
		t.Fatalf("scrub modified the directory: before=%s after=%s", before, after)
	}
}

// installSnapshotWAL reproduces, through this package's public API only,
// the exact durable shape internal/node.WALStorage.InstallSnapshot leaves
// behind when a follower catches up by snapshot rather than by log
// (internal/node/storage.go:161 — AppendMetadataSnapshot, Reaffirm,
// Truncate, CompactBefore, in that order):
//
//   - preBoundary log entries 1..preBoundary are already physically
//     present in the current (still-open) segment;
//   - the snapshot pointer jumps to boundary, far beyond them;
//   - Truncate(boundary+1) is a COUNTER-ONLY forward jump (wal.go:404 —
//     fromIndex >= nextLogIndex writes no bytes at all), so nothing is
//     physically removed;
//   - CompactBefore(boundary) cannot help either: it breaks at
//     id == currentID (wal.go:663), and the current segment is the one
//     holding those pre-boundary entries.
//
// firstLive is the index the caller then resumes appending at: boundary+1
// is the ordinary case, anything higher injects a genuine gap.
// liveCount entries are appended from there, contiguously.
//
// Segment rotation is deliberately left at the default size so
// everything lands in ONE segment — the shape that makes this reachable.
func installSnapshotWAL(t *testing.T, preBoundary, boundary, firstLive, liveCount uint64) string {
	t.Helper()
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	for i := uint64(0); i < preBoundary; i++ {
		if _, err := w.AppendLogEntry([]byte("pre-boundary-entry")); err != nil {
			t.Fatalf("AppendLogEntry %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// WALStorage.InstallSnapshot's four steps, in order.
	if err := w.AppendMetadataSnapshot(boundary); err != nil {
		t.Fatalf("AppendMetadataSnapshot(%d): %v", boundary, err)
	}
	if err := w.AppendHardState([]byte("reaffirmed-hard-state")); err != nil { // Reaffirm
		t.Fatalf("AppendHardState: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Truncate(firstLive); err != nil {
		t.Fatalf("Truncate(%d): %v", firstLive, err)
	}
	if err := w.CompactBefore(boundary); err != nil {
		t.Fatalf("CompactBefore(%d): %v", boundary, err)
	}

	// The follower resumes ordinary replication.
	for i := uint64(0); i < liveCount; i++ {
		if _, err := w.AppendLogEntry([]byte("live-suffix-entry")); err != nil {
			t.Fatalf("AppendLogEntry (live) %d: %v", i, err)
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
	if len(ids) != 1 {
		t.Fatalf("test setup expects exactly one segment (the shape that makes this reachable), got %v", ids)
	}
	return dir
}

// TestScrub_PostInstallSnapshotPreBoundaryEntriesAreNotAFinding is F1's
// regression (docs/v0.6.0-plan.md §21.3, SL-10, §31 gate 3's "zero
// findings on clean").
//
// Scrub's contiguity rule must be the same BOUNDARY-AWARE one Open
// already implements (wal.go:246 — "any physically-encountered index at
// or before LatestSnapshotIndex is superseded ... only the live suffix
// must be perfectly contiguous"). A flat index == last+1 check reports
// index_out_of_order on a data directory Open accepts as perfectly
// correct — a false corruption report on a follower that did nothing but
// catch up by InstallSnapshot and resume replicating. Open accepting the
// same directory is asserted here too, because that is precisely what
// makes the finding false rather than a matter of taste.
func TestScrub_PostInstallSnapshotPreBoundaryEntriesAreNotAFinding(t *testing.T) {
	dir := installSnapshotWAL(t, 50, 100, 101, 3) // 1..50 superseded; live suffix 101,102,103

	// Recovery's own verdict on this directory: completely clean.
	w, report, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open rejected a directory this test asserts is legitimate: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if report.FirstLogIndex != 101 || report.LastLogIndex != 103 {
		t.Fatalf("recovery report = %+v, want FirstLogIndex=101 LastLogIndex=103", report)
	}

	findings, stats, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("post-InstallSnapshot WAL that wal.Open accepts as clean: scrub findings = %+v, want none", findings)
	}
	if stats.SegmentsChecked != 1 || stats.FramesChecked == 0 {
		t.Fatalf("stats = %+v, want the single segment and its frames actually checked", stats)
	}
}

// TestScrub_LiveSuffixGapAboveTheBoundaryIsStillAFinding is the
// calibration half of the test above: boundary-awareness must suppress
// only genuinely superseded pre-boundary indices, never a real gap
// WITHIN the live suffix. Open rejects this same directory with
// ErrCorrupt ("out-of-order log index"), so scrub reporting it is the
// correct, fail-closed answer.
func TestScrub_LiveSuffixGapAboveTheBoundaryIsStillAFinding(t *testing.T) {
	dir := installSnapshotWAL(t, 50, 100, 101, 2) // 1..50 superseded; live 101,102

	// Re-open and force a second counter-only forward jump, so the live
	// suffix itself becomes 101,102,105 — a genuine gap no snapshot
	// pointer covers.
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	if err := w.Truncate(105); err != nil {
		t.Fatalf("Truncate(105): %v", err)
	}
	if _, err := w.AppendLogEntry([]byte("gapped-entry")); err != nil {
		t.Fatalf("AppendLogEntry: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery's own verdict: this one really is corrupt.
	if _, _, err := wal.Open(dir, wal.Options{}); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("wal.Open on a genuinely gapped live suffix: err = %v, want ErrCorrupt", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	found := false
	for _, f := range findings {
		if f.Kind == scrub.FindingIndexOutOfOrder {
			found = true
		}
	}
	if !found {
		t.Fatalf("genuine gap inside the live suffix: findings = %+v, want an index_out_of_order finding", findings)
	}
}

// TestScrub_LiveSuffixStartingAboveTheBoundaryIsStillAFinding covers the
// sub-case a naive "just skip everything <= boundary" fix would miss: the
// live suffix must begin at exactly boundary+1, because a suffix starting
// later means genuinely missing history the snapshot does not cover
// (wal.go:255's own rule). Open rejects it; scrub must too.
func TestScrub_LiveSuffixStartingAboveTheBoundaryIsStillAFinding(t *testing.T) {
	dir := installSnapshotWAL(t, 50, 100, 102, 2) // boundary 100, but live suffix starts at 102

	if _, _, err := wal.Open(dir, wal.Options{}); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("wal.Open on a live suffix starting above boundary+1: err = %v, want ErrCorrupt", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	found := false
	for _, f := range findings {
		if f.Kind == scrub.FindingIndexOutOfOrder {
			found = true
		}
	}
	if !found {
		t.Fatalf("live suffix starting above boundary+1: findings = %+v, want an index_out_of_order finding", findings)
	}
}
