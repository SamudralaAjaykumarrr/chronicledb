package wal

import "testing"

// This file is Phase 10's "Repeated compact / restart" adversarial
// pattern (docs/roadmap.md Phase 10 "ADVERSARIAL RECOVERY"): snapshot
// -> compact -> restart -> append -> snapshot -> compact -> restart,
// several times over, directly at the internal/wal level (existing
// coverage — TestReopenAfterSnapshotAndCompactionReplaysOnlyRemainingEntries
// et al. — proves exactly one snapshot/compact/reopen cycle; this test
// proves the pattern is safe under repetition, i.e. that FirstIndex,
// NextIndex, and Metadata().LatestSnapshotIndex all stay globally
// correct across many such cycles, not just the first one).
func TestRepeatedSnapshotCompactRestartCycleKeepsIndexesCorrect(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentMaxSize: 128})

	const cycles = 6
	const perCycle = 10
	var lastIndex uint64
	for cycle := 0; cycle < cycles; cycle++ {
		for i := 0; i < perCycle; i++ {
			idx, err := w.AppendLogEntry([]byte("0123456789"))
			if err != nil {
				t.Fatalf("cycle %d entry %d: AppendLogEntry: %v", cycle, i, err)
			}
			lastIndex = idx
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("cycle %d: Sync: %v", cycle, err)
		}

		// Snapshot and compact through everything appended so far
		// (this cycle's own entries plus every prior cycle's).
		if err := w.AppendMetadataSnapshot(lastIndex); err != nil {
			t.Fatalf("cycle %d: AppendMetadataSnapshot(%d): %v", cycle, lastIndex, err)
		}
		if err := w.CompactBefore(lastIndex); err != nil {
			t.Fatalf("cycle %d: CompactBefore(%d): %v", cycle, lastIndex, err)
		}

		if got := w.Metadata().LatestSnapshotIndex; got != lastIndex {
			t.Fatalf("cycle %d: Metadata().LatestSnapshotIndex = %d, want %d", cycle, got, lastIndex)
		}
		if got := w.NextIndex(); got != lastIndex+1 {
			t.Fatalf("cycle %d: NextIndex() = %d, want %d", cycle, got, lastIndex+1)
		}

		// Restart: close and reopen the same directory, confirming the
		// snapshot boundary and next-append point survive exactly, then
		// keep going — the next cycle's AppendLogEntry must resume
		// exactly at lastIndex+1 on the reopened WAL, not silently
		// diverge after however many prior restarts have already
		// happened.
		if err := w.Close(); err != nil {
			t.Fatalf("cycle %d: Close: %v", cycle, err)
		}
		w2, report := mustOpen(t, dir, Options{SegmentMaxSize: 128})
		if report.FirstLogIndex != 0 && report.FirstLogIndex <= lastIndex {
			t.Fatalf("cycle %d: reopen report.FirstLogIndex = %d, want either 0 (nothing left) or > %d", cycle, report.FirstLogIndex, lastIndex)
		}
		if got := w2.FirstIndex(); got != lastIndex+1 {
			t.Fatalf("cycle %d: reopened FirstIndex() = %d, want %d (everything at/before the snapshot boundary must be gone)", cycle, got, lastIndex+1)
		}
		if got := w2.NextIndex(); got != lastIndex+1 {
			t.Fatalf("cycle %d: reopened NextIndex() = %d, want %d", cycle, got, lastIndex+1)
		}
		if got := w2.Metadata().LatestSnapshotIndex; got != lastIndex {
			t.Fatalf("cycle %d: reopened Metadata().LatestSnapshotIndex = %d, want %d", cycle, got, lastIndex)
		}

		// A fresh replay from FirstIndex must find exactly zero entries
		// (everything at/before the boundary was compacted, and nothing
		// past it has been appended yet on this reopened handle).
		it, err := w2.Replay(w2.FirstIndex())
		if err != nil {
			t.Fatalf("cycle %d: Replay: %v", cycle, err)
		}
		if _, ok, err := it.Next(); err != nil {
			t.Fatalf("cycle %d: Replay.Next: %v", cycle, err)
		} else if ok {
			t.Fatalf("cycle %d: expected zero entries immediately after compaction+reopen, found at least one", cycle)
		}
		it.Close()

		w = w2
	}
	w.Close()

	// Final end-to-end check: reopen one more time and confirm the
	// index arithmetic across all `cycles` rounds of compaction landed
	// exactly where expected — a global correctness check, not just a
	// per-cycle local one.
	wFinal, report := mustOpen(t, dir, Options{SegmentMaxSize: 128})
	defer wFinal.Close()
	wantLast := uint64(cycles * perCycle)
	if lastIndex != wantLast {
		t.Fatalf("test setup error: lastIndex = %d, want %d", lastIndex, wantLast)
	}
	if report.FirstLogIndex != wantLast+1 {
		t.Fatalf("final reopen report.FirstLogIndex = %d, want %d (LatestSnapshotIndex+1: no log entries survive past the last compaction)", report.FirstLogIndex, wantLast+1)
	}
	if wFinal.NextIndex() != wantLast+1 {
		t.Fatalf("final NextIndex() = %d, want %d", wFinal.NextIndex(), wantLast+1)
	}
	if wFinal.Metadata().LatestSnapshotIndex != wantLast {
		t.Fatalf("final Metadata().LatestSnapshotIndex = %d, want %d", wFinal.Metadata().LatestSnapshotIndex, wantLast)
	}

	// The WAL must still accept new appends correctly positioned after
	// all of this repeated compaction.
	idx, err := wFinal.AppendLogEntry([]byte("post-cycles"))
	if err != nil {
		t.Fatalf("post-cycles AppendLogEntry: %v", err)
	}
	if idx != wantLast+1 {
		t.Fatalf("post-cycles AppendLogEntry index = %d, want %d", idx, wantLast+1)
	}
}
