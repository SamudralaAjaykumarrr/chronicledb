package backup_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// testHistory builds n sequential CommitTxn commands, each writing one
// disjoint key (so none ever conflicts regardless of StartSeq), and
// durably appends them — in order, indices 1..n — to a fresh WAL at
// dir. It returns the open WAL (ready to use as backup.Source.WAL) and
// the plain command list, so a test can independently reconstruct a
// reference FSM at any prefix boundary via fsmAtBoundary.
func testHistory(t *testing.T, dir string, n int) (*wal.WAL, []fsm.CommitTxnCommand) {
	t.Helper()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("opening source WAL: %v", err)
	}
	cmds := make([]fsm.CommitTxnCommand, 0, n)
	for i := 1; i <= n; i++ {
		cmd := fsm.CommitTxnCommand{
			RequestID: fsm.RequestID(fmt.Sprintf("r%d", i)),
			TxnID:     uint64(i),
			StartSeq:  0,
			Mutations: []mvcc.Mutation{{Key: fmt.Sprintf("k%d", i), Value: []byte(fmt.Sprintf("v%d", i))}},
		}
		idx, err := w.AppendLogEntry(fsm.EncodeCommitTxn(cmd))
		if err != nil {
			t.Fatalf("appending entry %d: %v", i, err)
		}
		if idx != uint64(i) {
			t.Fatalf("entry %d assigned index %d", i, idx)
		}
		cmds = append(cmds, cmd)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("syncing source WAL: %v", err)
	}
	return w, cmds
}

// fsmAtBoundary constructs a fresh FSM by applying cmds[:upto], in
// order, starting at index 1 — an independent reference for what any
// node's state deterministically must be at that boundary
// (docs/architecture.md §5, ADR-0007).
func fsmAtBoundary(t *testing.T, cmds []fsm.CommitTxnCommand, upto uint64) *fsm.FSM {
	t.Helper()
	f := fsm.New(mvcc.NewStore())
	for i := uint64(0); i < upto; i++ {
		if _, err := f.Apply(i+1, cmds[i]); err != nil {
			t.Fatalf("applying reference command %d: %v", i+1, err)
		}
	}
	return f
}

// requireSameState fails the test unless a and b's exported MVCC
// version chains are identical — the independent, structural proof that
// two FSMs (e.g. "state reconstructed from a restored data directory"
// vs. "state an independent reference model computed") represent
// exactly the same committed history, tombstones and all
// (docs/enterprise-v1-plan.md §6 "MVCC state").
func requireSameState(t *testing.T, got, want *fsm.FSM) {
	t.Helper()
	gotChains := got.Store().Export()
	wantChains := want.Store().Export()
	if !reflect.DeepEqual(gotChains, wantChains) {
		t.Fatalf("MVCC state mismatch:\n got:  %+v\n want: %+v", gotChains, wantChains)
	}
}

// applyRestoredWAL opens the WAL and snapshot at dataDir (exactly as
// internal/node.Open's own recovery sequence does, docs/recovery.md §1)
// and returns the fully-recovered FSM: the base snapshot's state (or a
// fresh empty store if none), with every WAL entry beyond it applied in
// order. This is deliberately independent of Restore's own internals —
// it re-derives state the same way a real node restart would, so a
// round-trip test proves Restore produced a data directory real
// recovery code actually accepts and reconstructs correctly, not merely
// that Restore's own bookkeeping is self-consistent.
func applyRestoredWAL(t *testing.T, dataDir string) *fsm.FSM {
	t.Helper()
	w, _, err := wal.Open(dataDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening restored WAL: %v", err)
	}
	defer w.Close()

	meta := w.Metadata()
	snapMgr, err := snapshot.NewManager(filepath.Join(dataDir, "snapshot"))
	if err != nil {
		t.Fatalf("opening restored snapshot manager: %v", err)
	}
	var f *fsm.FSM
	if meta.LatestSnapshotIndex > 0 {
		snap, ok, err := snapMgr.Load(meta.LatestSnapshotIndex)
		if err != nil || !ok {
			t.Fatalf("loading restored snapshot at %d: ok=%v err=%v", meta.LatestSnapshotIndex, ok, err)
		}
		f = snap.FSM
	} else {
		f = fsm.New(mvcc.NewStore())
	}

	it, err := w.Replay(meta.LatestSnapshotIndex + 1)
	if err != nil {
		t.Fatalf("opening restored WAL replay: %v", err)
	}
	defer it.Close()
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatalf("replaying restored WAL: %v", err)
		}
		if !ok {
			break
		}
		cmd, err := fsm.DecodeCommitTxn(rec.Payload)
		if err != nil {
			t.Fatalf("decoding restored entry %d: %v", rec.Index, err)
		}
		if _, err := f.Apply(rec.Index, cmd); err != nil {
			t.Fatalf("applying restored entry %d: %v", rec.Index, err)
		}
	}
	return f
}

func TestExportRestore_SnapshotOnlyRoundTrip(t *testing.T) {
	srcDir := t.TempDir()
	w, cmds := testHistory(t, srcDir, 5)
	defer w.Close()

	baseFSM := fsmAtBoundary(t, cmds, 5)
	src := backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: 5, LastIncludedTerm: 1},
		BaseFSM:  baseFSM,
		WAL:      w,
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	m, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: 5, ClusterID: "test-cluster"})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if m.LastIncludedIndex != 5 || m.WALUntilIndex != 5 || len(m.WALSegments) == 0 {
		t.Fatalf("unexpected manifest for snapshot-only backup: %+v", m)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	res, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if res.RestoredUntilIndex != 5 {
		t.Fatalf("RestoredUntilIndex = %d, want 5", res.RestoredUntilIndex)
	}

	got := applyRestoredWAL(t, dataDir)
	requireSameState(t, got, fsmAtBoundary(t, cmds, 5))
}

func TestExportRestore_ContinuousPITRRoundTrip(t *testing.T) {
	srcDir := t.TempDir()
	w, cmds := testHistory(t, srcDir, 10)
	defer w.Close()

	// Base snapshot boundary is deliberately earlier than the WAL's own
	// head, simulating a node that snapshotted at index 3 and has since
	// committed 7 more entries — exactly the "incremental/PITR backup"
	// shape docs/enterprise-v1-plan.md §6 describes.
	baseFSM := fsmAtBoundary(t, cmds, 3)
	src := backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: 3, LastIncludedTerm: 1},
		BaseFSM:  baseFSM,
		WAL:      w,
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	m, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: backup.UntilLatest, ClusterID: "test-cluster"})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if m.WALUntilIndex != 10 {
		t.Fatalf("WALUntilIndex = %d, want 10", m.WALUntilIndex)
	}

	for _, boundary := range []uint64{3, 6, 9, 10} {
		t.Run(fmt.Sprintf("boundary=%d", boundary), func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "restored")
			res, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: boundary})
			if err != nil {
				t.Fatalf("Restore to %d: %v", boundary, err)
			}
			if res.RestoredUntilIndex != boundary {
				t.Fatalf("RestoredUntilIndex = %d, want %d", res.RestoredUntilIndex, boundary)
			}
			got := applyRestoredWAL(t, dataDir)
			requireSameState(t, got, fsmAtBoundary(t, cmds, boundary))

			// Transactions strictly after the selected boundary must
			// never appear: the restored WAL's own next-assignable index
			// proves no entry beyond `boundary` was ever written.
			rw, _, err := wal.Open(dataDir, wal.Options{})
			if err != nil {
				t.Fatalf("reopening restored WAL: %v", err)
			}
			if next := rw.NextIndex(); next != boundary+1 {
				t.Fatalf("restored WAL NextIndex() = %d, want %d (entries beyond the PITR boundary leaked in)", next, boundary+1)
			}
			rw.Close()
		})
	}
}

// TestExportRestore_MVCCTombstonesAndAtomicMultiKeyMutationsPreserved
// closes a specific v0.3.0 acceptance-criterion gap: testHistory (used
// by every other round-trip test in this file) only ever writes one
// fresh key per commit, so no existing test here actually exercises a
// tombstone (delete) surviving a backup/restore round trip, nor a
// single transaction's multiple key mutations restoring atomically
// together. docs/enterprise-v1-plan.md §6 and docs/backup.md §2 both
// state a backup captures "every key's MVCC version chain, including
// tombstones" — this test is the direct proof of that specific claim,
// not an inference from the generic snapshot-only/PITR round-trip
// tests above.
func TestExportRestore_MVCCTombstonesAndAtomicMultiKeyMutationsPreserved(t *testing.T) {
	srcDir := t.TempDir()
	w, _, err := wal.Open(srcDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening source WAL: %v", err)
	}
	defer w.Close()

	cmds := []fsm.CommitTxnCommand{
		// Atomic multi-key write: a single transaction writing two
		// distinct keys together.
		{
			RequestID: "r1", TxnID: 1, StartSeq: 0,
			Mutations: []mvcc.Mutation{
				{Key: "a", Value: []byte("a-v1")},
				{Key: "b", Value: []byte("b-v1")},
			},
		},
		// Tombstone: deletes "a".
		{
			RequestID: "r2", TxnID: 2, StartSeq: 1,
			Mutations: []mvcc.Mutation{{Key: "a", Tombstone: true}},
		},
		// Atomic multi-key again: a fresh key "c" written in the same
		// transaction that tombstones "b".
		{
			RequestID: "r3", TxnID: 3, StartSeq: 2,
			Mutations: []mvcc.Mutation{
				{Key: "c", Value: []byte("c-v1")},
				{Key: "b", Tombstone: true},
			},
		},
	}
	for i, cmd := range cmds {
		idx, err := w.AppendLogEntry(fsm.EncodeCommitTxn(cmd))
		if err != nil {
			t.Fatalf("appending entry %d: %v", i+1, err)
		}
		if idx != uint64(i+1) {
			t.Fatalf("entry %d assigned index %d", i+1, idx)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("syncing source WAL: %v", err)
	}

	src := backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: 0, LastIncludedTerm: 1},
		BaseFSM:  fsm.New(mvcc.NewStore()),
		WAL:      w,
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: backup.UntilLatest, ClusterID: "test-cluster"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got := applyRestoredWAL(t, dataDir)
	requireSameState(t, got, fsmAtBoundary(t, cmds, uint64(len(cmds))))

	// Directly assert the tombstone/multi-key shape survived, not just
	// structural equality with the reference model (which would also
	// pass if both sides shared the same bug).
	chains := got.Store().Export()
	byKey := make(map[string][]mvcc.Version, len(chains))
	for _, kc := range chains {
		byKey[kc.Key] = kc.Versions
	}

	a := byKey["a"]
	if len(a) != 2 || a[0].Tombstone || string(a[0].Value) != "a-v1" || !a[1].Tombstone {
		t.Fatalf("key %q restored chain = %+v, want [write(a-v1), tombstone]", "a", a)
	}
	b := byKey["b"]
	if len(b) != 2 || b[0].Tombstone || string(b[0].Value) != "b-v1" || !b[1].Tombstone {
		t.Fatalf("key %q restored chain = %+v, want [write(b-v1), tombstone]", "b", b)
	}
	c := byKey["c"]
	if len(c) != 1 || c[0].Tombstone || string(c[0].Value) != "c-v1" {
		t.Fatalf("key %q restored chain = %+v, want [write(c-v1)]", "c", c)
	}
}

func TestRestore_UntilIndexOutOfRangeRejected(t *testing.T) {
	srcDir := t.TempDir()
	w, cmds := testHistory(t, srcDir, 5)
	defer w.Close()
	src := backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: 2, LastIncludedTerm: 1},
		BaseFSM:  fsmAtBoundary(t, cmds, 2),
		WAL:      w,
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	cases := []uint64{0, 1, 6, 100}
	for _, until := range cases {
		dataDir := filepath.Join(t.TempDir(), "restored")
		if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: until}); err == nil {
			t.Fatalf("Restore with out-of-range UntilIndex=%d unexpectedly succeeded", until)
		}
		assertDirAbsentOrEmpty(t, dataDir)
	}
}

// assertDirAbsentOrEmpty fails the test unless dir does not exist or
// exists but is empty — the proof that a rejected Restore never leaves
// any partial state behind (BACKUP CONSISTENCY / ATOMICITY).
func assertDirAbsentOrEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatalf("checking %s: %v", dir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("%s is not empty after a rejected/failed Restore: %v", dir, entries)
	}
}

func setupValidBackup(t *testing.T, n int, boundary uint64) (backupDir string, cmds []fsm.CommitTxnCommand) {
	t.Helper()
	srcDir := t.TempDir()
	w, cmds := testHistory(t, srcDir, n)
	t.Cleanup(func() { w.Close() })
	src := backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: boundary, LastIncludedTerm: 1},
		BaseFSM:  fsmAtBoundary(t, cmds, boundary),
		WAL:      w,
	}
	backupDir = filepath.Join(t.TempDir(), "backup")
	if _, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return backupDir, cmds
}

func TestRestore_CorruptedManifestChecksumRejected(t *testing.T) {
	backupDir, _ := setupValidBackup(t, 5, 2)
	corruptByteAt(t, filepath.Join(backupDir, "manifest.json"), 5)

	dataDir := filepath.Join(t.TempDir(), "restored")
	_, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest})
	if err == nil {
		t.Fatal("Restore against a tampered manifest unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_TruncatedManifestRejected(t *testing.T) {
	backupDir, _ := setupValidBackup(t, 5, 2)
	p := filepath.Join(backupDir, "manifest.json")
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading manifest: %v", err)
	}
	if err := os.WriteFile(p, data[:len(data)/2], 0o644); err != nil {
		t.Fatalf("truncating manifest: %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore against a truncated manifest unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_MissingManifestRejected(t *testing.T) {
	// Simulates a backup interrupted before Export's final atomic
	// manifest write ever happened — exactly the crash-mid-export case
	// docs/enterprise-v1-plan.md §6's Failure semantics describes.
	backupDir, _ := setupValidBackup(t, 5, 2)
	if err := os.Remove(filepath.Join(backupDir, "manifest.json")); err != nil {
		t.Fatalf("removing manifest: %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore against a backup with no manifest unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_CorruptedSnapshotRejected(t *testing.T) {
	backupDir, _ := setupValidBackup(t, 5, 2)
	snapPath := findFile(t, filepath.Join(backupDir, "snapshot"), ".snap")
	corruptByteAt(t, snapPath, 10)

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore against a corrupted snapshot unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_MissingSnapshotFileRejected(t *testing.T) {
	backupDir, _ := setupValidBackup(t, 5, 2)
	snapPath := findFile(t, filepath.Join(backupDir, "snapshot"), ".snap")
	if err := os.Remove(snapPath); err != nil {
		t.Fatalf("removing snapshot file: %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore with a missing snapshot file unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_CorruptedWALSegmentRejected(t *testing.T) {
	backupDir, _ := setupValidBackup(t, 5, 2)
	segPath := findFile(t, filepath.Join(backupDir, "wal"), ".seg")
	corruptByteAt(t, segPath, 20)

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore against a corrupted WAL segment unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_MissingWALSegmentRejected(t *testing.T) {
	backupDir, _ := setupValidBackup(t, 5, 2)
	segPath := findFile(t, filepath.Join(backupDir, "wal"), ".seg")
	if err := os.Remove(segPath); err != nil {
		t.Fatalf("removing WAL segment: %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore with a missing WAL segment unexpectedly succeeded")
	}
	assertDirAbsentOrEmpty(t, dataDir)
}

func TestRestore_TargetNotCleanRequiresForce(t *testing.T) {
	backupDir, cmds := setupValidBackup(t, 5, 2)
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "unexpected.txt"), []byte("pre-existing"), 0o644); err != nil {
		t.Fatalf("seeding target dir: %v", err)
	}

	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err == nil {
		t.Fatal("Restore into a non-clean directory without Force unexpectedly succeeded")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "unexpected.txt")); err != nil {
		t.Fatalf("pre-existing file was disturbed by a rejected restore: %v", err)
	}

	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest, Force: true}); err != nil {
		t.Fatalf("forced Restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "unexpected.txt")); !os.IsNotExist(err) {
		t.Fatalf("forced restore should have replaced pre-existing directory content, err=%v", err)
	}
	got := applyRestoredWAL(t, dataDir)
	requireSameState(t, got, fsmAtBoundary(t, cmds, 5))
}

// TestRestore_InterruptedRestoreRetryIsSafe simulates a crash partway
// through a previous Restore attempt by pre-seeding the fixed staging
// directory name Restore uses with garbage content, then confirms a
// fresh Restore call still succeeds and produces exactly the correct
// state — proving the documented "restarting the restore from the same
// backup directory is safe/idempotent" failure semantic
// (docs/enterprise-v1-plan.md §6).
func TestRestore_InterruptedRestoreRetryIsSafe(t *testing.T) {
	backupDir, cmds := setupValidBackup(t, 6, 2)
	dataDir := filepath.Join(t.TempDir(), "restored")

	staging := dataDir + ".restore-staging"
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatalf("seeding stale staging dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staging, "garbage"), []byte("leftover from a killed restore"), 0o644); err != nil {
		t.Fatalf("seeding stale staging dir: %v", err)
	}

	if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Restore after a simulated prior interruption: %v", err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatalf("stale staging directory should be gone after a successful restore, err=%v", err)
	}
	got := applyRestoredWAL(t, dataDir)
	requireSameState(t, got, fsmAtBoundary(t, cmds, 6))
}

func TestRestore_RepeatedRestoreIntoFreshDirsIsDeterministic(t *testing.T) {
	backupDir, cmds := setupValidBackup(t, 6, 2)

	first := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, first, backup.RestoreOptions{UntilIndex: 5}); err != nil {
		t.Fatalf("first Restore: %v", err)
	}
	second := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(backupDir, second, backup.RestoreOptions{UntilIndex: 5}); err != nil {
		t.Fatalf("second Restore: %v", err)
	}

	got1 := applyRestoredWAL(t, first)
	got2 := applyRestoredWAL(t, second)
	want := fsmAtBoundary(t, cmds, 5)
	requireSameState(t, got1, want)
	requireSameState(t, got2, want)
}

func findFile(t *testing.T, dir, suffix string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() && filepathHasSuffix(e.Name(), suffix) {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("no file with suffix %s found in %s", suffix, dir)
	return ""
}

func filepathHasSuffix(name, suffix string) bool {
	return len(name) >= len(suffix) && name[len(name)-len(suffix):] == suffix
}

func corruptByteAt(t *testing.T, path string, offset int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if offset >= len(data) {
		offset = len(data) - 1
	}
	data[offset] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("writing corrupted %s: %v", path, err)
	}
}
