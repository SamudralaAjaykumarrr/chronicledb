package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

func newManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func TestManagerCreateAndLoad(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	meta := Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}
	if _, err := m.Create(meta, f); err != nil {
		t.Fatalf("Create: %v", err)
	}
	snap, ok, err := m.Load(3)
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if snap.Meta != meta {
		t.Fatalf("Meta mismatch: %+v", snap.Meta)
	}
}

func TestManagerLoadZeroPointerReturnsNothing(t *testing.T) {
	m := newManager(t)
	if _, ok, err := m.Load(0); ok || err != nil {
		t.Fatalf("expected no snapshot for pointer 0, got ok=%v err=%v", ok, err)
	}
}

func TestManagerLoadIgnoresSnapshotNewerThanPointer(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// pointerIndex older than the file on disk: simulates a crash after
	// Create but before AppendMetadataSnapshot ever adopted it.
	if _, ok, err := m.Load(0); ok || err != nil {
		t.Fatalf("expected the not-yet-adopted snapshot to be ignored, got ok=%v err=%v", ok, err)
	}
}

func TestManagerRetainsOnlyLatestAfterNewCreate(t *testing.T) {
	m := newManager(t)
	f1 := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f1); err != nil {
		t.Fatalf("Create 1: %v", err)
	}
	f2 := fsm.New(mvcc.NewStore())
	if _, err := f2.Apply(1, fsm.CommitTxnCommand{RequestID: "x", Mutations: []mvcc.Mutation{{Key: "z", Value: []byte("v")}}}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := m.Create(Meta{LastIncludedIndex: 10, LastIncludedTerm: 2}, f2); err != nil {
		t.Fatalf("Create 2: %v", err)
	}
	cands, err := m.candidatesDescending()
	if err != nil {
		t.Fatalf("candidatesDescending: %v", err)
	}
	if len(cands) != 1 || cands[0].index != 10 {
		t.Fatalf("expected exactly one retained snapshot at index 10, got %+v", cands)
	}
}

func TestManagerLoadFallsBackOnCorruption(t *testing.T) {
	m := newManager(t)
	f1 := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f1); err != nil {
		t.Fatalf("Create 1: %v", err)
	}
	// Manually place a second, newer, corrupted snapshot file bypassing
	// pruning (simulating two generations coexisting).
	f2 := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 10, LastIncludedTerm: 2}, f2)
	data[len(data)-1] ^= 0xFF // corrupt checksum
	if err := os.WriteFile(m.path(10), data, 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	snap, ok, err := m.Load(10)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("expected fallback to older valid snapshot")
	}
	if snap.Meta.LastIncludedIndex != 3 {
		t.Fatalf("expected fallback to index 3, got %d", snap.Meta.LastIncludedIndex)
	}
}

func TestManagerInstallValidatesBeforeWriting(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f)
	data[len(data)-1] ^= 0xFF
	if _, err := m.Install(data); err == nil {
		t.Fatal("expected Install to reject corrupted data")
	}
	entries, _ := os.ReadDir(m.dir)
	for _, e := range entries {
		if !e.IsDir() && e.Name() != tmpDirName {
			t.Fatalf("Install must not write anything to disk on validation failure, found %s", e.Name())
		}
	}
}

func TestManagerInstallSucceedsAndIsLoadable(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f)
	snap, err := m.Install(data)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if snap.Meta.LastIncludedIndex != 3 {
		t.Fatalf("unexpected meta: %+v", snap.Meta)
	}
	loaded, ok, err := m.Load(3)
	if err != nil || !ok {
		t.Fatalf("Load after Install: ok=%v err=%v", ok, err)
	}
	if loaded.Meta != snap.Meta {
		t.Fatalf("meta mismatch after reload")
	}
}

func TestManagerCleansStaleTempFilesOnOpen(t *testing.T) {
	dir := t.TempDir()
	m1, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	stale := filepath.Join(dir, tmpDirName, "leftover.tmp")
	if err := os.WriteFile(stale, []byte("garbage"), 0o644); err != nil {
		t.Fatalf("write stale temp file: %v", err)
	}
	_ = m1

	m2, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager (reopen): %v", err)
	}
	_ = m2
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("expected stale temp file to be cleaned up, stat err=%v", err)
	}
}

func TestManagerBytesRoundTrip(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	meta := Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}
	if _, err := m.Create(meta, f); err != nil {
		t.Fatalf("Create: %v", err)
	}
	raw, ok, err := m.Bytes(3)
	if err != nil || !ok {
		t.Fatalf("Bytes: ok=%v err=%v", ok, err)
	}
	snap, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode(Bytes()): %v", err)
	}
	if snap.Meta != meta {
		t.Fatalf("meta mismatch via Bytes round trip")
	}
	if _, ok, err := m.Bytes(999); ok || err != nil {
		t.Fatalf("expected ok=false for missing snapshot, got ok=%v err=%v", ok, err)
	}
}

func TestManagerCreateLeavesNoTempFileOnSuccess(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f); err != nil {
		t.Fatalf("Create: %v", err)
	}
	entries, err := os.ReadDir(m.tmpDir())
	if err != nil {
		t.Fatalf("ReadDir tmp: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no leftover temp files, found %d", len(entries))
	}
}
