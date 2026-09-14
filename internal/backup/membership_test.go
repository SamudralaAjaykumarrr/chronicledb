package backup

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// buildTypedEntryPayload mirrors internal/node/storage.go's
// encodeEntryPayload for a typed (Type != EntryNormal) entry: this test
// file deliberately does not import internal/node (avoiding a real
// import cycle risk and keeping the pinned byte layout explicit here),
// only internal/raft for the EntryConfig payload itself.
func buildTypedEntryPayload(term uint64, entryType byte, data []byte) []byte {
	buf := make([]byte, 8+1+1+len(data))
	binary.BigEndian.PutUint64(buf, term)
	buf[8] = 0xFF
	buf[9] = entryType
	copy(buf[10:], data)
	return buf
}

func TestVoidConfigEntryPayloadMatchesRaftFraming(t *testing.T) {
	voided := raft.EncodeVoidedEntryConfigPayload()
	if !bytes.Equal(voided, voidedConfigChangePayload) {
		t.Fatalf("internal/backup's independent voided-payload encoding (%x) does not match internal/raft's EncodeVoidedEntryConfigPayload() (%x) — the two packages' framing has drifted", voidedConfigChangePayload, voided)
	}

	// A live EntryConfig payload (raft.EntryConfig == 1): the exact
	// bytes internal/raft.encodeConfigChange would have produced for an
	// AddLearner change are irrelevant here beyond "non-empty and not
	// already voided" — buildTypedEntryPayload wraps arbitrary data in
	// the node-level typed-entry header.
	liveConfigData := append([]byte{0xF0, 16}, []byte("pretend-config-bytes")...)
	payload := buildTypedEntryPayload(7, 1 /* raft.EntryConfig */, liveConfigData)

	got := voidConfigEntryPayload(payload)
	want := buildTypedEntryPayload(7, 1, voided)
	if !bytes.Equal(got, want) {
		t.Fatalf("voidConfigEntryPayload(%x) = %x, want %x", payload, got, want)
	}
}

func TestVoidConfigEntryPayloadPassesThroughNonConfigEntries(t *testing.T) {
	// An EntryNormal (untyped) payload: term(8B) || data.
	untyped := make([]byte, 8+5)
	binary.BigEndian.PutUint64(untyped, 3)
	copy(untyped[8:], []byte("hello"))
	if got := voidConfigEntryPayload(untyped); !bytes.Equal(got, untyped) {
		t.Fatalf("an untyped EntryNormal payload must pass through unchanged, got %x want %x", got, untyped)
	}

	// A typed but non-EntryConfig entry (e.g. some future EntryType) must
	// also pass through unchanged.
	typedOther := buildTypedEntryPayload(3, 99, []byte("data"))
	if got := voidConfigEntryPayload(typedOther); !bytes.Equal(got, typedOther) {
		t.Fatalf("a typed non-EntryConfig payload must pass through unchanged, got %x want %x", got, typedOther)
	}

	// An already-voided EntryConfig payload is idempotent under a second
	// application.
	alreadyVoided := buildTypedEntryPayload(3, 1, voidedConfigChangePayload)
	if got := voidConfigEntryPayload(alreadyVoided); !bytes.Equal(got, alreadyVoided) {
		t.Fatalf("voiding an already-voided payload must be a no-op, got %x want %x", got, alreadyVoided)
	}
}

func buildV2Snapshot(t *testing.T, hasConfig bool) []byte {
	t.Helper()
	f := fsm.New(mvcc.NewStore())
	if _, err := f.Apply(1, fsm.CommitTxnCommand{RequestID: "r1", Mutations: []mvcc.Mutation{{Key: "k", Value: []byte("v")}}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	meta := snapshot.Meta{LastIncludedIndex: 1, LastIncludedTerm: 1}
	if hasConfig {
		meta.HasConfiguration = true
		meta.Configuration = snapshot.Configuration{Voters: []snapshot.Member{{ID: "src-a", Address: "src-a:1"}}}
	}
	return snapshot.Encode(meta, f, snapshot.FormatVersion)
}

func TestStripSnapshotConfigurationV1NeedsNoTransform(t *testing.T) {
	f := fsm.New(mvcc.NewStore())
	v1 := snapshot.Encode(snapshot.Meta{LastIncludedIndex: 1, LastIncludedTerm: 1}, f, 1)
	got, err := stripSnapshotConfiguration(v1)
	if err != nil {
		t.Fatalf("stripSnapshotConfiguration: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatal("a v1 source snapshot (already HasConfiguration=false) must pass through byte-identical")
	}
}

func TestStripSnapshotConfigurationClearsV2Configuration(t *testing.T) {
	src := buildV2Snapshot(t, true)
	stripped, err := stripSnapshotConfiguration(src)
	if err != nil {
		t.Fatalf("stripSnapshotConfiguration: %v", err)
	}
	snap, err := snapshot.Decode(stripped)
	if err != nil {
		t.Fatalf("Decode(stripped): %v", err)
	}
	if snap.Meta.HasConfiguration {
		t.Fatal("expected HasConfiguration=false after stripping")
	}
	if len(snap.Meta.Configuration.Voters) != 0 {
		t.Fatalf("expected an empty Configuration after stripping, got %+v", snap.Meta.Configuration)
	}
	if snap.Meta.LastIncludedIndex != 1 || snap.Meta.LastIncludedTerm != 1 {
		t.Fatalf("stripping must preserve the consensus boundary exactly, got %+v", snap.Meta)
	}
	if _, found := snap.FSM.Store().Visible("k", 1); !found {
		t.Fatal("stripping must preserve FSM state exactly")
	}
}

// TestRestoreVoidsConfigEntryInStagedWAL is DM-21's core mechanism
// proof at the package level (the full multi-node consequence — a
// restored cluster electing a leader — is internal/fault's job):
// Export must copy a live EntryConfig entry through unvoided (§23/G6),
// while Restore's own buildStaging must void the copy it stages.
func TestRestoreVoidsConfigEntryInStagedWAL(t *testing.T) {
	srcDir := t.TempDir()
	srcW, _, err := wal.Open(srcDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening source WAL: %v", err)
	}
	configPayload := buildTypedEntryPayload(1, 1 /* raft.EntryConfig */, append([]byte{0xF0, 16}, []byte("live-config")...))
	if _, err := srcW.AppendLogEntry(configPayload); err != nil {
		t.Fatalf("appending EntryConfig entry to source WAL: %v", err)
	}
	if err := srcW.Sync(); err != nil {
		t.Fatalf("syncing source WAL: %v", err)
	}

	src := Source{
		BaseMeta: snapshot.Meta{},
		BaseFSM:  fsm.New(mvcc.NewStore()),
		WAL:      srcW,
	}
	backupDir := t.TempDir()
	if _, err := Export(src, backupDir, ExportOptions{UntilIndex: UntilLatest}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := srcW.Close(); err != nil {
		t.Fatalf("closing source WAL: %v", err)
	}

	// Export's own backup must retain the entry unvoided (§23/G6).
	backupWAL, _, err := wal.Open(backupDir+"/wal", wal.Options{})
	if err != nil {
		t.Fatalf("opening backup WAL: %v", err)
	}
	it, err := backupWAL.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	rec, ok, err := it.Next()
	it.Close()
	backupWAL.Close()
	if err != nil || !ok {
		t.Fatalf("expected one record in the backup's own WAL, ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(rec.Payload, configPayload) {
		t.Fatalf("Export voided its own backup's EntryConfig entry — got %x, want the original %x", rec.Payload, configPayload)
	}

	// Restore's staged WAL must carry the voided form.
	dataDir := t.TempDir() + "/restored"
	if _, err := Restore(backupDir, dataDir, RestoreOptions{UntilIndex: UntilLatest}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restoredWAL, _, err := wal.Open(dataDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening restored WAL: %v", err)
	}
	defer restoredWAL.Close()
	it2, err := restoredWAL.Replay(1)
	if err != nil {
		t.Fatalf("Replay restored: %v", err)
	}
	defer it2.Close()
	rec2, ok, err := it2.Next()
	if err != nil || !ok {
		t.Fatalf("expected one record in the restored WAL, ok=%v err=%v", ok, err)
	}
	want := buildTypedEntryPayload(1, 1, voidedConfigChangePayload)
	if !bytes.Equal(rec2.Payload, want) {
		t.Fatalf("restored WAL entry = %x, want the voided form %x", rec2.Payload, want)
	}
}

// TestRestoreNegativeControlWithoutVoiding is DM-21's negative control
// (dynamic-membership plan §19 gate 3): disabling part 2 of the
// transform must leave the source's live EntryConfig entry intact in
// the staged WAL, demonstrating the transform is load-bearing.
func TestRestoreNegativeControlWithoutVoiding(t *testing.T) {
	srcDir := t.TempDir()
	srcW, _, err := wal.Open(srcDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening source WAL: %v", err)
	}
	configPayload := buildTypedEntryPayload(1, 1, append([]byte{0xF0, 16}, []byte("live-config")...))
	if _, err := srcW.AppendLogEntry(configPayload); err != nil {
		t.Fatalf("appending EntryConfig entry: %v", err)
	}
	if err := srcW.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	src := Source{BaseMeta: snapshot.Meta{}, BaseFSM: fsm.New(mvcc.NewStore()), WAL: srcW}
	backupDir := t.TempDir()
	if _, err := Export(src, backupDir, ExportOptions{UntilIndex: UntilLatest}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := srcW.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	voidRestoredMembershipEntries = false
	defer func() { voidRestoredMembershipEntries = true }()

	dataDir := t.TempDir() + "/restored"
	if _, err := Restore(backupDir, dataDir, RestoreOptions{UntilIndex: UntilLatest}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restoredWAL, _, err := wal.Open(dataDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening restored WAL: %v", err)
	}
	defer restoredWAL.Close()
	it, err := restoredWAL.Replay(1)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	defer it.Close()
	rec, ok, err := it.Next()
	if err != nil || !ok {
		t.Fatalf("expected one record, ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(rec.Payload, configPayload) {
		t.Fatal("with voiding disabled, the restored WAL should still carry the source's live (unvoided) EntryConfig entry — the negative control did not reproduce the defect §7.6 exists to prevent")
	}
}
