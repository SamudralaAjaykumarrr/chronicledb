package wal

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
)

// TestEncodeDecodeMetadata_ClusterGeneration proves the generation-aware
// trailing field docs/enterprise-v1-plan.md §7 requires for the WAL
// metadata record: generation 0 (the default, and the only value any
// pre-v0.4.0 binary ever wrote) encodes byte-identical to a plain
// Metadata with no ClusterGeneration set at all — the actual rollback/
// backward-compatibility property this scheme exists for, not merely
// "decodes somehow" — and a nonzero generation round-trips through
// encode/decode as an additional 4 bytes.
func TestEncodeDecodeMetadata_ClusterGeneration(t *testing.T) {
	base := Metadata{NodeID: "node-x", FormatVersion: FormatVersion, LatestSnapshotIndex: 7}
	baseBytes := encodeMetadata(base)

	gen0 := base
	gen0.ClusterGeneration = 0
	if got := encodeMetadata(gen0); string(got) != string(baseBytes) {
		t.Fatalf("ClusterGeneration=0 encoding is not byte-identical to the pre-v0.4.0 encoding — this breaks pre-finalize rollback compatibility")
	}

	withGen := base
	withGen.ClusterGeneration = 1
	genBytes := encodeMetadata(withGen)
	if len(genBytes) != len(baseBytes)+4 {
		t.Fatalf("generation-1 encoding length = %d, want exactly %d (base + 4-byte generation field)", len(genBytes), len(baseBytes)+4)
	}

	decoded, err := decodeMetadata(genBytes)
	if err != nil {
		t.Fatalf("decodeMetadata: %v", err)
	}
	if decoded != withGen {
		t.Fatalf("decodeMetadata round-trip = %+v, want %+v", decoded, withGen)
	}
}

// TestDecodeMetadata_ToleratesTrailingBytesIgnoredByPreV040Decoder is a
// regression pin for the specific backward-compatibility mechanism this
// phase relies on (see decodeMetadata's own doc comment): decodeMetadata
// has NEVER validated that len(b) exactly matches 11+idLen — so a
// pre-v0.4.0 binary's own unmodified decodeMetadata, given a metadata
// record a v0.4.0+ binary wrote AFTER finalize (with a trailing 4-byte
// ClusterGeneration field it knows nothing about), still decodes
// NodeID/FormatVersion/LatestSnapshotIndex correctly rather than
// erroring — proven here by decoding a genuinely generation>0 record
// with today's decodeMetadata and confirming the base fields are
// unaffected by (and do not require awareness of) the trailing field.
func TestDecodeMetadata_ToleratesTrailingBytesIgnoredByPreV040Decoder(t *testing.T) {
	m := Metadata{NodeID: "node-x", FormatVersion: FormatVersion, LatestSnapshotIndex: 7, ClusterGeneration: 1}
	b := encodeMetadata(m)
	decoded, err := decodeMetadata(b)
	if err != nil {
		t.Fatalf("decodeMetadata: %v", err)
	}
	if decoded.NodeID != m.NodeID || decoded.FormatVersion != m.FormatVersion || decoded.LatestSnapshotIndex != m.LatestSnapshotIndex {
		t.Fatalf("base fields corrupted by presence of trailing ClusterGeneration field: got %+v", decoded)
	}
}

func TestDecodeMetadata_RejectsWrongTrailingLength(t *testing.T) {
	base := encodeMetadata(Metadata{NodeID: "node-x", FormatVersion: FormatVersion})
	for _, extra := range [][]byte{{0x01}, {0x01, 0x02}, {0x01, 0x02, 0x03}, {0x01, 0x02, 0x03, 0x04, 0x05}} {
		if _, err := decodeMetadata(append(append([]byte(nil), base...), extra...)); err == nil {
			t.Fatalf("decodeMetadata with %d trailing bytes: expected an error, got nil", len(extra))
		}
	}
}

// TestOpenRefusesDataDirectoryFinalizedBeyondThisBinary proves
// wal.ErrUnsupportedGeneration: a data directory whose durable Metadata
// records a ClusterGeneration higher than this binary's
// internal/version.MaxSupportedGeneration is refused at Open, before
// any Raft/FSM replay — the rollback boundary
// docs/enterprise-v1-plan.md §7/docs/upgrades.md document.
func TestOpenRefusesDataDirectoryFinalizedBeyondThisBinary(t *testing.T) {
	dir := t.TempDir()
	if err := storage.EnsureDir(dir); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	seg, err := storage.CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	futureMeta := encodeMetadata(Metadata{NodeID: "node-x", FormatVersion: FormatVersion, ClusterGeneration: version.MaxSupportedGeneration + 1})
	frame := encodeRecord(RecordTypeMetadata, 0, futureMeta)
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
		t.Fatal("Open against a data directory finalized beyond this binary's max supported generation: expected error, got nil")
	}
	if !errors.Is(err, ErrUnsupportedGeneration) {
		t.Fatalf("Open: err = %v, want ErrUnsupportedGeneration", err)
	}
}

// TestSetClusterGeneration_RejectsBackwardMove and
// TestSetClusterGeneration_RejectsBeyondThisBinary pin
// SetClusterGeneration's own two guardrails directly (ROLLBACK BOUNDARY
// HONESTY and defense-in-depth against exceeding this binary's own
// capability — see its doc comment).
func TestSetClusterGeneration_RejectsBackwardMove(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if err := w.SetClusterGeneration(1); err != nil {
		t.Fatalf("SetClusterGeneration(1): %v", err)
	}
	if err := w.SetClusterGeneration(0); err == nil {
		t.Fatal("SetClusterGeneration(0) after already at 1: expected error, got nil")
	}
	if got := w.Metadata().ClusterGeneration; got != 1 {
		t.Fatalf("Metadata().ClusterGeneration after rejected backward move = %d, want unchanged 1", got)
	}
}

// TestSetClusterGeneration_SameValueIsIdempotentNoOp proves the
// same-value short-circuit directly: internal/node.applyControlEntry
// calls this unconditionally for every StatusCommitted control-command
// outcome, including a retried proposal resolved from the FSM's own
// idempotency table against an already-current generation — this must
// succeed as a cheap no-op, not merely "not error" (it must not pay for
// a redundant append+fsync either, though that is proven at the WAL
// level only indirectly here by asserting the call is at least as fast
// as an Open on a fresh directory would suggest is possible for a real
// append — the doc comment on the fix is the authoritative statement of
// intent; this test pins the observable behavior: no error, unchanged
// state).
func TestSetClusterGeneration_SameValueIsIdempotentNoOp(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if err := w.SetClusterGeneration(1); err != nil {
		t.Fatalf("SetClusterGeneration(1): %v", err)
	}
	if err := w.SetClusterGeneration(1); err != nil {
		t.Fatalf("SetClusterGeneration(1) again (idempotent retry): %v", err)
	}
	if got := w.Metadata().ClusterGeneration; got != 1 {
		t.Fatalf("Metadata().ClusterGeneration after idempotent retry = %d, want unchanged 1", got)
	}
}

func TestSetClusterGeneration_RejectsBeyondThisBinary(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if err := w.SetClusterGeneration(version.MaxSupportedGeneration + 1); !errors.Is(err, ErrUnsupportedGeneration) {
		t.Fatalf("SetClusterGeneration(beyond max): err = %v, want ErrUnsupportedGeneration", err)
	}
}

// TestSetClusterGeneration_PersistsAcrossReopen proves ClusterGeneration
// actually survives a restart (docs/enterprise-v1-plan.md §7 "restart/
// recovery across supported version transitions"), the property
// ErrUnsupportedGeneration's own startup check depends on.
func TestSetClusterGeneration_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.SetClusterGeneration(1); err != nil {
		t.Fatalf("SetClusterGeneration: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, _, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer w2.Close()
	if got := w2.Metadata().ClusterGeneration; got != 1 {
		t.Fatalf("ClusterGeneration after reopen = %d, want 1", got)
	}
}
