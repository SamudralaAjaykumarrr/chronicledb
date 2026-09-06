package fsm

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// buildRichFSM applies a small but representative command history:
// two keys with multiple versions each, a tombstone, and both a
// COMMITTED and an ABORTED (conflicting) outcome — everything
// EncodeState/DecodeState must round-trip (docs/snapshots.md §2).
func buildRichFSM(t *testing.T) *FSM {
	t.Helper()
	f := New(mvcc.NewStore())
	if _, err := f.Apply(1, CommitTxnCommand{
		RequestID: "r1", TxnID: 1, StartSeq: 0,
		Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("v1")}, {Key: "k2", Value: []byte("v2")}},
	}); err != nil {
		t.Fatalf("Apply(1): %v", err)
	}
	if _, err := f.Apply(2, CommitTxnCommand{
		RequestID: "r2", TxnID: 2, StartSeq: 1,
		Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("v1b")}, {Key: "k2", Tombstone: true}},
	}); err != nil {
		t.Fatalf("Apply(2): %v", err)
	}
	// Deliberate conflict: StartSeq=0 is stale relative to k1's latest
	// CommitSeq (2), so this aborts rather than commits.
	if _, err := f.Apply(3, CommitTxnCommand{
		RequestID: "r3", TxnID: 3, StartSeq: 0,
		Mutations: []mvcc.Mutation{{Key: "k1", Value: []byte("stale")}},
	}); err != nil {
		t.Fatalf("Apply(3): %v", err)
	}
	return f
}

// TestEncodeStateDeterministic proves two independently constructed
// FSMs that applied the identical command history produce
// byte-identical EncodeState output (docs/snapshots.md §2's
// determinism requirement — the property that makes snapshot content
// itself a reproducible fact, not construction-order-dependent).
func TestEncodeStateDeterministic(t *testing.T) {
	f1 := buildRichFSM(t)
	f2 := buildRichFSM(t)
	got1, got2 := f1.EncodeState(), f2.EncodeState()
	if string(got1) != string(got2) {
		t.Fatal("EncodeState diverged across two independently-constructed, identically-applied FSMs")
	}
}

// TestEncodeStateDecodeStateRoundTrip proves EncodeState/DecodeState
// are mutual inverses for a representative state: MVCC version chains
// (including a tombstone and an older still-reachable version),
// the RequestID outcome table (both COMMITTED and ABORTED entries,
// including ABORTED's conflict details), and outcome fingerprints
// (needed to detect a mismatched-payload reuse post-restore).
func TestEncodeStateDecodeStateRoundTrip(t *testing.T) {
	f := buildRichFSM(t)
	data := f.EncodeState()

	restored, maxSeq, err := DecodeState(data)
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	// r3 (index 3) ABORTED, so it never assigned a CommitSeq — index 3
	// legitimately appears nowhere in committed state, so maxSeq is the
	// highest *committed* CommitSeq (r2's, 2), not the highest applied
	// index.
	if maxSeq != 2 {
		t.Fatalf("maxSeq = %d, want 2", maxSeq)
	}

	if v, ok := restored.Store().Visible("k1", 3); !ok || string(v) != "v1b" {
		t.Fatalf("k1 visible@3 = %q ok=%v, want v1b", v, ok)
	}
	if v, ok := restored.Store().Visible("k1", 1); !ok || string(v) != "v1" {
		t.Fatalf("k1 visible@1 = %q ok=%v, want v1 (older version still reachable)", v, ok)
	}
	if _, ok := restored.Store().Visible("k2", 3); ok {
		t.Fatal("k2 must be invisible (tombstoned) as of seq 3")
	}

	o1, ok := restored.GetOutcome("r1")
	if !ok || o1.Status != StatusCommitted || o1.CommitSeq != 1 {
		t.Fatalf("r1 outcome = %+v ok=%v, want Committed CommitSeq=1", o1, ok)
	}
	o2, ok := restored.GetOutcome("r2")
	if !ok || o2.Status != StatusCommitted || o2.CommitSeq != 2 {
		t.Fatalf("r2 outcome = %+v ok=%v, want Committed CommitSeq=2", o2, ok)
	}
	o3, ok := restored.GetOutcome("r3")
	if !ok || o3.Status != StatusAborted || o3.ConflictKey != "k1" {
		t.Fatalf("r3 outcome = %+v ok=%v, want Aborted ConflictKey=k1", o3, ok)
	}
	if _, ok := restored.GetOutcome("never-submitted"); ok {
		t.Fatal("a RequestID never submitted must remain unknown after restore (RECOVERY-NON-INVENTION)")
	}

	// The fingerprint round-trips too: a mismatched-payload retry of a
	// restored RequestID is still correctly detected, not silently
	// treated as a fresh, unknown request.
	if _, err := restored.Precheck(CommitTxnCommand{
		RequestID: "r1", TxnID: 999, StartSeq: 0,
		Mutations: []mvcc.Mutation{{Key: "different-key", Value: []byte("x")}},
	}); !errors.Is(err, ErrRequestIDPayloadMismatch) {
		t.Fatalf("Precheck with a mismatched payload for a restored RequestID: err = %v, want ErrRequestIDPayloadMismatch", err)
	}
}

func TestEncodeStateDecodeStateEmptyFSM(t *testing.T) {
	f := New(mvcc.NewStore())
	restored, maxSeq, err := DecodeState(f.EncodeState())
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	if maxSeq != 0 {
		t.Fatalf("maxSeq = %d, want 0", maxSeq)
	}
	if _, ok := restored.GetOutcome("anything"); ok {
		t.Fatal("a freshly-decoded empty FSM must have no outcomes")
	}
	if _, ok := restored.Store().Visible("anything", 0); ok {
		t.Fatal("a freshly-decoded empty FSM must have no keys")
	}
}

func TestDecodeStateRejectsUnsupportedVersion(t *testing.T) {
	data := buildRichFSM(t).EncodeState()
	data[0] = fsmStateVersion + 1
	if _, _, err := DecodeState(data); !errors.Is(err, ErrUnsupportedCommandVersion) {
		t.Fatalf("DecodeState with a bad version byte: err = %v, want ErrUnsupportedCommandVersion", err)
	}
}

// TestDecodeStateRejectsTruncatedInput mirrors DecodeCommitTxn's own
// bounded-decoding discipline (docs/failure-model.md §6): DecodeState
// must return a plain error, never panic, on any truncated prefix of an
// otherwise-valid encoding.
func TestDecodeStateRejectsTruncatedInput(t *testing.T) {
	data := buildRichFSM(t).EncodeState()
	for _, cut := range []int{0, 1, 3, 5, len(data) / 2, len(data) - 1} {
		cut := cut
		t.Run("", func(t *testing.T) {
			if _, _, err := DecodeState(data[:cut]); err == nil {
				t.Fatalf("DecodeState on %d/%d truncated bytes: expected an error, got nil", cut, len(data))
			}
		})
	}
}

func TestDecodeStateRejectsTrailingGarbage(t *testing.T) {
	data := buildRichFSM(t).EncodeState()
	data = append(data, 0xFF)
	if _, _, err := DecodeState(data); err == nil {
		t.Fatal("DecodeState with trailing garbage bytes: expected an error, got nil")
	}
}
