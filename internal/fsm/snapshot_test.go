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

// TestEncodeStateDecodeStateRoundTrip_ClusterGeneration proves the
// generation-aware trailing block docs/enterprise-v1-plan.md §7
// requires ("generation-aware encode/decode fuzz tests for each of the
// five formats, including 'old generation still decodes under the new
// binary' as an explicit assertion"):
//
//   - generation 0 (the default, and the ONLY value any pre-v0.4.0
//     binary ever produces or expects) encodes byte-identical to a plain
//     EncodeState call with no cluster-version command ever applied —
//     the actual rollback-safety property, not merely "decodes
//     somehow";
//   - a state that has had a SetClusterVersionCommand applied encodes a
//     longer form (clusterGeneration + the control-command idempotency
//     table, see encodeState's doc comment) that still round-trips
//     through DecodeState, including the idempotency table itself
//     (TestApplySetClusterVersion_SurvivesEncodeDecodeRoundTrip below
//     is the direct proof that this actually fixes retried-finalize
//     idempotency across a restart/snapshot boundary).
func TestEncodeStateDecodeStateRoundTrip_ClusterGeneration(t *testing.T) {
	base := buildRichFSM(t)
	baseBytes := base.EncodeState()

	gen0 := buildRichFSM(t) // clusterGeneration still 0: never finalized
	if got := gen0.EncodeState(); string(got) != string(baseBytes) {
		t.Fatalf("generation-0 EncodeState is not byte-identical to the pre-v0.4.0 encoding — this breaks pre-finalize rollback compatibility")
	}

	finalized := buildRichFSM(t)
	if _, err := finalized.ApplySetClusterVersion(4, SetClusterVersionCommand{RequestID: "finalize", TargetGeneration: 1}); err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	finalizedBytes := finalized.EncodeState()
	// clusterGeneration(4) + numControlOutcomes(4) + one entry
	// (requestIDLen(4) + "finalize"(8) + status(1) + CommitSeq(8)).
	wantExtra := 4 + 4 + (4 + len("finalize") + 1 + 8)
	if len(finalizedBytes) != len(baseBytes)+wantExtra {
		t.Fatalf("finalized EncodeState length = %d, want exactly %d (base + %d-byte cluster-generation/control-outcomes block)", len(finalizedBytes), len(baseBytes)+wantExtra, wantExtra)
	}

	restored, maxSeq, err := DecodeState(finalizedBytes)
	if err != nil {
		t.Fatalf("DecodeState(finalized): %v", err)
	}
	if got := restored.ClusterGeneration(); got != 1 {
		t.Fatalf("restored ClusterGeneration = %d, want 1", got)
	}
	// The SetClusterVersionCommand's own CommitSeq (4) IS a real,
	// committed Raft log index — like any other committed index, a
	// snapshot claiming a boundary below it while still reflecting its
	// effect (the advanced clusterGeneration) would be exactly the kind
	// of inconsistency the snapshot-boundary consistency check
	// (docs/snapshots.md §5 point 3) exists to catch, so it must be
	// folded into maxSeq exactly like a CommitTxn CommitSeq is.
	if maxSeq != 4 {
		t.Fatalf("maxSeq = %d, want 4 (the SetClusterVersionCommand's own committed index, correctly bounding the snapshot boundary)", maxSeq)
	}

	// The idempotency table itself must survive the round trip: a
	// second, identical retry under the same RequestID must return the
	// ORIGINAL recorded outcome, not be re-evaluated against the
	// already-advanced clusterGeneration (where it would wrongly abort).
	retryOutcome, err := restored.ApplySetClusterVersion(99, SetClusterVersionCommand{RequestID: "finalize", TargetGeneration: 1})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion (post-restore retry): %v", err)
	}
	if retryOutcome.Status != StatusCommitted || retryOutcome.CommitSeq != 4 {
		t.Fatalf("post-restore retry outcome = %+v, want the original Committed outcome (CommitSeq=4), proving idempotency survives a restore", retryOutcome)
	}
}

// TestDecodeState_RejectsGenerationFieldWrongLength proves an "extra
// bytes present but not exactly 0 or 4" state is genuinely malformed,
// not a newer generation this build simply declines to guess at (see
// DecodeState's own doc comment on this distinction).
func TestDecodeState_RejectsGenerationFieldWrongLength(t *testing.T) {
	data := buildRichFSM(t).EncodeState()
	for _, extra := range [][]byte{{0x01}, {0x01, 0x02}, {0x01, 0x02, 0x03}, {0x01, 0x02, 0x03, 0x04, 0x05}} {
		if _, _, err := DecodeState(append(append([]byte(nil), data...), extra...)); err == nil {
			t.Fatalf("DecodeState with %d trailing bytes: expected an error, got nil", len(extra))
		}
	}
}

// FuzzDecodeState is DecodeState's bounded-decoding proof
// (docs/failure-model.md §6): the cluster-generation/control-outcomes
// trailing block added by this phase is new, non-trivial decode logic
// and must never panic on malformed or adversarial input, exactly like
// every other decoder in this codebase.
func FuzzDecodeState(f *testing.F) {
	f.Add([]byte{})
	f.Add(New(mvcc.NewStore()).EncodeState())
	finalized := New(mvcc.NewStore())
	if _, err := finalized.ApplySetClusterVersion(1, SetClusterVersionCommand{RequestID: "seed", TargetGeneration: 1}); err != nil {
		f.Fatalf("seeding: %v", err)
	}
	f.Add(finalized.EncodeState())
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = DecodeState(b)
	})
}
