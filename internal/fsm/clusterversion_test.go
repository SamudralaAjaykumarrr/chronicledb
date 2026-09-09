package fsm

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

func TestEncodeDecodeSetClusterVersion_RoundTrip(t *testing.T) {
	cmd := SetClusterVersionCommand{RequestID: "r1", TargetGeneration: 1}
	b := EncodeSetClusterVersion(cmd)
	if !IsControlCommand(b) {
		t.Fatalf("encoded control command not recognized by IsControlCommand")
	}
	got, err := DecodeSetClusterVersion(b)
	if err != nil {
		t.Fatalf("DecodeSetClusterVersion: %v", err)
	}
	if got != cmd {
		t.Fatalf("got %+v, want %+v", got, cmd)
	}
}

// TestControlCommandMarker_NeverCollidesWithCommitTxnVersion is the
// backward-compatibility linchpin the whole discriminator scheme relies
// on (see ControlCommandMarker's doc comment): a pre-v0.4.0 binary's
// unmodified DecodeCommitTxn must fail closed, not misinterpret, any
// control-command payload — proven directly here by feeding a real
// encoded control command straight into DecodeCommitTxn.
func TestControlCommandMarker_NeverCollidesWithCommitTxnVersion(t *testing.T) {
	if ControlCommandMarker == commitTxnCommandVersion {
		t.Fatalf("ControlCommandMarker (%d) must never equal commitTxnCommandVersion (%d)", ControlCommandMarker, commitTxnCommandVersion)
	}
	b := EncodeSetClusterVersion(SetClusterVersionCommand{RequestID: "r1", TargetGeneration: 1})
	if _, err := DecodeCommitTxn(b); !errors.Is(err, ErrUnsupportedCommandVersion) {
		t.Fatalf("DecodeCommitTxn on a control-command payload: got err=%v, want ErrUnsupportedCommandVersion (this is the free backward-compat safety net a pre-v0.4.0 binary relies on)", err)
	}
}

// TestIsControlCommand_NeverMatchesCommitTxn proves the converse: every
// CommitTxn payload this codebase actually produces is never mistaken
// for a control command.
func TestIsControlCommand_NeverMatchesCommitTxn(t *testing.T) {
	b := EncodeCommitTxn(CommitTxnCommand{RequestID: "r1", TxnID: 1, StartSeq: 0})
	if IsControlCommand(b) {
		t.Fatalf("IsControlCommand incorrectly matched a CommitTxn payload")
	}
}

func TestApplySetClusterVersion_MonotonicSingleStep(t *testing.T) {
	f := New(mvcc.NewStore())

	// Skipping straight to generation 2 (no generation 1 first) must be
	// rejected — N/N+1-only, not N/N+2.
	out, err := f.ApplySetClusterVersion(1, SetClusterVersionCommand{RequestID: "skip", TargetGeneration: 2})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	if out.Status != StatusAborted {
		t.Fatalf("skip-ahead to generation 2 from 0: got %v, want StatusAborted", out.Status)
	}
	if got := f.ClusterGeneration(); got != 0 {
		t.Fatalf("ClusterGeneration after rejected skip-ahead = %d, want 0 (unchanged)", got)
	}

	// The legitimate single step: 0 -> 1.
	out, err = f.ApplySetClusterVersion(2, SetClusterVersionCommand{RequestID: "step1", TargetGeneration: 1})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	if out.Status != StatusCommitted || out.CommitSeq != 2 {
		t.Fatalf("legitimate step 0->1: got %+v, want Committed CommitSeq=2", out)
	}
	if got := f.ClusterGeneration(); got != 1 {
		t.Fatalf("ClusterGeneration after 0->1 = %d, want 1", got)
	}
}

// TestApplySetClusterVersion_RollbackBoundaryHonesty proves ROLLBACK
// BOUNDARY HONESTY at the deterministic FSM layer directly: once
// finalized to generation N, a command attempting to move the agreed
// generation backward (or sideways, to the same value again under a
// fresh RequestID) is deterministically rejected — the agreed
// generation never moves anywhere but forward.
func TestApplySetClusterVersion_RollbackBoundaryHonesty(t *testing.T) {
	f := New(mvcc.NewStore())
	if _, err := f.ApplySetClusterVersion(1, SetClusterVersionCommand{RequestID: "up", TargetGeneration: 1}); err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	if got := f.ClusterGeneration(); got != 1 {
		t.Fatalf("ClusterGeneration = %d, want 1", got)
	}

	out, err := f.ApplySetClusterVersion(2, SetClusterVersionCommand{RequestID: "back-to-0", TargetGeneration: 0})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	if out.Status != StatusAborted {
		t.Fatalf("attempted downgrade to generation 0: got %v, want StatusAborted", out.Status)
	}
	if got := f.ClusterGeneration(); got != 1 {
		t.Fatalf("ClusterGeneration after rejected downgrade = %d, want 1 (unchanged)", got)
	}

	out, err = f.ApplySetClusterVersion(3, SetClusterVersionCommand{RequestID: "same-again", TargetGeneration: 1})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	if out.Status != StatusAborted {
		t.Fatalf("re-proposing the already-current generation under a new RequestID: got %v, want StatusAborted", out.Status)
	}
}

// TestApplySetClusterVersion_Idempotent proves a retried finalize
// proposal under the SAME RequestID (docs/enterprise-v1-plan.md §7:
// "kill the process that issued finalize mid-call... finalize is
// either fully applied... or not applied at all") is answered from the
// recorded outcome, not re-evaluated against (possibly since-changed)
// current state.
func TestApplySetClusterVersion_Idempotent(t *testing.T) {
	f := New(mvcc.NewStore())
	first, err := f.ApplySetClusterVersion(5, SetClusterVersionCommand{RequestID: "dup", TargetGeneration: 1})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion: %v", err)
	}
	// A hypothetical second commit at a different index, same RequestID:
	// must return the ORIGINAL recorded outcome (index 5), never
	// re-evaluate against the (by-then) already-generation-1 state.
	second, err := f.ApplySetClusterVersion(9, SetClusterVersionCommand{RequestID: "dup", TargetGeneration: 1})
	if err != nil {
		t.Fatalf("ApplySetClusterVersion (retry): %v", err)
	}
	if second != first {
		t.Fatalf("retried Apply under same RequestID: got %+v, want unchanged original %+v", second, first)
	}
}

func TestDecodeSetClusterVersion_MalformedInput(t *testing.T) {
	valid := EncodeSetClusterVersion(SetClusterVersionCommand{RequestID: "r1", TargetGeneration: 1})
	for i := 1; i < len(valid); i++ {
		if _, err := DecodeSetClusterVersion(valid[:i]); err == nil {
			t.Fatalf("DecodeSetClusterVersion accepted truncated input of length %d without error", i)
		}
	}
	// Wrong marker byte entirely (e.g. a CommitTxn payload).
	if _, err := DecodeSetClusterVersion(EncodeCommitTxn(CommitTxnCommand{RequestID: "x"})); err == nil {
		t.Fatalf("DecodeSetClusterVersion accepted a non-control-command payload")
	}
	// Right marker, unknown kind byte.
	bad := append([]byte(nil), valid...)
	bad[1] = 0xFF
	if _, err := DecodeSetClusterVersion(bad); !errors.Is(err, ErrUnknownControlCommand) {
		t.Fatalf("DecodeSetClusterVersion with unknown kind byte: got err=%v, want ErrUnknownControlCommand", err)
	}
	// Trailing garbage after a structurally valid command.
	if _, err := DecodeSetClusterVersion(append(append([]byte(nil), valid...), 0xAB)); err == nil {
		t.Fatalf("DecodeSetClusterVersion accepted trailing garbage bytes")
	}
}

func FuzzDecodeSetClusterVersion(f *testing.F) {
	f.Add(EncodeSetClusterVersion(SetClusterVersionCommand{RequestID: "r1", TargetGeneration: 1}))
	f.Add([]byte{})
	f.Add([]byte{ControlCommandMarker})
	f.Fuzz(func(t *testing.T, b []byte) {
		// Must never panic, regardless of input (docs/failure-model.md §6).
		_, _ = DecodeSetClusterVersion(b)
	})
}
