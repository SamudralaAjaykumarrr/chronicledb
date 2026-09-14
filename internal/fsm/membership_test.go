package fsm

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

func TestControlKindRangesNeverCollide(t *testing.T) {
	raftRange := []byte{
		byte(membershipKindAddLearnerMirror), byte(membershipKindPromoteVoterMirror),
		byte(membershipKindRemoveServerMirror), byte(membershipKindVoidedMirror),
	}
	fsmRange := []byte{controlKindSetClusterVersion}
	for _, r := range raftRange {
		for _, f := range fsmRange {
			if r == f {
				t.Fatalf("internal/raft's membership-change-kind range collides with internal/fsm's control-kind range at byte %d", r)
			}
		}
	}
}

func TestRecordMembershipOutcomeIdempotentAndConflictDetected(t *testing.T) {
	f := New(mvcc.NewStore())

	outcome := Outcome{RequestID: "r1", Status: StatusCommitted, CommitSeq: 5}
	got, err := f.RecordMembershipOutcome("r1", MembershipAddLearner, "n4", "host:1", outcome)
	if err != nil {
		t.Fatalf("RecordMembershipOutcome: %v", err)
	}
	if got != outcome {
		t.Fatalf("got %+v, want %+v", got, outcome)
	}

	// Identical retry: returns the same recorded outcome, no error.
	got2, err := f.RecordMembershipOutcome("r1", MembershipAddLearner, "n4", "host:1", Outcome{RequestID: "r1", Status: StatusCommitted, CommitSeq: 999})
	if err != nil {
		t.Fatalf("RecordMembershipOutcome retry: %v", err)
	}
	if got2 != outcome {
		t.Fatalf("retry returned %+v, want the originally recorded %+v (never re-evaluated)", got2, outcome)
	}

	// Conflicting reuse: same RequestID, different {kind,nodeId,address}.
	if _, err := f.RecordMembershipOutcome("r1", MembershipAddLearner, "n5", "host:2", outcome); !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("got %v, want ErrRequestIDConflict", err)
	}
}

func TestGetMembershipOutcomePrecheck(t *testing.T) {
	f := New(mvcc.NewStore())
	if _, err := f.GetMembershipOutcome("unknown", MembershipRemoveServer, "n1", ""); !errors.Is(err, ErrRequestIDUnknown) {
		t.Fatalf("got %v, want ErrRequestIDUnknown", err)
	}

	outcome := Outcome{RequestID: "r1", Status: StatusCommitted, CommitSeq: 1}
	if _, err := f.RecordMembershipOutcome("r1", MembershipRemoveServer, "n1", "", outcome); err != nil {
		t.Fatalf("RecordMembershipOutcome: %v", err)
	}
	got, err := f.GetMembershipOutcome("r1", MembershipRemoveServer, "n1", "")
	if err != nil || got != outcome {
		t.Fatalf("GetMembershipOutcome = %+v, %v; want %+v, nil", got, err, outcome)
	}
	if _, err := f.GetMembershipOutcome("r1", MembershipRemoveServer, "n2", ""); !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("got %v, want ErrRequestIDConflict for a mismatched fingerprint", err)
	}
}

// TestMembershipOutcomesSurviveEncodeDecodeRoundTrip regresses
// MEMBERSHIP REQUEST OUTCOME STABILITY across a snapshot boundary.
func TestMembershipOutcomesSurviveEncodeDecodeRoundTrip(t *testing.T) {
	f := New(mvcc.NewStore())
	if _, err := f.ApplySetClusterVersion(1, SetClusterVersionCommand{RequestID: "gen1", TargetGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ApplySetClusterVersion(2, SetClusterVersionCommand{RequestID: "gen2", TargetGeneration: 2}); err != nil {
		t.Fatal(err)
	}
	want := Outcome{RequestID: "add1", Status: StatusCommitted, CommitSeq: 3}
	if _, err := f.RecordMembershipOutcome("add1", MembershipAddLearner, "n4", "host:1", want); err != nil {
		t.Fatal(err)
	}

	data := f.EncodeState()
	f2, _, err := DecodeState(data)
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	got, err := f2.GetMembershipOutcome("add1", MembershipAddLearner, "n4", "host:1")
	if err != nil {
		t.Fatalf("GetMembershipOutcome after round-trip: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v after round-trip, want %+v", got, want)
	}
}

// TestMembershipOutcomeGatedOnGeneration2NotJustNonZero pins ROLLBACK
// BOUNDARY HONESTY's extension: a cluster at generation 1 (v0.4.0's own
// finalize boundary, not yet v0.5.0's) must encode byte-identical
// output whether or not any membership outcome happens to be recorded
// in memory — a stray in-memory row at generation 1 can only arise from
// a bug, since a real one requires generation >= 2 (§8.2's propose-side
// gate), but the encoder must not depend on that being true to stay
// byte-identical.
func TestMembershipOutcomeGatedOnGeneration2NotJustNonZero(t *testing.T) {
	f := New(mvcc.NewStore())
	if _, err := f.ApplySetClusterVersion(1, SetClusterVersionCommand{RequestID: "gen1", TargetGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	baseline := f.EncodeState()

	f.membershipOutcomes["stray"] = membershipOutcomeEntry{outcome: Outcome{RequestID: "stray", Status: StatusCommitted}}
	withStray := f.EncodeState()

	if string(baseline) != string(withStray) {
		t.Fatal("EncodeState output at generation 1 changed due to an in-memory membership outcome row — this breaks ROLLBACK BOUNDARY HONESTY at the v0.4.0 finalize boundary")
	}
}
