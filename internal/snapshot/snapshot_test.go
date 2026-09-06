package snapshot

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

func buildFSM(t *testing.T) *fsm.FSM {
	t.Helper()
	f := fsm.New(mvcc.NewStore())
	cmds := []fsm.CommitTxnCommand{
		{RequestID: "r1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "a", Value: []byte("1")}}},
		{RequestID: "r2", TxnID: 2, StartSeq: 1, Mutations: []mvcc.Mutation{{Key: "b", Value: []byte("2")}}},
		{RequestID: "r3", TxnID: 3, StartSeq: 2, Mutations: []mvcc.Mutation{{Key: "a", Tombstone: true}}},
	}
	for i, cmd := range cmds {
		if _, err := f.Apply(uint64(i+1), cmd); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	return f
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	f := buildFSM(t)
	meta := Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}
	data := Encode(meta, f)

	snap, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if snap.Meta != meta {
		t.Fatalf("Meta mismatch: got %+v, want %+v", snap.Meta, meta)
	}
	if _, found := snap.FSM.Store().Visible("a", 3); found {
		t.Fatalf("expected key a tombstoned as of seq 3")
	}
	v, found := snap.FSM.Store().Visible("b", 3)
	if !found || string(v) != "2" {
		t.Fatalf("expected b=2, got %q found=%v", v, found)
	}
	for _, id := range []fsm.RequestID{"r1", "r2", "r3"} {
		if _, ok := snap.FSM.GetOutcome(id); !ok {
			t.Fatalf("expected outcome for %s to survive round trip", id)
		}
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 3}, f)
	data[0] ^= 0xFF
	if _, err := Decode(data); err == nil {
		t.Fatal("expected error for corrupted magic")
	}
}

func TestDecodeRejectsBadChecksum(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 3}, f)
	data[len(data)-1] ^= 0xFF
	if _, err := Decode(data); err == nil {
		t.Fatal("expected error for corrupted checksum")
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 3}, f)
	for _, n := range []int{0, 1, headerSize, headerSize + 5, len(data) - 1} {
		if n > len(data) {
			continue
		}
		if _, err := Decode(data[:n]); err == nil {
			t.Fatalf("expected error decoding truncated data at %d bytes", n)
		}
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 3}, f)
	data[4] = 99 // version byte
	// Recompute nothing: this should fail on version check before checksum.
	if _, err := Decode(data); err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestDecodeRejectsBoundaryViolation(t *testing.T) {
	f := buildFSM(t)
	// Claim a boundary lower than the actual max CommitSeq present (3).
	data := Encode(Meta{LastIncludedIndex: 2, LastIncludedTerm: 1}, f)
	if _, err := Decode(data); err == nil {
		t.Fatal("expected error for a snapshot claiming a boundary its own content exceeds")
	}
}

func TestEncodeDeterministic(t *testing.T) {
	f1 := buildFSM(t)
	f2 := buildFSM(t)
	d1 := Encode(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f1)
	d2 := Encode(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f2)
	if string(d1) != string(d2) {
		t.Fatal("expected byte-identical encoding for independently constructed, identically-applied FSMs")
	}
}
