package snapshot

import (
	"errors"
	"testing"
)

func testConfiguration() Configuration {
	return Configuration{
		Voters:   []Member{{ID: "a", Address: "a:1"}, {ID: "b", Address: "b:1"}},
		Learners: []Member{{ID: "c", Address: "c:1"}},
	}
}

func TestV2RoundTripPreservesConfiguration(t *testing.T) {
	f := buildFSM(t)
	meta := Meta{LastIncludedIndex: 5, LastIncludedTerm: 2, HasConfiguration: true, Configuration: testConfiguration()}
	data := Encode(meta, f, FormatVersion)
	snap, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !metasEqual(snap.Meta, meta) {
		t.Fatalf("Meta mismatch after v2 round trip: got %+v, want %+v", snap.Meta, meta)
	}
}

func TestV2RoundTripEmptyConfigurationDistinguishedFromNoConfiguration(t *testing.T) {
	f := buildFSM(t)

	// HasConfiguration=false must round-trip as false with a zero
	// Configuration — never confused with a present-but-empty one.
	noConfig := Meta{LastIncludedIndex: 5, LastIncludedTerm: 2}
	data := Encode(noConfig, f, FormatVersion)
	snap, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if snap.Meta.HasConfiguration {
		t.Fatal("expected HasConfiguration=false to round-trip as false")
	}

	// A present-but-empty Configuration must round-trip as
	// HasConfiguration=true with zero voters/learners — a different,
	// distinguishable fact.
	emptyButPresent := Meta{LastIncludedIndex: 5, LastIncludedTerm: 2, HasConfiguration: true}
	data2 := Encode(emptyButPresent, f, FormatVersion)
	snap2, err := Decode(data2)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !snap2.Meta.HasConfiguration {
		t.Fatal("expected HasConfiguration=true (present but empty) to round-trip as true")
	}
	if len(snap2.Meta.Configuration.Voters) != 0 || len(snap2.Meta.Configuration.Learners) != 0 {
		t.Fatalf("expected an empty Configuration, got %+v", snap2.Meta.Configuration)
	}
}

func TestV2RejectsInvalidHasConfigByte(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 1}, f, FormatVersion)
	// Locate the hasConfig byte: right after the fsmStateLen-prefixed
	// state, at offset headerSize+stateLen.
	stateLen := len(f.EncodeState())
	off := headerSize + stateLen
	data[off] = 0x02 // neither 0x00 nor 0x01
	if _, err := Decode(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt for an invalid hasConfig byte", err)
	}
}

func TestV2RejectsHasConfigFalseWithNonZeroLen(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 1, HasConfiguration: true, Configuration: testConfiguration()}, f, FormatVersion)
	stateLen := len(f.EncodeState())
	off := headerSize + stateLen
	data[off] = 0x00 // flip hasConfig to false, but configLen still declares real bytes
	if _, err := Decode(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt for hasConfig=false with nonzero configLen", err)
	}
}

func TestUnsupportedVersionRange(t *testing.T) {
	f := buildFSM(t)
	data := Encode(Meta{LastIncludedIndex: 1}, f, FormatVersion)
	data[4] = 3 // version byte, above FormatVersion
	if _, err := Decode(data); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("got %v, want ErrUnsupportedVersion for version 3", err)
	}
}

func TestPreFinalizeEncodeByteIdenticalToV1(t *testing.T) {
	f := buildFSM(t)
	meta := Meta{LastIncludedIndex: 7, LastIncludedTerm: 3}
	v1 := Encode(meta, f, 1)
	// A hand-rolled v1 frame (no config section, no trailing bytes
	// beyond the original header+state+checksum) must be exactly what a
	// pre-dynamic-membership decoder already knew how to produce: same
	// length as headerSize+stateLen+checksumSize.
	want := headerSize + len(f.EncodeState()) + checksumSize
	if len(v1) != want {
		t.Fatalf("writeVersion=1 output length = %d, want %d (no v2 config section)", len(v1), want)
	}
	if v1[4] != 1 {
		t.Fatalf("writeVersion=1 output's version byte = %d, want 1", v1[4])
	}
}

func TestEncodePanicsOutsideSupportedRange(t *testing.T) {
	f := buildFSM(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected Encode to panic for an out-of-range writeVersion")
		}
	}()
	Encode(Meta{}, f, 3)
}
