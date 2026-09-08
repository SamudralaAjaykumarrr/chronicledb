package audit

import "testing"

// FuzzDecodeFrame feeds arbitrary byte slices directly into the
// production frame decoder to prove it never panics and never trusts a
// length field beyond what is actually present, regardless of input —
// docs/failure-model.md §6 "no panic from malformed disk/network data",
// applied to the audit log exactly as internal/wal/fuzz_test.go applies
// it to WAL records. It is also part of this phase's required proof
// obligation that "audit hash-chain integrity is proven under
// fuzzing/tampering tests" (docs/enterprise-v1-plan.md §5).
func FuzzDecodeFrame(f *testing.F) {
	frame, _, err := encodeFrame([hashSize]byte{}, Entry{Principal: "n1", Role: "admin", Action: "fault.block", Endpoint: "/fault", Result: "allow"})
	if err != nil {
		f.Fatalf("encodeFrame: %v", err)
	}
	f.Add(frame)
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add(make([]byte, headerSize))
	f.Add(make([]byte, headerSize+hashSize))
	oversized := make([]byte, headerSize)
	oversized[headerLengthOff] = 0xFF
	oversized[headerLengthOff+1] = 0xFF
	oversized[headerLengthOff+2] = 0xFF
	oversized[headerLengthOff+3] = 0xFF
	f.Add(oversized)

	// A tampered-but-plausibly-shaped frame: same length as the seed
	// but with one payload byte flipped, so the fuzzer's corpus starts
	// with an actual tamper case, not just structurally malformed ones.
	tampered := append([]byte(nil), frame...)
	if len(tampered) > headerSize+hashSize {
		tampered[headerSize+hashSize] ^= 0xFF
	}
	f.Add(tampered)

	f.Fuzz(func(t *testing.T, data []byte) {
		rec, frameLen, err := decodeFrame(data)
		if err != nil {
			if frameLen != 0 {
				t.Fatalf("decodeFrame returned both a nonzero frameLen and an error: frameLen=%d err=%v", frameLen, err)
			}
			return
		}
		if frameLen < headerSize+hashSize+checksumSize+hashSize {
			t.Fatalf("decoded frameLen %d smaller than minimum possible frame size", frameLen)
		}
		if frameLen > len(data) {
			t.Fatalf("decoded frameLen %d exceeds input length %d", frameLen, len(data))
		}
		_ = rec
	})
}
