package wal

import "testing"

// FuzzDecodeFrameBytes feeds arbitrary byte slices directly into the
// production frame decoder to prove it never panics and never trusts a
// length field beyond what is actually present, regardless of input
// (docs/failure-model.md §6 "no panic from malformed disk/network data").
func FuzzDecodeFrameBytes(f *testing.F) {
	// Seed with a real, valid frame and hand-crafted edge cases.
	f.Add(encodeRecord(RecordTypeLogEntry, 1, []byte("hello")))
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add(make([]byte, headerSize)) // header-only, no payload/crc
	oversizedHeader := make([]byte, headerSize)
	oversizedHeader[headerLengthOff] = 0xFF
	oversizedHeader[headerLengthOff+1] = 0xFF
	oversizedHeader[headerLengthOff+2] = 0xFF
	oversizedHeader[headerLengthOff+3] = 0xFF
	f.Add(oversizedHeader)

	f.Fuzz(func(t *testing.T, data []byte) {
		rec, frameLen, err := decodeFrameBytes(data)
		if err != nil {
			if rec != nil {
				t.Fatalf("decodeFrameBytes returned both a record and an error: rec=%v err=%v", rec, err)
			}
			return
		}
		if rec == nil {
			t.Fatalf("decodeFrameBytes returned (nil, %d, nil): must return either a record or an error", frameLen)
		}
		if frameLen < headerSize+checksumSize {
			t.Fatalf("decoded frameLen %d smaller than minimum possible frame size", frameLen)
		}
		if frameLen > len(data) {
			t.Fatalf("decoded frameLen %d exceeds input length %d", frameLen, len(data))
		}
		if len(rec.Payload) > MaxRecordPayloadSize {
			t.Fatalf("decoded payload of %d bytes exceeds MaxRecordPayloadSize %d", len(rec.Payload), MaxRecordPayloadSize)
		}
		if !rec.Type.valid() {
			t.Fatalf("decodeFrameBytes returned an invalid record type %d without error", rec.Type)
		}
	})
}
