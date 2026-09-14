package node

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// FuzzDecodeEntryPayload feeds arbitrary bytes into decodeEntryPayload
// (dynamic-membership plan §6.1a, §17's "Entry.Type WAL encoding" row,
// which names this fuzz target as a proof obligation): it must never
// panic, and whenever it returns success it must never hand back an
// EntryType this build does not recognize — decodeEntryPayload's own
// doc comment claims it is "unambiguous in both directions" precisely
// because every unrecognized typed header is rejected as
// ErrUnknownEntryType, never silently reinterpreted as something else
// (NO SILENT FORMAT MISINTERPRETATION).
func FuzzDecodeEntryPayload(f *testing.F) {
	f.Add(encodeEntryPayload(1, raft.EntryNormal, []byte("hello")))
	f.Add(encodeEntryPayload(7, raft.EntryConfig, []byte("cfg-bytes")))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, entryPayloadTypeSentinel})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, entryPayloadTypeSentinel, 0xFF})

	f.Fuzz(func(t *testing.T, b []byte) {
		_, typ, _, err := decodeEntryPayload(b)
		if err != nil {
			return
		}
		if typ > knownEntryTypesMax {
			t.Fatalf("decodeEntryPayload(%x) returned unrecognized EntryType %d with no error", b, typ)
		}
	})
}
