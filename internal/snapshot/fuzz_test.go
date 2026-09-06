package snapshot

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// FuzzDecode feeds arbitrary byte slices directly into the production
// snapshot decoder to prove it never panics and never trusts a length
// field beyond what is actually present, regardless of input
// (docs/failure-model.md §6, mirroring internal/wal's
// FuzzDecodeFrameBytes and internal/fsm's FuzzDecodeCommitTxn
// discipline). This decoder sits on the restart-recovery and
// follower-catch-up paths (docs/snapshots.md §6), so malformed or
// corrupted bytes must fail closed with an error, never crash the
// process — this is a genuinely new fuzz target for Phase 10
// (docs/roadmap.md "FUZZING": "snapshot decoder"), not present before
// this phase.
func FuzzDecode(f *testing.F) {
	empty := fsm.New(mvcc.NewStore())
	f.Add(Encode(Meta{}, empty))

	populated := fsm.New(mvcc.NewStore())
	if _, err := populated.Apply(1, fsm.CommitTxnCommand{
		RequestID: "r1", TxnID: 1, StartSeq: 0,
		Mutations: []mvcc.Mutation{
			{Key: "k1", Value: []byte("v1")},
			{Key: "k2", Tombstone: true},
		},
	}); err != nil {
		f.Fatalf("Apply: %v", err)
	}
	f.Add(Encode(Meta{LastIncludedIndex: 1, LastIncludedTerm: 1}, populated))

	f.Add([]byte{})
	f.Add([]byte("CSNP"))
	f.Add(make([]byte, headerSize+checksumSize-1))
	f.Add(make([]byte, headerSize+checksumSize))

	// A corrupted checksum on an otherwise well-formed snapshot.
	corrupted := Encode(Meta{LastIncludedIndex: 1, LastIncludedTerm: 1}, populated)
	if len(corrupted) > 0 {
		corrupted[len(corrupted)-1] ^= 0xFF
	}
	f.Add(corrupted)

	// A well-formed header/checksum but an fsmStateLen field (the 8
	// bytes immediately before the fsm-state payload, per this file's
	// own frame-layout comment: magic(4) version(1) index(8) term(8)
	// fsmStateLen(8) ...) claiming far more bytes than actually follow
	// — the exact "don't trust a length field beyond the bytes present"
	// attack this fuzz target exists to guard against.
	oversizedLen := Encode(Meta{LastIncludedIndex: 1, LastIncludedTerm: 1}, populated)
	const fsmStateLenOffset = 4 + 1 + 8 + 8
	if len(oversizedLen) >= fsmStateLenOffset+8 {
		for i := fsmStateLenOffset; i < fsmStateLenOffset+8; i++ {
			oversizedLen[i] = 0xFF
		}
	}
	f.Add(oversizedLen)

	f.Fuzz(func(t *testing.T, data []byte) {
		snap, err := Decode(data)
		if err != nil {
			return
		}
		if snap.FSM == nil {
			t.Fatalf("Decode returned a nil FSM with no error for input %q", data)
		}
	})
}
