package fsm

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// FuzzDecodeCommitTxn feeds arbitrary byte slices directly into the
// production CommitTxn decoder to prove it never panics and never
// trusts a length/count field beyond what is actually present,
// regardless of input (docs/failure-model.md §6 "no panic from
// malformed disk/network data" — this decoder sits on the recovery
// replay path, so a malformed/corrupted record must fail closed with
// an error, never crash the process).
func FuzzDecodeCommitTxn(f *testing.F) {
	f.Add(EncodeCommitTxn(CommitTxnCommand{
		RequestID: "R1",
		TxnID:     1,
		StartSeq:  5,
		Mutations: []mvcc.Mutation{
			{Key: "K", Value: []byte("v")},
			{Key: "D", Tombstone: true},
		},
	}))
	f.Add(EncodeCommitTxn(CommitTxnCommand{RequestID: "", TxnID: 0, StartSeq: 0}))
	f.Add([]byte{})
	f.Add([]byte{2})
	f.Add(make([]byte, 25))

	oversizedCount := EncodeCommitTxn(CommitTxnCommand{RequestID: "x", TxnID: 1, StartSeq: 1})
	if len(oversizedCount) >= 4 {
		n := len(oversizedCount)
		// Corrupt the numMutations field (the last 4 bytes of a
		// zero-mutation command's encoding) to claim an enormous
		// mutation count with no backing bytes.
		oversizedCount[n-4] = 0xFF
		oversizedCount[n-3] = 0xFF
		oversizedCount[n-2] = 0xFF
		oversizedCount[n-1] = 0xFF
	}
	f.Add(oversizedCount)

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := DecodeCommitTxn(data)
		if err != nil {
			return
		}
		// A successful decode must never claim more mutations than
		// could possibly fit in the input.
		if len(cmd.Mutations) > len(data) {
			t.Fatalf("DecodeCommitTxn returned %d mutations from only %d input bytes", len(cmd.Mutations), len(data))
		}
	})
}
