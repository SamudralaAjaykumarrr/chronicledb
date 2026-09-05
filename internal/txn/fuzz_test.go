package txn

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// FuzzDecodeCommitTxn feeds arbitrary byte slices directly into the
// production CommitTxn decoder to prove it never panics and never
// trusts a length/count field beyond what is actually present,
// regardless of input (docs/failure-model.md §6 "no panic from
// malformed disk/network data" — this decoder sits on the recovery
// replay path, so a malformed/corrupted record must fail closed with an
// error, never crash the process).
func FuzzDecodeCommitTxn(f *testing.F) {
	f.Add(encodeCommitTxn(commitTxnCommand{
		txnID:    1,
		startSeq: 5,
		mutations: []mvcc.Mutation{
			{Key: "K", Value: []byte("v")},
			{Key: "D", Tombstone: true},
		},
	}))
	f.Add(encodeCommitTxn(commitTxnCommand{txnID: 0, startSeq: 0}))
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add(make([]byte, 21)) // exactly the fixed header size, zero mutations claimed... plus one stray byte
	oversizedCount := encodeCommitTxn(commitTxnCommand{txnID: 1, startSeq: 1})
	// Corrupt the numMutations field (last 4 bytes of the fixed header)
	// to claim an enormous mutation count with no backing bytes.
	if len(oversizedCount) >= 21 {
		oversizedCount[17] = 0xFF
		oversizedCount[18] = 0xFF
		oversizedCount[19] = 0xFF
		oversizedCount[20] = 0xFF
	}
	f.Add(oversizedCount)

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := decodeCommitTxn(data)
		if err != nil {
			return
		}
		// A successful decode must never claim more mutations than could
		// possibly fit in the input.
		if len(cmd.mutations) > len(data) {
			t.Fatalf("decodeCommitTxn returned %d mutations from only %d input bytes", len(cmd.mutations), len(data))
		}
	})
}
