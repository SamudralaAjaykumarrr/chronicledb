// Microbenchmarks for internal/fsm (docs/roadmap.md Phase 9 §FSM):
// deterministic Apply for a single-key transaction, a multi-key
// transaction, and duplicate-RequestID resolution (the Precheck
// idempotency short-circuit, docs/transactions.md §6).
//
// Run: go test ./internal/fsm/... -run '^$' -bench . -benchmem
package fsm

import (
	"fmt"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// BenchmarkApplySingleKey measures Apply's full idempotency-check ->
// conflict-check -> commit -> outcome-record sequence for a
// single-mutation command, each under a fresh RequestID (the common
// case: no retry).
func BenchmarkApplySingleKey(b *testing.B) {
	f := New(mvcc.NewStore())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := CommitTxnCommand{
			RequestID: RequestID(fmt.Sprintf("req-%d", i)),
			TxnID:     uint64(i + 1),
			StartSeq:  0,
			Mutations: []mvcc.Mutation{{Key: fmt.Sprintf("k%d", i), Value: []byte("v")}},
		}
		if _, err := f.Apply(uint64(i+1), cmd); err != nil {
			b.Fatalf("Apply: %v", err)
		}
	}
}

// BenchmarkApplyMultiKey measures Apply for a multi-key mutation set,
// varying the mutation-set size.
func BenchmarkApplyMultiKey(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			f := New(mvcc.NewStore())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				muts := make([]mvcc.Mutation, n)
				for k := range muts {
					muts[k] = mvcc.Mutation{Key: fmt.Sprintf("k%d-%d", i, k), Value: []byte("v")}
				}
				cmd := CommitTxnCommand{
					RequestID: RequestID(fmt.Sprintf("req-%d", i)),
					TxnID:     uint64(i + 1),
					StartSeq:  0,
					Mutations: muts,
				}
				if _, err := f.Apply(uint64(i+1), cmd); err != nil {
					b.Fatalf("Apply: %v", err)
				}
			}
		})
	}
}

// BenchmarkApplyDuplicateRequestID measures Apply's idempotency
// short-circuit (docs/transactions.md §6): the identical command,
// resubmitted under its already-completed RequestID, resolved from the
// in-memory outcome table without touching internal/mvcc again.
func BenchmarkApplyDuplicateRequestID(b *testing.B) {
	f := New(mvcc.NewStore())
	cmd := CommitTxnCommand{
		RequestID: RequestID("dup-req"),
		TxnID:     1,
		StartSeq:  0,
		Mutations: []mvcc.Mutation{{Key: "k", Value: []byte("v")}},
	}
	if _, err := f.Apply(1, cmd); err != nil {
		b.Fatalf("initial Apply: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Apply(1, cmd); err != nil {
			b.Fatalf("duplicate Apply: %v", err)
		}
	}
}

// BenchmarkPrecheckDuplicateRequestID isolates the read-only Precheck
// path a caller (internal/txn.Manager, internal/node.Node) uses to
// decide whether Apply even needs to run again.
func BenchmarkPrecheckDuplicateRequestID(b *testing.B) {
	f := New(mvcc.NewStore())
	cmd := CommitTxnCommand{
		RequestID: RequestID("dup-req"),
		TxnID:     1,
		StartSeq:  0,
		Mutations: []mvcc.Mutation{{Key: "k", Value: []byte("v")}},
	}
	if _, err := f.Apply(1, cmd); err != nil {
		b.Fatalf("initial Apply: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Precheck(cmd); err != nil {
			b.Fatalf("Precheck: %v", err)
		}
	}
}

// BenchmarkEncodeDecodeCommitTxn measures the CommitTxn command's own
// wire encoding/decoding cost (command.go) — relevant to both WAL
// append size and Raft message payload size.
func BenchmarkEncodeDecodeCommitTxn(b *testing.B) {
	cmd := CommitTxnCommand{
		RequestID: RequestID("req-1"),
		TxnID:     1,
		StartSeq:  42,
		Mutations: []mvcc.Mutation{
			{Key: "k1", Value: []byte("value-one")},
			{Key: "k2", Value: []byte("value-two")},
			{Key: "k3", Tombstone: true},
		},
	}
	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeCommitTxn(cmd)
		}
	})
	b.Run("Decode", func(b *testing.B) {
		encoded := EncodeCommitTxn(cmd)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := DecodeCommitTxn(encoded); err != nil {
				b.Fatalf("DecodeCommitTxn: %v", err)
			}
		}
	})
}
