// Microbenchmarks for internal/snapshot (docs/roadmap.md Phase 9
// §Snapshot performance): encoding a state-machine snapshot from a
// live FSM, and decoding (restoring) one — at several controlled key
// counts. Manager.Create/Install's own durable temp-file/fsync/rename
// sequence (docs/snapshots.md §3) is measured separately by
// internal/node's end-to-end snapshot-latency benchmark, which is
// where the fsync cost this phase's brief asks about ("how snapshot
// creation affects availability/latency") actually shows up — this
// file isolates the pure encode/decode CPU/allocation cost.
//
// Run: go test ./internal/snapshot/... -run '^$' -bench . -benchmem
package snapshot

import (
	"fmt"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

func fsmWithKeys(n int) *fsm.FSM {
	store := mvcc.NewStore()
	f := fsm.New(store)
	for i := 0; i < n; i++ {
		cmd := fsm.CommitTxnCommand{
			RequestID: fsm.RequestID(fmt.Sprintf("seed-%d", i)),
			TxnID:     uint64(i + 1),
			StartSeq:  0,
			Mutations: []mvcc.Mutation{{Key: fmt.Sprintf("key-%08d", i), Value: []byte("some-representative-value-bytes")}},
		}
		if _, err := f.Apply(uint64(i+1), cmd); err != nil {
			panic(err)
		}
	}
	return f
}

// BenchmarkEncode measures Encode's cost at several controlled key
// counts (docs/roadmap.md §Benchmark dataset sizes: "small/medium/
// larger-but-local-development-friendly").
func BenchmarkEncode(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			f := fsmWithKeys(n)
			meta := Meta{LastIncludedIndex: uint64(n), LastIncludedTerm: 1}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = Encode(meta, f)
			}
		})
	}
}

// BenchmarkDecode measures Decode (restore) at the same key counts —
// the CPU-bound half of both a fresh-startup snapshot restore
// (docs/recovery.md §1) and a follower's InstallSnapshot catch-up
// (docs/snapshots.md §7).
func BenchmarkDecode(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			f := fsmWithKeys(n)
			meta := Meta{LastIncludedIndex: uint64(n), LastIncludedTerm: 1}
			data := Encode(meta, f)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Decode(data); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
		})
	}
}
