// Microbenchmarks for internal/wal (docs/roadmap.md Phase 9 §WAL):
// append with and without crossing the durability (fsync) boundary,
// and replay of varying log sizes. Every benchmark uses a real
// temp-dir-backed WAL — never a mock of persistence (docs/roadmap.md's
// explicit "Do not cheat benchmarks: ... benchmarking mocked
// persistence while labeling it durable").
//
// Run: go test ./internal/wal/... -run '^$' -bench . -benchmem
package wal

import (
	"fmt"
	"testing"
)

func payloadOfSize(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// BenchmarkWALAppendNoSync measures AppendLogEntry alone — bytes
// appended to the current segment's in-process buffer/file handle, not
// yet guaranteed durable (docs/architecture.md §4 "Appended" vs.
// "Persisted"). This is the cost of framing + write(2), without the
// fsync durability boundary.
func BenchmarkWALAppendNoSync(b *testing.B) {
	for _, size := range []int{64, 256, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			w, _, err := Open(b.TempDir(), Options{})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer w.Close()
			payload := payloadOfSize(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.AppendLogEntry(payload); err != nil {
					b.Fatalf("AppendLogEntry: %v", err)
				}
			}
		})
	}
}

// BenchmarkWALAppendSync measures the full durability boundary
// (docs/wal.md §4): one AppendLogEntry followed by one Sync (fsync)
// per operation — the real cost a caller crossing "Persisted" pays for
// every commit in Phase 1's "every Sync call issues its own fsync, no
// group-commit optimization" model (wal.go's own doc comment).
func BenchmarkWALAppendSync(b *testing.B) {
	for _, size := range []int{64, 256, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			w, _, err := Open(b.TempDir(), Options{})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer w.Close()
			payload := payloadOfSize(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.AppendLogEntry(payload); err != nil {
					b.Fatalf("AppendLogEntry: %v", err)
				}
				if err := w.Sync(); err != nil {
					b.Fatalf("Sync: %v", err)
				}
			}
		})
	}
}

// BenchmarkWALReplay measures Replay throughput for a durable log of
// varying size — the dominant cost of recovery/restart before any
// snapshot exists (docs/recovery.md §1, docs/roadmap.md §Compaction/
// recovery performance's "WAL replay before compaction"). Each
// sub-benchmark builds a fresh log of the given entry count once (not
// timed), then times only repeated full replays of it.
func BenchmarkWALReplay(b *testing.B) {
	const payloadSize = 128
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			w, _, err := Open(dir, Options{})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			payload := payloadOfSize(payloadSize)
			for i := 0; i < n; i++ {
				if _, err := w.AppendLogEntry(payload); err != nil {
					b.Fatalf("AppendLogEntry: %v", err)
				}
			}
			if err := w.Sync(); err != nil {
				b.Fatalf("Sync: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it, err := w.Replay(1)
				if err != nil {
					b.Fatalf("Replay: %v", err)
				}
				count := 0
				for {
					_, ok, err := it.Next()
					if err != nil {
						b.Fatalf("Next: %v", err)
					}
					if !ok {
						break
					}
					count++
				}
				it.Close()
				if count != n {
					b.Fatalf("replayed %d entries, want %d", count, n)
				}
			}
			w.Close()
		})
	}
}

// BenchmarkWALAppendSequential measures sustained sequential append
// throughput without a per-record fsync — the "sequential append"
// target docs/roadmap.md §WAL names separately from the single-append
// latency benchmarks above, useful for segment-rotation/allocation
// behavior under sustained load rather than one operation's latency.
func BenchmarkWALAppendSequential(b *testing.B) {
	w, _, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer w.Close()
	payload := payloadOfSize(256)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.AppendLogEntry(payload); err != nil {
			b.Fatalf("AppendLogEntry: %v", err)
		}
	}
	b.StopTimer()
	if err := w.Sync(); err != nil {
		b.Fatalf("final Sync: %v", err)
	}
}
