package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/txn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// openStandaloneManager opens a real internal/wal-backed
// internal/txn.Manager rooted at dir — genuine disk I/O, matching the
// rest of this repository's testing style (docs/testing-strategy.md
// §1: component tests run against real dependencies, not stubs).
func openStandaloneManager(t *testing.T, dir string) *txn.Manager {
	t.Helper()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	mgr, err := txn.NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("txn.NewManager: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return mgr
}

func newStandaloneTestEngine(t *testing.T) Engine {
	t.Helper()
	return NewStandaloneEngine(openStandaloneManager(t, t.TempDir()))
}

func TestStandaloneEngineBeginIncreasingStartSeq(t *testing.T) {
	ctx := context.Background()
	e := newStandaloneTestEngine(t)
	txn1, err := e.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	seq0 := txn1.StartSeq()
	if err := txn1.Write("k", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := txn1.Commit("r1"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	txn2, err := e.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if txn2.StartSeq() <= seq0 {
		t.Errorf("StartSeq did not advance: seq0=%d seq1=%d", seq0, txn2.StartSeq())
	}
}

func TestTxnReadOwnWriteShadowsCommitted(t *testing.T) {
	ctx := context.Background()
	e := newStandaloneTestEngine(t)

	t1, _ := e.Begin(ctx)
	_ = t1.Write("k", []byte("committed"))
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	t2, _ := e.Begin(ctx)
	v, found, err := t2.Read("k")
	if err != nil || !found || string(v) != "committed" {
		t.Fatalf("Read before own write: v=%q found=%v err=%v", v, found, err)
	}
	_ = t2.Write("k", []byte("local"))
	v, found, err = t2.Read("k")
	if err != nil || !found || string(v) != "local" {
		t.Fatalf("Read after own write: v=%q found=%v err=%v", v, found, err)
	}
	_ = t2.Delete("k")
	_, found, err = t2.Read("k")
	if err != nil || found {
		t.Fatalf("Read after own delete: found=%v err=%v, want not found", found, err)
	}
	_ = t2.Abort()

	// The abort must leave no trace: a fresh transaction still sees
	// the original committed value.
	t3, _ := e.Begin(ctx)
	v, found, err = t3.Read("k")
	if err != nil || !found || string(v) != "committed" {
		t.Fatalf("Read after abort: v=%q found=%v err=%v, want unchanged committed value", v, found, err)
	}
}

func TestTxnScanPrefixMergesLocalAndCommitted(t *testing.T) {
	ctx := context.Background()
	e := newStandaloneTestEngine(t)

	setup, _ := e.Begin(ctx)
	_ = setup.Write("p/1", []byte("a"))
	_ = setup.Write("p/2", []byte("b"))
	_ = setup.Write("q/1", []byte("other-prefix"))
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx, _ := e.Begin(ctx)
	_ = tx.Write("p/3", []byte("c"))  // new local key under the prefix
	_ = tx.Delete("p/1")              // local tombstone shadowing a committed key
	_ = tx.Write("p/2", []byte("b2")) // local overwrite of a committed key

	kvs, err := tx.ScanPrefix("p/")
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	got := map[string]string{}
	for _, kv := range kvs {
		got[kv.Key] = string(kv.Value)
	}
	want := map[string]string{"p/2": "b2", "p/3": "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q = %q, want %q", k, got[k], v)
		}
	}
	if _, ok := got["p/1"]; ok {
		t.Errorf("p/1 should be shadowed by local tombstone, got %v", got)
	}
	if _, ok := got["q/1"]; ok {
		t.Errorf("q/1 should not match prefix p/, got %v", got)
	}
}

func TestTxnCommitConflict(t *testing.T) {
	ctx := context.Background()
	e := newStandaloneTestEngine(t)

	seed, _ := e.Begin(ctx)
	_ = seed.Write("k", []byte("v0"))
	if _, err := seed.Commit("seed"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	t1, _ := e.Begin(ctx)
	t2, _ := e.Begin(ctx)
	_ = t1.Write("k", []byte("from-t1"))
	_ = t2.Write("k", []byte("from-t2"))

	if _, err := t1.Commit("req-t1"); err != nil {
		t.Fatalf("t1 Commit should succeed (first committer): %v", err)
	}
	_, err := t2.Commit("req-t2")
	if err == nil {
		t.Fatal("t2 Commit should conflict (first-committer-wins), got nil error")
	}
	if !errors.Is(err, ErrConflict) {
		t.Errorf("t2 Commit error = %v, want ErrConflict", err)
	}
}

func TestMergeScanSharedHelperDeterministicOrder(t *testing.T) {
	store := mvcc.NewStore()
	if err := store.ApplyCommit(1, []mvcc.Mutation{
		{Key: "p/b", Value: []byte("2")},
		{Key: "p/a", Value: []byte("1")},
		{Key: "p/c", Value: []byte("3")},
	}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	kvs := mergeScan("p/", 1, store, nil)
	if len(kvs) != 3 {
		t.Fatalf("got %d results, want 3", len(kvs))
	}
	for i := 1; i < len(kvs); i++ {
		if kvs[i-1].Key >= kvs[i].Key {
			t.Errorf("results not sorted: %v", kvs)
		}
	}
}
