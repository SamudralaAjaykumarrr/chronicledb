package txn

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// TestReadRefusesBelowHorizon is SL-6's portion for internal/txn
// (docs/v0.6.0-plan.md §15.2a, §30.2): Txn.Read propagates
// mvcc.ErrSnapshotTooOld rather than silently serving a stale value.
//
// Standalone mode does not run GC (§14.6 — no cluster generation exists
// there), so nothing in production ever advances a standalone Store's
// watermark; the horizon guard is nonetheless compiled into
// internal/mvcc unconditionally ("with the watermark permanently 0 it
// can never fire" — §14.6's own words), and this test exercises that
// compiled-in guard directly via the Store, standing in for a future
// mechanism (or a library embedder) that might set it.
func TestReadRefusesBelowHorizon(t *testing.T) {
	m, _ := newTestManager(t)

	setup := m.Begin()
	if err := setup.Write("K", []byte("v0")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	startSeq, err := setup.Commit("r0")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	m.Store().SetGCWatermark(startSeq + 100)

	txn := m.Begin() // captures a fresh StartSeq, below the watermark just set
	if _, _, err := txn.Read("K"); !errors.Is(err, mvcc.ErrSnapshotTooOld) {
		t.Fatalf("Read below the GC horizon: err = %v, want ErrSnapshotTooOld", err)
	}
}
