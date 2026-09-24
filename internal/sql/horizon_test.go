package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// TestStandaloneTxn_SL6_ReadAndScanPrefixRefuseBelowHorizon is SL-6's
// standalone-mode portion (docs/v0.6.0-plan.md §15.2a, §30.2):
// standaloneTxn.Read and standaloneTxn.ScanPrefix both propagate
// mvcc.ErrSnapshotTooOld — proving mergeLocalWrites' replacement of the
// deleted mergeScan/visibleInChain bypass actually closes the gap for
// this package's own entry points, not only at the mvcc/txn layers
// underneath it.
func TestStandaloneTxn_SL6_ReadAndScanPrefixRefuseBelowHorizon(t *testing.T) {
	mgr := openStandaloneManager(t, t.TempDir())
	e := NewStandaloneEngine(mgr)

	setup, err := e.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := setup.Write("p/k", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	startSeq, err := setup.Commit("r0")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	mgr.Store().SetGCWatermark(startSeq + 100)

	txn, err := e.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, _, err := txn.Read("p/k"); !errors.Is(err, mvcc.ErrSnapshotTooOld) {
		t.Errorf("Read below horizon: err = %v, want ErrSnapshotTooOld", err)
	}
	if _, err := txn.ScanPrefix("p/"); !errors.Is(err, mvcc.ErrSnapshotTooOld) {
		t.Errorf("ScanPrefix below horizon: err = %v, want ErrSnapshotTooOld", err)
	}
}
