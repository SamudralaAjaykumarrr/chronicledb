package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/testfs"
)

// TestAC13_RealFilesystemDiskPressureTightensThenRefusesThenRecovers is
// AC-13 (docs/v0.6.0-plan.md §30.1): on a real, small, dedicated
// filesystem filled toward -disk-pressure-threshold and then
// -disk-critical-threshold, admission tightens (LowSpace) and then
// refuses outright (Critical) — both well before any real write ever
// hits ENOSPC (the filesystem never actually fills completely) — and
// de-escalates automatically, with no restart, once space is freed.
func TestAC13_RealFilesystemDiskPressureTightensThenRefusesThenRecovers(t *testing.T) {
	testfs.RunInNamespace(t, func(t *testing.T) {
		const totalSize = 8 << 20 // 8 MiB
		dir := t.TempDir()
		cleanup, err := testfs.MountTmpfs(dir, totalSize)
		if err != nil {
			t.Skipf("tmpfs unavailable: %v", err)
		}
		defer cleanup()

		tc := newTestClusterWithAdmissionOverride(t, 1, func(cfg *Config) {
			cfg.DataDir = dir
			cfg.DiskPressureThreshold = "50%"
			cfg.DiskCriticalThreshold = "20%"
			cfg.ResourcePollInterval = 30 * time.Millisecond
			cfg.MaxInflightProposals = 8
		})
		leaderID := tc.awaitLeader(5 * time.Second)
		n := tc.node(leaderID)
		// Stop the node (closing its WAL/audit file handles on the
		// tmpfs) before the deferred unmount above runs — defers run
		// LIFO, so this one, registered after cleanup, fires first.
		// tc's own t.Cleanup(Stop) still fires later, harmlessly:
		// Stop is idempotent.
		defer n.Stop()

		doPropose := func(reqID string) error {
			_, err := propose(t, n, cmd(reqID, 0, 0, "k-"+reqID, "v"), 5*time.Second)
			return err
		}

		// Baseline: plenty of free space, Normal, writes succeed.
		if err := doPropose("baseline"); err != nil {
			t.Fatalf("baseline propose (Normal state): %v", err)
		}
		if got := n.admission.write.Stats().EffectiveConcurrent; got != 8 {
			t.Fatalf("baseline EffectiveConcurrent = %d, want 8", got)
		}

		ballast := filepath.Join(dir, "ballast.bin")
		writeBallast := func(size int) {
			t.Helper()
			if err := os.WriteFile(ballast, make([]byte, size), 0o644); err != nil {
				t.Fatalf("writing %d-byte ballast: %v", size, err)
			}
		}

		// Consume space down to ~3 MiB free (< 50% pressure threshold
		// of 4 MiB, > 20% critical threshold of 1.6 MiB): LowSpace.
		writeBallast(5 << 20)
		pollUntil(t, 3*time.Second, func() bool { return n.Status().DiskPressure == "low" })
		if got := n.admission.write.Stats().EffectiveConcurrent; got != 2 { // max(1, 8/4)
			t.Fatalf("LowSpace EffectiveConcurrent = %d, want 2", got)
		}
		free, _, derr := storage.DiskUsage(dir)
		if derr != nil {
			t.Fatalf("DiskUsage: %v", derr)
		}
		if free == 0 {
			t.Fatalf("filesystem is already completely full in LowSpace — thresholds left no margin, test is not proving \"before any real write failure\"")
		}
		// A write still succeeds in LowSpace (tightened, not refused).
		if err := doPropose("lowspace"); err != nil {
			t.Fatalf("propose during LowSpace: %v (should still succeed, only tightened)", err)
		}

		// Consume further, down to ~1 MiB free (< 1.6 MiB critical): Critical.
		writeBallast((8 << 20) - (1 << 20))
		pollUntil(t, 3*time.Second, func() bool { return n.Status().DiskPressure == "critical" })
		free, _, derr = storage.DiskUsage(dir)
		if derr != nil {
			t.Fatalf("DiskUsage: %v", derr)
		}
		if free == 0 {
			t.Fatalf("filesystem is already completely full in Critical — not proving admission acts before real ENOSPC")
		}
		err = doPropose("critical")
		var rej *admission.RejectedError
		if !errors.As(err, &rej) || rej.Reason != admission.ReasonDiskCritical {
			t.Fatalf("propose during Critical: got err=%v, want a RejectedError{Reason: disk_critical}", err)
		}

		// Free the space: de-escalation requires no restart.
		if err := os.Remove(ballast); err != nil {
			t.Fatalf("removing ballast: %v", err)
		}
		pollUntil(t, 3*time.Second, func() bool { return n.Status().DiskPressure == "normal" })
		if got := n.admission.write.Stats().EffectiveConcurrent; got != 8 {
			t.Fatalf("post-recovery EffectiveConcurrent = %d, want 8 (fully restored)", got)
		}
		if err := doPropose("recovered"); err != nil {
			t.Fatalf("propose after recovery: %v", err)
		}
	})
}
