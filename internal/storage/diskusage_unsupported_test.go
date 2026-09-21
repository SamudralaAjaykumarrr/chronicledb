//go:build !unix

package storage

import (
	"errors"
	"testing"
)

// TestDiskUsageUnsupported is docs/v0.6.0-plan.md §33 slice 3's "the
// unsupported stub tested by build tag" exit criterion: on any non-unix
// build target (verified continuously in CI via `GOOS=windows go vet
// ./...`, since this repository's own CI runner is Linux-only —
// docs/support-matrix.md), DiskUsage always fails closed with
// ErrDiskUsageUnsupported rather than fabricating a value.
func TestDiskUsageUnsupported(t *testing.T) {
	free, total, err := DiskUsage(".")
	if !errors.Is(err, ErrDiskUsageUnsupported) {
		t.Errorf("DiskUsage(\".\") error = %v, want ErrDiskUsageUnsupported", err)
	}
	if free != 0 || total != 0 {
		t.Errorf("DiskUsage(\".\") = (%d, %d), want (0, 0) on the unsupported path", free, total)
	}
}
