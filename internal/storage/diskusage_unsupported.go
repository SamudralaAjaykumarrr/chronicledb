//go:build !unix

package storage

// DiskUsage is unsupported on this platform (docs/v0.6.0-plan.md §6.2):
// it always fails with ErrDiskUsageUnsupported. Callers (internal/node
// startup validation) must refuse to start if a -disk-*-threshold flag
// is set on a platform where this is the active implementation, rather
// than silently running with disk-pressure admission disabled.
func DiskUsage(path string) (freeBytes, totalBytes uint64, err error) {
	return 0, 0, ErrDiskUsageUnsupported
}
