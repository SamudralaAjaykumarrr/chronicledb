//go:build unix

package storage

import "syscall"

// DiskUsage reports free and total bytes on the filesystem containing
// path, via syscall.Statfs (docs/v0.6.0-plan.md §6.2) — standard
// library only, so docs/dependencies.md's zero-external-dependency
// policy is untouched. Per docs/support-matrix.md, only Linux amd64 is
// actually tested; the "unix" build tag also covers Darwin.
//
// freeBytes is Bavail (blocks available to an unprivileged user), not
// Bfree (blocks free including those reserved for root) — the correct
// figure for "how much room does this process actually have to write
// into," which is what admission's pressure thresholds must compare
// against.
func DiskUsage(path string) (freeBytes, totalBytes uint64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	bsize := uint64(stat.Bsize) //nolint:unconvert // Bsize's underlying type varies by unix platform (int32/int64)
	return uint64(stat.Bavail) * bsize, uint64(stat.Blocks) * bsize, nil
}
