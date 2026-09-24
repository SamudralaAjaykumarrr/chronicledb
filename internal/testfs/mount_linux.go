//go:build linux

package testfs

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// MountTmpfs mounts a fresh tmpfs capped at sizeBytes onto dir (created if
// necessary), returning a cleanup func that unmounts it. Only callable
// from inside RunInNamespace's fn (mounting anything at all requires the
// CAP_SYS_ADMIN that namespace grants within itself).
func MountTmpfs(dir string, sizeBytes int64) (cleanup func(), err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("testfs: creating %s: %w", dir, err)
	}
	opts := fmt.Sprintf("size=%d", sizeBytes)
	if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, opts); err != nil {
		return nil, fmt.Errorf("testfs: mounting tmpfs (size=%d) at %s: %w", sizeBytes, dir, err)
	}
	return func() {
		if err := syscall.Unmount(dir, 0); err != nil && !errors.Is(err, syscall.EINVAL) {
			// Best-effort: the namespace (and everything mounted inside
			// it) is torn down automatically when this process exits
			// regardless, so a failed explicit unmount here is never a
			// leak — only ever a diagnostic worth noting if this ever
			// runs somewhere that expects a longer-lived mount namespace.
			fmt.Fprintf(os.Stderr, "testfs: unmount %s: %v\n", dir, err)
		}
	}, nil
}
