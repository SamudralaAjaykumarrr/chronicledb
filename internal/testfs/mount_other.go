//go:build !linux

package testfs

import "errors"

// errMountUnsupported is returned by MountTmpfs on platforms other than
// Linux: mounting a size-bounded tmpfs via an unprivileged user+mount
// namespace (CLONE_NEWUSER/CLONE_NEWNS) is a Linux-specific mechanism —
// see this package's doc comment. Rather than pretending to succeed (or
// being hidden behind a runtime.GOOS check that would still require
// syscall.Mount/syscall.Unmount to exist for this build to compile),
// this stub reports the operation as unsupported so callers see a clear
// error instead of silent, incorrect behavior.
var errMountUnsupported = errors.New("testfs: MountTmpfs is only supported on Linux")

// MountTmpfs always fails on this platform. See errMountUnsupported.
func MountTmpfs(dir string, sizeBytes int64) (cleanup func(), err error) {
	return nil, errMountUnsupported
}
