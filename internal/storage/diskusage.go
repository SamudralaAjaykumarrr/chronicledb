package storage

import "errors"

// ErrDiskUsageUnsupported is returned by DiskUsage on a platform this
// build cannot probe disk headroom on (docs/v0.6.0-plan.md §6.2). It is
// declared here, outside either platform-specific file, so code that
// checks for it (internal/node's startup validation: refuse to start if
// a -disk-*-threshold flag is set and DiskUsage is unsupported) compiles
// identically regardless of build target.
var ErrDiskUsageUnsupported = errors.New("storage: disk usage probing is not supported on this platform")
