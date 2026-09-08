package backup

import "errors"

var (
	// ErrCorrupt indicates a backup failed validation: a bad manifest
	// checksum, a bad manifest format-version, a snapshot or WAL segment
	// whose bytes do not match the manifest's recorded size/checksum, or
	// a snapshot that fails internal/snapshot's own decode validation
	// (docs/enterprise-v1-plan.md §6 "BACKUP INTEGRITY"). A backup
	// failing with this error is never partially restored — Restore
	// validates every referenced component before it ever writes
	// anything to the target data directory.
	ErrCorrupt = errors.New("backup: corrupt or invalid backup")

	// ErrUnsupportedVersion indicates a manifest's format version byte
	// does not match FormatVersion. Never guessed at, mirroring
	// internal/wal and internal/snapshot's identical policy.
	ErrUnsupportedVersion = errors.New("backup: unsupported manifest format version")

	// ErrMissingComponent indicates a file the manifest references (the
	// manifest itself, the snapshot file, or a WAL segment file) is not
	// present on disk at all — distinct from ErrCorrupt (present but
	// invalid), though both are treated identically: Restore refuses to
	// proceed.
	ErrMissingComponent = errors.New("backup: referenced backup component is missing")

	// ErrTargetNotClean is returned by Restore when the target data
	// directory already contains WAL/snapshot state and RestoreOptions.Force
	// was not set (docs/enterprise-v1-plan.md §6 "DESTRUCTIVE RESTORE
	// ISOLATION"): restore never silently overwrites an existing, live
	// cluster's data.
	ErrTargetNotClean = errors.New("backup: target data directory is not clean (contains existing WAL/snapshot state)")

	// ErrInvalidBoundary indicates a requested PITR UntilIndex is
	// outside the range this backup can honor: below the backup's own
	// base snapshot boundary, or beyond the last index this backup
	// actually captured.
	ErrInvalidBoundary = errors.New("backup: requested restore boundary is outside what this backup includes")
)
