// Package testfs is test-support-only (imported exclusively from _test.go
// files): it gives a test a real, size-bounded filesystem that produces a
// genuine syscall.ENOSPC when filled, for the "real filesystem" proof
// tier docs/v0.6.0-plan.md's SL-14/SL-15/AC-13 require — deliberately not
// a fake/mocked ENOSPC, since the property under test (DISK-FULL
// EXPLICITNESS, §27.9) is specifically that the classification survives
// contact with a real kernel error, not just a test double built to
// already look like one.
//
// A plain unprivileged process cannot mount a filesystem. What it CAN do
// on Linux, without root, is create a new user namespace mapped so it
// appears to be root inside it (CLONE_NEWUSER) alongside a new mount
// namespace (CLONE_NEWNS) — inside that pair of namespaces, mounting a
// size-limited tmpfs is permitted. Go's runtime is already multi-threaded
// by the time any test body runs, and unshare(CLONE_NEWUSER) refuses to
// run in a multi-threaded process, so this package cannot simply call
// syscall.Unshare from within the test binary itself. Instead it re-execs
// the current test binary, restricted to exactly the calling test by
// name, as a child of the external `unshare` command — mirroring the
// standard library's own os/exec "TestHelperProcess" re-exec pattern,
// with unshare replacing fork+exec as the thing that changes namespaces
// before the child's main() ever runs.
package testfs

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"syscall"
	"testing"
)

// childEnvVar, when set to a test's own t.Name(), signals that this
// process is already the re-exec'd child running inside the unshare'd
// user+mount namespace — RunInNamespace's own recursion base case.
const childEnvVar = "CHRONICLEDB_TESTFS_CHILD"

// unshareAvailable caches a one-time preflight (unshare exists and this
// kernel/sandbox actually permits an unprivileged user+mount namespace —
// some hardened kernels disable CLONE_NEWUSER for unprivileged users
// entirely) so every test using this package skips uniformly, with the
// same message, rather than each re-deriving it.
var unshareAvailable = func() error {
	if _, err := exec.LookPath("unshare"); err != nil {
		return fmt.Errorf("testfs: %w", err)
	}
	out, err := exec.Command("unshare", "--user", "--mount", "--map-root-user", "--", "true").CombinedOutput()
	if err != nil {
		return fmt.Errorf("testfs: unprivileged user+mount namespaces are not available in this environment: %v: %s", err, out)
	}
	return nil
}()

// RunInNamespace runs fn(t) inside a fresh, unprivileged user+mount
// namespace (so fn can call MountTmpfs), skipping the test with a clear
// reason if this sandbox does not support that. It re-execs the current
// test binary, selecting only the calling test by exact name, exactly
// once — a nested RunInNamespace call from inside the re-exec'd child
// runs fn directly, since the child already has its own namespace.
//
// Requires t.Parallel to not have been called before this (the re-exec
// depends on -test.run selecting exactly one top-level test).
func RunInNamespace(t *testing.T, fn func(t *testing.T)) {
	t.Helper()

	if os.Getenv(childEnvVar) == t.Name() {
		fn(t)
		return
	}

	if err := unshareAvailable; err != nil {
		t.Skip(err)
	}

	pattern := "^" + regexp.QuoteMeta(t.Name()) + "$"
	cmd := exec.Command("unshare", "--user", "--mount", "--map-root-user", "--",
		os.Args[0], "-test.run="+pattern, "-test.v")
	cmd.Env = append(os.Environ(), childEnvVar+"="+t.Name())
	out, err := cmd.CombinedOutput()
	t.Logf("testfs: re-exec'd child output:\n%s", out)
	if err != nil {
		t.Fatalf("testfs: re-exec'd child failed: %v", err)
	}
}

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
