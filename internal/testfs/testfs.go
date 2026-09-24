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
	"fmt"
	"os"
	"os/exec"
	"regexp"
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

// MountTmpfs is declared per-platform: mount_linux.go has the real
// implementation (mounting anything requires Linux-only syscall.Mount/
// syscall.Unmount and the CLONE_NEWUSER/CLONE_NEWNS namespaces described
// above); mount_other.go stubs it out, reporting the operation as
// unsupported, for every other GOOS so this package still builds
// elsewhere.
