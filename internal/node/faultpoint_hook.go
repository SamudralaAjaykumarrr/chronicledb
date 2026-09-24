//go:build faulttest

package node

// SetFaultPointForTest arms fn as the hook triggerFaultPoint consults at
// every named FaultPoint from here on; fn returns true to crash the
// node right there, false to let it continue normally. Passing nil
// disarms it.
//
// This function exists only in a binary built with -tags faulttest
// (docs/v0.6.0-plan.md §33 slice 10a) — it is not present in an
// ordinary `go build`/`go test` (no tag), including every production
// binary cmd/chronicledb-node ships, so nothing outside a deliberately
// faulttest-tagged test binary can ever arm a crash hook.
func SetFaultPointForTest(fn func(FaultPoint) bool) {
	if fn == nil {
		faultHook.Store(nil)
		return
	}
	f := fn
	faultHook.Store(&f)
}

// CrashOnceAtFaultPointForTest is a convenience wrapper around
// SetFaultPointForTest: it arms a hook that crashes the very first time
// FaultPoint target is reached and never again (so a node that legally
// passes through the same point again after restarting — e.g. a second
// maybeSnapshot cycle — is not crashed a second time).
func CrashOnceAtFaultPointForTest(target FaultPoint) {
	fired := false
	SetFaultPointForTest(func(p FaultPoint) bool {
		if fired || p != target {
			return false
		}
		fired = true
		return true
	})
}
