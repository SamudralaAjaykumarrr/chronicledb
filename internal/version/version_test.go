package version

import (
	"strings"
	"testing"
)

func TestString_DefaultsBeforeLdflags(t *testing.T) {
	// A plain `go build`/`go test` with no -ldflags (i.e. not built via
	// scripts/build-release.sh or the release workflow) must report
	// placeholder values, never something that could be mistaken for a
	// tagged release.
	if Version != "dev" {
		t.Fatalf("Version default = %q, want %q", Version, "dev")
	}
	if Commit != "none" {
		t.Fatalf("Commit default = %q, want %q", Commit, "none")
	}
	if Date != "unknown" {
		t.Fatalf("Date default = %q, want %q", Date, "unknown")
	}
}

func TestString_ContainsAllFields(t *testing.T) {
	origVersion := Version
	defer func() { Version = origVersion }()

	Version = "v1.2.3"
	s := String()
	for _, want := range []string{"chronicledb-node", "v1.2.3", "commit", "built"} {
		if !strings.Contains(s, want) {
			t.Fatalf("String() = %q, missing %q", s, want)
		}
	}
}
