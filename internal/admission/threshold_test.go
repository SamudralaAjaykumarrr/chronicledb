package admission

import (
	"math"
	"strings"
	"testing"
)

func TestParseThresholdAbsolute(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"0", 0},
		{"100", 100},
		{"1B", 1},
		{"1KiB", 1024},
		{"2GiB", 2 * (1 << 30)},
		{"1MiB", 1 << 20},
		{"1TiB", 1 << 40},
		{"1.5MiB", uint64(1.5 * (1 << 20))},
		{"0GiB", 0},
	}
	for _, c := range cases {
		th, err := ParseThreshold(c.in)
		if err != nil {
			t.Errorf("ParseThreshold(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got := th.Bytes(0); got != c.want {
			t.Errorf("ParseThreshold(%q).Bytes(0) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseThresholdPercent(t *testing.T) {
	th, err := ParseThreshold("10%")
	if err != nil {
		t.Fatalf("ParseThreshold(10%%): %v", err)
	}
	if got, want := th.Bytes(1000), uint64(100); got != want {
		t.Errorf("Bytes(1000) = %d, want %d", got, want)
	}
}

func TestParseThresholdRejectsInvalid(t *testing.T) {
	cases := []string{
		"", "-1", "%", "abc", "GiB", "-5%", "150%", "1XiB",
		"NaN", "Inf", "-Inf", "1.2.3GiB", " ", "\x00\x01",
	}
	for _, in := range cases {
		if _, err := ParseThreshold(in); err == nil {
			t.Errorf("ParseThreshold(%q): expected error, got none", in)
		}
	}
}

func TestParseThresholdRoundTrips(t *testing.T) {
	cases := []string{"10%", "2GiB", "0", "512MiB", "1TiB", "0%", "100%"}
	for _, in := range cases {
		th, err := ParseThreshold(in)
		if err != nil {
			t.Fatalf("ParseThreshold(%q): %v", in, err)
		}
		th2, err := ParseThreshold(th.String())
		if err != nil {
			t.Fatalf("ParseThreshold(%q).String() = %q, which does not re-parse: %v", in, th.String(), err)
		}
		if th.Bytes(1<<40) != th2.Bytes(1<<40) {
			t.Errorf("round-trip mismatch for %q: %d != %d", in, th.Bytes(1<<40), th2.Bytes(1<<40))
		}
	}
}

// FuzzParseThreshold is AC-17 (docs/v0.6.0-plan.md §30.1): never
// panics on any input, and every accepted value round-trips through
// String/ParseThreshold.
func FuzzParseThreshold(f *testing.F) {
	seeds := []string{
		"10%", "2GiB", "-1", "%", "", "0", "100%", "150%", "-5%",
		"1KiB", "1MiB", "1TiB", "huge999999999999999999999999999GiB",
		"NaN", "Inf", "abc", "1.5MiB", "\x00", "über%", "  10%  ",
		strings.Repeat("9", 400) + "GiB",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		th, err := ParseThreshold(s)
		if err != nil {
			return // rejecting malformed input is correct behavior
		}
		// Every accepted value must round-trip.
		th2, err2 := ParseThreshold(th.String())
		if err2 != nil {
			t.Fatalf("ParseThreshold(%q) accepted, but its String() %q does not re-parse: %v", s, th.String(), err2)
		}
		b1, b2 := th.Bytes(1<<40), th2.Bytes(1<<40)
		if b1 != b2 {
			t.Fatalf("round-trip mismatch for %q: %d != %d", s, b1, b2)
		}
		// Bytes() must never be called in a way that produces NaN/Inf
		// artifacts leaking out as a nonsensical uint64.
		if math.IsNaN(float64(b1)) {
			t.Fatalf("Bytes() produced NaN-derived value for %q", s)
		}
	})
}
