package transport

import (
	"bytes"
	"testing"
)

// FuzzReadFrame proves readFrame never panics on arbitrary, adversarial
// byte input (docs/failure-model.md §6: a malformed frame from a peer,
// or an outright hostile one, must never crash a node). The panic-
// recovery inside readFrame/writeFrame is defense in depth; this fuzz
// target is what actually exercises it against inputs a human wouldn't
// think to write by hand.
func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{WireVersion, 0, 0, 0, 0})
	f.Add([]byte{WireVersion, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0, 0, 0, 0, 1, 'x'})

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("readFrame panicked on input %x: %v", data, r)
			}
		}()
		_, _ = readFrame(bytes.NewReader(data))
	})
}
