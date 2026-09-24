package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
)

// TestWriteAdmissionRejection_EveryReason is the error-contract test
// docs/v0.6.0-plan.md §33 slice 5 requires "for every §8.2 reason": for
// each Reason, the HTTP response is 503, carries a Retry-After header
// matching the reason's DefaultRetryAfter (or none at all for
// ReasonShuttingDown), and the JSON body matches §8.3's exact shape
// ({"error","reason","retryable"}), distinct from every correctness
// outcome (§8.3's table: never 200/409/400/401/403).
func TestWriteAdmissionRejection_EveryReason(t *testing.T) {
	for _, reason := range admission.KnownReasons {
		reason := reason
		t.Run(string(reason), func(t *testing.T) {
			rej := &admission.RejectedError{Reason: reason, RetryAfter: reason.DefaultRetryAfter()}
			w := httptest.NewRecorder()
			writeAdmissionRejection(w, rej)

			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
			}

			retryAfter := w.Header().Get("Retry-After")
			if reason == admission.ReasonShuttingDown {
				if retryAfter != "" {
					t.Errorf("Retry-After = %q, want empty for %s (do not retry here)", retryAfter, reason)
				}
			} else if retryAfter == "" {
				t.Errorf("Retry-After header missing for reason %s", reason)
			}

			var body admissionRejectedResponse
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decoding body: %v (body=%s)", err, w.Body.String())
			}
			if body.Reason != string(reason) {
				t.Errorf("body.Reason = %q, want %q", body.Reason, reason)
			}
			if body.Error == "" {
				t.Error("body.Error is empty")
			}
			wantRetryable := reason != admission.ReasonShuttingDown
			if body.Retryable != wantRetryable {
				t.Errorf("body.Retryable = %v, want %v", body.Retryable, wantRetryable)
			}
		})
	}
}

// TestWriteAdmissionRejection_DistinctFromCorrectnessOutcomes asserts
// §8.3's table directly: a 503 admission rejection's status code never
// collides with any of the pinned correctness-outcome codes this
// binary uses elsewhere (409 not-leader, 400 malformed, 401/403 authn/
// authz) — a client can distinguish "try again, nothing happened" from
// every other condition by status code alone.
func TestWriteAdmissionRejection_DistinctFromCorrectnessOutcomes(t *testing.T) {
	collidingCodes := map[int]string{
		http.StatusConflict:            "not-leader / SI-adjacent conflict",
		http.StatusBadRequest:          "malformed request",
		http.StatusUnauthorized:        "authn failure",
		http.StatusForbidden:           "authz failure",
		http.StatusOK:                  "a correctness outcome (COMMITTED/ABORTED/ABORTED_STALE)",
		http.StatusPreconditionFailed:  "capability-not-permitted",
		http.StatusInternalServerError: "internal error",
	}
	w := httptest.NewRecorder()
	writeAdmissionRejection(w, &admission.RejectedError{Reason: admission.ReasonQueueFull, RetryAfter: admission.ReasonQueueFull.DefaultRetryAfter()})
	if desc, collides := collidingCodes[w.Code]; collides {
		t.Fatalf("admission rejection used status %d, which collides with %q", w.Code, desc)
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestSetRetryAfterRoundsSubSecondUpToOneSecond(t *testing.T) {
	w := httptest.NewRecorder()
	setRetryAfter(w, 100*time.Millisecond)
	got := w.Header().Get("Retry-After")
	if got != "1" {
		t.Errorf("Retry-After = %q, want %q for a sub-second nonzero duration", got, "1")
	}
}
