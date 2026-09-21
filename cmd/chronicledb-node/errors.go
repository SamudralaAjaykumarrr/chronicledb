// This file implements the stable admission-rejection HTTP contract
// (docs/v0.6.0-plan.md §8.3): every gated endpoint's overload response
// is distinct from every correctness outcome (§8.3's table) — 503, a
// Retry-After header, and a body naming the stable §8.2 reason.
package main

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
)

// admissionRejectedResponse is /propose's exact §8.3 example body
// shape: {"error":"…","reason":"…","retryable":true}.
type admissionRejectedResponse struct {
	Error     string `json:"error"`
	Reason    string `json:"reason"`
	Retryable bool   `json:"retryable"`
}

// setRetryAfter sets the Retry-After header from d, rounding up to at
// least 1 second for any nonzero duration so a sub-second value never
// renders as the misleading "0" ("no wait"). d == 0 means "do not retry
// here" (§8.2's ReasonShuttingDown) and sets no header at all.
func setRetryAfter(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		return
	}
	seconds := int(d.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
}

// asAdmissionRejection extracts an *admission.RejectedError from err,
// if any.
func asAdmissionRejection(err error) (*admission.RejectedError, bool) {
	var rej *admission.RejectedError
	if errors.As(err, &rej) {
		return rej, true
	}
	return nil, false
}

// writeAdmissionRejection writes rej in the §8.3 HTTP contract: 503,
// Retry-After (when rej.RetryAfter > 0), and the fixed
// admissionRejectedResponse body. Used directly by /propose (whose
// error body IS exactly this shape); other admin endpoints translate
// rej into their own existing response shape instead (each already has
// its own established Reason/Error/Retry-After conventions — see
// membershipErrorResponse, for example) but must set the identical 503
// status and Retry-After header this function sets, so callers of
// those endpoints see the identical retry contract regardless of which
// endpoint rejected them.
func writeAdmissionRejection(w http.ResponseWriter, rej *admission.RejectedError) {
	setRetryAfter(w, rej.RetryAfter)
	writeJSON(w, http.StatusServiceUnavailable, admissionRejectedResponse{
		Error:     rej.Error(),
		Reason:    string(rej.Reason),
		Retryable: rej.Reason != admission.ReasonShuttingDown,
	})
}
