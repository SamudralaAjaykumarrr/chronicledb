package admission

import (
	"fmt"
	"time"
)

// ErrOverloaded is the sentinel every admission rejection satisfies:
// errors.Is(err, ErrOverloaded) is true for any *RejectedError,
// regardless of its specific Reason (docs/v0.6.0-plan.md §8.1).
var ErrOverloaded = fmt.Errorf("admission: overloaded")

// RejectedError is the concrete error a Gate rejection returns
// (docs/v0.6.0-plan.md §8.1), retrievable with errors.As and carrying
// the stable Reason (§8.2) and a suggested Retry-After.
//
// A rejection is never an fsm.Outcome (§8.1's Go-level statement of
// REJECTION SAFETY): internal/admission itself has no knowledge of
// fsm.Outcome at all — every gate acquisition happens strictly before a
// request reaches internal/node's proposeCh, and therefore strictly
// before raft.Core.Step(InputPropose), so a rejected request is
// indistinguishable, in every durable and replicated artifact, from a
// request that was never made (docs/v0.6.0-plan.md §8.4).
type RejectedError struct {
	Reason     Reason
	RetryAfter time.Duration
	Detail     string
}

func (e *RejectedError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("admission: overloaded (%s): %s", e.Reason, e.Detail)
	}
	return fmt.Sprintf("admission: overloaded (%s)", e.Reason)
}

// Is makes errors.Is(err, ErrOverloaded) true for every *RejectedError,
// independent of its specific Reason — the sentinel a caller checks
// when it only cares "was this rejected for capacity," and Reason/
// errors.As is for a caller that needs the specific reason.
func (e *RejectedError) Is(target error) bool { return target == ErrOverloaded }
