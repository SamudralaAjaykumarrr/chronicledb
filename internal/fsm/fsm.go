package fsm

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// fingerprint is the fixed-size digest of encodeCommitBody(cmd) — the
// part of a command that determines its deterministic outcome, minus
// the RequestID that indexes it. Used only to detect ambiguous
// RequestID reuse (ErrRequestIDPayloadMismatch); not a security
// primitive, so a standard non-keyed hash is sufficient.
type fingerprint [sha256.Size]byte

func fingerprintOf(cmd CommitTxnCommand) fingerprint {
	return sha256.Sum256(encodeCommitBody(cmd))
}

// outcomeEntry is what FSM durably-in-memory remembers per completed
// RequestID: the terminal outcome itself, plus the fingerprint of the
// command it was originally computed from (so a later mismatched reuse
// of the same RequestID can be detected — docs/transactions.md §6).
type outcomeEntry struct {
	outcome     Outcome
	fingerprint fingerprint
}

// FSM is ChronicleDB's deterministic replicated state-machine boundary
// (docs/architecture.md §5, ADR-0007): the sole function that turns an
// ordered CommitTxn command history into MVCC state and RequestID
// outcomes. FSM has no I/O of its own — internal/txn.Manager owns the
// durable log and decides, for each command, whether it has already
// been durably appended (a live commit) or is being replayed from one
// (recovery); either way it calls Apply with the log index the command
// occupies, and Apply's result is a pure function of that index, the
// command, and the FSM's own prior state.
//
// FSM is safe for concurrent use by multiple goroutines. In standalone
// mode, internal/txn.Manager additionally serializes Apply calls
// through its own single-writer ordering point (docs/transactions.md
// §9), so FSM's own lock is mostly a second, independent safety net
// (also what makes read-only accessors like GetOutcome safe to call
// from any goroutine without additional coordination).
type FSM struct {
	mu       sync.Mutex
	store    *mvcc.Store
	outcomes map[RequestID]outcomeEntry
}

// New returns an FSM that applies commands against store. store may
// already contain state restored some other way (e.g. a future
// state-machine snapshot install); New itself performs no I/O and no
// recovery — replaying a durable command history into a fresh FSM, in
// order, is the caller's responsibility (see internal/txn.Manager.recover).
func New(store *mvcc.Store) *FSM {
	return &FSM{store: store, outcomes: make(map[RequestID]outcomeEntry)}
}

// Store returns the FSM's underlying MVCC store, for read-only access
// (docs/mvcc.md §3 visibility reads do not go through Apply — only
// mutations do). Callers must never mutate committed state through
// this accessor except via Apply.
func (f *FSM) Store() *mvcc.Store { return f.store }

// Precheck is a read-only idempotency lookup, used by a caller (e.g.
// internal/txn.Manager) to decide whether a CommitTxn command even
// needs to be durably appended and applied at all (docs/transactions.md
// §6: "a pure retry that need not go through the log again once the
// outcome is already known — an optimization, not a correctness
// requirement"). It performs no mutation and assigns no CommitSeq.
//
//   - If cmd.RequestID has no recorded outcome, Precheck returns
//     ErrRequestIDUnknown: the caller must proceed to append+Apply.
//   - If cmd.RequestID has a recorded outcome from an identical command
//     (same TxnID, StartSeq, and Mutations), Precheck returns that
//     outcome unchanged and a nil error: the caller must not append or
//     re-apply anything.
//   - If cmd.RequestID has a recorded outcome from a *different*
//     command, Precheck returns ErrRequestIDPayloadMismatch: the
//     caller must reject this request outright (docs/transactions.md
//     §6) without touching the log or the original RequestID's
//     recorded outcome.
func (f *FSM) Precheck(cmd CommitTxnCommand) (Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookupLocked(cmd)
}

func (f *FSM) lookupLocked(cmd CommitTxnCommand) (Outcome, error) {
	entry, ok := f.outcomes[cmd.RequestID]
	if !ok {
		return Outcome{}, ErrRequestIDUnknown
	}
	if entry.fingerprint != fingerprintOf(cmd) {
		return Outcome{}, fmt.Errorf("%w: RequestID %q", ErrRequestIDPayloadMismatch, cmd.RequestID)
	}
	return entry.outcome, nil
}

// GetOutcome returns the durably recorded terminal outcome for id, if
// any (docs/transactions.md §7's conceptual GetRequestOutcome). ok is
// false if id has never completed — an explicit, client-knowledge-only
// "unknown" (docs/transactions.md §7.1), never guessed into success or
// failure.
func (f *FSM) GetOutcome(id RequestID) (outcome Outcome, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.outcomes[id]
	return entry.outcome, ok
}

// Apply is the sole deterministic boundary between "this command
// occupies log index index in the ordered committed history" and
// "database state" (docs/architecture.md §5, ADR-0007). It must be
// called, for a given FSM, with strictly increasing index values, each
// exactly once, in the same order the commands were durably appended
// (docs/transactions.md §4-5) — this holds both for a live commit
// (internal/txn.Manager.commit calls Apply immediately after its
// durable append+Sync) and for recovery replay
// (internal/txn.Manager.recover calls Apply for each record, in log
// order, from a freshly constructed FSM).
//
// Apply's steps, exactly as specified by docs/transactions.md §4:
//
//  1. Idempotency check first: if cmd.RequestID already has a recorded
//     outcome, return it (or ErrRequestIDPayloadMismatch on a
//     fingerprint mismatch) without evaluating conflicts or mutating
//     anything. This defends against a RequestID somehow occupying two
//     entries in the same command history (not possible via
//     internal/txn.Manager's own single-writer path today, since it
//     pre-checks Precheck before appending, but a future Raft-driven
//     proposal path could in principle re-propose an already-committed
//     RequestID — see docs/raft.md).
//  2. Conflict check (docs/mvcc.md §4): for each mutated key, if its
//     latest committed CommitSeq exceeds cmd.StartSeq, the whole
//     command aborts; no mutation is applied. This is deterministic
//     and reproducible on replay: replaying the identical prior command
//     sequence into a fresh FSM reconstructs the identical committed
//     state at every prior index, so the same command evaluated at the
//     same index always reaches the same conflict decision.
//  3. If no conflict: index becomes this command's CommitSeq, and every
//     mutation is applied atomically at that CommitSeq.
//  4. The terminal outcome (COMMITTED or ABORTED) is recorded for
//     cmd.RequestID as part of this same call, before returning — so a
//     crash between "Apply returned" and "outcome recorded" is not
//     possible; there is no such gap (docs/invariants.md IDEMPOTENCY:
//     "recording the outcome outside the atomic apply step" is exactly
//     the threat this closes).
//
// A non-nil error is returned only for an internal-consistency failure
// (e.g. store.ApplyCommit's monotonicity check, which should be
// unreachable given a correctly ordered index sequence) — never for a
// legitimate ABORTED business outcome, which is a normal Outcome value,
// not a Go error.
func (f *FSM) Apply(index uint64, cmd CommitTxnCommand) (Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if outcome, err := f.lookupLocked(cmd); err == nil {
		return outcome, nil
	} else if !errors.Is(err, ErrRequestIDUnknown) {
		return Outcome{}, err
	}

	var outcome Outcome
	if key, latest, conflict := f.store.CheckConflicts(cmd.StartSeq, cmd.Mutations); conflict {
		outcome = Outcome{
			RequestID:         cmd.RequestID,
			Status:            StatusAborted,
			ConflictKey:       key,
			ConflictLatestSeq: latest,
		}
	} else {
		if err := f.store.ApplyCommit(index, cmd.Mutations); err != nil {
			return Outcome{}, fmt.Errorf("fsm: applying command at index %d: %w", index, err)
		}
		outcome = Outcome{RequestID: cmd.RequestID, Status: StatusCommitted, CommitSeq: index}
	}

	f.outcomes[cmd.RequestID] = outcomeEntry{outcome: outcome, fingerprint: fingerprintOf(cmd)}
	return outcome, nil
}
