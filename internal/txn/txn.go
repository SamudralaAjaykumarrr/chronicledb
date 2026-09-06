// Package txn implements ChronicleDB's transaction session state
// (TxnID, StartSeq, local write set) and its commit/recovery
// integration with internal/wal and internal/fsm, as specified in
// docs/transactions.md and docs/mvcc.md.
//
// The deterministic conflict-check-then-apply decision and the
// RequestID idempotency table live in internal/fsm (Phase 3,
// docs/roadmap.md) — internal/txn.Manager is the orchestration layer
// around it: it owns the durable log, decides for each commit attempt
// whether internal/fsm even needs to see a fresh command (§6's
// documented retry-without-reappending optimization), and calls
// internal/fsm.Apply with the exact durable log index a command
// occupies, both for a live commit and for recovery replay (see
// Manager.commit and Manager.recover), so both paths run the identical
// deterministic decision.
package txn

import (
	"errors"
	"fmt"
	"sync"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// TxnID identifies one in-progress, client-visible transaction session
// (docs/architecture.md §3). It is ephemeral: assigned in-memory at
// Begin, never made durable on its own, and meaningless after restart
// (an open transaction does not survive a crash — docs/transactions.md
// §2).
type TxnID uint64

// State is a transaction's lifecycle state (docs/transactions.md §1).
type State int

const (
	// StateActive is the state from Begin until Commit or Abort.
	StateActive State = iota
	// StateCommitted is terminal: the transaction's mutation set (if
	// any) is durably applied and visible to snapshots with
	// StartSeq >= its CommitSeq.
	StateCommitted
	// StateAborted is terminal: none of the transaction's mutations were
	// ever applied. Reached via an explicit Abort, or implicitly when
	// Commit fails (conflict, a rejected RequestID reuse, or a
	// durability error) — either way, the transaction leaves no trace
	// in committed state.
	StateAborted
)

func (s State) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateCommitted:
		return "committed"
	case StateAborted:
		return "aborted"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// Manager owns the durable log, the deterministic internal/fsm Apply
// boundary, and the single serialization point through which every
// commit is decided (docs/mvcc.md §4: the conflict decision is made
// deterministically, in committed log/apply order — in standalone
// mode, append order and apply order are the same single-writer
// sequence, so Manager's mu is that sequence point). Manager is safe
// for concurrent use by multiple goroutines: Begin and each Txn's
// Commit/Abort may be called concurrently from different goroutines.
type Manager struct {
	walog *wal.WAL
	fsm   *fsm.FSM

	// mu serializes: reading lastSeq for a new Begin, and the entire
	// idempotency-precheck -> append -> sync -> apply sequence of a
	// commit. Holding it across the WAL Sync call (docs/storage.md §5)
	// is what makes standalone-mode commit ordering exactly equal to
	// durable append order, which is what the deterministic
	// conflict/apply rule requires (docs/mvcc.md §4.1). It does not
	// serialize reads: Txn.Read only ever touches internal/fsm's
	// mvcc.Store through its own, separate lock, so readers are never
	// blocked behind an in-flight commit's fsync.
	mu        sync.Mutex
	lastSeq   uint64
	nextTxnID uint64
}

// NewManager wires a Manager to an already-open WAL and MVCC store, and
// runs recovery (docs/recovery.md) before returning: every durably
// appended CommitTxn command in w's log is deterministically re-applied
// to a fresh internal/fsm.FSM over store, in order, exactly as a live
// commit would apply it (see Manager.recover). A non-nil error means
// recovery detected a durable-history inconsistency it refuses to guess
// a resolution for (RECOVERY-NON-INVENTION) — the caller must not treat
// store as usable.
func NewManager(w *wal.WAL, store *mvcc.Store) (*Manager, error) {
	m := &Manager{walog: w, fsm: fsm.New(store)}
	if err := m.recover(); err != nil {
		return nil, err
	}
	return m, nil
}

// recover replays every RecordTypeLogEntry record in the WAL from index
// 1 forward, decodes each as a CommitTxn command, and calls
// internal/fsm.Apply for it — the identical function a live commit
// calls (docs/recovery.md §11: "running internal/fsm.Apply for each, in
// order, exactly as if being applied for the first time"). Apply is
// deterministic, so replaying the exact same command sequence,
// in order, into a fresh FSM reproduces the exact same
// COMMITTED/ABORTED decisions — including for a command that
// legitimately conflicted live (its ABORTED outcome, and its
// RequestID's terminal record, are reconstructed identically, not
// treated as corruption; see docs/transactions.md §9/§10). Apply
// itself still fails closed (RECOVERY-NON-INVENTION) if it ever
// detects a genuine inconsistency — e.g. the same RequestID appearing
// twice in the durable log with two different command fingerprints,
// which internal/fsm.Apply reports as
// fsm.ErrRequestIDPayloadMismatch — which recover propagates as a
// startup-refusing error rather than guessing which one to keep.
func (m *Manager) recover() error {
	it, err := m.walog.Replay(1)
	if err != nil {
		return fmt.Errorf("txn: recovery: opening replay iterator: %w", err)
	}
	defer it.Close()

	for {
		rec, ok, err := it.Next()
		if err != nil {
			return fmt.Errorf("txn: recovery: replay: %w", err)
		}
		if !ok {
			break
		}
		cmd, err := fsm.DecodeCommitTxn(rec.Payload)
		if err != nil {
			return fmt.Errorf("txn: recovery: decoding record at index %d: %w", rec.Index, err)
		}
		if _, err := m.fsm.Apply(rec.Index, cmd); err != nil {
			return fmt.Errorf("txn: recovery: applying record at index %d: %w", rec.Index, err)
		}
		if rec.Index > m.lastSeq {
			m.lastSeq = rec.Index
		}
	}
	return nil
}

// Begin starts a new transaction, capturing StartSeq as the durable
// log's current committed watermark (docs/architecture.md §3: "Before
// Raft: derived from the local durable log's monotonic index"). Taking
// Manager.mu to read lastSeq means a Begin racing an in-flight Commit
// always sees either the fully-applied "before" or "after" state, never
// a torn intermediate one.
func (m *Manager) Begin() *Txn {
	m.mu.Lock()
	startSeq := m.lastSeq
	m.nextTxnID++
	id := TxnID(m.nextTxnID)
	m.mu.Unlock()

	return &Txn{
		mgr:      m,
		id:       id,
		startSeq: startSeq,
		state:    StateActive,
		writes:   make(map[string]mvcc.Mutation),
	}
}

// Store returns the Manager's underlying MVCC store, for read-only
// access (docs/mvcc.md §3: visibility reads do not go through Apply) by
// a caller that needs more than a single-key read — e.g. a full-table
// scan built on top of internal/mvcc.Store.Export (docs/sql.md §5's
// SELECT-without-predicate path). Mirrors internal/fsm.FSM.Store's
// identical accessor pattern. Callers must never mutate committed state
// through this accessor except via a Txn's Commit.
func (m *Manager) Store() *mvcc.Store { return m.fsm.Store() }

// GetRequestOutcome resolves requestID's durable terminal outcome
// (docs/transactions.md §7's conceptual GetRequestOutcome), without
// resubmitting any mutation payload. A nil error means outcome is the
// stable, terminal COMMITTED or ABORTED result. A non-nil error
// wrapping fsm.ErrRequestIDUnknown means requestID has never completed
// — an explicit client-knowledge-only "unknown" (docs/transactions.md
// §7.1), never a guess at success or failure.
func (m *Manager) GetRequestOutcome(requestID fsm.RequestID) (fsm.Outcome, error) {
	outcome, ok := m.fsm.GetOutcome(requestID)
	if !ok {
		return fsm.Outcome{}, fmt.Errorf("txn: %w: %q", fsm.ErrRequestIDUnknown, requestID)
	}
	return outcome, nil
}

// Resubmit runs the exact same deterministic commit path as Txn.Commit
// (docs/transactions.md §3-6, §9/§10), for a caller that already has a
// fully-formed logical CommitTxn request in hand — txnID, startSeq, and
// mutations exactly as originally submitted — rather than a live Txn
// session. This is the entry point a genuine retry of an uncertain or
// completed commit goes through: per docs/transactions.md §8, once a
// commit has been submitted its outcome is "no longer tied to the
// original client connection ... or the original in-memory session,"
// so resolving a retry must not require the original Txn object (which
// may not even exist any more, e.g. after a process restart) to still
// be alive — only the original request's own fields, which the client
// is responsible for remembering and resending unchanged
// (docs/transactions.md §6). Resubmitting with the identical
// RequestID and an identical (txnID, startSeq, mutations) tuple is
// exactly what makes Precheck recognize it as the same logical
// request; any field that differs from what was originally recorded is
// a mismatched reuse (fsm.ErrRequestIDPayloadMismatch), never silently
// accepted.
func (m *Manager) Resubmit(requestID fsm.RequestID, txnID TxnID, startSeq uint64, mutations []mvcc.Mutation) (fsm.Outcome, error) {
	return m.commit(txnID, requestID, startSeq, mutations)
}

// commit runs the deterministic idempotency-precheck -> append -> sync
// -> apply sequence for one transaction's mutation set
// (docs/transactions.md §3-6, §9/§10). A read-only transaction (no
// mutations) commits trivially: nothing is appended and internal/fsm is
// never consulted, since there is nothing whose durability, conflict
// status, or RequestID idempotency could matter (docs/transactions.md
// §2 — only a submitted mutation set becomes a durable command; §9 —
// this bypass is the one explicitly documented non-replicated path).
func (m *Manager) commit(id TxnID, requestID fsm.RequestID, startSeq uint64, mutations []mvcc.Mutation) (fsm.Outcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(mutations) == 0 {
		return fsm.Outcome{RequestID: requestID, Status: fsm.StatusCommitted, CommitSeq: m.lastSeq}, nil
	}

	cmd := fsm.CommitTxnCommand{RequestID: requestID, TxnID: uint64(id), StartSeq: startSeq, Mutations: mutations}

	outcome, err := m.fsm.Precheck(cmd)
	switch {
	case err == nil:
		// Known, matching retry (docs/transactions.md §6): the
		// original append+Apply already ran for this RequestID.
		// Returning its recorded outcome directly, without touching
		// the WAL again, is what makes retrying a completed RequestID
		// not grow the log — see TestRetryDoesNotAppendToWAL.
	case errors.Is(err, fsm.ErrRequestIDUnknown):
		payload := fsm.EncodeCommitTxn(cmd)
		idx, aerr := m.walog.AppendLogEntry(payload)
		if aerr != nil {
			return fsm.Outcome{}, fmt.Errorf("txn: txn %d durable append failed: %w", id, aerr)
		}
		if serr := m.walog.Sync(); serr != nil {
			return fsm.Outcome{}, fmt.Errorf("txn: txn %d durability sync failed: %w", id, serr)
		}
		// The command is now durably persisted at idx before Apply
		// ever runs (docs/failure-model.md §1.8: never treat a failed
		// Sync as success, and never mark a RequestID complete before
		// the durability path that backs it has actually succeeded).
		outcome, err = m.fsm.Apply(idx, cmd)
		if err != nil {
			return fsm.Outcome{}, fmt.Errorf("txn: txn %d internal apply failure: %w", id, err)
		}
		m.lastSeq = idx
	default:
		// A different command was previously recorded under this exact
		// RequestID: reject outright (docs/transactions.md §6's safe
		// default). The original RequestID's recorded outcome is
		// completely untouched.
		return fsm.Outcome{}, fmt.Errorf("txn: txn %d: %w", id, err)
	}

	if outcome.Status == fsm.StatusAborted {
		return outcome, fmt.Errorf("txn: txn %d commit conflict on key %q (latest committed=%d > StartSeq=%d): %w",
			id, outcome.ConflictKey, outcome.ConflictLatestSeq, startSeq, ErrConflict)
	}
	return outcome, nil
}

// Txn is one open transaction session (docs/transactions.md §1). A Txn
// is safe for concurrent use by multiple goroutines, though ordinary
// use is single-goroutine-per-transaction.
type Txn struct {
	mgr      *Manager
	id       TxnID
	startSeq uint64

	mu     sync.Mutex
	state  State
	writes map[string]mvcc.Mutation
	// order preserves first-write insertion order for building the
	// mutation slice at commit time, independent of Go's randomized map
	// iteration order — the encoded record's mutation order is fixed
	// once and for all at that point, so this only affects readability/
	// reproducibility of the encoded bytes, not correctness of replay
	// (which simply reads back whatever order was chosen).
	order []string
}

// ID returns the transaction's ephemeral session identifier.
func (t *Txn) ID() TxnID { return t.id }

// StartSeq returns the logical snapshot sequence captured at Begin.
// Fixed for the lifetime of the transaction (docs/mvcc.md §3).
func (t *Txn) StartSeq() uint64 { return t.startSeq }

// State returns the transaction's current lifecycle state.
func (t *Txn) State() State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// terminalErr returns the specific error explaining why a terminal
// transaction rejects a further operation. Callers must hold t.mu and
// must only call this when t.state != StateActive.
func (t *Txn) terminalErr() error {
	switch t.state {
	case StateCommitted:
		return ErrAlreadyCommitted
	case StateAborted:
		return ErrAlreadyAborted
	default:
		return ErrInvalidState
	}
}

// Read implements the transaction's read rule (docs/mvcc.md §3): the
// local write set is consulted first (own writes always shadow
// committed data, including a local tombstone shadowing an older
// committed value), then the MVCC store's visibility rule is applied
// against StartSeq. found is false for a key that does not exist as of
// this transaction's snapshot, whether because it was never written,
// its only versions have CommitSeq > StartSeq, the visible version is a
// tombstone, or the transaction's own local write is a delete.
func (t *Txn) Read(key string) (value []byte, found bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return nil, false, t.terminalErr()
	}
	if m, ok := t.writes[key]; ok {
		if m.Tombstone {
			return nil, false, nil
		}
		return m.Value, true, nil
	}
	v, ok := t.mgr.fsm.Store().Visible(key, t.startSeq)
	return v, ok, nil
}

// LocalWrites returns a snapshot of the transaction's own uncommitted
// write set, in first-write order (docs/transactions.md §1's local
// write set) — used by a caller (e.g. the SQL frontend, docs/sql.md
// §5) that needs to merge this transaction's own pending writes with a
// committed-data scan for a multi-key read, mirroring the own-write-
// shadows-committed-data rule Read already applies for a single key
// (docs/mvcc.md §3 step 1). Read-only: does not affect transaction
// state. Returns nil once the transaction is terminal (its write set no
// longer exists — see Commit/Abort).
func (t *Txn) LocalWrites() []mvcc.Mutation {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return nil
	}
	out := make([]mvcc.Mutation, 0, len(t.order))
	for _, k := range t.order {
		out = append(out, t.writes[k])
	}
	return out
}

// Write records key=value in the transaction's local write set
// (docs/transactions.md §1). It is not durable, not replicated, and not
// visible to any other transaction until (and unless) this transaction
// commits (docs/transactions.md §2).
func (t *Txn) Write(key string, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return t.terminalErr()
	}
	t.recordWriteLocked(key, append([]byte(nil), value...), false)
	return nil
}

// Delete records a tombstone for key in the transaction's local write
// set (docs/transactions.md §1). Like Write, it is private to this
// transaction until commit.
func (t *Txn) Delete(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return t.terminalErr()
	}
	t.recordWriteLocked(key, nil, true)
	return nil
}

// recordWriteLocked must be called with t.mu held.
func (t *Txn) recordWriteLocked(key string, value []byte, tombstone bool) {
	if _, exists := t.writes[key]; !exists {
		t.order = append(t.order, key)
	}
	t.writes[key] = mvcc.Mutation{Key: key, Value: value, Tombstone: tombstone}
}

// Commit submits the transaction's mutation set as one deterministic
// CommitTxn command carrying the given client-supplied RequestID
// (docs/transactions.md §3, §6). The outcome is deterministic and
// final:
//
//   - COMMITTED: returns the assigned CommitSeq, nil error.
//   - ABORTED (write-write conflict): returns a zero CommitSeq and an
//     error wrapping ErrConflict (docs/mvcc.md §5: either the entire
//     mutation set is applied, or none of it is).
//   - Rejected RequestID reuse: requestID was already used to complete
//     a *different* command; returns a zero CommitSeq and an error
//     wrapping fsm.ErrRequestIDPayloadMismatch. The RequestID's
//     original recorded outcome is completely unaffected.
//   - Durability failure: returns a zero CommitSeq and the underlying
//     error; requestID remains unresolved (docs/transactions.md §9's
//     documented Phase 2 risk still applies unchanged in Phase 3).
//
// Retrying Commit with the identical requestID and an identical
// mutation set (whichever of the above outcomes it produced, including
// after a restart) returns that same outcome again, without
// re-evaluating or re-applying anything (docs/transactions.md §6).
//
// Any outcome makes the transaction terminal; a second call to Commit
// or Abort on the same Txn returns ErrAlreadyCommitted or
// ErrAlreadyAborted rather than re-evaluating anything — RequestID
// identity, not Txn/TxnID identity, is what makes a *resubmitted*
// commit attempt idempotent.
func (t *Txn) Commit(requestID fsm.RequestID) (commitSeq uint64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return 0, t.terminalErr()
	}

	mutations := make([]mvcc.Mutation, 0, len(t.order))
	for _, k := range t.order {
		mutations = append(mutations, t.writes[k])
	}

	outcome, cerr := t.mgr.commit(t.id, requestID, t.startSeq, mutations)
	if cerr != nil {
		t.state = StateAborted
		t.writes = nil
		t.order = nil
		return 0, cerr
	}
	t.state = StateCommitted
	t.writes = nil
	t.order = nil
	return outcome.CommitSeq, nil
}

// Abort discards the transaction's local write set (docs/transactions.md
// §1). Nothing durable or visible to any other transaction ever
// existed for an aborted transaction's writes (docs/transactions.md
// §2), so Abort has no interaction with the WAL, internal/fsm, or any
// RequestID at all — it is purely in-memory bookkeeping.
func (t *Txn) Abort() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return t.terminalErr()
	}
	t.state = StateAborted
	t.writes = nil
	t.order = nil
	return nil
}
