// Package txn implements ChronicleDB's transaction session state
// (TxnID, StartSeq, local write set), Snapshot Isolation conflict
// detection, and the commit/recovery integration with internal/wal, as
// specified in docs/transactions.md and docs/mvcc.md.
//
// internal/fsm (the deterministic Apply boundary) does not exist yet —
// it is factored out in Phase 3, per docs/roadmap.md. Until then, this
// package plays that role directly for CommitTxn commands: it is the
// single place that turns a durably appended command into MVCC state,
// both for a live commit and for recovery replay (see Manager.commit
// and Manager.recover), so that role can move into internal/fsm later
// without changing the underlying semantics.
package txn

import (
	"fmt"
	"sync"

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
	// Commit fails (conflict or a durability error) — either way, the
	// transaction leaves no trace in committed state.
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

// Manager owns the durable log, the MVCC store, and the single
// serialization point through which every commit is decided
// (docs/mvcc.md §4: the conflict decision is made deterministically, in
// committed log/apply order — in standalone mode, append order and
// apply order are the same single-writer sequence, so Manager's mu is
// that sequence point). Manager is safe for concurrent use by multiple
// goroutines: Begin and each Txn's Commit/Abort may be called
// concurrently from different goroutines.
type Manager struct {
	walog *wal.WAL
	store *mvcc.Store

	// mu serializes: reading lastSeq for a new Begin, and the entire
	// check-conflict -> append -> sync -> apply sequence of a commit.
	// Holding it across the WAL Sync call (docs/storage.md §5) is what
	// makes standalone-mode commit ordering exactly equal to durable
	// append order, which is what the deterministic conflict/apply rule
	// requires (docs/mvcc.md §4.1). It does not serialize reads: Txn.Read
	// only ever touches the mvcc.Store's own, separate lock, so readers
	// are never blocked behind an in-flight commit's fsync.
	mu        sync.Mutex
	lastSeq   uint64
	nextTxnID uint64
}

// NewManager wires a Manager to an already-open WAL and MVCC store, and
// runs recovery (docs/recovery.md) before returning: every durably
// committed CommitTxn record in w's log is deterministically re-applied
// to store, in order, exactly as a live commit would apply it (see
// Manager.recover). A non-nil error means recovery detected a
// durable-history inconsistency it refuses to guess a resolution for
// (RECOVERY-NON-INVENTION) — the caller must not treat store as usable.
func NewManager(w *wal.WAL, store *mvcc.Store) (*Manager, error) {
	m := &Manager{walog: w, store: store}
	if err := m.recover(); err != nil {
		return nil, err
	}
	return m, nil
}

// recover replays every RecordTypeLogEntry record in the WAL from index
// 1 forward, decodes each as a CommitTxn command, and runs the same
// deterministic conflict-check-then-apply step commit uses for a live
// commit (docs/recovery.md §11: "running internal/fsm.Apply for each,
// in order, exactly as if being applied for the first time").
//
// Every record recover() finds was, by construction of the live commit
// path, only ever durably appended after passing its conflict check
// with no other commit able to interleave (Manager.mu held throughout).
// Replaying from an empty store, in the identical order, must therefore
// deterministically reproduce "no conflict" for every record. If it
// ever does not, the durable log is not a legitimate, deterministically
// reproducible history — a form of corruption distinct from what
// internal/wal itself checksums for — and recovery fails closed rather
// than inventing a resolution, per RECOVERY-NON-INVENTION
// (docs/invariants.md).
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
		cmd, err := decodeCommitTxn(rec.Payload)
		if err != nil {
			return fmt.Errorf("txn: recovery: decoding record at index %d: %w", rec.Index, err)
		}
		if key, latest, conflict := m.store.CheckConflicts(cmd.startSeq, cmd.mutations); conflict {
			return fmt.Errorf("txn: recovery: record at index %d unexpectedly conflicts on key %q (latest committed=%d, startSeq=%d): durable history is not deterministically reproducible: %w",
				rec.Index, key, latest, cmd.startSeq, ErrMalformedRecord)
		}
		if err := m.store.ApplyCommit(rec.Index, cmd.mutations); err != nil {
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

// commit runs the deterministic check-conflict -> append -> sync ->
// apply sequence for one transaction's mutation set (docs/mvcc.md §4-5,
// docs/transactions.md §3-5). A read-only transaction (no mutations)
// commits trivially: nothing is appended, since there is nothing whose
// durability or conflict status matters (docs/transactions.md §2 — only
// a submitted mutation set becomes a durable command).
func (m *Manager) commit(id TxnID, startSeq uint64, mutations []mvcc.Mutation) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(mutations) == 0 {
		return m.lastSeq, nil
	}

	if key, latest, conflict := m.store.CheckConflicts(startSeq, mutations); conflict {
		return 0, fmt.Errorf("txn: txn %d commit conflict on key %q (latest committed=%d > StartSeq=%d): %w",
			id, key, latest, startSeq, ErrConflict)
	}

	payload := encodeCommitTxn(commitTxnCommand{txnID: uint64(id), startSeq: startSeq, mutations: mutations})
	seq, err := m.walog.AppendLogEntry(payload)
	if err != nil {
		return 0, fmt.Errorf("txn: txn %d durable append failed: %w", id, err)
	}
	if err := m.walog.Sync(); err != nil {
		return 0, fmt.Errorf("txn: txn %d durability sync failed: %w", id, err)
	}
	if err := m.store.ApplyCommit(seq, mutations); err != nil {
		return 0, fmt.Errorf("txn: txn %d internal apply failure: %w", id, err)
	}
	m.lastSeq = seq
	return seq, nil
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
	v, ok := t.mgr.store.Visible(key, t.startSeq)
	return v, ok, nil
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
// CommitTxn command (docs/transactions.md §3). The outcome is
// deterministic and final: COMMITTED (returns the assigned CommitSeq,
// nil error) or ABORTED (returns a zero CommitSeq and an error wrapping
// ErrConflict for a write-write conflict, or wrapping the underlying
// cause for a durability failure — docs/mvcc.md §5: either the entire
// mutation set is applied, or none of it is). Either outcome makes the
// transaction terminal; a second call returns ErrAlreadyCommitted or
// ErrAlreadyAborted rather than re-evaluating anything.
func (t *Txn) Commit() (commitSeq uint64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateActive {
		return 0, t.terminalErr()
	}

	mutations := make([]mvcc.Mutation, 0, len(t.order))
	for _, k := range t.order {
		mutations = append(mutations, t.writes[k])
	}

	seq, cerr := t.mgr.commit(t.id, t.startSeq, mutations)
	if cerr != nil {
		t.state = StateAborted
		t.writes = nil
		t.order = nil
		return 0, cerr
	}
	t.state = StateCommitted
	t.writes = nil
	t.order = nil
	return seq, nil
}

// Abort discards the transaction's local write set (docs/transactions.md
// §1). Nothing durable or visible to any other transaction ever
// existed for an aborted transaction's writes (docs/transactions.md
// §2), so Abort has no interaction with the WAL or the MVCC store at
// all — it is purely in-memory bookkeeping.
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
