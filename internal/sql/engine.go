// Engine and Txn are the one seam between this package and
// ChronicleDB's real, already-proven transactional machinery
// (docs/sql.md §Execution Path, ADR-0013): every SQL statement's
// planned operation (plan.go) executes exclusively through a Txn, and
// every Txn is backed by either internal/txn.Manager (standalone mode)
// or internal/node.Node (replicated mode) — never by internal/mvcc or
// internal/storage directly. This file contains no SQL-specific logic
// at all; it is purely an adapter layer.
package sql

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/txn"
)

// defaultMaxConcurrentSQLStatements is docs/v0.6.0-plan.md §10.1's
// -max-concurrent-sql-statements default. internal/sql has no CLI flag
// surface of its own (docs/sql.md §8: it is a Go-library surface, not a
// deployed binary — cmd/chronicledb-node does not use this package at
// all), so this is simply the constructor default; an embedder that
// wants a different limit constructs its own Engine-wrapping gate at a
// future extension point rather than one this release adds.
const defaultMaxConcurrentSQLStatements = 256

// newSQLGate returns the per-Engine statement-admission gate
// (docs/v0.6.0-plan.md §9.2): bounds parser/planner/scan work, which is
// work internal/node's own gates never see. Panics only if
// defaultMaxConcurrentSQLStatements were ever misconfigured to <= 0,
// which it never is.
func newSQLGate() *admission.Gate {
	g, err := admission.NewGate("sql", admission.Limits{MaxConcurrent: defaultMaxConcurrentSQLStatements})
	if err != nil {
		panic(fmt.Sprintf("sql: constructing the statement admission gate: %v", err))
	}
	return g
}

// KV is one key/value pair returned by Txn.ScanPrefix.
type KV struct {
	Key   string
	Value []byte
}

// Txn abstracts one open ChronicleDB transaction session — the
// identical lifecycle docs/transactions.md §1 describes (BEGIN,
// READ/WRITE/DELETE against a local write set, COMMIT or ABORT) —
// whichever concrete deployment backs it. A SQL statement (plan.go,
// exec.go) is planned and executed purely in terms of this interface.
type Txn interface {
	// StartSeq is this transaction's fixed MVCC snapshot boundary
	// (docs/mvcc.md §3), captured once at Begin and never changing for
	// the transaction's lifetime.
	StartSeq() uint64
	// Read implements docs/mvcc.md §3's visibility rule for a single
	// key: this transaction's own local write set is consulted first
	// (own writes shadow committed data unconditionally), then the
	// committed version visible as of StartSeq.
	Read(key string) (value []byte, found bool, err error)
	// ScanPrefix returns every key with the given prefix currently
	// visible to this transaction — committed data as of StartSeq,
	// merged with this transaction's own not-yet-committed local
	// writes exactly as Read would resolve each key individually —
	// sorted ascending by key for deterministic results. Used only for
	// a SELECT with no primary-key predicate (docs/sql.md §5.2):
	// necessarily a full scan of every key sharing the prefix, not an
	// indexed lookup — see that section for the complexity this
	// implies.
	ScanPrefix(prefix string) ([]KV, error)
	// Write and Delete update the transaction's local write set
	// (docs/transactions.md §1-2): not durable, not replicated, not
	// visible to any other transaction, until (and unless) Commit
	// succeeds.
	Write(key string, value []byte) error
	Delete(key string) error
	// Commit submits every accumulated Write/Delete as one
	// deterministic CommitTxn command under requestID
	// (docs/transactions.md §3, §6): the identical RequestID-idempotent
	// commit path every other ChronicleDB client uses. A conflict
	// (docs/mvcc.md §4) is reported as an error wrapping ErrConflict,
	// not a panic or a silently-ignored partial write.
	Commit(requestID string) (commitSeq uint64, err error)
	// Abort discards the local write set with no durable trace
	// (docs/transactions.md §2).
	Abort() error
}

// RequestOutcome is a minimal, already-durably-decided answer to
// "what happened to this RequestID previously" (docs/transactions.md
// §6-7), independent of any specific mutation payload. It exists only
// for Session's auto-commit idempotency short-circuit (exec.go): a
// genuine retry of an already-completed statement must return its
// original outcome directly, without re-running the statement's
// semantic validation (e.g. INSERT's duplicate-primary-key check)
// against state that, by the time of the retry, already reflects the
// original attempt's own committed effect (see exec.go's
// mutationRetryResult for exactly why re-running that validation would
// otherwise incorrectly reject a legitimate retry).
type RequestOutcome struct {
	Committed bool
	CommitSeq uint64
}

// Engine begins new Txns against one specific ChronicleDB deployment.
// It is the only thing a Session (exec.go) needs to know about which
// deployment — standalone or replicated — it is talking to.
type Engine interface {
	Begin(ctx context.Context) (Txn, error)
	// LookupOutcome resolves requestID's durable terminal outcome, if
	// any, without submitting or re-evaluating any command
	// (docs/transactions.md §7's GetRequestOutcome). found is false if
	// requestID has never completed.
	LookupOutcome(requestID string) (outcome RequestOutcome, found bool)
	// sqlGate returns this Engine's shared statement-admission gate
	// (docs/v0.6.0-plan.md §9.2), acquired by Session.ExecuteStatement
	// once per statement. Unexported: only this package's own two
	// Engine implementations supply one, and internal/admission stays
	// out of Engine's public API — the gate is this package's
	// implementation detail, not something an embedder configures
	// through this interface.
	sqlGate() *admission.Gate
}

// --- Standalone adapter (internal/txn.Manager) ---

type standaloneEngine struct {
	mgr  *txn.Manager
	gate *admission.Gate
}

// NewStandaloneEngine adapts an already-open internal/txn.Manager
// (standalone, pre-Raft mode — docs/architecture.md §1's "before Raft
// exists" engine) into an Engine.
//
// In standalone mode, this Engine's statement gate is the ONLY
// admission protection that exists (docs/v0.6.0-plan.md §9.2):
// standalone mode has no internal/node.Node and therefore none of its
// Lane B/A gates. A library user embedding ChronicleDB in standalone
// mode must not assume node-level protection they do not have — see
// docs/admission-control.md.
func NewStandaloneEngine(mgr *txn.Manager) Engine {
	return &standaloneEngine{mgr: mgr, gate: newSQLGate()}
}

func (e *standaloneEngine) sqlGate() *admission.Gate { return e.gate }

// newStandaloneEngineWithGateLimitForTest is a test-only constructor
// exposing a smaller-than-default statement gate, so admission-
// saturation tests do not need 257 concurrent goroutines to exercise
// the default 256-statement ceiling.
func newStandaloneEngineWithGateLimitForTest(mgr *txn.Manager, limit int) Engine {
	g, err := admission.NewGate("sql", admission.Limits{MaxConcurrent: limit})
	if err != nil {
		panic(err)
	}
	return &standaloneEngine{mgr: mgr, gate: g}
}

func (e *standaloneEngine) Begin(ctx context.Context) (Txn, error) {
	return &standaloneTxn{t: e.mgr.Begin(), mgr: e.mgr}, nil
}

func (e *standaloneEngine) LookupOutcome(requestID string) (RequestOutcome, bool) {
	outcome, err := e.mgr.GetRequestOutcome(fsm.RequestID(requestID))
	if err != nil {
		return RequestOutcome{}, false
	}
	return RequestOutcome{Committed: outcome.Status == fsm.StatusCommitted, CommitSeq: outcome.CommitSeq}, true
}

type standaloneTxn struct {
	t   *txn.Txn
	mgr *txn.Manager
}

func (s *standaloneTxn) StartSeq() uint64 { return s.t.StartSeq() }

func (s *standaloneTxn) Read(key string) ([]byte, bool, error) { return s.t.Read(key) }

func (s *standaloneTxn) Write(key string, value []byte) error { return s.t.Write(key, value) }

func (s *standaloneTxn) Delete(key string) error { return s.t.Delete(key) }

func (s *standaloneTxn) Commit(requestID string) (uint64, error) {
	seq, err := s.t.Commit(fsm.RequestID(requestID))
	if err != nil {
		// Translate internal/txn's own ErrConflict to this package's
		// ErrConflict so a caller of the sql package sees one
		// consistent sentinel regardless of which Engine backs it
		// (docs/sql.md §6) — the underlying error is still available
		// via errors.Unwrap for anyone who needs the original detail.
		if errors.Is(err, txn.ErrConflict) {
			return 0, fmt.Errorf("%w: %v", ErrConflict, err)
		}
		return 0, err
	}
	return seq, nil
}

func (s *standaloneTxn) Abort() error { return s.t.Abort() }

func (s *standaloneTxn) ScanPrefix(prefix string) ([]KV, error) {
	committed, err := s.mgr.Store().ScanVisible(prefix, s.t.StartSeq())
	if err != nil {
		return nil, err
	}
	return mergeLocalWrites(prefix, committed, s.t.LocalWrites()), nil
}

// --- Replicated adapter (internal/node.Node) ---

type replicatedEngine struct {
	n    *node.Node
	gate *admission.Gate
}

// NewReplicatedEngine adapts an already-open internal/node.Node
// (replicated, real Raft/TCP/WAL mode — docs/roadmap.md Phase 5) into
// an Engine. n must currently be reachable as (or route through) the
// cluster leader for a Begin that will go on to Commit any mutation;
// Begin itself (via internal/node.Node.BeginReadIndex) fails with
// *node.NotLeaderError if n is not leader.
//
// In replicated mode this Engine's statement gate is IN ADDITION to
// n's own Lane B gates, and that is intentional (docs/v0.6.0-plan.md
// §9.2): this gate bounds parser/planner/scan work, which n never sees;
// n's own gates bound Raft work. Nested acquisition across the two
// cannot deadlock because the order is always sql -> node and never
// the reverse (ExecuteStatement acquires this gate first, then calls
// into n.BeginReadIndex/n.Propose, which acquire n's own gates).
func NewReplicatedEngine(n *node.Node) Engine { return &replicatedEngine{n: n, gate: newSQLGate()} }

func (e *replicatedEngine) sqlGate() *admission.Gate { return e.gate }

func (e *replicatedEngine) Begin(ctx context.Context) (Txn, error) {
	startSeq, err := e.n.BeginReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	return &replicatedTxn{ctx: ctx, node: e.n, startSeq: startSeq, writes: make(map[string]mvcc.Mutation)}, nil
}

func (e *replicatedEngine) LookupOutcome(requestID string) (RequestOutcome, bool) {
	outcome, ok := e.n.FSM().GetOutcome(fsm.RequestID(requestID))
	if !ok {
		return RequestOutcome{}, false
	}
	return RequestOutcome{Committed: outcome.Status == fsm.StatusCommitted, CommitSeq: outcome.CommitSeq}, true
}

// replicatedTxn implements Txn against internal/node.Node, which has
// no session/local-write-set concept of its own (its only mutation
// primitive is Propose, one whole CommitTxn command at a time — see
// internal/node.Node's own doc comment). replicatedTxn therefore
// maintains the identical local write-set bookkeeping
// internal/txn.Txn already does for standalone mode (docs/transactions.md
// §1: an in-memory map plus first-write insertion order, so the
// eventual encoded command's mutation order is fixed and reproducible
// rather than depending on Go's randomized map iteration —
// docs/invariants.md DETERMINISM BOUNDARY's spirit), accumulating it
// purely in memory until Commit submits it as one Propose call.
type replicatedTxn struct {
	ctx      context.Context
	node     *node.Node
	startSeq uint64

	writes map[string]mvcc.Mutation
	order  []string
}

func (r *replicatedTxn) StartSeq() uint64 { return r.startSeq }

func (r *replicatedTxn) Read(key string) ([]byte, bool, error) {
	if m, ok := r.writes[key]; ok {
		if m.Tombstone {
			return nil, false, nil
		}
		return m.Value, true, nil
	}
	return r.node.FSM().Store().Visible(key, r.startSeq)
}

func (r *replicatedTxn) recordWrite(key string, value []byte, tombstone bool) {
	if _, exists := r.writes[key]; !exists {
		r.order = append(r.order, key)
	}
	r.writes[key] = mvcc.Mutation{Key: key, Value: value, Tombstone: tombstone}
}

func (r *replicatedTxn) Write(key string, value []byte) error {
	r.recordWrite(key, append([]byte(nil), value...), false)
	return nil
}

func (r *replicatedTxn) Delete(key string) error {
	r.recordWrite(key, nil, true)
	return nil
}

func (r *replicatedTxn) ScanPrefix(prefix string) ([]KV, error) {
	local := make([]mvcc.Mutation, 0, len(r.order))
	for _, k := range r.order {
		local = append(local, r.writes[k])
	}
	committed, err := r.node.FSM().Store().ScanVisible(prefix, r.startSeq)
	if err != nil {
		return nil, err
	}
	return mergeLocalWrites(prefix, committed, local), nil
}

// txnIDFromRequestID deterministically derives a CommitTxnCommand's
// TxnID field from its RequestID (docs/architecture.md §3: TxnID is
// otherwise "ephemeral... lives only in the leader's in-memory session
// state," which internal/node.Node itself has no equivalent of for a
// SQL-originated commit). TxnID carries no correctness weight of its
// own — it is not consulted by the conflict decision (docs/mvcc.md §4)
// or by MVCC visibility, only recorded — except that a genuine retry of
// the same RequestID must present the identical TxnID or it is treated
// as a mismatched reuse (fsm.ErrRequestIDPayloadMismatch,
// docs/transactions.md §6). Deriving it from RequestID itself, rather
// than from any process-local counter or session state, guarantees a
// retry (even from a different SQL Session, even after this process
// restarts) reproduces the identical TxnID with no state to lose —
// exactly the property docs/transactions.md §8 requires of a
// resubmitted commit.
func txnIDFromRequestID(requestID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(requestID))
	return h.Sum64()
}

func (r *replicatedTxn) Commit(requestID string) (uint64, error) {
	mutations := make([]mvcc.Mutation, 0, len(r.order))
	for _, k := range r.order {
		mutations = append(mutations, r.writes[k])
	}
	if len(mutations) == 0 {
		// Mirrors internal/txn.Manager.commit's identical read-only
		// bypass (docs/transactions.md §9): nothing to durably record
		// or replicate for a transaction that never wrote anything.
		return r.startSeq, nil
	}
	outcome, err := r.node.Propose(r.ctx, fsm.CommitTxnCommand{
		RequestID: fsm.RequestID(requestID),
		TxnID:     txnIDFromRequestID(requestID),
		StartSeq:  r.startSeq,
		Mutations: mutations,
	})
	if err != nil {
		return 0, err
	}
	if outcome.Status == fsm.StatusAborted {
		return 0, fmt.Errorf("sql: commit conflict on key %q (latest committed=%d > StartSeq=%d): %w",
			outcome.ConflictKey, outcome.ConflictLatestSeq, r.startSeq, ErrConflict)
	}
	return outcome.CommitSeq, nil
}

func (r *replicatedTxn) Abort() error {
	r.writes = nil
	r.order = nil
	return nil
}

// mergeLocalWrites merges committed (already visibility-filtered and
// horizon-checked by mvcc.Store.ScanVisible — docs/v0.6.0-plan.md
// §15.2a) with a transaction's own local write set, deterministically
// (sorted ascending by key): own writes always shadow committed data
// (a local write present) or shadow it into absence (a local
// tombstone), regardless of what ScanVisible returned. This is the
// merge half of ScanPrefix that stays in internal/sql, where it
// belongs — the committed-data half (the visibility rule itself, and
// now the horizon check) lives entirely inside internal/mvcc, in the
// one place every committed read passes through. There is no local
// re-implementation of the visibility rule here anymore: the bypass
// that made that possible (mergeScan reading mvcc.Store.Export
// directly) is gone, and TestExportOnlyCalledFromSnapshotEncoding
// keeps it from reappearing.
func mergeLocalWrites(prefix string, committed []mvcc.KV, local []mvcc.Mutation) []KV {
	present := make(map[string][]byte, len(committed))
	for _, kv := range committed {
		present[kv.Key] = kv.Value
	}
	for _, m := range local {
		if !strings.HasPrefix(m.Key, prefix) {
			continue
		}
		if m.Tombstone {
			delete(present, m.Key)
		} else {
			present[m.Key] = m.Value
		}
	}
	keys := make([]string, 0, len(present))
	for k := range present {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]KV, len(keys))
	for i, k := range keys {
		out[i] = KV{Key: k, Value: present[k]}
	}
	return out
}
