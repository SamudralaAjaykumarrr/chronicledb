// Package oracle implements Phase 10's independent reference model and
// deterministic history-recording infrastructure
// (docs/adversarial-testing.md, docs/roadmap.md Phase 10). It is a
// test-only support package (like internal/fault): no production code
// imports it.
//
// The model here is deliberately much simpler than ChronicleDB itself
// and is written independently of internal/mvcc/internal/fsm's own
// code and types (docs/roadmap.md Phase 10: "the oracle must be
// structurally independent enough to detect implementation mistakes...
// do not copy ChronicleDB implementation details into the oracle").
// KVModel implements the *documented* first-committer-wins rule
// (docs/mvcc.md §4) from scratch, in a different data structure and a
// different code path than internal/mvcc.Store; OutcomeTracker makes no
// attempt to predict conflict outcomes at all — it only records what
// ChronicleDB itself returned the first time under a given RequestID
// and flags any later disagreement, which requires no reimplementation
// of ChronicleDB's own logic and is therefore the safest possible
// oracle for REQUEST-OUTCOME-STABILITY specifically.
//
// Digests produced here are testing/diagnostic evidence only, never
// correctness state: nothing in ChronicleDB's own code ever reads a
// value from this package.
package oracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// KVMutation is a minimal, independent representation of one key
// mutation. It is deliberately not internal/mvcc.Mutation — using a
// distinct type keeps this package's logic honest about depending only
// on the documented contract, never on internal/mvcc's internals.
type KVMutation struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// KVModel is the simplest possible independent model of ChronicleDB's
// committed key/value state: the latest committed value (or explicit
// absence, for a tombstone or a never-written key) of every key, plus
// the latest commit sequence number observed for each key — enough to
// independently predict first-committer-wins Snapshot Isolation
// conflict outcomes for a sequential (one-commit-attempt-at-a-time)
// history, per docs/mvcc.md §4.
type KVModel struct {
	value   map[string][]byte
	tomb    map[string]bool
	lastSeq map[string]uint64
}

// NewKVModel returns an empty model (no keys ever written).
func NewKVModel() *KVModel {
	return &KVModel{
		value:   make(map[string][]byte),
		tomb:    make(map[string]bool),
		lastSeq: make(map[string]uint64),
	}
}

// Predict reports whether a transaction that began at startSeq and
// wants to write muts would commit under the documented
// first-committer-wins rule: it conflicts if any written key's
// lastSeq strictly exceeds startSeq (docs/mvcc.md §4 — the same
// evaluation order real conflict checks use, first conflicting key
// wins the report, but computed here independently). Predict does not
// mutate the model — call Apply separately once the real outcome is
// known, so a caller can always trust the model's state to reflect
// only outcomes ChronicleDB itself actually confirmed, never a
// speculative prediction.
func (m *KVModel) Predict(startSeq uint64, muts []KVMutation) (wouldCommit bool, conflictKey string, conflictSeq uint64) {
	keys := make([]string, 0, len(muts))
	for _, mu := range muts {
		keys = append(keys, mu.Key)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if seq, ok := m.lastSeq[k]; ok && seq > startSeq {
			return false, k, seq
		}
	}
	return true, "", 0
}

// Apply records a real, already-decided commit at assignedSeq,
// applying every mutation atomically (all keys advance to assignedSeq
// together, matching ATOMICITY) and advancing each written key's
// lastSeq. Callers call this only after ChronicleDB has actually
// confirmed the commit — Apply never itself decides commit/abort.
func (m *KVModel) Apply(assignedSeq uint64, muts []KVMutation) {
	for _, mu := range muts {
		if mu.Tombstone {
			m.tomb[mu.Key] = true
			delete(m.value, mu.Key)
		} else {
			m.tomb[mu.Key] = false
			m.value[mu.Key] = append([]byte(nil), mu.Value...)
		}
		m.lastSeq[mu.Key] = assignedSeq
	}
}

// Get returns key's current modeled value. found is false both for a
// never-written key and for one whose latest mutation was a tombstone
// — the model does not distinguish the two, matching MVCC visibility's
// own external behavior (docs/mvcc.md §3: a tombstoned key reads as
// not-found).
func (m *KVModel) Get(key string) (value []byte, found bool) {
	if m.tomb[key] {
		return nil, false
	}
	v, ok := m.value[key]
	return v, ok
}

// Keys returns every key the model has ever seen a mutation for
// (including currently-tombstoned ones), sorted ascending.
func (m *KVModel) Keys() []string {
	seen := make(map[string]struct{}, len(m.lastSeq))
	for k := range m.lastSeq {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Digest returns a canonical, deterministic hex-encoded SHA-256 digest
// of the model's entire committed key/value state — every currently
// live (non-tombstoned) key/value pair, sorted by key, so the result
// never depends on Go's unordered map iteration. Comparing two
// digests is a cheap way to assert "these two views of committed state
// agree in full," without printing potentially large state on every
// check (only on a mismatch does a caller need to dump the detail).
func (m *KVModel) Digest() string {
	return CanonicalKVDigest(m.Keys(), m.Get)
}

// CanonicalKVDigest computes the identical canonical SHA-256 digest
// format KVModel.Digest uses, but against an arbitrary key/value
// source (get) — so a model's own Digest() and a real ChronicleDB
// cluster's live committed state (queried e.g. via
// node.FSM().Store().Visible) can be compared using byte-for-byte
// identical canonicalization, sorted ascending by key so the result
// never depends on iteration order.
func CanonicalKVDigest(keys []string, get func(key string) ([]byte, bool)) string {
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	h := sha256.New()
	for _, k := range sorted {
		v, ok := get(k)
		if !ok {
			continue
		}
		fmt.Fprintf(h, "%d:%s\x00%d:%s\x00", len(k), k, len(v), v)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RecordedOutcome is one terminal, already-durably-decided outcome for
// a RequestID, as ChronicleDB itself reported it.
type RecordedOutcome struct {
	Committed   bool
	CommitSeq   uint64
	ConflictKey string
}

func (o RecordedOutcome) String() string {
	if o.Committed {
		return fmt.Sprintf("COMMITTED(seq=%d)", o.CommitSeq)
	}
	return fmt.Sprintf("ABORTED(conflictKey=%q)", o.ConflictKey)
}

// OutcomeTracker independently verifies REQUEST-OUTCOME-STABILITY
// (docs/invariants.md): a completed RequestID must resolve to the
// identical terminal outcome on every later retry, failover, restart,
// or replay, regardless of which node answers. It makes no attempt to
// predict what that outcome should be — only that whatever it was
// stays the same — which needs no reimplementation of ChronicleDB's
// own conflict-resolution logic at all.
type OutcomeTracker struct {
	// fingerprint identifies "the same logical request" (RequestID
	// plus a stable hash of its payload) — a different fingerprint
	// under an identical RequestID string is ID-6's mismatched-payload-
	// reuse scenario, tracked separately by callers, not by this type.
	seen map[string]seenEntry
}

type seenEntry struct {
	fingerprint string
	outcome     RecordedOutcome
	firstSeenAt string // free-form provenance (e.g. "step 4, node n1"), for diagnostics only
}

// NewOutcomeTracker returns an empty tracker.
func NewOutcomeTracker() *OutcomeTracker {
	return &OutcomeTracker{seen: make(map[string]seenEntry)}
}

// Observe records requestID's outcome the first time it is seen under
// fingerprint, and on every subsequent observation verifies it is
// byte-for-byte identical. provenance is a short free-form string
// (e.g. "seed 7 step 12 node n2") used only in the returned error to
// help locate the exact history step that first recorded the
// conflicting value.
func (o *OutcomeTracker) Observe(requestID, fingerprint string, got RecordedOutcome, provenance string) error {
	prev, ok := o.seen[requestID]
	if !ok {
		o.seen[requestID] = seenEntry{fingerprint: fingerprint, outcome: got, firstSeenAt: provenance}
		return nil
	}
	if prev.fingerprint != fingerprint {
		// A different payload under the same RequestID is not this
		// tracker's concern (ID-6) — record nothing further, report
		// nothing: callers that care about payload-mismatch rejection
		// check that directly against ChronicleDB's own error.
		return nil
	}
	if prev.outcome != got {
		return fmt.Errorf("REQUEST-OUTCOME-STABILITY violated for RequestID %q: first observed %s (at %s), now observed %s (at %s)",
			requestID, prev.outcome, prev.firstSeenAt, got, provenance)
	}
	return nil
}

// Digest returns a canonical hex-encoded SHA-256 digest of every
// tracked RequestID's fingerprint and recorded outcome, sorted by
// RequestID.
func (o *OutcomeTracker) Digest() string {
	ids := make([]string, 0, len(o.seen))
	for id := range o.seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	h := sha256.New()
	for _, id := range ids {
		e := o.seen[id]
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00", id, e.fingerprint, e.outcome)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Fingerprint builds a stable, order-independent-safe fingerprint
// string for a mutation set plus its transaction metadata — enough to
// detect "this RequestID was resubmitted with different content"
// without needing ChronicleDB's own fsm.CommitTxnCommand encoding.
func Fingerprint(txnID, startSeq uint64, muts []KVMutation) string {
	// Mutations are already in a caller-fixed, deterministic order
	// (matching how a real request's own Mutations slice is ordered) —
	// fingerprinting must NOT sort them, since the fingerprint's job is
	// to detect that the exact same logical request was resubmitted,
	// and a real payload mismatch could hide behind a sort.
	var b strings.Builder
	fmt.Fprintf(&b, "%d:%d:%d:", txnID, startSeq, len(muts))
	for _, mu := range muts {
		fmt.Fprintf(&b, "%s=%v(%t);", mu.Key, mu.Value, mu.Tombstone)
	}
	return b.String()
}
