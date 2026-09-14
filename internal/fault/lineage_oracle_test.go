package fault

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file implements DM-10's independent configuration-lineage oracle
// (dynamic-membership plan §15 DM-10, §17 CONFIGURATION BRANCH
// CONFINEMENT, §2.3's W1/W2) and is exercised by DM-10's combined
// randomized schedule (membership_dm10_test.go) and by DM-22's
// calibration (membership_dm22_test.go).
//
// STRUCTURAL INDEPENDENCE (§15's explicit requirement, re-stated here so
// it stays visible next to the code it governs): this oracle never
// calls raft.Core.ConfigAt, raft.Core.ActiveConfig, raft.DecodeConfigEntry,
// or any of internal/raft/membership.go's unexported decode/classify
// helpers (decodeConfigChange, classifyTransition, ConfigAt itself —
// all unexported and, in any case, deliberately not used even where an
// exported equivalent exists). Every byte-level decision below —
// the wire marker/kind bytes, the length-prefixed field layout, the
// four-shape classification — is re-derived directly from
// docs/dynamic-membership-plan.md §2.5/§2.6's documented wire format,
// as fresh Go code, so a bug in internal/raft's own encoder/decoder or
// its own shape classifier cannot silently escape detection by being
// mirrored into the very oracle checking it (the zero-voter EntryConfig
// bug fixed in commit 7251bed is exactly this bug class, and this
// oracle re-derives that same MINIMUM VOTER INVARIANT check
// independently below, not by reading membership.go's fix).
//
// The only things this oracle reads from Core are primitive,
// non-derived accessors that predate dynamic membership and carry no
// membership decision logic of their own: Entries() (raw Index/Term/
// Type/Data, precisely the durable log bytes), CommitIndex(),
// SnapshotIndex(), and LastIndex() (existing Raft-core bookkeeping,
// used identically by the pre-existing committedOracle). The one
// exception is the final comparison step, which reads
// Core.ActiveConfig() deliberately — not to compute the oracle's own
// expectation, but as "the actual system state" the independently
// computed expectation is checked against, exactly mirroring
// committedOracle.observe's own use of Node.CommittedEntries().

// --- Independent byte-level decode of the EntryConfig wire payload
// (dynamic-membership plan §2.5) ---

const (
	oracleMarkerByte         = 0xF0
	oracleKindAddLearner     = 16
	oracleKindPromoteToVoter = 17
	oracleKindRemoveServer   = 18
	oracleKindVoided         = 19
)

// oracleDecoded is this oracle's own view of one EntryConfig payload,
// independently parsed.
type oracleDecoded struct {
	kind        byte
	cfg         raft.Configuration
	establishes bool // false only for a Voided entry (§7.6)
}

func oracleGetU32(b []byte, off int) (uint32, int, error) {
	if len(b)-off < 4 {
		return 0, off, fmt.Errorf("lineage oracle: truncated u32 at offset %d (len %d)", off, len(b))
	}
	return binary.BigEndian.Uint32(b[off:]), off + 4, nil
}

func oracleGetLenPrefixed(b []byte, off int) ([]byte, int, error) {
	n, off, err := oracleGetU32(b, off)
	if err != nil {
		return nil, off, err
	}
	if int(n) > len(b)-off {
		return nil, off, fmt.Errorf("lineage oracle: truncated field at offset %d (declared %d bytes, %d remain)", off, n, len(b)-off)
	}
	return b[off : off+int(n)], off + int(n), nil
}

// oracleDecodeConfiguration independently parses the fullConfig section
// of an EntryConfig payload: a voter-count-prefixed list of
// (id, address) pairs, then a learner-count-prefixed list of the same
// shape (§2.5).
func oracleDecodeConfiguration(b []byte) (raft.Configuration, error) {
	voterCount, off, err := oracleGetU32(b, 0)
	if err != nil {
		return raft.Configuration{}, err
	}
	var voters []raft.Member
	for i := uint32(0); i < voterCount; i++ {
		id, o, err := oracleGetLenPrefixed(b, off)
		if err != nil {
			return raft.Configuration{}, err
		}
		off = o
		addr, o2, err := oracleGetLenPrefixed(b, off)
		if err != nil {
			return raft.Configuration{}, err
		}
		off = o2
		voters = append(voters, raft.Member{ID: raft.NodeID(id), Address: string(addr)})
	}
	learnerCount, off2, err := oracleGetU32(b, off)
	if err != nil {
		return raft.Configuration{}, err
	}
	off = off2
	var learners []raft.Member
	for i := uint32(0); i < learnerCount; i++ {
		id, o, err := oracleGetLenPrefixed(b, off)
		if err != nil {
			return raft.Configuration{}, err
		}
		off = o
		addr, o2, err := oracleGetLenPrefixed(b, off)
		if err != nil {
			return raft.Configuration{}, err
		}
		off = o2
		learners = append(learners, raft.Member{ID: raft.NodeID(id), Address: string(addr)})
	}
	if off != len(b) {
		return raft.Configuration{}, fmt.Errorf("lineage oracle: %d trailing bytes in fullConfig", len(b)-off)
	}
	return raft.Configuration{Voters: voters, Learners: learners}, nil
}

// oracleDecodeEntryConfig independently parses one EntryConfig entry's
// Data payload per §2.5's documented layout:
//
//	byte[0] = 0xF0 marker
//	byte[1] = membership-change kind (16/17/18/19)
//	requestIDLen(4B) requestID  targetIDLen(4B) targetID
//	targetAddrLen(4B) targetAddr  fullConfigLen(4B) fullConfig
func oracleDecodeEntryConfig(data []byte) (oracleDecoded, error) {
	if len(data) < 2 {
		return oracleDecoded{}, fmt.Errorf("lineage oracle: payload too short (%d bytes)", len(data))
	}
	if data[0] != oracleMarkerByte {
		return oracleDecoded{}, fmt.Errorf("lineage oracle: payload does not carry the membership marker byte")
	}
	kind := data[1]
	off := 2
	if _, o, err := oracleGetLenPrefixed(data, off); err != nil { // requestID: unused by this oracle
		return oracleDecoded{}, err
	} else {
		off = o
	}
	if _, o, err := oracleGetLenPrefixed(data, off); err != nil { // targetID: unused by this oracle
		return oracleDecoded{}, err
	} else {
		off = o
	}
	if _, o, err := oracleGetLenPrefixed(data, off); err != nil { // targetAddr: unused by this oracle
		return oracleDecoded{}, err
	} else {
		off = o
	}
	cfgBytes, off2, err := oracleGetLenPrefixed(data, off)
	if err != nil {
		return oracleDecoded{}, err
	}
	off = off2
	if off != len(data) {
		return oracleDecoded{}, fmt.Errorf("lineage oracle: %d trailing bytes", len(data)-off)
	}
	switch kind {
	case oracleKindAddLearner, oracleKindPromoteToVoter, oracleKindRemoveServer:
		cfg, err := oracleDecodeConfiguration(cfgBytes)
		if err != nil {
			return oracleDecoded{}, err
		}
		// MINIMUM VOTER INVARIANT (§17), re-derived independently: no
		// legitimate establishing configuration ever has zero voters.
		// This mirrors, without reading, the fix in commit 7251bed.
		if len(cfg.Voters) == 0 {
			return oracleDecoded{}, fmt.Errorf("lineage oracle: establishing configuration has zero voters")
		}
		return oracleDecoded{kind: kind, cfg: cfg, establishes: true}, nil
	case oracleKindVoided:
		if len(cfgBytes) != 0 {
			return oracleDecoded{}, fmt.Errorf("lineage oracle: voided entry carries a non-empty fullConfig")
		}
		return oracleDecoded{kind: kind, establishes: false}, nil
	default:
		return oracleDecoded{}, fmt.Errorf("lineage oracle: unknown membership-change kind %d", kind)
	}
}

// --- Independent four-shape classifier (dynamic-membership plan §2.6),
// re-derived from the spec text rather than from
// internal/raft/membership.go's classifyTransition ---

func oracleMemberSet(members []raft.Member) map[raft.NodeID]raft.Member {
	s := make(map[raft.NodeID]raft.Member, len(members))
	for _, m := range members {
		s[m.ID] = m
	}
	return s
}

func oracleSetEqual(a, b map[raft.NodeID]raft.Member) bool {
	if len(a) != len(b) {
		return false
	}
	for id, m := range a {
		if bm, ok := b[id]; !ok || bm != m {
			return false
		}
	}
	return true
}

// oracleSetDiffOne returns the single member present in newSet but not
// oldSet, when newSet has exactly one more element than oldSet; ok is
// false otherwise.
func oracleSetDiffOne(newSet, oldSet map[raft.NodeID]raft.Member) (raft.Member, bool) {
	if len(newSet) != len(oldSet)+1 {
		return raft.Member{}, false
	}
	for id, m := range newSet {
		if _, ok := oldSet[id]; !ok {
			return m, true
		}
	}
	return raft.Member{}, false
}

// oracleIsSingleShapeChild reports whether new is exactly one of §2.6's
// four legal single-server transitions relative to old.
func oracleIsSingleShapeChild(old, new raft.Configuration) bool {
	oldV, newV := oracleMemberSet(old.Voters), oracleMemberSet(new.Voters)
	oldL, newL := oracleMemberSet(old.Learners), oracleMemberSet(new.Learners)
	for id := range newV {
		if _, ok := newL[id]; ok {
			return false // a member cannot be both voter and learner
		}
	}
	switch {
	case len(newV) == len(oldV) && len(newL) == len(oldL)+1:
		// AddLearner: voters unchanged; exactly one new learner, not
		// already present anywhere in old.
		if !oracleSetEqual(oldV, newV) {
			return false
		}
		added, ok := oracleSetDiffOne(newL, oldL)
		if !ok {
			return false
		}
		_, alreadyVoter := oldV[added.ID]
		return !alreadyVoter
	case len(newV) == len(oldV)+1 && len(newL)+1 == len(oldL):
		// PromoteToVoter: exactly one member moves learner -> voter,
		// identical Member value, everything else unchanged.
		addedV, ok := oracleSetDiffOne(newV, oldV)
		if !ok {
			return false
		}
		removedL, ok2 := oracleSetDiffOne(oldL, newL)
		if !ok2 {
			return false
		}
		if addedV != removedL {
			return false
		}
		for id, m := range oldV {
			if nm, ok := newV[id]; !ok || nm != m {
				return false
			}
		}
		return true
	case len(newV)+1 == len(oldV) && len(newL) == len(oldL):
		// RemoveServer (voter): exactly one voter removed, learners
		// unchanged, at least one voter remains.
		if len(newV) < 1 {
			return false
		}
		if !oracleSetEqual(oldL, newL) {
			return false
		}
		_, ok := oracleSetDiffOne(oldV, newV)
		return ok
	case len(newV) == len(oldV) && len(newL)+1 == len(oldL):
		// RemoveServer (learner): exactly one learner removed, voters
		// unchanged.
		if !oracleSetEqual(oldV, newV) {
			return false
		}
		_, ok := oracleSetDiffOne(oldL, newL)
		return ok
	default:
		return false
	}
}

// --- The oracle itself ---

// oracleLogEntry is this oracle's own accumulated copy of one raw log
// entry for one node: just the durable bytes (Term/Type/Data), nothing
// derived.
type oracleLogEntry struct {
	Term raft.Term
	Type raft.EntryType
	Data []byte
}

// oracleCommittedConfig is the cross-node record of what this oracle has
// independently decoded as committed at a given log index, and which
// node it first observed it from (for diagnostics only).
type oracleCommittedConfig struct {
	Term raft.Term
	Cfg  raft.Configuration
	Node raft.NodeID
}

// configLineageOracle is DM-10's independent reference model. See this
// file's top-of-file doc comment for the independence discipline it
// follows.
type configLineageOracle struct {
	bootstrap map[raft.NodeID]raft.Configuration
	// foundingBootstrap is the single Configuration every founding peer
	// shares (the same value as every entry of bootstrap) — kept
	// separately because it is also the correct anchor fallback for a
	// pool-added node that caught up entirely via InstallSnapshot
	// before any EntryConfig entry ever existed anywhere: the sending
	// node's own ConfigAt(LastIncludedIndex) bottoms out at ITS OWN
	// bootstrap (§6.3 step 3, §7.2's unconditional adoption of
	// msg.Configuration), and every live sender in this harness traces
	// back to the same NewCluster call, so that bottomed-out value is
	// always exactly foundingBootstrap — never the receiving pool-added
	// node's own (empty) bootstrap. See anchor's doc comment for why
	// this oracle cannot instead reconstruct this purely from observed
	// committed EntryConfig entries.
	foundingBootstrap raft.Configuration
	nodeLog           map[raft.NodeID]map[raft.Index]oracleLogEntry
	committed         map[raft.Index]oracleCommittedConfig
}

// newConfigLineageOracle seeds the oracle's bootstrap fact from peers —
// exactly the external, test-supplied argument to NewCluster, in the
// same order, mirroring Cluster.NewCluster's own bootstrap construction
// (cluster.go) as an independent fact the test itself controls, never
// as a value read back from any Core. A node added later via
// Cluster.AddNode is deliberately left out of this map: cluster.go seeds
// such a node with an empty Config.Bootstrap, which is exactly this
// map's zero-value default for an absent key.
func newConfigLineageOracle(peers []raft.NodeID) *configLineageOracle {
	o := &configLineageOracle{
		bootstrap: make(map[raft.NodeID]raft.Configuration),
		nodeLog:   make(map[raft.NodeID]map[raft.Index]oracleLogEntry),
		committed: make(map[raft.Index]oracleCommittedConfig),
	}
	voters := make([]raft.Member, len(peers))
	for i, id := range peers {
		voters[i] = raft.Member{ID: id, Address: string(id) + ":0"}
	}
	boot := raft.Configuration{Voters: voters}
	o.foundingBootstrap = boot
	for _, id := range peers {
		o.bootstrap[id] = boot
	}
	return o
}

// observe is the checkpoint entry point, mirroring committedOracle's own
// observe signature and oracleFataler substitution pattern (so a
// negative-control test can use recordingFataler exactly as
// TestDM12Step7 already does for committedOracle). It (1) accumulates
// every live node's current raw log entries into this oracle's own
// per-node history, (2) cross-checks every newly-committed EntryConfig
// entry against every other node's report at the same index (W1), and
// (3) independently reconstructs each live node's own configuration
// lineage and checks both its internal shape validity (W2) and its
// agreement with Core.ActiveConfig() (the actual system state).
func (o *configLineageOracle) observe(t oracleFataler, seed int64, cl *Cluster) {
	t.Helper()
	for _, id := range cl.NodeIDs() {
		n := cl.Node(id)
		if n.Crashed() {
			continue
		}
		core := n.Core()
		entries := core.Entries()
		commitIndex := core.CommitIndex()
		snapshotIndex := core.SnapshotIndex()
		lastIndex := core.LastIndex()

		log, ok := o.nodeLog[id]
		if !ok {
			log = make(map[raft.Index]oracleLogEntry)
			o.nodeLog[id] = log
		}
		for _, e := range entries {
			log[e.Index] = oracleLogEntry{Term: e.Term, Type: e.Type, Data: append([]byte(nil), e.Data...)}
			if e.Type != raft.EntryConfig || e.Index > commitIndex {
				continue
			}
			decoded, err := oracleDecodeEntryConfig(e.Data)
			if err != nil {
				t.Fatalf("seed %d: lineage oracle: node %s's committed EntryConfig at index %d failed independent decode: %v", seed, id, e.Index, err)
				continue
			}
			if !decoded.establishes {
				continue // Voided: recorded above, establishes nothing (§7.6)
			}
			if prev, exists := o.committed[e.Index]; exists {
				if prev.Term != e.Term || !prev.Cfg.Equal(decoded.cfg) {
					t.Fatalf("seed %d: CONFIGURATION BRANCH CONFINEMENT (W1) violated at index %d: node %s previously committed %+v (term %d), node %s now reports %+v (term %d)",
						seed, e.Index, prev.Node, prev.Cfg, prev.Term, id, decoded.cfg, e.Term)
				}
				continue
			}
			o.committed[e.Index] = oracleCommittedConfig{Term: e.Term, Cfg: decoded.cfg, Node: id}
		}

		oracleActive, chainErr := o.reconstruct(id, snapshotIndex, lastIndex)
		if chainErr != nil {
			t.Fatalf("seed %d: CONFIGURATION BRANCH CONFINEMENT (W2) violated on node %s: %v", seed, id, chainErr)
			continue
		}
		actual := core.ActiveConfig()
		if !oracleActive.Equal(actual) {
			t.Fatalf("seed %d: configuration-lineage divergence on node %s: independent oracle reconstructs %+v, Core.ActiveConfig() reports %+v", seed, id, oracleActive, actual)
		}
	}
}

// reconstruct independently replays node id's own accumulated log
// entries, strictly above floor (its current snapshot boundary) and up
// to and including upto (its current last index), starting from
// anchor's own independently-derived config (§6.3 steps 2-3's
// analogue). Every step must be a single-shape child of the previous
// one (W2) — checked here directly, never by calling
// internal/raft/membership.go's classifyTransition.
func (o *configLineageOracle) reconstruct(id raft.NodeID, floor, upto raft.Index) (raft.Configuration, error) {
	log := o.nodeLog[id]
	var idxs []raft.Index
	for idx := range log {
		if idx > floor && idx <= upto {
			idxs = append(idxs, idx)
		}
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })

	cur := o.anchor(id, floor)
	for _, idx := range idxs {
		e := log[idx]
		if e.Type != raft.EntryConfig {
			continue
		}
		decoded, err := oracleDecodeEntryConfig(e.Data)
		if err != nil {
			return raft.Configuration{}, fmt.Errorf("node %s entry %d: independent decode failed: %v", id, idx, err)
		}
		if !decoded.establishes {
			continue // Voided: accepted unconditionally, establishes nothing
		}
		// A node whose own prior configuration is still the zero value
		// has no locally-derivable "before" state to validate its
		// first-ever establishing entry against (a brand-new learner's
		// first bulk catch-up batch, dynamic-membership plan §2.6's
		// activateFromAppendedEntries doc comment) — every SUBSEQUENT
		// entry is validated normally.
		if !cur.IsZero() && !oracleIsSingleShapeChild(cur, decoded.cfg) {
			return raft.Configuration{}, fmt.Errorf("node %s entry %d: %+v is not a single-shape child of %+v", id, idx, decoded.cfg, cur)
		}
		cur = decoded.cfg
	}
	return cur, nil
}

// anchor independently derives the configuration effective at-or-below
// floor for node id — the analogue of §6.3's ConfigAt steps 2/3, but
// computed from this oracle's own separately-maintained history rather
// than from Core's private snapshotConfig/snapshotHasConfig fields
// (which this package cannot read directly in any case). It combines
// two independent sources, by INDEX, never by which one happens to
// answer first: (1) this node's own previously-recorded log, which
// this oracle never prunes even after Core itself compacts past floor;
// (2) the cross-node committed chain, built only from this oracle's own
// independently-decoded observations of every node, never from
// Core.ConfigAt/ActiveConfig. Comparing by index (not "prefer local if
// present") matters for a node that caught up via a later
// InstallSnapshot: its own per-node history can genuinely be STALE —
// missing a later establishing entry it never held as a raw log entry
// at all, because InstallSnapshot delivers a configuration as snapshot
// metadata, not as an EntryConfig entry Entries() would ever report —
// so the newest-by-index source must win, not whichever source this
// function checks first.
//
// Falls back to a bootstrap fact only when NEITHER source has anything
// at or below floor — and a pool-added node needs different handling
// there than a founding one. A founding node's own bootstrap
// (o.bootstrap[id]) is always correct in this branch: floor==0 with
// nothing else known is exactly the fresh-cluster seed. But a
// POOL-ADDED node reaching this branch with floor > 0 has, by
// construction, definitely received something (its own SnapshotIndex
// is nonzero) that neither source accounts for — and the only
// remaining legitimate source is an InstallSnapshot whose sender's own
// ConfigAt(LastIncludedIndex) bottomed out, on the sender's side, at
// the shared founding bootstrap (no EntryConfig entry existed anywhere
// yet at that boundary) — the unconditional adoption of
// msg.Configuration, one level removed from this oracle's own direct
// observation (it reads raw log entries and CommitIndex, never
// InstallSnapshot message contents). Every live sender in this harness
// traces back to the same NewCluster call, so that bottomed-out value
// is always exactly foundingBootstrap, never the pool-added receiver's
// own (empty) per-node bootstrap. A genuinely never-joined pool-added
// node instead has floor == 0 here, so o.bootstrap[id]'s zero-value
// default (empty) is still what it correctly falls back to.
func (o *configLineageOracle) anchor(id raft.NodeID, floor raft.Index) raft.Configuration {
	localCfg, localIdx, localFound := o.newestEstablishing(id, floor)
	globalCfg, globalIdx, globalFound := o.newestCommittedAtOrBelow(floor)
	switch {
	case localFound && globalFound:
		if localIdx >= globalIdx {
			return localCfg
		}
		return globalCfg
	case localFound:
		return localCfg
	case globalFound:
		return globalCfg
	default:
		if _, isFounding := o.bootstrap[id]; !isFounding && floor > 0 {
			return o.foundingBootstrap
		}
		return o.bootstrap[id]
	}
}

func (o *configLineageOracle) newestEstablishing(id raft.NodeID, floor raft.Index) (raft.Configuration, raft.Index, bool) {
	log := o.nodeLog[id]
	var best raft.Index
	var bestCfg raft.Configuration
	found := false
	for idx, e := range log {
		if idx > floor || e.Type != raft.EntryConfig {
			continue
		}
		decoded, err := oracleDecodeEntryConfig(e.Data)
		if err != nil || !decoded.establishes {
			continue
		}
		if !found || idx > best {
			found = true
			best = idx
			bestCfg = decoded.cfg
		}
	}
	return bestCfg, best, found
}

func (o *configLineageOracle) newestCommittedAtOrBelow(floor raft.Index) (raft.Configuration, raft.Index, bool) {
	var best raft.Index
	var bestCfg raft.Configuration
	found := false
	for idx, rec := range o.committed {
		if idx > floor {
			continue
		}
		if !found || idx > best {
			found = true
			best = idx
			bestCfg = rec.Cfg
		}
	}
	return bestCfg, best, found
}
