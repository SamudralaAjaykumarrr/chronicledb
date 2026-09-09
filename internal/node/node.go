package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/identity"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/transport"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// Config configures Open. It is supplied fresh at every process start,
// including a restart of the same node — persistent state itself lives
// entirely under DataDir (docs/raft.md §5).
type Config struct {
	// ID is this node's identity. Must be a member of Peers.
	ID raft.NodeID
	// Peers lists every voting member of the cluster, including ID
	// itself (docs/architecture.md §1: static membership in V1).
	Peers []raft.NodeID
	// PeerAddrs gives every OTHER member's dial address ("host:port")
	// for internal/transport. An entry for ID itself is ignored.
	PeerAddrs map[raft.NodeID]string
	// ListenAddr is the address this node's transport listens on for
	// inbound Raft RPCs from peers.
	ListenAddr string

	// DataDir is this node's durable log directory (internal/wal). This
	// node's snapshot directory (internal/snapshot.Manager) lives at
	// DataDir/snapshot, alongside the WAL's own segment files
	// (docs/storage.md §4).
	DataDir string
	// WALOptions configures the underlying WAL (segment size, etc.).
	// Zero value uses internal/wal's own defaults.
	WALOptions wal.Options

	// SnapshotThreshold is how many newly applied log entries since the
	// last snapshot boundary trigger creation of a fresh one
	// (docs/snapshots.md §3: "log growth since the last snapshot...
	// exceeding a configured threshold"). Zero means "use the package
	// default"; a caller that genuinely wants no snapshotting at all
	// (e.g. some tests exercising ordinary replication in isolation) is
	// not a supported configuration in V1 — every long-running node
	// should compact eventually, so there is no separate "off" value.
	SnapshotThreshold uint64

	// ElectionTimeoutTicks/ElectionTimeoutJitterTicks/HeartbeatTimeoutTicks
	// are in units of TickInterval (docs/raft.md §2). Zero means "use
	// the package default."
	ElectionTimeoutTicks       int
	ElectionTimeoutJitterTicks int
	HeartbeatTimeoutTicks      int
	// TickInterval is the real wall-clock duration of one logical Raft
	// tick. Zero means "use the package default." Tests that want fast
	// elections set this (and the tick counts) small; production
	// deployments use the default.
	TickInterval time.Duration

	// Logger receives operational diagnostics (leadership changes,
	// fatal local errors). Nil discards them. Never a correctness
	// dependency (docs/roadmap.md §Observability: "a correct decision
	// must never depend on whether a metric/log was recorded").
	Logger *log.Logger

	// PeerTLSCertFile/PeerTLSKeyFile/PeerTLSCAFile configure peer mTLS
	// (docs/enterprise-v1-plan.md §5 layer 2). All three empty (the
	// default) means plaintext peer replication, identical to
	// pre-Security-Foundation behavior. If any is set, all three are
	// required (validate below) — there is no partial/optional-mTLS
	// configuration for peer traffic: it is fully on or fully off, never
	// silently degraded (NO PLAINTEXT PEER REPLICATION).
	PeerTLSCertFile string
	PeerTLSKeyFile  string
	PeerTLSCAFile   string
}

// PeerTLSEnabled reports whether Config requests peer mTLS.
func (c Config) PeerTLSEnabled() bool {
	return c.PeerTLSCertFile != "" || c.PeerTLSKeyFile != "" || c.PeerTLSCAFile != ""
}

const (
	defaultElectionTimeoutTicks       = 10
	defaultElectionTimeoutJitterTicks = 10
	defaultHeartbeatTimeoutTicks      = 2
	defaultTickInterval               = 20 * time.Millisecond
	defaultSnapshotThreshold          = 4096
)

func (c *Config) setDefaults() {
	if c.ElectionTimeoutTicks <= 0 {
		c.ElectionTimeoutTicks = defaultElectionTimeoutTicks
	}
	if c.ElectionTimeoutJitterTicks <= 0 {
		c.ElectionTimeoutJitterTicks = defaultElectionTimeoutJitterTicks
	}
	if c.HeartbeatTimeoutTicks <= 0 {
		c.HeartbeatTimeoutTicks = defaultHeartbeatTimeoutTicks
	}
	if c.TickInterval <= 0 {
		c.TickInterval = defaultTickInterval
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = defaultSnapshotThreshold
	}
}

func (c Config) validate() error {
	if c.ID == "" {
		return fmt.Errorf("node: Config.ID must not be empty")
	}
	if c.DataDir == "" {
		return fmt.Errorf("node: Config.DataDir must not be empty")
	}
	found := false
	for _, p := range c.Peers {
		if p == c.ID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("node: Config.Peers must include Config.ID (%q)", c.ID)
	}
	if c.PeerTLSEnabled() {
		if c.PeerTLSCertFile == "" || c.PeerTLSKeyFile == "" || c.PeerTLSCAFile == "" {
			return fmt.Errorf("node: peer mTLS requires PeerTLSCertFile, PeerTLSKeyFile, and PeerTLSCAFile all set (got cert=%q key=%q ca=%q) — partial peer-TLS configuration is not supported (NO PLAINTEXT PEER REPLICATION)",
				c.PeerTLSCertFile, c.PeerTLSKeyFile, c.PeerTLSCAFile)
		}
	}
	return nil
}

// Status is a point-in-time, safe-to-read-from-any-goroutine snapshot
// of a Node's diagnostic state (docs/roadmap.md §Observability). It is
// never used by Node itself for any correctness decision.
type Status struct {
	ID            raft.NodeID
	Role          raft.Role
	Term          raft.Term
	Leader        raft.NodeID
	CommitIndex   raft.Index
	AppliedIndex  uint64
	LastIndex     raft.Index
	SnapshotIndex raft.Index
	// ClusterGeneration is this node's currently applied FSM cluster
	// generation (docs/enterprise-v1-plan.md §7) — 0 until a finalize
	// has actually been applied (live, or replayed on restart).
	ClusterGeneration uint32
	// MaxSupportedGeneration is this node's own binary capability
	// (internal/version.MaxSupportedGeneration) — the generation
	// UpgradePrecheck/FinalizeUpgrade would target next.
	MaxSupportedGeneration uint32
}

type waiter struct {
	requestID fsm.RequestID
	resultCh  chan proposeResult
}

type proposeResult struct {
	outcome fsm.Outcome
	err     error
}

type proposeReq struct {
	cmd      fsm.CommitTxnCommand
	payload  []byte
	resultCh chan proposeResult
}

type readResult struct {
	startSeq uint64
	err      error
}

type readIndexReq struct {
	resultCh chan readResult
}

type backupResult struct {
	manifest backup.Manifest
	err      error
}

type backupReq struct {
	outDir     string
	continuous bool
	clusterID  string
	resultCh   chan backupResult
}

// controlProposeReq is a pre-encoded, non-CommitTxn proposal (currently
// only fsm.SetClusterVersionCommand — see ProposeControl) handed to
// run's event-loop goroutine, mirroring proposeReq but without
// CommitTxn's Precheck fast-path (docs/enterprise-v1-plan.md §7):
// control commands are rare admin actions, not the hot client path
// that optimization exists for.
type controlProposeReq struct {
	requestID fsm.RequestID
	payload   []byte
	resultCh  chan proposeResult
}

// PeerGenerationInfo is one peer's last-known compatibility generation
// as observed via live Raft traffic (docs/enterprise-v1-plan.md §7's
// wire-protocol version handshake — see raft.Message.SenderGeneration's
// doc comment for why it rides on ordinary messages rather than a
// separate preamble).
type PeerGenerationInfo struct {
	// Generation is the peer's last-reported internal/version.MaxSupportedGeneration.
	// Meaningless when Known is false.
	Generation uint32
	// Known is false until at least one message from this peer has ever
	// been received — an operator running precheck against a cluster
	// that has never exchanged a single Raft message with some
	// configured peer (e.g. that peer's process was never started) must
	// see "unknown," never a misleading assumed 0.
	Known bool
}

// PrecheckResult is UpgradePrecheck's answer (docs/enterprise-v1-plan.md
// §7 "precheck confirms every live node reports support for the target
// version").
type PrecheckResult struct {
	// LocalClusterGeneration is this node's own currently-applied FSM
	// cluster generation (mirrors Status.ClusterGeneration).
	LocalClusterGeneration uint32
	// LocalMaxSupportedGeneration is this node's own binary capability.
	LocalMaxSupportedGeneration uint32
	// TargetGeneration is the generation FinalizeUpgrade would attempt
	// to raise the cluster to: LocalClusterGeneration+1, capped by
	// LocalMaxSupportedGeneration (fsm.ApplySetClusterVersion's own
	// single-step N/N+1-only rule — see its doc comment). Today's single
	// possible generation bump (0 -> 1, since MaxSupportedGeneration==1)
	// makes this identical to the flat constant LocalMaxSupportedGeneration
	// in every reachable case, but a future release that raises
	// MaxSupportedGeneration further must still step one generation at a
	// time from whatever a cluster currently holds, never jump straight
	// to that release's own max — computing it as current+1 here, rather
	// than the flat constant, is what keeps this true without requiring
	// every future caller to remember it.
	TargetGeneration uint32
	// Peers reports every OTHER configured cluster member's last-known
	// generation (this node's own entry is never included — see
	// LocalClusterGeneration/LocalMaxSupportedGeneration above).
	Peers map[string]PeerGenerationInfo
	// AlreadyFinalized is true when this node's own binary has nothing
	// further to offer the cluster (LocalClusterGeneration already
	// equals LocalMaxSupportedGeneration) — finalize would have nothing
	// to do.
	AlreadyFinalized bool
	// Ready is true iff every entry in Peers is Known and its Generation
	// is >= TargetGeneration, and AlreadyFinalized is false — i.e. iff
	// FinalizeUpgrade would actually be attempted and expected to
	// succeed right now.
	Ready bool
	// IsLeader/Leader are computed live, on the same event-loop
	// dispatch as everything else in this result (never from a
	// separately-read, possibly-stale Status() snapshot — see
	// FinalizeUpgrade's own doc comment for why that distinction is
	// load-bearing): whether this node was the Raft leader at the exact
	// moment this PrecheckResult was computed, and its best current
	// knowledge of who is if not.
	IsLeader bool
	Leader   raft.NodeID
}

type precheckReq struct {
	resultCh chan PrecheckResult
}

type pendingRead struct {
	term raft.Term
	// target is the log index a majority of peers must freshly
	// reconfirm (via requiredSeq below) before this read is safe, and
	// the index appliedIndex must reach before it is returned as
	// StartSeq.
	target raft.Index
	// requiredSeq is the value of Node.sentSeqCounter at the moment
	// this read was issued (see checkPendingReads): the read is safe
	// only once a majority's Node.ackSeq for that peer exceeds this
	// value, proving each contributed a Success AppendEntriesResponse
	// that itself echoes a request Seq assigned no earlier than this
	// read's issuance — a live round-trip in the current term, not
	// merely a possibly-stale cached replication fact (see
	// Node.ackSeq's doc comment for why comparing against matchIndex
	// directly, or against a purely local processing-order counter, is
	// not sufficient).
	requiredSeq uint64
	resultCh    chan readResult
}

// Node is ChronicleDB's process-level runtime (docs/architecture.md §5
// "internal/node"; docs/roadmap.md Phase 5): it owns a raft.Core, a
// concrete internal/wal-backed Storage, a concrete internal/transport,
// a deterministic internal/fsm, and the client-facing proposal path
// tying committed Raft entries to fsm.Apply. Every field below except
// the channels used to talk to it is owned exclusively by the single
// goroutine run() starts — this is the "controlled event-loop
// ownership" this phase's brief asks for, and is what makes it safe to
// avoid additional locking around raft.Core / WALStorage / fsm.FSM
// access from within the loop.
type Node struct {
	cfg Config

	core    *raft.Core
	walog   *wal.WAL
	storage *WALStorage
	snapMgr *snapshot.Manager
	// fsmachine is normally only ever touched by run's single goroutine
	// (this type's own doc comment), with one deliberate exception: a
	// snapshot install (handleInstallSnapshot) atomically replaces it
	// wholesale, and FSM() is documented as safe to call from any
	// goroutine for read-only access. An atomic.Pointer, not a plain
	// *fsm.FSM field, is what actually makes that safe — a plain field
	// swapped on the event-loop goroutine while FSM() reads it
	// concurrently from elsewhere is a genuine data race (found by
	// Phase 7 chaos testing under -race: a follower crashing and
	// restarting repeatedly during snapshot catch-up, polled
	// concurrently via FSM(), reliably raced the pointer swap in
	// handleInstallSnapshot against a concurrent FSM() read).
	fsmachine atomic.Pointer[fsm.FSM]
	tr        *transport.Transport
	// identityHolder is non-nil only when peer mTLS is configured
	// (Config.PeerTLSEnabled). ReloadPeerTLS reloads it in place
	// (docs/enterprise-v1-plan.md §5 layer 7, certificate rotation).
	identityHolder *identity.Holder
	logger         *log.Logger

	electionArmed      bool
	electionTicksLeft  int
	heartbeatArmed     bool
	heartbeatTicksLeft int
	// electionTicksPaused, when set via PauseTicksForTest, freezes only
	// the election side of this node's clock: electionTicksLeft stops
	// counting down, so this node can never time out and start an
	// election on its own. It is a test-only determinism seam, the
	// tick-clock analogue of internal/transport.Transport.Block/Unblock
	// (an existing seam of exactly this kind, baked into production code
	// but only ever exercised by tests): a real-disk/real-TCP
	// testCluster test that deliberately does NOT want to exercise
	// election/failover behavior can freeze the clock that would
	// otherwise drive one, instead of papering over a spurious
	// host-scheduling-induced re-election with a bigger timeout or a
	// retry. It never affects propose/replication correctness, which is
	// entirely message-driven (handlePropose,
	// handleAppendEntriesRequest/Response) and never waits on a tick.
	// Heartbeat ticks are deliberately NOT paused: they are what keeps
	// driving a leader's replication/catch-up retries to a peer once no
	// further Propose calls occur (e.g. a follower reconnecting after a
	// restart, as in SN-3's interrupted-snapshot-catchup tests) — pausing
	// them too would starve exactly that liveness path instead of merely
	// removing spurious elections. An atomic.Bool because it is written
	// from a test goroutine but read from run()'s event-loop goroutine.
	electionTicksPaused atomic.Bool

	appliedIndex uint64
	waiters      map[raft.Index]waiter
	pendingReads []pendingRead

	// clusterGeneration mirrors fsm.FSM.ClusterGeneration() but is
	// written directly by run's own goroutine (via
	// adoptClusterGeneration) instead of read through fsmachine's mutex
	// on every refreshStatusLocked call — this value changes at most
	// once or twice in a node's entire lifetime, so caching it here
	// avoids taking FSM.mu (contended by every Propose call's Precheck)
	// on the single hottest loop in the system for a value that is
	// otherwise constant. Always kept in sync with the FSM's own value
	// by construction: every code path that can change the FSM's
	// clusterGeneration (a committed control command, or adopting a
	// peer's snapshot via InstallSnapshot) goes through
	// adoptClusterGeneration, never updates fsmachine's generation any
	// other way.
	clusterGeneration uint32

	// sentSeqCounter/ackSeq implement ADR-0010's ReadIndex freshness
	// proof (docs/replication.md §4.1). sentSeqCounter is bumped once
	// per outbound MsgAppendEntriesRequest this node ever sends (see
	// processOutput), and that value is stamped onto the request's
	// wire-carried Message.Seq; the follower's Core-independent
	// response echoes the same Seq back (see step), and ackSeq[peer] is
	// then set to the highest such echoed Seq this node, as Leader, has
	// processed from peer in a legitimate (current-term) Success
	// AppendEntriesResponse.
	//
	// checkPendingReads requires ackSeq[peer] to exceed the
	// sentSeqCounter value captured when the read was issued
	// (pendingRead.requiredSeq), proving a live round-trip answered a
	// request sent no earlier than the read's issuance. This must be a
	// wire-carried, request-specific correlation token, not simply "was
	// some qualifying ack processed after the read was issued": Node's
	// single event-loop goroutine can be arbitrarily delayed relative
	// to real time (GC pause, scheduling contention, a burst of other
	// work already queued), so an ack that was already in flight (sent
	// and even received) before this node captured target/requiredSeq
	// could otherwise still be *processed* after — and, judged only by
	// processing order, be mistaken for live proof of post-issuance
	// connectivity it does not actually provide. Comparing against
	// raft.Core's own matchIndex directly would fail for a different,
	// simpler reason: matchIndex only ever increases and is never reset
	// by a partition, so a leader that already replicated up to its
	// current LastIndex before becoming isolated would pass a
	// matchIndex-based check vacuously, without any fresh contact at
	// all, defeating the safety property this check exists to prove.
	sentSeqCounter uint64
	ackSeq         map[raft.NodeID]uint64

	// peerGenerations holds each peer's last-known
	// internal/version.MaxSupportedGeneration, updated only from run's
	// event-loop goroutine (see the message-receive case in run) —
	// exactly the same single-goroutine-ownership discipline as every
	// other field in this block, and why UpgradePrecheck must dispatch
	// through precheckCh rather than reading this map directly from an
	// arbitrary caller goroutine (docs/enterprise-v1-plan.md §7).
	peerGenerations map[raft.NodeID]uint32

	proposeCh   chan proposeReq
	controlCh   chan controlProposeReq
	readIndexCh chan readIndexReq
	backupCh    chan backupReq
	precheckCh  chan precheckReq
	stopCh      chan struct{}
	doneCh      chan struct{}
	stopOnce    sync.Once

	statusMu sync.Mutex
	status   Status

	// metrics holds this node's diagnostic counters (docs/roadmap.md
	// Phase 9, see metrics.go). Every field is itself concurrency-safe
	// (sync/atomic-backed), so metrics is read via Metrics() from any
	// goroutine with no additional locking, while every increment site
	// below runs only on run's own event-loop goroutine.
	metrics Metrics

	fatalMu sync.Mutex
	fatal   error
}

// Open opens (or creates) the node's durable log under cfg.DataDir,
// restores any snapshot and reconstructs Raft persistent state and the
// deterministic state machine from it (docs/recovery.md §1, extended by
// this phase for steps 2-4/10/13-14: snapshot restore and log
// compaction), starts the node's transport and event loop, and returns
// a running Node. The node starts as a Follower (or, for a genuinely
// fresh single-node-so-far cluster, immediately eligible to become
// Candidate/Leader on its first election timeout) — it never assumes it
// is leader and never applies anything beyond what a legitimate commit
// boundary re-establishes (docs/raft.md §5.1, ADR-0008).
func Open(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	w, _, err := wal.Open(cfg.DataDir, cfg.WALOptions)
	if err != nil {
		return nil, fmt.Errorf("node: opening durable log: %w", err)
	}

	snapMgr, err := snapshot.NewManager(filepath.Join(cfg.DataDir, "snapshot"))
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("node: opening snapshot directory: %w", err)
	}

	// Recovery steps 1-4 (docs/recovery.md §1): locate and validate the
	// newest snapshot the durable metadata pointer references, falling
	// back to an older one if it fails validation, or to "no snapshot"
	// (baseIndex 0) if none validate — internal/snapshot.Manager.Load
	// already implements exactly this search.
	meta := w.Metadata()
	snap, haveSnapshot, err := snapMgr.Load(meta.LatestSnapshotIndex)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("node: loading snapshot: %w", err)
	}

	var (
		baseIndex uint64
		baseTerm  uint64
		fsmachine *fsm.FSM
	)
	if haveSnapshot {
		baseIndex = snap.Meta.LastIncludedIndex
		baseTerm = snap.Meta.LastIncludedTerm
		fsmachine = snap.FSM
	} else {
		fsmachine = fsm.New(mvcc.NewStore())
	}

	// docs/recovery.md §4: "the log does not cover full history from
	// index 1" (no snapshot) — or, generalized, does not cover full
	// history from baseIndex+1 (a validated snapshot exists, but the
	// durable log's own oldest surviving entry starts strictly after
	// where that snapshot leaves off, e.g. the pointer named a snapshot
	// that turned out corrupt and Load fell back to an older one whose
	// boundary the log was already compacted past). Either way there is
	// no safe local starting point; refuse startup rather than silently
	// skip the missing range (RECOVERY-NON-INVENTION).
	if w.FirstIndex() > baseIndex+1 {
		w.Close()
		return nil, fmt.Errorf("node: durable log begins at index %d but recoverable state only covers up to %d — gap requires operator intervention (docs/recovery.md §4): %w", w.FirstIndex(), baseIndex, ErrRecoveryGap)
	}

	st, err := OpenWALStorage(w)
	if err != nil {
		w.Close()
		return nil, err
	}

	hs, err := st.InitialState()
	if err != nil {
		w.Close()
		return nil, err
	}
	last, err := st.LastIndex()
	if err != nil {
		w.Close()
		return nil, err
	}
	// Entries strictly after baseIndex only — st's own mirror may still
	// hold a few not-yet-physically-compacted leftover entries at or
	// before baseIndex (harmless; see WALStorage.Compact/InstallSnapshot),
	// which raft.NewCoreFromSnapshot must never see (it requires its
	// entries argument to start exactly at baseIndex+1).
	entries, err := st.Entries(raft.Index(baseIndex)+1, last+1)
	if err != nil {
		w.Close()
		return nil, err
	}

	rcfg := raft.Config{
		ID:                         cfg.ID,
		Peers:                      append([]raft.NodeID(nil), cfg.Peers...),
		ElectionTimeoutTicks:       cfg.ElectionTimeoutTicks,
		ElectionTimeoutJitterTicks: cfg.ElectionTimeoutJitterTicks,
		HeartbeatTimeoutTicks:      cfg.HeartbeatTimeoutTicks,
		Rand:                       newSysRand(),
	}
	core, err := raft.NewCoreFromSnapshot(rcfg, hs, raft.Index(baseIndex), raft.Term(baseTerm), entries)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("node: constructing raft core: %w", err)
	}

	var (
		tr             *transport.Transport
		identityHolder *identity.Holder
	)
	if cfg.PeerTLSEnabled() {
		identityHolder, err = identity.NewHolder(cfg.PeerTLSCertFile, cfg.PeerTLSKeyFile, cfg.PeerTLSCAFile)
		if err != nil {
			w.Close()
			return nil, fmt.Errorf("node: loading peer TLS identity: %w", err)
		}
		// docs/enterprise-v1-plan.md §5 layer 1: "internal/node binds
		// NodeID to the certificate at startup and refuses to start on a
		// mismatch."
		if err := identity.BindNodeIdentity(string(cfg.ID), identityHolder.Current().Leaf); err != nil {
			w.Close()
			return nil, fmt.Errorf("node: %w", err)
		}
		tr, err = transport.NewTLS(cfg.ID, cfg.ListenAddr, cfg.PeerAddrs, identityHolder)
	} else {
		tr, err = transport.New(cfg.ID, cfg.ListenAddr, cfg.PeerAddrs)
	}
	if err != nil {
		w.Close()
		return nil, err
	}

	n := &Node{
		cfg:               cfg,
		core:              core,
		walog:             w,
		storage:           st,
		snapMgr:           snapMgr,
		tr:                tr,
		identityHolder:    identityHolder,
		logger:            cfg.Logger,
		appliedIndex:      baseIndex,
		clusterGeneration: fsmachine.ClusterGeneration(),
		waiters:           make(map[raft.Index]waiter),
		ackSeq:            make(map[raft.NodeID]uint64, len(cfg.Peers)),
		peerGenerations:   make(map[raft.NodeID]uint32, len(cfg.Peers)),
		proposeCh:         make(chan proposeReq),
		controlCh:         make(chan controlProposeReq),
		readIndexCh:       make(chan readIndexReq),
		backupCh:          make(chan backupReq),
		precheckCh:        make(chan precheckReq),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}
	n.fsmachine.Store(fsmachine)
	n.electionArmed = true
	n.electionTicksLeft = core.NewElectionTimeout()
	n.refreshStatusLocked()

	go n.run()
	return n, nil
}

func (n *Node) logf(format string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Printf(format, args...)
	}
}

// Transport returns the node's transport, for direct fault-injection
// control in tests (Transport.Block/Unblock) beyond what Node's own API
// exposes — mirroring internal/fault.Cluster's own accessor pattern.
func (n *Node) Transport() *transport.Transport { return n.tr }

// PeerTLSMaterial returns the node's currently loaded peer-TLS
// certificate material and true, or the zero value and false if peer
// mTLS is not configured. Read-only diagnostic use only (e.g. the
// certificate-expiry-remaining gauge, docs/enterprise-v1-plan.md §5
// Observability) — never a correctness dependency.
func (n *Node) PeerTLSMaterial() (identity.Material, bool) {
	if n.identityHolder == nil {
		return identity.Material{}, false
	}
	return n.identityHolder.Current(), true
}

// ErrPeerTLSNotConfigured is returned by ReloadPeerTLS when this node
// was not opened with peer mTLS configured — there is no certificate
// material to reload.
var ErrPeerTLSNotConfigured = errors.New("node: peer TLS is not configured on this node")

// ReloadPeerTLS re-reads this node's peer TLS certificate/key/CA
// material from the same file paths Config was opened with and
// atomically swaps it in for every future handshake
// (docs/enterprise-v1-plan.md §5 layer 7: certificate rotation via
// SIGHUP or /admin/reload-tls, with "an overlap window during which
// both old and new peer certificates validate" satisfied by
// internal/identity.Holder's hot-swap semantics — see its doc comment).
// A live connection using the previously loaded certificate is never
// forcibly dropped by this call.
func (n *Node) ReloadPeerTLS() error {
	if n.identityHolder == nil {
		return ErrPeerTLSNotConfigured
	}
	return n.identityHolder.Reload()
}

// FSM returns the node's deterministic state machine, for read-only
// access (docs/mvcc.md §3 visibility reads bypass Apply) by a caller
// that has otherwise established it is safe to read (e.g. after a
// successful BeginReadIndex). Mutations must only ever happen via
// Propose.
func (n *Node) FSM() *fsm.FSM { return n.fsmachine.Load() }

// PauseTicksForTest freezes this node's election clock: it can no
// longer time out and start an election on its own, until
// ResumeTicksForTest is called. Heartbeat ticks are unaffected — see
// Node.electionTicksPaused's doc comment for why. Test-only — never
// called from production code, and never a dependency of any Raft
// safety or liveness invariant (propose/replication is entirely
// message-driven, not tick-driven, and proceeds normally while paused).
// Safe to call from any goroutine.
func (n *Node) PauseTicksForTest() { n.electionTicksPaused.Store(true) }

// ResumeTicksForTest reverses PauseTicksForTest. Safe to call from any
// goroutine.
func (n *Node) ResumeTicksForTest() { n.electionTicksPaused.Store(false) }

// Status returns a snapshot of the node's current diagnostic state.
// Safe to call from any goroutine.
func (n *Node) Status() Status {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	return n.status
}

func (n *Node) refreshStatusLocked() {
	n.statusMu.Lock()
	n.status = Status{
		ID:                     n.cfg.ID,
		Role:                   n.core.Role(),
		Term:                   n.core.CurrentTerm(),
		Leader:                 n.core.LeaderID(),
		CommitIndex:            n.core.CommitIndex(),
		AppliedIndex:           n.appliedIndex,
		LastIndex:              n.core.LastIndex(),
		SnapshotIndex:          n.core.SnapshotIndex(),
		ClusterGeneration:      n.clusterGeneration,
		MaxSupportedGeneration: version.MaxSupportedGeneration,
	}
	n.statusMu.Unlock()
}

// Stop shuts the node down: its event loop exits, its transport and
// durable log close, and every pending Propose/BeginReadIndex caller
// unblocks with ErrNodeStopped. Stop is idempotent and safe to call
// more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	<-n.doneCh
}

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

// recordPeerGeneration updates this node's last-known view of peer's
// compatibility generation (docs/enterprise-v1-plan.md §7). Call only
// from run's goroutine (see peerGenerations' doc comment). A zero
// value legitimately means "this peer has never set SenderGeneration"
// — every pre-v0.4.0 binary — so it is recorded exactly like any other
// value, not treated specially: UpgradePrecheck's own >= comparison
// against a nonzero target generation is what actually makes an
// unset/0 peer report as "not ready," not a special case here.
func (n *Node) recordPeerGeneration(peer raft.NodeID, gen uint32) {
	if peer == "" || peer == n.cfg.ID {
		return
	}
	n.peerGenerations[peer] = gen
}

// send stamps this node's own compatibility generation
// (raft.Message.SenderGeneration — docs/enterprise-v1-plan.md §7's
// wire-protocol version handshake) and transmits msg via the
// transport. This is the single choke point every outbound
// raft.Message goes through instead of calling n.tr.Send directly, so
// a future call site cannot silently forget the stamp — exactly the
// kind of gap code review already caught once by hand at
// handleInstallSnapshot's own failure-reply site before this helper
// existed.
func (n *Node) send(msg raft.Message) {
	msg.SenderGeneration = version.MaxSupportedGeneration
	n.tr.Send(msg)
}

// computePrecheck builds a PrecheckResult from this node's current
// view, including a live (not cached) leadership read. Call only from
// run's goroutine.
func (n *Node) computePrecheck() PrecheckResult {
	local := n.clusterGeneration
	alreadyFinalized := local >= version.MaxSupportedGeneration
	// Single-step target: current+1, never a flat jump to this binary's
	// own max — see TargetGeneration's doc comment. When
	// alreadyFinalized, local+1 could nominally exceed
	// MaxSupportedGeneration (this binary has nothing further to offer);
	// the exact value reported in that case is not meaningful since
	// Ready is unconditionally false below, but is still computed
	// without overflow risk (uint32 wraparound is not a concern at these
	// magnitudes).
	target := local + 1

	peers := make(map[string]PeerGenerationInfo, len(n.cfg.Peers))
	ready := !alreadyFinalized
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		gen, known := n.peerGenerations[p]
		peers[string(p)] = PeerGenerationInfo{Generation: gen, Known: known}
		if !known || gen < target {
			ready = false
		}
	}
	return PrecheckResult{
		LocalClusterGeneration:      local,
		LocalMaxSupportedGeneration: version.MaxSupportedGeneration,
		TargetGeneration:            target,
		Peers:                       peers,
		AlreadyFinalized:            alreadyFinalized,
		Ready:                       ready,
		IsLeader:                    n.core.Role() == raft.Leader,
		Leader:                      n.core.LeaderID(),
	}
}

// UpgradePrecheck reports this node's current view of every configured
// peer's compatibility generation, and whether FinalizeUpgrade would
// currently be expected to succeed (docs/enterprise-v1-plan.md §7
// "precheck confirms every live node reports support for the target
// version"). It is a read-only, dry-run check: it never proposes
// anything and never mutates cluster state — safe to call as often as
// an operator likes (e.g. the -upgrade-precheck CLI flag, or repeatedly
// while rolling nodes one at a time per docs/upgrades.md).
//
// Called against a follower, this view can be incomplete: a follower
// only ever exchanges Raft messages directly with the leader (plus
// whichever peers it happened to vote for/against during a past
// election), never a full mesh of pairwise traffic with every other
// follower, so an entry can legitimately show Known=false for a peer
// that IS actually reachable and ready — this is why FinalizeUpgrade
// only ever consults its own precheck view after first confirming it is
// leader (see that method). Prefer calling this against the current
// leader (docs/upgrades.md's runbook does) for the authoritative
// picture.
func (n *Node) UpgradePrecheck(ctx context.Context) (PrecheckResult, error) {
	res, err := n.upgradePrecheck(ctx)
	if err != nil {
		return PrecheckResult{}, err
	}
	n.metrics.UpgradePrecheckTotal.Inc()
	return res, nil
}

// upgradePrecheck is the unexported channel round-trip UpgradePrecheck
// and FinalizeUpgrade both dispatch through — split out so
// FinalizeUpgrade's own internal re-check (see its doc comment) does
// not inflate UpgradePrecheckTotal, a metric meant to reflect explicit
// operator/CLI precheck polling (docs/enterprise-v1-plan.md §7
// Observability), not finalize's own bookkeeping — a dedicated
// UpgradeFinalizeTotal/UpgradeFinalizeFailedTotal pair already exists
// for that.
func (n *Node) upgradePrecheck(ctx context.Context) (PrecheckResult, error) {
	req := precheckReq{resultCh: make(chan PrecheckResult, 1)}
	select {
	case n.precheckCh <- req:
	case <-ctx.Done():
		return PrecheckResult{}, ctx.Err()
	case <-n.doneCh:
		return PrecheckResult{}, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res, nil
	case <-ctx.Done():
		return PrecheckResult{}, ctx.Err()
	case <-n.doneCh:
		return PrecheckResult{}, ErrNodeStopped
	}
}

// ProposeControl submits a pre-encoded FSM control command (currently
// only fsm.EncodeSetClusterVersion's output — see FinalizeUpgrade) as a
// replicated command, mirroring Propose's leader-gated, event-loop-
// dispatched shape but without CommitTxn's Precheck idempotency
// fast-path (see controlProposeReq's doc comment). Exported so a future
// control-command kind beyond SetClusterVersion (none exists yet) can
// reuse this same plumbing without internal/node needing to grow a new
// per-kind Propose* method each time.
func (n *Node) ProposeControl(ctx context.Context, requestID fsm.RequestID, payload []byte) (fsm.Outcome, error) {
	req := controlProposeReq{requestID: requestID, payload: payload, resultCh: make(chan proposeResult, 1)}
	select {
	case n.controlCh <- req:
	case <-ctx.Done():
		return fsm.Outcome{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Outcome{}, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res.outcome, res.err
	case <-ctx.Done():
		return fsm.Outcome{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Outcome{}, ErrNodeStopped
	}
}

// ErrUpgradeNotReady is returned by FinalizeUpgrade when
// UpgradePrecheck reports the cluster is not yet ready (some configured
// peer has never been heard from, or reports a generation below the
// target) — docs/enterprise-v1-plan.md §7 Failure semantics: "finalize
// called while any node still reports an old version fails the
// finalize action outright... rather than partially finalizing." No
// Raft proposal is even attempted in this case.
var ErrUpgradeNotReady = errors.New("node: upgrade precheck not satisfied; not every configured cluster member currently reports support for the target generation")

// ErrAlreadyFinalized is returned by FinalizeUpgrade when this node's
// cluster generation already equals its own binary's max supported
// generation — there is nothing further to finalize to.
var ErrAlreadyFinalized = errors.New("node: cluster is already finalized at this binary's max supported generation")

// FinalizeUpgrade is the admin-triggered action that durably, cluster-
// wide raises the agreed cluster generation to this node's own
// internal/version.MaxSupportedGeneration (docs/enterprise-v1-plan.md
// §7 "finalize is an explicit, admin-gated, audited action that raises
// the cluster version once every node has actually been upgraded").
// It first runs the identical check UpgradePrecheck reports (never
// trusting a caller to have checked separately — see Failure
// semantics), and only if that passes does it propose a
// SetClusterVersionCommand through the ordinary Raft log, exactly like
// any other replicated command: a crash between precheck passing and
// this call returning leaves the command either fully committed (by
// the underlying Raft/FSM machinery this proposal path already shares
// with CommitTxn) or not proposed/committed at all, never partially
// applied.
//
// FinalizeUpgrade must be called against the current leader (like
// Propose/BeginReadIndex, it returns *NotLeaderError otherwise) — an
// operator/CLI caller retries against the leader hint exactly as any
// other client of this node's HTTP control plane already does for
// /propose. Leadership is read live, from the exact same event-loop
// dispatch that computes the rest of the precheck result
// (PrecheckResult.IsLeader/Leader), never from a separately-read
// Status() snapshot: Status() is refreshed once per run() event-loop
// iteration by refreshStatusLocked, which — for a control-command
// proposal specifically — only runs *after* the very call that would
// unblock a waiting caller, so reading it from another goroutine
// immediately after such a call has no happens-before guarantee it
// reflects this node's just-changed role. A single live read inside
// upgradePrecheck's own dispatch has no such gap.
func (n *Node) FinalizeUpgrade(ctx context.Context) (fsm.Outcome, error) {
	// unexported upgradePrecheck, not the exported UpgradePrecheck: this
	// internal re-check must not inflate UpgradePrecheckTotal, a metric
	// meant to reflect explicit operator/CLI polling — see
	// upgradePrecheck's own doc comment.
	pre, err := n.upgradePrecheck(ctx)
	if err != nil {
		return fsm.Outcome{}, err
	}
	// A non-leader's own peerGenerations view is architecturally
	// incomplete (a follower only ever exchanges Raft messages directly
	// with the leader, plus whichever peers it happened to vote for/
	// against during a past election — never a full mesh of pairwise
	// traffic with every other follower), so a non-leader's own Ready/
	// AlreadyFinalized fields could be misleading for reasons entirely
	// unrelated to whether the actual leader is ready to finalize.
	// Checking IsLeader first, before trusting either of those fields,
	// keeps this honest: only the leader's own precheck view is ever
	// actually used to gate a real finalize decision.
	if !pre.IsLeader {
		return fsm.Outcome{}, &NotLeaderError{Leader: pre.Leader}
	}
	if pre.AlreadyFinalized {
		return fsm.Outcome{}, ErrAlreadyFinalized
	}
	if !pre.Ready {
		n.metrics.UpgradeFinalizeFailedTotal.Inc()
		return fsm.Outcome{}, ErrUpgradeNotReady
	}

	// Deterministic per-target RequestID (not per-call randomness): a
	// retried finalize call targeting the same generation — e.g. after a
	// client-visible timeout/crash mid-call, docs/enterprise-v1-plan.md
	// §7's "kill the process that issued finalize mid-call" chaos
	// scenario — reuses the identical RequestID, so
	// FSM.ApplySetClusterVersion's own idempotency table (not a second,
	// ad hoc retry mechanism) is what makes the retry safe.
	reqID := fsm.RequestID(fmt.Sprintf("\x00chronicledb-finalize\x00target=%d", pre.TargetGeneration))
	payload := fsm.EncodeSetClusterVersion(fsm.SetClusterVersionCommand{RequestID: reqID, TargetGeneration: pre.TargetGeneration})
	outcome, err := n.ProposeControl(ctx, reqID, payload)
	if err != nil {
		n.metrics.UpgradeFinalizeFailedTotal.Inc()
		return fsm.Outcome{}, err
	}
	if outcome.Status == fsm.StatusAborted {
		n.metrics.UpgradeFinalizeFailedTotal.Inc()
		return outcome, fmt.Errorf("node: finalize to generation %d was rejected by replicated cluster-version state (concurrent finalize, or generation moved between precheck and propose)", pre.TargetGeneration)
	}
	n.metrics.UpgradeFinalizeTotal.Inc()
	return outcome, nil
}

// Propose submits cmd as a replicated mutation (docs/architecture.md
// §6's request path; this phase's brief's proposal path). It first
// performs a read-only idempotency check (docs/transactions.md §6): a
// known, matching RequestID returns its recorded outcome immediately
// without a fresh Raft round; a mismatched reuse is rejected outright.
// Otherwise, cmd is encoded and handed to the event loop, which
// verifies this node is currently leader, proposes it to raft.Core, and
// the caller blocks until either the entry commits and is applied
// (returning its deterministic Outcome), leadership is lost or the
// entry is superseded before that happens (an honest "unknown, retry by
// RequestID" error — never a false failure), the context is canceled,
// or the node stops.
//
// A nil error means a terminal Outcome was actually determined by
// fsm.Apply — Outcome.Status distinguishes StatusCommitted from a
// legitimate StatusAborted business decision (e.g. a Snapshot
// Isolation conflict); both are successful *replications* of a
// deterministic decision (docs/transactions.md §4's ABORTED path is
// not an error). A non-nil error means no terminal Outcome was
// determined at all for this call: the request was never accepted or
// never resolved (not leader, leadership lost, superseded, canceled, or
// stopped).
func (n *Node) Propose(ctx context.Context, cmd fsm.CommitTxnCommand) (fsm.Outcome, error) {
	if outcome, err := n.fsmachine.Load().Precheck(cmd); err == nil {
		n.metrics.RequestIDDuplicatesTotal.Inc()
		return outcome, nil
	} else if !errors.Is(err, fsm.ErrRequestIDUnknown) {
		return fsm.Outcome{}, err
	}

	req := proposeReq{cmd: cmd, payload: fsm.EncodeCommitTxn(cmd), resultCh: make(chan proposeResult, 1)}
	select {
	case n.proposeCh <- req:
	case <-ctx.Done():
		return fsm.Outcome{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Outcome{}, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res.outcome, res.err
	case <-ctx.Done():
		return fsm.Outcome{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Outcome{}, ErrNodeStopped
	}
}

// BeginReadIndex implements docs/replication.md §4 / ADR-0010's
// ReadIndex protocol for establishing a transaction's StartSeq safely:
// it proves this node is still the legitimate leader via a fresh round
// of AppendEntries acknowledged by a majority in the same term, then
// waits for this node's own appliedIndex to catch up to the resulting
// read index, before returning it as a safe StartSeq watermark. Returns
// NotLeaderError if this node is not leader, or ErrLeadershipLost if it
// steps down before the check completes.
func (n *Node) BeginReadIndex(ctx context.Context) (uint64, error) {
	req := readIndexReq{resultCh: make(chan readResult, 1)}
	select {
	case n.readIndexCh <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.doneCh:
		return 0, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res.startSeq, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.doneCh:
		return 0, ErrNodeStopped
	}
}

// Backup exports a self-contained backup.Manifest-described backup of
// this node's currently-durable committed state to outDir
// (docs/enterprise-v1-plan.md §6): the node's own currently-adopted
// snapshot boundary (or the empty, never-snapshotted boundary) as the
// base, plus — when continuous is true — every WAL entry currently
// durable beyond that boundary ("continuous WAL archiving," RPO bounded
// only by "time since this call ran"); when continuous is false, no
// trailing suffix at all ("snapshot-only," RPO bounded by "time since
// the node's own last snapshot boundary" — docs/enterprise-v1-plan.md
// §6's RPO/RTO model). This boolean, rather than a caller-supplied
// index, is deliberate: a caller cannot know this node's current
// baseIndex in advance to ask for "exactly that" by number.
//
// Like Propose/BeginReadIndex, the actual work is dispatched to and
// performed entirely on run's own event-loop goroutine (handleBackup) so
// it reads a genuinely consistent snapshot of
// n.core/n.walog/n.snapMgr/n.fsmachine — never a torn view racing a
// concurrent Propose or the node's own maybeSnapshot compaction cycle.
//
// Backup never mutates this node's own retained WAL/snapshot state (no
// compaction, no pointer update, no interaction with maybeSnapshot's
// threshold) — it only reads what is already durable and copies it
// through internal/backup.Export into an independent directory
// (docs/enterprise-v1-plan.md §6: "backup semantics clearly separated
// from Raft snapshot/compaction semantics"). Like maybeSnapshot, the
// export itself runs synchronously on the event-loop goroutine, briefly
// blocking ticks/heartbeats/proposals for its duration — an accepted,
// pre-existing tradeoff this phase inherits rather than introduces (see
// maybeSnapshot's identical characteristic); docs/backup.md documents
// this operationally.
func (n *Node) Backup(ctx context.Context, outDir string, continuous bool, clusterID string) (backup.Manifest, error) {
	req := backupReq{outDir: outDir, continuous: continuous, clusterID: clusterID, resultCh: make(chan backupResult, 1)}
	select {
	case n.backupCh <- req:
	case <-ctx.Done():
		return backup.Manifest{}, ctx.Err()
	case <-n.doneCh:
		return backup.Manifest{}, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res.manifest, res.err
	case <-ctx.Done():
		return backup.Manifest{}, ctx.Err()
	case <-n.doneCh:
		return backup.Manifest{}, ErrNodeStopped
	}
}

// run is the node's single event-loop goroutine: every Core.Step call,
// every WALStorage/fsm.FSM mutation, and every waiter resolution
// happens here, avoiding locking cycles across raft/wal/fsm/transport
// (this phase's brief's concurrency section) by construction rather
// than by lock ordering discipline.
func (n *Node) run() {
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	defer n.shutdown()

	for {
		select {
		case <-ticker.C:
			n.tick()
		case msg := <-n.tr.Recv():
			n.metrics.RaftMessagesReceivedTotal.Inc()
			n.recordPeerGeneration(msg.From, msg.SenderGeneration)
			if msg.Type == raft.MsgInstallSnapshotRequest {
				n.handleInstallSnapshot(msg)
			} else {
				n.step(raft.Input{Kind: raft.InputMessage, Message: msg})
			}
		case req := <-n.proposeCh:
			n.handlePropose(req)
		case req := <-n.controlCh:
			n.handleControlPropose(req)
		case req := <-n.readIndexCh:
			n.handleReadIndex(req)
		case req := <-n.backupCh:
			n.handleBackup(req)
		case req := <-n.precheckCh:
			req.resultCh <- n.computePrecheck()
		case <-n.stopCh:
			return
		}
		n.refreshStatusLocked()
	}
}

func (n *Node) shutdown() {
	for idx, w := range n.waiters {
		n.metrics.ProposalsUnknownTotal.Inc()
		w.resultCh <- proposeResult{err: ErrNodeStopped}
		delete(n.waiters, idx)
	}
	for _, pr := range n.pendingReads {
		pr.resultCh <- readResult{err: ErrNodeStopped}
	}
	n.pendingReads = nil
	n.tr.Close()
	n.walog.Close()
	close(n.doneCh)
}

func (n *Node) tick() {
	if n.electionArmed && !n.electionTicksPaused.Load() {
		n.electionTicksLeft--
		if n.electionTicksLeft <= 0 {
			n.electionArmed = false
			n.step(raft.Input{Kind: raft.InputElectionTimeout})
		}
	}
	if n.heartbeatArmed {
		n.heartbeatTicksLeft--
		if n.heartbeatTicksLeft <= 0 {
			n.heartbeatArmed = false
			n.step(raft.Input{Kind: raft.InputHeartbeatTimeout})
		}
	}
}

// step delivers one Input to Core and processes the resulting Output.
// Call only from run's goroutine.
func (n *Node) step(in raft.Input) {
	// Recognize a legitimate, current-term Success AppendEntriesResponse
	// BEFORE handing it to Core, using exactly the same precondition
	// (Role==Leader, msg.Term==CurrentTerm) Core itself uses to decide
	// whether to actually honor it — see Node.ackSeq's doc comment.
	// Guarded with ">" (not unconditional) so an out-of-order-delivered
	// older ack can never regress ackSeq below a fresher one already
	// recorded.
	if in.Kind == raft.InputMessage && in.Message.Type == raft.MsgAppendEntriesResponse && in.Message.Success &&
		n.core.Role() == raft.Leader && in.Message.Term == n.core.CurrentTerm() &&
		in.Message.Seq > n.ackSeq[in.Message.From] {
		n.ackSeq[in.Message.From] = in.Message.Seq
	}
	// raft.Core.handleElectionTimeout no-ops for an already-Leader node
	// (docs/raft.md), so counting only the calls that can actually start
	// a new election avoids a vacuous counter that just tracks the
	// heartbeat/tick rate on a stable leader.
	if in.Kind == raft.InputElectionTimeout && n.core.Role() != raft.Leader {
		n.metrics.ElectionsTotal.Inc()
	}
	out := n.core.Step(in)
	if in.Kind == raft.InputMessage && in.Message.Type == raft.MsgAppendEntriesRequest {
		// Core is Seq-agnostic (see raft.Message.Seq's doc comment) and
		// never sets it on a response it builds; echo back exactly the
		// Seq this request carried so the original sender can correlate
		// its own ackSeq bookkeeping (see Node.ackSeq's doc comment) —
		// done here, once, rather than duplicated at every one of
		// Core's several internal AppendEntriesResponse construction
		// sites.
		for i := range out.Messages {
			if out.Messages[i].Type == raft.MsgAppendEntriesResponse {
				out.Messages[i].Seq = in.Message.Seq
			}
		}
	}
	n.processOutput(out)
}

// handlePropose is the leader-gated entry point for a fresh client
// mutation (docs replication.md §1.2 step 1: "the leader accepted the
// client's request, validated it is current leader").
func (n *Node) handlePropose(req proposeReq) {
	n.proposeAndAwait(req.payload, req.cmd.RequestID, req.resultCh,
		func() { n.metrics.ProposalsRejectedTotal.Inc() },
		func() { n.metrics.ProposalsTotal.Inc() })
}

// handleControlPropose is the leader-gated entry point for a control
// command (currently only FinalizeUpgrade's SetClusterVersionCommand),
// sharing proposeAndAwait's plumbing with handlePropose except for the
// missing CommitTxn-only Precheck fast-path (see controlProposeReq's
// doc comment) and CommitTxn-specific metrics (control commands are
// rare admin actions with their own metrics, counted in FinalizeUpgrade
// itself).
func (n *Node) handleControlPropose(req controlProposeReq) {
	n.proposeAndAwait(req.payload, req.requestID, req.resultCh, nil, nil)
}

// proposeAndAwait is the leader-gated proposal path shared by
// handlePropose and handleControlPropose (previously two
// near-identical copies of this logic): check leadership, hand payload
// to raft.Core, validate the resulting persist-request shape, register
// a waiter for the resulting index, and drive processOutput. onRejected/
// onAccepted, when non-nil, let a caller count its own command-kind-
// specific metrics at exactly the points where this shared logic
// decides "not leader" vs. "accepted" — the metrics themselves stay
// specific to each caller, not baked into shared plumbing that has no
// business knowing about them.
func (n *Node) proposeAndAwait(payload []byte, requestID fsm.RequestID, resultCh chan proposeResult, onRejected, onAccepted func()) {
	if n.core.Role() != raft.Leader {
		if onRejected != nil {
			onRejected()
		}
		resultCh <- proposeResult{err: &NotLeaderError{Leader: n.core.LeaderID()}}
		return
	}
	out := n.core.Step(raft.Input{Kind: raft.InputPropose, ProposeData: payload})
	if out.ProposalRejected {
		if onRejected != nil {
			onRejected()
		}
		resultCh <- proposeResult{err: &NotLeaderError{Leader: out.LeaderHint}}
		return
	}
	if onAccepted != nil {
		onAccepted()
	}
	if out.PersistRequest == nil || len(out.PersistRequest.Entries) != 1 {
		// Defensive: a leader-accepted InputPropose always produces
		// exactly this shape (raft.Core.handlePropose). Treat any other
		// shape as an unrecoverable local inconsistency rather than
		// silently dropping the caller's request.
		n.fail(fmt.Errorf("node: unexpected propose output shape: %+v", out))
		resultCh <- proposeResult{err: ErrNodeStopped}
		return
	}
	idx := out.PersistRequest.Entries[0].Index
	n.waiters[idx] = waiter{requestID: requestID, resultCh: resultCh}
	n.processOutput(out)
}

func (n *Node) handleReadIndex(req readIndexReq) {
	if n.core.Role() != raft.Leader {
		req.resultCh <- readResult{err: &NotLeaderError{Leader: n.core.LeaderID()}}
		return
	}
	term0 := n.core.CurrentTerm()
	target := n.core.LastIndex()
	// Every peer's ack must echo a request Seq strictly greater than
	// sentSeqCounter's value right now — see Node.ackSeq's doc comment
	// for why this must be a wire-carried, request-specific token
	// rather than "was some ack merely processed after this point."
	// This forced round's own outgoing requests (assigned fresh Seq
	// values by processOutput, below) satisfy that by construction; so
	// would any later, independently-timed heartbeat, which is exactly
	// as valid a proof of liveness.
	requiredSeq := n.sentSeqCounter
	// Force an immediate fresh round of AppendEntries to every peer, so
	// a subsequent Success acknowledgement in this same term proves
	// this node was still the legitimate leader after target was
	// captured (docs/replication.md §4.1 steps 1-2).
	out := n.core.Step(raft.Input{Kind: raft.InputHeartbeatTimeout})
	n.pendingReads = append(n.pendingReads, pendingRead{term: term0, target: target, requiredSeq: requiredSeq, resultCh: req.resultCh})
	n.processOutput(out)
}

// checkPendingReads resolves or fails every outstanding BeginReadIndex
// request whose condition can now be determined. Called after every
// processOutput, since any Step call can move matchIndex/appliedIndex
// or change term/role.
func (n *Node) checkPendingReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	remaining := n.pendingReads[:0]
	for _, pr := range n.pendingReads {
		if n.core.Role() != raft.Leader || n.core.CurrentTerm() != pr.term {
			pr.resultCh <- readResult{err: ErrLeadershipLost}
			continue
		}
		acked := 1 // self
		for _, p := range n.cfg.Peers {
			if p == n.cfg.ID {
				continue
			}
			if n.ackSeq[p] > pr.requiredSeq {
				acked++
			}
		}
		if acked < n.majority() {
			remaining = append(remaining, pr)
			continue
		}
		if n.appliedIndex < uint64(pr.target) {
			remaining = append(remaining, pr)
			continue
		}
		pr.resultCh <- readResult{startSeq: uint64(pr.target)}
	}
	n.pendingReads = remaining
}

// processOutput performs every side effect one raft.Output describes:
// durable persistence (recursing into the PersistenceComplete
// acknowledgement, exactly mirroring internal/fault.Node's synchronous
// model but against the real WAL), outbound message transmission,
// committed-entry application to internal/fsm, and pending
// waiter/read-index resolution. Call only from run's goroutine.
func (n *Node) processOutput(out raft.Output) {
	if out.ResetElectionTimer {
		n.electionArmed = true
		n.electionTicksLeft = out.ElectionTimeoutTicks
	}
	if out.ResetHeartbeatTimer {
		n.heartbeatArmed = true
		n.heartbeatTicksLeft = out.HeartbeatTimeoutTicks
	}

	for _, m := range out.Messages {
		if m.Type == raft.MsgInstallSnapshotRequest && len(m.SnapshotData) == 0 {
			// Core never carries snapshot bytes itself (docs/snapshots.md
			// §7 step 1, raft.MsgInstallSnapshotRequest's doc comment) —
			// fill them in from this node's own retained snapshot before
			// the message ever reaches the wire.
			data, ok, err := n.snapMgr.Bytes(uint64(m.LastIncludedIndex))
			if err != nil || !ok {
				n.logf("node %s: cannot serve snapshot %d to %s (ok=%v err=%v); skipping this round, leader will retry", n.cfg.ID, m.LastIncludedIndex, m.To, ok, err)
				continue
			}
			m.SnapshotData = data
		}
		if m.Type == raft.MsgAppendEntriesRequest {
			// Assign a fresh, strictly-increasing correlation token to
			// every outbound AppendEntries request (regular heartbeat
			// or a ReadIndex-forced one alike) — see Node.ackSeq's doc
			// comment and raft.Message.Seq's doc comment.
			n.sentSeqCounter++
			m.Seq = n.sentSeqCounter
		}
		n.send(m)
		n.metrics.RaftMessagesSentTotal.Inc()
	}

	if out.PersistRequest != nil {
		if err := raft.ApplyPersistRequest(n.storage, out.PersistRequest); err != nil {
			n.fail(fmt.Errorf("node: durable persistence failed: %w", err))
			return
		}
		ack := n.core.Step(raft.Input{Kind: raft.InputPersistenceComplete, PersistSeq: out.PersistRequest.Seq})
		n.processOutput(ack)
	}

	n.applyCommitted(out.CommittedEntries)

	if out.SteppedDown {
		for idx, w := range n.waiters {
			n.metrics.ProposalsUnknownTotal.Inc()
			w.resultCh <- proposeResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	if out.BecameLeader {
		n.metrics.LeaderChangesTotal.Inc()
		n.logf("node %s became leader for term %d", n.cfg.ID, n.core.CurrentTerm())
		n.proposeElectionNoOp()
	}

	n.checkPendingReads()
}

// proposeElectionNoOp submits a synthetic, empty-mutation CommitTxn
// command in this node's own new term immediately upon becoming leader
// (docs/replication.md §4.3). This is a driver-level liveness fix, not
// a change to internal/raft.Core's own documented election behavior
// (docs/raft.md's implementation note "no no-op entry is appended on
// election" still describes Core accurately — Core still never
// invents one internally): Raft's current-term commit rule
// (docs/raft.md §4) means a newly elected leader cannot advance its
// own commitIndex past entries from a *previous* term until it has
// committed at least one entry in its *own* current term, no matter
// how many nodes already durably hold those older entries. Without
// this, BeginReadIndex's target (Node.handleReadIndex's target :=
// n.core.LastIndex()) could reference an old-term entry that this
// leader can never independently recognize as committed until some
// unrelated future write arrives — an indefinite liveness stall, not a
// safety violation, discovered via Phase 8's real-cluster SQL testing
// (every SQL statement's Session.Begin calls BeginReadIndex, including
// for a fresh INSERT — see internal/sql/engine.go).
//
// The no-op's RequestID is deliberately built from a NUL byte no real
// client is expected to send, plus this node's ID and new term, making
// collision with a genuine client RequestID practically impossible
// (ChronicleDB V1 already assumes a trusted client/cluster network,
// docs/non-goals.md §Authentication and TLS) while guaranteeing every
// election, on every node, proposes a distinct RequestID. A rejected
// or superseded proposal (this node loses leadership again before the
// no-op commits) is silently dropped: nothing is waiting on its
// outcome, and a future election will try again.
func (n *Node) proposeElectionNoOp() {
	cmd := fsm.CommitTxnCommand{
		RequestID: fsm.RequestID(fmt.Sprintf("\x00chronicledb-election-noop\x00%s\x00%d", n.cfg.ID, n.core.CurrentTerm())),
		TxnID:     uint64(n.core.CurrentTerm()),
		StartSeq:  0,
		Mutations: nil,
	}
	if _, err := n.fsmachine.Load().Precheck(cmd); err == nil || !errors.Is(err, fsm.ErrRequestIDUnknown) {
		return // already proposed (or otherwise not fresh) for this exact node+term; never retry indefinitely
	}
	out := n.core.Step(raft.Input{Kind: raft.InputPropose, ProposeData: fsm.EncodeCommitTxn(cmd)})
	if out.ProposalRejected {
		return
	}
	n.processOutput(out)
}

// applyCommitted runs internal/fsm.Apply for every newly committed
// entry, in order, exactly once (docs/transactions.md §4-5,
// docs/recovery.md §11), resolving any Propose waiter registered for
// that index — verifying the RequestID actually at that index still
// matches what the waiter proposed, since a divergent-suffix repair can
// legitimately overwrite an uncommitted index with a different leader's
// entry before it ever reaches this point (ErrProposalSuperseded).
func (n *Node) applyCommitted(entries []raft.Entry) {
	for _, e := range entries {
		if uint64(e.Index) <= n.appliedIndex {
			continue // already applied (e.g. benign re-derivation after restart)
		}
		if fsm.IsControlCommand(e.Data) {
			if !n.applyControlEntry(e) {
				return // n.fail already recorded the error and stopped the node
			}
			continue
		}
		cmd, err := fsm.DecodeCommitTxn(e.Data)
		if err != nil {
			n.fail(fmt.Errorf("node: decoding committed entry %d: %w", e.Index, err))
			return
		}
		outcome, err := n.fsmachine.Load().Apply(uint64(e.Index), cmd)
		if err != nil {
			n.fail(fmt.Errorf("node: applying committed entry %d: %w", e.Index, err))
			return
		}
		n.appliedIndex = uint64(e.Index)
		n.core.SetApplied(e.Index)

		n.resolveWaiter(e.Index, cmd.RequestID, outcome, func(o fsm.Outcome) {
			switch o.Status {
			case fsm.StatusCommitted:
				n.metrics.ProposalsCommittedTotal.Inc()
			case fsm.StatusAborted:
				n.metrics.ProposalsAbortedTotal.Inc()
			}
		})
	}
	n.maybeSnapshot()
}

// resolveWaiter delivers outcome to the Propose/ProposeControl waiter
// registered for index, if any (previously duplicated near-identically
// between applyCommitted's CommitTxn path and applyControlEntry): a
// waiter whose requestID no longer matches what is actually at index
// was superseded by a divergent-suffix repair before it committed (see
// ErrProposalSuperseded) — resolved identically regardless of which
// command kind occupies that index. onMatched, when non-nil, lets a
// caller count its own command-kind-specific metrics for the common
// (matched) case; the superseded case's ProposalsUnknownTotal increment
// is identical for every caller and always happens here.
func (n *Node) resolveWaiter(index raft.Index, requestID fsm.RequestID, outcome fsm.Outcome, onMatched func(fsm.Outcome)) {
	w, ok := n.waiters[index]
	if !ok {
		return
	}
	delete(n.waiters, index)
	if w.requestID != requestID {
		n.metrics.ProposalsUnknownTotal.Inc()
		w.resultCh <- proposeResult{err: ErrProposalSuperseded}
		return
	}
	if onMatched != nil {
		onMatched(outcome)
	}
	w.resultCh <- proposeResult{outcome: outcome}
}

// adoptClusterGeneration is the single choke point every code path that
// can advance this node's cluster generation goes through — a
// committed control command (applyControlEntry) and adopting a peer's
// installed snapshot (handleInstallSnapshot) alike — so neither path
// can forget to durably persist it (wal.WAL.SetClusterGeneration) or
// forget to keep the cached n.clusterGeneration field (see its own doc
// comment) in sync with the FSM's own value. Returns false if it called
// n.fail, mirroring every other apply-path helper's control flow. Call
// only from run's goroutine.
func (n *Node) adoptClusterGeneration(generation uint32) bool {
	if err := n.walog.SetClusterGeneration(generation); err != nil {
		n.fail(fmt.Errorf("node: persisting cluster generation %d: %w", generation, err))
		return false
	}
	n.clusterGeneration = generation
	return true
}

// applyControlEntry applies one committed FSM control-command entry
// (fsm.ControlCommandMarker — currently only SetClusterVersionCommand),
// resolving any waiter registered for its index exactly as
// applyCommitted does for an ordinary CommitTxn entry. Returns false if
// it called n.fail (an unrecoverable decode/capability/apply error),
// mirroring applyCommitted's own early-return-on-failure control flow
// — the caller must stop processing further entries in that case.
func (n *Node) applyControlEntry(e raft.Entry) bool {
	cmd, err := fsm.DecodeSetClusterVersion(e.Data)
	if err != nil {
		// NO SILENT FORMAT MISINTERPRETATION: an unrecognized control
		// command kind (fsm.ErrUnknownControlCommand) or a malformed
		// payload both fail closed here exactly like an unrecognized
		// CommitTxn command version does below.
		n.fail(fmt.Errorf("node: decoding committed control entry %d: %w", e.Index, err))
		return false
	}
	if cmd.TargetGeneration > version.MaxSupportedGeneration {
		// STATE MACHINE SAFETY / fail-closed: this node's own binary is
		// older than the generation this already-committed command
		// requires. In practice FinalizeUpgrade's precheck should make
		// this unreachable (a finalize is only proposed once every live
		// node already reports support), but a node applying a command
		// it does not have the capability to safely interpret must still
		// refuse rather than guess, as defense in depth.
		n.fail(fmt.Errorf("node: committed entry %d requests cluster generation %d, this binary only supports up to %d",
			e.Index, cmd.TargetGeneration, version.MaxSupportedGeneration))
		return false
	}
	outcome, err := n.fsmachine.Load().ApplySetClusterVersion(uint64(e.Index), cmd)
	if err != nil {
		n.fail(fmt.Errorf("node: applying committed control entry %d: %w", e.Index, err))
		return false
	}
	n.appliedIndex = uint64(e.Index)
	n.core.SetApplied(e.Index)

	if outcome.Status == fsm.StatusCommitted {
		// Persist the new generation to WAL metadata (docs/wal.md §8)
		// immediately, on the same goroutine, before this entry is
		// considered fully applied — a restart between this point and
		// the next tick must still see the finalized generation via
		// wal.Open's own ErrUnsupportedGeneration check, not silently
		// forget it happened.
		if !n.adoptClusterGeneration(cmd.TargetGeneration) {
			return false // n.fail already recorded the error and stopped the node
		}
		n.logf("node %s: cluster generation finalized to %d at index %d", n.cfg.ID, cmd.TargetGeneration, e.Index)
	}

	n.resolveWaiter(e.Index, cmd.RequestID, outcome, nil)
	return true
}

// maybeSnapshot creates a fresh local snapshot and compacts this node's
// own log against it once durable log growth since the last snapshot
// boundary reaches cfg.SnapshotThreshold (docs/snapshots.md §3's
// trigger). It is checked after every batch of newly applied entries —
// a function of actual applied state, never a wall-clock timer.
//
// The write order matches docs/snapshots.md §3/§8 exactly: the snapshot
// file itself is created and confirmed durable (Manager.Create's own
// temp-file/fsync/atomic-rename/dir-fsync sequence) before the WAL's
// restart-discovery pointer is ever updated (AppendMetadataSnapshot),
// which in turn happens before HardState is re-affirmed into the
// current segment and old segments are physically deleted
// (CompactBefore) — "snapshot durable before truncation"
// (LOG-COMPACTION-SAFETY). raft.Core's own in-memory log is compacted
// only after every durable step above has already succeeded, so a
// crash at any point leaves Core's log a superset of what durable state
// actually needs, never a subset.
func (n *Node) maybeSnapshot() {
	snapIdx := uint64(n.core.SnapshotIndex())
	if n.appliedIndex-snapIdx < n.cfg.SnapshotThreshold {
		return
	}

	meta := snapshot.Meta{LastIncludedIndex: n.appliedIndex, LastIncludedTerm: uint64(n.termAtApplied())}
	if _, err := n.snapMgr.Create(meta, n.fsmachine.Load()); err != nil {
		n.fail(fmt.Errorf("node: creating snapshot at index %d: %w", meta.LastIncludedIndex, err))
		return
	}
	if err := n.walog.AppendMetadataSnapshot(meta.LastIncludedIndex); err != nil {
		n.fail(fmt.Errorf("node: recording snapshot pointer at index %d: %w", meta.LastIncludedIndex, err))
		return
	}
	n.core.Compact(raft.Index(meta.LastIncludedIndex))
	n.storage.Compact(raft.Index(meta.LastIncludedIndex))
	if err := n.storage.Reaffirm(); err != nil {
		n.fail(fmt.Errorf("node: reaffirming hard state before compaction: %w", err))
		return
	}
	if err := n.walog.CompactBefore(meta.LastIncludedIndex); err != nil {
		n.fail(fmt.Errorf("node: compacting log before index %d: %w", meta.LastIncludedIndex, err))
		return
	}
	n.metrics.SnapshotsCreatedTotal.Inc()
	n.logf("node %s: created snapshot at index %d, compacted log", n.cfg.ID, meta.LastIncludedIndex)
}

// handleBackup runs entirely on the event-loop goroutine (see Backup's
// doc comment): it reads this node's currently-adopted snapshot boundary
// (n.core.SnapshotIndex(), already durable via n.snapMgr — or, if this
// node has never snapshotted, the empty boundary at index 0) and this
// node's own live n.walog as the source for internal/backup.Export,
// exactly the same (already-durable, already-validated) state a restart
// would recover from — Backup invents nothing beyond what is already on
// disk.
func (n *Node) handleBackup(req backupReq) {
	baseIndex := uint64(n.core.SnapshotIndex())
	var baseFSM *fsm.FSM
	if baseIndex == 0 {
		baseFSM = fsm.New(mvcc.NewStore())
	} else {
		snap, ok, err := n.snapMgr.Load(baseIndex)
		if err != nil || !ok {
			req.resultCh <- backupResult{err: fmt.Errorf("node: backup: loading this node's own adopted snapshot at %d: ok=%v err=%w", baseIndex, ok, err)}
			return
		}
		baseFSM = snap.FSM
	}
	baseTerm := n.core.SnapshotTerm()
	if baseIndex == 0 {
		baseTerm = 0
	}

	src := backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: baseIndex, LastIncludedTerm: uint64(baseTerm)},
		BaseFSM:  baseFSM,
		WAL:      n.walog,
	}
	until := baseIndex
	if req.continuous {
		until = backup.UntilLatest
	}
	m, err := backup.Export(src, req.outDir, backup.ExportOptions{UntilIndex: until, ClusterID: req.clusterID})
	if err != nil {
		n.metrics.BackupsFailedTotal.Inc()
		req.resultCh <- backupResult{err: fmt.Errorf("node: backup: %w", err)}
		return
	}
	n.metrics.BackupsTotal.Inc()
	req.resultCh <- backupResult{manifest: m}
}

// termAtApplied returns the Raft term of the log entry at n.appliedIndex
// — needed to fill in Meta.LastIncludedTerm when creating a snapshot at
// the current applied boundary (docs/snapshots.md §2).
func (n *Node) termAtApplied() raft.Term {
	idx := raft.Index(n.appliedIndex)
	if idx == n.core.SnapshotIndex() {
		return n.core.SnapshotTerm()
	}
	e, ok := n.core.EntryAt(idx)
	if !ok {
		return 0
	}
	return e.Term
}

// handleInstallSnapshot is the driver-side entry point for a received
// MsgInstallSnapshotRequest (docs/snapshots.md §7 steps 2-4): it
// validates and durably installs msg.SnapshotData via
// internal/snapshot.Manager.Install and, if the snapshot actually
// advances this node's state, atomically replaces this node's
// fsmachine/durable log to match — all strictly BEFORE ever handing
// msg to raft.Core.Step, per that message's documented driver contract
// (see raft.MsgInstallSnapshotRequest's doc comment). Only after that
// (successful or not) does msg reach Core.Step, which is what actually
// determines and sends the MsgInstallSnapshotResponse.
//
// Whether this snapshot actually advances anything is decided by
// mirroring the exact condition under which Core.Step itself will
// advance c.snapshotIndex (msg.Term >= c.currentTerm and
// msg.LastIncludedIndex > c.snapshotIndex) — computed here from
// Core's own read-only accessors, without calling Step — so the driver
// never durably installs snapshot state that Core.Step would then go on
// to reject as stale, which would otherwise desynchronize this
// WALStorage's mirror from Core's own view of the log.
func (n *Node) handleInstallSnapshot(msg raft.Message) {
	snap, err := n.snapMgr.Install(msg.SnapshotData)
	if err != nil {
		n.send(raft.Message{
			Type: raft.MsgInstallSnapshotResponse, From: n.cfg.ID, To: msg.From,
			Term: n.core.CurrentTerm(), Success: false,
		})
		return
	}

	willAdvance := msg.Term >= n.core.CurrentTerm() && raft.Index(snap.Meta.LastIncludedIndex) > n.core.SnapshotIndex()
	if willAdvance {
		if err := n.storage.InstallSnapshot(raft.Index(snap.Meta.LastIncludedIndex)); err != nil {
			n.fail(fmt.Errorf("node: installing snapshot at index %d: %w", snap.Meta.LastIncludedIndex, err))
			return
		}
		n.fsmachine.Store(snap.FSM)
		n.appliedIndex = snap.Meta.LastIncludedIndex
		// A follower catching up via a peer's snapshot, rather than
		// replaying the committed SetClusterVersionCommand log entry
		// itself (e.g. that entry was already compacted away by the
		// leader before this node caught up), must still durably adopt
		// whatever cluster generation the installed snapshot's FSM state
		// carries — otherwise this node's WAL metadata would never learn
		// a generation its own in-memory FSM already reflects, silently
		// bypassing wal.Open's ErrUnsupportedGeneration rollback-refusal
		// check on a future restart with an older binary (see
		// adoptClusterGeneration's doc comment: this is the other of the
		// two paths that must go through it).
		if !n.adoptClusterGeneration(snap.FSM.ClusterGeneration()) {
			return // n.fail already recorded the error and stopped the node
		}
		n.metrics.SnapshotsInstalledTotal.Inc()
		// Any waiter for an index this install just superseded is never
		// resolved from here (applyCommitted no longer replays it) — it
		// will time out via its own context/ErrLeadershipLost path if the
		// caller is still waiting, exactly as any other superseded
		// proposal is handled.
	}

	n.step(raft.Input{Kind: raft.InputMessage, Message: msg})
}

// fail records a fatal local error (e.g. a disk write failure) and
// stops the node: per this phase's brief, "do not silently mark
// unsuccessful proposals committed" — a node that cannot trust its own
// persistence path must stop participating rather than continue in a
// possibly-inconsistent state. Call only from run's goroutine.
func (n *Node) fail(err error) {
	n.fatalMu.Lock()
	if n.fatal == nil {
		n.fatal = err
	}
	n.fatalMu.Unlock()
	n.logf("node %s: fatal error, stopping: %v", n.cfg.ID, err)
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// Err returns the fatal local error that caused this node to stop
// itself, if any. Only meaningful after Stop/the doneCh channel closes.
func (n *Node) Err() error {
	n.fatalMu.Lock()
	defer n.fatalMu.Unlock()
	return n.fatal
}

// Done returns a channel closed once the node's event loop has fully
// exited (whether via Stop or a fatal local error).
func (n *Node) Done() <-chan struct{} { return n.doneCh }
