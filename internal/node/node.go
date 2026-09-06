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

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/transport"
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

type pendingRead struct {
	term raft.Term
	// target is the log index a majority of peers must freshly
	// reconfirm (via baseline below) before this read is safe, and the
	// index appliedIndex must reach before it is returned as StartSeq.
	target raft.Index
	// baseline snapshots, per peer, the value of Node.lastAck at the
	// moment this read was issued (see checkPendingReads): the read is
	// safe only once a majority's lastAck has strictly advanced past
	// its own baseline, proving each contributed a Success
	// AppendEntriesResponse sent no earlier than this read's issuance —
	// a live round-trip in the current term, not merely a possibly-
	// stale cached replication fact (see Node.lastAck's doc comment for
	// why comparing against matchIndex directly is not suffient).
	baseline map[raft.NodeID]uint64
	resultCh chan readResult
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
	logger    *log.Logger

	electionArmed      bool
	electionTicksLeft  int
	heartbeatArmed     bool
	heartbeatTicksLeft int

	appliedIndex uint64
	waiters      map[raft.Index]waiter
	pendingReads []pendingRead

	// ackCounter/lastAck implement ADR-0010's ReadIndex freshness proof
	// (docs/replication.md §4.1): lastAck[peer] is bumped to a fresh,
	// strictly-increasing value exactly when this node, as Leader,
	// processes a legitimate (current-term) Success
	// AppendEntriesResponse from peer (see step). Comparing a pending
	// read's baseline snapshot (checkPendingReads) against the CURRENT
	// value proves a live round-trip happened after the read was
	// issued — comparing against raft.Core's own matchIndex directly
	// would not: matchIndex only ever increases and is never reset by a
	// partition, so a leader that already replicated up to its current
	// LastIndex before becoming isolated would otherwise pass a
	// matchIndex-based check vacuously, without any fresh contact at
	// all, defeating the safety property this check exists to prove.
	ackCounter uint64
	lastAck    map[raft.NodeID]uint64

	proposeCh   chan proposeReq
	readIndexCh chan readIndexReq
	stopCh      chan struct{}
	doneCh      chan struct{}
	stopOnce    sync.Once

	statusMu sync.Mutex
	status   Status

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

	tr, err := transport.New(cfg.ID, cfg.ListenAddr, cfg.PeerAddrs)
	if err != nil {
		w.Close()
		return nil, err
	}

	n := &Node{
		cfg:          cfg,
		core:         core,
		walog:        w,
		storage:      st,
		snapMgr:      snapMgr,
		tr:           tr,
		logger:       cfg.Logger,
		appliedIndex: baseIndex,
		waiters:      make(map[raft.Index]waiter),
		lastAck:      make(map[raft.NodeID]uint64, len(cfg.Peers)),
		proposeCh:    make(chan proposeReq),
		readIndexCh:  make(chan readIndexReq),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
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

// FSM returns the node's deterministic state machine, for read-only
// access (docs/mvcc.md §3 visibility reads bypass Apply) by a caller
// that has otherwise established it is safe to read (e.g. after a
// successful BeginReadIndex). Mutations must only ever happen via
// Propose.
func (n *Node) FSM() *fsm.FSM { return n.fsmachine.Load() }

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
		ID:            n.cfg.ID,
		Role:          n.core.Role(),
		Term:          n.core.CurrentTerm(),
		Leader:        n.core.LeaderID(),
		CommitIndex:   n.core.CommitIndex(),
		AppliedIndex:  n.appliedIndex,
		LastIndex:     n.core.LastIndex(),
		SnapshotIndex: n.core.SnapshotIndex(),
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
			if msg.Type == raft.MsgInstallSnapshotRequest {
				n.handleInstallSnapshot(msg)
			} else {
				n.step(raft.Input{Kind: raft.InputMessage, Message: msg})
			}
		case req := <-n.proposeCh:
			n.handlePropose(req)
		case req := <-n.readIndexCh:
			n.handleReadIndex(req)
		case <-n.stopCh:
			return
		}
		n.refreshStatusLocked()
	}
}

func (n *Node) shutdown() {
	for idx, w := range n.waiters {
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
	if n.electionArmed {
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
	// whether to actually honor it — see Node.lastAck's doc comment.
	if in.Kind == raft.InputMessage && in.Message.Type == raft.MsgAppendEntriesResponse && in.Message.Success &&
		n.core.Role() == raft.Leader && in.Message.Term == n.core.CurrentTerm() {
		n.ackCounter++
		n.lastAck[in.Message.From] = n.ackCounter
	}
	out := n.core.Step(in)
	n.processOutput(out)
}

// handlePropose is the leader-gated entry point for a fresh client
// mutation (docs replication.md §1.2 step 1: "the leader accepted the
// client's request, validated it is current leader").
func (n *Node) handlePropose(req proposeReq) {
	if n.core.Role() != raft.Leader {
		req.resultCh <- proposeResult{err: &NotLeaderError{Leader: n.core.LeaderID()}}
		return
	}
	out := n.core.Step(raft.Input{Kind: raft.InputPropose, ProposeData: req.payload})
	if out.ProposalRejected {
		req.resultCh <- proposeResult{err: &NotLeaderError{Leader: out.LeaderHint}}
		return
	}
	if out.PersistRequest == nil || len(out.PersistRequest.Entries) != 1 {
		// Defensive: a leader-accepted InputPropose always produces
		// exactly this shape (raft.Core.handlePropose). Treat any other
		// shape as an unrecoverable local inconsistency rather than
		// silently dropping the caller's request.
		n.fail(fmt.Errorf("node: unexpected propose output shape: %+v", out))
		req.resultCh <- proposeResult{err: ErrNodeStopped}
		return
	}
	idx := out.PersistRequest.Entries[0].Index
	n.waiters[idx] = waiter{requestID: req.cmd.RequestID, resultCh: req.resultCh}
	n.processOutput(out)
}

func (n *Node) handleReadIndex(req readIndexReq) {
	if n.core.Role() != raft.Leader {
		req.resultCh <- readResult{err: &NotLeaderError{Leader: n.core.LeaderID()}}
		return
	}
	term0 := n.core.CurrentTerm()
	target := n.core.LastIndex()
	baseline := make(map[raft.NodeID]uint64, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		baseline[p] = n.lastAck[p]
	}
	// Force an immediate fresh round of AppendEntries to every peer, so
	// a subsequent Success acknowledgement in this same term proves
	// this node was still the legitimate leader after target was
	// captured (docs/replication.md §4.1 steps 1-2).
	out := n.core.Step(raft.Input{Kind: raft.InputHeartbeatTimeout})
	n.pendingReads = append(n.pendingReads, pendingRead{term: term0, target: target, baseline: baseline, resultCh: req.resultCh})
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
			if n.lastAck[p] > pr.baseline[p] {
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
		n.tr.Send(m)
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
			w.resultCh <- proposeResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	if out.BecameLeader {
		n.logf("node %s became leader for term %d", n.cfg.ID, n.core.CurrentTerm())
	}

	n.checkPendingReads()
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

		if w, ok := n.waiters[e.Index]; ok {
			delete(n.waiters, e.Index)
			if w.requestID == cmd.RequestID {
				w.resultCh <- proposeResult{outcome: outcome}
			} else {
				w.resultCh <- proposeResult{err: ErrProposalSuperseded}
			}
		}
	}
	n.maybeSnapshot()
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
	n.logf("node %s: created snapshot at index %d, compacted log", n.cfg.ID, meta.LastIncludedIndex)
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
		n.tr.Send(raft.Message{
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
