// Command chronicledb-node is a real OS-process entry point for one
// ChronicleDB replicated node (docs/roadmap.md Phase 5). It exists
// specifically to provide "real multi-process/real-disk proof" evidence
// this phase's brief requires beyond internal/fault's deterministic,
// in-process simulator: an integration test drives several actual
// chronicledb-node processes (see main_test.go), each with its own
// persistent data directory and real TCP sockets, submitting mutations
// and killing/restarting processes via a minimal local HTTP control
// plane.
//
// This is deliberately not a general-purpose client protocol
// (docs/architecture.md's internal/protocol package is out of Phase 5
// scope per docs/roadmap.md — only internal/transport and internal/node
// are): it is the smallest real thing that lets an external test
// process observe and drive a real chronicledb-node process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
)

func main() {
	var (
		id                = flag.String("id", "", "this node's ID")
		listenAddr        = flag.String("listen", "", "raft transport listen address (host:port)")
		httpAddr          = flag.String("http", "", "control-plane HTTP listen address (host:port)")
		peersFlag         = flag.String("peers", "", "comma-separated id=addr list for every OTHER cluster member")
		allFlag           = flag.String("cluster", "", "comma-separated id list of every cluster member, including this one")
		dataDir           = flag.String("datadir", "", "durable log directory")
		snapshotThreshold = flag.Uint64("snapshot-threshold", 0, "log entries since last snapshot before compacting (0 = package default); tests use a small value to force snapshot/compaction chaos quickly")
		showVersion       = flag.Bool("version", false, "print version information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return
	}

	if *id == "" || *listenAddr == "" || *httpAddr == "" || *dataDir == "" || *allFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: chronicledb-node -id=ID -listen=HOST:PORT -http=HOST:PORT -datadir=DIR -cluster=id1,id2,id3 -peers=id2=host:port,id3=host:port")
		os.Exit(2)
	}

	peerAddrs := map[raft.NodeID]string{}
	if *peersFlag != "" {
		for _, kv := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "invalid -peers entry %q\n", kv)
				os.Exit(2)
			}
			peerAddrs[raft.NodeID(parts[0])] = parts[1]
		}
	}
	var peers []raft.NodeID
	for _, p := range strings.Split(*allFlag, ",") {
		peers = append(peers, raft.NodeID(p))
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", *id), log.LstdFlags|log.Lmicroseconds)
	logger.Printf("starting %s", version.String())

	cfg := node.Config{
		ID:                         raft.NodeID(*id),
		Peers:                      peers,
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 *listenAddr,
		DataDir:                    *dataDir,
		ElectionTimeoutTicks:       10,
		ElectionTimeoutJitterTicks: 10,
		HeartbeatTimeoutTicks:      2,
		TickInterval:               25 * time.Millisecond,
		SnapshotThreshold:          *snapshotThreshold,
		Logger:                     logger,
	}

	n, err := node.Open(cfg)
	if err != nil {
		logger.Fatalf("opening node: %v", err)
	}

	srv := newControlServer(n, logger)
	httpSrv := &http.Server{Addr: *httpAddr, Handler: srv}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("control-plane HTTP server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-sigCh:
		logger.Printf("received shutdown signal")
	case <-n.Done():
		logger.Printf("node stopped itself: %v", n.Err())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	n.Stop()
}

// controlServer is the minimal local HTTP control plane an integration
// test uses to drive a real chronicledb-node process (see this file's
// package doc comment).
type controlServer struct {
	n      *node.Node
	logger *log.Logger
	mux    *http.ServeMux
}

func newControlServer(n *node.Node, logger *log.Logger) *controlServer {
	s := &controlServer{n: n, logger: logger, mux: http.NewServeMux()}
	s.mux.HandleFunc("/status", s.handleStatus)
	s.mux.HandleFunc("/propose", s.handlePropose)
	s.mux.HandleFunc("/outcome", s.handleOutcome)
	s.mux.HandleFunc("/fault", s.handleFault)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

// handleMetrics is Phase 9's minimal metrics endpoint (docs/roadmap.md
// §Optional metrics endpoint, docs/observability.md): a stable text
// exposition format (Prometheus' own text format, since it costs
// nothing beyond fmt.Fprintf and is scrapable by that ecosystem's
// tooling without pulling in a client library) over Node.Metrics() and
// Node.Status(). Every value here is read-only diagnostic state; no
// production code path ever depends on this endpoint being called
// (docs/roadmap.md §Observability: "never as a correctness
// dependency").
func (s *controlServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	st := s.n.Status()
	m := s.n.Metrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	line := func(name, help, typ string, value float64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, value)
	}
	line("chronicledb_raft_role", "current Raft role (0=Follower, 1=Candidate, 2=Leader)", "gauge", float64(st.Role))
	line("chronicledb_raft_term", "current Raft term", "gauge", float64(st.Term))
	line("chronicledb_raft_commit_index", "highest known committed Raft log index", "gauge", float64(st.CommitIndex))
	line("chronicledb_raft_applied_index", "highest index applied to the local state machine", "gauge", float64(st.AppliedIndex))
	line("chronicledb_raft_last_log_index", "highest index present in the local durable log", "gauge", float64(st.LastIndex))
	line("chronicledb_raft_snapshot_index", "last-included index of the most recent local snapshot boundary", "gauge", float64(st.SnapshotIndex))
	line("chronicledb_raft_elections_total", "elections this node's core has started", "counter", float64(m.ElectionsTotal))
	line("chronicledb_raft_leader_changes_total", "times this node has become leader", "counter", float64(m.LeaderChangesTotal))
	line("chronicledb_raft_messages_sent_total", "outbound Raft protocol messages", "counter", float64(m.RaftMessagesSentTotal))
	line("chronicledb_raft_messages_received_total", "inbound Raft protocol messages", "counter", float64(m.RaftMessagesReceivedTotal))
	line("chronicledb_proposals_total", "client mutations accepted as leader and handed to Raft", "counter", float64(m.ProposalsTotal))
	line("chronicledb_proposals_rejected_total", "proposals rejected outright because this node was not leader", "counter", float64(m.ProposalsRejectedTotal))
	line("chronicledb_proposals_committed_total", "accepted proposals whose terminal outcome was COMMITTED", "counter", float64(m.ProposalsCommittedTotal))
	line("chronicledb_proposals_aborted_total", "accepted proposals whose terminal outcome was ABORTED (a Snapshot Isolation conflict)", "counter", float64(m.ProposalsAbortedTotal))
	line("chronicledb_proposals_unknown_total", "accepted proposals that never reached a terminal outcome on this node (leadership lost, superseded, or node stopped)", "counter", float64(m.ProposalsUnknownTotal))
	line("chronicledb_requestid_duplicates_total", "Propose calls resolved as a known-RequestID retry without a fresh Raft round", "counter", float64(m.RequestIDDuplicatesTotal))
	line("chronicledb_snapshots_created_total", "local snapshots this node has created", "counter", float64(m.SnapshotsCreatedTotal))
	line("chronicledb_snapshots_installed_total", "peer snapshots this node has installed", "counter", float64(m.SnapshotsInstalledTotal))
}

// healthResponse is an honest, minimal health signal (docs/roadmap.md
// §Health): every boolean field is true unconditionally once this
// handler is reachable at all (a process that could not open its
// durable log or initialize Raft never gets this far — see main's
// node.Open call above) — this endpoint's value is in Role/Leader, not
// in those constants. It deliberately does NOT report a cluster-wide
// "quorum available" boolean: a Follower/Candidate cannot reliably know
// that, and a Leader only knows it as of its own last successful
// heartbeat round (which can be staler than "right now") — reporting
// either as a flat true/false would be the exact overclaim
// docs/roadmap.md's brief prohibits ("do not claim quorum availability
// if the implementation cannot reliably know it").
type healthResponse struct {
	Alive           bool   `json:"alive"`
	NodeStarted     bool   `json:"nodeStarted"`
	RaftInitialized bool   `json:"raftInitialized"`
	StorageOpened   bool   `json:"storageOpened"`
	Role            string `json:"role"`
	LeaderKnown     bool   `json:"leaderKnown"`
	Leader          string `json:"leader,omitempty"`
	Note            string `json:"note"`
}

func (s *controlServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	st := s.n.Status()
	resp := healthResponse{
		Alive:           true,
		NodeStarted:     true,
		RaftInitialized: true,
		StorageOpened:   true,
		Role:            st.Role.String(),
		LeaderKnown:     st.Leader != "",
		Leader:          string(st.Leader),
		Note:            "quorum availability is not reported: a Follower/Candidate cannot reliably know it, and a Leader only knows it as of its last successful heartbeat round",
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleFault is Phase 7's minimal real-process fault-injection hook
// (docs/roadmap.md Phase 7's "real network partition injection...
// controlled transport fault hooks only if consistent with the repo
// architecture"): it exposes internal/transport.Transport's
// Block/Unblock and directional BlockSend/BlockRecv/UnblockSend/
// UnblockRecv (this phase's own addition, for asymmetric partitions)
// over the same local control plane /propose and /status already use,
// so an integration test can inject and heal a real network partition
// between genuine OS processes — not just in-process
// (internal/node/chaos_test.go) or in the deterministic simulator
// (internal/fault/chaos_test.go).
func (s *controlServer) handleFault(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	action := r.URL.Query().Get("action")
	peer := raft.NodeID(r.URL.Query().Get("peer"))
	if peer == "" {
		http.Error(w, "peer is required", http.StatusBadRequest)
		return
	}
	tr := s.n.Transport()
	switch action {
	case "block":
		tr.Block(peer)
	case "unblock":
		tr.Unblock(peer)
	case "blocksend":
		tr.BlockSend(peer)
	case "unblocksend":
		tr.UnblockSend(peer)
	case "blockrecv":
		tr.BlockRecv(peer)
	case "unblockrecv":
		tr.UnblockRecv(peer)
	default:
		http.Error(w, "unknown action: "+action, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *controlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *controlServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.n.Status())
}

type mutationJSON struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Tombstone bool   `json:"tombstone"`
}

type proposeRequest struct {
	RequestID string         `json:"requestId"`
	TxnID     uint64         `json:"txnId"`
	StartSeq  uint64         `json:"startSeq"`
	Mutations []mutationJSON `json:"mutations"`
}

type proposeResponse struct {
	Status            string `json:"status"`
	CommitSeq         uint64 `json:"commitSeq,omitempty"`
	ConflictKey       string `json:"conflictKey,omitempty"`
	ConflictLatestSeq uint64 `json:"conflictLatestSeq,omitempty"`
	Error             string `json:"error,omitempty"`
	LeaderHint        string `json:"leaderHint,omitempty"`
}

func (s *controlServer) handlePropose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req proposeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, proposeResponse{Error: err.Error()})
		return
	}
	muts := make([]mvcc.Mutation, 0, len(req.Mutations))
	for _, m := range req.Mutations {
		muts = append(muts, mvcc.Mutation{Key: m.Key, Value: []byte(m.Value), Tombstone: m.Tombstone})
	}
	cmd := fsm.CommitTxnCommand{
		RequestID: fsm.RequestID(req.RequestID),
		TxnID:     req.TxnID,
		StartSeq:  req.StartSeq,
		Mutations: muts,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	outcome, err := s.n.Propose(ctx, cmd)
	if err != nil {
		var nle *node.NotLeaderError
		resp := proposeResponse{Error: err.Error()}
		if errors.As(err, &nle) {
			resp.LeaderHint = string(nle.Leader)
		}
		writeJSON(w, http.StatusConflict, resp)
		return
	}
	writeJSON(w, http.StatusOK, proposeResponse{
		Status:            outcome.Status.String(),
		CommitSeq:         outcome.CommitSeq,
		ConflictKey:       outcome.ConflictKey,
		ConflictLatestSeq: outcome.ConflictLatestSeq,
	})
}

func (s *controlServer) handleOutcome(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("requestId")
	outcome, ok := s.n.FSM().GetOutcome(fsm.RequestID(id))
	if !ok {
		writeJSON(w, http.StatusNotFound, proposeResponse{Error: "unknown RequestID"})
		return
	}
	writeJSON(w, http.StatusOK, proposeResponse{
		Status:            outcome.Status.String(),
		CommitSeq:         outcome.CommitSeq,
		ConflictKey:       outcome.ConflictKey,
		ConflictLatestSeq: outcome.ConflictLatestSeq,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
