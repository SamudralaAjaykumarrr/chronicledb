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
	"crypto/tls"
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

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/authz"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/identity"
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

		// Security Foundation flags (docs/enterprise-v1-plan.md §5).
		tlsCertFile     = flag.String("tls-cert", "", "control-plane HTTP TLS certificate file (enables client TLS when set)")
		tlsKeyFile      = flag.String("tls-key", "", "control-plane HTTP TLS private key file")
		tlsCAFile       = flag.String("tls-ca", "", "CA bundle used to verify client certificates presented to the control-plane HTTP server (optional unless -auth-mode=mtls)")
		peerTLSCertFile = flag.String("peer-tls-cert", "", "peer Raft transport mTLS certificate file (enables peer mTLS when set, together with -peer-tls-key/-peer-tls-ca)")
		peerTLSKeyFile  = flag.String("peer-tls-key", "", "peer Raft transport mTLS private key file")
		peerTLSCAFile   = flag.String("peer-tls-ca", "", "CA bundle trusted for peer mTLS")
		authModeFlag    = flag.String("auth-mode", string(authModeNone), `client authentication mode: "none" (default; matches pre-Security-Foundation behavior), "token", or "mtls"`)
		authTokenFile   = flag.String("auth-token-file", "", `bearer token file for -auth-mode=token, lines of "<token>:<principal>"`)
		rbacMappingFile = flag.String("rbac-mapping-file", "", "JSON file mapping principal name to role (admin|operator|read-only); required when -auth-mode is not none")
		auditLogDir     = flag.String("audit-log-dir", "", "directory for the hash-chained administrative audit log (default: <datadir>/audit)")
		enableFault     = flag.Bool("enable-fault-endpoint", false, "register the /fault fault-injection endpoint (off by default; also requires admin auth when -auth-mode is not none)")

		// Backup / Disaster Recovery / PITR flags (docs/enterprise-v1-plan.md
		// §6). Restore is a startup-only preflight: it runs, and must
		// complete, entirely before node.Open ever touches -datadir (see
		// the restore preflight block below and DESTRUCTIVE RESTORE
		// ISOLATION in docs/backup.md).
		restoreFrom    = flag.String("restore-from", "", "restore -datadir from the backup at this directory before starting (must be empty/absent unless -force-overwrite is also set); the process exits after a successful restore is not required — normal startup continues against the now-restored data directory")
		restoreUntil   = flag.String("restore-until", "", "PITR boundary log index to restore up to (empty = everything the backup includes); only meaningful with -restore-from")
		forceOverwrite = flag.Bool("force-overwrite", false, "required in addition to -restore-from to restore over a -datadir that already contains WAL/snapshot state (DESTRUCTIVE RESTORE ISOLATION: this destroys that existing state)")
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

	secFlags := securityFlags{
		tlsCertFile:     *tlsCertFile,
		tlsKeyFile:      *tlsKeyFile,
		tlsCAFile:       *tlsCAFile,
		peerTLSCertFile: *peerTLSCertFile,
		peerTLSKeyFile:  *peerTLSKeyFile,
		peerTLSCAFile:   *peerTLSCAFile,
		authModeFlag:    *authModeFlag,
		authTokenFile:   *authTokenFile,
		rbacMappingFile: *rbacMappingFile,
		auditLogDir:     *auditLogDir,
		enableFault:     *enableFault,
	}
	if secFlags.auditLogDir == "" {
		secFlags.auditLogDir = audit.Path(*dataDir)
	}
	authModeValue, err := validateSecurityFlags(secFlags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "chronicledb-node: invalid security configuration: %v\n", err)
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

	// Restore preflight (docs/enterprise-v1-plan.md §6): must complete,
	// successfully, entirely before node.Open ever opens -datadir —
	// restore only ever targets a not-yet-opened data directory, never a
	// live node's own in-use one.
	if *restoreFrom != "" {
		res, err := runRestore(*restoreFrom, *dataDir, *restoreUntil, *forceOverwrite)
		if err != nil {
			logger.Fatalf("restore from %s into %s failed: %v", *restoreFrom, *dataDir, err)
		}
		logger.Printf("restored %s from backup %s: manifest range [%d,%d], restored up to index %d (force-overwrite=%v)",
			*dataDir, *restoreFrom, res.Manifest.LastIncludedIndex, res.Manifest.WALUntilIndex, res.RestoredUntilIndex, *forceOverwrite)
		if err := recordRestoreAudit(secFlags.auditLogDir, *restoreFrom, *dataDir, *forceOverwrite, res); err != nil {
			if *forceOverwrite {
				// DESTRUCTIVE RESTORE ISOLATION: a forced restore that
				// destroyed pre-existing state must not proceed without
				// its required audit record — fail closed exactly like
				// the HTTP admin middleware chain does for every other
				// administrative action (AUDIT COMPLETENESS).
				logger.Fatalf("forced restore succeeded but its required audit record could not be written: %v", err)
			}
			logger.Printf("warning: restore audit record could not be written (restore itself succeeded, target was already clean): %v", err)
		}
	}

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
		PeerTLSCertFile:            secFlags.peerTLSCertFile,
		PeerTLSKeyFile:             secFlags.peerTLSKeyFile,
		PeerTLSCAFile:              secFlags.peerTLSCAFile,
	}

	n, err := node.Open(cfg)
	if err != nil {
		logger.Fatalf("opening node: %v", err)
	}

	sec, err := newSecurity(secFlags, authModeValue, logger)
	if err != nil {
		n.Stop()
		logger.Fatalf("initializing security (auth/RBAC/audit): %v", err)
	}
	defer sec.Close()

	var clientTLSHolder *identity.Holder
	insecure := secFlags.tlsCertFile == "" || authModeValue == authModeNone
	if secFlags.tlsCertFile != "" {
		clientTLSHolder, err = identity.NewHolder(secFlags.tlsCertFile, secFlags.tlsKeyFile, secFlags.tlsCAFile)
		if err != nil {
			n.Stop()
			logger.Fatalf("loading control-plane TLS material: %v", err)
		}
	}

	srv := newControlServer(n, logger, sec, clientTLSHolder, secFlags.enableFault, *allFlag)
	httpSrv := &http.Server{Addr: *httpAddr, Handler: srv}

	if clientTLSHolder != nil {
		clientAuth := tls.NoClientCert
		switch {
		case authModeValue == authModeMTLS:
			clientAuth = tls.RequireAndVerifyClientCert
		case secFlags.tlsCAFile != "":
			clientAuth = tls.VerifyClientCertIfGiven
		}
		httpSrv.TLSConfig = buildTLSConfig(clientTLSHolder, clientAuth)
		go func() {
			if err := httpSrv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("control-plane HTTPS server: %v", err)
			}
		}()
	} else {
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("control-plane HTTP server: %v", err)
			}
		}()
	}

	if insecure {
		logInsecureWarning(logger)
		go warnInsecurePeriodically(logger, n.Done())
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

shutdownLoop:
	for {
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				// docs/enterprise-v1-plan.md §5 layer 7: certificate
				// rotation triggered by SIGHUP, without a restart.
				if err := reloadTLS(n, clientTLSHolder, logger); err != nil {
					logger.Printf("SIGHUP: TLS reload failed: %v", err)
				}
				continue
			}
			logger.Printf("received shutdown signal")
			break shutdownLoop
		case <-n.Done():
			logger.Printf("node stopped itself: %v", n.Err())
			break shutdownLoop
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	n.Stop()
}

// logInsecureWarning prints the loud, repeated warning
// docs/enterprise-v1-plan.md §5's Compatibility implications require:
// "startup prints a loud, repeated warning when running without
// TLS/auth."
func logInsecureWarning(logger *log.Logger) {
	logger.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	logger.Printf("!! WARNING: this node is running WITHOUT TLS and/or WITHOUT authentication.   !!")
	logger.Printf("!! Anyone who can reach its ports can read/write cluster state. This is a     !!")
	logger.Printf("!! required migration, not a supported permanent configuration — see          !!")
	logger.Printf("!! docs/security.md. Set -tls-cert/-tls-key and -auth-mode=token|mtls.         !!")
	logger.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
}

func warnInsecurePeriodically(logger *log.Logger, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			logInsecureWarning(logger)
		case <-done:
			return
		}
	}
}

// controlServer is the minimal local HTTP control plane an integration
// test uses to drive a real chronicledb-node process (see this file's
// package doc comment).
type controlServer struct {
	n               *node.Node
	logger          *log.Logger
	mux             *http.ServeMux
	sec             *security
	clientTLSHolder *identity.Holder
	// clusterID is recorded, diagnostically only, in every backup this
	// node's /admin/backup produces (docs/enterprise-v1-plan.md §6's
	// manifest "cluster ID" field) — see backup.Manifest.ClusterID's doc
	// comment for why it is never validated by Restore.
	clusterID string
}

// newControlServer wires every route through the Security Foundation
// middleware chain (security.wrap — a no-op when sec is nil/auth-mode
// none) and registers /fault only when enableFault is true
// (docs/enterprise-v1-plan.md §5 layer 8, FAULT SURFACE OFF BY DEFAULT:
// "the flag's absence must make the handler structurally unreachable
// (registered conditionally, not just checked-and-rejected)").
func newControlServer(n *node.Node, logger *log.Logger, sec *security, clientTLSHolder *identity.Holder, enableFault bool, clusterID string) *controlServer {
	s := &controlServer{n: n, logger: logger, mux: http.NewServeMux(), sec: sec, clientTLSHolder: clientTLSHolder, clusterID: clusterID}
	s.mux.HandleFunc("/status", sec.wrap(authz.EndpointStatus, s.handleStatus))
	s.mux.HandleFunc("/propose", sec.wrap(authz.EndpointPropose, s.handlePropose))
	s.mux.HandleFunc("/outcome", sec.wrap(authz.EndpointOutcome, s.handleOutcome))
	s.mux.HandleFunc("/metrics", sec.wrap(authz.EndpointMetrics, s.handleMetrics))
	s.mux.HandleFunc("/health", sec.wrap(authz.EndpointHealth, s.handleHealth))
	s.mux.HandleFunc("/admin/reload-tls", sec.wrap(authz.EndpointReloadTLS, s.handleReloadTLS))
	s.mux.HandleFunc("/admin/backup", sec.wrap(authz.EndpointBackup, s.handleBackup))
	if enableFault {
		s.mux.HandleFunc("/fault", sec.wrap(authz.EndpointFault, s.handleFault))
	}
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

	// Security Foundation metrics (docs/enterprise-v1-plan.md §5
	// Observability: "auth success/failure counts (no credential value
	// in any label)... certificate-expiry-remaining gauge per
	// configured certificate, audit-write-failure counter").
	if s.sec != nil {
		line("chronicledb_auth_success_total", "authentication checks that succeeded", "counter", float64(s.sec.authSuccessTotal.Load()))
		line("chronicledb_auth_failure_total", "authentication checks that failed", "counter", float64(s.sec.authFailureTotal.Load()))
		line("chronicledb_audit_write_failures_total", "audit log append calls that failed", "counter", float64(s.sec.auditWriteFailuresTotal.Load()))
	}
	if m, ok := s.n.PeerTLSMaterial(); ok {
		remaining := time.Until(m.Leaf.NotAfter).Seconds()
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s{cert=\"peer\"} %v\n",
			"chronicledb_cert_expiry_seconds", "seconds until this certificate's NotAfter (negative if already expired)",
			"chronicledb_cert_expiry_seconds", "gauge", "chronicledb_cert_expiry_seconds", remaining)
	}
	if s.clientTLSHolder != nil {
		remaining := time.Until(s.clientTLSHolder.Current().Leaf.NotAfter).Seconds()
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s{cert=\"client\"} %v\n",
			"chronicledb_cert_expiry_seconds", "seconds until this certificate's NotAfter (negative if already expired)",
			"chronicledb_cert_expiry_seconds", "gauge", "chronicledb_cert_expiry_seconds", remaining)
	}
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
