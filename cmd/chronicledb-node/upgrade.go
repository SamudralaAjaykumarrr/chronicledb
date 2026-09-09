// This file implements the control-plane's Compatibility / Rolling
// Upgrades surface (docs/enterprise-v1-plan.md §7): the admin-gated
// /admin/upgrade/precheck (dry-run, read-only) and /admin/upgrade/finalize
// (admin-gated, audited, actually raises the cluster's agreed generation)
// HTTP endpoints, plus the -upgrade-precheck CLI flag main.go wires up
// as a standalone, no-datadir "dry run against the running cluster"
// invocation (docs/enterprise-v1-plan.md §7 "new CLI flag
// -upgrade-precheck (dry-run check against the running cluster)").
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
)

type precheckPeerJSON struct {
	Generation uint32 `json:"generation"`
	Known      bool   `json:"known"`
}

type precheckResponse struct {
	LocalClusterGeneration      uint32                      `json:"localClusterGeneration"`
	LocalMaxSupportedGeneration uint32                      `json:"localMaxSupportedGeneration"`
	TargetGeneration            uint32                      `json:"targetGeneration"`
	Peers                       map[string]precheckPeerJSON `json:"peers"`
	AlreadyFinalized            bool                        `json:"alreadyFinalized"`
	Ready                       bool                        `json:"ready"`
	Error                       string                      `json:"error,omitempty"`
}

func toPrecheckResponse(r node.PrecheckResult) precheckResponse {
	peers := make(map[string]precheckPeerJSON, len(r.Peers))
	for id, info := range r.Peers {
		peers[id] = precheckPeerJSON{Generation: info.Generation, Known: info.Known}
	}
	return precheckResponse{
		LocalClusterGeneration:      r.LocalClusterGeneration,
		LocalMaxSupportedGeneration: r.LocalMaxSupportedGeneration,
		TargetGeneration:            r.TargetGeneration,
		Peers:                       peers,
		AlreadyFinalized:            r.AlreadyFinalized,
		Ready:                       r.Ready,
	}
}

// handleUpgradePrecheck reports this node's current view of every
// configured peer's compatibility generation and whether a finalize
// would currently be expected to succeed (docs/enterprise-v1-plan.md
// §7). Read-only: never proposes anything.
func (s *controlServer) handleUpgradePrecheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	res, err := s.n.UpgradePrecheck(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, precheckResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, toPrecheckResponse(res))
}

type finalizeResponse struct {
	Status        string `json:"status"`
	NewGeneration uint32 `json:"newGeneration,omitempty"`
	Error         string `json:"error,omitempty"`
	LeaderHint    string `json:"leaderHint,omitempty"`
}

// handleUpgradeFinalize raises the cluster's agreed generation to this
// node's own max supported generation, once every configured peer
// reports support for it (docs/enterprise-v1-plan.md §7). Must be
// called against the current leader — mirrors /propose's
// NotLeaderError-with-hint behavior exactly.
func (s *controlServer) handleUpgradeFinalize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	outcome, err := s.n.FinalizeUpgrade(ctx)
	if err != nil {
		var nle *node.NotLeaderError
		resp := finalizeResponse{Status: "error", Error: err.Error()}
		if errors.As(err, &nle) {
			resp.LeaderHint = string(nle.Leader)
		}
		writeJSON(w, http.StatusConflict, resp)
		return
	}
	// NewGeneration is derived from version.MaxSupportedGeneration (a
	// compile-time constant), not from s.n.Status() read right after
	// FinalizeUpgrade returns: Node.run()'s event-loop goroutine only
	// calls refreshStatusLocked() AFTER handleControlPropose (and the
	// applyControlEntry inside it that signals FinalizeUpgrade's
	// resultCh) returns — there is no happens-before edge forcing that
	// refresh to complete before this HTTP handler goroutine, unblocked
	// by the resultCh send, gets to read Status(). A successful finalize
	// (Outcome.Status == StatusCommitted) always raises the cluster to
	// exactly this binary's own MaxSupportedGeneration by construction
	// (FinalizeUpgrade only ever proposes that target), so reading the
	// constant directly is both race-free and correct — no need to
	// observe the FSM's own state at all for this value.
	newGeneration := uint32(0)
	if outcome.Status == fsm.StatusCommitted {
		newGeneration = version.MaxSupportedGeneration
	}
	writeJSON(w, http.StatusOK, finalizeResponse{Status: outcome.Status.String(), NewGeneration: newGeneration})
}

// runUpgradePrecheck implements the -upgrade-precheck CLI flag: a
// standalone, no-datadir dry-run HTTP GET against an already-running
// node's /admin/upgrade/precheck (docs/enterprise-v1-plan.md §7 "dry-run
// check against the running cluster"). It never calls node.Open and
// never touches -datadir.
func runUpgradePrecheck(targetHTTPAddr string, insecure bool) (precheckResponse, error) {
	scheme := "https"
	if insecure {
		scheme = "http"
	}
	url := fmt.Sprintf("%s://%s/admin/upgrade/precheck", scheme, targetHTTPAddr)
	resp, err := http.Get(url)
	if err != nil {
		return precheckResponse{}, fmt.Errorf("requesting precheck from %s: %w", targetHTTPAddr, err)
	}
	defer resp.Body.Close()
	var out precheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return precheckResponse{}, fmt.Errorf("decoding precheck response from %s: %w", targetHTTPAddr, err)
	}
	if resp.StatusCode != http.StatusOK {
		if out.Error == "" {
			out.Error = fmt.Sprintf("precheck request to %s returned HTTP %d", targetHTTPAddr, resp.StatusCode)
		}
		return out, fmt.Errorf("%s", out.Error)
	}
	return out, nil
}
