//go:build integration

// This file is the "real multi-process/real-disk proof" evidence this
// phase's brief requires beyond internal/fault's deterministic
// simulator and internal/node's in-process (goroutine-based) tests: it
// spawns several actual chronicledb-node OS processes, each with its
// own persistent data directory and real TCP sockets, drives them via
// their local HTTP control plane, and kills/restarts processes with a
// real SIGKILL — not a simulated crash.
//
// It is gated behind the "integration" build tag (not run by a plain
// `go test ./...`) because it is slower and touches real ports/
// processes; invoke it explicitly:
//
//	go test -tags=integration ./cmd/chronicledb-node/... -v
//
// It uses bounded polling throughout, never an arbitrary sleep to
// "probably be done."
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// realNode wraps one real chronicledb-node OS process.
type realNode struct {
	id       string
	raftAddr string
	httpAddr string
	dataDir  string
	args     []string
	cmd      *exec.Cmd
}

func buildBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "chronicledb-node")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "."
	var stderr bytes.Buffer
	build.Stderr = &stderr
	if err := build.Run(); err != nil {
		t.Fatalf("building chronicledb-node: %v\n%s", err, stderr.String())
	}
	return bin
}

func newRealCluster(t *testing.T, bin string, n int) []*realNode {
	t.Helper()
	ids := make([]string, n)
	raftAddrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		raftAddrs[ids[i]] = freePort(t)
	}
	clusterFlag := strings.Join(ids, ",")

	nodes := make([]*realNode, n)
	for i, id := range ids {
		var peerParts []string
		for _, other := range ids {
			if other != id {
				peerParts = append(peerParts, other+"="+raftAddrs[other])
			}
		}
		nodes[i] = &realNode{
			id:       id,
			raftAddr: raftAddrs[id],
			httpAddr: freePort(t),
			dataDir:  t.TempDir(),
			args: []string{
				"-id=" + id,
				"-listen=" + raftAddrs[id],
				"-cluster=" + clusterFlag,
				"-peers=" + strings.Join(peerParts, ","),
			},
		}
	}
	for _, rn := range nodes {
		startRealNode(t, bin, rn)
	}
	t.Cleanup(func() {
		for _, rn := range nodes {
			stopRealNode(rn)
		}
	})
	return nodes
}

func startRealNode(t *testing.T, bin string, rn *realNode) {
	t.Helper()
	args := append([]string{}, rn.args...)
	args = append(args, "-http="+rn.httpAddr, "-datadir="+rn.dataDir)
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", rn.id, err)
	}
	rn.cmd = cmd
}

func stopRealNode(rn *realNode) {
	if rn.cmd == nil || rn.cmd.Process == nil {
		return
	}
	rn.cmd.Process.Signal(syscall.SIGKILL)
	rn.cmd.Wait()
}

// crash sends a real SIGKILL — an actual ungraceful process
// termination, not a clean exit.
func (rn *realNode) crash() {
	stopRealNode(rn)
	rn.cmd = nil
}

func (rn *realNode) restart(t *testing.T, bin string) {
	startRealNode(t, bin, rn)
}

type statusJSON struct {
	ID            string `json:"ID"`
	Role          int    `json:"Role"`
	Term          uint64 `json:"Term"`
	Leader        string `json:"Leader"`
	CommitIndex   uint64 `json:"CommitIndex"`
	AppliedIndex  uint64 `json:"AppliedIndex"`
	LastIndex     uint64 `json:"LastIndex"`
	SnapshotIndex uint64 `json:"SnapshotIndex"`
}

const roleLeader = 2 // raft.Leader's ordinal value (Follower=0, Candidate=1, Leader=2)

func (rn *realNode) status() (statusJSON, error) {
	resp, err := http.Get("http://" + rn.httpAddr + "/status")
	if err != nil {
		return statusJSON{}, err
	}
	defer resp.Body.Close()
	var s statusJSON
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return statusJSON{}, err
	}
	return s, nil
}

func awaitLeader(t *testing.T, nodes []*realNode, timeout time.Duration) *realNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *realNode
		count := 0
		for _, rn := range nodes {
			if rn.cmd == nil {
				continue
			}
			st, err := rn.status()
			if err != nil {
				continue
			}
			if st.Role == roleLeader {
				leader = rn
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no single leader emerged among real processes within timeout")
	return nil
}

func propose(rn *realNode, requestID, key, value string) (proposeResponse, int, error) {
	body, _ := json.Marshal(proposeRequest{
		RequestID: requestID,
		TxnID:     1,
		StartSeq:  0,
		Mutations: []mutationJSON{{Key: key, Value: value}},
	})
	resp, err := http.Post("http://"+rn.httpAddr+"/propose", "application/json", bytes.NewReader(body))
	if err != nil {
		return proposeResponse{}, 0, err
	}
	defer resp.Body.Close()
	var pr proposeResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return proposeResponse{}, resp.StatusCode, err
	}
	return pr, resp.StatusCode, nil
}

func awaitOutcomeCommitted(t *testing.T, rn *realNode, requestID string, timeout time.Duration) proposeResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + rn.httpAddr + "/outcome?requestId=" + requestID)
		if err == nil {
			var pr proposeResponse
			if json.NewDecoder(resp.Body).Decode(&pr) == nil && pr.Status == "committed" {
				resp.Body.Close()
				return pr
			}
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s never observed RequestID %s as committed within %s", rn.id, requestID, timeout)
	return proposeResponse{}
}

// TestRealProcesses_ElectionReplicationCrashRestartFailover is the
// central real-process integration proof this phase's brief requires:
// genuine OS processes, persistent directories, real TCP sockets,
// leader election, a replicated write, a real process kill, a restart,
// failover, and a RequestID retry surviving all of it.
func TestRealProcesses_ElectionReplicationCrashRestartFailover(t *testing.T) {
	bin := buildBinary(t)
	nodes := newRealCluster(t, bin, 3)

	leader := awaitLeader(t, nodes, 10*time.Second)
	t.Logf("leader elected: %s", leader.id)

	resp, status, err := propose(leader, "r1", "k1", "v1")
	if err != nil || status != http.StatusOK || resp.Status != "committed" {
		t.Fatalf("propose to real leader process: resp=%+v status=%d err=%v", resp, status, err)
	}

	for _, rn := range nodes {
		awaitOutcomeCommitted(t, rn, "r1", 10*time.Second)
	}

	// Real SIGKILL of the leader process.
	leader.crash()

	newLeader := awaitLeader(t, nodes, 10*time.Second)
	if newLeader.id == leader.id {
		t.Fatalf("expected a different leader after killing %s", leader.id)
	}

	// Retry the same RequestID against the new leader.
	retry, status, err := propose(newLeader, "r1", "k1", "v1")
	if err != nil || status != http.StatusOK || retry.Status != "committed" || retry.CommitSeq != resp.CommitSeq {
		t.Fatalf("retry against new leader process: retry=%+v status=%d err=%v, want CommitSeq=%d", retry, status, err, resp.CommitSeq)
	}

	// Restart the killed node and confirm it rejoins and catches up.
	leader.restart(t, bin)
	deadline := time.Now().Add(10 * time.Second)
	caught := false
	for time.Now().Before(deadline) {
		st, err := leader.status()
		if err == nil && st.AppliedIndex >= resp.CommitSeq {
			caught = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !caught {
		t.Fatalf("restarted process %s never caught up to CommitSeq %d", leader.id, resp.CommitSeq)
	}
}
