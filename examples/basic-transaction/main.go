// Command basic-transaction is a runnable example showing
// ChronicleDB's real transaction engine directly, with no SQL and no
// Raft: internal/wal for durable storage, internal/mvcc for versioned
// state, and internal/txn.Manager for Snapshot Isolation transactions
// (see docs/transactions.md and docs/mvcc.md). This is the standalone
// (pre-Raft) mode docs/architecture.md §1 describes — the same engine
// a replicated internal/node.Node commits through, minus Raft.
//
// Run it:
//
//	go run ./examples/basic-transaction
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/txn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

func main() {
	dataDir, err := os.MkdirTemp("", "chronicledb-basic-transaction-*")
	if err != nil {
		log.Fatalf("creating temp data dir: %v", err)
	}
	defer os.RemoveAll(dataDir)
	fmt.Printf("durable log directory: %s\n\n", dataDir)

	w, _, err := wal.Open(dataDir, wal.Options{})
	if err != nil {
		log.Fatalf("opening WAL: %v", err)
	}
	defer w.Close()

	mgr, err := txn.NewManager(w, mvcc.NewStore())
	if err != nil {
		log.Fatalf("opening transaction manager: %v", err)
	}

	// Transaction 1: write two keys and commit atomically.
	t1 := mgr.Begin()
	if err := t1.Write("account:alice", []byte("100")); err != nil {
		log.Fatalf("write: %v", err)
	}
	if err := t1.Write("account:bob", []byte("50")); err != nil {
		log.Fatalf("write: %v", err)
	}
	commitSeq, err := t1.Commit(fsm.RequestID("example-txn-1"))
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
	fmt.Printf("committed txn 1 at CommitSeq=%d\n", commitSeq)

	// Transaction 2: a fresh snapshot sees committed data from txn 1.
	t2 := mgr.Begin()
	value, found, err := t2.Read("account:alice")
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	fmt.Printf("txn 2 reads account:alice = %q (found=%v)\n", value, found)
	if err := t2.Abort(); err != nil {
		log.Fatalf("abort: %v", err)
	}

	// Retrying the same RequestID against an already-committed
	// transaction returns the identical outcome instead of re-applying
	// it (docs/transactions.md §6-7) — this is the RequestID
	// idempotency guarantee, demonstrated with no network/Raft
	// involved at all.
	outcome, err := mgr.GetRequestOutcome(fsm.RequestID("example-txn-1"))
	if err != nil {
		log.Fatalf("GetRequestOutcome: %v", err)
	}
	fmt.Printf("RequestID \"example-txn-1\" outcome: status=%s commitSeq=%d\n",
		outcome.Status, outcome.CommitSeq)
}
