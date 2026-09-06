// Command sql-basics is a runnable example of ChronicleDB's
// constrained SQL frontend (internal/sql, see docs/sql.md) against a
// real, WAL-backed standalone engine — no Raft, no network. It
// exercises exactly the supported grammar: CREATE TABLE, INSERT,
// SELECT, UPDATE, DELETE, and an explicit BEGIN/COMMIT. There is no
// SQL CLI or wire protocol (docs/sql.md §8) — this is the actual,
// currently-available way to run SQL against ChronicleDB: as a Go
// library, one internal/sql.Session per connection.
//
// Run it:
//
//	go run ./examples/sql-basics
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/sql"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/txn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

func main() {
	dataDir, err := os.MkdirTemp("", "chronicledb-sql-basics-*")
	if err != nil {
		log.Fatalf("creating temp data dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	w, _, err := wal.Open(dataDir, wal.Options{})
	if err != nil {
		log.Fatalf("opening WAL: %v", err)
	}
	defer w.Close()

	mgr, err := txn.NewManager(w, mvcc.NewStore())
	if err != nil {
		log.Fatalf("opening transaction manager: %v", err)
	}

	engine := sql.NewStandaloneEngine(mgr)
	session := sql.NewSession(engine)
	ctx := context.Background()

	run := func(requestID, stmt string) sql.Result {
		res, err := session.Execute(ctx, stmt, requestID)
		if err != nil {
			log.Fatalf("executing %q: %v", stmt, err)
		}
		return res
	}

	run("req-1", "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER)")
	fmt.Println("created table accounts")

	run("req-2", "INSERT INTO accounts (id, name, balance) VALUES (1, 'alice', 100)")
	run("req-3", "INSERT INTO accounts (id, name, balance) VALUES (2, 'bob', 50)")
	fmt.Println("inserted 2 rows")

	res := run("req-4", "SELECT id, name, balance FROM accounts WHERE id = 1")
	fmt.Printf("SELECT WHERE id = 1: columns=%v rows=%v\n", res.Columns, res.Rows)

	run("req-5", "UPDATE accounts SET balance = 90 WHERE id = 1")
	res = run("req-6", "SELECT balance FROM accounts WHERE id = 1")
	fmt.Printf("after UPDATE, balance = %v\n", res.Rows[0][0])

	// An explicit multi-statement transaction: SQL execution here
	// flows through the identical internal/txn.Manager commit path as
	// the basic-transaction example — never a separate SQL-only
	// storage path (ADR-0013).
	run("req-7", "BEGIN")
	run("req-8", "UPDATE accounts SET balance = 40 WHERE id = 2")
	commitRes := run("req-9", "COMMIT")
	fmt.Printf("explicit transaction committed at CommitSeq=%d\n", commitRes.CommitSeq)

	res = run("req-10", "SELECT id, name, balance FROM accounts")
	fmt.Println("final table state:")
	for _, row := range res.Rows {
		fmt.Printf("  %v\n", row)
	}
}
