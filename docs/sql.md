# Constrained SQL

Status: Phase 8 (`internal/sql`) is implemented and tested. The SQL
surface below is deliberately small and complete as documented — this
is not a subset of a larger, partially-implemented grammar. See
[ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md) for why
SQL is scoped this way and sequenced after the transactional engine, and
[`docs/non-goals.md`](non-goals.md) §SQL surface for what remains
permanently deferred.

## 1. Data model: how tables map onto existing ChronicleDB state

SQL introduces **no second storage engine and no SQL-only database
state** (docs/architecture.md §5's dependency rule, ADR-0013). A table's
schema and every one of its rows are ordinary keys in the same
`internal/mvcc.Store` version-chain keyspace every other ChronicleDB
mutation already uses, mutated exclusively through
`internal/txn.Manager` (standalone mode) or `internal/node.Node`
(replicated mode) — the identical `CommitTxn` command path
(docs/transactions.md §3) proven in Phases 1-7.

### 1.1 Key namespace (`internal/sql/row.go`, `schema.go`)

Two disjoint key prefixes, using control bytes (`0x01`, `0x02`) that
can never appear in a table/column identifier (identifiers are
restricted to `[A-Za-z_][A-Za-z0-9_]*`, §4 below):

| Key | Layout | Holds |
|---|---|---|
| Schema key | `0x01` + table name | One table's encoded `Schema` (§1.3) |
| Row key | `0x02` + table name + `0x00` + encoded primary-key value | One row's encoded column values (§1.4) |

The `0x00` separator between table name and encoded primary key
guarantees one table's row-key prefix is never itself a prefix of a
different table's row keys (e.g. table `a` vs. table `ab`) — required
for `SELECT` without a predicate (§5.2) to scan exactly one table.

### 1.2 Primary-key value encoding

`encodePKValue` (`row.go`) prepends a one-byte type tag (`INTEGER`=1,
`TEXT`=2, `BOOLEAN`=3) before the value's own encoding (8-byte
big-endian for `INTEGER`, raw UTF-8 bytes for `TEXT`, one byte for
`BOOLEAN`). The type tag guarantees two primary-key values of
*different* declared types can never collide on the same encoded bytes,
even if their raw encodings would otherwise coincide.

### 1.3 Schema encoding

A table's `Schema` (`internal/sql/schema.go`) — table name, an ordered
list of `(name, type)` columns, and the index of the single primary-key
column — is encoded deterministically: a version byte, then each field
in a fixed layout, iterating `Columns` as the explicit ordered slice it
already is (never a map). This is the same determinism discipline
`internal/fsm.EncodeCommitTxn` already follows (docs/invariants.md
DETERMINISM BOUNDARY) — encoding a `Schema` never depends on Go's
unordered map iteration, so two replicas that commit the identical
`CREATE TABLE` produce byte-identical schema records.

### 1.4 Row encoding

A row's values (`internal/sql/row.go`) are encoded in the table's own
`Schema.Columns` order — fixed once at `CREATE TABLE` time — never in
whatever order an `INSERT`'s column list happened to name them. Each
value carries its own type tag, redundant with the schema but making a
corrupted or mismatched row fail closed with `ErrTypeMismatch` rather
than silently misinterpreting bytes under the wrong type.

### 1.5 Tombstones

A `DELETE` is an ordinary `internal/mvcc` tombstone (docs/mvcc.md §2) on
the row's key — no SQL-specific delete mechanism exists. Snapshot
Isolation's visibility rule (docs/mvcc.md §3) already treats a
tombstone as "does not exist as of this snapshot," so a deleted row
disappears from `SELECT` (point lookup or full scan) exactly the same
way any other ChronicleDB tombstoned key would.

### 1.6 No NULLs, no defaults

Every column must be given an explicit value on `INSERT` — there is no
`NULL` literal (rejected explicitly, `ErrUnsupportedFeature`, not
silently coerced) and no default-value mechanism. This is a
deliberate, permanent-for-V1 simplification, not an oversight: it keeps
the row encoding and every column's type simple (every column always
has a concrete typed value once a row exists at all).

## 2. Supported grammar

Exactly the statements below; anything else is `ErrUnsupportedStatement`
or `ErrSyntax`, never silently accepted (`internal/sql/parser.go`).

```
CreateTableStmt : CREATE TABLE name '(' ColumnDef (',' ColumnDef)* ')'
ColumnDef       : name ColumnType [ PRIMARY KEY ]
ColumnType      : INTEGER | TEXT | BOOLEAN

InsertStmt      : INSERT INTO name [ '(' name (',' name)* ')' ]
                    VALUES '(' Literal (',' Literal)* ')'

SelectStmt      : SELECT ( '*' | name (',' name)* ) FROM name [ WHERE Predicate ]

UpdateStmt      : UPDATE name SET Assignment (',' Assignment)* WHERE Predicate
Assignment      : name '=' Literal

DeleteStmt      : DELETE FROM name WHERE Predicate

Predicate       : name '=' Literal    -- name MUST be the table's primary-key column

Literal         : INTEGER-LITERAL | STRING-LITERAL | TRUE | FALSE

BeginStmt       : BEGIN
CommitStmt      : COMMIT
RollbackStmt    : ROLLBACK
```

Exactly one statement per `Parse` call, with an optional single
trailing `;`; a second statement or any other trailing input is a
syntax error — this subset never executes a batch of statements from
one string (docs/sql.md §6 explains why multi-statement work goes
through explicit `BEGIN`/`COMMIT`/`ROLLBACK` instead).

### 2.1 Types

Exactly three (`internal/sql/value.go`), matching
[`docs/non-goals.md`](non-goals.md) §SQL surface's "a limited type
system":

| SQL type | Go representation | Literal syntax |
|---|---|---|
| `INTEGER` | `int64` | optional `-`, digits, ≤ `MaxIntegerDigits` (32) |
| `TEXT` | `string` (UTF-8, no declared length limit beyond `MaxStringLiteralBytes`) | `'single-quoted'`, `''` doubles to escape a literal quote |
| `BOOLEAN` | `bool` | `TRUE` / `FALSE` (case-insensitive) |

No `FLOAT`/`DOUBLE`, no `TIMESTAMP`/`DATE` (docs/architecture.md §3:
`CommitSeq`/`StartSeq` are logical, never wall-clock — a `TIMESTAMP`
column inviting confusion with those is deliberately not offered), no
`BLOB` distinct from `TEXT`, no arrays/composite types.

### 2.2 `CREATE TABLE`

- At least one column, at most `MaxColumns` (64).
- Exactly one column must be `PRIMARY KEY`: zero is
  `ErrMissingPrimaryKey`, more than one is `ErrMultiplePrimaryKeys`.
- Duplicate column names (after identifier normalization, §4) are
  `ErrDuplicateColumn`.
- A table name that already has a recorded schema is
  `ErrDuplicateTable` — checked via a real `Read` against committed
  state (§5.1), not a local cache, so this is itself a validation of
  currently-committed cluster state, not a client-side guess.

### 2.3 `INSERT`

- An explicit column list is optional; when omitted, values are bound
  to every column in schema-declared order and there must be exactly as
  many values as columns.
- When given, the column list must name every column exactly once (no
  NULLs/defaults, §1.6) — an incomplete or duplicated list is
  `ErrUnsupportedFeature` / `ErrDuplicateColumn`.
- Every value's literal kind must match its target column's declared
  type (`ErrTypeMismatch`).
- A primary-key value that already has a visible row (as of the
  executing transaction's snapshot) is `ErrDuplicatePrimaryKey`.

### 2.4 Predicates: exactly one shape

`WHERE column = literal`, and `column` **must** be the table's primary
key. This is the *only* predicate shape this subset understands — no
other columns, no `<`/`>`/`!=`, no `AND`/`OR`, no subqueries. Anything
else is `ErrInvalidPredicate` (wrong column) or a syntax error (wrong
operator/shape). `SELECT` may omit the predicate entirely (full-table
scan, §5.2); `UPDATE` and `DELETE` require one.

### 2.5 `UPDATE` / `DELETE`

- `WHERE` is mandatory for both (the parser rejects its absence
  outright, `ErrInvalidPredicate` — there is no accidental unqualified
  "update/delete every row").
- `UPDATE`'s `SET` clause may not name the primary-key column
  (`ErrUnsupportedFeature`) — this subset has no row "rename"; delete
  and re-insert under a new key instead.
- A predicate that matches no visible row is `ErrRowNotFound` — an
  **explicit error**, not a silent zero-rows-affected success. This is
  a deliberate simplification (§8) that trades a small departure from
  typical SQL engine behavior for a simpler, more explicit contract
  consistent with this subset's error-reporting philosophy throughout.

## 3. Lexer / Parser / AST

`internal/sql/lexer.go` + `parser.go` + `ast.go`: a small, hand-written
lexer and recursive-descent parser — no parser-generator dependency
(docs/sql.md §Parser: "avoid a massive parser generator"). Every
production in §2's grammar corresponds to exactly one parser function.

- **Tokens**: identifiers (case-folded ASCII, §4), integers, single-
  quoted strings (`''` escapes a literal quote — the only supported
  escape), and the punctuation/operators the grammar needs (`( ) , ; = *`).
- **Never panics**: every token/byte access is bounds-checked; any
  malformed or oversized input (§7) returns a `*PositionError`
  (wrapping a sentinel from `errors.go`) carrying the byte offset of the
  failure, never a crash. Proven by `FuzzParse`/`FuzzDecodeSchema`/
  `FuzzDecodeRow` (§9) and by explicit malformed-input unit tests.
- **Explicit typed AST**: `CreateTableStmt`, `InsertStmt`, `SelectStmt`,
  `UpdateStmt`, `DeleteStmt`, `BeginStmt`, `CommitStmt`, `RollbackStmt`
  (`ast.go`) — execution never re-parses or otherwise touches raw SQL
  text; only the parser ever sees the source string.
- **Useful errors**: every error wraps one of `errors.go`'s sentinels
  (`ErrSyntax`, `ErrUnsupportedStatement`, `ErrInvalidIdentifier`, …), so
  a caller can `errors.Is` against a stable category while the message
  still names the specific token/position.

## 4. Identifiers

- Grammar: `[A-Za-z_][A-Za-z0-9_]*`, at most `MaxIdentifierBytes` (128)
  bytes.
- **Case-insensitive, ASCII-only fold** (`foldIdent`, `lexer.go`):
  `Users` and `USERS` name the same table. The fold is a manual
  byte-wise `'A'-'Z' -> 'a'-'z'` translation, deliberately not
  `strings.ToLower` or any locale-aware transform — this subset's
  identifier grammar is ASCII-only by construction, so there is nothing
  for a locale to disagree about (docs/invariants.md DETERMINISM
  BOUNDARY's spirit: no environment-dependent text transformation on a
  path that affects replicated state).
- **Keywords may never be used as names** — `expectName` in
  `parser.go` rejects any of this subset's ~20 reserved keywords as a
  table/column identifier (`ErrInvalidIdentifier`), even though the
  grammar could in principle disambiguate some of them by position.
  This keeps error messages and the grammar itself simple, at the cost
  of forbidding table/column names like `select` or `key`.
- Table and column names live in the *same* namespace as every other
  ChronicleDB key only through the fixed `0x01`/`0x02` prefixes (§1.1)
  — there is no risk of a SQL identifier colliding with a non-SQL key a
  future feature might introduce, as long as that feature avoids those
  two tag bytes.

## 5. Execution semantics

### 5.1 Execution path

```
SQL text
  -> Parse (lexer.go, parser.go)               typed Statement (AST)
  -> plan.go (binder / semantic validation)     resolved, type-checked plan
  -> exec.go, against a Txn (engine.go)          Read/Write/Delete calls
  -> Txn.Commit(requestID)
       standalone: internal/txn.Manager.commit   (docs/transactions.md §3-6)
       replicated: internal/node.Node.Propose    (docs/replication.md, docs/raft.md)
  -> internal/fsm.Apply                          deterministic conflict check + apply
  -> RequestID outcome recorded
  -> Result returned to the caller
```

`internal/sql` never imports `internal/mvcc` or `internal/storage`
directly, and never constructs a `CommitTxnCommand` itself outside of
`engine.go`'s two adapters — every mutation is a `Txn.Write`/`Delete`
call whose eventual commit is entirely internal/txn's or
internal/node's own already-proven mechanism (ADR-0013).

### 5.2 Reads: two execution shapes, honestly documented complexity

- **Primary-key lookup** (`WHERE pk = value`): one `Txn.Read` — a
  single `internal/mvcc.Store.Visible` call. Effectively O(1) (a Go map
  lookup plus a binary search over that one key's version chain).
- **Full-table scan** (no `WHERE`, or a projection-only `SELECT`):
  `Txn.ScanPrefix`, which is `internal/mvcc.Store.Export()` filtered
  down to the target table's row-key prefix — **a scan of every key the
  entire store currently holds**, not an indexed range scan. This
  subset has no secondary or even primary-key *index* structure beyond
  the row key itself; a full scan's cost is `O(total keys in the
  store)`, not `O(rows in this table)`. This is a deliberate,
  documented limitation for a "deliberately small... first-stage
  execution" (docs/sql.md's own brief), acceptable for the small test
  datasets this subset targets, and explicitly not a claim of scalable
  query performance (`docs/roadmap.md` Phase 9 is where real performance
  measurement/engineering happens, if ever pursued for SQL).
- A `SELECT`'s results, from either shape, always reflect the executing
  transaction's own local write set shadowing committed data first
  (docs/mvcc.md §3 step 1), then the committed version visible as of
  its `StartSeq` — the identical rule every other ChronicleDB read
  already follows, applied per-key by `Txn.Read` and merged
  deterministically (sorted by key) by `Txn.ScanPrefix`
  (`internal/sql/engine.go`'s `mergeScan`).

### 5.3 Consistency / isolation — exactly what the engine already proves, no more

- **Isolation level: Snapshot Isolation**, identical to every other
  ChronicleDB transaction (docs/mvcc.md §1). SQL does **not** claim
  SERIALIZABLE isolation (docs/invariants.md ISOLATION TRUTHFULNESS) —
  write skew is possible through the SQL frontend exactly as it is
  through the raw transaction API (docs/mvcc.md §1.1), demonstrated by
  `TestExecWriteSkewIsPossibleUnderSnapshotIsolation`
  (`internal/sql/exec_test.go`).
- **Read consistency, replicated mode**: every `Session.Begin` — for
  both a read-only `SELECT` and a mutating statement — establishes its
  `StartSeq` via `internal/node.Node.BeginReadIndex`
  (docs/replication.md §4, ADR-0010): a leader-only, quorum-
  acknowledgment-proven strong read. SQL never claims a stronger
  distributed-reads guarantee than that mechanism already proves, and
  never claims a follower can safely answer a SQL read on its own — it
  cannot, and this frontend never routes one there.
- **A completed `RequestID` never re-executes** (docs/transactions.md
  §6): a genuine retry of an auto-commit mutating statement, under the
  identical `RequestID`, returns its original recorded outcome directly
  (`Session.executeDML`'s idempotency short-circuit, `exec.go`) —
  without re-running the statement's own semantic validation (e.g.
  `INSERT`'s duplicate-primary-key check) against state that, by the
  time of a legitimate retry, already reflects the original attempt's
  own committed effect. See §8's note on the replicated-mode ordering
  subtlety this required.

### 5.4 Transactions

- **`BEGIN`/`COMMIT`/`ROLLBACK` are real, explicit SQL statements**
  (in scope per `docs/non-goals.md` §SQL surface and ADR-0013's initial
  surface, not deferred) — a `Session` (`exec.go`) tracks at most one
  open explicit transaction at a time. `BEGIN` while one is already
  open is `ErrTransactionAlreadyActive`; `COMMIT`/`ROLLBACK` with none
  open is `ErrNoActiveTransaction`.
- **Every statement between `BEGIN` and `COMMIT` accumulates into the
  same underlying `Txn`'s local write set** (docs/transactions.md §1) —
  `COMMIT` submits it as **one** deterministic `CommitTxn` command
  (docs/transactions.md §3), exactly matching how a hand-written
  `internal/txn.Txn` session already works. This is not a fake/
  simulated multi-statement transaction: it is the literal same
  mechanism, reached through SQL syntax instead of Go method calls.
- **A statement error inside an explicit transaction aborts the whole
  transaction**, not just the failing statement (`executeDML`) — this
  subset does not implement partial recovery from one failed statement
  within an otherwise-still-open transaction. Proven by
  `TestExecExplicitTransactionAbortsWholeTransactionOnStatementError`.
- **Without an explicit `BEGIN`, every statement is its own
  single-statement auto-commit transaction** — `Session.executeDML`
  begins, executes, and commits a fresh `Txn` for it. A read-only
  `SELECT` commits trivially (`internal/txn.Manager`'s and
  `internal/node`'s identical empty-mutation-set bypass,
  docs/transactions.md §9): nothing is durably appended, and no
  `RequestID` outcome is ever recorded for a pure read.
- **`RequestID` is a caller-supplied parameter to `Execute`**, never
  derived from SQL text and never generated by ChronicleDB itself
  (docs/architecture.md §3: `RequestID` is client-supplied by
  definition). It attaches to whichever statement actually triggers a
  commit: an implicit auto-commit statement, or an explicit `COMMIT`.
  It is ignored (never consulted) for a statement that does not trigger
  a commit — `BEGIN`, or any statement executed inside a still-open
  explicit transaction.

## 6. Errors

Every SQL-level error wraps exactly one sentinel from
`internal/sql/errors.go`:

| Sentinel | Meaning |
|---|---|
| `ErrSyntax` | Lexer/parser rejected malformed input |
| `ErrUnsupportedStatement` | Not one of §2's eight statement kinds |
| `ErrUnsupportedFeature` | Valid statement shape, unsupported detail (NULL, bad type, incomplete column list, …) |
| `ErrInvalidIdentifier` | Bad identifier grammar, or a reserved keyword used as a name |
| `ErrLimitExceeded` | A fixed limit from §7 was exceeded |
| `ErrUnknownTable` | No recorded schema for the named table |
| `ErrDuplicateTable` | `CREATE TABLE` named an already-existing table |
| `ErrUnknownColumn` | Column not present in the resolved schema |
| `ErrDuplicateColumn` | Same column named twice (schema declaration, or an `INSERT`/`UPDATE` column list) |
| `ErrTypeMismatch` | Literal's kind doesn't match its target column's declared type |
| `ErrMissingPrimaryKey` | `CREATE TABLE` declared zero `PRIMARY KEY` columns |
| `ErrMultiplePrimaryKeys` | `CREATE TABLE` declared more than one |
| `ErrDuplicatePrimaryKey` | `INSERT`'s primary-key value already has a visible row |
| `ErrInvalidPredicate` | `WHERE` outside the one supported shape (§2.4), or missing where mandatory |
| `ErrRowNotFound` | `UPDATE`/`DELETE` matched no visible row |
| `ErrNoActiveTransaction` / `ErrTransactionAlreadyActive` | `BEGIN`/`COMMIT`/`ROLLBACK` misuse |
| `ErrConflict` | Snapshot Isolation write-write conflict at commit (docs/mvcc.md §4) — the same first-committer-wins rule, surfaced through this frontend |

All of the above except `ErrConflict` and `ErrLimitExceeded`/
`ErrSyntax` (which are input-shape errors, not replicated-outcome
errors) affect only whether a statement is ever proposed at all — none
of them can differ between replicas for the identical committed
command, since schema/row validation reads and writes flow through the
same deterministic `Txn` every replica applies identically.

## 7. Limits (`internal/sql/limits.go`)

Applied before any unbounded allocation from untrusted SQL text, the
same bounded-decoding discipline `internal/fsm.DecodeCommitTxn` already
applies to durable command bytes (docs/failure-model.md §6):

| Limit | Value |
|---|---|
| Statement length | 64 KiB |
| Identifier length | 128 bytes |
| String literal length | 64 KiB |
| Columns per `CREATE TABLE` / values per `INSERT` / columns per `SELECT` or `UPDATE` | 64 |
| Integer literal digits | 32 |

Exceeding any of these is `ErrLimitExceeded` at the exact point of
failure (a `*PositionError` naming the byte offset), never a silent
truncation and never an unbounded allocation attempt first.

## 8. Explicitly unsupported (compatibility boundaries)

Permanently out of scope for this subset (`docs/non-goals.md` §SQL
surface, ADR-0013):

- Joins of any kind, subqueries, views, triggers, foreign keys, stored
  procedures.
- Any predicate shape beyond a single primary-key equality match — no
  `<`/`>`/`!=`/`LIKE`/`IN`, no `AND`/`OR`, no predicates on non-primary-
  key columns.
- Aggregation (`COUNT`/`SUM`/`GROUP BY`/…), sorting (`ORDER BY`),
  pagination (`LIMIT`/`OFFSET`).
- `NULL` values, column defaults, `ALTER TABLE`, `DROP TABLE`,
  secondary indexes.
- Any type beyond `INTEGER`/`TEXT`/`BOOLEAN` (no floats, timestamps,
  blobs, arrays).
- A cost-based optimizer of any kind — every supported statement has
  exactly one possible physical execution (§5.2), so there is nothing
  to optimize between.
- PostgreSQL wire-protocol compatibility, JDBC/ODBC, a SQL CLI, a web
  admin console — none of these were built (docs/roadmap.md Phase 8
  brief allowed a minimal CLI only if the roadmap explicitly placed one
  there; it does not, so this phase stayed a library, not a client
  tool).
- Deliberately non-standard, explicit deviations from typical SQL
  engine behavior, documented here rather than silently differing:
  `UPDATE`/`DELETE` on a missing row is an error (`ErrRowNotFound`),
  not a silent zero-rows-affected success (§2.5); every `INSERT` column
  must be given a value, with no `NULL`/default fallback (§1.6).

## 9. Testing and fuzzing

`internal/sql`'s test suite (see `docs/testing-strategy.md` and
`docs/scenario-corpus.md` §SQL for the full accounting) covers, per
`docs/sql.md`'s own testing brief:

- Parser/lexer: valid statements of every kind, malformed syntax, bad
  literals, invalid/keyword identifiers, every fixed limit
  (`parser_test.go`, `lexer_test.go`).
- Schema/row encode-decode round-trips and bounded-decode-never-panics
  properties (`schema_test.go`, `row_test.go`).
- `CREATE TABLE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE` semantics,
  including every explicitly documented error case above, `RequestID`
  retry-safety, and explicit-transaction commit/rollback/abort-on-error
  behavior, against a real `internal/wal`-backed standalone engine
  (`exec_test.go`, `engine_test.go`).
- Restart/recovery: schema and rows (including rolled-back work leaving
  no trace) survive a real close-and-reopen of the same on-disk WAL
  directory (`restart_test.go`).
- Distributed evidence: a real three-node `internal/node` cluster over
  real TCP/disk proving SQL `INSERT` → Raft commit → replicated state →
  `SELECT` returns the identical row on every node; `RequestID` retry
  against a newly elected leader after the original leader crashes;
  and SQL-visible state surviving a real
  `internal/snapshot`-create-and-install/compaction cycle plus a
  follower crash/restart (`distributed_test.go`).
- Fuzzing: `FuzzParse`, `FuzzDecodeSchema`, `FuzzDecodeRow`
  (`fuzz_test.go`) — arbitrary bytes into the parser and both durable
  decoders, proving no panic and no over-claimed decoded length,
  mirroring `internal/fsm.FuzzDecodeCommitTxn`'s existing discipline.

## 10. A liveness fix this phase's testing found in Phase 5

Building the real-cluster distributed tests above (§9) exposed a
genuine, previously-undiscovered liveness gap in
`internal/node.Node.BeginReadIndex` — not a new SQL-layer bug, but a
Phase 5 mechanism this phase's testing happened to newly exercise (a
`SELECT`/`INSERT`'s `Session.Begin` calls `BeginReadIndex` for *every*
statement, including the first one after a leader failover). See
[`docs/replication.md`](replication.md) §4.3 for the fix and why it
belongs in `internal/node` (the driver), not `internal/raft.Core`
(whose own "no no-op entry on election" decision, `docs/raft.md`, is
unchanged).
