package sql

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/oracle"
)

// This file is Phase 10's SQL-layer model-based adversarial testing
// (docs/roadmap.md Phase 10 "SQL ADVERSARIAL TESTING",
// "MODEL-BASED TESTING"): a tiny, independent single-table model
// (schema existence + a primary-key -> row map, built from scratch,
// never touching internal/sql's own row/schema encoding) predicts the
// result of a deterministic history of CREATE TABLE / INSERT / UPDATE
// / DELETE / SELECT / RequestID-retry statements, and every statement's
// real outcome (including the exact documented error for a
// duplicate-primary-key INSERT or a missing-row UPDATE/DELETE — this
// SQL subset's explicit ErrRowNotFound deviation from a silent
// zero-rows-affected success, docs/sql.md §2.5) is checked against it.
// This does not re-prove durability/MVCC/Raft (ADR-0013's own scope
// boundary — those are internal/txn's and internal/node's job,
// including this phase's own internal/node/model_test.go); it proves
// SQL-visible state stays byte-for-byte consistent with an independent
// model of the documented statement semantics across a long randomized
// sequence, per SQL-CONSISTENCY (docs/invariants.md).

// sqlRowModel is the independent reference model: no primary key may
// repeat while live, a deleted key frees its primary key for reuse
// (docs/scenario-corpus.md SQ-5), and every live row's exact "v" column
// value is tracked for full-scan/point-lookup comparison.
type sqlRowModel struct {
	rows map[int64]string // pk -> v; absent means "no live row"
}

func newSQLRowModel() *sqlRowModel { return &sqlRowModel{rows: make(map[int64]string)} }

func (m *sqlRowModel) exists(pk int64) bool { _, ok := m.rows[pk]; return ok }

func (m *sqlRowModel) insert(pk int64, v string) { m.rows[pk] = v }
func (m *sqlRowModel) update(pk int64, v string) { m.rows[pk] = v }
func (m *sqlRowModel) delete(pk int64)           { delete(m.rows, pk) }

func (m *sqlRowModel) digest() string {
	keys := make([]string, 0, len(m.rows))
	get := make(map[string][]byte, len(m.rows))
	for pk, v := range m.rows {
		k := fmt.Sprintf("%d", pk)
		keys = append(keys, k)
		get[k] = []byte(v)
	}
	sort.Strings(keys)
	return oracle.CanonicalKVDigest(keys, func(k string) ([]byte, bool) {
		v, ok := get[k]
		return v, ok
	})
}

func sqlAdversarialSeeds(defaultN int) int {
	if v := os.Getenv("CHRONICLEDB_ADVERSARIAL_SEEDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultN
}

// TestModel_SQLAdversarialHistoryAgainstIndependentModel drives a
// single real standalone SQL engine through a long, deterministic,
// seeded history of statements, checking every one against
// sqlRowModel.
func TestModel_SQLAdversarialHistoryAgainstIndependentModel(t *testing.T) {
	seeds := sqlAdversarialSeeds(8)
	const steps = 60
	const keyspace = 8

	for seedI := 0; seedI < seeds; seedI++ {
		seed := int64(seedI)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSQLModelHistory(t, seed, steps, keyspace)
		})
	}
}

type sqlOpKind int

const (
	sqlOpInsert sqlOpKind = iota
	sqlOpUpdate
	sqlOpDelete
	sqlOpSelectPoint
	sqlOpSelectFull
	sqlOpRetry
	sqlOpNumKinds
)

func runSQLModelHistory(t *testing.T, seed int64, steps, keyspace int) {
	t.Helper()
	ctx := context.Background()
	s := newTestSession(t)
	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", fmt.Sprintf("create-seed%d", seed))

	model := newSQLRowModel()
	rec := oracle.NewRecorder(seed)
	sched := rand.New(rand.NewSource(seed ^ 0x59c1))

	type lastMutation struct {
		sqlText   string
		requestID string
		result    Result
	}
	var everMutated []lastMutation

	fail := func(format string, args ...interface{}) {
		t.Fatalf("%s\n\n%s", fmt.Sprintf(format, args...), rec.Tail(steps))
	}

	reqSeq := 0
	nextReqID := func() string {
		reqSeq++
		return fmt.Sprintf("sql-seed%d-req%d", seed, reqSeq)
	}

	for i := 0; i < steps; i++ {
		switch sqlOpKind(sched.Intn(int(sqlOpNumKinds))) {
		case sqlOpInsert:
			pk := int64(sched.Intn(keyspace))
			v := fmt.Sprintf("v-%d", i)
			sqlText := fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", pk, v)
			reqID := nextReqID()
			res, err := s.Execute(ctx, sqlText, reqID)
			wantOK := !model.exists(pk)
			rec.Record(oracle.Step{Node: "standalone", Op: "insert", RequestID: reqID, Args: sqlText, Outcome: fmt.Sprintf("err=%v", err)})
			if wantOK {
				if err != nil {
					fail("seed %d: INSERT of a fresh pk %d unexpectedly failed: %v (model says no live row exists for this pk)", seed, pk, err)
				}
				model.insert(pk, v)
				everMutated = append(everMutated, lastMutation{sqlText: sqlText, requestID: reqID, result: res})
			} else if !errors.Is(err, ErrDuplicatePrimaryKey) {
				fail("seed %d: INSERT of an already-live pk %d: err=%v, want ErrDuplicatePrimaryKey", seed, pk, err)
			}

		case sqlOpUpdate:
			pk := int64(sched.Intn(keyspace))
			v := fmt.Sprintf("upd-%d", i)
			sqlText := fmt.Sprintf("UPDATE t SET v = '%s' WHERE id = %d", v, pk)
			reqID := nextReqID()
			res, err := s.Execute(ctx, sqlText, reqID)
			wantOK := model.exists(pk)
			rec.Record(oracle.Step{Node: "standalone", Op: "update", RequestID: reqID, Args: sqlText, Outcome: fmt.Sprintf("err=%v", err)})
			if wantOK {
				if err != nil {
					fail("seed %d: UPDATE of live pk %d unexpectedly failed: %v", seed, pk, err)
				}
				model.update(pk, v)
				everMutated = append(everMutated, lastMutation{sqlText: sqlText, requestID: reqID, result: res})
			} else if !errors.Is(err, ErrRowNotFound) {
				fail("seed %d: UPDATE of a non-existent pk %d: err=%v, want ErrRowNotFound", seed, pk, err)
			}

		case sqlOpDelete:
			pk := int64(sched.Intn(keyspace))
			sqlText := fmt.Sprintf("DELETE FROM t WHERE id = %d", pk)
			reqID := nextReqID()
			res, err := s.Execute(ctx, sqlText, reqID)
			wantOK := model.exists(pk)
			rec.Record(oracle.Step{Node: "standalone", Op: "delete", RequestID: reqID, Args: sqlText, Outcome: fmt.Sprintf("err=%v", err)})
			if wantOK {
				if err != nil {
					fail("seed %d: DELETE of live pk %d unexpectedly failed: %v", seed, pk, err)
				}
				model.delete(pk)
				everMutated = append(everMutated, lastMutation{sqlText: sqlText, requestID: reqID, result: res})
			} else if !errors.Is(err, ErrRowNotFound) {
				fail("seed %d: DELETE of a non-existent pk %d: err=%v, want ErrRowNotFound", seed, pk, err)
			}

		case sqlOpSelectPoint:
			pk := int64(sched.Intn(keyspace))
			sqlText := fmt.Sprintf("SELECT id, v FROM t WHERE id = %d", pk)
			res, err := s.Execute(ctx, sqlText, nextReqID())
			if err != nil {
				fail("seed %d: SELECT point lookup failed: %v", seed, err)
			}
			rec.Record(oracle.Step{Node: "standalone", Op: "select-point", Args: sqlText, Outcome: fmt.Sprintf("rows=%d", len(res.Rows))})
			if wantV, ok := model.rows[pk]; ok {
				if len(res.Rows) != 1 || res.Rows[0][1].Text != wantV {
					fail("seed %d: SELECT id=%d got %+v, model says v=%q", seed, pk, res.Rows, wantV)
				}
			} else if len(res.Rows) != 0 {
				fail("seed %d: SELECT id=%d got %d rows, model says the row does not exist", seed, pk, len(res.Rows))
			}

		case sqlOpSelectFull:
			res, err := s.Execute(ctx, "SELECT id, v FROM t", nextReqID())
			if err != nil {
				fail("seed %d: full-scan SELECT failed: %v", seed, err)
			}
			rec.Record(oracle.Step{Node: "standalone", Op: "select-full", Outcome: fmt.Sprintf("rows=%d", len(res.Rows))})
			if len(res.Rows) != len(model.rows) {
				fail("seed %d: full-scan SELECT returned %d rows, model has %d live rows", seed, len(res.Rows), len(model.rows))
			}
			for _, row := range res.Rows {
				pk := row[0].Int
				wantV, ok := model.rows[pk]
				if !ok {
					fail("seed %d: full-scan SELECT returned pk=%d, which the model says does not exist", seed, pk)
				}
				if row[1].Text != wantV {
					fail("seed %d: full-scan SELECT pk=%d v=%q, model says v=%q", seed, pk, row[1].Text, wantV)
				}
			}

		case sqlOpRetry:
			if len(everMutated) == 0 {
				continue
			}
			prev := everMutated[sched.Intn(len(everMutated))]
			res, err := s.Execute(ctx, prev.sqlText, prev.requestID)
			rec.Record(oracle.Step{Node: "standalone", Op: "retry", RequestID: prev.requestID, Args: prev.sqlText, Outcome: fmt.Sprintf("err=%v", err)})
			if err != nil {
				fail("seed %d: retry of already-committed statement %q (RequestID %s) failed: %v", seed, prev.sqlText, prev.requestID, err)
			}
			if res.CommitSeq != prev.result.CommitSeq || res.RowsAffected != prev.result.RowsAffected {
				fail("seed %d: REQUEST-OUTCOME-STABILITY violated for RequestID %s: original %+v, retry %+v",
					seed, prev.requestID, prev.result, res)
			}
		}
	}

	// Final check: a fresh full-table scan must match the model exactly,
	// via the identical canonical digest function internal/node's model
	// test uses (byte-for-byte cross-package consistency, not just a
	// row-count spot check).
	res, err := s.Execute(ctx, "SELECT id, v FROM t", "final-scan")
	if err != nil {
		fail("seed %d: final SELECT failed: %v", seed, err)
	}
	gotKeys := make([]string, 0, len(res.Rows))
	gotVal := make(map[string][]byte, len(res.Rows))
	for _, row := range res.Rows {
		k := fmt.Sprintf("%d", row[0].Int)
		gotKeys = append(gotKeys, k)
		gotVal[k] = []byte(row[1].Text)
	}
	gotDigest := oracle.CanonicalKVDigest(gotKeys, func(k string) ([]byte, bool) {
		v, ok := gotVal[k]
		return v, ok
	})
	if gotDigest != model.digest() {
		fail("seed %d: final SQL-visible state digest %s != independent model digest %s", seed, gotDigest, model.digest())
	}
}
