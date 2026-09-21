package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"testing"
)

// This file is TestAdmissionNeverReachableFromEventLoop
// (docs/v0.6.0-plan.md §4.2 Rule CP-2, §4.2/§3.2a Rule CP-3): a
// structural test, using only go/parser+go/ast (both standard library —
// docs/dependencies.md's zero-external-dependency policy is untouched),
// of the same class as the existing TestControlKindRangesNeverCollide —
// a property the code cannot silently lose to a future refactor,
// checked by the compiler-adjacent tooling rather than by review
// discipline.
//
// Scoping note: Rule CP-1/CP-2 is about *gate acquisition* — "internal/
// node must contain exactly one gate acquisition per entry point... in
// a method that runs on a caller's goroutine... never in a method
// reachable from run()" — not about every identifier in
// internal/admission. §5.3/§9.1a's own event-loop ceiling checks
// (handlePropose/handleReadIndex, both reachable from run()) construct
// plain admission.RejectedError values to express a ceiling rejection,
// which is correct and intended: constructing an error value has no
// blocking/resource semantics. What must never be reachable from run()
// is the actual blocking mechanism — admission.Gate.Acquire (and the
// acquireSingleSlot helper that wraps it), plus gate/monitor
// construction. This test is scoped precisely to that, which is what
// "gate acquisition" in Rule CP-1's own wording means.

// buildNodePackageAST parses every non-test .go file in this package
// directory.
func buildNodePackageAST(t *testing.T) (*token.FileSet, []*ast.File) {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("globbing package files: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	found := false
	for _, p := range paths {
		if len(p) >= 8 && p[len(p)-8:] == "_test.go" {
			continue
		}
		found = true
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", p, err)
		}
		files = append(files, f)
	}
	if !found {
		t.Fatal("no non-test .go files found in internal/node — glob pattern broken?")
	}
	return fset, files
}

// funcKey identifies a function or method declaration: "Node.run" for
// a method with receiver type *Node, or the bare name for a
// package-level function.
type funcKey = string

// nodeFuncTable indexes every FuncDecl in files by funcKey, and records
// each *Node method's receiver variable name (needed to recognize
// n.foo(...) as a call to method "foo" on the receiver, whatever the
// receiver happens to be named — this codebase always uses "n", but the
// test does not hardcode that assumption).
type nodeFuncTable struct {
	decls    map[funcKey]*ast.FuncDecl
	receiver map[funcKey]string // funcKey -> receiver variable name, methods only
}

func indexNodeFuncs(files []*ast.File) *nodeFuncTable {
	tbl := &nodeFuncTable{decls: map[funcKey]*ast.FuncDecl{}, receiver: map[funcKey]string{}}
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Recv == nil {
				tbl.decls[fn.Name.Name] = fn
				continue
			}
			if len(fn.Recv.List) != 1 {
				continue
			}
			recvType := fn.Recv.List[0].Type
			star, ok := recvType.(*ast.StarExpr)
			if !ok {
				continue
			}
			ident, ok := star.X.(*ast.Ident)
			if !ok || ident.Name != "Node" {
				continue // this test only follows the call graph across *Node methods
			}
			key := "Node." + fn.Name.Name
			tbl.decls[key] = fn
			if len(fn.Recv.List[0].Names) == 1 {
				tbl.receiver[key] = fn.Recv.List[0].Names[0].Name
			}
		}
	}
	return tbl
}

// calleesOf returns every funcKey key's body calls, as best-effort
// resolved against tbl: a bare identifier call to another indexed
// package-level function, or a selector call `<receiver>.Method(...)`
// where <receiver> is key's own receiver variable and "Node.Method" is
// indexed.
func (tbl *nodeFuncTable) calleesOf(key funcKey) []funcKey {
	fn := tbl.decls[key]
	if fn == nil || fn.Body == nil {
		return nil
	}
	recvName := tbl.receiver[key]
	var out []funcKey
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			if _, ok := tbl.decls[fun.Name]; ok {
				out = append(out, fun.Name)
			}
		case *ast.SelectorExpr:
			if recvName == "" {
				return true
			}
			if x, ok := fun.X.(*ast.Ident); ok && x.Name == recvName {
				candidate := "Node." + fun.Sel.Name
				if _, ok := tbl.decls[candidate]; ok {
					out = append(out, candidate)
				}
			}
		}
		return true
	})
	return out
}

// reachableFrom computes the transitive closure of calleesOf starting
// at root (root itself included).
func (tbl *nodeFuncTable) reachableFrom(root funcKey) map[funcKey]bool {
	seen := map[funcKey]bool{root: true}
	queue := []funcKey{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, callee := range tbl.calleesOf(cur) {
			if !seen[callee] {
				seen[callee] = true
				queue = append(queue, callee)
			}
		}
	}
	return seen
}

// gateAcquisitionCalls reports every gate-acquisition call site
// (Rule CP-1/CP-2's actual subject — see this file's header comment)
// found anywhere in key's own body: a call whose target is ".Acquire",
// "acquireSingleSlot", "newAdmissionGates", "admission.NewGate", or
// "admission.NewPressureMonitor".
func gateAcquisitionCalls(tbl *nodeFuncTable, key funcKey) []string {
	fn := tbl.decls[key]
	if fn == nil || fn.Body == nil {
		return nil
	}
	var hits []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			if fun.Name == "acquireSingleSlot" || fun.Name == "newAdmissionGates" {
				hits = append(hits, fun.Name)
			}
		case *ast.SelectorExpr:
			if fun.Sel.Name == "Acquire" {
				hits = append(hits, "*.Acquire")
			}
			if pkg, ok := fun.X.(*ast.Ident); ok && pkg.Name == "admission" &&
				(fun.Sel.Name == "NewGate" || fun.Sel.Name == "NewPressureMonitor") {
				hits = append(hits, "admission."+fun.Sel.Name)
			}
		}
		return true
	})
	return hits
}

// laneGateFieldSelector reports whether n is a selector chain of the
// shape `<expr>.admission.<field>` for the given field name (e.g.
// "control", "maintenance", "backupSlot", "scrubSlot") — the pattern
// every gate acquisition in this package uses
// (n.admission.control.Acquire(...), etc.).
func laneGateFieldSelector(n ast.Node, field string) bool {
	sel, ok := n.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != field {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return inner.Sel.Name == "admission"
}

// referencesLaneField reports whether key's own body contains a
// selector chain matching `.admission.<field>` anywhere.
func referencesLaneField(tbl *nodeFuncTable, key funcKey, field string) bool {
	fn := tbl.decls[key]
	if fn == nil || fn.Body == nil {
		return false
	}
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if n != nil && laneGateFieldSelector(n, field) {
			found = true
		}
		return true
	})
	return found
}

// TestAdmissionNeverReachableFromEventLoop is docs/v0.6.0-plan.md's
// AC-18: Rule CP-2 (no gate acquisition reachable from (*Node).run) and
// Rule CP-3 (Lane A1's control gate and Lane A2's maintenance/
// backupSlot/scrubSlot gates are mutually unreachable across their own
// entry points).
func TestAdmissionNeverReachableFromEventLoop(t *testing.T) {
	_, files := buildNodePackageAST(t)
	tbl := indexNodeFuncs(files)

	if _, ok := tbl.decls["Node.run"]; !ok {
		t.Fatal("Node.run not found — this test's premise (a call graph rooted at (*Node).run) does not hold; check buildNodePackageAST/indexNodeFuncs against a node.go rename")
	}

	t.Run("CP-2_no_gate_acquisition_reachable_from_run", func(t *testing.T) {
		reachable := tbl.reachableFrom("Node.run")
		var offenders []string
		for key := range reachable {
			if hits := gateAcquisitionCalls(tbl, key); len(hits) > 0 {
				offenders = append(offenders, key)
			}
		}
		sort.Strings(offenders)
		if len(offenders) > 0 {
			t.Fatalf("gate acquisition reachable from (*Node).run via: %v — Rule CP-1/CP-2 requires every admission.Gate.Acquire (and acquireSingleSlot/newAdmissionGates/admission.NewGate/admission.NewPressureMonitor) call to happen only in a method that runs on a caller's goroutine, never in anything run() can reach", offenders)
		}
	})

	t.Run("CP-3_backup_never_reaches_control_gate", func(t *testing.T) {
		for _, root := range []funcKey{"Node.Backup"} { // Node.Scrub joins this list once slice 12 adds it
			reachable := tbl.reachableFrom(root)
			for key := range reachable {
				if referencesLaneField(tbl, key, "control") {
					t.Fatalf("%s's call graph (via %s) references the Lane A1 control gate — Rule CP-3 requires Lane A2 (maintenance) to never hold or reference a Lane A1 (control) slot", root, key)
				}
			}
		}
	})

	t.Run("CP-3_control_entry_points_never_reach_maintenance_gate", func(t *testing.T) {
		// The six Lane A1 (control) entry points named in
		// docs/v0.6.0-plan.md §3.2's table.
		roots := []funcKey{
			"Node.AddLearner", "Node.PromoteToVoter", "Node.RemoveServer",
			"Node.MembershipStatus", "Node.UpgradePrecheck", "Node.FinalizeUpgrade",
		}
		for _, root := range roots {
			if _, ok := tbl.decls[root]; !ok {
				t.Fatalf("control entry point %s not found in internal/node — update this test's root list to match the current six", root)
			}
			reachable := tbl.reachableFrom(root)
			for key := range reachable {
				for _, field := range []string{"maintenance", "backupSlot", "scrubSlot"} {
					if referencesLaneField(tbl, key, field) {
						t.Fatalf("%s's call graph (via %s) references the Lane A2 %s gate — Rule CP-3 requires Lane A1 (control) to never hold or reference a Lane A2 (maintenance) slot", root, key, field)
					}
				}
			}
		}
	})
}

// TestAdmissionNeverReachableFromEventLoop_NegativeControl is AC-7's
// class of proof applied to this structural test itself: it must be
// possible for the test to actually fail. A hand-built, deliberately
// bad call graph (a function that calls .Acquire() directly, made
// reachable from a synthetic "run" root) proves gateAcquisitionCalls
// and the reachability walk actually detect the violation shape they
// exist to catch — a regression test that cannot fail when its
// mechanism is broken does not count (§31 gate 3).
func TestAdmissionNeverReachableFromEventLoop_NegativeControl(t *testing.T) {
	src := `package fakepkg

type fakeGate struct{}
func (g *fakeGate) Acquire() {}

type Node struct { admission *struct{ write *fakeGate } }

func (n *Node) run() {
	n.helper()
}

func (n *Node) helper() {
	n.admission.write.Acquire()
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fake.go", src, 0)
	if err != nil {
		t.Fatalf("parsing synthetic source: %v", err)
	}
	tbl := indexNodeFuncs([]*ast.File{f})
	reachable := tbl.reachableFrom("Node.run")
	var offenders []string
	for key := range reachable {
		if hits := gateAcquisitionCalls(tbl, key); len(hits) > 0 {
			offenders = append(offenders, key)
		}
	}
	if len(offenders) == 0 {
		t.Fatal("negative control: expected the synthetic run->helper->Acquire() call graph to be flagged, but it was not — the detection logic itself is broken")
	}
}
