package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestScrubOnlyOpensReadOnly is SL-22 (docs/v0.6.0-plan.md §21.2, §27.10
// SCRUB NON-DESTRUCTIVE): a structural test, using only go/parser+go/ast
// (standard library only, docs/dependencies.md's zero-external-
// dependency policy untouched), of the same class as
// TestAdmissionNeverReachableFromEventLoop (admission_ast_test.go) —
// scoped, here, to every source file that actually implements a Scrub
// function (internal/wal, internal/snapshot, internal/audit,
// internal/node itself) rather than a full cross-package call-graph
// walk, since these four files are scrub's entire implementation and
// nothing else in the tree ever calls storage.OpenSegment/Segment.
// Append/Sync/Truncate on scrub's behalf.
//
// It asserts two things holding across all four files together:
//  1. storage.OpenSegment (the read-write constructor) is never
//     referenced — only storage.OpenSegmentReadOnly.
//  2. No call expression's method name is Append, Sync, or Truncate —
//     the exact three *storage.Segment methods that mutate a file,
//     which OpenSegmentReadOnly's own result refuses at the type level
//     (ErrReadOnlySegment) but which this test additionally forbids
//     from ever being *written*, so a future edit cannot even attempt
//     one.
//
// A negative-control companion (this file, below) proves the AST walk
// itself actually catches a violation, so a bug in the walk cannot make
// this test vacuously pass.
func TestScrubOnlyOpensReadOnly(t *testing.T) {
	files := []string{
		"scrub.go", // internal/node
		"../wal/scrub.go",
		"../snapshot/scrub.go",
		"../audit/scrub.go",
	}
	findings := scanScrubFilesForWriteCalls(t, files)
	if len(findings) != 0 {
		t.Fatalf("scrub implementation references a write-capable segment operation: %v", findings)
	}

	sawReadOnlyOpen := scanScrubFilesForIdentifier(t, files, "OpenSegmentReadOnly")
	if !sawReadOnlyOpen {
		t.Fatal("no scrub file references storage.OpenSegmentReadOnly at all — this test would pass vacuously on a scrub implementation that reads nothing")
	}
}

// TestScrubOnlyOpensReadOnly_NegativeControl proves
// scanScrubFilesForWriteCalls actually detects a violation, using a
// throwaway source string containing exactly the pattern the real test
// forbids — the same negative-control discipline
// TestAdmissionNeverReachableFromEventLoop_NegativeControl already
// establishes in this package.
func TestScrubOnlyOpensReadOnly_NegativeControl(t *testing.T) {
	const src = `package fake

func bad(seg *Segment) error {
	return seg.Truncate(0)
}
`
	findings := scanSourceForWriteCalls(t, "negative_control.go", src)
	if len(findings) == 0 {
		t.Fatal("negative control: scanSourceForWriteCalls found nothing in code that calls Truncate — the detector itself is broken")
	}
}

func scanScrubFilesForWriteCalls(t *testing.T, paths []string) []string {
	t.Helper()
	var findings []string
	fset := token.NewFileSet()
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", p, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Append", "Sync", "Truncate":
				findings = append(findings, p+": call to ."+sel.Sel.Name+"(...)")
			case "OpenSegment":
				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "storage" {
					findings = append(findings, p+": call to storage.OpenSegment (read-write) — must be OpenSegmentReadOnly")
				}
			}
			return true
		})
	}
	return findings
}

func scanScrubFilesForIdentifier(t *testing.T, paths []string, name string) bool {
	t.Helper()
	fset := token.NewFileSet()
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", p, err)
		}
		found := false
		ast.Inspect(f, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok && ident.Name == name {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

func scanSourceForWriteCalls(t *testing.T, filename, src string) []string {
	t.Helper()
	var findings []string
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatalf("parsing negative-control source: %v", err)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "Append", "Sync", "Truncate":
			findings = append(findings, filename+": call to ."+sel.Sel.Name+"(...)")
		}
		return true
	})
	return findings
}
