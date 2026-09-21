package mvcc

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot locates the module root by walking up from the current
// working directory (go test's cwd is always the package directory)
// until go.mod is found.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repository root (go.mod not found in any parent directory)")
		}
		dir = parent
	}
}

// findZeroArgExportCalls walks root and returns "path:line" for every
// zero-argument call whose selector name is exactly "Export", found in
// a non-test .go file under any of excludeRelDirs (relative to root).
// Disambiguated from unrelated same-named methods/functions by arity
// alone (Store.Export takes no arguments; e.g. internal/backup.Export
// is a 3-argument package-level function and is correctly never
// flagged) — no full type resolution (go/types) needed.
func findZeroArgExportCalls(t *testing.T, root string, excludeRelDirs ...string) []string {
	t.Helper()
	var offenders []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			base := info.Name()
			if base == "vendor" || base == ".git" || strings.HasPrefix(base, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		for _, ex := range excludeRelDirs {
			if strings.HasPrefix(rel, ex+string(filepath.Separator)) {
				return nil
			}
		}

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Export" {
				return true
			}
			if len(call.Args) != 0 {
				return true // not Store.Export's zero-arg shape
			}
			offenders = append(offenders, fmt.Sprintf("%s:%d", rel, fset.Position(call.Pos()).Line))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return offenders
}

// TestExportOnlyCalledFromSnapshotEncoding is SL-6b (docs/v0.6.0-plan.md
// §15.2a): a structural, go/parser+go/ast test (the same class as
// TestControlKindRangesNeverCollide) asserting no package other than
// internal/fsm calls Store.Export — the bypass that made SL-6's gap
// possible in the first place (internal/sql's now-deleted mergeScan)
// must not reappear undetected.
func TestExportOnlyCalledFromSnapshotEncoding(t *testing.T) {
	root := repoRoot(t)
	offenders := findZeroArgExportCalls(t, root, filepath.Join("internal", "fsm"), filepath.Join("internal", "mvcc"))
	if len(offenders) > 0 {
		t.Fatalf("mvcc.Store.Export()-shaped call(s) found outside internal/fsm: %v — Export's only legitimate caller is internal/fsm's snapshot encoding (docs/v0.6.0-plan.md §15.2a); every other committed-read path must use Visible/ScanVisible", offenders)
	}
}

// TestExportOnlyCalledFromSnapshotEncoding_NegativeControl is AC-7's
// class of proof applied to this structural test itself (§31 gate 3): a
// synthetic package containing an offending Store.Export()-shaped call
// outside internal/fsm must actually be detected.
func TestExportOnlyCalledFromSnapshotEncoding_NegativeControl(t *testing.T) {
	dir := t.TempDir()
	src := `package fakepkg

type Store struct{}
func (s *Store) Export() []int { return nil }

func bypass(s *Store) {
	_ = s.Export()
}
`
	if err := os.WriteFile(filepath.Join(dir, "bypass.go"), []byte(src), 0o644); err != nil {
		t.Fatalf("writing synthetic source: %v", err)
	}
	offenders := findZeroArgExportCalls(t, dir) // no exclusions: dir has no internal/fsm or internal/mvcc subpath
	if len(offenders) == 0 {
		t.Fatal("negative control: expected the synthetic bypass.go's Store.Export() call to be flagged, but it was not — the detection logic itself is broken")
	}
}
