package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAReadAfterAFailedImportAnswersTheStore loops the engine axis,
// optimizerModes, and its subject, the EA cache after a failed import, is
// engine by the matrix comment in optimizer_modes_test.go. CLAUDE.md item 6:
// "The reach is measured, and the loop carries the measurement as its
// literal". This test measures the reach the way -reach does, by running the
// test's body with the report on, and reads the options its legsFor literal
// names from the source; the two are equal.
func TestTheFailedImportReadLoopCarriesItsMeasuredReach(t *testing.T) {
	saved := *reachReport
	*reachReport = filepath.Join(t.TempDir(), "reach.txt")
	t.Run("measure", func(t *testing.T) { TestAReadAfterAFailedImportAnswersTheStore(t) })
	*reachReport = saved

	reachRecorded.Lock()
	var measured []string
	for _, o := range allOptions {
		if reachRecorded.byTest[t.Name()][o] {
			measured = append(measured, o.String())
		}
	}
	reachRecorded.Unlock()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "import_failure_cache_test.go", nil, 0)
	require.NoError(t, err)
	var literal []string
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "TestAReadAfterAFailedImportAnswersTheStore" {
			return true
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "legsFor" {
				found = true
				for _, arg := range call.Args[1:] {
					literal = append(literal, arg.(*ast.Ident).Name)
				}
			}
			return true
		})
		return false
	})
	require.True(t, found, "the test loops legsFor")
	names := map[string]string{}
	for _, o := range allOptions {
		names[o.String()+"Option"] = o.String()
	}
	var named []string
	for _, ident := range literal {
		for o := range names {
			if equalFold(o, ident) {
				named = append(named, names[o])
			}
		}
	}
	sort.Strings(measured)
	sort.Strings(named)
	require.Equal(t, measured, named, "the loop's legsFor literal names the reach a -reach run records")
}

// equalFold compares an option's rendered name plus "Option" with an
// identifier, ignoring the case of the first letter only.
func equalFold(rendered, ident string) bool {
	if len(rendered) != len(ident) || len(ident) == 0 {
		return false
	}
	lower := func(b byte) byte {
		if b >= 'A' && b <= 'Z' {
			return b + 'a' - 'A'
		}
		return b
	}
	return lower(rendered[0]) == lower(ident[0]) && rendered[1:] == ident[1:]
}
