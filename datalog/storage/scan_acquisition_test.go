package storage

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// acquisitionsOutsideTheOpeners lists every function that reaches a store or
// reader directly instead of acquiring through OpenScan/OpenKeyScan.
//
// An entry states that this function's scan is accounted for some other way, or
// that it is not accounted for at all, and says which.
//
// No read a query causes is here. Reads no pattern asked for pass
// DiscardIntake, reads nested inside resolution acquire through the report of
// the arm they serve, and every arm's own scan carries a report that completes.
//
// What remains is the seam implementing itself: store methods and the shared
// derivations behind them, which answer by scanning the store they are part of.
// These sit below the layer a report lives at — routing them through an opener
// would be a store calling a module-level function to call itself, and there is
// no arm on whose behalf they read. A sixth such function reds this test, which
// is the point: the category is narrow and named, not open.
var acquisitionsOutsideTheOpeners = map[string]string{
	"MaxElementID":         "MemoryStore answering its own interface method",
	"MaxTxForEntity":       "MemoryStore answering its own interface method",
	"DatomsAfter":          "MemoryStore answering its own interface method",
	"maxElementIDByScan":   "shared derivation behind MaxElementID",
	"maxTxForEntityByScan": "shared derivation behind MaxTxForEntity",
}

// TestScanAcquisitionGoesThroughAReport is the check that closes Family 2.
//
// The family's generator is that the obligated set came from a review's
// enumeration rather than from the code: the next arm written was invisible
// to both the trace and the gate. This derives the set from the source on
// every run, which is the property the enumeration lacked.
//
// It walks the whole module, not this package. The openers are exported for
// exactly that reason — a query path added in db, executor or a command would
// otherwise bypass the accounting with the gate none the wiser, which is the
// generator relocated one package over.
//
// Detection is by call shape rather than by receiver name: a one-argument
// Scan/ScanKeysOnly. A name whitelist missed db.Store().Scan(bound), whose
// receiver is a call and not a field, and would go on missing whatever the next
// spelling is. Arity separates these from bufio.Scanner.Scan, which takes none.
//
// Occurrences are attributed to the enclosing function because that is what
// stays stable while line numbers move.
func TestScanAcquisitionGoesThroughAReport(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	root, err := moduleRoot(cwd)
	require.NoError(t, err)

	fset := token.NewFileSet()
	found := map[string]string{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		// scan_report.go holds the openers themselves; they are the one place a
		// reader is reached on purpose.
		if name == "scan_report.go" {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if sel.Sel.Name != "Scan" && sel.Sel.Name != "ScanKeysOnly" {
					return true
				}
				if len(call.Args) != 1 {
					return true
				}
				found[fn.Name.Name] = fmt.Sprintf("%s:%d", rel, fset.Position(call.Pos()).Line)
				return true
			})
		}
		return nil
	})
	require.NoError(t, err)

	for name, where := range found {
		if _, accepted := acquisitionsOutsideTheOpeners[name]; !accepted {
			t.Errorf("%s (%s) acquires a scan without a report.\n"+
				"Use storage.OpenScan/OpenKeyScan, which attach the accounting to the "+
				"acquisition. If this read is genuinely no pattern's, pass "+
				"storage.DiscardIntake. If it is nested under an arm that accounts "+
				"for it, acquire through that arm's report. If neither, add it to "+
				"acquisitionsOutsideTheOpeners with which case it is.", name, where)
		}
	}

	// A stale entry would let the next direct acquisition hide under a name
	// that no longer takes one.
	for name := range acquisitionsOutsideTheOpeners {
		if _, still := found[name]; !still {
			t.Errorf("%s no longer acquires directly; remove it from "+
				"acquisitionsOutsideTheOpeners", name)
		}
	}
}
