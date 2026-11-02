package tests

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

const (
	testifyAssert  = "github.com/stretchr/testify/assert"
	testifyRequire = "github.com/stretchr/testify/require"
)

// failures are the testing methods that fail the test they are called on.
var failures = map[string]bool{
	"Error": true, "Errorf": true, "Fatal": true, "Fatalf": true, "Fail": true, "FailNow": true,
}

// moduleFunc is a function the module declares: its declaration, the file
// that declares it, and that file's directory.
type moduleFunc struct {
	decl *ast.FuncDecl
	file *ast.File
	dir  string
}

// unverifiedTest is a test that checks nothing about what the code it runs
// produced.
type unverifiedTest struct {
	pos  token.Position
	name string
}

// moduleSource is every Go file of a module, parsed, its functions and
// methods indexed by directory and name, and its Test and Fuzz functions.
type moduleSource struct {
	root, path string
	fset       *token.FileSet
	funcs      map[string]map[string][]moduleFunc
	tests      []moduleFunc
	verdicts   map[*ast.FuncDecl]bool
	visiting   map[*ast.FuncDecl]bool
}

// moduleRoot is the directory holding the go.mod of the module the test runs
// in.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod above the test's directory")
		dir = parent
	}
}

// parseModule parses every Go file of the module rooted at root, leaving out
// hidden directories, testdata, vendor and nested modules.
func parseModule(t *testing.T, root string) *moduleSource {
	t.Helper()
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	require.NoError(t, err)
	var path string
	for _, line := range strings.Split(string(mod), "\n") {
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			path = strings.TrimSpace(rest)
			break
		}
	}
	require.NotEmpty(t, path, "the go.mod at %s names its module", root)
	m := &moduleSource{
		root:     root,
		path:     path,
		fset:     token.NewFileSet(),
		funcs:    map[string]map[string][]moduleFunc{},
		verdicts: map[*ast.FuncDecl]bool{},
		visiting: map[*ast.FuncDecl]bool{},
	}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == root {
				return nil
			}
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		file, err := parser.ParseFile(m.fset, p, nil, 0)
		if err != nil {
			return err
		}
		dir := filepath.Dir(p)
		if m.funcs[dir] == nil {
			m.funcs[dir] = map[string][]moduleFunc{}
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			f := moduleFunc{decl: fd, file: file, dir: dir}
			m.funcs[dir][fd.Name.Name] = append(m.funcs[dir][fd.Name.Name], f)
			if strings.HasSuffix(p, "_test.go") && isTestFunc(fd) {
				m.tests = append(m.tests, f)
			}
		}
		return nil
	})
	require.NoError(t, err)
	return m
}

// isTestFunc reports whether fd is a function go test runs as a test: a Test
// or Fuzz function taking *testing.T or *testing.F.
func isTestFunc(fd *ast.FuncDecl) bool {
	if fd.Recv != nil {
		return false
	}
	name := fd.Name.Name
	rest, isTest := strings.CutPrefix(name, "Test")
	if !isTest {
		var isFuzz bool
		if rest, isFuzz = strings.CutPrefix(name, "Fuzz"); !isFuzz {
			return false
		}
	}
	if rest != "" && unicode.IsLower(rune(rest[0])) {
		return false
	}
	params := fd.Type.Params.List
	return len(params) == 1 && len(params[0].Names) <= 1 && isTestingParam(params[0].Type)
}

// isTestingParam reports whether expr is the type of a value a test fails
// through: *testing.T, *testing.B, *testing.F or testing.TB.
func isTestingParam(expr ast.Expr) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "testing" {
		return false
	}
	switch sel.Sel.Name {
	case "T", "B", "F", "TB":
		return true
	}
	return false
}

// checksAnError reports whether cond is an error-absence condition, err != nil
// on an error-named variable or a disjunction of such conditions, so a
// failure it guards checks only that no error occurred. A disjunction with
// any other operand fails on a wrong result too.
func checksAnError(cond ast.Expr) bool {
	bin, ok := cond.(*ast.BinaryExpr)
	if !ok {
		return false
	}
	if bin.Op == token.LOR {
		return checksAnError(bin.X) && checksAnError(bin.Y)
	}
	if bin.Op != token.NEQ {
		return false
	}
	isNil := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && id.Name == "nil"
	}
	isErr := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && (id.Name == "err" || strings.HasSuffix(id.Name, "Err") || strings.HasSuffix(id.Name, "err"))
	}
	return (isNil(bin.Y) && isErr(bin.X)) || (isNil(bin.X) && isErr(bin.Y))
}

// verifies reports whether f fails on a wrong result: whether its body, a
// function literal within it, or a module function it calls or passes to a
// call makes a check other than that no error occurred, or checks the error a
// test's function returns on a wrong result.
func (m *moduleSource) verifies(f moduleFunc) bool {
	if v, ok := m.verdicts[f.decl]; ok {
		return v
	}
	if m.visiting[f.decl] {
		return false
	}
	m.visiting[f.decl] = true
	v := m.bodyVerifies(f)
	delete(m.visiting, f.decl)
	m.verdicts[f.decl] = v
	return v
}

// anyVerifies reports whether a function or method named name declared in
// dir verifies.
func (m *moduleSource) anyVerifies(dir, name string) bool {
	for _, f := range m.funcs[dir][name] {
		if m.verifies(f) {
			return true
		}
	}
	return false
}

func (m *moduleSource) bodyVerifies(f moduleFunc) bool {
	imports := map[string]string{}
	for _, spec := range f.file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
	}
	testers := map[string]bool{}
	addTesters := func(ft *ast.FuncType) {
		for _, field := range ft.Params.List {
			if isTestingParam(field.Type) {
				for _, n := range field.Names {
					testers[n.Name] = true
				}
			}
		}
	}
	addTesters(f.decl.Type)
	// assignment is one assignment to a variable: where it is, and whether
	// the variable receives the error of a call that reports a wrong result.
	type assignment struct {
		pos     token.Pos
		reports bool
	}
	assigned := map[*ast.Object][]assignment{}
	var errorChecks []*ast.IfStmt
	ast.Inspect(f.decl.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			addTesters(n.Type)
		case *ast.IfStmt:
			if checksAnError(n.Cond) {
				errorChecks = append(errorChecks, n)
			}
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || id.Obj == nil {
					continue
				}
				// A variable receives the result of its own right-hand
				// side, or the last result of the one call on the right.
				reports := false
				if len(n.Rhs) == len(n.Lhs) {
					call, isCall := n.Rhs[i].(*ast.CallExpr)
					reports = isCall && m.reportsAWrongResult(f.dir, imports, call)
				} else if i == len(n.Lhs)-1 {
					call, isCall := n.Rhs[0].(*ast.CallExpr)
					reports = isCall && m.reportsAWrongResult(f.dir, imports, call)
				}
				assigned[id.Obj] = append(assigned[id.Obj], assignment{pos: n.Pos(), reports: reports})
			}
		}
		return true
	})
	// readsAReport reports whether expr is the error of a call that reports a
	// wrong result: the call itself, or a variable whose latest assignment
	// before expr received it.
	readsAReport := func(expr ast.Expr) bool {
		switch e := expr.(type) {
		case *ast.CallExpr:
			return m.reportsAWrongResult(f.dir, imports, e)
		case *ast.Ident:
			latest := -1
			for i, a := range assigned[e.Obj] {
				if a.pos < e.Pos() && (latest < 0 || a.pos > assigned[e.Obj][latest].pos) {
					latest = i
				}
			}
			return latest >= 0 && assigned[e.Obj][latest].reports
		}
		return false
	}
	// An error check guards the failures in its body, unless the error it
	// reads reports a wrong result: a failure there fails on one.
	var guards []*ast.BlockStmt
	for _, check := range errorChecks {
		reported := false
		ast.Inspect(check.Cond, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && readsAReport(id) {
				reported = true
			}
			return !reported
		})
		if !reported {
			guards = append(guards, check.Body)
		}
	}
	guarded := func(pos token.Pos) bool {
		for _, g := range guards {
			if g.Pos() <= pos && pos < g.End() {
				return true
			}
		}
		return false
	}
	verified := false
	ast.Inspect(f.decl.Body, func(n ast.Node) bool {
		if verified {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			verified = m.anyVerifies(f.dir, fun.Name)
		case *ast.SelectorExpr:
			x, isIdent := fun.X.(*ast.Ident)
			switch {
			case isIdent && testers[x.Name]:
				verified = failures[fun.Sel.Name] && !guarded(call.Pos())
			case isIdent && (imports[x.Name] == testifyAssert || imports[x.Name] == testifyRequire):
				// An assertion that no error occurred fails on a wrong
				// result only when the error it reads reports one.
				switch fun.Sel.Name {
				case "NoError", "NoErrorf":
					verified = len(call.Args) > 1 && readsAReport(call.Args[1])
				default:
					verified = true
				}
			case isIdent && strings.HasPrefix(imports[x.Name], m.path+"/"):
				verified = m.anyVerifies(filepath.Join(m.root, filepath.FromSlash(strings.TrimPrefix(imports[x.Name], m.path+"/"))), fun.Sel.Name)
			default:
				verified = m.anyVerifies(f.dir, fun.Sel.Name)
			}
		}
		// A module function passed to a call is reached through it. An
		// identifier the file resolves to a variable or parameter names no
		// function, whatever it is called.
		for _, arg := range call.Args {
			if verified {
				break
			}
			switch ref := arg.(type) {
			case *ast.Ident:
				if ref.Obj == nil || ref.Obj.Kind == ast.Fun {
					verified = m.anyVerifies(f.dir, ref.Name)
				}
			case *ast.SelectorExpr:
				if x, isIdent := ref.X.(*ast.Ident); isIdent && strings.HasPrefix(imports[x.Name], m.path+"/") {
					verified = m.anyVerifies(filepath.Join(m.root, filepath.FromSlash(strings.TrimPrefix(imports[x.Name], m.path+"/"))), ref.Sel.Name)
				}
			}
		}
		return true
	})
	return verified
}

// reportsAWrongResult reports whether call calls a test's function that
// returns an error on a wrong result: the function literal a variable is
// declared with, or a function or method a test file in dir declares. A
// production function's error is the behavior a test checks, never a check of
// it, and another package's test files are not importable.
func (m *moduleSource) reportsAWrongResult(dir string, imports map[string]string, call *ast.CallExpr) bool {
	var name string
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		if fun.Obj != nil && fun.Obj.Kind == ast.Var {
			var names, values []ast.Expr
			switch decl := fun.Obj.Decl.(type) {
			case *ast.AssignStmt:
				names, values = decl.Lhs, decl.Rhs
			case *ast.ValueSpec:
				for _, n := range decl.Names {
					names = append(names, n)
				}
				values = decl.Values
			}
			if len(names) != len(values) {
				return false
			}
			for i, n := range names {
				if id, ok := n.(*ast.Ident); ok && id.Obj == fun.Obj {
					lit, isLit := values[i].(*ast.FuncLit)
					return isLit && returnsAWrongResult(lit.Type, lit.Body)
				}
			}
			return false
		}
		name = fun.Name
	case *ast.SelectorExpr:
		if x, isIdent := fun.X.(*ast.Ident); isIdent && imports[x.Name] != "" {
			return false
		}
		name = fun.Sel.Name
	default:
		return false
	}
	for _, c := range m.funcs[dir][name] {
		if strings.HasSuffix(m.fset.Position(c.decl.Pos()).Filename, "_test.go") && returnsAWrongResult(c.decl.Type, c.decl.Body) {
			return true
		}
	}
	return false
}

// returnsAWrongResult reports whether a function of type ft with body body
// reports a wrong result as its error: its last result is an error, and it
// returns a non-nil one inside an if whose condition is not an error check.
// A non-nil error returned under an error check passes another error on.
func returnsAWrongResult(ft *ast.FuncType, body *ast.BlockStmt) bool {
	if ft.Results == nil || len(ft.Results.List) == 0 {
		return false
	}
	if last, ok := ft.Results.List[len(ft.Results.List)-1].Type.(*ast.Ident); !ok || last.Name != "error" {
		return false
	}
	var enclosing []ast.Node
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		if n == nil {
			enclosing = enclosing[:len(enclosing)-1]
			return true
		}
		switch n := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			if len(n.Results) > 0 {
				if id, isIdent := n.Results[len(n.Results)-1].(*ast.Ident); !isIdent || id.Name != "nil" {
					for i := len(enclosing) - 1; i >= 0; i-- {
						if cond, ok := enclosing[i].(*ast.IfStmt); ok {
							found = !checksAnError(cond.Cond)
							break
						}
					}
				}
			}
		}
		enclosing = append(enclosing, n)
		return true
	})
	return found
}

// unverified is the module's tests that check nothing about what the code
// they run produced, in file and line order.
func (m *moduleSource) unverified() []unverifiedTest {
	var found []unverifiedTest
	for _, test := range m.tests {
		if !m.verifies(test) {
			pos := m.fset.Position(test.decl.Pos())
			if rel, err := filepath.Rel(m.root, pos.Filename); err == nil {
				pos.Filename = rel
			}
			found = append(found, unverifiedTest{pos: pos, name: test.decl.Name.Name})
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].pos.Filename != found[j].pos.Filename {
			return found[i].pos.Filename < found[j].pos.Filename
		}
		return found[i].pos.Line < found[j].pos.Line
	})
	return found
}

// A test verifies the code it runs only when it fails on a wrong result.
// Every Test and Fuzz function of the module checks something about what the
// code produced, in its own body, in a function literal within it, or in a
// function of the module it calls: a testify assertion other than the absence
// of an error, a t.Error, t.Fatal or t.Fail that no err != nil condition
// guards, or a check that no error occurred on the error a test's function
// returns on a wrong result. A test whose only checks are that no error
// occurred otherwise passes on any answer.
func TestEveryTestVerifiesItsResult(t *testing.T) {
	found := parseModule(t, moduleRoot(t)).unverified()
	lines := make([]string, len(found))
	for i, u := range found {
		lines[i] = u.pos.String() + " " + u.name
	}
	require.Empty(t, found, "%d tests check nothing about what the code they run produced:\n%s", len(found), strings.Join(lines, "\n"))
}

// The check flags a test that only logs, those whose only checks are that no
// error occurred, under one error check or under either of two, one that
// passes a function that only logs to a call, one that passes a variable
// named like a comparing function, one that checks the error of a function
// whose only error passes another on, and one that checks an error assigned
// after a reported comparison. It passes a test that compares a value, one
// that compares a value in the condition that checks the error, one that
// expects an error, one that compares through a function it calls in a
// subtest, one that passes a comparing function to a call, and ones that check
// the error a function returns on a wrong result, directly or under an error
// check.
func TestEveryTestVerifiesItsResult_FlagsATestThatChecksNothingAboutItsResult(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/m\n\ngo 1.26\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "p"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "p", "p_test.go"), []byte(`package p

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func produce() (int, error) { return 1, nil }

func requireOne(t *testing.T, n int) { require.Equal(t, 1, n) }

func logsOne(t *testing.T, n int) { t.Logf("produced %d", n) }

func each(t *testing.T, run func(*testing.T, int)) { run(t, 1) }

func TestOnlyLogs(t *testing.T) {
	n, _ := produce()
	t.Logf("produced %d", n)
}

func TestOnlyErrors(t *testing.T) {
	_, err := produce()
	require.NoError(t, err)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func TestOnlyTwoErrors(t *testing.T) {
	_, err := produce()
	_, againErr := produce()
	if err != nil || againErr != nil {
		t.Fatalf("produce: %v, %v", err, againErr)
	}
}

func TestComparesAValue(t *testing.T) {
	n, err := produce()
	require.NoError(t, err)
	if n != 1 {
		t.Errorf("got %d", n)
	}
}

func TestComparesAValueBesideTheError(t *testing.T) {
	n, err := produce()
	if err != nil || n != 1 {
		t.Errorf("got %d, %v", n, err)
	}
}

func TestExpectsAnError(t *testing.T) {
	if err := errors.New("refused"); err == nil {
		t.Fatal("expected an error")
	}
}

func TestThroughAFunctionInASubtest(t *testing.T) {
	n, _ := produce()
	t.Run("one", func(t *testing.T) { requireOne(t, n) })
}

func TestThroughAFunctionValue(t *testing.T) {
	each(t, requireOne)
}

func TestThroughAFunctionValueThatLogs(t *testing.T) {
	each(t, logsOne)
}

func TestPassesAVariableNamedLikeAComparingFunction(t *testing.T) {
	requireOne := 1
	t.Logf("%d", requireOne)
}

func compareOne() error {
	n, err := produce()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("not one")
	}
	return nil
}

func TestThroughAComparisonItsErrorReports(t *testing.T) {
	check := func() error {
		n, err := produce()
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.New("not one")
		}
		return nil
	}
	require.NoError(t, check())
}

func TestThroughAReportedComparisonUnderAnErrorCheck(t *testing.T) {
	if err := compareOne(); err != nil {
		t.Fatal(err)
	}
}

func TestThroughAnErrorThatOnlyPassesAnotherOn(t *testing.T) {
	check := func() error {
		n, err := produce()
		if err != nil {
			return err
		}
		t.Logf("produced %d", n)
		return nil
	}
	require.NoError(t, check())
}

func TestChecksAnErrorAssignedAfterAReportedComparison(t *testing.T) {
	err := compareOne()
	t.Logf("compared: %v", err)
	n, err := produce()
	t.Logf("produced %d", n)
	require.NoError(t, err)
}
`), 0o644))

	var names []string
	for _, u := range parseModule(t, root).unverified() {
		names = append(names, u.name)
	}
	require.Equal(t, []string{"TestOnlyLogs", "TestOnlyErrors", "TestOnlyTwoErrors", "TestThroughAFunctionValueThatLogs", "TestPassesAVariableNamedLikeAComparingFunction", "TestThroughAnErrorThatOnlyPassesAnotherOn", "TestChecksAnErrorAssignedAfterAReportedComparison"}, names)
}
