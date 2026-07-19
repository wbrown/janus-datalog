package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func scopePattern(names ...string) *DataPattern {
	elements := make([]PatternElement, len(names))
	for i, name := range names {
		if name == "_" {
			elements[i] = Blank{}
		} else if name[0] == '?' {
			elements[i] = Variable{Name: datalog.NewSymbol(name)}
		} else {
			elements[i] = Constant{Value: datalog.NewKeyword(":" + name)}
		}
	}
	return &DataPattern{Elements: elements}
}

func assertSymbols(t *testing.T, label string, got []Symbol, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for _, name := range want {
		if !ContainsSymbol(got, datalog.NewSymbol(name)) {
			t.Errorf("%s = %v, missing %s", label, got, name)
		}
	}
}

// TestScopeOf pins the canonical scope interface per clause form: Provides
// are the variables a clause can bind in its enclosing scope, Correlates its
// free variables that unify with the enclosing scope when bound there, and
// scoped-away variables (explicit-join bodies, subquery inner queries)
// appear in neither.
func TestScopeOf(t *testing.T) {
	t.Run("data pattern provides its variables", func(t *testing.T) {
		scope := ScopeOf(scopePattern("?e", "a/b", "?v"))
		assertSymbols(t, "Provides", scope.Provides, "?e", "?v")
		assertSymbols(t, "Correlates", scope.Correlates)
	})

	t.Run("predicate correlates on its required symbols", func(t *testing.T) {
		scope := ScopeOf(&Comparison{
			Op:    datalog.SymLT,
			Left:  VariableTerm{Symbol: datalog.NewSymbol("?x")},
			Right: ConstantTerm{Value: int64(10)},
		})
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?x")
	})

	t.Run("expression provides its binding, correlates on arguments", func(t *testing.T) {
		scope := ScopeOf(&Expression{
			Function: ArithmeticFunction{
				Op: datalog.SymAdd,
				Args: []Term{
					VariableTerm{Symbol: datalog.NewSymbol("?a")},
					VariableTerm{Symbol: datalog.NewSymbol("?b")},
				},
			},
			Binding: datalog.NewSymbol("?sum"),
		})
		assertSymbols(t, "Provides", scope.Provides, "?sum")
		assertSymbols(t, "Correlates", scope.Correlates, "?a", "?b")
	})

	t.Run("subquery provides its binding, correlates on variable inputs, inner query invisible", func(t *testing.T) {
		scope := ScopeOf(&SubqueryPattern{
			Query: &Query{Where: []Clause{scopePattern("?inner", "x/y", "?hidden")}},
			Inputs: []PatternElement{
				Constant{Value: datalog.SymDollar},
				Variable{Name: datalog.NewSymbol("?in")},
			},
			Binding: TupleBinding{Variables: []Symbol{datalog.NewSymbol("?out")}},
		})
		assertSymbols(t, "Provides", scope.Provides, "?out")
		assertSymbols(t, "Correlates", scope.Correlates, "?in")
	})

	t.Run("not correlates on its body's free variables", func(t *testing.T) {
		scope := ScopeOf(&NotClause{Clauses: []Clause{scopePattern("?d", "seen/val", "?v")}})
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?d", "?v")
		if !scope.CorrelatesOptional {
			t.Error("a plain NOT's correlates are existential when the query cannot bind them")
		}
	})

	t.Run("predicate correlates are mandatory", func(t *testing.T) {
		scope := ScopeOf(&Comparison{
			Op:    datalog.SymLT,
			Left:  VariableTerm{Symbol: datalog.NewSymbol("?x")},
			Right: ConstantTerm{Value: int64(10)},
		})
		if scope.CorrelatesOptional {
			t.Error("a predicate needs every correlate bound; an unbindable one is a query error")
		}
	})

	t.Run("not-join header correlates are mandatory", func(t *testing.T) {
		scope := ScopeOf(&NotJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?e")},
			Clauses:  []Clause{scopePattern("?e", "a/b", "?x")},
		})
		if scope.CorrelatesOptional {
			t.Error("an explicit header is a declaration that the variables are bound outside")
		}
	})

	t.Run("or-default correlates are optional", func(t *testing.T) {
		scope := ScopeOf(&OrDefaultClause{Branches: [][]Clause{
			{scopePattern("?e", "a/b", "?x")},
		}})
		if !scope.CorrelatesOptional {
			t.Error("or-default falls back to global evaluation when correlation is unbindable")
		}
	})

	t.Run("not sees a nested or-join's header, not its branch locals", func(t *testing.T) {
		scope := ScopeOf(&NotClause{Clauses: []Clause{
			&OrJoinClause{
				JoinVars: []Symbol{datalog.NewSymbol("?v")},
				Branches: [][]Clause{{scopePattern("?local", "seen/val", "?v")}},
			},
		}})
		assertSymbols(t, "Correlates", scope.Correlates, "?v")
	})

	t.Run("not-join correlates on exactly its header", func(t *testing.T) {
		scope := ScopeOf(&NotJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?e")},
			Clauses:  []Clause{scopePattern("?e", "user/blocked", "?why")},
		})
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
	})

	t.Run("or provides the intersection of branch provides", func(t *testing.T) {
		scope := ScopeOf(&OrClause{Branches: [][]Clause{
			{scopePattern("?e", "a/b", "?x")},
			{scopePattern("?e", "a/c", "?y")},
		}})
		assertSymbols(t, "Provides", scope.Provides, "?e")
	})

	t.Run("or-join provides its header, branch locals invisible", func(t *testing.T) {
		scope := ScopeOf(&OrJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?local1")},
				{scopePattern("?e", "a/c", "?local2")},
			},
		})
		assertSymbols(t, "Provides", scope.Provides, "?e")
		assertSymbols(t, "Correlates", scope.Correlates)
	})

	t.Run("or-join header var not produced by all branches correlates", func(t *testing.T) {
		scope := ScopeOf(&OrJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?x")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?x")},
				{scopePattern("?e", "a/c", "?other")},
			},
		})
		assertSymbols(t, "Provides", scope.Provides, "?e")
		assertSymbols(t, "Correlates", scope.Correlates, "?x")
	})

	t.Run("or-default provides the intersection of branch provides", func(t *testing.T) {
		// The executor's output schema is outer ∪ branch intersection
		// (computeOrBranchOutputSymbols): a symbol only one branch binds
		// is absent when the other branch fires and never appears in the
		// relation. Union-provides was a schema hole.
		scope := ScopeOf(&OrDefaultClause{Branches: [][]Clause{
			{scopePattern("?e", "a/b", "?x")},
			{&Expression{
				Function: &GroundFunction{Value: int64(0)},
				Binding:  datalog.NewSymbol("?x"),
			}},
		}})
		assertSymbols(t, "Provides", scope.Provides, "?x")
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
	})

	t.Run("or-default-join branch outputs escape past the header", func(t *testing.T) {
		// Live contract: the header declares correlation keys, not the
		// complete interface — the algebra emitter binds outputs outside
		// the header (the get-else/fallback decorrelation shape).
		scope := ScopeOf(&OrDefaultJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{&Expression{
					Function: &GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?out"),
				}},
			},
		})
		assertSymbols(t, "Provides", scope.Provides, "?e", "?out")
		assertSymbols(t, "Correlates", scope.Correlates)
		if !scope.CorrelatesOptional {
			t.Error("or-default-join falls back per correlation group; unbindable correlates are legal")
		}
	})

	t.Run("or-default-join header var no branch produces correlates", func(t *testing.T) {
		scope := ScopeOf(&OrDefaultJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{&Expression{
					Function: &GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?out"),
				}},
			},
		})
		assertSymbols(t, "Provides", scope.Provides, "?out")
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
	})
}

// TestFreeVariables pins the clause-list union: every Provides and
// Correlates, deduplicated, first-appearance order, scoped variables absent.
func TestFreeVariables(t *testing.T) {
	free := FreeVariables([]Clause{
		scopePattern("?e", "a/b", "?v"),
		&OrJoinClause{
			JoinVars: []Symbol{datalog.NewSymbol("?v")},
			Branches: [][]Clause{{scopePattern("?local", "c/d", "?v")}},
		},
		&Comparison{
			Op:    datalog.SymLT,
			Left:  VariableTerm{Symbol: datalog.NewSymbol("?v")},
			Right: ConstantTerm{Value: int64(3)},
		},
	})
	assertSymbols(t, "FreeVariables", free, "?e", "?v")
}
