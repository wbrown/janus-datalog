package query

import (
	"strings"
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
// appear in neither. These forms carry no invocation, so they are read with
// no rule set.
func TestScopeOf(t *testing.T) {
	t.Run("data pattern provides its variables", func(t *testing.T) {
		scope := ScopeOf(scopePattern("?e", "a/b", "?v"), nil)
		assertSymbols(t, "Provides", scope.Provides, "?e", "?v")
		assertSymbols(t, "Correlates", scope.Correlates)
	})

	t.Run("predicate correlates on its required symbols", func(t *testing.T) {
		scope := ScopeOf(&Comparison{
			Op:    datalog.SymLT,
			Left:  VariableTerm{Symbol: datalog.NewSymbol("?x")},
			Right: ConstantTerm{Value: int64(10)},
		}, nil)
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
		}, nil)
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
		}, nil)
		assertSymbols(t, "Provides", scope.Provides, "?out")
		assertSymbols(t, "Correlates", scope.Correlates, "?in")
	})

	t.Run("not-join header of two variables, every one mandatory", func(t *testing.T) {
		scope := ScopeOf(&NotJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?d"), datalog.NewSymbol("?v")},
			Clauses:      []Clause{scopePattern("?d", "seen/val", "?v")},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?d", "?v")
		assertSymbols(t, "Mandatory", scope.Mandatory, "?d", "?v")
	})

	t.Run("not-join header variable the body reads by a predicate alone is mandatory", func(t *testing.T) {
		// The header is read whole; which of its variables the body binds is
		// InferHeaders' check at preparation.
		scope := ScopeOf(&NotJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?d"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w")},
			Clauses: []Clause{
				scopePattern("?d", "seen/val", "?v"),
				&Comparison{
					Op:    datalog.SymLT,
					Left:  VariableTerm{Symbol: datalog.NewSymbol("?w")},
					Right: VariableTerm{Symbol: datalog.NewSymbol("?v")},
				},
			},
		}, nil)
		assertSymbols(t, "Correlates", scope.Correlates, "?d", "?v", "?w")
		assertSymbols(t, "Mandatory", scope.Mandatory, "?d", "?v", "?w")
	})

	t.Run("predicate correlates are mandatory", func(t *testing.T) {
		scope := ScopeOf(&Comparison{
			Op:    datalog.SymLT,
			Left:  VariableTerm{Symbol: datalog.NewSymbol("?x")},
			Right: ConstantTerm{Value: int64(10)},
		}, nil)
		assertSymbols(t, "Mandatory", scope.Mandatory, "?x")
	})

	t.Run("not-join header correlates are mandatory", func(t *testing.T) {
		scope := ScopeOf(&NotJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			Clauses:      []Clause{scopePattern("?e", "a/b", "?x")},
		}, nil)
		assertSymbols(t, "Mandatory", scope.Mandatory, "?e")
	})

	t.Run("or-default-join reads its header without re-checking the branches", func(t *testing.T) {
		// Whether every branch binds every output is InferHeaders' check at
		// preparation; ScopeOf reads the declaration as written.
		scope := ScopeOf(&OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			OutputVars:   []Symbol{datalog.NewSymbol("?x")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?x")},
				{scopePattern("?e", "a/c", "?other")},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides, "?x")
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
	})

	t.Run("not-join over a nested or-join reads its own header, the branch local invisible", func(t *testing.T) {
		scope := ScopeOf(&NotJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?v")},
			Clauses: []Clause{
				&OrJoinClause{
					OutputVars: []Symbol{datalog.NewSymbol("?v")},
					Branches:   [][]Clause{{scopePattern("?local", "seen/val", "?v")}},
				},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?v")
	})

	t.Run("not-join correlates on exactly its header", func(t *testing.T) {
		scope := ScopeOf(&NotJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			Clauses:      []Clause{scopePattern("?e", "user/blocked", "?why")},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
	})

	t.Run("or-join with a required run and no outputs provides nothing", func(t *testing.T) {
		scope := ScopeOf(&OrJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?x")},
				{scopePattern("?e", "a/c", "?y")},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
		assertSymbols(t, "Mandatory", scope.Mandatory, "?e")
	})

	t.Run("or-join provides its header, branch locals invisible", func(t *testing.T) {
		scope := ScopeOf(&OrJoinClause{
			OutputVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?local1")},
				{scopePattern("?e", "a/c", "?local2")},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides, "?e")
		assertSymbols(t, "Correlates", scope.Correlates)
	})

	t.Run("or-join reads its header without re-checking the branches", func(t *testing.T) {
		// Whether every branch binds every output is InferHeaders' check at
		// preparation; ScopeOf reads the declaration as written.
		scope := ScopeOf(&OrJoinClause{
			OutputVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?x")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?x")},
				{scopePattern("?e", "a/c", "?other")},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides, "?e", "?x")
		assertSymbols(t, "Correlates", scope.Correlates)
	})

	t.Run("or-default-join with a required run and no outputs provides nothing", func(t *testing.T) {
		scope := ScopeOf(&OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?x")},
				{&Expression{
					Function: &GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?x"),
				}},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides)
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
		assertSymbols(t, "Mandatory", scope.Mandatory, "?e")
	})

	t.Run("or-default-join provides its outputs, correlates on its required vars", func(t *testing.T) {
		// The declared header is the complete interface: required vars
		// quantify the per-group fallback decision (mandatory — or-default
		// is non-monotone), output vars are bound by every branch, and
		// branch locals never escape.
		scope := ScopeOf(&OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			OutputVars:   []Symbol{datalog.NewSymbol("?out")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{&Expression{
					Function: &GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?out"),
				}},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides, "?out")
		assertSymbols(t, "Correlates", scope.Correlates, "?e")
		assertSymbols(t, "Mandatory", scope.Mandatory, "?e")
	})

	t.Run("or-default-join with no required vars is the global form", func(t *testing.T) {
		scope := ScopeOf(&OrDefaultJoinClause{
			OutputVars: []Symbol{datalog.NewSymbol("?out")},
			Branches: [][]Clause{
				{scopePattern("?d", "a/b", "?out")},
				{&Expression{
					Function: &GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?out"),
				}},
			},
		}, nil)
		assertSymbols(t, "Provides", scope.Provides, "?out")
		assertSymbols(t, "Correlates", scope.Correlates)
	})
}

// An or-join's header is its whole interface: the required run is its
// mandatory correlates, its outputs are provided, and a variable a branch
// reads outside the header is the branch's own, in neither set.
func TestScopeOf_OrJoinReadsItsHeaderAlone(t *testing.T) {
	scope := ScopeOf(&OrJoinClause{
		RequiredVars: []Symbol{datalog.NewSymbol("?a")},
		OutputVars:   []Symbol{datalog.NewSymbol("?e")},
		Branches: [][]Clause{
			{scopePattern("?e", "?a", "?v")},
			{scopePattern("?e", "t/tag", "?x"), &Comparison{
				Op:    datalog.SymGT,
				Left:  VariableTerm{Symbol: datalog.NewSymbol("?x")},
				Right: VariableTerm{Symbol: datalog.NewSymbol("?n")},
			}},
		},
	}, nil)
	assertSymbols(t, "Provides", scope.Provides, "?e")
	assertSymbols(t, "Correlates", scope.Correlates, "?a")
	assertSymbols(t, "Mandatory", scope.Mandatory, "?a")
}

// TestSubqueryPatternValidateInputCount pins that a subquery passes as many
// inputs as its inner :in declares, one per source, % and value position in
// that order, so a call with more or fewer is refused as a static shape at
// every user boundary.
func TestSubqueryPatternValidateInputCount(t *testing.T) {
	inner := &Query{
		Find:  []FindElement{FindVariable{Symbol: datalog.NewSymbol("?v")}},
		In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: datalog.NewSymbol("?x")}},
		Where: []Clause{scopePattern("?x", "a/b", "?v")},
	}
	call := func(inputs ...PatternElement) *SubqueryPattern {
		return &SubqueryPattern{Query: inner, Inputs: inputs, Binding: ScalarBinding{Variable: datalog.NewSymbol("?out")}}
	}
	dollar := Constant{Value: datalog.SymDollar}
	x, y := Variable{Name: datalog.NewSymbol("?x")}, Variable{Name: datalog.NewSymbol("?y")}
	if err := call(dollar, x).Validate(); err != nil {
		t.Errorf("one input per :in position is valid, got %v", err)
	}
	if err := call(dollar, x, y).Validate(); err == nil {
		t.Error("three inputs for two :in positions are refused")
	}
	if err := call(dollar).Validate(); err == nil {
		t.Error("one input for two :in positions is refused")
	}

	// An omitted :in is [$], and a tuple or relation input takes one value
	// per symbol, as the executor binds them.
	implicit := &SubqueryPattern{
		Query:   &Query{Find: inner.Find, Where: inner.Where},
		Inputs:  []PatternElement{dollar},
		Binding: ScalarBinding{Variable: datalog.NewSymbol("?out")},
	}
	if err := implicit.Validate(); err != nil {
		t.Errorf("$ for an omitted :in is valid, got %v", err)
	}
	pairs := &SubqueryPattern{
		Query: &Query{
			Find:  inner.Find,
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RelationInput{Symbols: []Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?w")}}},
			Where: inner.Where,
		},
		Inputs:  []PatternElement{dollar, x, y},
		Binding: ScalarBinding{Variable: datalog.NewSymbol("?out")},
	}
	if err := pairs.Validate(); err != nil {
		t.Errorf("one value per symbol of a relation input is valid, got %v", err)
	}
	tuple := &SubqueryPattern{
		Query: &Query{
			Find:  inner.Find,
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, TupleInput{Symbols: []Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?w")}}},
			Where: inner.Where,
		},
		Inputs:  []PatternElement{dollar, x, y},
		Binding: ScalarBinding{Variable: datalog.NewSymbol("?out")},
	}
	if err := tuple.Validate(); err != nil {
		t.Errorf("one value per symbol of a tuple input is valid, got %v", err)
	}
	tuple.Inputs = []PatternElement{dollar, x}
	if err := tuple.Validate(); err == nil {
		t.Error("one value for a tuple input of two symbols is refused")
	}
}

// TestOrDefaultJoinValidate pins the shape checks the text alone decides,
// Validate's: at least one header variable, at least two branches, and
// required and output sets disjoint. The header's binding checks are
// InferHeaders', read under the rule set: every branch binds every output,
// and a branch reads nothing outside the header.
func TestOrDefaultJoinValidate(t *testing.T) {
	ground := func(sym string) Clause {
		return &Expression{
			Function: &GroundFunction{Value: int64(0)},
			Binding:  datalog.NewSymbol(sym),
		}
	}
	refused := func(t *testing.T, clause Clause, parts ...string) {
		t.Helper()
		q := &Query{
			Find:  []FindElement{FindVariable{Symbol: datalog.NewSymbol("?e")}},
			Where: []Clause{scopePattern("?e", "a/n", "_"), clause},
		}
		_, err := InferHeaders(q, nil)
		if err == nil {
			t.Fatalf("the query is inferred; it is refused naming %q", parts)
		}
		for _, part := range parts {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal %q does not carry %q", err, part)
			}
		}
	}

	t.Run("valid per-group form", func(t *testing.T) {
		clause := &OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			OutputVars:   []Symbol{datalog.NewSymbol("?out")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{ground("?out")},
			},
		}
		if err := clause.Validate(); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("a branch missing an output is refused at inference", func(t *testing.T) {
		refused(t, &OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			OutputVars:   []Symbol{datalog.NewSymbol("?out")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{scopePattern("?e", "a/c", "?other")},
			},
		}, "(or-default-join [[?e] ?out]", "branch 2 does not bind ?out", "move ?out into the required run")
	})

	t.Run("a flat header output the ground branch does not bind is refused at inference", func(t *testing.T) {
		refused(t, &OrDefaultJoinClause{
			OutputVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{ground("?out")},
			},
		}, "(or-default-join [?e]", "branch 2 does not bind ?e", "move ?e into the required run")
	})

	t.Run("a branch reading a variable outside the header is refused at inference", func(t *testing.T) {
		refused(t, &OrDefaultJoinClause{
			OutputVars: []Symbol{datalog.NewSymbol("?out")},
			Branches: [][]Clause{
				{scopePattern("?d", "a/b", "?out")},
				{&Expression{
					Function: &GetElseFunction{
						Entity:  VariableTerm{Symbol: datalog.NewSymbol("?e")},
						Attr:    datalog.NewKeyword(":a/b"),
						Default: int64(0),
					},
					Binding: datalog.NewSymbol("?out"),
				}},
			},
		}, "(or-default-join [?out]", "?e")
	})

	t.Run("required and output overlap errors", func(t *testing.T) {
		clause := &OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?out")},
			OutputVars:   []Symbol{datalog.NewSymbol("?out")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{ground("?out")},
			},
		}
		if err := clause.Validate(); err == nil {
			t.Error("expected error: ?out is both required and output")
		}
	})

	t.Run("a header of required variables alone is a per-group filter", func(t *testing.T) {
		clause := &OrDefaultJoinClause{
			RequiredVars: []Symbol{datalog.NewSymbol("?e")},
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{ground("?out")},
			},
		}
		if err := clause.Validate(); err != nil {
			t.Errorf("a header declaring required variables and no output is refused: %v", err)
		}
	})

	t.Run("an empty header errors", func(t *testing.T) {
		clause := &OrDefaultJoinClause{
			Branches: [][]Clause{
				{scopePattern("?e", "a/b", "?out")},
				{ground("?out")},
			},
		}
		if err := clause.Validate(); err == nil {
			t.Error("expected error: no variable declared in the header")
		}
	})
}

func TestOrJoinClauseValidate(t *testing.T) {
	branches := [][]Clause{
		{scopePattern("?e", "a/b", "?v")},
		{scopePattern("?e", "a/c", "?v")},
	}

	t.Run("a header of a required run and outputs over two branches is valid", func(t *testing.T) {
		clause := &OrJoinClause{RequiredVars: []Symbol{datalog.NewSymbol("?e")}, OutputVars: []Symbol{datalog.NewSymbol("?v")}, Branches: branches}
		if err := clause.Validate(); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("a header of a required run alone is valid", func(t *testing.T) {
		clause := &OrJoinClause{RequiredVars: []Symbol{datalog.NewSymbol("?e")}, Branches: branches}
		if err := clause.Validate(); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("an empty header errors", func(t *testing.T) {
		clause := &OrJoinClause{Branches: branches}
		if err := clause.Validate(); err == nil {
			t.Error("expected error: no variable declared in the header")
		}
	})

	t.Run("a header over one branch is valid", func(t *testing.T) {
		clause := &OrJoinClause{OutputVars: []Symbol{datalog.NewSymbol("?e")}, Branches: branches[:1]}
		if err := clause.Validate(); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("no branch errors", func(t *testing.T) {
		clause := &OrJoinClause{OutputVars: []Symbol{datalog.NewSymbol("?e")}}
		if err := clause.Validate(); err == nil {
			t.Error("expected error: an or-join has at least one branch")
		}
	})

	t.Run("required and output overlap errors", func(t *testing.T) {
		clause := &OrJoinClause{RequiredVars: []Symbol{datalog.NewSymbol("?v")}, OutputVars: []Symbol{datalog.NewSymbol("?v")}, Branches: branches}
		if err := clause.Validate(); err == nil {
			t.Error("expected error: ?v is both required and output")
		}
	})
}

// ScopeOf reads a header. A headerless form, an or, a not or an or-default,
// is refused as an unknown clause is: InferHeaders rewrites every one before
// a reader of the scope sees the query (design §5.1).
func TestScopeOf_RefusesAHeaderlessForm(t *testing.T) {
	branch := []Clause{scopePattern("?e", "a/b", "?v")}
	for _, form := range []struct {
		name   string
		clause Clause
	}{
		{"or", &OrClause{Branches: [][]Clause{branch, branch}}},
		{"not", &NotClause{Clauses: branch}},
		{"or-default", &OrDefaultClause{Branches: [][]Clause{branch, branch}}},
	} {
		t.Run(form.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("ScopeOf returned a scope for the headerless %s; a headerless form is refused as an unknown clause", form.name)
				}
			}()
			ScopeOf(form.clause, nil)
		})
	}
}

// TestFreeVariables pins the clause-list union: every Provides and
// Correlates, deduplicated, first-appearance order, scoped variables absent.
func TestFreeVariables(t *testing.T) {
	free := FreeVariables([]Clause{
		scopePattern("?e", "a/b", "?v"),
		&OrJoinClause{
			OutputVars: []Symbol{datalog.NewSymbol("?v")},
			Branches:   [][]Clause{{scopePattern("?local", "c/d", "?v")}},
		},
		&Comparison{
			Op:    datalog.SymLT,
			Left:  VariableTerm{Symbol: datalog.NewSymbol("?v")},
			Right: ConstantTerm{Value: int64(3)},
		},
	}, nil)
	assertSymbols(t, "FreeVariables", free, "?e", "?v")
}
