package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// assertKeywordsInOrder compares two attribute rosters position by position.
// Keywords are interned, so the comparison is pointer equality.
func assertKeywordsInOrder(t *testing.T, got, want []datalog.Keyword) {
	if len(got) != len(want) {
		t.Fatalf("collected %d attributes, want %d:\n got  %v\n want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attribute %d is %s, want %s", i, got[i], want[i])
		}
	}
}

// TestDefaultSourceAttributes pins the roster of the default source: every
// keyword constant written in the attribute position of a pattern on it, in
// order of first appearance and each once, through NOT/NOT-JOIN bodies,
// OR/OR-JOIN/OR-DEFAULT branches and subquery bodies, with the :find pull
// pattern's attributes first and the database functions' as an expression
// and as a predicate in clause order. A pattern on a named source names
// nothing on it, and a blank or a variable in the attribute position names
// nothing, whatever binds the variable: the fixture binds such variables
// through ground, identity of a keyword, identity of a variable, a tuple
// ground, a form's header and an identity alias inside a branch, and none of
// those keywords is named.
func TestDefaultSourceAttributes(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	entityTerm := VariableTerm{Symbol: datalog.NewSymbol("?e")}
	q := &Query{
		Find: []FindElement{
			FindVariable{Symbol: datalog.NewSymbol("?e")},
			FindPull{Variable: datalog.NewSymbol("?e"), Pattern: &PullPattern{Specs: []PullAttrSpec{
				&PullAttribute{Attr: datalog.NewKeyword(":t/pulled")},
			}}},
		},
		Where: []Clause{
			walkPattern("top"),
			&DataPattern{Source: datalog.SymDollar, Elements: []PatternElement{
				entity, Constant{Value: datalog.NewKeyword(":t/dollar")}, Blank{},
			}},
			&DataPattern{Source: datalog.NewSymbol("$cache"), Elements: []PatternElement{
				entity, Constant{Value: datalog.NewKeyword(":t/named")}, Blank{},
			}},
			&DataPattern{Elements: []PatternElement{
				entity, Variable{Name: datalog.NewSymbol("?a")}, Blank{},
			}},
			&DataPattern{Elements: []PatternElement{entity, Blank{}, Blank{}}},
			walkPattern("top"),
			&NotClause{Clauses: []Clause{scopePattern("?e", "t/not", "_")}},
			&NotJoinClause{Clauses: []Clause{walkPattern("notjoin")}},
			&OrClause{Branches: [][]Clause{{scopePattern("?e", "t/or1", "_")}, {scopePattern("?e", "t/or2", "_")}}},
			&OrJoinClause{Branches: [][]Clause{{walkPattern("orjoin")}}},
			&OrDefaultClause{Branches: [][]Clause{{scopePattern("?e", "t/ordefault", "_")}}},
			&OrDefaultJoinClause{Branches: [][]Clause{{walkPattern("ordefaultjoin")}}},
			&SubqueryPattern{
				Query: &Query{Where: []Clause{
					walkPattern("sub"),
					&NotClause{Clauses: []Clause{scopePattern("?sub", "t/subnot", "_")}},
				}},
				Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
				Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
			},
			&Expression{Function: GroundFunction{Value: int64(1)}, Binding: datalog.NewSymbol("?g")},
			&Comparison{Op: datalog.SymLT, Left: ConstantTerm{Value: int64(1)}, Right: ConstantTerm{Value: int64(2)}},
			&Expression{
				Function: &GetElseFunction{Entity: entityTerm, Attr: datalog.NewKeyword(":t/getelse"), Default: "none"},
				Binding:  datalog.NewSymbol("?ge"),
			},
			&Expression{
				Function: &GetSomeFunction{Entity: entityTerm, Attrs: []datalog.Keyword{
					datalog.NewKeyword(":t/some1"), datalog.NewKeyword(":t/some2"),
				}},
				Binding: TupleBinding{Variables: []Symbol{datalog.NewSymbol("?sa"), datalog.NewSymbol("?sv")}},
			},
			&Expression{
				Function: &MissingFunction{Entity: entityTerm, Attr: datalog.NewKeyword(":t/missingexpr")},
				Binding:  datalog.NewSymbol("?m"),
			},
			&DatabaseFunctionPredicate{
				Function: &MissingFunction{Entity: entityTerm, Attr: datalog.NewKeyword(":t/missingpred")},
			},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/ground")}, Binding: datalog.NewSymbol("?ga")},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ga")}, Blank{}}},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ia")}, Blank{}}},
			&Expression{Function: IdentityFunction{Arg: ConstantTerm{Value: datalog.NewKeyword(":t/identity")}}, Binding: datalog.NewSymbol("?ia")},
			&Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: datalog.NewSymbol("?ga")}}, Binding: datalog.NewSymbol("?aa")},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?aa")}, Blank{}}},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/unused")}, Binding: datalog.NewSymbol("?gu")},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/local")}, Binding: datalog.NewSymbol("?la")},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/shared")}, Binding: datalog.NewSymbol("?sh")},
			&OrJoinClause{OutputVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?sh")}, Branches: [][]Clause{
				{&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?la")}, Variable{Name: datalog.NewSymbol("?sh")}}}},
				{&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?sh")}, Blank{}}}},
			}},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/negated")}, Binding: datalog.NewSymbol("?na")},
			&NotClause{Clauses: []Clause{
				&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?na")}, Blank{}}},
			}},
			&Expression{
				Function: GroundFunction{Value: []interface{}{datalog.NewKeyword(":t/tuple"), datalog.NewKeyword(":t/x")}},
				Binding:  TupleBinding{Variables: []Symbol{datalog.NewSymbol("?ta"), datalog.NewSymbol("?tb")}},
			},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ta")}, Variable{Name: datalog.NewSymbol("?tb")}}},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/inferred")}, Binding: datalog.NewSymbol("?oa")},
			&OrClause{Branches: [][]Clause{
				{&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?oa")}, Blank{}}}},
				{&DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/orother")}, Blank{}}}},
			}},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/fallen")}, Binding: datalog.NewSymbol("?fa")},
			&OrDefaultClause{Branches: [][]Clause{
				{&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?fa")}, Blank{}}}},
				{&DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/fallback")}, Blank{}}}},
			}},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/aliased")}, Binding: datalog.NewSymbol("?sa")},
			&OrJoinClause{RequiredVars: []Symbol{datalog.NewSymbol("?sa")}, OutputVars: []Symbol{datalog.NewSymbol("?e")}, Branches: [][]Clause{{
				&Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: datalog.NewSymbol("?sa")}}, Binding: datalog.NewSymbol("?al")},
				&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?al")}, Blank{}}},
			}}},
		},
	}

	want := []datalog.Keyword{
		datalog.NewKeyword(":t/pulled"),
		datalog.NewKeyword(":t/top"),
		datalog.NewKeyword(":t/dollar"),
		datalog.NewKeyword(":t/not"),
		datalog.NewKeyword(":t/notjoin"),
		datalog.NewKeyword(":t/or1"),
		datalog.NewKeyword(":t/or2"),
		datalog.NewKeyword(":t/orjoin"),
		datalog.NewKeyword(":t/ordefault"),
		datalog.NewKeyword(":t/ordefaultjoin"),
		datalog.NewKeyword(":t/sub"),
		datalog.NewKeyword(":t/subnot"),
		datalog.NewKeyword(":t/getelse"),
		datalog.NewKeyword(":t/some1"),
		datalog.NewKeyword(":t/some2"),
		datalog.NewKeyword(":t/missingexpr"),
		datalog.NewKeyword(":t/missingpred"),
		datalog.NewKeyword(":t/orother"),
		datalog.NewKeyword(":t/fallback"),
	}
	inferred, err := InferHeaders(q, nil)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	assertKeywordsInOrder(t, CollectRosters(inferred).SourceAttributes(datalog.SymDollar), want)
}

// A source no pattern names has no roster: SourceAttributes answers nil for
// it, beside the roster of a source a pattern names.
func TestSourceAttributesOfASourceNoPatternNames(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	cache := datalog.NewSymbol("$cache")
	rosters := CollectRosters(&Query{Where: []Clause{
		walkPattern("top"),
		&DataPattern{Source: cache, Elements: []PatternElement{
			entity, Constant{Value: datalog.NewKeyword(":t/named")}, Blank{},
		}},
	}})
	assertKeywordsInOrder(t, rosters.SourceAttributes(cache), []datalog.Keyword{datalog.NewKeyword(":t/named")})
	if got := rosters.SourceAttributes(datalog.NewSymbol("$absent")); got != nil {
		t.Errorf("a source no pattern names has no roster, got %v", got)
	}
}

// TestSourceAttributes pins the roster SourceAttributes collects for a named
// source: every attribute a data pattern on that source names, in order of
// first appearance and each once, through NOT/NOT-JOIN bodies,
// OR/OR-JOIN/OR-DEFAULT branches and subquery bodies. A pattern on the
// default source or on another named source, a database function, which
// reads the default source, and a pull pattern, which reads it too, name
// nothing on this source.
func TestSourceAttributes(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	entityTerm := VariableTerm{Symbol: datalog.NewSymbol("?e")}
	cache := datalog.NewSymbol("$cache")
	on := func(source Symbol, attr string) *DataPattern {
		return &DataPattern{Source: source, Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(attr)}, Blank{}}}
	}
	q := &Query{
		Find: []FindElement{
			FindPull{Variable: datalog.NewSymbol("?e"), Pattern: &PullPattern{Specs: []PullAttrSpec{
				&PullAttribute{Attr: datalog.NewKeyword(":t/pulled")},
			}}},
		},
		Where: []Clause{
			on(cache, ":t/top"),
			walkPattern("default"),
			on(datalog.NewSymbol("$other"), ":t/other"),
			on(cache, ":t/top"),
			&NotClause{Clauses: []Clause{on(cache, ":t/not")}},
			&NotJoinClause{Clauses: []Clause{on(cache, ":t/notjoin")}},
			&OrClause{Branches: [][]Clause{{on(cache, ":t/or1")}, {on(cache, ":t/or2")}}},
			&OrJoinClause{Branches: [][]Clause{{on(cache, ":t/orjoin")}}},
			&OrDefaultClause{Branches: [][]Clause{{on(cache, ":t/ordefault")}}},
			&OrDefaultJoinClause{Branches: [][]Clause{{on(cache, ":t/ordefaultjoin")}}},
			&SubqueryPattern{
				Query:   &Query{Where: []Clause{on(cache, ":t/sub"), walkPattern("subdefault")}},
				Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
				Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
			},
			&Expression{
				Function: &GetElseFunction{Entity: entityTerm, Attr: datalog.NewKeyword(":t/getelse"), Default: "none"},
				Binding:  datalog.NewSymbol("?ge"),
			},
		},
	}

	want := []datalog.Keyword{
		datalog.NewKeyword(":t/top"),
		datalog.NewKeyword(":t/not"),
		datalog.NewKeyword(":t/notjoin"),
		datalog.NewKeyword(":t/or1"),
		datalog.NewKeyword(":t/or2"),
		datalog.NewKeyword(":t/orjoin"),
		datalog.NewKeyword(":t/ordefault"),
		datalog.NewKeyword(":t/ordefaultjoin"),
		datalog.NewKeyword(":t/sub"),
	}
	inferred, err := InferHeaders(q, nil)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	assertKeywordsInOrder(t, CollectRosters(inferred).SourceAttributes(cache), want)
}

// TestPullPatternAttributes pins the roster PullPatternAttributes collects:
// every attribute a pull pattern names, through nested map specs, in order of
// first appearance and each once. A wildcard names no attribute.
func TestPullPatternAttributes(t *testing.T) {
	pattern := &PullPattern{Specs: []PullAttrSpec{
		&PullAttribute{Attr: datalog.NewKeyword(":t/a")},
		&PullWildcard{},
		&PullMapSpec{Attr: datalog.NewKeyword(":t/ref"), Pattern: &PullPattern{Specs: []PullAttrSpec{
			&PullAttribute{Attr: datalog.NewKeyword(":t/nested")},
			&PullLimitExpr{Attr: datalog.NewKeyword(":t/lim"), Limit: 2},
			&PullDefaultExpr{Attr: datalog.NewKeyword(":t/def"), Default: "none"},
			&PullMapSpec{Attr: datalog.NewKeyword(":t/ref2"), Pattern: &PullPattern{Specs: []PullAttrSpec{
				&PullAttribute{Attr: datalog.NewKeyword(":t/a")},
			}}},
		}}},
	}}

	want := []datalog.Keyword{
		datalog.NewKeyword(":t/a"),
		datalog.NewKeyword(":t/ref"),
		datalog.NewKeyword(":t/nested"),
		datalog.NewKeyword(":t/lim"),
		datalog.NewKeyword(":t/def"),
		datalog.NewKeyword(":t/ref2"),
	}
	assertKeywordsInOrder(t, PullPatternAttributes(pattern), want)

	wildcardOnly := &PullPattern{Specs: []PullAttrSpec{&PullWildcard{}}}
	if got := PullPatternAttributes(wildcardOnly); len(got) != 0 {
		t.Errorf("a wildcard-only pattern names no attribute, got %v", got)
	}
}
