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

// TestDefaultSourceAttributes pins the roster DefaultSourceAttributes
// collects: every attribute the query names on the default source, in order
// of first appearance and each once, through NOT/NOT-JOIN bodies,
// OR/OR-JOIN/OR-DEFAULT branches and subquery bodies, through the database
// functions as an expression and as a predicate, and through the :find pull
// pattern. A pattern on a named source reads that source's attributes, a
// variable or blank in the attribute position names none, and an expression
// that is not a database function names none.
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
			&NotClause{Clauses: []Clause{walkPattern("not")}},
			&NotJoinClause{Clauses: []Clause{walkPattern("notjoin")}},
			&OrClause{Branches: [][]Clause{{walkPattern("or1")}, {walkPattern("or2")}}},
			&OrJoinClause{Branches: [][]Clause{{walkPattern("orjoin")}}},
			&OrDefaultClause{Branches: [][]Clause{{walkPattern("ordefault")}}},
			&OrDefaultJoinClause{Branches: [][]Clause{{walkPattern("ordefaultjoin")}}},
			&SubqueryPattern{
				Query: &Query{Where: []Clause{
					walkPattern("sub"),
					&NotClause{Clauses: []Clause{walkPattern("subnot")}},
				}},
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
	}
	assertKeywordsInOrder(t, q.DefaultSourceAttributes(), want)
}

// A query's roster is computed once, on the first call, and every later call
// returns it.
func TestDefaultSourceAttributesComputedOnce(t *testing.T) {
	q := &Query{Where: []Clause{walkPattern("top"), walkPattern("other")}}
	first := q.DefaultSourceAttributes()
	second := q.DefaultSourceAttributes()
	assertKeywordsInOrder(t, first, []datalog.Keyword{datalog.NewKeyword(":t/top"), datalog.NewKeyword(":t/other")})
	if &first[0] != &second[0] {
		t.Errorf("every call returns the roster the first call built; got two rosters")
	}
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
