package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A scope's clauses are read against the sources its call names: every data
// pattern at any depth on an inner source, nil standing for $, names the
// call's source in its place, every nested call passing that inner source
// passes the call's source, a nested inner query keeps its own sources, and
// clauses on no renamed source come back as they are.
func TestRenameSources_RenamesEveryPatternAndNestedCallAtAnyDepth(t *testing.T) {
	inner, outer, deeper := datalog.NewSymbol("$t"), datalog.NewSymbol("$other"), datalog.NewSymbol("$u")
	e, n := datalog.NewSymbol("?e"), datalog.NewSymbol("?n")
	pattern := func(source Symbol) *DataPattern {
		return &DataPattern{Source: source, Elements: []PatternElement{Variable{Name: e}, Constant{Value: datalog.NewKeyword(":p/name")}, Variable{Name: n}}}
	}
	nested := &Query{Find: []FindElement{FindVariable{Symbol: n}}, In: []InputSpec{DatabaseInput{Name: deeper}}, Where: []Clause{pattern(deeper)}}
	sources, err := SubstitutionOf([]Symbol{inner}, []Symbol{outer})
	if err != nil {
		t.Fatal(err)
	}

	renamed := RenameSources([]Clause{
		pattern(inner),
		&OrJoinClause{OutputVars: []Symbol{n}, Branches: [][]Clause{{pattern(inner)}, {pattern(nil)}}},
		&NotClause{Clauses: []Clause{pattern(inner)}},
		&SubqueryPattern{Query: nested, Inputs: []PatternElement{Constant{Value: inner}}, Binding: RelationBinding{Variables: []Symbol{n}}},
	}, sources)
	if got := renamed[0].(*DataPattern).Source; got != outer {
		t.Errorf("a pattern on $t names $other; got %v", got)
	}
	branches := renamed[1].(*OrJoinClause).Branches
	if got := branches[0][0].(*DataPattern).Source; got != outer {
		t.Errorf("a pattern on $t inside a branch names $other; got %v", got)
	}
	if got := branches[1][0].(*DataPattern).Source; got != nil {
		t.Errorf("a pattern on the default source is not renamed when the call names $t alone; got %v", got)
	}
	if got := renamed[2].(*NotClause).Clauses[0].(*DataPattern).Source; got != outer {
		t.Errorf("a pattern on $t under a not names $other; got %v", got)
	}
	call := renamed[3].(*SubqueryPattern)
	if got := call.Inputs[0].(Constant).Value; got != outer {
		t.Errorf("a nested call passing $t passes $other; got %v", got)
	}
	if call.Query != nested {
		t.Error("a nested inner query keeps its own sources")
	}

	byDefault, err := SubstitutionOf([]Symbol{datalog.SymDollar}, []Symbol{outer})
	if err != nil {
		t.Fatal(err)
	}
	defaulted := RenameSources([]Clause{pattern(nil), pattern(datalog.SymDollar)}, byDefault)
	for i, clause := range defaulted {
		if got := clause.(*DataPattern).Source; got != outer {
			t.Errorf("pattern %d on the default source names $other when the inner :in names $; got %v", i, got)
		}
	}

	same := []Clause{pattern(nil), pattern(deeper)}
	if got := RenameSources(same, sources); got[0] != same[0] || got[1] != same[1] {
		t.Error("clauses on no renamed source come back as they are")
	}
}

// A call's sources are read once from its text: each inner :in source, the $
// of an omitted :in included, stands for the constant source the call passes
// at its place when that differs from the inner name. A source passed under
// its own name, a scalar position, and a variable argument name no source.
func TestCallSources_PairsEachInnerSourceWithTheSourceItsCallPasses(t *testing.T) {
	inner, outer, deeper := datalog.NewSymbol("$t"), datalog.NewSymbol("$other"), datalog.NewSymbol("$u")
	k, v := datalog.NewSymbol("?k"), datalog.NewSymbol("?v")
	finding := &Query{Find: []FindElement{FindVariable{Symbol: v}}}
	renames := func(call *SubqueryPattern, from Symbol) (Symbol, bool) {
		return CallSources(call).Of(from)
	}

	declared := &SubqueryPattern{
		Query:   &Query{Find: finding.Find, In: []InputSpec{DatabaseInput{Name: inner}, ScalarInput{Symbol: k}, DatabaseInput{Name: deeper}}},
		Inputs:  []PatternElement{Constant{Value: outer}, Constant{Value: datalog.NewKeyword(":t/x")}, Constant{Value: deeper}},
		Binding: RelationBinding{Variables: []Symbol{v}},
	}
	if to, ok := renames(declared, inner); !ok || to != outer {
		t.Errorf("$t given $other stands for $other; got %v, %v", to, ok)
	}
	if to, ok := renames(declared, deeper); ok {
		t.Errorf("$u given $u is not renamed; got %v", to)
	}
	if to, ok := renames(declared, k); ok {
		t.Errorf("a scalar position names no source; got %v", to)
	}

	omitted := &SubqueryPattern{
		Query:   &Query{Find: finding.Find},
		Inputs:  []PatternElement{Constant{Value: outer}},
		Binding: RelationBinding{Variables: []Symbol{v}},
	}
	if to, ok := renames(omitted, datalog.SymDollar); !ok || to != outer {
		t.Errorf("the $ of an omitted :in given $other stands for $other; got %v, %v", to, ok)
	}

	byVariable := &SubqueryPattern{
		Query:   &Query{Find: finding.Find, In: []InputSpec{DatabaseInput{Name: inner}}},
		Inputs:  []PatternElement{Variable{Name: v}},
		Binding: RelationBinding{Variables: []Symbol{v}},
	}
	if to, ok := renames(byVariable, inner); ok {
		t.Errorf("a variable argument names no source; got %v", to)
	}
}
