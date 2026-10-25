package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A data pattern naming one variable at several positions names one value,
// and UnifyPatterns writes it so: the pattern over distinct variables, each
// later occurrence a UnifiedVariable, followed by the equality of each with
// the first, which selects the datoms whose positions agree. A blank is no
// variable and is never equated, a pattern at any depth is reached, and a
// query holding no such pattern is returned as it is.

var (
	unifyX = datalog.NewSymbol("?x")
	unifyV = datalog.NewSymbol("?v")
	unifyP = Constant{Value: datalog.NewKeyword(":p")}
)

// unifiedClauses asserts that clauses are one pattern followed by one
// equality per later occurrence, and returns the pattern and the equalities.
func unifiedClauses(t *testing.T, clauses []Clause, equalities int) (*DataPattern, []*Comparison) {
	t.Helper()
	if len(clauses) != 1+equalities {
		t.Fatalf("want a pattern and %d equalities, got %d clauses: %v", equalities, len(clauses), clauses)
	}
	pattern, ok := clauses[0].(*DataPattern)
	if !ok {
		t.Fatalf("the first clause is the pattern, got %T", clauses[0])
	}
	var found []*Comparison
	for _, clause := range clauses[1:] {
		equality, ok := clause.(*Comparison)
		if !ok || equality.Op != datalog.SymEQ {
			t.Fatalf("a later clause is an equality, got %v", clause)
		}
		found = append(found, equality)
	}
	return pattern, found
}

// equates asserts that the equality holds first and the unified variable.
func equates(t *testing.T, equality *Comparison, first, unified Symbol) {
	t.Helper()
	left, ok := equality.Left.(VariableTerm)
	right, ok2 := equality.Right.(VariableTerm)
	if !ok || !ok2 || left.Symbol != first || right.Symbol != unified {
		t.Errorf("want [(= %s %s)], got %v", first, unified, equality)
	}
}

func TestUnifyPatternsWritesALaterOccurrenceAsAUnifiedVariableAndAnEquality(t *testing.T) {
	t.Run("the entity and the value", func(t *testing.T) {
		q := &Query{Where: []Clause{&DataPattern{Elements: []PatternElement{Variable{unifyX}, unifyP, Variable{unifyX}}}}}
		pattern, equalities := unifiedClauses(t, UnifyPatterns(q).Where, 1)
		unified := UnifiedVariable(unifyX, 2)
		if pattern.Elements[0] != (Variable{unifyX}) || pattern.Elements[2] != (Variable{unified}) {
			t.Errorf("the first occurrence stays and the later is %s, got %v", unified, pattern)
		}
		equates(t, equalities[0], unifyX, unified)
	})
	t.Run("the entity and the attribute", func(t *testing.T) {
		q := &Query{Where: []Clause{&DataPattern{Elements: []PatternElement{Variable{unifyX}, Variable{unifyX}, Variable{unifyV}}}}}
		pattern, equalities := unifiedClauses(t, UnifyPatterns(q).Where, 1)
		unified := UnifiedVariable(unifyX, 1)
		if pattern.Elements[1] != (Variable{unified}) || pattern.Elements[2] != (Variable{unifyV}) {
			t.Errorf("the attribute position is %s and the value stays, got %v", unified, pattern)
		}
		equates(t, equalities[0], unifyX, unified)
	})
	t.Run("three occurrences", func(t *testing.T) {
		q := &Query{Where: []Clause{&DataPattern{Elements: []PatternElement{Variable{unifyX}, unifyP, Variable{unifyX}, Variable{unifyX}}}}}
		pattern, equalities := unifiedClauses(t, UnifyPatterns(q).Where, 2)
		if pattern.Elements[2] != (Variable{UnifiedVariable(unifyX, 2)}) || pattern.Elements[3] != (Variable{UnifiedVariable(unifyX, 3)}) {
			t.Errorf("each later occurrence is its own unified variable, got %v", pattern)
		}
		equates(t, equalities[0], unifyX, UnifiedVariable(unifyX, 2))
		equates(t, equalities[1], unifyX, UnifiedVariable(unifyX, 3))
	})
	t.Run("the source is kept", func(t *testing.T) {
		source := datalog.NewSymbol("$other")
		q := &Query{Where: []Clause{&DataPattern{Source: source, Elements: []PatternElement{Variable{unifyX}, unifyP, Variable{unifyX}}}}}
		pattern, _ := unifiedClauses(t, UnifyPatterns(q).Where, 1)
		if pattern.Source != source {
			t.Errorf("the rewritten pattern reads the same source, got %v", pattern.Source)
		}
	})
}

func TestUnifyPatternsLeavesBlanksAndDistinctVariablesAsTheyAre(t *testing.T) {
	blanks := &DataPattern{Elements: []PatternElement{Blank{}, unifyP, Blank{}}}
	distinct := &DataPattern{Elements: []PatternElement{Variable{unifyX}, unifyP, Variable{unifyV}}}
	q := &Query{Where: []Clause{blanks, distinct}}
	if UnifyPatterns(q) != q {
		t.Errorf("a query holding no pattern naming a variable twice is returned as it is")
	}
	if clauses := unifyPattern(blanks); len(clauses) != 1 || clauses[0] != Clause(blanks) {
		t.Errorf("a pattern of blanks is itself, got %v", clauses)
	}
	if clauses := unifyPattern(distinct); len(clauses) != 1 || clauses[0] != Clause(distinct) {
		t.Errorf("a pattern of distinct variables is itself, got %v", clauses)
	}
	equality := &Comparison{Op: datalog.SymEQ, Left: VariableTerm{Symbol: unifyX}, Right: VariableTerm{Symbol: unifyV}}
	if clauses := unifyClause(equality); len(clauses) != 1 || clauses[0] != Clause(equality) {
		t.Errorf("a clause that is no pattern and no form is itself, got %v", clauses)
	}
	list := []Clause{blanks, distinct, equality}
	if out, changed := unifyList(list); changed || len(out) != len(list) || &out[0] != &list[0] {
		t.Errorf("a list holding no pattern naming a variable twice is the same slice, got %v, changed %t", out, changed)
	}
	branches := [][]Clause{{blanks}, {distinct}}
	if out, changed := unifyBranches(branches); changed || len(out) != len(branches) || &out[0] != &branches[0] {
		t.Errorf("branches holding no such pattern are the same slice, got %v, changed %t", out, changed)
	}
}

func TestUnifyPatternsReachesABranchAndAnInnerQuery(t *testing.T) {
	repeated := func() *DataPattern {
		return &DataPattern{Elements: []PatternElement{Variable{unifyX}, unifyP, Variable{unifyX}}}
	}
	unified := UnifiedVariable(unifyX, 2)

	t.Run("an or-join branch", func(t *testing.T) {
		q := &Query{Where: []Clause{&OrJoinClause{OutputVars: []Symbol{unifyX}, Branches: [][]Clause{{repeated()}, {&DataPattern{Elements: []PatternElement{Variable{unifyX}, unifyP, Variable{unifyV}}}}}}}}
		form, ok := UnifyPatterns(q).Where[0].(*OrJoinClause)
		if !ok {
			t.Fatalf("the form stays an or-join, got %T", UnifyPatterns(q).Where[0])
		}
		pattern, equalities := unifiedClauses(t, form.Branches[0], 1)
		if pattern.Elements[2] != (Variable{unified}) {
			t.Errorf("the branch's pattern is rewritten, got %v", pattern)
		}
		equates(t, equalities[0], unifyX, unified)
		if len(form.Branches[1]) != 1 {
			t.Errorf("the other branch is as it was, got %v", form.Branches[1])
		}
	})
	t.Run("a not-join body", func(t *testing.T) {
		q := &Query{Where: []Clause{&NotJoinClause{RequiredVars: []Symbol{unifyX}, Clauses: []Clause{repeated()}}}}
		form, ok := UnifyPatterns(q).Where[0].(*NotJoinClause)
		if !ok {
			t.Fatalf("the form stays a not-join, got %T", UnifyPatterns(q).Where[0])
		}
		pattern, _ := unifiedClauses(t, form.Clauses, 1)
		if pattern.Elements[2] != (Variable{unified}) {
			t.Errorf("the body's pattern is rewritten, got %v", pattern)
		}
	})
	t.Run("an or-default-join branch", func(t *testing.T) {
		q := &Query{Where: []Clause{&OrDefaultJoinClause{OutputVars: []Symbol{unifyX}, Branches: [][]Clause{{repeated()}}}}}
		form, ok := UnifyPatterns(q).Where[0].(*OrDefaultJoinClause)
		if !ok {
			t.Fatalf("the form stays an or-default-join, got %T", UnifyPatterns(q).Where[0])
		}
		pattern, _ := unifiedClauses(t, form.Branches[0], 1)
		if pattern.Elements[2] != (Variable{unified}) {
			t.Errorf("the branch's pattern is rewritten, got %v", pattern)
		}
	})
	t.Run("a subquery's inner query", func(t *testing.T) {
		inner := &Query{Find: []FindElement{FindVariable{Symbol: unifyX}}, Where: []Clause{repeated()}}
		q := &Query{Where: []Clause{&SubqueryPattern{Query: inner, Inputs: []PatternElement{Variable{datalog.SymDollar}}, Binding: CollectionBinding{Variable: unifyV}}}}
		call, ok := UnifyPatterns(q).Where[0].(*SubqueryPattern)
		if !ok {
			t.Fatalf("the clause stays a subquery, got %T", UnifyPatterns(q).Where[0])
		}
		pattern, _ := unifiedClauses(t, call.Query.Where, 1)
		if pattern.Elements[2] != (Variable{unified}) {
			t.Errorf("the inner query's pattern is rewritten, got %v", pattern)
		}
		if call.Query == inner || len(call.Query.Find) != 1 {
			t.Errorf("the inner query is derived over its rewritten where with its find, got %v", call.Query)
		}
	})
}
