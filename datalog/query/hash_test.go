package query

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// structuralFixture is a query over every clause kind, every input kind, the
// three find kinds, the four expression binding forms and a pull pattern of
// every specification kind. With functionsAsPointers the arithmetic and
// string functions are held as pointers, as the parser builds them; without
// it they are values, as the builder makes them.
func structuralFixture(functionsAsPointers bool) *Query {
	e, v, w, n, k, tx := datalog.NewSymbol("?e"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w"), datalog.NewSymbol("?n"), datalog.NewSymbol("?k"), datalog.NewSymbol("?tx")
	name := datalog.NewKeyword(":n")
	pattern := func(x, y Symbol) *DataPattern {
		return &DataPattern{Elements: []PatternElement{Variable{Name: x}, Constant{Value: name}, Variable{Name: y}}}
	}
	var plus Function = ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{VariableTerm{Symbol: v}, ConstantTerm{Value: int64(1)}}}
	var concat Function = StringConcatFunction{Terms: []Term{VariableTerm{Symbol: v}, ConstantTerm{Value: "x"}}}
	if functionsAsPointers {
		plus = &ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{&VariableTerm{Symbol: v}, &ConstantTerm{Value: int64(1)}}}
		concat = &StringConcatFunction{Terms: []Term{&VariableTerm{Symbol: v}, &ConstantTerm{Value: "x"}}}
	}
	limit := 10
	return &Query{
		Find: []FindElement{
			FindVariable{Symbol: e},
			FindAggregate{Function: datalog.SymMax, Arg: v},
			FindPull{Variable: e, Pattern: &PullPattern{Specs: []PullAttrSpec{
				&PullAttribute{Attr: name},
				&PullWildcard{},
				&PullMapSpec{Attr: name, Pattern: &PullPattern{Specs: []PullAttrSpec{
					&PullLimitExpr{Attr: name, Limit: 2},
					&PullDefaultExpr{Attr: name, Default: int64(0)},
				}}},
			}}},
		},
		In: []InputSpec{
			DatabaseInput{Name: datalog.SymDollar},
			RulesInput{},
			ScalarInput{Symbol: k},
			CollectionInput{Symbol: w},
			TupleInput{Symbols: []Symbol{n}},
			RelationInput{Symbols: []Symbol{tx}},
		},
		Where: []Clause{
			pattern(e, v),
			&DataPattern{Source: datalog.NewSymbol("$other"), Elements: []PatternElement{Variable{Name: e}, Constant{Value: name}, Blank{}, Variable{Name: tx}}},
			&Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: v}, Right: ConstantTerm{Value: int64(1)}},
			&ChainedComparison{Op: datalog.SymLT, Terms: []Term{ConstantTerm{Value: int64(0)}, VariableTerm{Symbol: v}, ConstantTerm{Value: int64(100)}}},
			&NotEqualPredicate{Comparison: Comparison{Op: datalog.SymNE, Left: VariableTerm{Symbol: v}, Right: VariableTerm{Symbol: k}}},
			&GroundPredicate{Variables: []Symbol{v}},
			&MissingPredicate{Variables: []Symbol{w}},
			&StrStartsWithPredicate{Value: VariableTerm{Symbol: v}, Prefix: ConstantTerm{Value: "a"}},
			&FunctionPredicate{Fn: "my/pred", Args: []PatternElement{Variable{Name: v}, Constant{Value: int64(2)}}},
			&DatabaseFunctionPredicate{Function: &MissingFunction{Entity: VariableTerm{Symbol: e}, Attr: name}},
			&TxRangePredicate{TxVar: tx, Low: 1, High: 9},
			&Expression{Function: plus, Binding: n},
			&Expression{Function: concat, Binding: TupleBinding{Variables: []Symbol{w}}},
			&Expression{Function: EnumerateFunction{VecTerm: VariableTerm{Symbol: v}}, Binding: RelationBinding{Variables: []Symbol{k, w}}},
			&Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: e}, Attr: name, Default: "none"}, Binding: CollectionBinding{Variable: w}},
			&Expression{Function: GroundFunction{Value: []interface{}{int64(1), "two"}}},
			&NotClause{Clauses: []Clause{pattern(e, w)}},
			&NotJoinClause{RequiredVars: []Symbol{e}, Clauses: []Clause{pattern(e, w)}},
			&OrClause{Branches: [][]Clause{{pattern(e, v)}, {pattern(v, e)}}},
			&OrJoinClause{RequiredVars: []Symbol{e}, OutputVars: []Symbol{v}, Branches: [][]Clause{{pattern(e, v)}}},
			&OrDefaultClause{Branches: [][]Clause{{pattern(e, v)}, {&Expression{Function: GroundFunction{Value: int64(0)}, Binding: v}}}},
			&OrDefaultJoinClause{RequiredVars: []Symbol{e}, OutputVars: []Symbol{v}, Branches: [][]Clause{{pattern(e, v)}}},
			&SubqueryPattern{
				Query:   &Query{Find: []FindElement{FindVariable{Symbol: n}}, In: []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: e}}, Where: []Clause{pattern(e, n)}},
				Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: e}},
				Binding: ScalarBinding{Variable: n},
			},
			&RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []PatternElement{Variable{Name: e}, VectorConstant{Values: []interface{}{int64(1)}}}},
		},
		OrderBy: []OrderByClause{{Variable: v, Descending: true}},
		Limit:   &limit,
	}
}

// Two queries alike part for part hash alike and are equal, whichever way
// their functions are held.
func TestQueryIdentity_AlikeQueriesHashAlikeAndAreEqual(t *testing.T) {
	values, pointers := structuralFixture(false), structuralFixture(true)
	if values.Hash() != pointers.Hash() {
		t.Errorf("two queries alike part for part hash apart: %d and %d", values.Hash(), pointers.Hash())
	}
	if !values.Equal(pointers) || !pointers.Equal(values) {
		t.Error("two queries alike part for part are not equal")
	}
	if !values.Equal(values) {
		t.Error("a query is not equal to itself")
	}
}

// A query that differs from another in any one part hashes apart from it and
// is not equal to it: a find element, an input, a clause, a constant's value
// or type, a symbol, a binding, a function, the ordering, the limit, or the
// scalar marker.
func TestQueryIdentity_EveryPartDecidesIt(t *testing.T) {
	e, v, w, n, k := datalog.NewSymbol("?e"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w"), datalog.NewSymbol("?n"), datalog.NewSymbol("?k")
	name, other := datalog.NewKeyword(":n"), datalog.NewKeyword(":m")
	pattern := func(x, y Symbol) *DataPattern {
		return &DataPattern{Elements: []PatternElement{Variable{Name: x}, Constant{Value: name}, Variable{Name: y}}}
	}
	eleven := 11
	variations := []struct {
		name string
		vary func(q *Query)
	}{
		{"a find aggregate's predicate variable", func(q *Query) { q.Find[1] = FindAggregate{Function: datalog.SymMax, Arg: v, Predicate: w} }},
		{"a find aggregate's function", func(q *Query) { q.Find[1] = FindAggregate{Function: datalog.SymMin, Arg: v} }},
		{"a find variable", func(q *Query) { q.Find[0] = FindVariable{Symbol: v} }},
		{"a pull's attribute", func(q *Query) {
			q.Find[2] = FindPull{Variable: e, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: other}}}}
		}},
		{"a pull absent", func(q *Query) { q.Find[2] = FindPull{Variable: e} }},
		{"a find element removed", func(q *Query) { q.Find = q.Find[:2] }},
		{"an input's kind", func(q *Query) { q.In[2] = CollectionInput{Symbol: k} }},
		{"an input's symbol", func(q *Query) { q.In[2] = ScalarInput{Symbol: n} }},
		{"a rules input removed", func(q *Query) { q.In = append(q.In[:1:1], q.In[2:]...) }},
		{"a pattern's variable", func(q *Query) { q.Where[0] = pattern(e, w) }},
		{"a pattern's attribute", func(q *Query) {
			q.Where[0] = &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: other}, Variable{Name: v}}}
		}},
		{"a pattern's source", func(q *Query) {
			q.Where[0] = &DataPattern{Source: datalog.SymDollar, Elements: q.Where[0].(*DataPattern).Elements}
		}},
		{"a pattern's blank", func(q *Query) {
			q.Where[0] = &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: name}, Blank{}}}
		}},
		{"a constant's value", func(q *Query) {
			q.Where[2] = &Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: v}, Right: ConstantTerm{Value: int64(2)}}
		}},
		{"a constant's type", func(q *Query) {
			q.Where[2] = &Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: v}, Right: ConstantTerm{Value: float64(1)}}
		}},
		{"a comparison's operator", func(q *Query) {
			q.Where[2] = &Comparison{Op: datalog.SymLT, Left: VariableTerm{Symbol: v}, Right: ConstantTerm{Value: int64(1)}}
		}},
		{"a chained comparison's term", func(q *Query) {
			q.Where[3] = &ChainedComparison{Op: datalog.SymLT, Terms: []Term{ConstantTerm{Value: int64(0)}, VariableTerm{Symbol: w}, ConstantTerm{Value: int64(100)}}}
		}},
		{"a not-equal's term", func(q *Query) {
			q.Where[4] = &NotEqualPredicate{Comparison: Comparison{Op: datalog.SymNE, Left: VariableTerm{Symbol: v}, Right: VariableTerm{Symbol: w}}}
		}},
		{"a ground's variable", func(q *Query) { q.Where[5] = &GroundPredicate{Variables: []Symbol{w}} }},
		{"a missing's variable", func(q *Query) { q.Where[6] = &MissingPredicate{Variables: []Symbol{v}} }},
		{"a prefix", func(q *Query) {
			q.Where[7] = &StrStartsWithPredicate{Value: VariableTerm{Symbol: v}, Prefix: ConstantTerm{Value: "b"}}
		}},
		{"a predicate function's name", func(q *Query) {
			q.Where[8] = &FunctionPredicate{Fn: "my/other", Args: []PatternElement{Variable{Name: v}, Constant{Value: int64(2)}}}
		}},
		{"a database predicate's attribute", func(q *Query) {
			q.Where[9] = &DatabaseFunctionPredicate{Function: &MissingFunction{Entity: VariableTerm{Symbol: e}, Attr: other}}
		}},
		{"a transaction range's bound", func(q *Query) {
			q.Where[10] = &TxRangePredicate{TxVar: q.Where[10].(*TxRangePredicate).TxVar, Low: 1, High: 10}
		}},
		{"an arithmetic operator", func(q *Query) {
			q.Where[11] = &Expression{Function: ArithmeticFunction{Op: datalog.SymSubtract, Args: []Term{VariableTerm{Symbol: v}, ConstantTerm{Value: int64(1)}}}, Binding: n}
		}},
		{"an expression's binding symbol", func(q *Query) { q.Where[11] = &Expression{Function: q.Where[11].(*Expression).Function, Binding: k} }},
		{"an expression's binding form", func(q *Query) {
			q.Where[11] = &Expression{Function: q.Where[11].(*Expression).Function, Binding: TupleBinding{Variables: []Symbol{n}}}
		}},
		{"an expression's binding absent", func(q *Query) { q.Where[11] = &Expression{Function: q.Where[11].(*Expression).Function} }},
		{"a function's kind", func(q *Query) {
			q.Where[13] = &Expression{Function: FirstFunction{VecTerm: VariableTerm{Symbol: v}}, Binding: RelationBinding{Variables: []Symbol{k, w}}}
		}},
		{"a relation binding's variable", func(q *Query) {
			q.Where[13] = &Expression{Function: EnumerateFunction{VecTerm: VariableTerm{Symbol: v}}, Binding: RelationBinding{Variables: []Symbol{k, n}}}
		}},
		{"a get-else's default", func(q *Query) {
			q.Where[14] = &Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: e}, Attr: name, Default: "some"}, Binding: CollectionBinding{Variable: w}}
		}},
		{"a ground's vector", func(q *Query) {
			q.Where[15] = &Expression{Function: GroundFunction{Value: []interface{}{int64(1), "three"}}}
		}},
		{"a not's clause", func(q *Query) { q.Where[16] = &NotClause{Clauses: []Clause{pattern(e, v)}} }},
		{"a not-join's header", func(q *Query) {
			q.Where[17] = &NotJoinClause{RequiredVars: []Symbol{v}, Clauses: []Clause{pattern(e, w)}}
		}},
		{"an or's branch", func(q *Query) { q.Where[18] = &OrClause{Branches: [][]Clause{{pattern(e, v)}, {pattern(w, e)}}} }},
		{"an or's branch removed", func(q *Query) { q.Where[18] = &OrClause{Branches: [][]Clause{{pattern(e, v)}}} }},
		{"a form's kind", func(q *Query) { q.Where[18] = &OrDefaultClause{Branches: q.Where[18].(*OrClause).Branches} }},
		{"an or-join's outputs", func(q *Query) {
			q.Where[19] = &OrJoinClause{RequiredVars: []Symbol{e}, OutputVars: []Symbol{v, w}, Branches: [][]Clause{{pattern(e, v)}}}
		}},
		{"an or-default's fallback", func(q *Query) {
			q.Where[20] = &OrDefaultClause{Branches: [][]Clause{{pattern(e, v)}, {&Expression{Function: GroundFunction{Value: int64(1)}, Binding: v}}}}
		}},
		{"an or-default-join's required run", func(q *Query) {
			q.Where[21] = &OrDefaultJoinClause{OutputVars: []Symbol{v}, Branches: [][]Clause{{pattern(e, v)}}}
		}},
		{"a subquery's inner query", func(q *Query) {
			sub := q.Where[22].(*SubqueryPattern)
			q.Where[22] = &SubqueryPattern{Query: &Query{Find: sub.Query.Find, In: sub.Query.In, Where: []Clause{pattern(e, k)}}, Inputs: sub.Inputs, Binding: sub.Binding}
		}},
		{"a subquery's input", func(q *Query) {
			sub := q.Where[22].(*SubqueryPattern)
			q.Where[22] = &SubqueryPattern{Query: sub.Query, Inputs: []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: v}}, Binding: sub.Binding}
		}},
		{"a subquery's binding form", func(q *Query) {
			sub := q.Where[22].(*SubqueryPattern)
			q.Where[22] = &SubqueryPattern{Query: sub.Query, Inputs: sub.Inputs, Binding: CollectionBinding{Variable: n}}
		}},
		{"an invocation's predicate", func(q *Query) {
			q.Where[23] = &RuleInvocation{Name: datalog.NewSymbol("reaches"), Args: q.Where[23].(*RuleInvocation).Args}
		}},
		{"an invocation's vector argument", func(q *Query) {
			q.Where[23] = &RuleInvocation{Name: q.Where[23].(*RuleInvocation).Name, Args: []PatternElement{Variable{Name: e}, VectorConstant{Values: []interface{}{int64(2)}}}}
		}},
		{"a clause removed", func(q *Query) { q.Where = q.Where[:len(q.Where)-1] }},
		{"the ordering's direction", func(q *Query) { q.OrderBy = []OrderByClause{{Variable: v}} }},
		{"the ordering's variable", func(q *Query) { q.OrderBy = []OrderByClause{{Variable: w, Descending: true}} }},
		{"the limit", func(q *Query) { q.Limit = &eleven }},
		{"the limit absent", func(q *Query) { q.Limit = nil }},
		{"the scalar marker", func(q *Query) { q.ScalarReturn = true }},
	}
	base := structuralFixture(false)
	for _, variation := range variations {
		t.Run(variation.name, func(t *testing.T) {
			varied := structuralFixture(false)
			variation.vary(varied)
			if varied.Hash() == base.Hash() {
				t.Errorf("a query differing in %s hashes alike", variation.name)
			}
			if base.Equal(varied) || varied.Equal(base) {
				t.Errorf("a query differing in %s is equal", variation.name)
			}
		})
	}
}

// Every kind of clause, function, input, find element and binding is itself
// and no other: two of one kind over the same parts are alike, and two of
// different kinds over the same parts are not.
func TestQueryIdentity_EveryKindIsItself(t *testing.T) {
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	name := datalog.NewKeyword(":n")
	term, constant := VariableTerm{Symbol: v}, ConstantTerm{Value: int64(1)}
	pattern := &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: name}, Variable{Name: v}}}
	comparison := &Comparison{Op: datalog.SymLT, Left: term, Right: constant}
	chained := &ChainedComparison{Op: datalog.SymLT, Terms: []Term{constant, term}}
	functions := []Function{
		ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{term, constant}},
		StringConcatFunction{Terms: []Term{term, constant}},
		TimeExtractionFunction{Field: datalog.SymYear, TimeTerm: term},
		GroundFunction{Value: int64(1)},
		IdentityFunction{Arg: term},
		ComparisonFunction{Comparison: comparison},
		ChainedComparisonFunction{ChainedComparison: chained},
		AndFunction{Terms: []Symbol{v}},
		CustomFunction{Fn: "my/fn", Args: []Term{term, constant}},
		NthFunction{VecTerm: term, IndexTerm: constant},
		FirstFunction{VecTerm: term},
		LastFunction{VecTerm: term},
		LengthFunction{VecTerm: term},
		ContainsFunction{VecTerm: term, ValueTerm: constant},
		IndexOfFunction{VecTerm: term, ValueTerm: constant},
		SubvecFunction{VecTerm: term, StartTerm: constant, EndTerm: constant},
		EnumerateFunction{VecTerm: term},
		&GetElseFunction{Entity: term, Attr: name, Default: int64(1)},
		&MissingFunction{Entity: term, Attr: name},
		&GetSomeFunction{Entity: term, Attrs: []datalog.Keyword{name}},
	}
	expression := func(fn Function) *Query {
		return &Query{Where: []Clause{&Expression{Function: fn, Binding: e}}}
	}
	for i, a := range functions {
		for j, b := range functions {
			x, y := expression(a), expression(b)
			if (x.Hash() == y.Hash()) != (i == j) || x.Equal(y) != (i == j) {
				t.Errorf("functions %s and %s: hash alike %v, equal %v; want %v", a, b, x.Hash() == y.Hash(), x.Equal(y), i == j)
			}
		}
	}

	clauses := []Clause{
		pattern,
		comparison,
		chained,
		&NotEqualPredicate{Comparison: *comparison},
		&GroundPredicate{Variables: []Symbol{v}},
		&MissingPredicate{Variables: []Symbol{v}},
		&StrStartsWithPredicate{Value: term, Prefix: constant},
		&FunctionPredicate{Fn: "my/pred", Args: []PatternElement{Variable{Name: v}}},
		&DatabaseFunctionPredicate{Function: &MissingFunction{Entity: term, Attr: name}},
		&TxRangePredicate{TxVar: v, Low: 1, High: 2},
		&Expression{Function: IdentityFunction{Arg: term}, Binding: e},
		&NotClause{Clauses: []Clause{pattern}},
		&NotJoinClause{RequiredVars: []Symbol{e}, Clauses: []Clause{pattern}},
		&OrClause{Branches: [][]Clause{{pattern}}},
		&OrJoinClause{OutputVars: []Symbol{e, v}, Branches: [][]Clause{{pattern}}},
		&OrDefaultClause{Branches: [][]Clause{{pattern}}},
		&OrDefaultJoinClause{OutputVars: []Symbol{e, v}, Branches: [][]Clause{{pattern}}},
		&SubqueryPattern{Query: &Query{Find: []FindElement{FindVariable{Symbol: v}}, Where: []Clause{pattern}}, Binding: ScalarBinding{Variable: v}},
		&RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []PatternElement{Variable{Name: e}, Variable{Name: v}}},
	}
	for i, a := range clauses {
		for j, b := range clauses {
			x, y := &Query{Where: []Clause{a}}, &Query{Where: []Clause{b}}
			if (x.Hash() == y.Hash()) != (i == j) || x.Equal(y) != (i == j) {
				t.Errorf("clauses %s and %s: hash alike %v, equal %v; want %v", a, b, x.Hash() == y.Hash(), x.Equal(y), i == j)
			}
		}
	}

	inputs := []InputSpec{
		DatabaseInput{Name: datalog.SymDollar},
		ScalarInput{Symbol: v},
		CollectionInput{Symbol: v},
		TupleInput{Symbols: []Symbol{v}},
		RelationInput{Symbols: []Symbol{v}},
		RulesInput{},
	}
	for i, a := range inputs {
		for j, b := range inputs {
			x, y := &Query{In: []InputSpec{a}}, &Query{In: []InputSpec{b}}
			if (x.Hash() == y.Hash()) != (i == j) || x.Equal(y) != (i == j) {
				t.Errorf("inputs %s and %s: hash alike %v, equal %v; want %v", a, b, x.Hash() == y.Hash(), x.Equal(y), i == j)
			}
		}
	}

	finds := []FindElement{
		FindVariable{Symbol: v},
		FindAggregate{Function: datalog.SymCount, Arg: v},
		FindPull{Variable: v, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: name}}}},
	}
	for i, a := range finds {
		for j, b := range finds {
			x, y := &Query{Find: []FindElement{a}}, &Query{Find: []FindElement{b}}
			if (x.Hash() == y.Hash()) != (i == j) || x.Equal(y) != (i == j) {
				t.Errorf("find elements %s and %s: hash alike %v, equal %v; want %v", a, b, x.Hash() == y.Hash(), x.Equal(y), i == j)
			}
		}
	}

	bindings := []interface{}{nil, v, TupleBinding{Variables: []Symbol{v}}, CollectionBinding{Variable: v}, RelationBinding{Variables: []Symbol{v}}}
	for i, a := range bindings {
		for j, b := range bindings {
			x := &Query{Where: []Clause{&Expression{Function: IdentityFunction{Arg: term}, Binding: a}}}
			y := &Query{Where: []Clause{&Expression{Function: IdentityFunction{Arg: term}, Binding: b}}}
			if (x.Hash() == y.Hash()) != (i == j) || x.Equal(y) != (i == j) {
				t.Errorf("bindings %v and %v: hash alike %v, equal %v; want %v", a, b, x.Hash() == y.Hash(), x.Equal(y), i == j)
			}
		}
	}

	specs := []PullAttrSpec{
		&PullAttribute{Attr: name},
		&PullWildcard{},
		&PullMapSpec{Attr: name, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullWildcard{}}}},
		&PullLimitExpr{Attr: name, Limit: 1},
		&PullDefaultExpr{Attr: name, Default: int64(1)},
	}
	for i, a := range specs {
		for j, b := range specs {
			x := &Query{Find: []FindElement{FindPull{Variable: v, Pattern: &PullPattern{Specs: []PullAttrSpec{a}}}}}
			y := &Query{Find: []FindElement{FindPull{Variable: v, Pattern: &PullPattern{Specs: []PullAttrSpec{b}}}}}
			if (x.Hash() == y.Hash()) != (i == j) || x.Equal(y) != (i == j) {
				t.Errorf("pull specifications %s and %s: hash alike %v, equal %v; want %v", a, b, x.Hash() == y.Hash(), x.Equal(y), i == j)
			}
		}
	}
}

// A term's constant may be the vector literal or the blank the parser wraps
// as the term's value: two terms over one vector literal are alike, the
// literal decides the identity, and a blank term is itself.
func TestQueryIdentity_ATermCarriesAVectorLiteralOrABlank(t *testing.T) {
	v := datalog.NewSymbol("?v")
	contains := func(value interface{}) *Query {
		return &Query{Where: []Clause{&Expression{Function: ContainsFunction{VecTerm: ConstantTerm{Value: value}, ValueTerm: VariableTerm{Symbol: v}}}}}
	}
	one := contains(VectorConstant{Values: []interface{}{int64(1), int64(2)}})
	again := contains(VectorConstant{Values: []interface{}{int64(1), int64(2)}})
	if one.Hash() != again.Hash() || !one.Equal(again) {
		t.Error("two terms over one vector literal are not alike")
	}
	others := []interface{}{
		VectorConstant{Values: []interface{}{int64(1), int64(3)}},
		VectorConstant{Values: []interface{}{int64(1)}},
		Blank{},
		int64(1),
	}
	for _, other := range others {
		q := contains(other)
		if q.Hash() == one.Hash() || one.Equal(q) || q.Equal(one) {
			t.Errorf("a term over %v is alike to one over the vector literal [1 2]", other)
		}
	}
	blank, blankAgain := contains(Blank{}), contains(Blank{})
	if blank.Hash() != blankAgain.Hash() || !blank.Equal(blankAgain) {
		t.Error("two blank terms are not alike")
	}
}

// reachRules are the two rules of reach over :edge, built afresh on each
// call, with the second rule's body as given.
func reachRules(recursiveBody func(x, y, z Symbol, edge func(from, to Symbol) *DataPattern) []Clause) []*Rule {
	x, y, z := datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")
	edge := func(from, to Symbol) *DataPattern {
		return &DataPattern{Elements: []PatternElement{Variable{Name: from}, Constant{Value: datalog.NewKeyword(":edge")}, Variable{Name: to}}}
	}
	reach := datalog.NewSymbol("reach")
	return []*Rule{
		{Name: reach, Head: []Symbol{x, y}, Body: []Clause{edge(x, y)}},
		{Name: reach, Head: []Symbol{x, y}, Body: recursiveBody(x, y, z, edge)},
	}
}

// A rule set built twice from one text is one identity, and sets differing
// in a rule's body, in a predicate's required run, or in a seed are two.
func TestRuleSetIdentity(t *testing.T) {
	oneHop := func(x, y, z Symbol, edge func(from, to Symbol) *DataPattern) []Clause {
		return []Clause{edge(x, z), &RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []PatternElement{Variable{Name: z}, Variable{Name: y}}}}
	}
	twoHops := func(x, y, z Symbol, edge func(from, to Symbol) *DataPattern) []Clause {
		w := datalog.NewSymbol("?w")
		return []Clause{edge(x, w), edge(w, z), &RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []PatternElement{Variable{Name: z}, Variable{Name: y}}}}
	}
	set := func(t *testing.T, rules []*Rule, seeds ...SeedPredicate) *RuleSet {
		rs, err := NewSeededRuleSet(rules, seeds)
		if err != nil {
			t.Fatalf("NewSeededRuleSet: %v", err)
		}
		return rs
	}
	first, again := set(t, reachRules(oneHop)), set(t, reachRules(oneHop))
	if first.Hash() != again.Hash() {
		t.Errorf("two sets built from one text hash apart: %d and %d", first.Hash(), again.Hash())
	}
	if !first.Equal(again) || !again.Equal(first) || !first.Equal(first) {
		t.Error("two sets built from one text are not equal")
	}

	required := reachRules(oneHop)
	required[0].Required = []Symbol{required[0].Head[0]}
	required[1].Required = []Symbol{required[1].Head[0]}
	seeded := set(t, reachRules(oneHop), SeedPredicate{Name: datalog.NewSymbol("magic.b/reach"), Arity: 1})
	others := []struct {
		name string
		set  *RuleSet
	}{
		{"a rule's body", set(t, reachRules(twoHops))},
		{"a predicate's required run", set(t, required)},
		{"a seed", seeded},
		{"a seed's arity", set(t, reachRules(oneHop), SeedPredicate{Name: datalog.NewSymbol("magic.b/reach"), Arity: 2})},
	}
	for _, other := range others {
		t.Run(fmt.Sprintf("differing in %s", other.name), func(t *testing.T) {
			if other.set.Hash() == first.Hash() {
				t.Errorf("a set differing in %s hashes alike", other.name)
			}
			if first.Equal(other.set) || other.set.Equal(first) {
				t.Errorf("a set differing in %s is equal", other.name)
			}
		})
	}
	if seeded.Equal(others[3].set) || seeded.Hash() == others[3].set.Hash() {
		t.Error("two sets whose seeds differ in arity are alike")
	}
}
