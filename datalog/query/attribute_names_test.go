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
// functions as an expression and as a predicate, through the :find pull
// pattern, and, at the scope's end, through a variable in the attribute
// position that ground, identity of a keyword, or identity of such a
// variable binds to a keyword. A variable a nested form's branch binds
// outside the form's interface is the branch's own, so a ground outside the
// form does not reach it; a variable the interface shares, an or-join header
// variable or a variable of a not body, resolves against the enclosing
// scope. A pattern on a named source reads that source's attributes, a blank
// or a variable nothing binds to a keyword in the attribute position names
// none, and a ground whose variable stands in no attribute position names
// none.
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
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/ground")}, Binding: datalog.NewSymbol("?ga")},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ga")}, Blank{}}},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ia")}, Blank{}}},
			&Expression{Function: IdentityFunction{Arg: ConstantTerm{Value: datalog.NewKeyword(":t/identity")}}, Binding: datalog.NewSymbol("?ia")},
			&Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: datalog.NewSymbol("?ga")}}, Binding: datalog.NewSymbol("?aa")},
			&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?aa")}, Blank{}}},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/unused")}, Binding: datalog.NewSymbol("?gu")},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/local")}, Binding: datalog.NewSymbol("?la")},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/shared")}, Binding: datalog.NewSymbol("?sh")},
			&OrJoinClause{JoinVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?sh")}, Branches: [][]Clause{
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
		datalog.NewKeyword(":t/ground"),
		datalog.NewKeyword(":t/identity"),
		datalog.NewKeyword(":t/tuple"),
		datalog.NewKeyword(":t/shared"),
		datalog.NewKeyword(":t/negated"),
	}
	assertKeywordsInOrder(t, q.DefaultSourceAttributes(), want)
}

// TestInvocationConstants pins the roster InvocationConstants collects: for
// every rule invocation at any depth, subquery bodies included, each keyword
// constant argument with its predicate and position, in order of first
// appearance and each once, and, at the end of each scope, each variable
// argument the scope binds to a keyword through ground or identity, or that
// the form's interface shares with an enclosing scope binding it. A constant
// of another type, a variable nothing binds, and a variable a branch binds
// outside its form's interface name nothing.
func TestInvocationConstants(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	has := datalog.NewSymbol("has")
	q := &Query{Where: []Clause{
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/direct")}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: "text"}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ga")}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?free")}}},
		&OrClause{Branches: [][]Clause{{
			&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/branch")}}},
		}}},
		&SubqueryPattern{
			Query: &Query{Where: []Clause{
				&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/sub")}}},
			}},
			Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
		},
		&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/ground")}, Binding: datalog.NewSymbol("?ga")},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/direct")}}},
		&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/local")}, Binding: datalog.NewSymbol("?la")},
		&OrJoinClause{JoinVars: []Symbol{datalog.NewSymbol("?e")}, Branches: [][]Clause{{
			&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?la")}}},
		}}},
		&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/shared")}, Binding: datalog.NewSymbol("?sh")},
		&OrJoinClause{JoinVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?sh")}, Branches: [][]Clause{{
			&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?sh")}}},
		}}},
	}}
	want := []InvocationConstant{
		{Name: has, Position: 1, Attr: datalog.NewKeyword(":t/direct")},
		{Name: has, Position: 1, Attr: datalog.NewKeyword(":t/branch")},
		{Name: has, Position: 1, Attr: datalog.NewKeyword(":t/sub")},
		{Name: has, Position: 1, Attr: datalog.NewKeyword(":t/ground")},
		{Name: has, Position: 1, Attr: datalog.NewKeyword(":t/shared")},
	}
	got := q.InvocationConstants()
	if len(got) != len(want) {
		t.Fatalf("collected %d invocation constants, want %d:\n got  %v\n want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("invocation constant %d is %v, want %v", i, got[i], want[i])
		}
	}
	if &q.InvocationConstants()[0] != &got[0] {
		t.Errorf("every call returns the roster the first call built; got two rosters")
	}
}

// A query's rosters are collected on the first call of any accessor, and
// every later call of each accessor returns the roster that call built: the
// default source's attributes, its attribute variables, and each named
// source's attributes. A source no pattern names has no roster.
func TestDefaultSourceAttributesComputedOnce(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	cache := datalog.NewSymbol("$cache")
	q := &Query{Where: []Clause{
		walkPattern("top"),
		&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?a")}, Blank{}}},
		&DataPattern{Source: cache, Elements: []PatternElement{
			entity, Constant{Value: datalog.NewKeyword(":t/named")}, Blank{},
		}},
		walkPattern("other"),
	}}
	attributes := q.DefaultSourceAttributes()
	variables := q.DefaultSourceAttributeVariables()
	named := q.SourceAttributes(cache)
	assertKeywordsInOrder(t, attributes, []datalog.Keyword{datalog.NewKeyword(":t/top"), datalog.NewKeyword(":t/other")})
	if len(variables) != 1 || variables[0] != datalog.NewSymbol("?a") {
		t.Fatalf("collected variables %v, want [?a]", variables)
	}
	assertKeywordsInOrder(t, named, []datalog.Keyword{datalog.NewKeyword(":t/named")})
	if &q.DefaultSourceAttributes()[0] != &attributes[0] {
		t.Errorf("every call returns the attribute roster the first call built; got two rosters")
	}
	if &q.DefaultSourceAttributeVariables()[0] != &variables[0] {
		t.Errorf("every call returns the variable roster the first call built; got two rosters")
	}
	if &q.SourceAttributes(cache)[0] != &named[0] {
		t.Errorf("every call returns the named-source roster the first call built; got two rosters")
	}
	if got := q.SourceAttributes(datalog.NewSymbol("$absent")); got != nil {
		t.Errorf("a source no pattern names has no roster, got %v", got)
	}
}

// TestDefaultSourceAttributeVariables pins the variables
// DefaultSourceAttributeVariables collects: every variable in the attribute
// position of a data pattern on the default source, in order of first
// appearance and each once, through NOT/NOT-JOIN bodies and
// OR/OR-JOIN/OR-DEFAULT branches. A pattern on a named source reads that
// source's attributes, a constant or blank in the attribute position is no
// variable, and a subquery body's inputs are the subquery's own to check.
func TestDefaultSourceAttributeVariables(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	attrVar := func(name string) *DataPattern {
		return &DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol(name)}, Blank{}}}
	}
	clauses := []Clause{
		attrVar("?top"),
		&DataPattern{Source: datalog.SymDollar, Elements: []PatternElement{
			entity, Variable{Name: datalog.NewSymbol("?dollar")}, Blank{},
		}},
		&DataPattern{Source: datalog.NewSymbol("$cache"), Elements: []PatternElement{
			entity, Variable{Name: datalog.NewSymbol("?named")}, Blank{},
		}},
		walkPattern("constant"),
		&DataPattern{Elements: []PatternElement{entity, Blank{}, Blank{}}},
		attrVar("?top"),
		&NotClause{Clauses: []Clause{attrVar("?not")}},
		&NotJoinClause{Clauses: []Clause{attrVar("?notjoin")}},
		&OrClause{Branches: [][]Clause{{attrVar("?or1")}, {attrVar("?or2")}}},
		&OrJoinClause{Branches: [][]Clause{{attrVar("?orjoin")}}},
		&OrDefaultClause{Branches: [][]Clause{{attrVar("?ordefault")}}},
		&OrDefaultJoinClause{Branches: [][]Clause{{attrVar("?ordefaultjoin")}}},
		&SubqueryPattern{
			Query:   &Query{Where: []Clause{attrVar("?sub")}},
			Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
		},
	}

	want := []Symbol{
		datalog.NewSymbol("?top"),
		datalog.NewSymbol("?dollar"),
		datalog.NewSymbol("?not"),
		datalog.NewSymbol("?notjoin"),
		datalog.NewSymbol("?or1"),
		datalog.NewSymbol("?or2"),
		datalog.NewSymbol("?orjoin"),
		datalog.NewSymbol("?ordefault"),
		datalog.NewSymbol("?ordefaultjoin"),
	}
	got := (&Query{Where: clauses}).DefaultSourceAttributeVariables()
	if len(got) != len(want) {
		t.Fatalf("collected %d variables, want %d:\n got  %v\n want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("variable %d is %s, want %s", i, got[i], want[i])
		}
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
	assertKeywordsInOrder(t, q.SourceAttributes(cache), want)
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
