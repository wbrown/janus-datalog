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
// variable binds to a keyword. A variable an or-join branch binds outside
// the header is the branch's own, so a ground outside the form does not
// reach it; a variable the header shares, and every variable of an or,
// or-default or not branch, resolves against the enclosing scope, through an
// identity alias the branch records to it too. A pattern on a named source
// reads that source's attributes, a blank or a variable nothing binds to a
// keyword in the attribute position names none, and a ground whose variable
// stands in no attribute position names none.
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
		datalog.NewKeyword(":t/ground"),
		datalog.NewKeyword(":t/identity"),
		datalog.NewKeyword(":t/tuple"),
		datalog.NewKeyword(":t/shared"),
		datalog.NewKeyword(":t/negated"),
		datalog.NewKeyword(":t/inferred"),
		datalog.NewKeyword(":t/fallen"),
		datalog.NewKeyword(":t/aliased"),
	}
	inferred, err := InferHeaders(q, nil)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	assertKeywordsInOrder(t, CollectRosters(inferred).DefaultSourceAttributes(nil), want)
}

// TestDefaultSourceAttributesThroughInvocations pins what a query names on
// the default source through its rule invocations under a rule set reading a
// position as an attribute: for every invocation at any depth, subquery
// bodies included, each keyword constant argument at such a position, after
// the patterns' attributes, in order of first appearance and each once, and,
// at the end of each scope, each variable argument the scope binds to a
// keyword through ground or identity, or that the form's header shares with
// an enclosing scope binding it. A variable passed inside an explicit-header
// branch outside the header is the branch's own and names nothing, whatever
// the rule set requires, as does a variable a pattern of the branch provides;
// TestDefaultSourceAttributesThroughAHeader carries the header forms. A
// constant of another type, a variable nothing binds, and a
// keyword at a position the rule set reads as a value name nothing, and a
// keyword a pattern also names is named once, at the pattern's position.
// Under no rule set the query names the patterns' attributes alone, and a
// resolution under a rule set leaves that roster as it is.
func TestDefaultSourceAttributesThroughInvocations(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	has, also := datalog.NewSymbol("has"), datalog.NewSymbol("also")
	q := &Query{Where: []Clause{
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/direct")}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/x")}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Constant{Value: "text"}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ga")}}},
		&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?free")}}},
		&RuleInvocation{Name: also, Args: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/value")}}},
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
		&OrJoinClause{OutputVars: []Symbol{datalog.NewSymbol("?e")}, Branches: [][]Clause{{
			&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?la")}}},
		}}},
		&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/shared")}, Binding: datalog.NewSymbol("?sh")},
		&OrJoinClause{OutputVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?sh")}, Branches: [][]Clause{{
			&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?sh")}}},
		}}},
		&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/provided")}, Binding: datalog.NewSymbol("?pa")},
		&OrJoinClause{OutputVars: []Symbol{datalog.NewSymbol("?e")}, Branches: [][]Clause{{
			&RuleInvocation{Name: has, Args: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?pa")}}},
			&DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/x")}, Variable{Name: datalog.NewSymbol("?pa")}}},
		}}},
	}}
	rules := mustRuleSet(t, []*Rule{
		defineRule("has", []string{"?e", "?a"}, attrVarPattern("?e", "?a")),
		defineRule("also", []string{"?e", "?p"}, scopePattern("?e", "t/kind", "?p")),
	})
	patterns := []datalog.Keyword{datalog.NewKeyword(":t/x")}
	inferred, err := InferHeaders(q, rules)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	rosters := CollectRosters(inferred)
	assertKeywordsInOrder(t, rosters.DefaultSourceAttributes(rules), []datalog.Keyword{
		datalog.NewKeyword(":t/x"),
		datalog.NewKeyword(":t/direct"),
		datalog.NewKeyword(":t/branch"),
		datalog.NewKeyword(":t/sub"),
		datalog.NewKeyword(":t/ground"),
		datalog.NewKeyword(":t/shared"),
	})
	assertKeywordsInOrder(t, rosters.DefaultSourceAttributes(nil), patterns)
}

// headerPassingQuery builds a query whose explicit-header forms pass, inside
// a branch, variables the enclosing scope binds to keywords: one named in the
// required run and passed to an invocation alone, one named there and passed
// to two invocations, one outside the header that a ground of the branch
// binds to a keyword of its own, one outside the header that a pattern of the
// branch provides, one named in the required run and passed inside an or
// nested in the branch, one named in a not-join's header and passed inside
// its body, and, in a subquery body, a tree of its own, one named in the
// required run of an or-join whose other branch reads it in the attribute
// position. The predicates has and also take the passed variable at position
// 0.
func headerPassingQuery() *Query {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	has, also := datalog.NewSymbol("has"), datalog.NewSymbol("also")
	header := []Symbol{datalog.NewSymbol("?e")}
	requiring := func(v string) []Symbol { return []Symbol{datalog.NewSymbol(v)} }
	ground := func(attr, v string) *Expression {
		return &Expression{Function: GroundFunction{Value: datalog.NewKeyword(attr)}, Binding: datalog.NewSymbol(v)}
	}
	passing := func(name Symbol, v string) *RuleInvocation {
		return &RuleInvocation{Name: name, Args: []PatternElement{Variable{Name: datalog.NewSymbol(v)}, entity}}
	}
	other := &DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/other")}, Blank{}}}
	return &Query{Where: []Clause{
		ground(":t/direct", "?da"),
		passing(has, "?da"),
		ground(":t/outside", "?oa"),
		&OrJoinClause{OutputVars: header, Branches: [][]Clause{{ground(":t/local", "?oa"), passing(has, "?oa")}, {other}}},
		ground(":t/required", "?ra"),
		&OrJoinClause{RequiredVars: requiring("?ra"), OutputVars: header, Branches: [][]Clause{{passing(has, "?ra")}, {other}}},
		ground(":t/paired", "?pa"),
		&OrJoinClause{RequiredVars: requiring("?pa"), OutputVars: header, Branches: [][]Clause{{passing(has, "?pa"), passing(also, "?pa")}, {other}}},
		ground(":t/provided", "?pv"),
		&OrJoinClause{OutputVars: header, Branches: [][]Clause{{
			passing(has, "?pv"),
			&DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/x")}, Variable{Name: datalog.NewSymbol("?pv")}}},
		}, {other}}},
		ground(":t/nested", "?na"),
		&OrJoinClause{RequiredVars: requiring("?na"), OutputVars: header, Branches: [][]Clause{{
			&OrClause{Branches: [][]Clause{{passing(has, "?na")}, {other}}},
		}, {other}}},
		ground(":t/negated", "?nj"),
		&NotJoinClause{RequiredVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?nj")}, Clauses: []Clause{passing(has, "?nj")}},
		&SubqueryPattern{
			Query: &Query{Where: []Clause{
				ground(":t/inner", "?ia"),
				&OrJoinClause{RequiredVars: requiring("?ia"), OutputVars: header, Branches: [][]Clause{{passing(has, "?ia")}, {
					&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ia")}, Blank{}}},
				}}},
			}},
			Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
		},
	}}
}

// requiringRuleSet builds the rule set headerPassingQuery runs under: has,
// reading its position 0 as an attribute, and also, reading its position 0
// as a value, each requiring position 0 bound when named in required.
func requiringRuleSet(t *testing.T, required ...string) *RuleSet {
	t.Helper()
	has := defineRule("has", []string{"?a", "?e"}, attrVarPattern("?e", "?a"))
	also := defineRule("also", []string{"?p", "?x"}, scopePattern("?x", "t/kind", "?p"))
	for _, name := range required {
		switch name {
		case "has":
			has.Required = []Symbol{sym("?a")}
		case "also":
			also.Required = []Symbol{sym("?p")}
		default:
			t.Fatalf("requiringRuleSet: no predicate %s", name)
		}
	}
	return mustRuleSet(t, []*Rule{has, also})
}

// An explicit-header form shares with its branches the variables its header
// names, under any rule set. A variable passed inside a branch and named in
// the required run resolves to the keyword the enclosing scope binds it to,
// which is passed at every position of the branch passing it, and a pattern
// of another branch reading the variable in the attribute position names it.
// A variable outside the header is the branch's own, whatever positions the
// rule set requires: one a ground of the branch binds resolves to the
// branch's keyword and never to the enclosing scope's, and one a pattern of
// the branch provides names nothing. The sharing reaches an invocation inside
// an or nested in the branch, which shares every variable with the branch,
// and a not-join's body through the not-join's header. The roster reads the
// prepared query, so each case infers the headers under its rule set first.
// The keywords the invocations pass at the positions the rule set reads as
// attributes follow the attributes the query names under no rule set; a
// keyword passed at a position the rule set reads as a value names nothing.
func TestDefaultSourceAttributesThroughAHeader(t *testing.T) {
	q := headerPassingQuery()
	named := []datalog.Keyword{
		datalog.NewKeyword(":t/other"),
		datalog.NewKeyword(":t/x"),
		datalog.NewKeyword(":t/inner"),
	}
	passed := append(named[:len(named):len(named)],
		datalog.NewKeyword(":t/direct"),
		datalog.NewKeyword(":t/local"),
		datalog.NewKeyword(":t/required"),
		datalog.NewKeyword(":t/paired"),
		datalog.NewKeyword(":t/nested"),
		datalog.NewKeyword(":t/negated"),
	)
	cases := []struct {
		name  string
		rules *RuleSet
		want  []datalog.Keyword
	}{
		{"no rule set", nil, named},
		{"a rule set requiring nothing", requiringRuleSet(t), passed},
		{"a rule set requiring the passed positions", requiringRuleSet(t, "has", "also"), passed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inferred, err := InferHeaders(q, c.rules)
			if err != nil {
				t.Fatalf("InferHeaders: %v", err)
			}
			assertKeywordsInOrder(t, CollectRosters(inferred).DefaultSourceAttributes(c.rules), c.want)
		})
	}
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

// TestDefaultSourceAttributeVariables pins the variables the roster
// collects as the query's attribute variables: every variable of the query's
// own scope in the attribute position of a data pattern on the default
// source, in order of first appearance and each once, a variable an identity
// expression aliases to a root variable counting as that variable, and, inside
// a form, a variable the form's header names and no other. A variable a
// branch reads in the attribute position outside the header is the branch's
// own, whether the header is written or inferred, so no input reaches it. A
// pattern on a named source reads that source's attributes, a constant or
// blank in the attribute position is no variable, and a subquery body's
// variables are the inner query's own.
func TestDefaultSourceAttributeVariables(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	header := []Symbol{datalog.NewSymbol("?e")}
	requiring := func(v string) []Symbol { return []Symbol{datalog.NewSymbol(v)} }
	attrVar := func(name string) *DataPattern {
		return &DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol(name)}, Blank{}}}
	}
	other := &DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/other")}, Blank{}}}
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
		&Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: datalog.NewSymbol("?src")}}, Binding: datalog.NewSymbol("?via")},
		attrVar("?via"),
		&NotClause{Clauses: []Clause{attrVar("?top"), attrVar("?not")}},
		&NotJoinClause{RequiredVars: []Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?notjoin")}, Clauses: []Clause{attrVar("?notjoin")}},
		&NotJoinClause{RequiredVars: header, Clauses: []Clause{attrVar("?notlocal")}},
		&OrClause{Branches: [][]Clause{{attrVar("?or1")}, {attrVar("?or2")}}},
		&OrJoinClause{RequiredVars: requiring("?orjoin"), OutputVars: header, Branches: [][]Clause{{attrVar("?orjoin")}, {other}}},
		&OrJoinClause{OutputVars: header, Branches: [][]Clause{{attrVar("?orlocal")}, {other}}},
		&OrDefaultClause{Branches: [][]Clause{{attrVar("?ordefault")}, {other}}},
		&OrDefaultJoinClause{RequiredVars: requiring("?ordefaultjoin"), OutputVars: header, Branches: [][]Clause{{attrVar("?ordefaultjoin")}, {other}}},
		&OrDefaultJoinClause{OutputVars: header, Branches: [][]Clause{{attrVar("?fallocal")}, {other}}},
		&SubqueryPattern{
			Query:   &Query{Where: []Clause{attrVar("?sub")}},
			Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
		},
	}

	want := []Symbol{
		datalog.NewSymbol("?top"),
		datalog.NewSymbol("?dollar"),
		datalog.NewSymbol("?src"),
		datalog.NewSymbol("?notjoin"),
		datalog.NewSymbol("?orjoin"),
		datalog.NewSymbol("?ordefaultjoin"),
	}
	inferred, err := InferHeaders(&Query{Where: clauses}, nil)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	got := CollectRosters(inferred).AttributeVariables(nil)
	if len(got) != len(want) {
		t.Fatalf("collected %d variables, want %d:\n got  %v\n want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("variable %d is %s, want %s", i, got[i], want[i])
		}
	}
}

// A subquery's call names what its inner query reads at an attribute
// position through its :in. An inner :in symbol standing in the attribute
// position of a pattern of the inner query, at any depth of calls, makes the
// call's variable argument at its place an attribute variable of the
// enclosing scope, and a keyword constant argument at that place an attribute
// the query names; a keyword the enclosing scope binds to such an argument
// through ground is named at the scope's end. An argument whose inner symbol
// stands in a value position names nothing, and the inner query's own
// attribute variables are its own.
func TestDefaultSourceAttributesThroughASubqueryCall(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	symbol := datalog.NewSymbol
	call := func(inner *Query, argument PatternElement, bound string) *SubqueryPattern {
		return &SubqueryPattern{
			Query:   inner,
			Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, argument},
			Binding: RelationBinding{Variables: []Symbol{symbol(bound)}},
		}
	}
	// reading is an inner query whose :in symbol stands in the attribute
	// position of its pattern.
	reading := func(in string) *Query {
		return &Query{
			Find:  []FindElement{FindVariable{Symbol: symbol("?v")}},
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: symbol(in)}},
			Where: []Clause{&DataPattern{Elements: []PatternElement{entity, Variable{Name: symbol(in)}, Variable{Name: symbol("?v")}}}},
		}
	}
	// nested is an inner query passing its :in symbol to a call of reading.
	nested := &Query{
		Find:  []FindElement{FindVariable{Symbol: symbol("?w")}},
		In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: symbol("?middle")}},
		Where: []Clause{call(reading("?deepest"), Variable{Name: symbol("?middle")}, "?w")},
	}
	// valued is an inner query whose :in symbol stands in the value position
	// of its pattern.
	valued := &Query{
		Find:  []FindElement{FindVariable{Symbol: symbol("?e")}},
		In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: symbol("?k")}},
		Where: []Clause{&DataPattern{Elements: []PatternElement{entity, Constant{Value: datalog.NewKeyword(":t/kind")}, Variable{Name: symbol("?k")}}}},
	}
	q := &Query{Where: []Clause{
		call(reading("?attr"), Variable{Name: symbol("?passed")}, "?pv"),
		call(reading("?attr"), Constant{Value: datalog.NewKeyword(":t/constant")}, "?cv"),
		&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/grounded")}, Binding: symbol("?ga")},
		call(reading("?attr"), Variable{Name: symbol("?ga")}, "?gv"),
		call(nested, Variable{Name: symbol("?deep")}, "?nv"),
		call(valued, Variable{Name: symbol("?value")}, "?vv"),
	}}
	inferred, err := InferHeaders(q, nil)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	rosters := CollectRosters(inferred)
	variables := rosters.AttributeVariables(nil)
	wantVariables := []Symbol{symbol("?passed"), symbol("?ga"), symbol("?deep")}
	if len(variables) != len(wantVariables) {
		t.Fatalf("collected %d attribute variables, want %d:\n got  %v\n want %v", len(variables), len(wantVariables), variables, wantVariables)
	}
	for i := range wantVariables {
		if variables[i] != wantVariables[i] {
			t.Errorf("attribute variable %d is %s, want %s", i, variables[i], wantVariables[i])
		}
	}
	assertKeywordsInOrder(t, rosters.DefaultSourceAttributes(nil), []datalog.Keyword{
		datalog.NewKeyword(":t/constant"),
		datalog.NewKeyword(":t/kind"),
		datalog.NewKeyword(":t/grounded"),
	})
}

// A relation ground binds its variable to the value of every row, so a
// variable in an attribute position bound by a relation ground of keywords
// names each row's keyword, in the rows' order; a tuple ground of keywords
// names its one keyword per variable as before.
func TestDefaultSourceAttributesThroughARelationGround(t *testing.T) {
	entity := Variable{Name: datalog.NewSymbol("?e")}
	q := &Query{Where: []Clause{
		&Expression{
			Function: GroundFunction{Value: []interface{}{
				[]interface{}{datalog.NewKeyword(":t/first"), datalog.NewKeyword(":t/other")},
				[]interface{}{datalog.NewKeyword(":t/second"), datalog.NewKeyword(":t/other")},
			}},
			Binding: RelationBinding{Variables: []Symbol{datalog.NewSymbol("?attr"), datalog.NewSymbol("?unread")}},
		},
		&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?attr")}, Blank{}}},
		&Expression{
			Function: &GroundFunction{Value: []interface{}{datalog.NewKeyword(":t/third"), int64(1)}},
			Binding:  TupleBinding{Variables: []Symbol{datalog.NewSymbol("?ta"), datalog.NewSymbol("?tn")}},
		},
		&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ta")}, Blank{}}},
		&Expression{
			Function: GroundFunction{Value: []interface{}{datalog.NewKeyword(":t/fourth"), datalog.NewKeyword(":t/fifth")}},
			Binding:  CollectionBinding{Variable: datalog.NewSymbol("?ca")},
		},
		&DataPattern{Elements: []PatternElement{entity, Variable{Name: datalog.NewSymbol("?ca")}, Blank{}}},
	}}
	inferred, err := InferHeaders(q, nil)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	assertKeywordsInOrder(t, CollectRosters(inferred).DefaultSourceAttributes(nil), []datalog.Keyword{
		datalog.NewKeyword(":t/first"),
		datalog.NewKeyword(":t/second"),
		datalog.NewKeyword(":t/third"),
		datalog.NewKeyword(":t/fourth"),
		datalog.NewKeyword(":t/fifth"),
	})
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
