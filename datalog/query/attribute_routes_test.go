package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// keywordsNamed builds the keywords of the names given, nil for none.
func keywordsNamed(names ...string) []datalog.Keyword {
	var attrs []datalog.Keyword
	for _, name := range names {
		attrs = append(attrs, datalog.NewKeyword(name))
	}
	return attrs
}

// A roster names, per source, the keyword constants written in the attribute
// position of the data patterns on that source, at any depth of header forms
// and subquery bodies, each once in order of first appearance, and on the
// default source the pull patterns' and the database functions' attributes
// before them. A keyword reaching the position through a variable is a
// value, whatever binds the variable, a ground, a form's output, a subquery's
// binding, a rule's head or an input, and a keyword an invocation or a call
// passes as an argument is a value too, so none of them is named on any
// source (docs/reference/SCHEMA.md, "Unknown attributes are refused on
// request").
func TestRostersNameConstantsPerSource(t *testing.T) {
	e := Variable{Name: sym("?e")}
	cache := datalog.NewSymbol("$cache")
	constant := func(source Symbol, attr string) *DataPattern {
		return &DataPattern{Source: source, Elements: []PatternElement{e, Constant{Value: datalog.NewKeyword(attr)}, Blank{}}}
	}
	attrVar := func(source Symbol, v string) *DataPattern {
		return &DataPattern{Source: source, Elements: []PatternElement{e, Variable{Name: sym(v)}, Blank{}}}
	}
	ground := func(attr, v string) *Expression {
		return &Expression{Function: GroundFunction{Value: datalog.NewKeyword(attr)}, Binding: sym(v)}
	}
	union := func(out string, branches ...Clause) *OrJoinClause {
		form := &OrJoinClause{OutputVars: []Symbol{sym(out)}}
		for _, branch := range branches {
			form.Branches = append(form.Branches, []Clause{branch})
		}
		return form
	}
	dollar := []InputSpec{DatabaseInput{Name: datalog.SymDollar}}
	finding := func(find string, in []InputSpec, where ...Clause) *Query {
		return &Query{Find: []FindElement{FindVariable{Symbol: sym(find)}}, In: in, Where: where}
	}
	call := func(inner *Query, binding BindingForm, args ...PatternElement) *SubqueryPattern {
		return &SubqueryPattern{Query: inner, Inputs: append([]PatternElement{Constant{Value: datalog.SymDollar}}, args...), Binding: binding}
	}
	pulling := []FindElement{FindPull{Variable: sym("?e"), Pattern: &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: datalog.NewKeyword(":t/pulled")}}}}}
	cases := []struct {
		name      string
		find      []FindElement
		in        []InputSpec
		where     []Clause
		onDefault []datalog.Keyword
		onCache   []datalog.Keyword
	}{
		{"a constant on the default source",
			nil, nil, []Clause{constant(nil, ":t/x")}, keywordsNamed(":t/x"), nil},
		{"a constant on the default source written as $",
			nil, nil, []Clause{constant(datalog.SymDollar, ":t/x")}, keywordsNamed(":t/x"), nil},
		{"a constant on a named source",
			nil, nil, []Clause{constant(cache, ":t/x")}, nil, keywordsNamed(":t/x")},
		{"a constant named twice on one source",
			nil, nil, []Clause{constant(nil, ":t/x"), constant(cache, ":t/x"), constant(nil, ":t/x")}, keywordsNamed(":t/x"), keywordsNamed(":t/x")},
		{"a constant in each branch of an or-join",
			nil, nil, []Clause{union("?e", constant(nil, ":t/x"), constant(cache, ":t/y"))}, keywordsNamed(":t/x"), keywordsNamed(":t/y")},
		{"a constant in each branch of an or-default-join",
			nil, nil, []Clause{&OrDefaultJoinClause{OutputVars: []Symbol{sym("?e")}, Branches: [][]Clause{{constant(nil, ":t/x")}, {constant(cache, ":t/y")}}}}, keywordsNamed(":t/x"), keywordsNamed(":t/y")},
		{"a constant in a not-join body",
			nil, nil, []Clause{constant(nil, ":t/x"), &NotJoinClause{RequiredVars: []Symbol{sym("?e")}, Clauses: []Clause{constant(nil, ":t/y"), constant(cache, ":t/z")}}}, keywordsNamed(":t/x", ":t/y"), keywordsNamed(":t/z")},
		{"a constant in a form nested in a form",
			nil, nil, []Clause{union("?e", union("?e", constant(nil, ":t/x"), constant(cache, ":t/y")), constant(nil, ":t/z"))}, keywordsNamed(":t/x", ":t/z"), keywordsNamed(":t/y")},
		{"a constant inside a subquery body",
			nil, nil, []Clause{call(finding("?v", dollar, constant(nil, ":t/x"), constant(cache, ":t/y")), RelationBinding{Variables: []Symbol{sym("?v")}})}, keywordsNamed(":t/x"), keywordsNamed(":t/y")},
		{"a pull pattern in :find, before the patterns' constants",
			pulling, nil, []Clause{constant(nil, ":t/x")}, keywordsNamed(":t/pulled", ":t/x"), nil},
		{"a database function as an expression and as a predicate",
			nil, nil, []Clause{
				constant(nil, ":t/x"),
				&Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: sym("?e")}, Attr: datalog.NewKeyword(":t/getelse"), Default: "none"}, Binding: sym("?g")},
				&DatabaseFunctionPredicate{Function: &MissingFunction{Entity: VariableTerm{Symbol: sym("?e")}, Attr: datalog.NewKeyword(":t/missing")}},
			}, keywordsNamed(":t/x", ":t/getelse", ":t/missing"), nil},
		{"a ground into the attribute position",
			nil, nil, []Clause{ground(":t/x", "?a"), attrVar(nil, "?a")}, nil, nil},
		{"a ground into a named source's attribute position",
			nil, nil, []Clause{ground(":t/x", "?a"), attrVar(cache, "?a")}, nil, nil},
		{"an identity alias of a ground into the attribute position",
			nil, nil, []Clause{ground(":t/x", "?a"), &Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: sym("?a")}}, Binding: sym("?b")}, attrVar(nil, "?b")}, nil, nil},
		{"a form's output into the attribute position",
			nil, nil, []Clause{union("?a", ground(":t/x", "?a"), ground(":t/y", "?a")), attrVar(nil, "?a")}, nil, nil},
		{"a subquery's binding into the attribute position",
			nil, nil, []Clause{call(finding("?a", dollar, ground(":t/x", "?a")), RelationBinding{Variables: []Symbol{sym("?b")}}), attrVar(nil, "?b")}, nil, nil},
		{"a rule's head into the attribute position",
			nil, nil, []Clause{&RuleInvocation{Name: sym("attr"), Args: []PatternElement{Variable{Name: sym("?a")}}}, attrVar(nil, "?a")}, nil, nil},
		{"an input into the attribute position",
			nil, append(dollar, ScalarInput{Symbol: sym("?a")}), []Clause{attrVar(nil, "?a")}, nil, nil},
		{"a keyword constant passed to an invocation",
			nil, nil, []Clause{&RuleInvocation{Name: sym("has"), Args: []PatternElement{e, Constant{Value: datalog.NewKeyword(":t/x")}}}}, nil, nil},
		{"a keyword constant passed to a subquery call",
			nil, nil, []Clause{call(finding("?v", append(dollar, ScalarInput{Symbol: sym("?k")}), attrVar(nil, "?k")), RelationBinding{Variables: []Symbol{sym("?v")}}, Constant{Value: datalog.NewKeyword(":t/x")})}, nil, nil},
		{"a blank in the attribute position",
			nil, nil, []Clause{&DataPattern{Elements: []PatternElement{e, Blank{}, Blank{}}}}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rosters := CollectRosters(&Query{Find: c.find, In: c.in, Where: c.where})
			assertKeywordsInOrder(t, rosters.SourceAttributes(datalog.SymDollar), c.onDefault)
			assertKeywordsInOrder(t, rosters.SourceAttributes(cache), c.onCache)
		})
	}
}

// An inner query's patterns are named under the sources its call passes:
// a pattern on an inner :in source, the $ of an omitted or declared :in
// included, names its attribute on the source the call passes at that
// place, and a nested call passing an inner source passes the call's, at any
// depth. A source passed under its own name stays, a pattern on the inner $
// where the inner :in declares a named source alone reads the enclosing
// default source, and a constant passed at a scalar position is a value
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.4).
func TestRostersReadAnInnerQueryUnderTheSourcesItsCallPasses(t *testing.T) {
	e := Variable{Name: sym("?e")}
	other, inT, inU := datalog.NewSymbol("$other"), datalog.NewSymbol("$t"), datalog.NewSymbol("$u")
	on := func(source Symbol, attr string) *DataPattern {
		return &DataPattern{Source: source, Elements: []PatternElement{e, Constant{Value: datalog.NewKeyword(attr)}, Variable{Name: sym("?v")}}}
	}
	inner := func(in []InputSpec, where ...Clause) *Query {
		return &Query{Find: []FindElement{FindVariable{Symbol: sym("?v")}}, In: in, Where: where}
	}
	call := func(q *Query, args ...PatternElement) *SubqueryPattern {
		return &SubqueryPattern{Query: q, Inputs: args, Binding: RelationBinding{Variables: []Symbol{sym("?v")}}}
	}
	source := func(name Symbol) Constant { return Constant{Value: name} }
	declaring := func(names ...Symbol) []InputSpec {
		var in []InputSpec
		for _, name := range names {
			in = append(in, DatabaseInput{Name: name})
		}
		return in
	}
	cases := []struct {
		name   string
		clause Clause
		want   map[Symbol][]datalog.Keyword
	}{
		{"a pattern on a named inner source given a named source",
			call(inner(declaring(inT), on(inT, ":t/x")), source(other)),
			map[Symbol][]datalog.Keyword{other: keywordsNamed(":t/x")}},
		{"a pattern on the inner $ given a named source",
			call(inner(declaring(datalog.SymDollar), on(nil, ":t/x")), source(other)),
			map[Symbol][]datalog.Keyword{other: keywordsNamed(":t/x")}},
		{"a pattern on the inner $ under an omitted :in given a named source",
			call(inner(nil, on(nil, ":t/x")), source(other)),
			map[Symbol][]datalog.Keyword{other: keywordsNamed(":t/x")}},
		{"a pattern on the inner $ given $",
			call(inner(declaring(datalog.SymDollar), on(nil, ":t/x")), source(datalog.SymDollar)),
			map[Symbol][]datalog.Keyword{datalog.SymDollar: keywordsNamed(":t/x")}},
		{"a pattern on a named inner source given the same name",
			call(inner(declaring(inT), on(inT, ":t/x")), source(inT)),
			map[Symbol][]datalog.Keyword{inT: keywordsNamed(":t/x")}},
		{"a nested call passing the inner source on",
			call(inner(declaring(inT), call(inner(declaring(inU), on(inU, ":t/x")), source(inT))), source(other)),
			map[Symbol][]datalog.Keyword{other: keywordsNamed(":t/x")}},
		{"a pattern on $ inside an inner query declaring a named source alone",
			call(inner(declaring(inT), on(nil, ":t/x")), source(other)),
			map[Symbol][]datalog.Keyword{datalog.SymDollar: keywordsNamed(":t/x")}},
		{"two inner sources given two names",
			call(inner(declaring(inT, inU), on(inT, ":t/x"), on(inU, ":t/y")), source(other), source(datalog.SymDollar)),
			map[Symbol][]datalog.Keyword{other: keywordsNamed(":t/x"), datalog.SymDollar: keywordsNamed(":t/y")}},
		{"a constant passed at a scalar position",
			call(inner(append(declaring(datalog.SymDollar), ScalarInput{Symbol: sym("?k")}), &DataPattern{Elements: []PatternElement{e, Variable{Name: sym("?k")}, Variable{Name: sym("?v")}}}), source(datalog.SymDollar), Constant{Value: datalog.NewKeyword(":t/x")}),
			map[Symbol][]datalog.Keyword{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rosters := CollectRosters(&Query{Where: []Clause{c.clause}})
			for _, src := range []Symbol{datalog.SymDollar, other, inT, inU} {
				assertKeywordsInOrder(t, rosters.SourceAttributes(src), c.want[src])
			}
		})
	}
}

// A rule set names, per source, the keyword constants its bodies write in the
// attribute position of a pattern on that source, an inner query's under the
// sources its call passes, collected once over every body. A variable in the
// attribute position and a keyword passed to an invocation are values and
// name nothing.
func TestRuleSetNamesItsBodiesConstantsPerSource(t *testing.T) {
	e := Variable{Name: sym("?e")}
	cache, inT := datalog.NewSymbol("$cache"), datalog.NewSymbol("$t")
	rs := mustRuleSet(t, []*Rule{
		defineRule("rn", []string{"?v"},
			&DataPattern{Elements: []PatternElement{e, Constant{Value: datalog.NewKeyword(":t/x")}, Variable{Name: sym("?v")}}}),
		defineRule("named", []string{"?v"},
			&DataPattern{Source: cache, Elements: []PatternElement{e, Constant{Value: datalog.NewKeyword(":t/named")}, Variable{Name: sym("?v")}}}),
		defineRule("sub", []string{"?v"},
			&SubqueryPattern{
				Query: &Query{
					Find:  []FindElement{FindVariable{Symbol: sym("?v")}},
					In:    []InputSpec{DatabaseInput{Name: inT}},
					Where: []Clause{&DataPattern{Source: inT, Elements: []PatternElement{e, Constant{Value: datalog.NewKeyword(":t/inner")}, Variable{Name: sym("?v")}}}},
				},
				Inputs:  []PatternElement{Constant{Value: cache}},
				Binding: RelationBinding{Variables: []Symbol{sym("?v")}},
			}),
		defineRule("has", []string{"?e", "?a"}, attrVarPattern("?e", "?a")),
		defineRule("marked", []string{"?e"},
			&RuleInvocation{Name: sym("has"), Args: []PatternElement{e, Constant{Value: datalog.NewKeyword(":t/marked")}}}),
		defineRule("grounded", []string{"?e"},
			&Expression{Function: GroundFunction{Value: datalog.NewKeyword(":t/grounded")}, Binding: sym("?a")},
			attrVarPattern("?e", "?a")),
	})
	first := rs.SourceAttributes(datalog.SymDollar)
	assertKeywordsInOrder(t, first, keywordsNamed(":t/x"))
	assertKeywordsInOrder(t, rs.SourceAttributes(cache), keywordsNamed(":t/named", ":t/inner"))
	assertKeywordsInOrder(t, rs.SourceAttributes(inT), nil)
	if second := rs.SourceAttributes(datalog.SymDollar); &first[0] != &second[0] {
		t.Error("the bodies' roster is collected once, and a second call returns the same slice")
	}
}
