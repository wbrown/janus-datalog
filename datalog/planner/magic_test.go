package planner

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A rule set rewritten for an invocation with bound arguments derives only
// the tuples those arguments reach: each predicate the bindings reach gets a
// copy adorned with its binding pattern and guarded by a magic predicate
// holding the bindings it is asked with, and the bindings pass through each
// scope in the planner's order into the invocations after them, a subquery's
// inner query among them as a scope of its own. An invocation nothing binds
// keeps the original rules, and a rewrite that does not stratify is
// declined.

func sym(name string) query.Symbol { return datalog.NewSymbol(name) }

// invoke builds a rule invocation. A "?x" argument is a variable, "_" a
// blank, and anything else a string constant.
func invoke(name string, args ...string) *query.RuleInvocation {
	inv := &query.RuleInvocation{Name: datalog.NewSymbol(name)}
	for _, arg := range args {
		switch {
		case arg == "_":
			inv.Args = append(inv.Args, query.Blank{})
		case arg[0] == '?':
			inv.Args = append(inv.Args, query.Variable{Name: datalog.NewSymbol(arg)})
		default:
			inv.Args = append(inv.Args, query.Constant{Value: arg})
		}
	}
	return inv
}

// scopePattern builds a data pattern: a "?x" element is a variable, "_" a
// blank, and anything else a keyword constant.
func scopePattern(names ...string) *query.DataPattern {
	elements := make([]query.PatternElement, len(names))
	for i, name := range names {
		if name == "_" {
			elements[i] = query.Blank{}
		} else if name[0] == '?' {
			elements[i] = query.Variable{Name: datalog.NewSymbol(name)}
		} else {
			elements[i] = query.Constant{Value: datalog.NewKeyword(":" + name)}
		}
	}
	return &query.DataPattern{Elements: elements}
}

// defineRule builds a rule from its name, its head variables and its body.
func defineRule(name string, head []string, body ...query.Clause) *query.Rule {
	r := &query.Rule{Name: datalog.NewSymbol(name), Body: body}
	for _, h := range head {
		r.Head = append(r.Head, datalog.NewSymbol(h))
	}
	return r
}

// followsRules is the closure over :page/next: a page follows the page its
// next names, and follows whatever that page follows.
func followsRules() []*query.Rule {
	return []*query.Rule{
		defineRule("follows", []string{"?a", "?b"},
			scopePattern("?a", "page/next", "?b")),
		defineRule("follows", []string{"?a", "?c"},
			scopePattern("?a", "page/next", "?b"),
			invoke("follows", "?b", "?c")),
	}
}

// small holds of a number below ten and requires the caller to bind it: its
// body consumes ?n and nothing in it provides ?n.
func smallRule() *query.Rule {
	r := defineRule("small", []string{"?n"}, &query.Comparison{
		Op:    datalog.SymLT,
		Left:  query.VariableTerm{Symbol: sym("?n")},
		Right: query.ConstantTerm{Value: int64(10)},
	})
	r.Required = []query.Symbol{sym("?n")}
	return r
}

func assertSymbols(t *testing.T, label string, got []query.Symbol, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for _, name := range want {
		if !query.ContainsSymbol(got, datalog.NewSymbol(name)) {
			t.Errorf("%s = %v, missing %s", label, got, name)
		}
	}
}

// assertRules compares two rule lists rendered rule by rule.
func assertRules(t *testing.T, label string, got, want []*query.Rule) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rules, want %d:\n%s\n--\n%s", label, len(got), len(want), renderRules(got), renderRules(want))
	}
	for i := range want {
		if got[i].String() != want[i].String() {
			t.Errorf("%s rule %d:\n got %s\nwant %s", label, i, got[i], want[i])
		}
	}
}

func renderRules(rules []*query.Rule) string {
	lines := make([]string, len(rules))
	for i, r := range rules {
		lines[i] = r.String()
	}
	return strings.Join(lines, "\n")
}

func mustRuleSet(t *testing.T, rules []*query.Rule) *query.RuleSet {
	t.Helper()
	rs, err := query.NewRuleSet(rules)
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	return rs
}

// mustMagic rewrites the set for an invocation of name under bound, through
// a planner on the clause-based path.
func mustMagic(t *testing.T, rs *query.RuleSet, name string, bound ...bool) *Magic {
	t.Helper()
	magic, err := Rewrite(NewClauseBasedPlanner(nil, PlannerOptions{}), rs, sym(name), bound, nil)
	if err != nil {
		t.Fatalf("Rewrite(%s): %v", name, err)
	}
	return magic
}

func TestMagic_ClosureBoundFirst(t *testing.T) {
	rs := mustRuleSet(t, followsRules())
	magic := mustMagic(t, rs, "follows", true, false)
	if magic.Predicate != sym("adorned.bf/follows") || magic.Seed != sym("magic.bf/follows") {
		t.Errorf("Predicate %s, Seed %s; want adorned.bf/follows and magic.bf/follows", magic.Predicate, magic.Seed)
	}
	assertRules(t, "adorned.bf/follows", magic.Rules.Rules(sym("adorned.bf/follows")), []*query.Rule{
		defineRule("adorned.bf/follows", []string{"?a", "?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
		defineRule("adorned.bf/follows", []string{"?a", "?c"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b"),
			invoke("adorned.bf/follows", "?b", "?c")),
	})
	assertRules(t, "magic.bf/follows", magic.Rules.Rules(sym("magic.bf/follows")), []*query.Rule{
		defineRule("magic.bf/follows", []string{"?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
	})
	if arity, ok := magic.Rules.Arity(sym("magic.bf/follows")); !ok || arity != 1 {
		t.Errorf("Arity(magic.bf/follows) = %d, %v; want 1, true: the seed predicate is declared", arity, ok)
	}
	// The magic rule reads only its own predicate and the edge, so the magic
	// set is a recursive stratum of its own, complete before the closure
	// restricted to it runs.
	strata := magic.Rules.Strata()
	if len(strata) != 2 || !strata[0].Recursive || !strata[1].Recursive {
		t.Fatalf("strata = %+v, want two recursive strata, the magic set then the closure", strata)
	}
	assertSymbols(t, "first stratum", strata[0].Predicates, "magic.bf/follows")
	assertSymbols(t, "second stratum", strata[1].Predicates, "adorned.bf/follows")
	assertRules(t, "the original set", rs.Rules(sym("follows")), followsRules())
}

func TestMagic_ConstantAndRepeatedArguments(t *testing.T) {
	rs := mustRuleSet(t, []*query.Rule{
		defineRule("pair", []string{"?x", "?y"}, scopePattern("?x", "page/next", "?y")),
		defineRule("twin", []string{"?x"}, invoke("pair", "?x", "k"), invoke("pair", "?x", "?x")),
	})
	magic := mustMagic(t, rs, "twin", true)
	assertRules(t, "adorned.b/twin", magic.Rules.Rules(sym("adorned.b/twin")), []*query.Rule{
		defineRule("adorned.b/twin", []string{"?x"},
			invoke("magic.b/twin", "?x"),
			invoke("adorned.bb/pair", "?x", "k"),
			invoke("adorned.bf/pair", "?x", "?x")),
	})
	assertRules(t, "adorned.bb/pair", magic.Rules.Rules(sym("adorned.bb/pair")), []*query.Rule{
		defineRule("adorned.bb/pair", []string{"?x", "?y"},
			invoke("magic.bb/pair", "?x", "?y"),
			scopePattern("?x", "page/next", "?y")),
	})
	assertRules(t, "adorned.bf/pair", magic.Rules.Rules(sym("adorned.bf/pair")), []*query.Rule{
		defineRule("adorned.bf/pair", []string{"?x", "?y"},
			invoke("magic.bf/pair", "?x"),
			scopePattern("?x", "page/next", "?y")),
	})
	// A constant at a bound position is a fresh head variable a ground
	// expression binds; a variable repeated across positions is bound at its
	// first position only, so the second stays free.
	assertRules(t, "magic.bb/pair", magic.Rules.Rules(sym("magic.bb/pair")), []*query.Rule{
		defineRule("magic.bb/pair", []string{"?x", "?magic.bb/pair.1"},
			invoke("magic.b/twin", "?x"),
			&query.Expression{Function: &query.GroundFunction{Value: "k"}, Binding: sym("?magic.bb/pair.1")}),
	})
	assertRules(t, "magic.bf/pair", magic.Rules.Rules(sym("magic.bf/pair")), []*query.Rule{
		defineRule("magic.bf/pair", []string{"?x"},
			invoke("magic.b/twin", "?x"),
			invoke("adorned.bb/pair", "?x", "k")),
	})
}

func TestMagic_InvocationInsideOrJoinBranch(t *testing.T) {
	orJoin := &query.OrJoinClause{OutputVars: []query.Symbol{sym("?b"), sym("?c")}, Branches: [][]query.Clause{
		{invoke("follows", "?b", "?c")},
		{scopePattern("?b", "page/next", "?c")},
	}}
	rs := mustRuleSet(t, []*query.Rule{
		defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
		defineRule("follows", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?b"), orJoin),
	})
	magic := mustMagic(t, rs, "follows", true, false)
	got := magic.Rules.Rules(sym("adorned.bf/follows"))
	assertRules(t, "adorned.bf/follows", got, []*query.Rule{
		defineRule("adorned.bf/follows", []string{"?a", "?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
		defineRule("adorned.bf/follows", []string{"?a", "?c"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b"),
			&query.OrJoinClause{OutputVars: []query.Symbol{sym("?b"), sym("?c")}, Branches: [][]query.Clause{
				{invoke("adorned.bf/follows", "?b", "?c")},
				{scopePattern("?b", "page/next", "?c")},
			}}),
	})
	assertRules(t, "magic.bf/follows", magic.Rules.Rules(sym("magic.bf/follows")), []*query.Rule{
		defineRule("magic.bf/follows", []string{"?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
	})
	if len(got) == 2 && len(got[1].Body) == 3 {
		rewritten, ok := got[1].Body[2].(*query.OrJoinClause)
		if !ok || rewritten == orJoin {
			t.Error("the or-join on the path to the adorned invocation is copied, not shared")
		} else if rewritten.Branches[1][0] != orJoin.Branches[1][0] {
			t.Error("a branch holding no invocation shares its clauses")
		}
	}
}

// An invocation whose variables no clause of the body provides is bound by
// nothing in any order: it keeps its name and its predicate's original rules
// join the set, and the planner places it after the pattern seeking on the
// bound head position.
func TestMagic_UnboundInvocationKeepsOriginalRules(t *testing.T) {
	follows := followsRules()
	rs := mustRuleSet(t, append(follows,
		defineRule("any", []string{"?x"}, invoke("follows", "?y", "?z"), scopePattern("?x", "page/kind", "_"))))
	magic := mustMagic(t, rs, "any", true)
	assertRules(t, "adorned.b/any", magic.Rules.Rules(sym("adorned.b/any")), []*query.Rule{
		defineRule("adorned.b/any", []string{"?x"},
			invoke("magic.b/any", "?x"),
			scopePattern("?x", "page/kind", "_"),
			invoke("follows", "?y", "?z")),
	})
	got := magic.Rules.Rules(sym("follows"))
	if len(got) != 2 || got[0] != follows[0] || got[1] != follows[1] {
		t.Errorf("Rules(follows) = %v, want the two original rules unchanged: nothing binds the invocation", got)
	}
	assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.b/any", "adorned.b/any", "follows")
	if rules := magic.Rules.Rules(sym("magic.b/any")); len(rules) != 0 {
		t.Errorf("the seed predicate of a non-recursive rule has no rules, got %v", rules)
	}
	if arity, ok := magic.Rules.Arity(sym("magic.b/any")); !ok || arity != 1 {
		t.Errorf("Arity(magic.b/any) = %d, %v; want 1, true", arity, ok)
	}
}

// subqueryCall is a call of inner with the sources, the rule set and the one
// argument, bound to the variable named.
func subqueryCall(inner *query.Query, argument query.PatternElement, bound string) *query.SubqueryPattern {
	return &query.SubqueryPattern{
		Query:   inner,
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Constant{Value: datalog.NewSymbol("%")}, argument},
		Binding: query.RelationBinding{Variables: []query.Symbol{sym(bound)}},
	}
}

// A subquery is a scope of the rewrite: its inner :in symbol stands for the
// call's argument, so an invocation inside the inner query is adorned by the
// arguments the clauses before the call bind, and its magic rule names the
// argument the inner symbol stands for, derived from the clauses before the
// call. The subquery on the path to the adorned invocation is copied over an
// inner query derived over the rewritten clauses, and the original set is
// unchanged.
func TestMagic_InvocationInsideASubqueryBodyIsAdornedThroughTheCall(t *testing.T) {
	inner := &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: sym("?w")}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?p")}},
		Where: []query.Clause{invoke("reach", "?p", "?w")},
	}
	call := subqueryCall(inner, query.Variable{Name: sym("?y")}, "?z")
	rules := []*query.Rule{
		defineRule("reach", []string{"?x", "?y"}, scopePattern("?x", "edge", "?y")),
		defineRule("reach", []string{"?x", "?z"}, scopePattern("?x", "edge", "?y"), call),
	}
	rs := mustRuleSet(t, rules)
	magic := mustMagic(t, rs, "reach", true, false)
	got := magic.Rules.Rules(sym("adorned.bf/reach"))
	assertRules(t, "adorned.bf/reach", got, []*query.Rule{
		defineRule("adorned.bf/reach", []string{"?x", "?y"},
			invoke("magic.bf/reach", "?x"),
			scopePattern("?x", "edge", "?y")),
		defineRule("adorned.bf/reach", []string{"?x", "?z"},
			invoke("magic.bf/reach", "?x"),
			scopePattern("?x", "edge", "?y"),
			subqueryCall(&query.Query{Find: inner.Find, In: inner.In, Where: []query.Clause{invoke("adorned.bf/reach", "?p", "?w")}}, query.Variable{Name: sym("?y")}, "?z")),
	})
	assertRules(t, "magic.bf/reach", magic.Rules.Rules(sym("magic.bf/reach")), []*query.Rule{
		defineRule("magic.bf/reach", []string{"?y"},
			invoke("magic.bf/reach", "?x"),
			scopePattern("?x", "edge", "?y")),
	})
	if len(got) == 2 && len(got[1].Body) == 2 {
		rewritten, ok := got[1].Body[1].(*query.SubqueryPattern)
		if !ok || rewritten == call || rewritten.Query == inner {
			t.Error("the subquery on the path to the adorned invocation is copied over a derived inner query, not shared")
		}
	}
	assertRules(t, "the original set", rs.Rules(sym("reach")), rules)
	if inner.Where[0] != query.Clause(rules[1].Body[1].(*query.SubqueryPattern).Query.Where[0]) {
		t.Error("the original inner query is unchanged")
	}
	assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.bf/reach", "adorned.bf/reach")
	strata := magic.Rules.Strata()
	if len(strata) != 2 || !strata[0].Recursive || !strata[1].Recursive {
		t.Fatalf("strata = %+v, want two recursive strata, the magic set then the closure", strata)
	}
}

// The magic rule of an invocation inside an inner query reads the inner
// query's clauses before the invocation as a subquery of its own over the
// same call, finding the invocation's bound variable: a local the inner
// clauses bind is bound to a fresh variable named for the magic predicate
// and the position, as a constant argument's is. The subquery on the path
// is copied over an inner query holding the adorned invocation after the
// clauses before it, which are shared.
func TestMagic_InvocationInsideASubqueryBodyReadsTheInnerClausesBeforeIt(t *testing.T) {
	kind := scopePattern("?p", "kind", "?k")
	inner := &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: sym("?w")}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?p")}},
		Where: []query.Clause{kind, invoke("reach", "?k", "?w")},
	}
	rs := mustRuleSet(t, []*query.Rule{
		defineRule("reach", []string{"?x", "?y"}, scopePattern("?x", "edge", "?y")),
		defineRule("reach", []string{"?x", "?z"}, scopePattern("?x", "edge", "?y"), subqueryCall(inner, query.Variable{Name: sym("?y")}, "?z")),
	})
	magic := mustMagic(t, rs, "reach", true, false)
	got := magic.Rules.Rules(sym("adorned.bf/reach"))
	assertRules(t, "adorned.bf/reach", got, []*query.Rule{
		defineRule("adorned.bf/reach", []string{"?x", "?y"},
			invoke("magic.bf/reach", "?x"),
			scopePattern("?x", "edge", "?y")),
		defineRule("adorned.bf/reach", []string{"?x", "?z"},
			invoke("magic.bf/reach", "?x"),
			scopePattern("?x", "edge", "?y"),
			subqueryCall(&query.Query{Find: inner.Find, In: inner.In, Where: []query.Clause{kind, invoke("adorned.bf/reach", "?k", "?w")}}, query.Variable{Name: sym("?y")}, "?z")),
	})
	assertRules(t, "magic.bf/reach", magic.Rules.Rules(sym("magic.bf/reach")), []*query.Rule{
		defineRule("magic.bf/reach", []string{"?magic.bf/reach.0"},
			invoke("magic.bf/reach", "?x"),
			scopePattern("?x", "edge", "?y"),
			subqueryCall(&query.Query{Find: []query.FindElement{query.FindVariable{Symbol: sym("?k")}}, In: inner.In, Where: []query.Clause{kind}}, query.Variable{Name: sym("?y")}, "?magic.bf/reach.0")),
	})
	if len(got) == 2 && len(got[1].Body) == 2 {
		if rewritten, ok := got[1].Body[1].(*query.SubqueryPattern); ok && rewritten.Query.Where[0] != query.Clause(kind) {
			t.Error("the inner clause before the adorned invocation is shared")
		}
	}
}

// An invocation inside a subquery whose argument nothing before the call
// binds is not adorned: the clause is shared, and the predicate it invokes
// keeps its original rules.
func TestMagic_InvocationInsideASubqueryBodyWithAnUnboundArgumentStands(t *testing.T) {
	sub := subqueryCall(&query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: sym("?v")}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?w")}},
		Where: []query.Clause{invoke("s", "?w", "?v")},
	}, query.Variable{Name: sym("?w")}, "?v")
	s := defineRule("s", []string{"?w", "?v"}, scopePattern("?w", "page/c", "?v"))
	rs := mustRuleSet(t, []*query.Rule{
		s,
		defineRule("q", []string{"?x", "?z"}, scopePattern("?x", "page/a", "?z")),
		defineRule("q", []string{"?x", "?z"},
			scopePattern("?x", "page/a", "?y"),
			invoke("q", "?y", "?w"),
			&query.NotClause{Clauses: []query.Clause{sub}},
			scopePattern("?w", "page/b", "?z")),
	})
	magic := mustMagic(t, rs, "q", true, false)
	got := magic.Rules.Rules(sym("adorned.bf/q"))
	assertRules(t, "adorned.bf/q", got, []*query.Rule{
		defineRule("adorned.bf/q", []string{"?x", "?z"},
			invoke("magic.bf/q", "?x"),
			scopePattern("?x", "page/a", "?z")),
		defineRule("adorned.bf/q", []string{"?x", "?z"},
			invoke("magic.bf/q", "?x"),
			scopePattern("?x", "page/a", "?y"),
			invoke("adorned.bf/q", "?y", "?w"),
			scopePattern("?w", "page/b", "?z"),
			&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?w")}, Clauses: []query.Clause{sub}}),
	})
	if len(got) == 2 && len(got[1].Body) == 5 {
		if negation, ok := got[1].Body[4].(*query.NotJoinClause); ok && negation.Clauses[0] != query.Clause(sub) {
			t.Error("a subquery holding no adorned invocation is shared")
		}
	}
	if original := magic.Rules.Rules(sym("s")); len(original) != 1 || original[0] != s {
		t.Errorf("the predicate the subquery body invokes keeps its original rules, got %v", original)
	}
	assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.bf/q", "adorned.bf/q", "s")
}

// An invocation of a predicate with a required position is adorned by what
// the planned clauses before it bind, and the planner places the clause
// binding the position before the invocation whatever the written order,
// so a rule written with the invocation first adorns it as a rule written
// with the binder first does.
func TestMagic_RequiredPositionBoundByThePlannedOrder(t *testing.T) {
	rs := mustRuleSet(t, []*query.Rule{
		smallRule(),
		defineRule("holds", []string{"?x"}, scopePattern("?x", "page/n", "?n"), invoke("small", "?n")),
		defineRule("loose", []string{"?x"}, invoke("small", "?n"), scopePattern("?x", "page/n", "?n")),
	})
	loose := mustMagic(t, rs, "loose", true)
	assertRules(t, "adorned.b/loose", loose.Rules.Rules(sym("adorned.b/loose")), []*query.Rule{
		defineRule("adorned.b/loose", []string{"?x"},
			invoke("magic.b/loose", "?x"),
			scopePattern("?x", "page/n", "?n"),
			invoke("adorned.b/small", "?n")),
	})

	magic := mustMagic(t, rs, "holds", true)
	assertRules(t, "adorned.b/holds", magic.Rules.Rules(sym("adorned.b/holds")), []*query.Rule{
		defineRule("adorned.b/holds", []string{"?x"},
			invoke("magic.b/holds", "?x"),
			scopePattern("?x", "page/n", "?n"),
			invoke("adorned.b/small", "?n")),
	})
	if adorned := magic.Rules.Rules(sym("adorned.b/small")); len(adorned) != 1 || len(adorned[0].Required) != 0 {
		t.Errorf("adorned.b/small has no required run, its guard binding the position small requires, got %v", adorned)
	}
	if required := magic.Rules.Required(sym("adorned.b/small")); query.CountBound(required) != 0 {
		t.Errorf("Required(adorned.b/small) = %v, want no required position", required)
	}
}

// A magic rule reads, of the planned clauses before the invocation, those
// that are monotone. A comparison or a not-join written before the
// invocation and reading a variable the invocation provides is placed after
// it by the planner, which orders a clause after its binders, so neither is
// read; an or-default is not read, so the predicate it invokes stays below
// the recursive stratum.
func TestMagic_MagicRuleReadsReadyMonotoneClauses(t *testing.T) {
	t.Run("a comparison the planner places after the invocation is not read", func(t *testing.T) {
		rs := mustRuleSet(t, append(followsRules(),
			defineRule("t", []string{"?a", "?c"},
				&query.Comparison{Op: datalog.SymLT, Left: query.VariableTerm{Symbol: sym("?n")}, Right: query.ConstantTerm{Value: int64(3)}},
				invoke("follows", "?a", "?c"),
				scopePattern("?c", "page/n", "?n"))))
		magic := mustMagic(t, rs, "t", true, false)
		assertRules(t, "magic.bf/follows", magic.Rules.Rules(sym("magic.bf/follows")), []*query.Rule{
			defineRule("magic.bf/follows", []string{"?a"},
				invoke("magic.bf/t", "?a")),
			defineRule("magic.bf/follows", []string{"?b"},
				invoke("magic.bf/follows", "?a"),
				scopePattern("?a", "page/next", "?b")),
		})
	})

	t.Run("a not-join the planner places after the invocation is not read", func(t *testing.T) {
		rs := mustRuleSet(t, append(followsRules(),
			defineRule("t", []string{"?a", "?c"},
				&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?w")}, Clauses: []query.Clause{scopePattern("?w", "page/marked", "_")}},
				invoke("follows", "?a", "?c"),
				scopePattern("?w", "page/next", "?c"))))
		magic := mustMagic(t, rs, "t", true, false)
		assertRules(t, "magic.bf/follows", magic.Rules.Rules(sym("magic.bf/follows")), []*query.Rule{
			defineRule("magic.bf/follows", []string{"?a"},
				invoke("magic.bf/t", "?a")),
			defineRule("magic.bf/follows", []string{"?b"},
				invoke("magic.bf/follows", "?a"),
				scopePattern("?a", "page/next", "?b")),
		})
	})

	t.Run("a fallback form is not read", func(t *testing.T) {
		rs := mustRuleSet(t, []*query.Rule{
			defineRule("marked", []string{"?b"}, scopePattern("?b", "page/marked", "_")),
			defineRule("walk", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?c")),
			defineRule("walk", []string{"?a", "?c"},
				scopePattern("?a", "page/next", "?b"),
				&query.OrDefaultClause{Branches: [][]query.Clause{
					{invoke("marked", "?b")},
					{scopePattern("?b", "page/kind", "_")},
				}},
				invoke("walk", "?b", "?c")),
		})
		magic := mustMagic(t, rs, "walk", true, false)
		assertRules(t, "magic.bf/walk", magic.Rules.Rules(sym("magic.bf/walk")), []*query.Rule{
			defineRule("magic.bf/walk", []string{"?b"},
				invoke("magic.bf/walk", "?a"),
				scopePattern("?a", "page/next", "?b")),
		})
		stratumOf := func(name string) int {
			for i, stratum := range magic.Rules.Strata() {
				if query.ContainsSymbol(stratum.Predicates, sym(name)) {
					return i
				}
			}
			t.Fatalf("no stratum holds %s in %+v", name, magic.Rules.Strata())
			return -1
		}
		if marked, walk := stratumOf("adorned.b/marked"), stratumOf("adorned.bf/walk"); marked >= walk {
			t.Errorf("adorned.b/marked is in stratum %d and adorned.bf/walk in %d; the fallback's predicate sits below the recursive stratum", marked, walk)
		}
	})
}

// A binding reaches an invocation under not or not-join from the head's
// bound positions and the planned clauses before the first invocation of
// the rule's own stratum, and its magic rule reads those clauses. A clause
// after that invocation is adorned under what it binds, so a magic rule
// reading it would depend on the adorned predicate, which depends through
// the negation on the negated one: a cycle the original set does not have.
// The planner places a not-join after the pattern seeking on the
// recursion's output, so the rewritten body ends with it.
func TestMagic_NegationAfterARecursiveInvocationRewrites(t *testing.T) {
	t.Run("a binding the recursion provides does not reach the negation", func(t *testing.T) {
		rs := mustRuleSet(t, []*query.Rule{
			defineRule("q", []string{"?x", "?z"}, scopePattern("?x", "page/a", "?z")),
			defineRule("q", []string{"?x", "?z"},
				scopePattern("?x", "page/a", "?y"),
				invoke("q", "?y", "?w"),
				&query.NotClause{Clauses: []query.Clause{invoke("s", "?w")}},
				scopePattern("?w", "page/b", "?z")),
			defineRule("s", []string{"?w"}, scopePattern("?w", "page/c", "_")),
		})
		magic := mustMagic(t, rs, "q", true, false)
		assertRules(t, "adorned.bf/q", magic.Rules.Rules(sym("adorned.bf/q")), []*query.Rule{
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?z")),
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y"),
				invoke("adorned.bf/q", "?y", "?w"),
				scopePattern("?w", "page/b", "?z"),
				&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?w")}, Clauses: []query.Clause{invoke("s", "?w")}}),
		})
		assertRules(t, "magic.bf/q", magic.Rules.Rules(sym("magic.bf/q")), []*query.Rule{
			defineRule("magic.bf/q", []string{"?y"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y")),
		})
		assertRules(t, "s", magic.Rules.Rules(sym("s")), rs.Rules(sym("s")))
		assertRules(t, "magic.b/s", magic.Rules.Rules(sym("magic.b/s")), nil)
	})

	t.Run("a binding a clause before the recursion provides reaches the negation", func(t *testing.T) {
		rs := mustRuleSet(t, []*query.Rule{
			defineRule("q", []string{"?x", "?z"}, scopePattern("?x", "page/a", "?z")),
			defineRule("q", []string{"?x", "?z"},
				scopePattern("?x", "page/a", "?y"),
				invoke("q", "?y", "?w"),
				&query.NotClause{Clauses: []query.Clause{invoke("s", "?y")}},
				scopePattern("?w", "page/b", "?z")),
			defineRule("s", []string{"?w"}, scopePattern("?w", "page/c", "_")),
		})
		magic := mustMagic(t, rs, "q", true, false)
		assertRules(t, "adorned.bf/q", magic.Rules.Rules(sym("adorned.bf/q")), []*query.Rule{
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?z")),
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y"),
				invoke("adorned.bf/q", "?y", "?w"),
				scopePattern("?w", "page/b", "?z"),
				&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?y")}, Clauses: []query.Clause{invoke("adorned.b/s", "?y")}}),
		})
		assertRules(t, "magic.b/s", magic.Rules.Rules(sym("magic.b/s")), []*query.Rule{
			defineRule("magic.b/s", []string{"?y"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y")),
		})
		assertRules(t, "adorned.b/s", magic.Rules.Rules(sym("adorned.b/s")), []*query.Rule{
			defineRule("adorned.b/s", []string{"?w"},
				invoke("magic.b/s", "?w"),
				scopePattern("?w", "page/c", "_")),
		})
	})

	t.Run("a binding a clause after the recursion provides does not reach the negation", func(t *testing.T) {
		rs := mustRuleSet(t, []*query.Rule{
			defineRule("q", []string{"?x", "?z"}, scopePattern("?x", "page/a", "?z")),
			defineRule("q", []string{"?x", "?z"},
				scopePattern("?x", "page/a", "?y"),
				invoke("q", "?y", "?w"),
				scopePattern("?w", "page/d", "?u"),
				&query.NotClause{Clauses: []query.Clause{invoke("s", "?u")}},
				scopePattern("?w", "page/b", "?z")),
			defineRule("s", []string{"?w"}, scopePattern("?w", "page/c", "_")),
		})
		magic := mustMagic(t, rs, "q", true, false)
		assertRules(t, "adorned.bf/q", magic.Rules.Rules(sym("adorned.bf/q")), []*query.Rule{
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?z")),
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y"),
				invoke("adorned.bf/q", "?y", "?w"),
				scopePattern("?w", "page/d", "?u"),
				scopePattern("?w", "page/b", "?z"),
				&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?u")}, Clauses: []query.Clause{invoke("s", "?u")}}),
		})
		assertRules(t, "s", magic.Rules.Rules(sym("s")), rs.Rules(sym("s")))
		assertRules(t, "magic.b/s", magic.Rules.Rules(sym("magic.b/s")), nil)
	})
}

// A site under a non-monotone form is left unadorned where adorning it would
// place its adorned predicate in the rule's own stratum: q's second rule
// invokes s after the recursive invocation of q, so the magic rule of
// adorned.b/s reads the adorned q and adorned.b/s joins q's stratum, and q's
// third rule negates s under the same adornment, an edge through the
// negation within that stratum. The negated site keeps s's name, s's
// original rules join the set, the positive site stays adorned, and the set
// is accepted (NON_MONOTONE_FORMS_PROOF.md T6, L4).
func TestMagic_ANegatedSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned(t *testing.T) {
	s := defineRule("s", []string{"?w"}, scopePattern("?w", "page/c", "_"))
	rs := mustRuleSet(t, []*query.Rule{
		defineRule("q", []string{"?x", "?z"}, scopePattern("?x", "page/a", "?z")),
		defineRule("q", []string{"?x", "?z"},
			scopePattern("?x", "page/a", "?y"),
			invoke("q", "?y", "?w"),
			invoke("s", "?w"),
			scopePattern("?w", "page/b", "?z")),
		defineRule("q", []string{"?x", "?z"},
			scopePattern("?x", "page/a", "?w"),
			&query.NotClause{Clauses: []query.Clause{invoke("s", "?w")}},
			scopePattern("?w", "page/b", "?z")),
		s,
	})
	magic := mustMagic(t, rs, "q", true, false)
	assertRules(t, "adorned.bf/q", magic.Rules.Rules(sym("adorned.bf/q")), []*query.Rule{
		defineRule("adorned.bf/q", []string{"?x", "?z"},
			invoke("magic.bf/q", "?x"),
			scopePattern("?x", "page/a", "?z")),
		defineRule("adorned.bf/q", []string{"?x", "?z"},
			invoke("magic.bf/q", "?x"),
			scopePattern("?x", "page/a", "?y"),
			invoke("adorned.bf/q", "?y", "?w"),
			scopePattern("?w", "page/b", "?z"),
			invoke("adorned.b/s", "?w")),
		defineRule("adorned.bf/q", []string{"?x", "?z"},
			invoke("magic.bf/q", "?x"),
			scopePattern("?x", "page/a", "?w"),
			scopePattern("?w", "page/b", "?z"),
			&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?w")}, Clauses: []query.Clause{invoke("s", "?w")}}),
	})
	if original := magic.Rules.Rules(sym("s")); len(original) != 1 || original[0] != s {
		t.Errorf("the negated site keeps s's original rules, got %v", original)
	}
	if adorned := magic.Rules.Rules(sym("adorned.b/s")); len(adorned) != 1 {
		t.Errorf("the positive site adorns s, got %v", adorned)
	}
	assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.bf/q", "adorned.bf/q", "magic.b/s", "adorned.b/s", "s")
}

// The rewrite walks every scope in the planner's order: a body under its
// head's bound positions with the free ones as its find, a header form's
// branch under the form's header, a not-join body under its header with no
// find, and a subquery's inner query under its data inputs with the inner
// find. The clauses before an invocation are then the planner's, so an
// invocation written before the pattern binding its argument is adorned by
// that pattern and its magic rule reads it, and every rewritten scope holds
// its clauses in the planned order (NESTED_SCOPES_AS_QUERIES.md §5.5). In
// each scope below the planner places a pattern seeking on an available
// variable before an invocation with none, and an invocation with an
// available argument before a pattern reading nothing available.
func TestMagic_EveryScopeIsWalkedInThePlannersOrder(t *testing.T) {
	next := func() *query.Rule {
		return defineRule("next", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b"))
	}
	adornedNext := defineRule("adorned.bf/next", []string{"?a", "?b"}, invoke("magic.bf/next", "?a"), scopePattern("?a", "page/next", "?b"))
	kind := scopePattern("?p", "kind", "?k")
	inner := &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: sym("?w")}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?p")}},
		Where: []query.Clause{invoke("next", "?k", "?w"), kind},
	}
	x, y := []query.Symbol{sym("?x")}, []query.Symbol{sym("?y")}
	cases := []struct {
		name    string
		rule    *query.Rule
		adorned *query.Rule
		magic   *query.Rule
	}{
		{
			name:    "a body",
			rule:    defineRule("reached", []string{"?x", "?y"}, invoke("next", "?w", "?y"), scopePattern("?x", "page/next", "?w")),
			adorned: defineRule("adorned.bf/reached", []string{"?x", "?y"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/next", "?w"), invoke("adorned.bf/next", "?w", "?y")),
			magic:   defineRule("magic.bf/next", []string{"?w"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/next", "?w")),
		},
		{
			name: "an or-join branch",
			rule: defineRule("reached", []string{"?x", "?y"}, scopePattern("?x", "page/root", "_"), &query.OrJoinClause{RequiredVars: x, OutputVars: y,
				Branches: [][]query.Clause{{invoke("next", "?x", "?y"), scopePattern("?x", "page/kind", "_")}}}),
			adorned: defineRule("adorned.bf/reached", []string{"?x", "?y"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/root", "_"), &query.OrJoinClause{RequiredVars: x, OutputVars: y,
				Branches: [][]query.Clause{{scopePattern("?x", "page/kind", "_"), invoke("adorned.bf/next", "?x", "?y")}}}),
			magic: defineRule("magic.bf/next", []string{"?x"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/root", "_")),
		},
		{
			name: "an or-default-join branch",
			rule: defineRule("reached", []string{"?x", "?y"}, scopePattern("?x", "page/root", "_"), &query.OrDefaultJoinClause{RequiredVars: x, OutputVars: y,
				Branches: [][]query.Clause{{invoke("next", "?x", "?y"), scopePattern("?x", "page/kind", "_")}, {scopePattern("?x", "page/other", "?y")}}}),
			adorned: defineRule("adorned.bf/reached", []string{"?x", "?y"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/root", "_"), &query.OrDefaultJoinClause{RequiredVars: x, OutputVars: y,
				Branches: [][]query.Clause{{scopePattern("?x", "page/kind", "_"), invoke("adorned.bf/next", "?x", "?y")}, {scopePattern("?x", "page/other", "?y")}}}),
			magic: defineRule("magic.bf/next", []string{"?x"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/root", "_")),
		},
		{
			name: "a not-join body",
			rule: defineRule("reached", []string{"?x", "?y"},
				&query.NotJoinClause{RequiredVars: x, Clauses: []query.Clause{scopePattern("?z", "page/kind", "?k"), invoke("next", "?x", "?z")}},
				scopePattern("?x", "page/next", "?y")),
			adorned: defineRule("adorned.bf/reached", []string{"?x", "?y"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/next", "?y"),
				&query.NotJoinClause{RequiredVars: x, Clauses: []query.Clause{invoke("adorned.bf/next", "?x", "?z"), scopePattern("?z", "page/kind", "?k")}}),
			magic: defineRule("magic.bf/next", []string{"?x"}, invoke("magic.bf/reached", "?x"), scopePattern("?x", "page/next", "?y")),
		},
		{
			name: "a subquery's inner query",
			rule: defineRule("reach2", []string{"?x", "?z"}, subqueryCall(inner, query.Variable{Name: sym("?y")}, "?z"), scopePattern("?x", "edge", "?y")),
			adorned: defineRule("adorned.bf/reach2", []string{"?x", "?z"}, invoke("magic.bf/reach2", "?x"), scopePattern("?x", "edge", "?y"),
				subqueryCall(&query.Query{Find: inner.Find, In: inner.In, Where: []query.Clause{kind, invoke("adorned.bf/next", "?k", "?w")}}, query.Variable{Name: sym("?y")}, "?z")),
			magic: defineRule("magic.bf/next", []string{"?magic.bf/next.0"}, invoke("magic.bf/reach2", "?x"), scopePattern("?x", "edge", "?y"),
				subqueryCall(&query.Query{Find: []query.FindElement{query.FindVariable{Symbol: sym("?k")}}, In: inner.In, Where: []query.Clause{kind}}, query.Variable{Name: sym("?y")}, "?magic.bf/next.0")),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rs := mustRuleSet(t, []*query.Rule{next(), c.rule})
			magic := mustMagic(t, rs, c.rule.Name.String(), true, false)
			assertRules(t, c.adorned.Name.String(), magic.Rules.Rules(c.adorned.Name), []*query.Rule{c.adorned})
			assertRules(t, "magic.bf/next", magic.Rules.Rules(sym("magic.bf/next")), []*query.Rule{c.magic})
			assertRules(t, "adorned.bf/next", magic.Rules.Rules(sym("adorned.bf/next")), []*query.Rule{adornedNext})
		})
	}
}

// Each branch of a header form is planned as a scope under the form's
// required run and outputs, so a branch written with an invocation before
// the pattern seeking on the required run is answered pattern first, and a
// branch of one clause as it is; a not-join's body is planned under its
// header with no outputs, where an invocation bound by the header precedes
// a pattern reading nothing available.
func TestPlannedBranchesPlanEachBranchUnderTheFormsHeader(t *testing.T) {
	rs := mustRuleSet(t, []*query.Rule{defineRule("next", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b"))})
	w := &magicRewriter{rs: rs, planner: NewClauseBasedPlanner(nil, PlannerOptions{})}
	x, y := []query.Symbol{sym("?x")}, []query.Symbol{sym("?y")}
	asRule := func(label string, clauses []query.Clause) []*query.Rule {
		return []*query.Rule{defineRule(label, []string{"?x", "?y"}, clauses...)}
	}

	branches, err := w.plannedBranches(x, y, [][]query.Clause{
		{invoke("next", "?x", "?y"), scopePattern("?x", "page/kind", "_")},
		{scopePattern("?x", "page/other", "?y")},
	})
	if err != nil {
		t.Fatalf("plannedBranches: %v", err)
	}
	if len(branches) != 2 {
		t.Fatalf("planned %d branches, want 2", len(branches))
	}
	assertRules(t, "branch 1", asRule("b", branches[0]), asRule("b", []query.Clause{scopePattern("?x", "page/kind", "_"), invoke("next", "?x", "?y")}))
	assertRules(t, "branch 2", asRule("b", branches[1]), asRule("b", []query.Clause{scopePattern("?x", "page/other", "?y")}))

	body, err := w.plannedBranches(x, nil, [][]query.Clause{{scopePattern("?z", "page/kind", "?k"), invoke("next", "?x", "?z")}})
	if err != nil {
		t.Fatalf("plannedBranches: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("planned %d bodies, want 1", len(body))
	}
	assertRules(t, "the not-join body", asRule("b", body[0]), asRule("b", []query.Clause{invoke("next", "?x", "?z"), scopePattern("?z", "page/kind", "?k")}))
}

// A fallback site is left unadorned the same way: walk's second rule invokes
// marked positively after the recursion, so adorned.b/marked's magic rule
// reads the adorned recursion and adorned.b/marked joins walk's stratum, and
// walk's third rule invokes marked under an or-default where the sealed
// passing would adorn it the same way, a non-monotone edge within that
// stratum. The fallback's site keeps marked's name, marked's original rules
// join the set, the positive site stays adorned, and the set is accepted.
func TestMagic_AFallbackSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned(t *testing.T) {
	marked := defineRule("marked", []string{"?b"}, scopePattern("?b", "page/marked", "_"))
	rs := mustRuleSet(t, []*query.Rule{
		marked,
		defineRule("walk", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?c")),
		defineRule("walk", []string{"?a", "?c"},
			scopePattern("?a", "page/next", "?b"),
			invoke("walk", "?b", "?c"),
			invoke("marked", "?c")),
		defineRule("walk", []string{"?a", "?c"},
			scopePattern("?a", "page/next", "?b"),
			&query.OrDefaultClause{Branches: [][]query.Clause{
				{invoke("marked", "?b")},
				{scopePattern("?b", "page/kind", "_")},
			}},
			invoke("walk", "?b", "?c")),
	})
	magic := mustMagic(t, rs, "walk", true, false)
	rules := magic.Rules.Rules(sym("adorned.bf/walk"))
	if len(rules) != 3 {
		t.Fatalf("adorned.bf/walk has %d rules, want 3:\n%s", len(rules), renderRules(rules))
	}
	var fallback *query.OrDefaultJoinClause
	for _, clause := range rules[2].Body {
		if c, ok := clause.(*query.OrDefaultJoinClause); ok {
			fallback = c
		}
	}
	if fallback == nil {
		t.Fatalf("the third rule holds the fallback: %s", rules[2])
	}
	if inv, ok := fallback.Branches[0][0].(*query.RuleInvocation); !ok || inv.Name != sym("marked") {
		t.Errorf("the fallback's site keeps marked's name, got %v", fallback.Branches[0][0])
	}
	if original := magic.Rules.Rules(sym("marked")); len(original) != 1 || original[0] != marked {
		t.Errorf("marked keeps its original rules, got %v", original)
	}
	if adorned := magic.Rules.Rules(sym("adorned.b/marked")); len(adorned) != 1 {
		t.Errorf("the positive site adorns marked, got %v", adorned)
	}
}

// An aggregating inner query is a non-monotone form, so an invocation inside
// it is adorned by the sealed passing. With the call's argument bound by the
// recursion, the planner places the call after the recursion, the sealed
// passing holds nothing of the argument, and the invocation keeps its name,
// the subquery is shared and marked's original rules join the set; a magic
// rule reading the adorned recursion would put adorned marked in walk's
// stratum under an aggregate, which the constructor refuses. With the
// argument bound by the head, the invocation is adorned from the guard.
func TestMagic_AnInvocationInsideAnAggregatingInnerQueryReadsTheSealedPassing(t *testing.T) {
	marked := defineRule("marked", []string{"?p", "?m"}, scopePattern("?p", "page/mark", "?m"))
	count := func() *query.Query {
		return &query.Query{
			Find:  []query.FindElement{query.FindAggregate{Function: datalog.SymCount, Arg: sym("?m")}},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?p")}},
			Where: []query.Clause{invoke("marked", "?p", "?m")},
		}
	}
	walk := func(call *query.SubqueryPattern) []*query.Rule {
		return []*query.Rule{
			marked,
			defineRule("walk", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?c")),
			defineRule("walk", []string{"?a", "?c"},
				scopePattern("?a", "page/next", "?b"),
				invoke("walk", "?b", "?c"),
				call),
		}
	}
	t.Run("the argument the recursion binds leaves the invocation unadorned", func(t *testing.T) {
		call := subqueryCall(count(), query.Variable{Name: sym("?c")}, "?n")
		magic := mustMagic(t, mustRuleSet(t, walk(call)), "walk", true, false)
		assertRules(t, "adorned.bf/walk", magic.Rules.Rules(sym("adorned.bf/walk")), []*query.Rule{
			defineRule("adorned.bf/walk", []string{"?a", "?c"},
				invoke("magic.bf/walk", "?a"),
				scopePattern("?a", "page/next", "?c")),
			defineRule("adorned.bf/walk", []string{"?a", "?c"},
				invoke("magic.bf/walk", "?a"),
				scopePattern("?a", "page/next", "?b"),
				invoke("adorned.bf/walk", "?b", "?c"),
				call),
		})
		if original := magic.Rules.Rules(sym("marked")); len(original) != 1 || original[0] != marked {
			t.Errorf("marked keeps its original rules, got %v", original)
		}
		assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.bf/walk", "adorned.bf/walk", "marked")
	})
	t.Run("the argument the head binds adorns the invocation from the guard", func(t *testing.T) {
		call := subqueryCall(count(), query.Variable{Name: sym("?a")}, "?n")
		magic := mustMagic(t, mustRuleSet(t, walk(call)), "walk", true, false)
		if adorned := magic.Rules.Rules(sym("adorned.bf/marked")); len(adorned) != 1 {
			t.Errorf("the invocation adorns marked, got %v", adorned)
		}
		if rules := magic.Rules.Rules(sym("magic.bf/marked")); len(rules) != 1 {
			t.Errorf("one magic rule derives the argument from the guard, got %v", rules)
		}
	})
}

// A predicate with required positions under a negation is adorned when the
// sealed passing binds the positions it requires, its adorned copy having no
// required run, and left unadorned when the sealed passing leaves one free,
// its original rules joining the set so that the executor derives it at its
// clause under the positions the branch binds there
// (NON_MONOTONE_FORMS_PROOF.md T5).
func TestMagic_ARequiredPredicateUnderANegation(t *testing.T) {
	q := func(small *query.Rule, negated *query.RuleInvocation) []*query.Rule {
		return []*query.Rule{
			small,
			defineRule("q", []string{"?x", "?z"}, scopePattern("?x", "page/a", "?z")),
			defineRule("q", []string{"?x", "?z"},
				scopePattern("?x", "page/a", "?y"),
				invoke("q", "?y", "?w"),
				&query.NotClause{Clauses: []query.Clause{negated}},
				scopePattern("?w", "page/b", "?z")),
		}
	}
	t.Run("bound by the sealed passing is adorned", func(t *testing.T) {
		magic := mustMagic(t, mustRuleSet(t, q(smallRule(), invoke("small", "?x"))), "q", true, false)
		assertRules(t, "magic.b/small", magic.Rules.Rules(sym("magic.b/small")), []*query.Rule{
			defineRule("magic.b/small", []string{"?x"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y")),
		})
		if adorned := magic.Rules.Rules(sym("adorned.b/small")); len(adorned) != 1 || len(adorned[0].Required) != 0 {
			t.Errorf("adorned.b/small has one rule with no required run, got %v", adorned)
		}
		if original := magic.Rules.Rules(sym("small")); len(original) != 0 {
			t.Errorf("small's original rules stay out of the set, got %v", original)
		}
	})
	t.Run("bound only after the recursion is left unadorned", func(t *testing.T) {
		small := smallRule()
		magic := mustMagic(t, mustRuleSet(t, q(small, invoke("small", "?w"))), "q", true, false)
		assertRules(t, "adorned.bf/q", magic.Rules.Rules(sym("adorned.bf/q")), []*query.Rule{
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?z")),
			defineRule("adorned.bf/q", []string{"?x", "?z"},
				invoke("magic.bf/q", "?x"),
				scopePattern("?x", "page/a", "?y"),
				invoke("adorned.bf/q", "?y", "?w"),
				scopePattern("?w", "page/b", "?z"),
				&query.NotJoinClause{RequiredVars: []query.Symbol{sym("?w")}, Clauses: []query.Clause{invoke("small", "?w")}}),
		})
		if original := magic.Rules.Rules(sym("small")); len(original) != 1 || original[0] != small {
			t.Errorf("small keeps its original rules, got %v", original)
		}
		if adorned := magic.Rules.Rules(sym("adorned.b/small")); len(adorned) != 0 {
			t.Errorf("the site is left unadorned, got %v", adorned)
		}
	})
}
