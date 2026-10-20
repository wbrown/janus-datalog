package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// mustPrepare returns q prepared under rules, the query the engine takes.
func mustPrepare(t *testing.T, q *query.Query, rules *query.RuleSet) *query.Query {
	t.Helper()
	prepared, err := query.Prepare(q, rules)
	require.NoError(t, err)
	return prepared
}

// TestCompile_InfersAnOrsHeaderFromTheText: an or shares ?q with the pattern
// and the find, and every branch binds it, so ?q is the form's output; it
// shares ?a with the ground written after it, and one branch binds it, so
// ?a is required. Preparation writes that header, and the union keys on the
// whole of it, the required run and the outputs, and requires the header
// variables the outer relation binds. The decompiled clause states the
// header, and the ground binding ?a folds before the form whatever the
// written order.
func TestCompile_InfersAnOrsHeaderFromTheText(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?q
	  :where [?q :t/n _]
	         (or [?q ?a ?v] [?q :t/tag :t/none])
	         [(ground :t/tag) ?a]]`)
	require.NoError(t, err)

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)

	a, qs := datalog.NewSymbol("?a"), datalog.NewSymbol("?q")
	unionNode := findAlgebraNode(root, RuleUnion)
	require.NotNil(t, unionNode, "the or compiles to a union")
	union := unionNode.Data.(*Union)
	require.Equal(t, []query.Symbol{a}, union.RequiredVars)
	require.Equal(t, []query.Symbol{a, qs}, union.Output)
	require.Equal(t, union.Output, union.JoinVars)
	require.Equal(t, []query.Symbol{a, qs}, union.Required)
	require.Equal(t, "∪_join([?a] ?q)", union.String())

	clauses, err := Decompile(root)
	require.NoError(t, err)
	ground, orJoin := -1, -1
	for i, c := range clauses {
		switch c := c.(type) {
		case *query.Expression:
			ground = i
		case *query.OrJoinClause:
			orJoin = i
			require.Equal(t, "(or-join [[?a] ?q] [?q ?a ?v] [?q :t/tag :t/none])", c.String())
		}
	}
	require.NotEqual(t, -1, ground, "the ground decompiles, got %v", clauses)
	require.NotEqual(t, -1, orJoin, "the or decompiles as an or-join, got %v", clauses)
	require.Less(t, ground, orJoin, "the ground binding ?a folds before the or-join that requires it, got %v", clauses)
}

// TestPrepare_RefusesAFormSharingNothing: an or whose variables occur in no
// other clause, input or find element unifies with nothing, and preparation
// refuses the query naming the form.
func TestPrepare_RefusesAFormSharingNothing(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e
	  :where [?e :t/n _]
	         (or [?x :t/tag :t/a] [?x :t/tag :t/b])]`)
	require.NoError(t, err)

	_, err = query.Prepare(q, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "shares no variable with its enclosing scope")
	require.Contains(t, err.Error(), "[?x :t/tag :t/a]")
}

// TestCompile_RequiresAPreparedQuery: the engine takes a prepared query and
// infers nothing, so a query holding a plain or reaches ScopeOf, which
// refuses a headerless form, through the fold's ordering.
func TestCompile_RequiresAPreparedQuery(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?q
	  :where [?q :t/n _]
	         (or [?q :t/tag ?v] [?q :t/n ?v])]`)
	require.NoError(t, err)

	defer func() {
		if recover() == nil {
			t.Error("Compile returned on a query holding a plain or; the engine takes a prepared query")
		}
	}()
	_, _ = Compile(q, nil)
}

// TestCompile_OrdersAnInvocationAfterItsRequiredBinder: under the rule set,
// succ consumes ?n at its required position, so the fold places the pattern
// binding ?n before the invocation written ahead of it.
func TestCompile_OrdersAnInvocationAfterItsRequiredBinder(t *testing.T) {
	rules, err := parser.ParseRules(`[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`)
	require.NoError(t, err)
	q, err := parser.ParseQuery(`[:find ?m :in $ % :where (succ ?n ?m) [?e :t/n ?n]]`)
	require.NoError(t, err)

	root, err := Compile(q, rules)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2)
	_, isPattern := clauses[0].(*query.DataPattern)
	require.True(t, isPattern, "the pattern binding ?n folds first, got %s", clauses[0])
	_, isInvocation := clauses[1].(*query.RuleInvocation)
	require.True(t, isInvocation, "the invocation folds after its binder, got %s", clauses[1])
}

// TestParse_OrJoinBranchOfOneClauseReadingOutsideTheHeaderIsRefused: an
// or-join branch of one clause that reads a variable outside the header reads
// a local no clause of the branch binds, so the header check, which every
// boundary runs, refuses the query at the parse naming the form, the branch
// and the variable.
func TestParse_OrJoinBranchOfOneClauseReadingOutsideTheHeaderIsRefused(t *testing.T) {
	_, err := parser.ParseQuery(`[:find ?e :where [?e :q _] (or-join [?y] [(identity ?e) ?y] [?l ?a ?y])]`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "(or-join [?y]")
	require.Contains(t, err.Error(), "branch 1 reads ?e")
}

// TestCompile_NotJoinBodyCompilesUnderItsHeader: a not-join's body sees its
// header and nothing else of the enclosing scope. A body whose one clause
// binds nothing starts from the header's variables, so the anti-join keys on
// the header alone. A body whose predicate reads an input its header does not
// name reads a local no clause of the body binds, and the header check,
// which every boundary runs, refuses the query at the parse naming the form,
// the branch and the variable.
func TestCompile_NotJoinBodyCompilesUnderItsHeader(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e :where [?e :t/n ?v] (not-join [?v] [(> ?v 1)])]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	v := datalog.NewSymbol("?v")
	antiNode := findAlgebraNode(root, RuleAntiJoin)
	require.NotNil(t, antiNode, "the not-join compiles to an anti-join")
	anti := antiNode.Data.(*AntiJoin)
	require.Equal(t, []query.Symbol{v}, anti.JoinSymbols)
	require.Empty(t, anti.Required)
	require.Equal(t, []query.Symbol{v}, antiNode.Children[1].Symbols(), "the body carries the header and nothing of the outer relation")

	_, err = parser.ParseQuery(`[:find ?e :in $ ?limit :where [?e :t/n ?v] (not-join [?v] [?v :t/m ?w] [(> ?w ?limit)])]`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "(not-join [?v]")
	require.Contains(t, err.Error(), "branch 1 reads ?limit")
}

// TestCompile_OrDefaultJoinRequiredRunAnInputBinds: an or-default-join whose
// required run names an input compiles to a lateral union whose outer schema
// is the outer relation's symbols and the required run, so the tree analyzes
// and the decompiled clause states the declared header.
func TestCompile_OrDefaultJoinRequiredRunAnInputBinds(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v
	  :in $ ?s
	  :where [?e :t/n _]
	         (or-default-join [[?e ?s] ?v] [?e :t/q ?v] [(identity ?s) ?v])]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	_, err = Analyze(root)
	require.NoError(t, err)

	e, s := datalog.NewSymbol("?e"), datalog.NewSymbol("?s")
	unionNode := findAlgebraNode(root, RuleLateralUnion)
	require.NotNil(t, unionNode, "the or-default-join compiles to a lateral union")
	union := unionNode.Data.(*LateralUnion)
	require.Equal(t, []query.Symbol{e, s}, union.RequiredVars)
	require.Contains(t, union.Required, s)

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Equal(t, "(or-default-join [[?e ?s] ?v] [?e :t/q ?v] [(identity ?s) ?v])", clauses[len(clauses)-1].String())
}

// lateralJoinsOf returns every lateral join in the tree under n, depth first.
func lateralJoinsOf(n *Node) []*LateralJoin {
	var found []*LateralJoin
	if lj, ok := n.Data.(*LateralJoin); ok {
		found = append(found, lj)
	}
	for _, child := range n.Children {
		found = append(found, lateralJoinsOf(child)...)
	}
	return found
}

// passesRules reports whether a subquery call's arguments pass %.
func passesRules(inputs []query.PatternElement) bool {
	for _, input := range inputs {
		if constant, ok := input.(query.Constant); ok && constant.Value == datalog.SymRules {
			return true
		}
	}
	return false
}

// TestCompile_LateralJoinCarriesTheRuleSetItsCallBinds: a subquery call
// passing % compiles to a lateral join carrying the rule set % is bound to,
// and a call passing no % carries none.
func TestCompile_LateralJoinCarriesTheRuleSetItsCallBinds(t *testing.T) {
	rules, err := parser.ParseRules(`[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`)
	require.NoError(t, err)
	q, err := parser.ParseQuery(`[:find ?e ?c ?d
	  :in $ %
	  :where [?e :t/n ?n]
	         [(q [:find (count ?m) :in $ % ?x :where (succ ?x ?m)] $ % ?n) [[?c]]]
	         [(q [:find (count ?t) :in $ ?x :where [?t :t/root ?x]] $ ?e) [[?d]]]]`)
	require.NoError(t, err)

	root, err := Compile(q, rules)
	require.NoError(t, err)
	joins := lateralJoinsOf(root)
	require.Len(t, joins, 2)
	for _, lj := range joins {
		if passesRules(lj.Inputs) {
			require.Same(t, rules, lj.Rules, "the call passing %% carries the rule set %% binds")
		} else {
			require.Nil(t, lj.Rules, "the call passing no %% carries no rule set")
		}
	}
}

// TestDecorrelation_CompilesTheInnerQueryUnderItsCallsRuleSet: a correlated
// aggregating subquery passing % is decorrelated, and the inner query the
// pass compiles is ordered under the rule set the call binds, so the pattern
// binding ?k comes before the invocation of succ, which requires ?k bound.
func TestDecorrelation_CompilesTheInnerQueryUnderItsCallsRuleSet(t *testing.T) {
	rules, err := parser.ParseRules(`[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`)
	require.NoError(t, err)
	q, err := parser.ParseQuery(`[:find ?e ?c
	  :in $ %
	  :where [?e :t/n _]
	         [(q [:find (count ?m) :in $ % ?x :where (succ ?k ?m) [?x :t/k ?k]] $ % ?e) [[?c]]]]`)
	require.NoError(t, err)

	root, err := Compile(q, rules)
	require.NoError(t, err)
	optimized, err := NewOptimizer(DefaultPasses(nil)...).Optimize(root)
	require.NoError(t, err)
	clauses, err := Decompile(optimized)
	require.NoError(t, err)

	var inner []query.Clause
	for _, c := range clauses {
		if sp, ok := c.(*query.SubqueryPattern); ok {
			inner = sp.Query.Where
		}
	}
	require.NotNil(t, inner, "the decorrelated subquery decompiles, got %v", clauses)
	pattern, invocation := -1, -1
	for i, c := range inner {
		switch c.(type) {
		case *query.DataPattern:
			pattern = i
		case *query.RuleInvocation:
			invocation = i
		}
	}
	require.NotEqual(t, -1, pattern, "the inner pattern decompiles, got %v", inner)
	require.NotEqual(t, -1, invocation, "the inner invocation decompiles, got %v", inner)
	require.Less(t, pattern, invocation, "the pattern binding ?k precedes the invocation requiring it, got %v", inner)
}

// TestDecompilerDeclaresPercentByRulesUse: a decompiled subquery whose body
// passes % to a call of its own declares % and passes it, so the phase text
// parses back.
func TestDecompilerDeclaresPercentByRulesUse(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?k ?m
	  :in $ %
	  :where [?x :t/k ?k]
	         [(q [:find ?m :in $ % ?k :where [?k :t/n ?m]] $ % ?k) [[?m] ...]]]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	k, m := datalog.NewSymbol("?k"), datalog.NewSymbol("?m")
	project := &Node{Op: RuleProject, Data: &Project{Symbols: []query.Symbol{k, m}}, Children: []*Node{root}}
	clauses, err := Decompile(project)
	require.NoError(t, err)
	require.Len(t, clauses, 1)
	sp, ok := clauses[0].(*query.SubqueryPattern)
	require.True(t, ok, "the projection decompiles to a subquery, got %s", clauses[0])
	require.Contains(t, sp.Query.In, query.InputSpec(query.RulesInput{}))
	require.True(t, passesRules(sp.Inputs), "the call passes %%, got %v", sp.Inputs)

	phase := &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: k}, query.FindVariable{Symbol: m}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		Where: clauses,
	}
	_, err = parser.ParseQuery(parser.FormatQuery(phase))
	require.NoError(t, err)
}

// TestDecompile_UncorrelatedFallbackStatesItsHeader: a plain or-default whose
// subquery is uncorrelated shares ?count with the find, and both branches
// bind it, so its header is the flat [?count], the global fallback. It lowers
// to a lateral join with defaults and no join variable, decompiles to the
// or-default-join with that header, never a plain or-default, and the phase
// holding it parses back to the same header.
func TestDecompile_UncorrelatedFallbackStatesItsHeader(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?count
	  :where (or-default
	           [(q [:find (count ?c) :in $ :where [?c :t/parent _]] $) [[?count]]]
	           [(ground 0) ?count])]`)
	require.NoError(t, err)

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 1)
	fallback, ok := clauses[0].(*query.OrDefaultJoinClause)
	require.True(t, ok, "the fallback decompiles to an or-default-join, got %T %s", clauses[0], clauses[0])
	require.Equal(t, []query.Symbol{datalog.NewSymbol("?count")}, fallback.OutputVars)
	require.Empty(t, fallback.RequiredVars)

	phase := &query.Query{Find: q.Find, In: q.In, Where: clauses}
	reparsed, err := parser.ParseQuery(parser.FormatQuery(phase))
	require.NoError(t, err)
	require.Len(t, reparsed.Where, 1)
	again, ok := reparsed.Where[0].(*query.OrDefaultJoinClause)
	require.True(t, ok, "the phase parses back to an or-default-join, got %T %s", reparsed.Where[0], reparsed.Where[0])
	require.Equal(t, fallback.OutputVars, again.OutputVars)
	require.Empty(t, again.RequiredVars)
}

// TestCompile_FallbackLoweringKeepsABranchPredicateInsideTheBranch: an
// or-default-join whose first branch is a subquery and a predicate on its
// result falls back per tuple when the predicate fails, so the predicate
// stays inside the first branch and is never applied after the fallback.
func TestCompile_FallbackLoweringKeepsABranchPredicateInsideTheBranch(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?c
	  :where [?e :t/n _]
	         (or-default-join [[?e] ?c]
	           (and [(q [:find (count ?t) :in $ ?s :where [?t :t/root ?s]] $ ?e) [[?c]]]
	                [(> ?c 1)])
	           [(ground 0) ?c])]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)
	var fallback *query.OrDefaultJoinClause
	for _, c := range clauses {
		switch c := c.(type) {
		case *query.Comparison:
			t.Fatalf("the predicate is applied after the fallback, got %v", clauses)
		case *query.OrDefaultJoinClause:
			fallback = c
		}
	}
	require.NotNil(t, fallback, "the fallback decompiles to an or-default-join, got %v", clauses)
	require.Len(t, fallback.Branches, 2)
	inFirst := false
	for _, c := range fallback.Branches[0] {
		if _, ok := c.(*query.Comparison); ok {
			inFirst = true
		}
	}
	require.True(t, inFirst, "the predicate stays in the first branch, got %s", fallback)
}

// TestCompile_FallbackLoweringKeepsABranchGuard: an or-default-join whose
// second branch guards its default with a not-join supplies the default only
// where the guard holds, so the guard stays in the second branch.
func TestCompile_FallbackLoweringKeepsABranchGuard(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?c
	  :where [?e :t/n _]
	         (or-default-join [[?e] ?c]
	           [(q [:find (count ?t) :in $ ?s :where [?t :t/root ?s]] $ ?e) [[?c]]]
	           (and (not-join [?e] [?e :t/deleted true])
	                [(ground 0) ?c]))]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)
	var fallback *query.OrDefaultJoinClause
	for _, c := range clauses {
		if f, ok := c.(*query.OrDefaultJoinClause); ok {
			fallback = f
		}
	}
	require.NotNil(t, fallback, "the fallback decompiles to an or-default-join, got %v", clauses)
	require.Len(t, fallback.Branches, 2)
	guarded := false
	for _, c := range fallback.Branches[1] {
		if _, ok := c.(*query.NotJoinClause); ok {
			guarded = true
		}
	}
	require.True(t, guarded, "the guard stays in the second branch, got %s", fallback)
}

// TestCompile_NotJoinBodyWithASubqueryOverItsHeader: a scope's clause list
// holds a pattern and a count subquery over a variable the scope's header
// binds and the pattern does not. The subquery reads the variable from the
// header, so the lateral join's output is the pattern's symbols and the
// binding's, the variable is the scope's free requirement, and the not-join
// records it as a correlation requirement beside the key the pattern binds.
// The tree analyzes, survives the passes, and decompiles to a subquery still
// connected to the scope through the variable, among its inputs where the
// passes leave it correlated and in its binding where they group it, in
// either written order of the not-join body, inside an or-join branch over
// the required run, and at the top level with the variable bound by :in.
func TestCompile_NotJoinBodyWithASubqueryOverItsHeader(t *testing.T) {
	e, y := datalog.NewSymbol("?e"), datalog.NewSymbol("?y")
	count := `[(q [:find (count ?s) :in $ ?y :where [?s :t/p ?y]] $ ?y) [[?c] ...]]`
	cases := []struct {
		name string
		text string
	}{
		{"pattern then subquery", `[:find ?e :where [?e :t/q ?y] [?y :t/q _] (not-join [?e ?y] [?m :t/q ?e] ` + count + `)]`},
		{"subquery then pattern", `[:find ?e :where [?e :t/q ?y] [?y :t/q _] (not-join [?e ?y] ` + count + ` [?m :t/q ?e])]`},
		{"inside an or-join branch", `[:find ?e :where [?e :t/q ?y] (or-join [[?y] ?e] (and [?m :t/q ?e] ` + count + `) [?e :t/r ?y])]`},
		{"at the top level under an input", `[:find ?e :in $ ?y :where [?e :t/q _] ` + count + `]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tc.text)
			require.NoError(t, err)
			root, err := Compile(mustPrepare(t, q, nil), nil)
			require.NoError(t, err)
			_, err = Analyze(root)
			require.NoError(t, err)
			requireLateralJoinOutputs(t, root)

			optimized, err := NewOptimizer(DefaultPasses(nil)...).Optimize(root)
			require.NoError(t, err)
			clauses, err := Decompile(optimized)
			require.NoError(t, err)
			subqueries := subqueriesIn(clauses)
			require.Len(t, subqueries, 1, "the subquery decompiles as itself, got %v", clauses)
			connected := query.ContainsSymbol(subqueries[0].Binding.BoundVariables(), y)
			for _, input := range subqueries[0].Inputs {
				if v, ok := input.(query.Variable); ok && v.Name == y {
					connected = true
				}
			}
			require.True(t, connected, "the subquery stays connected to the scope through %s, got %s", y, subqueries[0])
		})
	}

	q, err := parser.ParseQuery(cases[0].text)
	require.NoError(t, err)
	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	antiNode := findAlgebraNode(root, RuleAntiJoin)
	require.NotNil(t, antiNode, "the not-join compiles to an anti-join")
	anti := antiNode.Data.(*AntiJoin)
	require.Equal(t, []query.Symbol{e, y}, anti.JoinSymbols)
	require.Equal(t, []query.Symbol{y}, anti.Required, "the body consumes ?y through the subquery and does not produce it")
}

// requireLateralJoinOutputs asserts, for every lateral join under root, that
// its output is its child's symbols and the binding's symbols, or the
// correlation variables and the binding's symbols when it has no child.
func requireLateralJoinOutputs(t *testing.T, root *Node) {
	t.Helper()
	var walk func(n *Node)
	walk = func(n *Node) {
		if lj, ok := n.Data.(*LateralJoin); ok {
			var expected []query.Symbol
			if len(n.Children) == 1 {
				expected = cloneSymbols(n.Children[0].Symbols())
			} else {
				expected = cloneSymbols(lj.CorrelationVars)
			}
			expected = mergeSymbols(expected, lj.Binding.(query.BindingForm).BoundVariables())
			require.Equal(t, expected, lj.Output, "lateral join %s", lj)
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(root)
}

// subqueriesIn collects every subquery pattern in clauses, descending into
// the branches of the header forms.
func subqueriesIn(clauses []query.Clause) []*query.SubqueryPattern {
	var found []*query.SubqueryPattern
	for _, c := range clauses {
		switch v := c.(type) {
		case *query.SubqueryPattern:
			found = append(found, v)
		case *query.NotJoinClause:
			found = append(found, subqueriesIn(v.Clauses)...)
		case *query.OrJoinClause:
			for _, branch := range v.Branches {
				found = append(found, subqueriesIn(branch)...)
			}
		case *query.OrDefaultJoinClause:
			for _, branch := range v.Branches {
				found = append(found, subqueriesIn(branch)...)
			}
		}
	}
	return found
}
