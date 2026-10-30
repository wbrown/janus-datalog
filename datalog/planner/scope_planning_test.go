package planner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A scope's branches are planned at PlanQuery on both planner paths, so a
// branch no order runs is refused there, with one text from "cannot order
// clauses:" on, whether the stall is a not-join's body, an or-join or an
// or-default-join branch, a subquery's inner query, or a form inside a
// branch (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3 and §4.1).
func TestPlanQuery_RefusesABranchNoOrderRunsOnBothPaths(t *testing.T) {
	rules, err := parser.ParseRules(`[[(r ?a ?b) [?a :t/p ?b]]]`)
	require.NoError(t, err)
	// cycle binds each of two locals from the other, so no order runs it.
	const cycle = `[(identity ?z) ?m] [(identity ?m) ?z]`
	cases := []struct{ name, text string }{
		{"a subquery binding its own input inside a not-join body", `[:find ?v :in $ % :where (not-join [?j] [(q [:find (max ?f) :in $ % [?j ?k] :where [(ground [1 2 3]) [?f ...]] (r ?w ?e)] $ % ?j ?w) ?w]) (r ?j ?k) :limit 1]`},
		{"a not-join body", `[:find ?e :where [?e :t/k ?a] (not-join [?e] [?e :t/q ?q] ` + cycle + `)]`},
		{"an or-join branch", `[:find ?e ?x :where [?x :t/r 2.0] [?e :t/k ?a] (or-join [?e] [?e :t/q ?m] (and [(identity ?x) ?e] [(identity ?e) ?x]))]`},
		{"an or-default-join branch", `[:find ?e ?k :where [?e :t/k ?a] (or-default-join [[?e] ?k] (and ` + cycle + ` [(ground 1) ?k]) [(ground 0) ?k])]`},
		{"a subquery's inner query", `[:find ?e ?x :where [?e :t/k ?a] [(q [:find ?x :in $ ?e :where ` + cycle + ` [(identity ?e) ?x]] $ ?e) [[?x]]]]`},
		{"an or-join inside a not-join body", `[:find ?e :where [?e :t/k ?a] (not-join [?e] [?e :t/q ?q] (or-join [?q] (and [(identity ?z) ?q] [(identity ?q) ?z])))]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			var ruleSet *query.RuleSet
			for _, in := range q.In {
				if _, ok := in.(query.RulesInput); ok {
					ruleSet = rules
				}
			}
			prepared, err := query.Prepare(q, ruleSet)
			require.NoError(t, err)
			bindings := query.Bindings{Rules: ruleSet}
			stall := map[bool]string{}
			for _, algebra := range []bool{true, false} {
				p := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra})
				_, err := p.PlanQuery(prepared, bindings, nil)
				require.Error(t, err, "algebra=%v: a branch no order runs is refused at PlanQuery", algebra)
				at := strings.Index(err.Error(), "cannot order clauses:")
				require.GreaterOrEqual(t, at, 0, "algebra=%v: the refusal names the stall: %v", algebra, err)
				stall[algebra] = err.Error()[at:]
			}
			require.Equal(t, stall[true], stall[false], "one text for one stall on both paths")
		})
	}
}

// A rule body the query's invocations reach is planned at PlanQuery on both
// planner paths, so a body no order runs is refused there, with one text from
// "cannot order clauses:" on, whether the invocation is free or bound, at the
// top level or inside a form, and whether the stall is the body itself or a
// form inside it (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3 and
// §4.1).
func TestPlanQuery_RefusesARuleBodyNoOrderRunsOnBothPaths(t *testing.T) {
	const cycle = `[(identity ?z) ?m] [(identity ?m) ?z]`
	const stuck = `[(p ?a ?b) [?a :t/p ?b] ` + cycle + `]`
	cases := []struct{ name, rules, text string }{
		{"a body invoked free", "[" + stuck + "]", `[:find ?x ?y :in $ % :where (p ?x ?y)]`},
		{"a body invoked bound", "[" + stuck + "]", `[:find ?y :in $ % :where [?x :t/k _] (p ?x ?y)]`},
		{"a body invoked inside an or-join branch", "[" + stuck + "]", `[:find ?x ?y :in $ % :where [?x :t/k _] (or-join [[?x] ?y] (p ?x ?y) [?x :t/p ?y])]`},
		{"a body invoked inside a not-join body", "[" + stuck + "]", `[:find ?x :in $ % :where [?x :t/k _] (not-join [?x] (p ?x ?y))]`},
		{"a body invoked inside a subquery's inner query", "[" + stuck + "]", `[:find ?x ?y :in $ % :where [?x :t/k _] [(q [:find ?y :in $ % ?e :where (p ?e ?y)] $ % ?x) [[?y]]]]`},
		{"a body reached through another rule", `[[(r ?a ?b) (p ?a ?b)] ` + stuck + `]`, `[:find ?x ?y :in $ % :where (r ?x ?y)]`},
		{"an or-join branch inside a body", `[[(p ?a ?b) [?a :t/p ?b] (or-join [[?a] ?k] (and ` + cycle + ` [(identity ?a) ?k]) [(identity ?a) ?k])]]`, `[:find ?x ?y :in $ % :where (p ?x ?y)]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rules, err := parser.ParseRules(c.rules)
			require.NoError(t, err)
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			prepared, err := query.Prepare(q, rules)
			require.NoError(t, err)
			bindings := query.Bindings{Rules: rules}
			stall := map[bool]string{}
			for _, algebra := range []bool{true, false} {
				p := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra})
				_, err := p.PlanQuery(prepared, bindings, nil)
				require.Error(t, err, "algebra=%v: a rule body no order runs is refused at PlanQuery", algebra)
				at := strings.Index(err.Error(), "cannot order clauses:")
				require.GreaterOrEqual(t, at, 0, "algebra=%v: the refusal names the stall: %v", algebra, err)
				stall[algebra] = err.Error()[at:]
			}
			require.Equal(t, stall[true], stall[false], "one text for one stall on both paths")
		})
	}
}

// formsQuery holds one form of each kind after a pattern binding ?e and ?a.
const formsQuery = `[:find ?e ?m ?c :where [?e :t/k ?a] (or-join [[?e] ?m] [?e :t/q ?m] [(identity ?e) ?m]) (not-join [?e] [?e :t/n _]) [(q [:find (count ?s) :in $ ?a :where [?s :t/k ?a]] $ ?a) [[?c]]]]`

// heldPlan reports whether the planner's cache holds a plan for the scope,
// under the bindings and options the walk plans a scope under.
func heldPlan(p *ClauseBasedPlanner, scope Scope) bool {
	_, ok := p.cache.GetWithOptions(scope.Query(), query.Bindings{}, p.options)
	return ok
}

// planFormScope frames a form as the executor frames it over the groups in
// hand and plans each frame into the cache: an or-join whose header no group
// binds has each branch planned over the required run alone; one whose
// header a group binds has each branch planned over the required run and the
// carried outputs, in the header's order, after a check over the required
// run alone; a not-join's body is planned over its header; a subquery's
// inner query is planned over its inner symbols with its find; and a clause
// that is no form plans nothing.
func TestPlanFormScope_PlansEachFrameTheExecutorFrames(t *testing.T) {
	e, m, a := datalog.NewSymbol("?e"), datalog.NewSymbol("?m"), datalog.NewSymbol("?a")
	q, err := parser.ParseQuery(formsQuery)
	require.NoError(t, err)
	orJoin, notJoin, subquery := q.Where[1].(*query.OrJoinClause), q.Where[2].(*query.NotJoinClause), q.Where[3].(*query.SubqueryPattern)
	outputs := []query.FindElement{query.FindVariable{Symbol: m}}
	planner := func() *ClauseBasedPlanner {
		return NewClauseBasedPlanner(nil, PlannerOptions{Cache: NewPlanCache(10, 0)})
	}
	t.Run("an or-join whose header no group binds", func(t *testing.T) {
		p := planner()
		require.NoError(t, p.planFormScope(orJoin, nil, nil, query.Bindings{}, nil))
		for i, branch := range orJoin.Branches {
			require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Find: outputs, Clauses: branch}), "branch %d is planned over the required run alone", i+1)
		}
		_, _, size := p.cache.Stats()
		require.Equal(t, 2, size, "one plan per branch")
	})
	t.Run("an or-join whose header a group binds, its output among them", func(t *testing.T) {
		p := planner()
		require.NoError(t, p.planFormScope(orJoin, []query.Symbol{m, e}, nil, query.Bindings{}, nil))
		for i, branch := range orJoin.Branches {
			require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Find: outputs, Clauses: branch}), "branch %d is first planned over the required run alone", i+1)
			require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e, m}, Clauses: branch}), "branch %d is planned over the required run and the carried output, in the header's order", i+1)
		}
		_, _, size := p.cache.Stats()
		require.Equal(t, 4, size, "the check and the frame per branch")
	})
	t.Run("a not-join", func(t *testing.T) {
		p := planner()
		require.NoError(t, p.planFormScope(notJoin, []query.Symbol{e}, nil, query.Bindings{}, nil))
		require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Clauses: notJoin.Clauses}), "the body is planned over its header")
		_, _, size := p.cache.Stats()
		require.Equal(t, 1, size)
	})
	t.Run("a subquery", func(t *testing.T) {
		p := planner()
		require.NoError(t, p.planFormScope(subquery, []query.Symbol{a}, nil, query.Bindings{}, nil))
		require.True(t, heldPlan(p, Scope{Required: []query.Symbol{a}, Find: subquery.Query.Find, Clauses: subquery.Query.Where}), "the inner query is planned over its inner symbols with its find")
		_, _, size := p.cache.Stats()
		require.Equal(t, 1, size)
	})
	t.Run("a pattern", func(t *testing.T) {
		p := planner()
		require.NoError(t, p.planFormScope(q.Where[0], nil, nil, query.Bindings{}, nil))
		_, _, size := p.cache.Stats()
		require.Equal(t, 0, size, "a clause that is no form plans nothing")
	})
}

// planFormScopes walks the phases a planner made, each form framed with the
// symbols the clauses before it provide as the subject's: after the walk the
// cache holds each form's frames, the or-join's branches over ?e with ?m as
// their find, since ?m is bound by nothing before the form, the not-join's
// body over ?e, and the subquery's inner query over ?a.
func TestPlanFormScopes_PlansEveryFormOfThePhases(t *testing.T) {
	e, m, a := datalog.NewSymbol("?e"), datalog.NewSymbol("?m"), datalog.NewSymbol("?a")
	q, err := parser.ParseQuery(formsQuery)
	require.NoError(t, err)
	prepared, err := query.Prepare(q, nil)
	require.NoError(t, err)
	orJoin, notJoin, subquery := prepared.Where[1].(*query.OrJoinClause), prepared.Where[2].(*query.NotJoinClause), prepared.Where[3].(*query.SubqueryPattern)
	phased, err := NewClauseBasedPlanner(nil, PlannerOptions{}).Plan(prepared, query.Bindings{}, nil)
	require.NoError(t, err)
	p := NewClauseBasedPlanner(nil, PlannerOptions{Cache: NewPlanCache(10, 0)})
	_, err = p.planFormScopes(phased.Phases, query.Bindings{}, nil)
	require.NoError(t, err)
	outputs := []query.FindElement{query.FindVariable{Symbol: m}}
	for i, branch := range orJoin.Branches {
		require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Find: outputs, Clauses: branch}), "or-join branch %d", i+1)
	}
	require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Clauses: notJoin.Clauses}), "the not-join's body")
	require.True(t, heldPlan(p, Scope{Required: []query.Symbol{a}, Find: subquery.Query.Find, Clauses: subquery.Query.Where}), "the subquery's inner query")
	_, _, size := p.cache.Stats()
	require.Equal(t, 4, size, "every form's frames and nothing else")
}

// planInvocation plans the bodies an invocation runs into the cache, as the
// executor's evaluation runs them.
func TestPlanInvocation_PlansTheBodiesAnInvocationRuns(t *testing.T) {
	follows := sym("follows")
	rs, err := query.NewRuleSet(followsRules())
	require.NoError(t, err)
	bodyQuery := func(rule *query.Rule, body []query.Clause) *query.Query {
		find := make([]query.FindElement, len(rule.Head))
		for i, head := range rule.Head {
			find[i] = query.FindVariable{Symbol: head}
		}
		return &query.Query{Find: find, In: []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}}, Where: body}
	}
	heldBody := func(p *ClauseBasedPlanner, r *query.RuleSet, rule *query.Rule, body []query.Clause) bool {
		_, ok := p.cache.GetWithOptions(bodyQuery(rule, body), query.Bindings{Rules: r, Held: query.HeldByFixpoint}, p.options)
		return ok
	}
	planner := func() *ClauseBasedPlanner {
		return NewClauseBasedPlanner(nil, PlannerOptions{Cache: NewPlanCache(10, 0)})
	}
	t.Run("a free invocation under a query", func(t *testing.T) {
		p := planner()
		_, err := p.planInvocation(invoke("follows", "?x", "?y"), nil, nil, query.Bindings{Rules: rs}, nil)
		require.NoError(t, err)
		base, recursive := rs.Rules(follows)[0], rs.Rules(follows)[1]
		require.True(t, heldBody(p, rs, base, base.Body), "the base rule's plain body")
		require.True(t, heldBody(p, rs, recursive, markDelta(recursive.Body, map[query.Symbol]bool{follows: true}, 0)), "the recursive rule's delta copy")
		_, _, size := p.cache.Stats()
		require.Equal(t, 2, size, "the two bodies and nothing else")
	})
	t.Run("a held predicate under a body a fixpoint runs", func(t *testing.T) {
		p := planner()
		planned, err := p.planInvocation(invoke("follows", "?x", "?y"), nil, nil, query.Bindings{Rules: rs, Held: query.HeldByFixpoint}, nil)
		require.NoError(t, err)
		require.Nil(t, planned, "a predicate the fixpoint holds evaluates nothing at its clause")
		_, _, size := p.cache.Stats()
		require.Equal(t, 0, size, "a predicate the fixpoint holds plans nothing")
	})
	t.Run("a required predicate under a body a fixpoint runs", func(t *testing.T) {
		reaching, err := parser.ParseRules(`[[(near [?p] ?q) [?p :page/next ?q]]]`)
		require.NoError(t, err)
		p := planner()
		_, err = p.planInvocation(invoke("near", "?x", "?y"), []query.Symbol{sym("?x")}, nil, query.Bindings{Rules: reaching, Held: query.HeldByFixpoint}, nil)
		require.NoError(t, err)
		magic, err := EvaluatedSet(planner(), reaching, sym("near"), []bool{true, false}, nil)
		require.NoError(t, err)
		adorned := magic.Rules.Rules(magic.Predicate)[0]
		require.True(t, heldBody(p, magic.Rules, adorned, adorned.Body), "the adorned body of the predicate derived at its clause")
	})
	t.Run("any invocation under the rewrite's reading", func(t *testing.T) {
		p := planner()
		planned, err := p.planInvocation(invoke("follows", "?x", "?y"), nil, nil, query.Bindings{Rules: rs, Held: query.HeldEvery}, nil)
		require.NoError(t, err)
		require.Nil(t, planned, "the rewrite decides each site itself")
		_, _, size := p.cache.Stats()
		require.Equal(t, 0, size, "the rewrite decides each site itself")
	})
}

// planFormScopes plans the window scope of every windowable clause, the
// frame a run reads when the clause's group streams
// (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md T1, T5): a pattern after a
// pattern binding one of its symbols is planned over that symbol with its
// other symbol as the find, an invocation after such a pattern likewise, and
// a window scope's own clause, standing after scope/required alone, is
// planned no window scope of its own.
func TestPlanFormScopes_PlansTheWindowScopeOfAWindowableClause(t *testing.T) {
	e := datalog.NewSymbol("?e")
	planner := func() *ClauseBasedPlanner {
		return NewClauseBasedPlanner(nil, PlannerOptions{Cache: NewPlanCache(10, 0)})
	}
	phasesOf := func(t *testing.T, text string, rs *query.RuleSet) []RealizedPhase {
		q, err := parser.ParseQuery(text)
		require.NoError(t, err)
		prepared, err := query.Prepare(q, rs)
		require.NoError(t, err)
		phased, err := NewClauseBasedPlanner(nil, PlannerOptions{}).Plan(prepared, query.Bindings{Rules: rs}, nil)
		require.NoError(t, err)
		return phased.Phases
	}
	t.Run("a pattern after a pattern binding one of its symbols", func(t *testing.T) {
		phases := phasesOf(t, `[:find ?e ?b :where [?e :t/k ?a] [?e :t/q ?b]]`, nil)
		require.Len(t, phases, 1)
		second := phases[0].Query.Where[1].(*query.DataPattern)
		var other query.Symbol
		for _, sym := range second.Symbols() {
			if sym != e {
				other = sym
			}
		}
		p := planner()
		_, err := p.planFormScopes(phases, query.Bindings{}, nil)
		require.NoError(t, err)
		window := Scope{Required: []query.Symbol{e}, Find: []query.FindElement{query.FindVariable{Symbol: other}}, Clauses: []query.Clause{second}}
		require.True(t, heldPlan(p, window), "the second pattern's window scope over ?e")
		_, _, size := p.cache.Stats()
		require.Equal(t, 1, size, "the second pattern's window scope and nothing else")
	})
	t.Run("an invocation after a pattern binding its argument", func(t *testing.T) {
		rs, err := parser.ParseRules(`[[(r ?a ?b) [?a :t/p ?b]]]`)
		require.NoError(t, err)
		y := datalog.NewSymbol("?y")
		phases := phasesOf(t, `[:find ?e ?y :in $ % :where [?e :t/k ?a] (r ?e ?y)]`, rs)
		require.Len(t, phases, 1)
		inv := phases[0].Query.Where[1].(*query.RuleInvocation)
		p := planner()
		_, err = p.planFormScopes(phases, query.Bindings{Rules: rs}, nil)
		require.NoError(t, err)
		window := Scope{Required: []query.Symbol{e}, Find: []query.FindElement{query.FindVariable{Symbol: y}}, Clauses: []query.Clause{inv}}
		_, held := p.cache.GetWithOptions(window.Query(), query.Bindings{Rules: rs}, p.options)
		require.True(t, held, "the invocation's window scope over ?e")
	})
	t.Run("a clause sharing no bound symbol", func(t *testing.T) {
		phases := phasesOf(t, `[:find ?e ?b :where [?e :t/k ?a] [?x :t/q ?b]]`, nil)
		p := planner()
		_, err := p.planFormScopes(phases, query.Bindings{}, nil)
		require.NoError(t, err)
		_, _, size := p.cache.Stats()
		require.Equal(t, 0, size, "a clause sharing no bound symbol never windows and is planned no window scope")
	})
	t.Run("a window scope's own clause", func(t *testing.T) {
		phases := phasesOf(t, `[:find ?e ?b :where [?e :t/k ?a] [?e :t/q ?b]]`, nil)
		second := phases[0].Query.Where[1].(*query.DataPattern)
		var other query.Symbol
		for _, sym := range second.Symbols() {
			if sym != e {
				other = sym
			}
		}
		window := Scope{Required: []query.Symbol{e}, Find: []query.FindElement{query.FindVariable{Symbol: other}}, Clauses: []query.Clause{second}}
		p := planner()
		_, err := p.PlanScope(window, query.Bindings{}, nil)
		require.NoError(t, err)
		_, _, size := p.cache.Stats()
		require.Equal(t, 1, size, "the window scope itself and no window scope of its clause")
	})
}
