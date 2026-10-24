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
	require.NoError(t, p.planFormScopes(phased.Phases, nil, nil))
	outputs := []query.FindElement{query.FindVariable{Symbol: m}}
	for i, branch := range orJoin.Branches {
		require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Find: outputs, Clauses: branch}), "or-join branch %d", i+1)
	}
	require.True(t, heldPlan(p, Scope{Required: []query.Symbol{e}, Clauses: notJoin.Clauses}), "the not-join's body")
	require.True(t, heldPlan(p, Scope{Required: []query.Symbol{a}, Find: subquery.Query.Find, Clauses: subquery.Query.Where}), "the subquery's inner query")
	_, _, size := p.cache.Stats()
	require.Equal(t, 4, size, "every form's frames and nothing else")
}
