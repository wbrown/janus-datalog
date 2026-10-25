package storage

// Every scope a query runs under its rule set is planned at PlanQuery, the
// bodies its invocations reach included, so Query, Explain and ExplainAlgebra
// refuse the same text with the one stall text and accept the same text, and
// executing a query Explain planned plans no body.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// pathGraph is the fixture's entities: a :edge b; b :edge c.
type pathGraph struct {
	a, b, c namedEntity
}

func newPathGraph() pathGraph {
	return pathGraph{a: named("a"), b: named("b"), c: named("c")}
}

func (g pathGraph) facts() []fact {
	return []fact{{g.a, ":edge", g.b}, {g.b, ":edge", g.c}}
}

// cycleClauses bind each of two locals from the other, so no order runs
// them.
const cycleClauses = `[(identity ?z) ?m] [(identity ?m) ?z]`

// stuckRule is a rule whose body no order runs.
const stuckRule = `[(p ?a ?b) [?a :edge ?b] ` + cycleClauses + `]`

// ruleQuery is one query over a rule set with its inputs.
type ruleQuery struct {
	name, rules, query string
	inputs             []interface{}
}

// runToEnd runs the query and pulls its relation to the end, returning the
// first error, planning's or evaluation's, since a rule body an invocation
// reaches is evaluated as the relation is pulled.
func runToEnd(d *Database, text string, inputs ...interface{}) error {
	rel, err := d.Query(text, inputs...)
	if err != nil {
		return err
	}
	return executor.ForEach(rel, func(executor.Tuple) error { return nil })
}

// Query, Explain and ExplainAlgebra refuse a rule body no order runs, at
// every place a body can be reached from the query: invoked at the top
// level, free or bound, inside a top-level form or subquery, through another
// rule, or with the stall inside a form or inner query of the body itself.
func TestExplain_RefusesARuleBodyAsQueryDoes(t *testing.T) {
	cases := []ruleQuery{
		{"a rule body", "[" + stuckRule + "]",
			`[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil},
		{"a not-join in a rule body", `[[(p ?a ?b) [?a :edge ?b] (not-join [?a] [?a :edge ?q] ` + cycleClauses + `)]]`,
			`[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil},
		{"an or-join branch in a rule body", `[[(p ?a ?b) [?a :edge ?b] (or-join [[?a] ?k] (and ` + cycleClauses + ` [(identity ?a) ?k]) [(identity ?a) ?k])]]`,
			`[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil},
		{"an or-default-join branch in a rule body", `[[(p ?a ?b) [?a :edge ?b] (or-default-join [[?a] ?k] (and ` + cycleClauses + ` [(identity ?a) ?k]) [(ground 0) ?k])]]`,
			`[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil},
		{"a subquery's inner query in a rule body", `[[(p ?a ?b) [?a :edge ?b] [(q [:find ?y :in $ ?e :where ` + cycleClauses + ` [(identity ?e) ?y]] $ ?a) [[?y]]]]]`,
			`[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil},
		{"a rule body invoked inside a top-level subquery", "[" + stuckRule + "]",
			`[:find ?x ?y :in $ % :where [?x :edge _] [(q [:find ?y :in $ % ?e :where (p ?e ?y)] $ % ?x) [[?y]]]]`, nil},
		{"a rule body invoked bound", "[" + stuckRule + "]",
			`[:find ?y :in $ % :where [?x :edge _] (p ?x ?y)]`, nil},
		{"a rule body invoked inside a top-level not-join", "[" + stuckRule + "]",
			`[:find ?x :in $ % :where [?x :edge _] (not-join [?x] (p ?x ?y))]`, nil},
		{"a rule body invoked inside a top-level or-join branch", "[" + stuckRule + "]",
			`[:find ?x ?y :in $ % :where [?x :edge _] (or-join [[?x] ?y] (p ?x ?y) [?x :edge ?y])]`, nil},
		{"a rule body invoked inside a top-level or-default-join branch", "[" + stuckRule + "]",
			`[:find ?x ?y :in $ % :where [?x :edge _] (or-default-join [[?x] ?y] (p ?x ?y) [(ground 0) ?y])]`, nil},
		{"a body reached through another rule", `[[(r ?a ?b) (p ?a ?b)] ` + stuckRule + `]`,
			`[:find ?x ?y :in $ % :where (r ?x ?y)]`, nil},
		{"a body inside a branch of a body", `[[(r ?a ?b) [?a :edge ?m] (or-join [[?m] ?b] (p ?m ?b) [?m :edge ?b])] ` + stuckRule + `]`,
			`[:find ?x ?y :in $ % :where (r ?x ?y)]`, nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, newPathGraph().facts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					inputs := append([]interface{}{mustRuleSet(t, c.rules)}, c.inputs...)
					queryErr := runToEnd(d, c.query, inputs...)
					require.Error(t, queryErr, "Query refuses a rule body no order runs")
					_, explainErr := d.Explain(c.query, inputs...)
					require.Error(t, explainErr, "Explain refuses a rule body no order runs; Query: %v", queryErr)
					_, algebraErr := d.ExplainAlgebra(c.query, inputs...)
					require.Error(t, algebraErr, "ExplainAlgebra refuses a rule body no order runs; Query: %v", queryErr)
					require.Equal(t, stallText(t, queryErr), stallText(t, explainErr), "one text for one stall at Explain and at Query")
					require.Equal(t, stallText(t, queryErr), stallText(t, algebraErr), "one text for one stall at ExplainAlgebra and at Query")
				})
			}
		})
	}
}

// A plan the cache holds for a text under one rule set is not served to the
// same text under another: a second set whose body no order runs is refused
// by Query, Explain and ExplainAlgebra with the one stall text whatever warmed
// the cache, the invocation free, bound by a pattern, inside an or-join branch
// or inside a subquery. The cold case is the control, and the orderable set
// answers the text after the second set is refused.
func TestExplain_RefusesARuleBodyWhateverThePlanCacheHolds(t *testing.T) {
	const orderable = `[[(p ?a ?b) [?a :edge ?b]]]`
	const stuck = "[" + stuckRule + "]"
	g := newPathGraph()
	shapes := []struct {
		name, query string
		want        []string
	}{
		{"a free invocation", `[:find ?x ?y :in $ % :where (p ?x ?y)]`, []string{"a b", "b c"}},
		{"an invocation a pattern binds", `[:find ?x ?y :in $ % :where [?x :edge _] (p ?x ?y)]`, []string{"a b", "b c"}},
		{"an invocation inside an or-join branch", `[:find ?x ?y :in $ % :where [?x :edge _] (or-join [[?x] ?y] (p ?x ?y) [?x :edge ?y])]`, []string{"a b", "b c"}},
		{"an invocation inside a subquery", `[:find ?x ?y :in $ % :where [?x :edge _] [(q [:find ?y :in $ % ?e :where (p ?e ?y)] $ % ?x) [[?y]]]]`, []string{"a b", "b c"}},
	}
	warmers := []struct {
		name string
		warm func(d *Database, text string, rs *query.RuleSet) error
	}{
		{"cold", func(*Database, string, *query.RuleSet) error { return nil }},
		{"warmed by Query", func(d *Database, text string, rs *query.RuleSet) error { return runToEnd(d, text, rs) }},
		{"warmed by Explain", func(d *Database, text string, rs *query.RuleSet) error {
			_, err := d.Explain(text, rs)
			return err
		}},
		{"warmed by ExplainAlgebra", func(d *Database, text string, rs *query.RuleSet) error {
			_, err := d.ExplainAlgebra(text, rs)
			return err
		}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, g.facts())
			for _, s := range shapes {
				for _, w := range warmers {
					t.Run(s.name+", "+w.name, func(t *testing.T) {
						d.ClearPlanCache()
						first := mustRuleSet(t, orderable)
						require.NoError(t, w.warm(d, s.query, first))
						second := mustRuleSet(t, stuck)
						queryErr := runToEnd(d, s.query, second)
						require.Error(t, queryErr, "Query refuses the second set's body")
						_, explainErr := d.Explain(s.query, second)
						require.Error(t, explainErr, "Explain refuses the second set's body; Query: %v", queryErr)
						_, algebraErr := d.ExplainAlgebra(s.query, second)
						require.Error(t, algebraErr, "ExplainAlgebra refuses the second set's body; Query: %v", queryErr)
						require.Equal(t, stallText(t, queryErr), stallText(t, explainErr), "one text for one stall at Explain and at Query")
						require.Equal(t, stallText(t, queryErr), stallText(t, algebraErr), "one text for one stall at ExplainAlgebra and at Query")
						rel, err := d.Query(s.query, first)
						require.NoError(t, err, "the orderable set answers the text")
						require.Equal(t, s.want, renderedTuples(t, rel, names))
					})
				}
			}
		})
	}
}

// A body orderable under a bound invocation alone, the head's required
// position supplying the position its required predicate needs, is accepted
// by Query, Explain and ExplainAlgebra alike when the invocation binds it.
// The free invocation of the same rule is a required run no order binds,
// refused by all three as the planner's stall with the one stall text
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3).
func TestExplain_AcceptsARuleBodyAsQueryDoes(t *testing.T) {
	const rules = `[[(nb [?x] ?y) [?x :edge ?y]] [(p [?a] ?b) (nb ?a ?b)]]`
	const bound = `[:find ?y :in $ % ?x :where (p ?x ?y)]`
	const free = `[:find ?x ?y :in $ % :where (p ?x ?y)]`
	g := newPathGraph()
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, g.facts())
			rs := mustRuleSet(t, rules)
			rel, err := d.Query(bound, rs, g.a.id)
			require.NoError(t, err, "Query answers the bound invocation")
			require.Equal(t, []string{"b"}, renderedTuples(t, rel, names))
			_, err = d.Explain(bound, rs, g.a.id)
			require.NoError(t, err, "Explain plans what Query answers")
			_, err = d.ExplainAlgebra(bound, rs, g.a.id)
			require.NoError(t, err, "ExplainAlgebra plans what Query answers")
			var stall *query.NoOrderError
			queryErr := runToEnd(d, free, rs)
			require.ErrorAs(t, queryErr, &stall, "Query refuses the free invocation at the planner's stall")
			_, explainErr := d.Explain(free, rs)
			require.ErrorAs(t, explainErr, &stall, "Explain refuses the free invocation at the planner's stall; Query: %v", queryErr)
			_, algebraErr := d.ExplainAlgebra(free, rs)
			require.ErrorAs(t, algebraErr, &stall, "ExplainAlgebra refuses the free invocation at the planner's stall; Query: %v", queryErr)
			require.Equal(t, stallText(t, queryErr), stallText(t, explainErr), "one text for one stall at Explain and at Query")
			require.Equal(t, stallText(t, queryErr), stallText(t, algebraErr), "one text for one stall at ExplainAlgebra and at Query")
		})
	}
}

// Executing a query Explain has planned plans no body: the plan cache's
// misses and size are what the planner left them, under a free invocation, a
// bound one with its adorned bodies, a recursive predicate with its delta
// copies, a lower seed-restricted predicate with its lower copies, an
// invocation inside an or-join branch and inside a subquery, and a required
// predicate the executor derives at its clause inside a body. An invocation
// bound at the top level is bound by a constant, since a plan is served
// under the symbols bound when it is made
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 10), so a scalar
// input would have Query plan the query itself again under its symbol.
func TestExplain_ExecutingAnExplainedQueryPlansNoBody(t *testing.T) {
	const closure = `[(p ?a ?b) [?a :edge ?b]] [(p ?a ?c) [?a :edge ?b] (p ?b ?c)]`
	const required = `[(nb [?x] ?y) [?x :edge ?y]] [(q ?x ?z) [?x :edge ?z]] [(q ?x ?z) [?x :edge ?y] (q ?y ?w) (not (nb ?w ?u)) [?w :edge ?z]]`
	g := newPathGraph()
	cases := []ruleQuery{
		{"a free invocation", "[" + closure + "]", `[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil},
		{"a bound invocation", "[" + closure + "]", `[:find ?y :in $ % :where (p #id "a" ?y)]`, nil},
		{"a lower seed-restricted predicate", "[" + closure + ` [(r ?a ?c) (p ?a ?b) [?b :edge ?c]]]`, `[:find ?y :in $ % :where (r #id "a" ?y)]`, nil},
		{"an invocation inside an or-join branch", "[" + closure + "]", `[:find ?x ?y :in $ % :where [?x :edge _] (or-join [[?x] ?y] (p ?x ?y) [?x :edge ?y])]`, nil},
		{"an invocation inside a subquery", "[" + closure + "]", `[:find ?x ?y :in $ % :where [?x :edge _] [(q [:find ?y :in $ % ?e :where (p ?e ?y)] $ % ?x) [[?y]]]]`, nil},
		{"a required predicate derived at its clause inside a body", "[" + required + "]", `[:find ?z :in $ % :where (q #id "a" ?z)]`, nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, g.facts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					inputs := append([]interface{}{mustRuleSet(t, c.rules)}, c.inputs...)
					d.ClearPlanCache()
					_, err := d.Explain(c.query, inputs...)
					require.NoError(t, err)
					_, plannedMisses, plannedSize := d.PlanCache().Stats()
					require.NoError(t, runToEnd(d, c.query, inputs...), "the query runs to its end")
					_, misses, size := d.PlanCache().Stats()
					require.Equal(t, plannedMisses, misses, "executing a planned query plans nothing the planner did not")
					require.Equal(t, plannedSize, size, "executing a planned query adds no plan to the cache")
				})
			}
		})
	}
}
