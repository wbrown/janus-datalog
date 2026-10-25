package storage

// A subquery's inner :in symbol is found in the subject by the enclosing
// variable its call passes at that place, and a constant argument's inner
// symbol by nothing: it takes the call's constant, whatever symbols the
// enclosing scope binds. The planner and the executor frame one call alike,
// so executing a query Explain planned plans nothing.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// constantArgumentFacts: a :p 1, :name "ay"; c :q 5, :r a, :name "cee"; d
// and e :q 1, :r a, :name "ay". A call passing 5 at an inner symbol read at
// :q finds c alone, and one passing 1 finds d and e.
func constantArgumentFacts() []fact {
	a, c, d, e := named("a"), named("c"), named("d"), named("e")
	return []fact{
		{a, ":p", int64(1)}, {a, ":name", "ay"},
		{c, ":q", int64(5)}, {c, ":r", a}, {c, ":name", "cee"},
		{d, ":q", int64(1)}, {d, ":r", a}, {d, ":name", "ay"},
		{e, ":q", int64(1)}, {e, ":r", a}, {e, ":name", "ay"},
	}
}

// queryCase is one query over a fixture, its rule set and inputs, and the
// tuples it answers, rendered and sorted.
type queryCase struct {
	name   string
	rules  string
	query  string
	inputs []interface{}
	want   []string
}

// runQueryCases commits the facts in both optimizer modes, runs each case's
// query over them with its rule set and inputs, and asserts its rendered
// tuples.
func runQueryCases(t *testing.T, facts []fact, cases []queryCase) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					var inputs []interface{}
					if c.rules != "" {
						inputs = append(inputs, mustRuleSet(t, c.rules))
					}
					inputs = append(inputs, c.inputs...)
					rel, err := d.Query(c.query, inputs...)
					require.NoError(t, err, c.query)
					got := renderedTuples(t, rel, names)
					if len(c.want) == 0 {
						require.Empty(t, got, c.query)
						return
					}
					require.Equal(t, c.want, got, c.query)
				})
			}
		})
	}
}

// The call passes 5 at inner ?v and ?x at inner ?w, so the inner query finds
// the ?y with :q 5 and :r a, which is c, whatever the enclosing scope binds
// under the name ?v.
func TestSubqueryConstantArgument_TheInnerSymbolReadsTheConstant(t *testing.T) {
	const inner = `[:find ?y :in $ ?v ?w :where [?y :q ?v] [?y :r ?w]]`
	cases := []queryCase{
		{"the subject binds no symbol of the inner symbol's name", "",
			`[:find ?x ?y :where [?x :p ?u] [(q ` + inner + ` $ 5 ?x) [[?y]]]]`, nil, []string{"a c"}},
		{"the subject carries a symbol of the inner symbol's name", "",
			`[:find ?x ?y :where [?x :p ?v] [(q ` + inner + ` $ 5 ?x) [[?y]]]]`, nil, []string{"a c"}},
		{"the subject carries it from an :in scalar", "",
			`[:find ?x ?y :in $ ?v :where [?x :p ?v] [(q ` + inner + ` $ 5 ?x) [[?y]]]]`, []interface{}{int64(1)}, []string{"a c"}},
		{"an aggregating inner query", "",
			`[:find ?x ?n :where [?x :p ?v] [(q [:find (count ?y) :in $ ?v ?w :where [?y :q ?v] [?y :r ?w]] $ 5 ?x) [[?n]]]]`, nil, []string{"a 1"}},
		{"a string constant at a value position", "",
			`[:find ?x ?y :where [?x :p ?v] [?x :name ?s] [(q [:find ?y :in $ ?s ?w :where [?y :name ?s] [?y :r ?w]] $ "cee" ?x) [[?y]]]]`, nil, []string{"a c"}},
		{"a nested call passing a constant at an inner symbol the outer inner query binds", "",
			`[:find ?x ?y :where [?x :p _] [(q [:find ?y :in $ ?w :where [?w :p ?v] [(q [:find ?y :in $ ?v ?k :where [?y :q ?v] [?y :r ?k]] $ 5 ?w) [[?y]]]] $ ?x) [[?y]]]]`, nil, []string{"a c"}},
		{"the constant's inner symbol named as the enclosing variable passed at another position", "",
			`[:find ?x ?y :where [?x :p ?w] [(q [:find ?y :in $ ?w ?k :where [?y :r ?w] [?y :q ?k]] $ #id "a" ?w) [[?y]]]]`, nil, []string{"a d", "a e"}},
		{"the constant's inner symbol also a find symbol the binding carries", "",
			`[:find ?x ?y :where [?x :p ?v] [(q [:find ?v ?y :in $ ?v ?w :where [?y :q ?v] [?y :r ?w]] $ 5 ?x) [[?v ?y]]]]`, nil, nil},
	}
	runQueryCases(t, constantArgumentFacts(), cases)
}

// The same call under every form that can enclose it with a header or a run
// carrying ?v: an or-join branch, a not-join body, a fallback branch, a rule
// body and a relation input.
func TestSubqueryConstantArgument_UnderEveryEnclosingForm(t *testing.T) {
	const call = `[(q [:find ?y :in $ ?v ?w :where [?y :q ?v] [?y :r ?w]] $ 5 ?x) [[?y]]]`
	cases := []queryCase{
		{"an or-join branch whose header carries the symbol", "",
			`[:find ?x ?y :where [?x :p ?v] (or-join [[?x ?v] ?y] (and [?x :p ?v] ` + call + `) (and [?x :p ?v] [?x :none ?y]))]`, nil, []string{"a c"}},
		{"a not-join whose header carries the symbol", "",
			`[:find ?x :where [?x :p ?v] (not-join [?x ?v] [?x :p ?v] ` + call + ` [?y :name "ay"])]`, nil, []string{"a"}},
		{"a fallback branch whose required run carries the symbol", "",
			`[:find ?x ?y :where [?x :p ?v] (or-default-join [[?x ?v] ?y] (and [?x :p ?v] ` + call + `) (and [?x :p ?v] [(ground "none") ?y]))]`, nil, []string{"a c"}},
		{"a rule body", `[[(r ?x ?y) [?x :p ?v] ` + call + `]]`,
			`[:find ?x ?y :in $ % :where (r ?x ?y)]`, nil, []string{"a c"}},
		{"a relation input carrying the symbol", "",
			`[:find ?x ?y :in $ [[?x ?v]] :where ` + call + `]`, []interface{}{[][]interface{}{{datalog.NewIdentity("a"), int64(1)}}}, []string{"a c"}},
	}
	runQueryCases(t, constantArgumentFacts(), cases)
}

// Executing a query Explain has planned plans nothing: the plan cache's
// misses and size are what the planner left them, with the constant
// argument's inner symbol shared by no outer symbol and with it carried by
// the subject.
func TestSubqueryConstantArgument_ExecutingAnExplainedQueryPlansNothing(t *testing.T) {
	queries := []struct{ name, text string }{
		{"the inner symbol shared by no outer symbol",
			`[:find ?x ?y :where [?x :p ?u] [(q [:find ?y :in $ ?v ?w :where [?y :q 5]] $ 5 ?x) [[?y] ...]]]`},
		{"the inner symbol carried by the subject",
			`[:find ?x ?y :where [?x :p ?v] [(q [:find ?y :in $ ?v ?w :where [?y :q 5]] $ 5 ?x) [[?y] ...]]]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, constantArgumentFacts())
			for _, q := range queries {
				t.Run(q.name, func(t *testing.T) {
					d.ClearPlanCache()
					_, err := d.Explain(q.text)
					require.NoError(t, err)
					_, plannedMisses, plannedSize := d.PlanCache().Stats()
					_, err = d.Query(q.text)
					require.NoError(t, err)
					_, misses, size := d.PlanCache().Stats()
					require.Equal(t, plannedMisses, misses, "executing a planned query plans nothing the planner did not")
					require.Equal(t, plannedSize, size, "executing a planned query adds no plan to the cache")
				})
			}
		})
	}
}
