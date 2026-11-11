package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// Every phase the planner emits for a query over a rule set parses back to
// itself on every leg of the matrix, and so does the query the algebra engine
// rewrites it to
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6): a subquery passing
// % whose body invokes nothing, one nesting a subquery that invokes, one
// nesting a subquery passing % that invokes nothing, an aggregating and a
// non-aggregating subquery invoking the closure, an expression over an
// invocation's output, an aggregate over two invocations, two invocations in
// a chain, and the same subqueries spelled without %.
func TestPlan_PhasesOfRuleQueriesParseBackToThemselves(t *testing.T) {
	const rules = `[[(reach ?a ?b) [?a :edge ?b]] [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]
	                [(lbl ?x ?l) [?x :type ?t] [(str ?t) ?l]]]`
	queries := []struct {
		name, text string
		rules      bool
	}{
		{"a subquery passing % whose body invokes nothing", `[:find ?x ?n :in $ % :where [?x :edge _] [(q [:find (count ?y) :in $ % ?x :where [?x :edge ?y]] $ % ?x) [[?n]]]]`, true},
		{"a subquery passing % nesting one that invokes", `[:find ?x ?n :in $ % :where [?x :edge _] [(q [:find (count ?z) :in $ % ?x :where [?x :edge ?y] [(q [:find ?z :in $ % ?y :where (reach ?y ?z)] $ % ?y) [[?z] ...]]] $ % ?x) [[?n]]]]`, true},
		{"a subquery passing % nesting one passing % that invokes nothing", `[:find ?x ?n :in $ % :where [?x :edge _] [(q [:find (count ?z) :in $ % ?x :where [?x :edge ?y] [(q [:find ?z :in $ % ?y :where [?y :edge ?z]] $ % ?y) [[?z] ...]]] $ % ?x) [[?n]]]]`, true},
		{"an aggregating subquery invoking the closure", `[:find ?x ?n :in $ % :where [?x :edge _] [(q [:find (count ?y) :in $ % ?x :where [?x :edge _] (reach ?x ?y)] $ % ?x) [[?n]]]]`, true},
		{"a non-aggregating subquery invoking the closure", `[:find ?x ?y :in $ % :where [?x :edge _] [(q [:find ?y :in $ % ?x :where [?x :edge _] (reach ?x ?y)] $ % ?x) [[?y] ...]]]`, true},
		{"a pattern over an invocation's output", `[:find ?y :in $ % :where [?x :type :node] (reach ?x ?y) [?y :type ?t]]`, true},
		{"an aggregate over two invocations", `[:find ?l (count ?y) :in $ % :where (reach ?x ?y) (lbl ?y ?l) [?x :type :node]]`, true},
		{"two invocations in a chain", `[:find ?x ?w :in $ % :where [?x :type :node] (reach ?x ?y) (reach ?y ?w)]`, true},
		{"a subquery passing % under a pattern over its output", `[:find ?y :in $ % :where [?x :type :node] [(q [:find ?y :in $ % ?x :where [?x :edge ?y]] $ % ?x) [[?y] ...]] [?y :type ?t]]`, true},
		{"the same subquery spelled without %", `[:find ?y :in $ :where [?x :type :node] [(q [:find ?y :in $ ?x :where [?x :edge ?y]] $ ?x) [[?y] ...]] [?y :type ?t]]`, false},
		{"a nested subquery passing % through both levels", `[:find ?x ?n :in $ % :where [?x :edge _] [(q [:find (count ?y) :in $ % ?x :where [?x :edge _] [(q [:find ?y :in $ % ?x :where [?x :edge ?y]] $ % ?x) [[?y] ...]]] $ % ?x) [[?n]]]]`, true},
		{"the same nested subquery spelled without %", `[:find ?x ?n :in $ :where [?x :edge _] [(q [:find (count ?y) :in $ ?x :where [?x :edge _] [(q [:find ?y :in $ ?x :where [?x :edge ?y]] $ ?x) [[?y] ...]]] $ ?x) [[?n]]]]`, false},
	}
	spec := chain(5)
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeGraph(t, d, spec)
			rs := mustRuleSet(t, rules)
			for _, q := range queries {
				t.Run(q.name, func(t *testing.T) {
					var inputs []interface{}
					if q.rules {
						inputs = append(inputs, rs)
					}
					_, err := executor.CollectTuples(d.Query(q.text, inputs...))
					require.NoError(t, err)
					plan, err := d.Explain(q.text, inputs...)
					require.NoError(t, err)
					for i, phase := range plan.Phases {
						rendered := phase.Query.String()
						again, err := parser.ParseQuery(rendered)
						require.NoError(t, err, "phase %d parses back:\n%s", i, rendered)
						require.Equal(t, rendered, again.String(), "phase %d renders the same again", i)
					}
					explanation, err := d.ExplainAlgebra(q.text, inputs...)
					require.NoError(t, err)
					if explanation.Rewritten != nil {
						rendered := explanation.Rewritten.String()
						again, err := parser.ParseQuery(rendered)
						require.NoError(t, err, "the rewritten query parses back:\n%s", rendered)
						require.Equal(t, rendered, again.String(), "the rewritten query renders the same again")
					}
				})
			}
		})
	}
}
