package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// orderingRules declares follows, a closure over :page/next, and path, two
// steps of :page/next, for the scheduling tests below.
const orderingRules = `[[(follows ?x ?y) [?x :page/next ?y]]
                        [(follows ?x ?z) [?x :page/next ?y] (follows ?y ?z)]
                        [(path ?x ?y ?z) [?x :page/next ?y] [?y :page/next ?z]]]`

// A data pattern or an invocation without visible selectivity, a constant
// beyond the attribute, an available variable or argument, or a predicate the
// seed restricts, is selected only when no other clause is selectable: a scan
// of a whole extent follows a pattern seeking on an available variable, an
// invocation bound by an available argument, and an expression over available
// inputs, whatever the written order (REWRITE_ORDER_PROOF.md T3).
func TestCreatePhasesGreedy_AScanOfAWholeExtentIsSelectedLast(t *testing.T) {
	a := datalog.NewSymbol("?a")
	rules, err := parser.ParseRules(orderingRules)
	require.NoError(t, err)
	cases := []struct {
		name, text string
		scan, other int
	}{
		{"a pattern seeking on an available variable", `[:find ?b ?v :in $ ?a :where [?e :item/val ?v ?tx] [?a :item/next ?b]]`, 0, 1},
		{"an invocation bound by an available argument", `[:find ?c ?w :in $ % ?a :where [?x :item/val ?w] (follows ?a ?c)]`, 0, 1},
		{"an expression over available inputs", `[:find ?e ?w :in $ ?a :where [?e :item/val ?x] [(+ ?a 1) ?w]]`, 0, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			phases, err := createPhasesGreedy(q.Where, physicalFindSymbols(q.Find), map[query.Symbol]bool{a: true}, query.Bindings{Rules: rules})
			require.NoError(t, err)
			assertScheduledAfter(t, phases, q.Where[c.scan], q.Where[c.other],
				"the scan of a whole extent is selected after the clause with visible selectivity")
		})
	}
}

// An invocation with no available argument waits on any selectable provider
// of one of its arguments, and an invocation with an available argument waits
// on a provider with visible selectivity and on no scan of a whole extent
// (REWRITE_ORDER_PROOF.md §6).
func TestCreatePhasesGreedy_AnInvocationWaitsOnAProviderByItsSelectivity(t *testing.T) {
	a := datalog.NewSymbol("?a")
	rules, err := parser.ParseRules(orderingRules)
	require.NoError(t, err)
	cases := []struct {
		name, text     string
		inputs         map[query.Symbol]bool
		later, earlier int
		msg            string
	}{
		{
			"no available argument, a scan providing one",
			`[:find ?c :in $ % :where (follows ?b ?c) [?x :page/next ?b]]`,
			map[query.Symbol]bool{}, 0, 1,
			"an invocation with no available argument waits on the scan providing its argument",
		},
		{
			"an available argument, a seek providing the other",
			`[:find ?c :in $ % ?a :where (path ?a ?b ?c) [?a :item/link ?b]]`,
			map[query.Symbol]bool{a: true}, 0, 1,
			"an invocation with an available argument waits on a seek providing its other argument",
		},
		{
			"an available argument, a scan providing the other",
			`[:find ?c :in $ % ?a :where [?x :page/next ?b] (path ?a ?b ?c)]`,
			map[query.Symbol]bool{a: true}, 0, 1,
			"an invocation with an available argument waits on no scan providing its other argument",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			phases, err := createPhasesGreedy(q.Where, physicalFindSymbols(q.Find), c.inputs, query.Bindings{Rules: rules})
			require.NoError(t, err)
			assertScheduledAfter(t, phases, q.Where[c.later], q.Where[c.earlier], c.msg)
		})
	}
}
