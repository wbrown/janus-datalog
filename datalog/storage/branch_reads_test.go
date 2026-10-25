package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A branch is given its form's required run, and a clause of the branch that
// binds a given symbol unifies with it, so a branch whose clauses each
// provide the symbol another consumes runs over the run as any branch that
// reads it does, on both planner paths and in every written order. A frame
// over no run would leave both symbols unbound, and the planner would refuse
// the branch as unorderable.

// edgeFacts: :t/p a→b, b→b and c→a.
func edgeFacts() []fact {
	a, b, c := named("a"), named("b"), named("c")
	return []fact{{a, ":t/p", b}, {b, ":t/p", b}, {c, ":t/p", a}}
}

// identityCycle binds each of two required symbols from the other, so the
// body holds exactly when the two agree.
const identityCycle = `[(identity ?e) ?w] [(identity ?w) ?e]`

// localCycle binds a branch local from a required symbol and the required
// symbol back from the local, so the body holds of every tuple.
const localCycle = `[(identity ?e) ?x] [(identity ?x) ?e]`

// Under not and not-join the cycle drops the self-edge b→b; under or-join it
// keeps it alone; under or-default-join it marks it with 1 and the others
// with 0; under a subquery it answers the self-edge's entity. The cycle over
// a local keeps every tuple under or-join and drops every tuple under not.
func TestFrame_ABranchProvidingARequiredSymbolReadsIt(t *testing.T) {
	cases := []struct {
		name, find, form string
		want             []string
	}{
		{"not", "?e ?w", "(not " + identityCycle + ")", []string{"a b", "c a"}},
		{"not-join", "?e ?w", "(not-join [?e ?w] " + identityCycle + ")", []string{"a b", "c a"}},
		{"or-join", "?e ?w", "(or-join [[?e ?w]] (and " + identityCycle + "))", []string{"b b"}},
		{"or-default-join", "?e ?w ?k", "(or-default-join [[?e ?w] ?k] (and " + identityCycle + " [(ground 1) ?k]) [(ground 0) ?k])", []string{"a b 0", "b b 1", "c a 0"}},
		{"subquery", "?e ?w ?x", "[(q [:find ?x :in $ ?e ?w :where " + identityCycle + " [(identity ?e) ?x]] $ ?e ?w) [[?x]]]", []string{"b b b"}},
		{"or-join over a local", "?e ?w", "(or-join [[?e]] (and " + localCycle + "))", []string{"a b", "b b", "c a"}},
		{"not over a local", "?e ?w", "(not " + localCycle + ")", nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, edgeFacts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					everyOrder(t, d, names, c.find, []string{"[?e :t/p ?w]", c.form}, c.want)
				})
			}
		})
	}
}

// kindFacts: d carries :t/r 2.0 and :t/k :p, e carries :t/k :q and :t/q to
// d, and f carries :t/k :p.
func kindFacts() []fact {
	d, e, f := named("d"), named("e"), named("f")
	return []fact{
		{d, ":t/r", 2.0}, {d, ":t/k", datalog.NewKeyword(":p")},
		{e, ":t/k", datalog.NewKeyword(":q")}, {e, ":t/q", d},
		{f, ":t/k", datalog.NewKeyword(":p")},
	}
}

// A flat or-join header names outputs every branch computes alone, so a
// branch that binds ?e only from a local bound from ?e has no order that
// runs, and the text is refused at Query on both planner paths, whether the
// or-join's subject holds a tuple or none: a text's acceptance does not
// depend on the store.
func TestFrame_ABranchNoOrderRunsIsRefusedAtQuery(t *testing.T) {
	const text = `[:find ?e ?x :where [?x :t/r 2.0] [?e :t/k ?a] [?x :t/k :p] (or-join [?e] [?e :t/q ?m] (and [(identity ?x) ?e] [(identity ?e) ?x])) (not [(identity ?m) ?e] [(identity ?e) ?m])]`
	stores := []struct {
		name  string
		facts []fact
	}{
		{"a subject holding tuples", kindFacts()},
		{"an empty subject", []fact{{named("z"), ":t/n", int64(1)}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, store := range stores {
				t.Run(store.name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{})
					commitFacts(t, d, store.facts)
					_, err := d.Query(text)
					require.Error(t, err, "a branch no order runs is refused at Query, whatever the store holds")
					require.Contains(t, err.Error(), "cannot order clauses: [(identity ?x) ?e] waits on ?x; [(identity ?e) ?x] waits on ?e")
				})
			}
		})
	}
}

// Explain plans the prepared query, so a branch no order runs is refused at
// Explain as it is at Query, on both planner paths, with one text from
// "cannot order clauses:" on
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3 and §4.1).
func TestExplain_RefusesABranchNoOrderRunsAsQueryDoes(t *testing.T) {
	const text = `[:find ?e ?x :where [?x :t/r 2.0] [?e :t/k ?a] [?x :t/k :p] (or-join [?e] [?e :t/q ?m] (and [(identity ?x) ?e] [(identity ?e) ?x])) (not [(identity ?m) ?e] [(identity ?e) ?m])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, kindFacts())
			_, explainErr := d.Explain(text)
			require.Error(t, explainErr, "Explain refuses a branch no order runs")
			_, queryErr := d.Query(text)
			require.Error(t, queryErr, "Query refuses a branch no order runs")
			require.Equal(t, stallText(t, queryErr), stallText(t, explainErr), "one text for one stall at Explain and at Query")
		})
	}
}

// stallText is a refusal's text from "cannot order clauses:" on, the part of
// the message both planner paths and every boundary state alike.
func stallText(t *testing.T, err error) string {
	t.Helper()
	at := strings.Index(err.Error(), "cannot order clauses:")
	require.GreaterOrEqual(t, at, 0, "the refusal names the stall: %v", err)
	return err.Error()[at:]
}

// numberFacts: p, q and s carry :t/n 1, 2 and 3.
func numberFacts() []fact {
	p, q, s := named("p"), named("q"), named("s")
	return []fact{{p, ":t/n", int64(1)}, {q, ":t/n", int64(2)}, {s, ":t/n", int64(3)}}
}

// An arithmetic cycle over a required symbol, ?v bound from ?n and ?n bound
// back from ?v, holds of every ?n: a not holding it empties the result, and
// an or-join holding it keeps every entity.
func TestFrame_AnArithmeticCycleOverARequiredSymbolRuns(t *testing.T) {
	const cycle = `[(+ ?n 1) ?v] [(- ?v 1) ?n]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, numberFacts())
			everyOrder(t, d, names, "?e", []string{"[?e :t/n ?n]", "(not " + cycle + ")"}, nil)
			everyOrder(t, d, names, "?e", []string{"[?e :t/n ?n]", "(or-join [[?n]] (and " + cycle + "))"}, []string{"p", "q", "s"})
		})
	}
}
