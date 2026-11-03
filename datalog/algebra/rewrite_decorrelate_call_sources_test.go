package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Rule 1 compiles an aggregating subquery's inner query into the outer tree
// under the sources its call passes. The get-else scan rewrite, which runs
// over the decorrelated tree, then asks about the source the call passes,
// and the decorrelated call declares each source once and passes it under
// its own name.
func TestADecorrelatedAggregateReadsTheSourcesItsCallPasses(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"the call passes $src at the inner $",
			`[:find ?e ?n :in $ $src :where [?e :t/name _]
			  [(q [:find (count ?v) :in $ ?x :where [?x :t/name _] [(get-else $ ?x :t/a "none") ?v]] $src ?e) [[?n]]]]`},
		{"the call passes $src at two inner positions",
			`[:find ?e ?n :in $ $src :where [?e :t/name _]
			  [(q [:find (count ?v) :in $ $b ?x :where [$b ?x :t/name _] [(get-else $ ?x :t/a "none") ?v]] $src $src ?e) [[?n]]]]`},
	}
	src := datalog.NewSymbol("$src")
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			prepared, err := query.Prepare(parsed, nil)
			require.NoError(t, err)
			root, err := Compile(prepared, nil)
			require.NoError(t, err)

			var asked []query.Symbol
			answers := func(source query.Symbol, _ datalog.Keyword) bool {
				asked = append(asked, source)
				return true
			}
			optimized, err := NewOptimizer(DefaultPasses(nil, answers)...).Optimize(root)
			require.NoError(t, err)
			require.Equal(t, []query.Symbol{src}, asked,
				"the get-else scan rewrite asks about the source the call passes")

			var aggregates []*Aggregate
			var walk func(n *Node)
			walk = func(n *Node) {
				if aggregate, isAggregate := n.Data.(*Aggregate); isAggregate {
					aggregates = append(aggregates, aggregate)
				}
				for _, child := range n.Children {
					walk(child)
				}
			}
			walk(optimized)
			require.Len(t, aggregates, 1, "the subquery is decorrelated")
			require.Equal(t, []query.InputSpec{query.DatabaseInput{Name: src}}, aggregates[0].In,
				"the decorrelated inner query declares the source the call passes, once")
			require.Equal(t, []query.PatternElement{query.Constant{Value: src}}, aggregates[0].Inputs,
				"the decorrelated call passes the source under its own name")
		})
	}
}

// underCallSources reads a decorrelated inner query under the sources its
// call passes: each clause's source renamed by the call's substitution, each
// :in source the source standing for it, declared once, and the call passing
// each under its own name. Its clauses are RenameSources over CallSources, the
// composition ReadSubqueryFrame and the roster read a call's clauses through.
// Neither renames the :in, which Rule 1's rebuilt call declares.
func TestUnderCallSourcesRenamesTheInnerQueryToTheSourcesItsCallPasses(t *testing.T) {
	src := datalog.NewSymbol("$src")
	b := datalog.NewSymbol("$b")
	x := query.Variable{Name: datalog.NewSymbol("?x")}
	named := func(source query.Symbol) *query.DataPattern {
		return &query.DataPattern{Source: source, Elements: []query.PatternElement{
			x, query.Constant{Value: datalog.NewKeyword(":t/name")}, query.Blank{},
		}}
	}
	decorrelated := &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: x.Name}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.DatabaseInput{Name: b}},
		Where: []query.Clause{named(nil), named(b)},
	}
	call := &query.SubqueryPattern{
		Query: &query.Query{In: []query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar}, query.DatabaseInput{Name: b}, query.ScalarInput{Symbol: x.Name},
		}},
		Inputs: []query.PatternElement{query.Constant{Value: src}, query.Constant{Value: src}, query.Variable{Name: datalog.NewSymbol("?e")}},
	}

	renamed, inputs := underCallSources(decorrelated, query.CallSources(call),
		[]query.PatternElement{query.Constant{Value: src}, query.Constant{Value: src}})
	require.Equal(t, []query.InputSpec{query.DatabaseInput{Name: src}}, renamed.In,
		"the source the call passes at two places is declared once")
	require.Equal(t, []query.PatternElement{query.Constant{Value: src}}, inputs,
		"the call passes the source under its own name")
	require.Len(t, renamed.Where, 2)
	for _, clause := range renamed.Where {
		require.Equal(t, src, clause.(*query.DataPattern).Source, "every clause reads the source the call passes")
	}
}
