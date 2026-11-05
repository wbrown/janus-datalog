package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// newAggregationLayout reads an aggregating :find's group keys, its variables
// and its pulls' entity variables, and its aggregates, each in written order,
// with the result's symbols in written order, each aggregate under its display
// symbol and a pull under its entity variable, and the position each key and
// each aggregate takes in a result tuple.
func TestNewAggregationLayoutReadsEachElementAtItsWrittenPlace(t *testing.T) {
	k, v, e := datalog.NewSymbol("?k"), datalog.NewSymbol("?v"), datalog.NewSymbol("?e")
	count := query.FindAggregate{Function: datalog.SymCount, Arg: v}
	sum := query.FindAggregate{Function: datalog.SymSum, Arg: v}
	pull := query.FindPull{Variable: e, Pattern: &query.PullPattern{}}
	countSymbol, sumSymbol := datalog.NewSymbol("(count ?v)"), datalog.NewSymbol("(sum ?v)")

	cases := []struct {
		name        string
		find        []query.FindElement
		symbols     []query.Symbol
		groupByVars []query.Symbol
		aggregates  []query.FindAggregate
		keyAt       []int
		aggregateAt []int
	}{
		{"a key written first",
			[]query.FindElement{query.FindVariable{Symbol: k}, count},
			[]query.Symbol{k, countSymbol},
			[]query.Symbol{k}, []query.FindAggregate{count}, []int{0}, []int{1}},
		{"an aggregate first, a variable and a pull between, an aggregate last",
			[]query.FindElement{count, query.FindVariable{Symbol: k}, pull, sum},
			[]query.Symbol{countSymbol, k, e, sumSymbol},
			[]query.Symbol{k, e}, []query.FindAggregate{count, sum}, []int{1, 2}, []int{0, 3}},
		{"aggregates alone",
			[]query.FindElement{sum, count},
			[]query.Symbol{sumSymbol, countSymbol},
			nil, []query.FindAggregate{sum, count}, nil, []int{0, 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			layout := newAggregationLayout(c.find)
			require.Equal(t, c.symbols, layout.symbols, "the result's symbols are the find's in written order, each aggregate under its display symbol and a pull under its entity variable")
			require.Equal(t, c.groupByVars, layout.groupByVars, "the group keys are the variables and the pulls' entity variables in written order")
			require.Equal(t, c.aggregates, layout.aggregates, "the aggregates are the find's in written order")
			require.Equal(t, c.keyAt, layout.keyAt, "each key's position is its element's")
			require.Equal(t, c.aggregateAt, layout.aggregateAt, "each aggregate's position is its element's")
		})
	}
}
