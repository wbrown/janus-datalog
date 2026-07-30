package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestGroupedAggregationPublishesGroupKey(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	input := NewMaterializedRelation(
		[]query.Symbol{group, value},
		[]Tuple{
			{"a", int64(1)},
			{"a", int64(2)},
			{"b", int64(3)},
		},
	)
	aggregates := []query.FindAggregate{{Function: datalog.SymSum, Arg: value}}

	result := executeGroupedAggregation(input, []query.Symbol{group}, aggregates)
	require.Equal(t,
		RelationProperties{Keys: [][]query.Symbol{{group}}},
		result.Properties(),
	)
	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{"a", int64(3)},
		{"b", int64(3)},
	}, tuples)
}

func TestRealizedStreamingAggregationRetainsGroupKey(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	base := NewMaterializedRelation(
		[]query.Symbol{group, value},
		[]Tuple{
			{"a", int64(1)},
			{"a", int64(2)},
			{"b", int64(3)},
		},
	)
	result := NewStreamingAggregateRelation(
		NewStreamingRelation(
			[]query.Symbol{group, value},
			base.Iterator(),
		),
		[]query.Symbol{group},
		[]query.FindAggregate{{Function: datalog.SymSum, Arg: value}},
	)
	materialized := result.Materialize()
	require.Equal(t,
		RelationProperties{Keys: [][]query.Symbol{{group}}},
		materialized.Properties(),
	)
	tuples, err := CollectTuples(materialized, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 2)
}
