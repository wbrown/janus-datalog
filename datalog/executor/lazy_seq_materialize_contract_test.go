package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestLazySeqRelationMaterializeIsReplayableWithoutEagerRealization(t *testing.T) {
	nextCalls := 0
	relation := NewLazySeqRelation(
		NewTupleSeq(
			&countingSliceIterator{
				tuples:    []Tuple{{int64(1)}, {int64(2)}, {int64(3)}},
				nextCalls: &nextCalls,
			},
			false,
		),
		[]query.Symbol{datalog.NewSymbol("?x")},
	)

	materialized := relation.Materialize()
	require.Same(t, relation, materialized)
	require.Zero(t, nextCalls)

	first, err := CollectTuples(materialized, nil)
	require.NoError(t, err)
	second, err := CollectTuples(materialized, nil)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 3, nextCalls, "the shared source must advance only once")
}

func TestLazySeqRelationEagerConsumersStillRealizeCompleteRelation(t *testing.T) {
	nextCalls := 0
	x := datalog.NewSymbol("?x")
	relation := NewLazySeqRelation(
		NewTupleSeq(
			&countingSliceIterator{
				tuples:    []Tuple{{int64(3)}, {int64(1)}, {int64(2)}},
				nextCalls: &nextCalls,
			},
			false,
		),
		[]query.Symbol{x},
	)

	sorted := relation.Sort([]query.OrderByClause{{
		Variable:   x,
		Descending: false,
	}})
	tuples, err := CollectTuples(sorted, nil)
	require.NoError(t, err)
	require.Equal(t,
		[][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		tuples,
	)
	require.Equal(t, 3, nextCalls)
}
