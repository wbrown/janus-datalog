package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestCachingIteratorEarlyClosePublishesIncompleteError(t *testing.T) {
	stream := NewStreamingRelation(
		testSymbols(),
		NewMaterializedRelation(
			testSymbols(),
			[]Tuple{{int64(1)}, {int64(2)}, {int64(3)}},
		).Iterator(),
	)
	stream.Materialize()
	first := stream.Iterator()
	require.True(t, first.Next())
	require.Equal(t, Tuple{int64(1)}, first.Tuple())
	require.ErrorIs(t, first.Close(), errIncompleteMaterialization)

	replay := stream.Iterator()
	for replay.Next() {
	}
	require.ErrorIs(t, replay.Error(), errIncompleteMaterialization)
	require.NoError(t, replay.Close())
}

func TestPredicateFilterIteratorPropagatesEvaluationError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	missing := datalog.NewSymbol("?missing")
	iterator := NewPredicateFilterIterator(
		NewMaterializedRelation(
			[]query.Symbol{x},
			[]Tuple{{int64(1)}},
		).Iterator(),
		[]query.Symbol{x},
		&query.Comparison{
			Op:    datalog.SymGT,
			Left:  query.VariableTerm{Symbol: missing},
			Right: query.ConstantTerm{Value: int64(0)},
		},
	)

	require.False(t, iterator.Next())
	require.ErrorContains(t, iterator.Error(), "?missing")
	require.NoError(t, iterator.Close())
}

func TestStreamingFilterUsesRelationIteratorAndBuildsReplayCache(t *testing.T) {
	x := datalog.NewSymbol("?x")
	stream := NewStreamingRelationWithOptions(
		[]query.Symbol{x},
		NewMaterializedRelation(
			[]query.Symbol{x},
			[]Tuple{{int64(1)}, {int64(2)}, {int64(3)}},
		).Iterator(),
		ExecutorOptions{EnableIteratorComposition: true},
	)
	stream.Materialize()
	filtered := stream.FilterWithPredicate(&query.Comparison{
		Op:    datalog.SymGT,
		Left:  query.VariableTerm{Symbol: x},
		Right: query.ConstantTerm{Value: int64(1)},
	})
	filteredRows, err := CollectTuples(filtered, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, filteredRows)

	replayedRows, err := CollectTuples(stream, nil)
	require.NoError(t, err)
	require.Equal(t,
		[][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		replayedRows,
	)
}

func TestHashJoinPropagatesBuildAndProbeCloseErrors(t *testing.T) {
	key := datalog.NewSymbol("?key")
	value := datalog.NewSymbol("?value")
	closeErr := errors.New("hash join close failure")

	t.Run("build", func(t *testing.T) {
		build := failingRelation{
			Relation: NewMaterializedRelation(
				[]query.Symbol{key},
				[]Tuple{{int64(1)}},
			),
			failAfter: 100,
			closeErr:  closeErr,
		}
		probe := NewMaterializedRelation(
			[]query.Symbol{key, value},
			[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
		)
		require.ErrorIs(t, driveErr(HashJoin(build, probe, []query.Symbol{key})), closeErr)
	})

	t.Run("probe", func(t *testing.T) {
		build := NewMaterializedRelation(
			[]query.Symbol{key},
			[]Tuple{{int64(1)}},
		)
		probe := failingRelation{
			Relation: NewMaterializedRelation(
				[]query.Symbol{key, value},
				[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
			),
			failAfter: 100,
			closeErr:  closeErr,
		}
		require.ErrorIs(t, driveErr(HashJoin(build, probe, []query.Symbol{key})), closeErr)
	})
}

func TestStreamingRelationGetMaterializesForRandomAccess(t *testing.T) {
	stream := NewStreamingRelation(
		testSymbols(),
		NewMaterializedRelation(
			testSymbols(),
			[]Tuple{{int64(1)}, {int64(2)}, {int64(3)}},
		).Iterator(),
	)

	require.Equal(t, Tuple{int64(2)}, stream.Get(1))
	require.Nil(t, stream.Get(3))
}
