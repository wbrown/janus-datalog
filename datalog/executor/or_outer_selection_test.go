package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestFindOuterRelationLeavesSingleStreamLazy(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	stream := NewStreamingRelation(
		[]query.Symbol{entity},
		base.Iterator(),
	)
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})

	selected, consumed := exec.findOuterRelation(
		[]query.Symbol{entity},
		Relations{stream},
	)
	require.Same(t, stream, selected)
	require.Equal(t, []int{0}, consumed)
	require.False(t, stream.shouldCache,
		"single outer selection must not request eager materialization")
}

// Two streaming groups sharing no symbol are joined as their product, a
// subject carrying both groups' symbols and consuming both groups.
func TestFindOuterRelationCombinesTwoStreamsSharingNoSymbolAsTheirProduct(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	leftBase := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(2)}})
	rightBase := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(3)}})
	left := NewStreamingRelation([]query.Symbol{x}, leftBase.Iterator())
	right := NewStreamingRelation([]query.Symbol{y}, rightBase.Iterator())
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})

	selected, consumed := exec.findOuterRelation(
		[]query.Symbol{x, y},
		Relations{left, right},
	)
	require.Equal(t, []int{0, 1}, consumed)
	syms := selected.Symbols()
	xi, yi := query.SymbolIndex(syms, x), query.SymbolIndex(syms, y)
	require.True(t, xi >= 0 && yi >= 0, "the subject carries both groups' symbols; got %v", syms)
	tuples, err := CollectTuples(selected, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	require.Equal(t, int64(2), tuples[0][xi])
	require.Equal(t, int64(3), tuples[0][yi])
}

// collectBranch, the boundary at which a branch's result is kept for its
// index, surfaces the result's iteration error and its close error.
func TestCollectBranchPreservesIteratorAndCloseErrors(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	branch := NewMaterializedRelation(
		[]query.Symbol{entity, value},
		[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
	)

	_, iterationFailure := collectBranch(failingRelation{Relation: branch, failAfter: 1})
	require.ErrorIs(t, iterationFailure, errInjectedIterator)

	closeFailure := errors.New("collect branch close failure")
	_, closeResult := collectBranch(failingRelation{Relation: branch, failAfter: 100, closeErr: closeFailure})
	require.ErrorIs(t, closeResult, closeFailure)
}

// A subject's iteration error and its close error are the result's: the
// first surfaces when the result is driven, the second when it is closed.
func TestOrFallbackSurfacesTheSubjectsIteratorAndCloseErrors(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)

	testCases := []struct {
		name     string
		outer    Relation
		expected error
	}{
		{
			name:     "iteration",
			outer:    failingRelation{Relation: base, failAfter: 1},
			expected: errInjectedIterator,
		},
		{
			name:     "close",
			outer:    failingRelation{Relation: base, failAfter: 100, closeErr: errors.New("subject close failure")},
			expected: errors.New("subject close failure"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			relation, err := NewOrFallbackRelation(
				newQueryExecutor(nil, nil, ExecutorOptions{}),
				NewContext(),
				[]query.Symbol{entity},
				[]query.Symbol{value},
				[][]query.Clause{{&query.Expression{
					Function: &query.GroundFunction{Value: "fallback"},
					Binding:  value,
				}}},
				testCase.outer,
				ExecutorOptions{},
				true,
			)
			require.NoError(t, err)
			iterator := relation.Iterator()
			for iterator.Next() {
			}
			err = iterator.Error()
			if closeErr := iterator.Close(); err == nil {
				err = closeErr
			}
			require.EqualError(t, err, testCase.expected.Error())
		})
	}
}
