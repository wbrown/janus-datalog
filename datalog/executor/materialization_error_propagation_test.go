package executor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Reproductions for BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS.md
//
// Several materialization/union paths call collectTuplesInto and ignore its
// returned error, turning a failed partial stream into a clean materialized
// relation. Each must instead carry the error onto the derived relation (replayed
// at the next public boundary via Iterator().Error()).

// TestMaterializeResult_PropagatesIteratorError: MaterializeResult must not launder
// a failed source into a clean materialized relation.
func TestMaterializeResult_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	rel := MaterializeResult(src, testSymbols())
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestSortRelation_PropagatesIteratorError: SortRelation collects then sorts; a
// source failure must survive materialization rather than yield clean sorted rows.
func TestSortRelation_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(1, Tuple{int64(3)}, Tuple{int64(1)}, Tuple{int64(2)})
	rel := SortRelation(src, []query.OrderByClause{{Variable: datalog.NewSymbol("?x")}})
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestCombineSubqueryResultsSimple_PropagatesIteratorError: unioning subquery
// results must not drop an error from one branch and return the others as success.
func TestCombineSubqueryResultsSimple_PropagatesIteratorError(t *testing.T) {
	clean := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}})
	failing := newFailingStream(0, Tuple{int64(2)})
	rel := combineSubqueryResultsSimple([]Relation{clean, failing})
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

func TestAggregationPropagatesIteratorAndCloseErrors(t *testing.T) {
	x := datalog.NewSymbol("?x")
	aggregate := []query.FindAggregate{{Function: datalog.SymCount, Arg: x}}
	closeErr := errors.New("aggregate close failure")

	for _, failAfter := range []int{0, 2} {
		t.Run(fmt.Sprintf("streaming/fail_after_%d", failAfter), func(t *testing.T) {
			source := newFailingStream(
				failAfter,
				Tuple{int64(1)},
				Tuple{int64(2)},
				Tuple{int64(3)},
			)
			result := NewStreamingAggregateRelation(source, []query.Symbol{x}, aggregate)
			require.ErrorIs(t, driveErr(result), errInjectedIterator)
		})
		t.Run(fmt.Sprintf("batch/fail_after_%d", failAfter), func(t *testing.T) {
			source := newFailingRelation(
				failAfter,
				Tuple{int64(1)},
				Tuple{int64(2)},
				Tuple{int64(3)},
			)
			result := executeGroupedAggregation(source, []query.Symbol{x}, aggregate)
			require.ErrorIs(t, driveErr(result), errInjectedIterator)
		})
	}

	for _, mode := range []string{"single", "grouped", "streaming"} {
		t.Run(mode+"/close_error", func(t *testing.T) {
			source := failingRelation{
				Relation: NewMaterializedRelation(
					[]query.Symbol{x},
					[]Tuple{{int64(1)}, {int64(2)}},
				),
				failAfter: 100,
				closeErr:  closeErr,
			}
			var result Relation
			switch mode {
			case "single":
				result = executeSingleAggregation(source, aggregate)
			case "grouped":
				result = executeGroupedAggregation(source, []query.Symbol{x}, aggregate)
			case "streaming":
				result = NewStreamingAggregateRelation(source, []query.Symbol{x}, aggregate)
			}
			require.ErrorIs(t, driveErr(result), closeErr)
		})
	}
}
