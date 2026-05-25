package executor

import (
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
