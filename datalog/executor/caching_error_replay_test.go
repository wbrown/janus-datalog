package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Part 2 of docs/proposals/ITERATOR_ERROR_CONTRACT_ENFORCEMENT.md:
// caching relations must replay the source iterator's error through their own
// Iterator().Error(), so the failure survives across internal materialization
// (order-by, aggregation, joins, union) and reaches an Error()-checking boundary.

// TestMaterialize_ReplaysSourceError: Materialize() of a stream that fails
// mid-iteration must report the error on EVERY iteration. The first drive builds
// the cache (and propagates via the real iterator); the replay must report it
// too — that's the cached-failure path that currently drops it.
func TestMaterialize_ReplaysSourceError(t *testing.T) {
	m := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)}).Materialize()
	require.ErrorIs(t, driveErr(m), errInjectedIterator, "first iteration (cache build)")
	require.ErrorIs(t, driveErr(m), errInjectedIterator, "replay from cache must also report the error")
}

// TestMaterialize_ReplaysSourceError_ImmediateFailure: a stream that fails
// before yielding anything still reports the error on replay.
func TestMaterialize_ReplaysSourceError_ImmediateFailure(t *testing.T) {
	m := newFailingStream(0, Tuple{int64(1)}).Materialize()
	require.ErrorIs(t, driveErr(m), errInjectedIterator, "first iteration (cache build)")
	require.ErrorIs(t, driveErr(m), errInjectedIterator, "replay from cache must also report the error")
}

// TestSort_ReplaysSourceError: order-by materializes; the error must survive.
func TestSort_ReplaysSourceError(t *testing.T) {
	orderBy := []query.OrderByClause{{Variable: datalog.NewSymbol("?x"), Descending: false}}
	sorted := newFailingStream(1, Tuple{int64(2)}, Tuple{int64(1)}, Tuple{int64(3)}).Sort(orderBy)
	require.ErrorIs(t, driveErr(sorted), errInjectedIterator)
}

// TestCollectTuples_OverMaterializedFailingRelation: the end-to-end property —
// a boundary over a materialized failing relation returns the error (Part 1 +
// Part 2 together).
func TestCollectTuples_OverMaterializedFailingRelation(t *testing.T) {
	m := newFailingStream(2, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)}).Materialize()
	_, err := CollectTuples(m, nil)
	require.ErrorIs(t, err, errInjectedIterator)
}
