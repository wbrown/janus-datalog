package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Reproductions for docs/bugs/BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP.md

// TestUnionIterator_InnerIteratorErrorPropagates: when an inner relation's
// iterator yields some tuples and then fails, UnionIterator closes it without
// checking Error() first, so the failure is lost. Consuming the union to
// exhaustion and checking Error() must surface the inner failure.
func TestUnionIterator_InnerIteratorErrorPropagates(t *testing.T) {
	syms := []query.Symbol{datalog.NewSymbol("?x")}

	ch := make(chan relationItem, 1)
	ch <- relationItem{relation: newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})}
	close(ch)

	ur := NewUnionRelation(ch, syms, ExecutorOptions{})
	it := ur.Iterator()
	defer it.Close()
	for it.Next() {
		// drain to exhaustion
	}
	require.ErrorIs(t, it.Error(), errInjectedIterator)
}

// NOTE: the UnionRelation concurrent-iteration / one-shot-channel cache-build
// race is a separate bug (BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP,
// "Failure Mode 1"). Its reproduction belongs with that fix, not the
// error-propagation work here.
