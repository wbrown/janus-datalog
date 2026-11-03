package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestMatchTreatsErroredEmptyBindingAsError: an errored relation that
// materialized empty (the genuine route: a stream fails, Sort materializes
// zero tuples plus the taint) is not an empty binding. Match's emptiness
// fallback must surface the error instead of discarding the binding and
// scanning unbound — the laundering half of
// BUG_MISSING_ON_LOOKUPLESS_MATCHER_SILENTLY_EMPTY.md.
func TestMatchTreatsErroredEmptyBindingAsError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	valAttr := datalog.NewKeyword(":item/val")
	matcher := factsDatabase(t, []datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	})

	erroredEmpty := NewFailingStream(0, Tuple{int64(1)}).Sort(nil)
	require.Equal(t, 0, erroredEmpty.Size(), "fixture: materialized empty")
	require.ErrorIs(t, EmptyRelationError(erroredEmpty), ErrInjectedIterator, "fixture: carries the taint")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: x},
		query.Constant{Value: valAttr},
		query.Blank{},
	}}
	_, err := matcher.Match(query.PatternQuery(pattern), Relations{erroredEmpty})
	require.ErrorIs(t, err, ErrInjectedIterator)
}
