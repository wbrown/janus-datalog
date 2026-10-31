package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A held relation's identity projection is the relation itself, with its
// properties and its deferred error; its projection onto a permutation of
// its symbols is another relation holding the same tuples under the new
// order (docs/proposals/BINDING_MATCH_PROOF.md, T1).
func TestProjectOntoItsOwnSymbolsAnswersTheRelation(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	symbols := []query.Symbol{a, b}
	rel := NewMaterializedRelation(symbols, []Tuple{{int64(1), "x"}, {int64(2), "y"}})

	same, err := rel.Project([]query.Symbol{a, b})
	require.NoError(t, err)
	require.Same(t, rel, same, "the identity projection is the relation itself")

	permuted, err := rel.Project([]query.Symbol{b, a})
	require.NoError(t, err)
	require.NotSame(t, rel, permuted, "a permutation is another relation")
	require.Equal(t, []query.Symbol{b, a}, permuted.Symbols())
	tuples, err := CollectTuples(permuted, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x", int64(1)}, {"y", int64(2)}}, tuples)

	tainted := newMaterializedRelationFromSet(symbols, []Tuple{{int64(1), "x"}}, ExecutorOptions{}, RelationProperties{})
	tainted.err = errors.New("the scan failed")
	same, err = tainted.Project([]query.Symbol{a, b})
	require.NoError(t, err)
	require.Same(t, tainted, same, "the identity projection of tainted data is the tainted relation")
}

// A selection rejecting nothing over a held relation is the relation itself,
// with its deferred error; a selection rejecting a tuple is another relation
// holding the survivors; a selection over a stream is a held relation holding
// the stream's tuples (docs/proposals/BINDING_MATCH_PROOF.md, T2).
func TestSelectRejectingNothingAnswersTheRelation(t *testing.T) {
	v := datalog.NewSymbol("?v")
	symbols := []query.Symbol{v}
	rel := NewMaterializedRelation(symbols, []Tuple{{int64(1)}, {int64(2)}, {int64(3)}})
	all := func(Tuple) bool { return true }
	require.Same(t, rel, Select(rel, all), "a selection rejecting nothing is the relation itself")

	tainted := newMaterializedRelationFromSet(symbols, []Tuple{{int64(1)}}, ExecutorOptions{}, RelationProperties{})
	tainted.err = errors.New("the scan failed")
	require.Same(t, tainted, Select(tainted, all), "a selection rejecting nothing of tainted data is the tainted relation")

	odd := Select(rel, func(tuple Tuple) bool { return tuple[0].(int64)%2 == 1 })
	require.NotSame(t, rel, odd, "a selection rejecting a tuple is another relation")
	tuples, err := CollectTuples(odd, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, tuples)

	stream := NewStreamingRelationWithOptions(symbols, &sliceIterator{tuples: []Tuple{{int64(1)}, {int64(2)}}, pos: -1}, ExecutorOptions{})
	held := Select(stream, all)
	require.IsType(t, &MaterializedRelation{}, held, "a selection over a stream is a held relation")
	tuples, err = CollectTuples(held, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, tuples)
}
