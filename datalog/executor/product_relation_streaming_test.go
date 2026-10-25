package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestProductRelation_StreamingRightSide_NoPanic is a regression test for
// BUG_PRODUCT_RELATION_REOPENS_STREAMING_RELATIONS.
//
// The nested-loop ProductIterator consumes the leftmost operand once and rewinds
// every other operand (reopening its Iterator()) once per outer tuple. A
// StreamingRelation is single-use and panics on a second Iterator() call, so the
// non-leftmost operands must be made re-iterable first. This also exercises the
// re-iteration: the right side must replay its tuples for each left tuple.
func TestProductRelation_StreamingRightSide_NoPanic(t *testing.T) {
	opts := ExecutorOptions{EnableTrueStreaming: true}

	leftSrc := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, []Tuple{{int64(1)}, {int64(2)}})
	rightSrc := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?y")}, []Tuple{{"a"}, {"b"}})

	left := NewStreamingRelationWithOptions(leftSrc.Symbols(), leftSrc.Iterator(), opts)
	right := NewStreamingRelationWithOptions(rightSrc.Symbols(), rightSrc.Iterator(), opts)

	product := Relations{left, right}.Product()

	tuples, err := CollectTuples(product, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 4, "2x2 cross-product should yield 4 tuples (right replayed per left tuple)")
}

// TestProductRelation_SurfacesInnerStreamError verifies the product surfaces a
// failed (non-leftmost) operand's iterator error rather than laundering it into a
// clean cross-product.
func TestProductRelation_SurfacesInnerStreamError(t *testing.T) {
	left := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, []Tuple{{int64(1)}, {int64(2)}})
	right := newFailingStream(0, Tuple{int64(10)}) // inner operand fails immediately

	product := Relations{left, Relation(right)}.Product()
	require.ErrorIs(t, driveErr(product), errInjectedIterator)
}

// streamOf wraps a materialized relation's tuples as a streaming relation,
// single-use until Materialize marks it for replay.
func streamOf(symbol string, tuples ...Tuple) *StreamingRelation {
	src := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol(symbol)}, tuples)
	return NewStreamingRelationWithOptions(src.Symbols(), src.Iterator(), ExecutorOptions{EnableTrueStreaming: true})
}

// A product over an empty leftmost relation answers no tuple and leaves a
// right relation marked for replay replayable whole: the product never opens
// it.
func TestProductRelation_EmptyLeftLeavesTheRightReplayable(t *testing.T) {
	left := streamOf("?x")
	right := streamOf("?y", Tuple{"a"}, Tuple{"b"}).Materialize()

	tuples, err := CollectTuples(Relations{left, right}.Product(), nil)
	require.NoError(t, err)
	require.Empty(t, tuples)

	replay, err := CollectTuples(right, nil)
	require.NoError(t, err, "the right relation replays whole after the product")
	require.Len(t, replay, 2)
}

// A product over a marked non-empty leftmost relation and an empty right one
// answers no tuple and leaves the left replayable whole: the product pulled
// it once and reads it to its end before closing it.
func TestProductRelation_EmptyRightLeavesTheLeftReplayable(t *testing.T) {
	left := streamOf("?x", Tuple{int64(1)}, Tuple{int64(2)}).Materialize()
	right := streamOf("?y")

	tuples, err := CollectTuples(Relations{left, right}.Product(), nil)
	require.NoError(t, err)
	require.Empty(t, tuples)

	replay, err := CollectTuples(left, nil)
	require.NoError(t, err, "the left relation replays whole after the product")
	require.Len(t, replay, 2)
}

// A product over three relations with the middle one empty answers no tuple
// and leaves the marked left and right replayable whole.
func TestProductRelation_EmptyMiddleLeavesTheOthersReplayable(t *testing.T) {
	left := streamOf("?x", Tuple{int64(1)}, Tuple{int64(2)}).Materialize()
	middle := streamOf("?m")
	right := streamOf("?y", Tuple{"a"}, Tuple{"b"}).Materialize()

	tuples, err := CollectTuples(Relations{left, middle, right}.Product(), nil)
	require.NoError(t, err)
	require.Empty(t, tuples)

	replay, err := CollectTuples(left, nil)
	require.NoError(t, err, "the left relation replays whole after the product")
	require.Len(t, replay, 2)
	replay, err = CollectTuples(right, nil)
	require.NoError(t, err, "the right relation replays whole after the product")
	require.Len(t, replay, 2)
}

// completeCache reads an iterator filling a replay cache to its end, so that
// closing it leaves its relation replayable whole, and leaves any other
// iterator where it stands.
func TestCompleteCacheReadsACachingIteratorToItsEnd(t *testing.T) {
	rel := streamOf("?x", Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)}).Materialize()
	it := rel.Iterator()
	require.True(t, it.Next())
	completeCache(it)
	require.NoError(t, it.Close())
	replay, err := CollectTuples(rel, nil)
	require.NoError(t, err, "the relation replays whole after its iterator was completed and closed")
	require.Len(t, replay, 3)

	plain := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, []Tuple{{int64(1)}, {int64(2)}}).Iterator()
	require.True(t, plain.Next())
	completeCache(plain)
	require.True(t, plain.Next(), "an iterator filling no cache is left where it stands")
	require.NoError(t, plain.Close())
}
