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
