package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Reproductions for BUG_SEMI_ANTI_JOIN_DROP_ITERATOR_ERRORS.
//
// SemiJoin and AntiJoin consume both inputs but never check Iterator.Error() or
// Close(). A side that fails after a prefix yields a clean MaterializedRelation
// built from partial data — especially dangerous for AntiJoin, where a missing
// right-side key from a decode failure is indistinguishable from a real "no
// match". Both must surface the first iterator error on the result.

func TestSemiJoin_SurfacesRightIteratorError(t *testing.T) {
	left := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	right := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})
	joined := SemiJoin(left, right, testSymbols())
	require.ErrorIs(t, driveErr(joined), errInjectedIterator)
}

func TestSemiJoin_SurfacesLeftIteratorError(t *testing.T) {
	left := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})
	right := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	joined := SemiJoin(left, right, testSymbols())
	require.ErrorIs(t, driveErr(joined), errInjectedIterator)
}

func TestAntiJoin_SurfacesRightIteratorError(t *testing.T) {
	left := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	right := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})
	joined := AntiJoin(left, right, testSymbols())
	require.ErrorIs(t, driveErr(joined), errInjectedIterator)
}

func TestAntiJoin_SurfacesLeftIteratorError(t *testing.T) {
	left := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})
	right := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	joined := AntiJoin(left, right, testSymbols())
	require.ErrorIs(t, driveErr(joined), errInjectedIterator)
}
