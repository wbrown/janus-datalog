package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Reproductions for docs/bugs/BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md
//
// The Iterator contract requires callers to check Error() after Next() returns
// false. CollectTuples (and the QueryInto/QueryOneInto storage boundaries) don't,
// so an iterator failure is reported as an empty or truncated success.

var errInjectedIterator = errors.New("injected iterator failure")

// failingRelation behaves like its embedded Relation except that its iterator
// stops after failAfter tuples and reports errInjectedIterator via Error() —
// mimicking KeyOnlyIterator/CRDTResolvingIterator deferring failures to Error().
type failingRelation struct {
	Relation
	failAfter int
	closeErr  error
}

func (f failingRelation) Iterator() Iterator {
	return &failingIterator{inner: f.Relation.Iterator(), failAfter: f.failAfter, closeErr: f.closeErr}
}

type failingIterator struct {
	inner     Iterator
	failAfter int
	yielded   int
	failed    bool
	closeErr  error // if set, Close() returns this (to test Close vs iteration error precedence)
}

func (it *failingIterator) Next() bool {
	if it.failed {
		return false
	}
	if it.yielded >= it.failAfter {
		it.failed = true // deferred failure: Next() returns false, Error() reports it
		return false
	}
	if it.inner.Next() {
		it.yielded++
		return true
	}
	return false
}

func (it *failingIterator) Tuple() Tuple { return it.inner.Tuple() }

func (it *failingIterator) Close() error {
	_ = it.inner.Close()
	return it.closeErr
}

func (it *failingIterator) Error() error {
	if it.failed {
		return errInjectedIterator
	}
	return it.inner.Error()
}

func testSymbols() []query.Symbol { return []query.Symbol{datalog.NewSymbol("?x")} }

func newFailingRelation(failAfter int, tuples ...Tuple) failingRelation {
	return failingRelation{
		Relation:  NewMaterializedRelation(testSymbols(), tuples),
		failAfter: failAfter,
	}
}

// newFailingStream wraps a failing iterator in a real StreamingRelation, so
// that transforms which consume via Iterator() (Materialize, Sort, Aggregate)
// run the real collectTuplesInto path against a failing source.
func newFailingStream(failAfter int, tuples ...Tuple) *StreamingRelation {
	inner := NewMaterializedRelation(testSymbols(), tuples).Iterator()
	return NewStreamingRelation(testSymbols(), &failingIterator{inner: inner, failAfter: failAfter})
}

// driveErr drives a relation's iterator to exhaustion and returns Error().
func driveErr(rel Relation) error {
	it := rel.Iterator()
	defer it.Close()
	for it.Next() {
	}
	return it.Error()
}

// TestCollectTuples_ReturnsIteratorError: an iterator that fails immediately must
// not be reported as an empty successful result.
func TestCollectTuples_ReturnsIteratorError(t *testing.T) {
	rel := newFailingRelation(0, Tuple{int64(1)}, Tuple{int64(2)})
	_, err := CollectTuples(rel, nil)
	require.ErrorIs(t, err, errInjectedIterator)
}

// TestCollectTuples_ReturnsIteratorErrorAfterPartialResults: an iterator that
// fails partway must not be reported as a truncated successful result.
func TestCollectTuples_ReturnsIteratorErrorAfterPartialResults(t *testing.T) {
	rel := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	_, err := CollectTuples(rel, nil)
	require.ErrorIs(t, err, errInjectedIterator)
}

// The pins below guard the 2026-07 error-swallow sweep fixes
// (docs/bugs/BUG_ERROR_SWALLOW_SWEEP_2026_07.md): each transform must carry a
// failed scan as its result's deferred error (or return it), never present it
// as a clean result.

func TestCrossProductCarriesScanErrors(t *testing.T) {
	good := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?y")}, []Tuple{{int64(9)}})
	t.Run("left fails", func(t *testing.T) {
		rel := crossProduct(newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)}), good)
		require.ErrorIs(t, driveErr(rel), errInjectedIterator)
	})
	t.Run("right fails", func(t *testing.T) {
		rel := crossProduct(good, newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)}))
		require.ErrorIs(t, driveErr(rel), errInjectedIterator)
	})
}

func TestSelectCarriesScanError(t *testing.T) {
	rel := Select(newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)}), func(Tuple) bool { return true })
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

func TestPrependedRelationMaterializeCarriesScanError(t *testing.T) {
	rest := newFailingRelation(1, Tuple{int64(2)}, Tuple{int64(3)})
	rel := NewPrependedRelation(testSymbols(), Tuple{int64(1)}, rest, ExecutorOptions{})
	require.ErrorIs(t, driveErr(rel.Materialize()), errInjectedIterator)
}

func TestBufferedIteratorCloneCarriesSourceError(t *testing.T) {
	inner := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}}).Iterator()
	src := &failingIterator{inner: inner, failAfter: 1}
	buf := NewBufferedIterator(src)

	clone := buf.Clone()
	for clone.Next() {
	}
	require.ErrorIs(t, clone.Error(), errInjectedIterator)
	require.ErrorIs(t, buf.Error(), errInjectedIterator)
}

func TestExtractEntityIDsSurfacesScanError(t *testing.T) {
	alice := datalog.NewIdentity("user:alice")
	rel := failingRelation{
		Relation:  NewMaterializedRelation(testSymbols(), []Tuple{{alice}, {alice}}),
		failAfter: 1,
	}
	_, err := extractEntityIDs(rel, testSymbols())
	require.ErrorIs(t, err, errInjectedIterator)
}
