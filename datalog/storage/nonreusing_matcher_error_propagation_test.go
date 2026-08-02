// Reproductions for BUG_NONREUSING_MATCHER_DROPS_ITERATOR_ERRORS.md
//
// The non-reusing matcher path (NoReuse strategy) has two iterator-consuming
// loops that, per the storage.Iterator contract, must check Error() after
// Next() returns false — a deferred failure (Tier-3 blob decode, KeyOnly key
// decode, CRDT unique-walk sub-scan) shows up only through Error(), not through
// a Datom() error or a non-empty Next().
//
//  1. matchWithoutIteratorReuse drains the binding relation into bindingTuples.
//  2. nonReusingIterator.Next consumes each per-binding storage scan.
//
// Both must surface a deferred error rather than launder it into a clean
// (empty/truncated) result.

package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// deferredErrorScan models the storage.Iterator deferred-failure contract:
// it yields nothing and parks an error in Error(), the way a wrapping iterator
// aborts (Next() == false) before emitting anything. The error is NOT returned
// from Datom() — nonReusingIterator already handles immediate Datom() errors;
// the loss site is specifically the deferred Error() channel.
type deferredErrorScan struct {
	remaining int
	scanned   int
	err       error
	closed    bool
}

func (it *deferredErrorScan) Next() bool {
	if it.remaining > 0 {
		it.remaining--
		it.scanned++
		return true
	}
	return false
}
func (it *deferredErrorScan) Datom() (*datalog.Datom, error) { return &datalog.Datom{}, nil }
func (it *deferredErrorScan) Close() error                   { it.closed = true; return nil }
func (it *deferredErrorScan) Seek(bound ScanBound) {
	panic("deferredErrorScan has no index to seek within")
}
func (it *deferredErrorScan) ElementID() datalog.ElementID { return datalog.ElementID{} }
func (it *deferredErrorScan) Error() error                 { return it.err }
func (it *deferredErrorScan) Scanned() int                 { return it.scanned }

var _ Iterator = (*deferredErrorScan)(nil)

// deferredErrorBindingRel is an executor.Relation whose iterator yields its
// tuples and then reports a deferred error via Error() after Next() returns
// false — modeling a storage-backed binding relation that aborts mid-scan and
// parks the failure in Error().
type deferredErrorBindingRel struct {
	executor.Relation
	err error
}

func (r deferredErrorBindingRel) Iterator() executor.Iterator {
	return &deferredErrorTupleIter{inner: r.Relation.Iterator(), err: r.err}
}

type deferredErrorTupleIter struct {
	inner   executor.Iterator
	err     error
	drained bool
}

func (it *deferredErrorTupleIter) Next() bool {
	if it.drained {
		return false
	}
	if it.inner.Next() {
		return true
	}
	it.drained = true // deferred failure: Next() == false, Error() reports it
	return false
}
func (it *deferredErrorTupleIter) Tuple() executor.Tuple { return it.inner.Tuple() }
func (it *deferredErrorTupleIter) Close() error          { return it.inner.Close() }
func (it *deferredErrorTupleIter) Error() error {
	if it.drained {
		return it.err
	}
	return it.inner.Error()
}

var (
	_ executor.Relation = deferredErrorBindingRel{}
	_ executor.Iterator = (*deferredErrorTupleIter)(nil)
)

// TestNonReusingIterator_SurfacesDeferredScanError pins loss site #2: a
// per-binding storage scan that exhausts (Next() == false) with a non-nil
// deferred Error() must be surfaced by the outer nonReusingIterator, not
// dropped when the scan is closed.
func TestNonReusingIterator_SurfacesDeferredScanError(t *testing.T) {
	sentinel := errors.New("deferred per-binding scan failure")

	// One binding tuple, its scan already open and about to exhaust with a
	// parked error. After the scan exhausts, currentIdx advances past the
	// single binding and Next() returns false — the only place the deferred
	// error can be captured is right before the scan is closed.
	it := &nonReusingIterator{
		currentScan:   &deferredErrorScan{err: sentinel},
		currentIdx:    0,
		bindingTuples: []executor.Tuple{{nil}},
	}

	require.False(t, it.Next(), "iterator should report exhaustion")
	require.ErrorIs(t, it.Error(), sentinel,
		"deferred per-binding scan error must surface via Error(), not be laundered into clean exhaustion")
}

// TestMatchWithoutIteratorReuse_SurfacesDeferredBindingError pins loss site #1:
// when the binding relation's iterator yields a prefix and then reports a
// deferred error, matchWithoutIteratorReuse must not launder it into a clean
// result. The error must surface either from the call itself or from draining
// the returned relation.
func TestMatchWithoutIteratorReuse_SurfacesDeferredBindingError(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{ReplicaID: 1})
			m := db.Matcher().(*PatternMatcher)

			sentinel := errors.New("deferred binding-relation scan failure")
			eSym := datalog.NewSymbol("?e")
			vSym := datalog.NewSymbol("?v")
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: eSym},
					query.Constant{Value: datalog.NewKeyword(":test/attr")},
					query.Variable{Name: vSym},
				},
			}
			symbols := pattern.Symbols()

			// Binding relation yields one tuple, then defers the error.
			base := executor.NewMaterializedRelation(
				[]query.Symbol{eSym},
				[]executor.Tuple{{datalog.NewIdentity("e1")}},
			)
			bindingRel := deferredErrorBindingRel{Relation: base, err: sentinel}

			rel, err := m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, nil)
			if err != nil {
				require.ErrorIs(t, err, sentinel)
				return
			}
			// Fix may instead surface the error through the returned relation's
			// iterator. Drive it to exhaustion via the contract-enforcing ForEach.
			drainErr := executor.ForEach(rel, func(executor.Tuple) error { return nil })
			require.ErrorIs(t, drainErr, sentinel,
				"deferred binding-relation error must surface, not be laundered into a clean result")
		})
	}
}
