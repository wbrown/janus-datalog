package storage

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// unboundIterator streams results for patterns without bindings
type unboundIterator struct {
	matcher *PatternMatcher
	// report accounts for the scan this iterator reads. It was acquired through
	// the report, so the run, the clock and the intake are already in it; the
	// arm adds what it made of what it read and closes it.
	report      *scanReport
	pattern     *query.DataPattern
	symbols     []query.Symbol
	e, a, v, tx interface{}
	constraints []executor.StorageConstraint

	storageIter  Iterator
	currentTuple executor.Tuple
	workspace    executor.Tuple // Reusable workspace for tuple building

	// Optimized tuple builder
	tupleBuilder *query.InternedTupleBuilder

	// CRDT cardinality-one support: stop after first result
	returnOnlyFirst bool
	foundFirst      bool

	err error // First error from storage operations
}

func (it *unboundIterator) Next() bool {
	// CRDT cardinality-one: if we already found the first result, stop
	if it.returnOnlyFirst && it.foundFirst {
		return false
	}

	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			it.err = err
			return false
		}

		if it.report != nil {
			it.report.resolved++
		}

		// Check if datom matches pattern
		if it.matcher.matchesDatom(datom, it.e, it.a, it.v, it.tx) {
			// Apply transaction and constraint validation
			if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
				it.tupleBuilder.BuildTupleInternedInto(datom, it.workspace)
				it.currentTuple = it.workspace
				if it.report != nil {
					it.report.matched++
				}
				it.foundFirst = true
				return true
			}
		}
	}

	// Storage iterator exhausted — propagate any deferred error so
	// callers observing Error() see failures that occurred inside
	// the inner iterator (e.g., CRDTResolvingIterator unique-walk
	// sub-scan failures that cause Next() to return false).
	if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
		it.err = srcErr
	}
	return false
}

func (it *unboundIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *unboundIterator) Error() error { return it.err }

func (it *unboundIterator) Close() error {
	// The storage close comes first: the scan accrues its intake as it closes,
	// and the report is what reads that.
	var closeErr error
	if it.storageIter != nil {
		closeErr = it.storageIter.Close()
	}
	if it.report != nil {
		it.report.close(it.err == nil)
	}
	return closeErr
}
