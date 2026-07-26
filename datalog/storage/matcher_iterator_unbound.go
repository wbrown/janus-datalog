package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// unboundIterator streams results for patterns without bindings
type unboundIterator struct {
	matcher     *PatternMatcher
	index       IndexType // reported in the scan statistics on Close
	opened      time.Time // scan open; Close reports the lifetime as the event's latency
	pattern     *query.DataPattern
	symbols     []query.Symbol
	e, a, v, tx interface{}
	constraints []executor.StorageConstraint

	storageIter  Iterator
	currentTuple executor.Tuple
	workspace    executor.Tuple // Reusable workspace for tuple building

	// Statistics tracking
	datomsScanned int
	datomsMatched int

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

		it.datomsScanned++

		// Check if datom matches pattern
		if it.matcher.matchesDatom(datom, it.e, it.a, it.v, it.tx) {
			// Apply transaction and constraint validation
			if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
				it.tupleBuilder.BuildTupleInternedInto(datom, it.workspace)
				it.currentTuple = it.workspace
				it.datomsMatched++
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
	// Emit scan statistics if handler is available
	emitIteratorStatistics(
		it.matcher.handler,
		annotations.PatternStorageScan,
		it.pattern,
		it.index,
		it.opened,
		it.datomsScanned,
		it.datomsMatched,
		nil, // no extra data
	)

	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}
