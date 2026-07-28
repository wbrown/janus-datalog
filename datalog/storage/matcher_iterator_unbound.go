package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// unboundIterator streams results for patterns without bindings
type unboundIterator struct {
	matcher *PatternMatcher
	// bound is the run this scan walks, kept whole rather than as its index
	// alone: Close reports it, and a scan line that named only the index would
	// leave the reader to find the bound on a different event — which is the
	// state the formatter used to carry and could not carry correctly once the
	// engine emitted from parallel workers.
	bound       ScanBound
	opened      time.Time // scan open; Close reports the lifetime as the event's latency
	pattern     *query.DataPattern
	symbols     []query.Symbol
	e, a, v, tx interface{}
	constraints []executor.StorageConstraint

	storageIter  Iterator
	currentTuple executor.Tuple
	workspace    executor.Tuple // Reusable workspace for tuple building

	// Statistics tracking
	datomsResolved int
	datomsMatched  int

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

		it.datomsResolved++

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
	if it.matcher.handler != nil {
		// storageIter is assigned after the literal, so match the nil stance
		// the Close below already takes rather than assume it is always set.
		scanned := 0
		if it.storageIter != nil {
			scanned = it.storageIter.Scanned()
		}
		// The run travels with the cost. addBoundFields writes the index, the
		// positions the run binds and their values, which is everything the
		// scan line needs — so the line is rendered from this event alone.
		run := map[string]interface{}{}
		addBoundFields(run, it.bound)
		emitScanCompletion(
			it.matcher.handler,
			annotations.PatternStorageScan,
			it.pattern,
			it.opened,
			scanFunnel{
				scanned:  scanned,
				resolved: it.datomsResolved,
				matched:  it.datomsMatched,
			},
			run,
		)
	}

	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}
