package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// nonReusingIterator opens a new scan for each binding tuple
type nonReusingIterator struct {
	matcher       *PatternMatcher
	pattern       *query.DataPattern
	bindingRel    executor.Relation
	bindingTuples []executor.Tuple
	symbols       []query.Symbol
	constraints   []executor.StorageConstraint

	// Iterator state
	currentIdx   int
	currentScan  Iterator
	currentTuple executor.Tuple
	workspace    executor.Tuple // Reusable workspace for tuple building
	totalScanned int
	totalMatched int
	err          error // First error from storage operations

	// Pattern extraction utility
	patternExtractor *query.PatternExtractor

	// Optimized tuple builder
	tupleBuilder *query.InternedTupleBuilder
}

func (it *nonReusingIterator) Next() bool {
	// If we have a current scan, check for more results
	if it.currentScan != nil {
		for it.currentScan.Next() {
			datom, err := it.currentScan.Datom()
			if err != nil {
				it.err = err
				return false
			}

			it.totalScanned++

			// Check if datom matches pattern with current binding
			if it.matchesWithBinding(datom, it.bindingTuples[it.currentIdx]) {
				// Apply transaction and constraint validation
				if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
					it.tupleBuilder.BuildTupleInternedInto(datom, it.workspace)
					it.currentTuple = it.workspace
					it.totalMatched++
					return true
				}
			}
		}

		// Done with current scan. The scan may have exhausted because of an
		// internal failure (CRDT unique-walk sub-scan, KeyOnly key decode)
		// that surfaces only through Error() after Next() returns false —
		// capture it before discarding so the query boundary sees it instead
		// of clean exhaustion. A Close() error is a weaker signal and must not
		// mask the iteration error. Abort on any error rather than scanning
		// further bindings.
		if scanErr := it.currentScan.Error(); scanErr != nil && it.err == nil {
			it.err = scanErr
		}
		if closeErr := it.currentScan.Close(); closeErr != nil && it.err == nil {
			it.err = closeErr
		}
		it.currentScan = nil
		if it.err != nil {
			return false
		}
	}

	// Move to next binding tuple
	it.currentIdx++
	if it.currentIdx >= len(it.bindingTuples) {
		return false
	}

	// Set up scan for new binding tuple
	bindingTuple := it.bindingTuples[it.currentIdx]
	e, a, v, tx := it.extractBoundValues(bindingTuple)

	// Choose index and create scan
	rawIter, err := it.matcher.reader.ScanKeysOnly(it.matcher.chooseIndex(e, a, v, tx))
	if err != nil {
		it.err = err
		return false
	}

	// Wrap with CRDT resolution unless in history mode
	if it.matcher.isHistoryMode() {
		it.currentScan = rawIter
	} else {
		it.currentScan = NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher.crdtTxID(), it.matcher)
	}

	// Try to find first match
	return it.Next()
}

func (it *nonReusingIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *nonReusingIterator) Close() error {
	if it.currentScan != nil {
		return it.currentScan.Close()
	}
	return nil
}

func (it *nonReusingIterator) Error() error { return it.err }

func (it *nonReusingIterator) extractBoundValues(bindingTuple executor.Tuple) (e, a, v, tx interface{}) {
	// Use the pattern extractor to get all bound values at once
	values := it.patternExtractor.Extract(bindingTuple)
	return values.E, values.A, values.V, values.T
}

func (it *nonReusingIterator) matchesWithBinding(datom *datalog.Datom, bindingTuple executor.Tuple) bool {
	e, a, v, tx := it.extractBoundValues(bindingTuple)
	return it.matcher.matchesDatom(datom, e, a, v, tx)
}
