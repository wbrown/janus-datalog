package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// nonReusingIterator opens a new scan for each binding tuple
type nonReusingIterator struct {
	matcher       *BadgerMatcher
	pattern       *query.DataPattern
	bindingRel    executor.Relation
	bindingTuples []executor.Tuple
	columns       []query.Symbol
	constraints   []executor.StorageConstraint

	// Iterator state
	currentIdx   int
	currentScan  Iterator
	currentTuple executor.Tuple
	totalScanned int
	totalMatched int

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
				continue
			}

			it.totalScanned++

			// Check if datom matches pattern with current binding
			if it.matchesWithBinding(datom, it.bindingTuples[it.currentIdx]) {
				// Apply transaction and constraint validation
				if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
					it.currentTuple = it.tupleBuilder.BuildTupleInterned(datom)
					it.totalMatched++
					return true
				}
			}
		}

		// Done with current scan
		it.currentScan.Close()
		it.currentScan = nil
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
	index, start, end := it.matcher.chooseIndex(e, a, v, tx)

	var err error
	it.currentScan, err = it.matcher.store.ScanKeysOnly(index, start, end)
	if err != nil {
		return false
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

func (it *nonReusingIterator) extractBoundValues(bindingTuple executor.Tuple) (e, a, v, tx interface{}) {
	// Use the pattern extractor to get all bound values at once
	values := it.patternExtractor.Extract(bindingTuple)
	return values.E, values.A, values.V, values.T
}

func (it *nonReusingIterator) matchesWithBinding(datom *datalog.Datom, bindingTuple executor.Tuple) bool {
	e, a, v, tx := it.extractBoundValues(bindingTuple)
	return it.matcher.matchesDatom(datom, e, a, v, tx)
}
