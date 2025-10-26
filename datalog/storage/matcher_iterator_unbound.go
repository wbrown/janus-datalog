package storage

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// unboundIterator streams results for patterns without bindings
type unboundIterator struct {
	matcher     *BadgerMatcher
	index       IndexType
	start, end  []byte
	pattern     *query.DataPattern
	columns     []query.Symbol
	e, a, v, tx interface{}
	constraints []executor.StorageConstraint

	storageIter  Iterator
	currentTuple executor.Tuple

	// Statistics tracking
	datomsScanned int
	datomsMatched int

	// Optimized tuple builder
	tupleBuilder *query.InternedTupleBuilder
}

func (it *unboundIterator) Next() bool {
	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			continue
		}

		it.datomsScanned++

		// Check if datom matches pattern
		if it.matcher.matchesDatom(datom, it.e, it.a, it.v, it.tx) {
			// Apply transaction and constraint validation
			if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
				it.currentTuple = it.tupleBuilder.BuildTupleInterned(datom)
				if it.currentTuple != nil {
					it.datomsMatched++
					return true
				}
			}
		}
	}

	return false
}

func (it *unboundIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *unboundIterator) Close() error {
	// Emit scan statistics if handler is available
	emitIteratorStatistics(
		it.matcher.handler,
		"pattern/storage-scan",
		it.pattern,
		it.index,
		it.datomsScanned,
		it.datomsMatched,
		nil, // no extra data
	)

	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

// unboundMaskIterator streams results using key mask filtering
type unboundMaskIterator struct {
	matcher     *BadgerMatcher
	index       IndexType
	start, end  []byte
	pattern     *query.DataPattern
	columns     []query.Symbol
	e, a, v, tx interface{}
	keyMask     *KeyMaskConstraint
	constraints []executor.StorageConstraint // For non-mask constraints

	storageIter  Iterator
	currentTuple executor.Tuple

	// Statistics tracking
	datomsScanned int
	datomsMatched int
	keysFiltered  int

	// Optimized tuple builder
	tupleBuilder *query.InternedTupleBuilder
}

func (it *unboundMaskIterator) Next() bool {
	// The KeyMaskIterator already handles the filtering
	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			continue
		}

		it.datomsScanned++

		// Check if datom matches pattern
		if it.matcher.matchesDatom(datom, it.e, it.a, it.v, it.tx) {
			// Apply transaction and constraint validation
			// NOTE: We must evaluate ALL constraints, not skip the ones that could be
			// converted to masks, because ScanKeysOnlyWithMask doesn't actually apply
			// the mask (it was disabled for performance reasons but the code path
			// remained - see badger_store.go:179)
			if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
				it.currentTuple = it.tupleBuilder.BuildTupleInterned(datom)
				if it.currentTuple != nil {
					it.datomsMatched++
					return true
				}
			}
		}
	}

	// Collect statistics from the key mask iterator
	if maskIter, ok := it.storageIter.(*KeyMaskIterator); ok {
		keysScanned, keysMatched, _ := maskIter.Stats()
		it.keysFiltered = keysScanned - keysMatched
	}

	return false
}

func (it *unboundMaskIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *unboundMaskIterator) Close() error {
	// Emit scan statistics if handler is available
	emitIteratorStatistics(
		it.matcher.handler,
		"pattern/storage-scan",
		it.pattern,
		it.index,
		it.datomsScanned,
		it.datomsMatched,
		map[string]interface{}{
			"keys.filtered": it.keysFiltered,
			"filter.type":   "key-mask",
		},
	)

	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}
