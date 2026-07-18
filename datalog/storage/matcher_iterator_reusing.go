package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// reusingIterator implements iterator reuse optimization
type reusingIterator struct {
	matcher     *BadgerMatcher
	pattern     *query.DataPattern
	bindingRel  executor.Relation
	tuples      []executor.Tuple
	position    int       // Which position is changing (0=E, 1=A, 2=V)
	index       IndexType // Which index to use
	symbols     []query.Symbol
	constraints []executor.StorageConstraint

	// Iterator state
	storageIter  Iterator       // The BadgerDB iterator we're reusing
	currentIdx   int            // Current tuple index
	currentTuple executor.Tuple // Current result tuple
	workspace    executor.Tuple // Reusable workspace for tuple building

	// Cached bound values for current tuple to avoid recreating pattern
	currentE, currentA, currentV, currentTx interface{}

	// Pattern extraction utility
	patternExtractor *query.PatternExtractor

	// Optimized tuple builder
	tupleBuilder *query.InternedTupleBuilder

	// Performance tracking
	datomsScanned int // Total datoms scanned
	datomsMatched int // Total datoms matched

	err error // First error from storage operations
}

func (it *reusingIterator) Next() bool {
	// NOTE: We removed the foundForTuple logic because it was wrong!
	// We need to find ALL matches for each binding value, not just one.

	// First call - initialize
	if it.currentIdx < 0 {
		it.currentIdx = 0
		if it.currentIdx >= len(it.tuples) {
			return false
		}

		// Calculate range that encompasses all tuples
		// Start with the first tuple's seek key
		firstTuple := it.tuples[0]
		it.updateBoundPattern(firstTuple)
		index, startKey, _ := it.calculateSeekKey(firstTuple)
		// The scan decodes with the same index the keys were encoded for.
		it.index = index

		// For the end key, we need to go past the last tuple
		// The safest approach is to use a key that's definitely past all our data
		// For EAVT index with E bound, we can use the last entity + max suffix
		lastTuple := it.tuples[len(it.tuples)-1]
		it.updateBoundPattern(lastTuple)
		_, _, endKey := it.calculateSeekKey(lastTuple)
		// Extend the end key to ensure we capture all datoms for the last entity
		endKey = append(endKey, 0xFF, 0xFF, 0xFF, 0xFF)

		var err error
		rawIter, err := it.matcher.store.ScanKeysOnly(it.index, startKey, endKey)
		if err != nil {
			it.err = err
			return false
		}

		// Wrap with CRDT resolution unless in history mode
		if it.matcher.isHistoryMode() {
			it.storageIter = rawIter
		} else {
			it.storageIter = NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher.crdtTxID(), it.matcher)
		}

		// Reset to first tuple for actual processing
		it.updateBoundPattern(firstTuple)

		// Iterator is now positioned before the first item
		// The main loop will call Next() to get the first datom
	}

	for {
		// Try to get more datoms from current position
		if it.storageIter != nil && it.currentIdx >= 0 && it.currentIdx < len(it.tuples) {
			// Now that BadgerIterator.Seek() is fixed, Next() will correctly position us
			hasNext := it.storageIter.Next()
			for hasNext {
				datom, err := it.storageIter.Datom()
				if err != nil {
					it.err = err
					return false
				}

				// Track datom scan
				it.datomsScanned++

				// Check if datom matches the current binding pattern
				if it.matchesCurrentPattern(datom) {
					// Apply transaction and constraint validation
					if validateDatomWithConstraints(datom, it.matcher.txID, it.constraints) {
						it.datomsMatched++
						it.tupleBuilder.BuildTupleInternedInto(datom, it.workspace)
						it.currentTuple = it.workspace
						// Found a match!
						return true
					}
				}

				// Check if we've moved past the current binding value based on index type
				// If we have, we should immediately move to the next binding
				movedPast := false
				bindingTuple := it.tuples[it.currentIdx]
				if len(bindingTuple) > 0 {
					switch it.position {
					case 0: // Entity bound
						// Check which index we're using
						if it.index == 1 { // AEVT index (A is primary, E is secondary)
							// Pattern: [?e :constant-attr ?v] with AEVT index
							// Index order: Attribute + Entity + Value + Tx
							// We're looking for specific (A, E) pairs
							if expectedE, ok := bindingTuple[0].(datalog.Identity); ok {
								if !datom.E.Equal(expectedE) {
									// Different entity - moved past this binding
									movedPast = true
								}
							}
						} else { // EAVT index (E is primary, A is secondary)
							if expectedE, ok := bindingTuple[0].(datalog.Identity); ok {
								if !datom.E.Equal(expectedE) {
									// We've moved to a different entity - definitely past our target
									movedPast = true
								} else if c, ok := it.pattern.GetA().(query.Constant); ok {
									// Same entity but checking attribute
									if targetKw, ok := c.Value.(datalog.Keyword); ok {
										// Check if we've passed the target attribute in sort order
										if datom.A.String() > targetKw.String() {
											// We've passed it - it doesn't exist for this entity
											movedPast = true
										}
									}
								}
							}
						}

					case 1: // Attribute bound (AEVT index)
						// AEVT: Attribute + Entity + Value + Tx
						// When attribute changes, we're past this binding
						if expectedA, ok := bindingTuple[0].(datalog.Keyword); ok {
							if datom.A.String() != expectedA.String() {
								movedPast = true
							}
						}

					case 2: // Value bound (AVET or VAET index)
						// For AVET: Attribute + Value + Entity + Tx
						// For VAET: Value + Attribute + Entity + Tx
						expectedV := bindingTuple[0]

						// For AVET, also check if we're still on the right attribute
						if it.index == 2 { // AVET
							if c, ok := it.pattern.GetA().(query.Constant); ok {
								if targetKw, ok := c.Value.(datalog.Keyword); ok {
									if datom.A.String() != targetKw.String() {
										// Wrong attribute - we've gone too far
										movedPast = true
										break
									}
								}
							}
						}

						// Check if value still matches
						if !it.matcher.valuesEqual(datom.V, expectedV) {
							movedPast = true
						}

					case 3: // Transaction bound (TAEV index)
						// TAEV: Tx + Attribute + Entity + Value
						// When transaction changes, we're past this binding
						if eid, ok := datalog.DerefElementID(bindingTuple[0]); ok {
							if datom.Tx != eid {
								movedPast = true
							}
						} else if expectedTx, ok := bindingTuple[0].(uint64); ok {
							if datom.Tx.Lamport != expectedTx {
								movedPast = true
							}
						}
					}
				}

				// If we've moved past the current binding, immediately seek to next
				if movedPast {
					break // Exit the inner loop to move to next binding
				}

				// Continue to next datom
				hasNext = it.storageIter.Next()
			}
		}

		// Move to the next binding tuple
		it.currentIdx++
		if it.currentIdx >= len(it.tuples) {
			if it.storageIter != nil {
				// Propagate any deferred error before discarding the inner
				// iterator (e.g., a CRDTResolvingIterator decode/blob failure
				// that made Next() return false rather than erroring in Datom()).
				if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
					it.err = srcErr
				}
				it.storageIter.Close()
			}
			return false
		}

		// Get the binding tuple and prepare for matching
		bindingTuple := it.tuples[it.currentIdx]
		it.updateBoundPattern(bindingTuple)

		// Calculate seek key based on the binding
		_, seekKey, _ := it.calculateSeekKey(bindingTuple)

		// Seek to the new position
		it.storageIter.Seek(seekKey)
		// Loop back to try reading from the new position
	}
}

func (it *reusingIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *reusingIterator) Close() error {
	// Emit scan statistics if handler is available
	emitIteratorStatistics(
		it.matcher.handler,
		"pattern/iterator-reuse-complete",
		it.pattern,
		it.index,
		it.datomsScanned,
		it.datomsMatched,
		map[string]interface{}{
			"binding.size": len(it.tuples),
			"strategy":     "iterator-reuse",
		},
	)

	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

func (it *reusingIterator) Error() error { return it.err }

// getSymbolIndex returns the index of a symbol in the binding relation symbols
func (it *reusingIterator) getSymbolIndex(variable query.Variable) int {
	symbols := it.bindingRel.Symbols()
	for i, sym := range symbols {
		if sym == variable.Name {
			return i
		}
	}
	return -1
}

// updateBoundPattern updates the cached bound values based on current binding tuple
func (it *reusingIterator) updateBoundPattern(bindingTuple executor.Tuple) {
	// Use the pattern extractor to get all bound values at once
	values := it.patternExtractor.Extract(bindingTuple)
	it.currentE = values.E
	it.currentA = values.A
	it.currentV = values.V
	it.currentTx = values.T
}

// calculateSeekKey calculates the index and key range to seek to based on the
// binding tuple and position. The returned index is authoritative for the
// scan: the keys are encoded for it, and the storage iterator must decode
// with it. (The strategy's index is a planning hint; chooseIndex is the one
// authority for key encoding, so the two must not be mixed — a range encoded
// for one index layout decodes as garbage under another.)
func (it *reusingIterator) calculateSeekKey(bindingTuple executor.Tuple) (IndexType, []byte, []byte) {
	// Get symbol mapping
	symbols := it.bindingRel.Symbols()
	symIndex := make(map[query.Symbol]int)
	for i, sym := range symbols {
		symIndex[sym] = i
	}

	// Extract values based on pattern
	var e, a, v, tx interface{}

	// Get E value
	if c, ok := it.pattern.GetE().(query.Constant); ok {
		e = c.Value
	} else if sym, ok := it.pattern.GetE().(query.Variable); ok {
		if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
			e = bindingTuple[idx]
		}
	}

	// Get A value - but DON'T bind it from the tuple!
	// The attribute is constant in the pattern: :product/price
	if c, ok := it.pattern.GetA().(query.Constant); ok {
		a = c.Value
	}
	// Don't bind A from tuple - it's not a variable in our pattern!

	// Get V value
	if c, ok := it.pattern.GetV().(query.Constant); ok {
		v = c.Value
	} else if sym, ok := it.pattern.GetV().(query.Variable); ok {
		if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
			v = bindingTuple[idx]
		}
	}

	// Get T value
	if len(it.pattern.Elements) > 3 {
		if c, ok := it.pattern.GetT().(query.Constant); ok {
			tx = c.Value
		} else if sym, ok := it.pattern.GetT().(query.Variable); ok {
			if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
				tx = bindingTuple[idx]
			}
		}
	}

	// When the strategy chose TAEV (position 3), only pass tx to avoid
	// chooseIndex selecting AETV due to a constant A in the pattern.
	if it.position == 3 && it.index == TAEV {
		index, start, end := it.matcher.chooseIndex(nil, nil, nil, tx)
		return index, start, end
	}
	index, start, end := it.matcher.chooseIndex(e, a, v, tx)
	return index, start, end
}

// matchesCurrentPattern checks if datom matches with current binding
func (it *reusingIterator) matchesCurrentPattern(datom *datalog.Datom) bool {
	// Get current binding tuple
	if it.currentIdx < 0 || it.currentIdx >= len(it.tuples) {
		return false
	}

	// Use cached bound values instead of recreating pattern
	return it.matcher.matchesDatom(datom, it.currentE, it.currentA, it.currentV, it.currentTx)
}
