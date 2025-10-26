package executor

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Note: EnableSymmetricHashJoin is now managed by ExecutorOptions

// SymmetricHashJoin performs a symmetric hash join between two potentially streaming relations.
// Unlike standard hash join which builds one side completely then probes with the other,
// symmetric hash join processes tuples from both sides incrementally, maintaining hash tables
// for both and emitting results as matches are found.
//
// Algorithm:
// 1. Read a batch of tuples from left
// 2. For each left tuple:
//   - Probe right hash table for matches, emit results
//   - Add to left hash table
//
// 3. Read a batch of tuples from right
// 4. For each right tuple:
//   - Probe left hash table for matches, emit results
//   - Add to right hash table
//
// 5. Repeat until both iterators exhausted
//
// This allows both relations to be streaming without materializing either completely.
func SymmetricHashJoin(left, right Relation, joinCols []query.Symbol) Relation {
	// Try to get options from either relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}
	return SymmetricHashJoinWithOptions(left, right, joinCols, opts)
}

// SymmetricHashJoinWithOptions performs a streaming symmetric hash join with explicit options
func SymmetricHashJoinWithOptions(left, right Relation, joinCols []query.Symbol, opts ExecutorOptions) Relation {
	// Build column mappings
	leftIndices := make([]int, len(joinCols))
	rightIndices := make([]int, len(joinCols))
	for i, col := range joinCols {
		leftIndices[i] = ColumnIndex(left, col)
		rightIndices[i] = ColumnIndex(right, col)
		if leftIndices[i] < 0 || rightIndices[i] < 0 {
			// Join column not found
			return NewMaterializedRelation(nil, nil)
		}
	}

	// Determine output columns (union without duplicates)
	outputCols := append([]query.Symbol{}, left.Columns()...)
	rightColSet := make(map[query.Symbol]bool)
	for _, col := range joinCols {
		rightColSet[col] = true
	}

	// Add right columns that aren't in join columns
	for _, col := range right.Columns() {
		if !rightColSet[col] {
			outputCols = append(outputCols, col)
		}
	}

	// Create the symmetric hash join iterator
	iter := &symmetricHashJoinIterator{
		leftIt:       left.Iterator(),
		rightIt:      right.Iterator(),
		leftTable:    NewTupleKeyMapWithCapacity(1000), // Start with reasonable size
		rightTable:   NewTupleKeyMapWithCapacity(1000),
		leftIndices:  leftIndices,
		rightIndices: rightIndices,
		joinCols:     joinCols,
		leftCols:     left.Columns(),
		rightCols:    right.Columns(),
		outputCols:   outputCols,
		resultQueue:  make([]Tuple, 0),
		seen:         NewTupleKeyMapWithCapacity(1000),
		batchSize:    100, // Process tuples in batches for efficiency
	}

	// Return a streaming relation with the symmetric join iterator
	return NewStreamingRelationWithOptions(outputCols, iter, opts)
}

// symmetricHashJoinIterator implements Iterator for symmetric hash join
type symmetricHashJoinIterator struct {
	leftIt, rightIt           Iterator
	leftTable, rightTable     *TupleKeyMap
	leftIndices, rightIndices []int
	joinCols                  []query.Symbol
	leftCols, rightCols       []query.Symbol
	outputCols                []query.Symbol
	resultQueue               []Tuple
	seen                      *TupleKeyMap // For deduplication
	leftDone, rightDone       bool
	batchSize                 int
	resultPos                 int
}

// Next advances to the next result tuple
func (it *symmetricHashJoinIterator) Next() bool {
	// If we have queued results, return them first
	if it.resultPos < len(it.resultQueue) {
		return true
	}

	// Clear the result queue for new results
	it.resultQueue = it.resultQueue[:0]
	it.resultPos = 0

	// Process batches from both sides until we get results or both are exhausted
	for len(it.resultQueue) == 0 && (!it.leftDone || !it.rightDone) {
		// Process a batch from left
		if !it.leftDone {
			it.processLeftBatch()
		}

		// Process a batch from right
		if !it.rightDone {
			it.processRightBatch()
		}
	}

	return len(it.resultQueue) > 0
}

// processLeftBatch processes a batch of tuples from the left iterator
func (it *symmetricHashJoinIterator) processLeftBatch() {
	processed := 0
	for processed < it.batchSize && it.leftIt.Next() {
		leftTuple := it.leftIt.Tuple()

		// Extract join key from left tuple
		key := NewTupleKey(leftTuple, it.leftIndices)

		// Probe right table for matches
		if rightMatchesVal, ok := it.rightTable.Get(key); ok {
			rightMatches := rightMatchesVal.([]Tuple)
			for _, rightTuple := range rightMatches {
				// Combine tuples
				joined := it.combineTuples(leftTuple, rightTuple, true)

				// Deduplicate
				dedupKey := NewTupleKeyFull(joined)
				if !it.seen.Exists(dedupKey) {
					it.seen.Put(dedupKey, true)
					it.resultQueue = append(it.resultQueue, joined)
				}
			}
		}

		// Add to left table
		if existingVal, ok := it.leftTable.Get(key); ok {
			existing := existingVal.([]Tuple)
			it.leftTable.Put(key, append(existing, leftTuple))
		} else {
			it.leftTable.Put(key, []Tuple{leftTuple})
		}

		processed++
	}

	// Check if left is exhausted
	if processed < it.batchSize {
		it.leftDone = true
	}
}

// processRightBatch processes a batch of tuples from the right iterator
func (it *symmetricHashJoinIterator) processRightBatch() {
	processed := 0
	for processed < it.batchSize && it.rightIt.Next() {
		rightTuple := it.rightIt.Tuple()

		// Extract join key from right tuple
		key := NewTupleKey(rightTuple, it.rightIndices)

		// Probe left table for matches
		if leftMatchesVal, ok := it.leftTable.Get(key); ok {
			leftMatches := leftMatchesVal.([]Tuple)
			for _, leftTuple := range leftMatches {
				// Combine tuples
				joined := it.combineTuples(leftTuple, rightTuple, true)

				// Deduplicate
				dedupKey := NewTupleKeyFull(joined)
				if !it.seen.Exists(dedupKey) {
					it.seen.Put(dedupKey, true)
					it.resultQueue = append(it.resultQueue, joined)
				}
			}
		}

		// Add to right table
		if existingVal, ok := it.rightTable.Get(key); ok {
			existing := existingVal.([]Tuple)
			it.rightTable.Put(key, append(existing, rightTuple))
		} else {
			it.rightTable.Put(key, []Tuple{rightTuple})
		}

		processed++
	}

	// Check if right is exhausted
	if processed < it.batchSize {
		it.rightDone = true
	}
}

// combineTuples combines left and right tuples, avoiding duplication of join columns
func (it *symmetricHashJoinIterator) combineTuples(leftTuple, rightTuple Tuple, leftFirst bool) Tuple {
	// Start with all columns from left
	result := make(Tuple, len(it.outputCols))
	copy(result, leftTuple)

	// Add non-join columns from right
	rightOffset := len(leftTuple)
	rightColIndex := 0
	for i, col := range it.rightCols {
		// Skip join columns
		isJoinCol := false
		for _, joinCol := range it.joinCols {
			if col == joinCol {
				isJoinCol = true
				break
			}
		}

		if !isJoinCol {
			if rightOffset+rightColIndex < len(result) {
				result[rightOffset+rightColIndex] = rightTuple[i]
				rightColIndex++
			}
		}
	}

	return result
}

// Tuple returns the current result tuple
func (it *symmetricHashJoinIterator) Tuple() Tuple {
	if it.resultPos < len(it.resultQueue) {
		tuple := it.resultQueue[it.resultPos]
		it.resultPos++
		return tuple
	}
	return nil
}

// Close releases resources
func (it *symmetricHashJoinIterator) Close() error {
	var err1, err2 error
	if it.leftIt != nil {
		err1 = it.leftIt.Close()
	}
	if it.rightIt != nil {
		err2 = it.rightIt.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// ChooseJoinStrategy selects the appropriate join strategy based on relation types
func ChooseJoinStrategy(left, right Relation, joinCols []query.Symbol, opts ExecutorOptions) string {
	leftStreaming := isStreaming(left)
	rightStreaming := isStreaming(right)

	if opts.EnableSymmetricHashJoin && leftStreaming && rightStreaming {
		// Both streaming - use symmetric hash join
		return "symmetric"
	} else if leftStreaming != rightStreaming {
		// One streaming, one materialized - use standard hash join
		// with materialized side as build
		return "asymmetric"
	} else {
		// Both materialized - use standard size-based optimization
		return "standard"
	}
}
