package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// Note: Join settings are now managed by ExecutorOptions in options.go

// hashJoinIterator implements streaming hash join execution
//
// CONCURRENCY: This iterator is NOT thread-safe. It maintains mutable state
// (currentProbeTuple, currentJoined, matches, matchIdx) that would be corrupted
// by concurrent access. Each goroutine must create its own iterator by calling
// Relation.Iterator(), which returns independent iterator instances.
type hashJoinIterator struct {
	hashTable    *TupleKeyMap
	probeIt      Iterator
	seen         *TupleKeyMap
	buildIsLeft  bool
	joinCols     []query.Symbol
	leftCols     []query.Symbol
	rightCols    []query.Symbol
	probeIndices []int
	options      ExecutorOptions

	// Current state - NOT safe for concurrent access
	currentProbeTuple Tuple
	currentJoined     Tuple
	matches           []Tuple
	matchIdx          int
	closed            bool

	// Debug counters
	probeCount  int
	matchCount  int
	resultCount int
}

func (it *hashJoinIterator) Next() bool {
	if it.closed {
		return false
	}

	for {
		// If we have matches for current probe tuple, iterate through them
		if it.matchIdx < len(it.matches) {
			buildTuple := it.matches[it.matchIdx]
			it.matchIdx++

			// Combine tuples
			var joined Tuple
			if it.buildIsLeft {
				joined = combineTuples(buildTuple, it.currentProbeTuple, it.joinCols, it.leftCols, it.rightCols)
			} else {
				joined = combineTuples(it.currentProbeTuple, buildTuple, it.joinCols, it.leftCols, it.rightCols)
			}

			// Check for duplicates using seen map
			dedupKey := NewTupleKeyFull(joined)
			if !it.seen.Exists(dedupKey) {
				it.seen.Put(dedupKey, true)
				// BUG FIX: Make a copy since combineTuples might return a slice that gets reused
				joinedCopy := make(Tuple, len(joined))
				copy(joinedCopy, joined)
				it.currentJoined = joinedCopy // Store for Tuple() to return
				it.resultCount++
				return true
			}
			// Duplicate, continue to next match
			continue
		}

		// Need next probe tuple
		if !it.probeIt.Next() {
			if it.options.EnableDebugLogging {
				fmt.Printf("[hashJoinIterator] Probe exhausted after %d tuples, %d matched, produced %d results\n",
					it.probeCount, it.matchCount, it.resultCount)
			}
			return false
		}

		it.probeCount++
		it.currentProbeTuple = it.probeIt.Tuple()

		// BUG FIX: Make a copy of the tuple since the probe iterator might reuse the slice
		tupleCopy := make(Tuple, len(it.currentProbeTuple))
		copy(tupleCopy, it.currentProbeTuple)
		it.currentProbeTuple = tupleCopy

		key := NewTupleKey(it.currentProbeTuple, it.probeIndices)

		if it.options.EnableDebugLogging && it.probeCount == 1 {
			fmt.Printf("[hashJoinIterator] First probe key: %v\n", key)
		}

		// Look up matches in hash table
		if matchesVal, ok := it.hashTable.Get(key); ok {
			it.matches = matchesVal.([]Tuple)
			it.matchIdx = 0
			it.matchCount++
			continue
		}

		// No matches, continue to next probe tuple
	}
}

func (it *hashJoinIterator) Tuple() Tuple {
	return it.currentJoined
}

func (it *hashJoinIterator) Close() error {
	if !it.closed {
		it.closed = true
		if it.probeIt != nil {
			return it.probeIt.Close()
		}
	}
	return nil
}

// HashJoin performs a hash join on specified columns
// It attempts to get options from the input relations
func HashJoin(left, right Relation, joinCols []query.Symbol) Relation {
	// Try to get options from either relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}
	return HashJoinWithOptions(left, right, joinCols, opts)
}

// HashJoinWithOptions performs a hash join with explicit options
func HashJoinWithOptions(left, right Relation, joinCols []query.Symbol, opts ExecutorOptions) Relation {
	// Default capacity for unknown sizes (-1)
	const defaultCapacity = 1000

	// Check if we should use symmetric hash join for streaming relations
	if opts.EnableSymmetricHashJoin {
		strategy := ChooseJoinStrategy(left, right, joinCols, opts)
		if strategy == "symmetric" {
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Using symmetric hash join for streaming-to-streaming\n")
			}
			return SymmetricHashJoinWithOptions(left, right, joinCols, opts)
		}
	}

	if opts.EnableDebugLogging {
		// Be careful with Size() calls - they force materialization!
		leftSize := -1
		rightSize := -1
		if !isStreaming(left) {
			leftSize = left.Size()
		}
		if !isStreaming(right) {
			rightSize = right.Size()
		}
		if opts.EnableDebugLogging {
			fmt.Printf("[HashJoin] Called with left (type=%T, size=%d), right (type=%T, size=%d), joinCols=%v, EnableStreamingJoins=%v\n",
				left, leftSize, right, rightSize, joinCols, opts.EnableStreamingJoins)
			fmt.Printf("[HashJoin] Left columns: %v\n", left.Columns())
			fmt.Printf("[HashJoin] Right columns: %v\n", right.Columns())

			// Debug: check left relation's shouldCache flag if it's a StreamingRelation
			if sr, ok := left.(*StreamingRelation); ok {
				fmt.Printf("[HashJoin] Left is StreamingRelation: shouldCache=%v, iteratorCalled=%v, cache len=%d\n",
					sr.shouldCache, sr.iteratorCalled, len(sr.cache))
			}
		}
	}

	// Build column mappings
	leftIndices := make([]int, len(joinCols))
	rightIndices := make([]int, len(joinCols))
	for i, col := range joinCols {
		leftIndices[i] = ColumnIndex(left, col)
		rightIndices[i] = ColumnIndex(right, col)
		if leftIndices[i] < 0 || rightIndices[i] < 0 {
			// Join column not found - return empty relation with options
			opts := left.Options()
			if opts == (ExecutorOptions{}) {
				opts = right.Options()
			}
			return NewMaterializedRelationWithOptions(nil, nil, opts)
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

	// Choose smaller relation to build hash table
	var buildRel, probeRel Relation
	var buildIndices, probeIndices []int
	var buildIsLeft bool

	leftStreaming := isStreaming(left)
	rightStreaming := isStreaming(right)

	// Handle streaming relations appropriately
	if leftStreaming && !rightStreaming {
		// Left is streaming, right is materialized - use right as build
		buildRel, probeRel = right, left
		buildIndices, probeIndices = rightIndices, leftIndices
		buildIsLeft = false
		if opts.EnableDebugLogging {
			fmt.Printf("[HashJoin] Left is streaming, using materialized right as build\n")
		}
	} else if rightStreaming && !leftStreaming {
		// Right is streaming, left is materialized - use left as build
		buildRel, probeRel = left, right
		buildIndices, probeIndices = leftIndices, rightIndices
		buildIsLeft = true
		if opts.EnableDebugLogging {
			fmt.Printf("[HashJoin] Right is streaming, using materialized left as build\n")
		}
	} else if leftStreaming && rightStreaming {
		// Both streaming - should have used symmetric join, but fallback to
		// arbitrarily choosing left as build (will force materialization)
		buildRel, probeRel = left, right
		buildIndices, probeIndices = leftIndices, rightIndices
		buildIsLeft = true
		if opts.EnableDebugLogging {
			fmt.Printf("[HashJoin] WARNING: Both relations streaming, will materialize left as build\n")
		}
	} else {
		// Both materialized - use size-based optimization
		leftSize := left.Size()
		rightSize := right.Size()
		if leftSize >= 0 && rightSize >= 0 && leftSize < rightSize {
			buildRel, probeRel = left, right
			buildIndices, probeIndices = leftIndices, rightIndices
			buildIsLeft = true
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Using left as build (smaller: %d < %d)\n", leftSize, rightSize)
			}
		} else {
			buildRel, probeRel = right, left
			buildIndices, probeIndices = rightIndices, leftIndices
			buildIsLeft = false
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Using right as build (size: left=%d, right=%d)\n", leftSize, rightSize)
			}
		}
	}

	// Build phase - create hash table using efficient TupleKeyMap
	// For temporal data, we need to deduplicate by keeping only the latest version
	// Pre-size based on build relation size to avoid map growth
	buildSize := buildRel.Size()
	if buildSize < 0 {
		// Unknown size (streaming), use configurable default
		// 256 is a good balance: small enough for common cases (50-500 tuples),
		// large enough to avoid excessive rehashing for medium cases (500-2000 tuples)
		buildSize = opts.DefaultHashTableSize
		if buildSize == 0 {
			buildSize = 256 // Default if not configured
		}
	}
	hashTable := NewTupleKeyMapWithCapacity(buildSize)

	if opts.EnableDebugLogging {
		fmt.Printf("[HashJoin] Building hash table from relation with Size()=%d\n", buildRel.Size())
	}

	// Check if any column name matches transaction ID patterns
	// We'll verify the actual type on the first tuple during iteration
	txIndex := -1
	for i, col := range buildRel.Columns() {
		if col == query.Symbol("?tx") || col == query.Symbol("?t") ||
			col == query.Symbol("?txid") || col == query.Symbol("?transaction") {
			txIndex = i
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Found potential tx column %s at index %d\n", col, i)
			}
			break
		}
	}

	// CRITICAL: Check if build relation was already consumed
	// This should never happen - it indicates a bug in the executor
	if sr, ok := buildRel.(*StreamingRelation); ok {
		sr.mu.Lock()
		alreadyConsumed := sr.iteratorCalled && !sr.cacheReady
		sr.mu.Unlock()

		if alreadyConsumed {
			panic("BUG: HashJoin received a StreamingRelation that was already consumed. " +
				"Relations passed to Join must either be materialized or not yet iterated. " +
				"This indicates the executor is reusing relations incorrectly.")
		}
	}

	// Create build iterator - single iteration only
	buildIt := buildRel.Iterator()
	defer buildIt.Close()

	// Check first tuple to determine if we have a valid tx column
	if txIndex >= 0 {
		// Potential tx column found - check first tuple's type
		if !buildIt.Next() {
			// Empty relation - hash table stays empty
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Build relation is empty\n")
			}
		} else {
			firstTuple := buildIt.Tuple()
			hasTxColumn := false
			if txIndex < len(firstTuple) {
				switch firstTuple[txIndex].(type) {
				case uint64, int64, int:
					hasTxColumn = true
					if opts.EnableDebugLogging {
						fmt.Printf("[HashJoin] Confirmed tx column at index %d with type %T\n", txIndex, firstTuple[txIndex])
					}
				default:
					if opts.EnableDebugLogging {
						fmt.Printf("[HashJoin] Column at index %d is not a tx ID (type %T), using normal path\n", txIndex, firstTuple[txIndex])
					}
				}
			}

			// Process first tuple in the appropriate path
			if hasTxColumn {
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Using tx deduplication path (txIndex=%d, buildRel.Size()=%d)\n", txIndex, buildRel.Size())
			}
			// Deduplicate by keeping only the latest transaction
			// Pre-size based on build relation size
			latestTuples := NewTupleKeyMapWithCapacity(buildRel.Size())
			latestTx := NewTupleKeyMapWithCapacity(buildRel.Size())

			// Process first tuple
			tuple := firstTuple
			key := NewTupleKey(tuple, buildIndices)

			// Extract transaction ID
			var txID uint64
			switch v := tuple[txIndex].(type) {
			case uint64:
				txID = v
			case int64:
				txID = uint64(v)
			case int:
				txID = uint64(v)
			}

			// Keep only if this is newer than what we have
			if existingTxVal, exists := latestTx.Get(key); !exists || txID > existingTxVal.(uint64) {
				latestTuples.Put(key, tuple)
				latestTx.Put(key, txID)
			}

			// Process remaining tuples
			buildIterCount := 1
			for buildIt.Next() {
				buildIterCount++
				tuple := buildIt.Tuple()
				key := NewTupleKey(tuple, buildIndices)

				// Extract transaction ID
				switch v := tuple[txIndex].(type) {
				case uint64:
					txID = v
				case int64:
					txID = uint64(v)
				case int:
					txID = uint64(v)
				}

				// Keep only if this is newer than what we have
				if existingTxVal, exists := latestTx.Get(key); !exists || txID > existingTxVal.(uint64) {
					latestTuples.Put(key, tuple)
					latestTx.Put(key, txID)
				}
			}
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Build iterator produced %d tuples, latestTuples has %d entries\n",
					buildIterCount, len(latestTuples.m))
			}

			// Convert to the expected format
			txDedupCount := 0
			for _, entries := range latestTuples.m {
				for _, entry := range entries {
					// BUG FIX: Use the join key, not full tuple key!
					// We need to hash by buildIndices, not all columns
					tuple := entry.value.(Tuple)
					key := NewTupleKey(tuple, buildIndices)
					hashTable.Put(key, []Tuple{tuple})
					txDedupCount++
				}
			}
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Built hash table with %d tuples after tx deduplication\n", txDedupCount)
			}
		} else {
			// No transaction column or not a valid tx type, use normal path
			// Process first tuple
			tuple := firstTuple
			key := NewTupleKey(tuple, buildIndices)
			if existing, ok := hashTable.Get(key); ok {
				hashTable.Put(key, append(existing.([]Tuple), tuple))
			} else {
				hashTable.Put(key, []Tuple{tuple})
			}

			buildCount := 1
			var firstBuildKey *TupleKey
			var firstBuildTuple Tuple
			if opts.EnableDebugLogging {
				firstBuildKey = &key
				firstBuildTuple = tuple
			}

			// Process remaining tuples
			for buildIt.Next() {
				tuple := buildIt.Tuple()
				key := NewTupleKey(tuple, buildIndices)
				if existing, ok := hashTable.Get(key); ok {
					hashTable.Put(key, append(existing.([]Tuple), tuple))
				} else {
					hashTable.Put(key, []Tuple{tuple})
				}
				buildCount++
			}
			if opts.EnableDebugLogging {
				if firstBuildKey != nil {
					fmt.Printf("[HashJoin] Built hash table with %d tuples, first key: %v, first tuple: %v\n",
						buildCount, firstBuildKey, firstBuildTuple)
				} else {
					fmt.Printf("[HashJoin] Built hash table with %d tuples from iterator\n", buildCount)
				}
			}
		}
		}
	} else {
		// No potential tx column - use normal path for all tuples
		buildCount := 0
		var firstBuildKey *TupleKey
		var firstBuildTuple Tuple
		for buildIt.Next() {
			tuple := buildIt.Tuple()
			key := NewTupleKey(tuple, buildIndices)
			if buildCount == 0 && opts.EnableDebugLogging {
				firstBuildKey = &key
				firstBuildTuple = tuple
			}
			if existing, ok := hashTable.Get(key); ok {
				hashTable.Put(key, append(existing.([]Tuple), tuple))
			} else {
				hashTable.Put(key, []Tuple{tuple})
			}
			buildCount++
		}
		if opts.EnableDebugLogging {
			if firstBuildKey != nil {
				fmt.Printf("[HashJoin] Built hash table with %d tuples, first key: %v, first tuple: %v\n",
					buildCount, firstBuildKey, firstBuildTuple)
			} else {
				fmt.Printf("[HashJoin] Built hash table with %d tuples from iterator\n", buildCount)
			}
		}
	}

	// Probe phase - find matches
	// Check if streaming mode is enabled
	if opts.EnableStreamingJoins {
		// Return streaming relation with lazy evaluation
		// Handle unknown sizes (-1) with reasonable default
		expectedResults := probeRel.Size()
		if expectedResults < 0 {
			expectedResults = defaultCapacity
		}
		buildSize := buildRel.Size()
		if buildSize > 0 && buildSize < expectedResults {
			expectedResults = buildSize
		}

		if opts.EnableDebugLogging {
			probeSize := probeRel.Size()
			if probeSize < 0 {
				probeSize = -1 // Keep as unknown for logging
			}
			buildSizeLog := buildRel.Size()
			if buildSizeLog < 0 {
				buildSizeLog = -1
			}
			fmt.Printf("[HashJoin STREAMING] Creating hashJoinIterator with buildSize=%d, probeSize=%d\n",
				buildSizeLog, probeSize)
		}

		iter := &hashJoinIterator{
			hashTable:    hashTable,
			probeIt:      probeRel.Iterator(),
			seen:         NewTupleKeyMapWithCapacity(expectedResults),
			buildIsLeft:  buildIsLeft,
			joinCols:     joinCols,
			leftCols:     left.Columns(),
			rightCols:    right.Columns(),
			probeIndices: probeIndices,
			options:      opts,
			matchIdx:     0,
		}

		// Return streaming result - no forced materialization
		// StreamingRelation enforces single-use semantics via panic if Iterator() called twice
		// Caller can explicitly call Materialize() if multiple iterations needed
		return &StreamingRelation{
			columns:  outputCols,
			iterator: iter,
			size:     -1, // unknown size until consumed
			options:  opts,
		}
	}

	// Materialized mode (original implementation)
	// Use efficient TupleKeyMap for deduplication
	// Pre-size seen map - worst case is probe size, but likely smaller due to filtering
	// Use min(probeSize, buildSize) as estimate
	// Handle unknown sizes (-1) with reasonable default
	expectedResults := probeRel.Size()
	if expectedResults < 0 {
		expectedResults = defaultCapacity
	}
	probeBuildSize := buildRel.Size()
	if probeBuildSize > 0 && probeBuildSize < expectedResults {
		expectedResults = probeBuildSize
	}
	seen := NewTupleKeyMapWithCapacity(expectedResults)
	var results []Tuple

	// CRITICAL: Check if probe relation was already consumed
	// This should never happen - it indicates a bug in the executor
	if sr, ok := probeRel.(*StreamingRelation); ok {
		sr.mu.Lock()
		alreadyConsumed := sr.iteratorCalled && !sr.cacheReady
		sr.mu.Unlock()

		if alreadyConsumed {
			panic("BUG: HashJoin received a StreamingRelation that was already consumed. " +
				"Relations passed to Join must either be materialized or not yet iterated. " +
				"This indicates the executor is reusing relations incorrectly.")
		}
	}

	probeIt := probeRel.Iterator()
	defer probeIt.Close()

	probeCount := 0
	matchCount := 0
	for probeIt.Next() {
		probeTuple := probeIt.Tuple()
		key := NewTupleKey(probeTuple, probeIndices)
		probeCount++

		if opts.EnableDebugLogging && probeCount == 1 {
			fmt.Printf("[HashJoin] First probe tuple: %v, key: %v\n", probeTuple, key)
		}

		if matchesVal, ok := hashTable.Get(key); ok {
			matchCount++
			if opts.EnableDebugLogging && matchCount == 1 {
				fmt.Printf("[HashJoin] Found match! probe key matched, matches count: %d\n", len(matchesVal.([]Tuple)))
			}
			matches := matchesVal.([]Tuple)
			for _, buildTuple := range matches {
				// Combine tuples
				var joined Tuple
				if buildIsLeft {
					joined = combineTuples(buildTuple, probeTuple, joinCols, left.Columns(), right.Columns())
				} else {
					joined = combineTuples(probeTuple, buildTuple, joinCols, left.Columns(), right.Columns())
				}

				// Create a key for deduplication based on all tuple values
				dedupKey := NewTupleKeyFull(joined)
				if !seen.Exists(dedupKey) {
					seen.Put(dedupKey, true)
					results = append(results, joined)
				}
			}
		}
	}

	if opts.EnableDebugLogging {
		fmt.Printf("[HashJoin] Probe phase complete: probed %d tuples, found %d matches, produced %d results\n",
			probeCount, matchCount, len(results))
	}

	// We already deduplicated with 'seen', no need to do it again
	return NewMaterializedRelationNoDedupeWithOptions(outputCols, results, opts)
}

// SemiJoin returns tuples from left that have matches in right
func SemiJoin(left, right Relation, joinCols []query.Symbol) Relation {
	// Build indices
	leftIndices := make([]int, len(joinCols))
	rightIndices := make([]int, len(joinCols))
	for i, col := range joinCols {
		leftIndices[i] = ColumnIndex(left, col)
		rightIndices[i] = ColumnIndex(right, col)
	}

	// Extract options from left relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}

	// Build set of keys from right relation using efficient TupleKeyMap
	// Pre-size based on right relation size
	rightKeys := NewTupleKeyMapWithCapacity(right.Size())
	rightIt := right.Iterator()
	defer rightIt.Close()

	for rightIt.Next() {
		tuple := rightIt.Tuple()
		key := NewTupleKey(tuple, rightIndices)
		rightKeys.Put(key, true)
	}

	// Filter left relation
	var results []Tuple
	leftIt := left.Iterator()
	defer leftIt.Close()

	for leftIt.Next() {
		tuple := leftIt.Tuple()
		key := NewTupleKey(tuple, leftIndices)
		if rightKeys.Exists(key) {
			results = append(results, tuple)
		}
	}

	return NewMaterializedRelationWithOptions(left.Columns(), results, opts)
}

// AntiJoin returns tuples from left that have no matches in right
func AntiJoin(left, right Relation, joinCols []query.Symbol) Relation {
	// Build indices
	leftIndices := make([]int, len(joinCols))
	rightIndices := make([]int, len(joinCols))
	for i, col := range joinCols {
		leftIndices[i] = ColumnIndex(left, col)
		rightIndices[i] = ColumnIndex(right, col)
	}

	// Extract options from left relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}

	// Build set of keys from right relation using efficient TupleKeyMap
	// Pre-size based on right relation size
	rightKeys := NewTupleKeyMapWithCapacity(right.Size())
	rightIt := right.Iterator()
	defer rightIt.Close()

	for rightIt.Next() {
		tuple := rightIt.Tuple()
		key := NewTupleKey(tuple, rightIndices)
		rightKeys.Put(key, true)
	}

	// Filter left relation
	var results []Tuple
	leftIt := left.Iterator()
	defer leftIt.Close()

	for leftIt.Next() {
		tuple := leftIt.Tuple()
		key := NewTupleKey(tuple, leftIndices)
		if !rightKeys.Exists(key) {
			results = append(results, tuple)
		}
	}

	return NewMaterializedRelationWithOptions(left.Columns(), results, opts)
}

// Helper functions

func isStreaming(rel Relation) bool {
	_, ok := rel.(*StreamingRelation)
	return ok
}

func combineTuples(left, right Tuple, joinCols []query.Symbol, leftCols, rightCols []query.Symbol) Tuple {
	// Create set of join columns for quick lookup
	joinSet := make(map[query.Symbol]bool, len(joinCols))
	for _, col := range joinCols {
		joinSet[col] = true
	}

	// Calculate exact result size to avoid repeated allocations
	rightNonJoinCount := 0
	for _, col := range rightCols {
		if !joinSet[col] {
			rightNonJoinCount++
		}
	}

	// Pre-allocate result with exact size
	result := make(Tuple, len(left)+rightNonJoinCount)

	// Copy left tuple
	copy(result, left)

	// Add values from right that aren't join columns
	offset := len(left)
	for i, col := range rightCols {
		if !joinSet[col] {
			result[offset] = right[i]
			offset++
		}
	}

	return result
}

func crossProduct(left, right Relation) Relation {
	// Warning: This can be very expensive!
	// Extract options from left relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}

	outputCols := append(left.Columns(), right.Columns()...)
	var results []Tuple

	leftIt := left.Iterator()
	defer leftIt.Close()

	// For each left tuple
	for leftIt.Next() {
		leftTuple := leftIt.Tuple()

		// Match with every right tuple
		rightIt := right.Iterator()
		for rightIt.Next() {
			rightTuple := rightIt.Tuple()
			combined := append(append(Tuple{}, leftTuple...), rightTuple...)
			results = append(results, combined)
		}
		rightIt.Close()
	}

	return NewMaterializedRelationWithOptions(outputCols, results, opts)
}
