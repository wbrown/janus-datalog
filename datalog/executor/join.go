package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
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
	buildErr     error // deferred error captured from the (eagerly consumed) build relation
	seen         *TupleKeyMap
	buildIsLeft  bool
	// probeNeedsCopy is true when the probe relation's iterator reuses its
	// tuple workspace (RequiresCopy()); only then must currentProbeTuple be
	// copied before use. Materialized probes return stable tuples and skip it.
	probeNeedsCopy bool
	probeIndices   []int
	// rightNonJoinIndices identifies positions in the right-side tuple
	// (per buildIsLeft, the probe side when buildIsLeft is true, otherwise
	// the build side) whose values must be appended to each result tuple.
	// Computed once at iterator setup; the combine inner loop is a pure
	// indexed gather, never a per-call symbol scan.
	rightNonJoinIndices []int
	resultWidth         int
	options             ExecutorOptions

	// Current state - NOT safe for concurrent access
	currentProbeTuple Tuple
	currentJoined     Tuple
	matches           []Tuple
	matchIdx          int
	closed            bool

	// debug is non-nil only when EnableDebugLogging is set; the per-row
	// counters live behind it so the hot path neither writes them nor carries
	// them in the iterator struct when debug is off.
	debug *hashJoinDebug
}

// hashJoinDebug holds per-row counters used only for debug logging. It is
// allocated and incremented only when ExecutorOptions.EnableDebugLogging is
// set, keeping these writes off the hot path entirely.
type hashJoinDebug struct {
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

			// Combine tuples via the precomputed projection plan.
			var joined Tuple
			if it.buildIsLeft {
				joined = combineTuplesIndexed(buildTuple, it.currentProbeTuple, it.rightNonJoinIndices, it.resultWidth)
			} else {
				joined = combineTuplesIndexed(it.currentProbeTuple, buildTuple, it.rightNonJoinIndices, it.resultWidth)
			}

			// Check for duplicates using seen map (single bucket walk)
			dedupKey := NewTupleKeyFull(joined)
			if !it.seen.PutIfAbsent(dedupKey, true) {
				// combineTuplesIndexed returns a fresh slice on every call and
				// nothing mutates it, so no defensive copy is needed here. Any
				// downstream consumer that retains tuples copies at its own
				// boundary (this join's StreamingRelation has RequiresCopy()==true).
				it.currentJoined = joined // Store for Tuple() to return
				if it.debug != nil {
					it.debug.resultCount++
				}
				return true
			}
			// Duplicate, continue to next match
			continue
		}

		// Need next probe tuple
		if !it.probeIt.Next() {
			if it.debug != nil {
				fmt.Printf("[hashJoinIterator] Probe exhausted after %d tuples, %d matched, produced %d results\n",
					it.debug.probeCount, it.debug.matchCount, it.debug.resultCount)
			}
			return false
		}

		if it.debug != nil {
			it.debug.probeCount++
		}
		it.currentProbeTuple = it.probeIt.Tuple()

		// Only copy when the probe iterator reuses its tuple workspace. A
		// materialized probe returns stable tuples (RequiresCopy()==false), so
		// the copy is skipped — saving one alloc per probe row. Values read
		// from currentProbeTuple are consumed before the next probeIt.Next()
		// (combineTuplesIndexed copies them into fresh result slices), so a
		// reused buffer only needs copying when the iterator says so.
		if it.probeNeedsCopy {
			tupleCopy := make(Tuple, len(it.currentProbeTuple))
			copy(tupleCopy, it.currentProbeTuple)
			it.currentProbeTuple = tupleCopy
		}

		key := NewTupleKey(it.currentProbeTuple, it.probeIndices)

		if it.debug != nil && it.debug.probeCount == 1 {
			fmt.Printf("[hashJoinIterator] First probe key: %v\n", key)
		}

		// Look up matches in hash table
		if matchesVal, ok := it.hashTable.Get(key); ok {
			it.matches = matchesVal.([]Tuple)
			it.matchIdx = 0
			if it.debug != nil {
				it.debug.matchCount++
			}
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

func (it *hashJoinIterator) Error() error {
	if it.buildErr != nil {
		return it.buildErr
	}
	if it.probeIt != nil {
		return it.probeIt.Error()
	}
	return nil
}

// HashJoin performs a hash join on specified symbols
// It attempts to get options from the input relations
func HashJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	// Try to get options from either relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}
	return HashJoinWithOptions(left, right, joinSyms, opts)
}

// HashJoinWithOptions performs a hash join with explicit options
func HashJoinWithOptions(left, right Relation, joinSyms []query.Symbol, opts ExecutorOptions) Relation {
	// Default capacity for unknown sizes (-1)
	const defaultCapacity = 1000

	// Check if we should use symmetric hash join for streaming relations
	if opts.EnableSymmetricHashJoin {
		strategy := ChooseJoinStrategy(left, right, joinSyms, opts)
		if strategy == "symmetric" {
			if opts.EnableDebugLogging {
				fmt.Printf("[HashJoin] Using symmetric hash join for streaming-to-streaming\n")
			}
			return SymmetricHashJoinWithOptions(left, right, joinSyms, opts)
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
			fmt.Printf("[HashJoin] Called with left (type=%T, size=%d), right (type=%T, size=%d), joinSyms=%v, EnableStreamingJoins=%v\n",
				left, leftSize, right, rightSize, joinSyms, opts.EnableStreamingJoins)
			fmt.Printf("[HashJoin] Left symbols: %v\n", left.Symbols())
			fmt.Printf("[HashJoin] Right symbols: %v\n", right.Symbols())

			// Debug: check left relation's shouldCache flag if it's a StreamingRelation
			if sr, ok := left.(*StreamingRelation); ok {
				fmt.Printf("[HashJoin] Left is StreamingRelation: shouldCache=%v, iteratorCalled=%v, cache len=%d\n",
					sr.shouldCache, sr.iteratorCalled, len(sr.cache))
			}
		}
	}

	// Build symbol mappings
	leftIndices := make([]int, len(joinSyms))
	rightIndices := make([]int, len(joinSyms))
	for i, sym := range joinSyms {
		leftIndices[i] = SymbolIndex(left, sym)
		rightIndices[i] = SymbolIndex(right, sym)
		if leftIndices[i] < 0 || rightIndices[i] < 0 {
			// Join symbol not found - return empty relation with options
			opts := left.Options()
			if opts == (ExecutorOptions{}) {
				opts = right.Options()
			}
			return NewMaterializedRelationWithOptions(nil, nil, opts)
		}
	}

	// Determine output symbols and the right-side projection plan in a
	// single pass: positions in right.Symbols() that are not join columns
	// both append to outputSyms (the result schema) and to
	// rightNonJoinIndices (the gather indices used by combineTuplesIndexed
	// on every matched row). Precomputing these here turns the per-row
	// inner loop into a pure indexed copy.
	leftSyms := left.Symbols()
	outputSyms := append([]query.Symbol{}, leftSyms...)
	joinSymSet := make(map[query.Symbol]bool, len(joinSyms))
	for _, sym := range joinSyms {
		joinSymSet[sym] = true
	}
	rightSyms := right.Symbols()
	rightNonJoinIndices := make([]int, 0, len(rightSyms))
	for i, sym := range rightSyms {
		if !joinSymSet[sym] {
			outputSyms = append(outputSyms, sym)
			rightNonJoinIndices = append(rightNonJoinIndices, i)
		}
	}
	resultWidth := len(leftSyms) + len(rightNonJoinIndices)

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

	// Build phase - create hash table using efficient TupleKeyMap.
	// This is a pure relational join: every build row is preserved. CRDT/temporal
	// resolution is the storage layer's responsibility (EATV ordering), never
	// inferred here from a column's name.
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
	// Close explicitly after build loop, not deferred. The build relation may share
	// underlying iterators with the probe relation (e.g., OrFallbackRelation wraps the
	// same StreamingRelation that is the probe side). Deferring Close() causes deadlock.
	buildIt := buildRel.Iterator()

	// Check if we need to copy tuples from the build relation
	// This avoids unnecessary copies when the source guarantees stable tuples
	needsCopy := buildRel.RequiresCopy()
	// copyCount/passthruCount feed the JoinBuildCopy annotation below; only
	// track them when a collector will read them. Inlined into the build loop
	// (no closure) to avoid a per-join heap allocation.
	trackCopy := opts.Collector != nil
	var copyCount, passthruCount int

	// Pure relational build: group every build tuple by its join key. All rows
	// are preserved; identical output tuples are deduplicated downstream by set
	// semantics, not here. CRDT/temporal "latest transaction wins" resolution is
	// handled by the storage layer (EATV index ordering), never inferred from a
	// column's name.
	buildCount := 0
	var firstBuildKey *TupleKey
	var firstBuildTuple Tuple
	for buildIt.Next() {
		tuple := buildIt.Tuple()
		// Copy only when the build relation reuses its tuple workspace; the
		// hash table retains these tuples for the join's lifetime. Key is built
		// after the copy — same values, so the join key is unaffected.
		if needsCopy {
			tuple = copyTuple(tuple)
			if trackCopy {
				copyCount++
			}
		} else if trackCopy {
			passthruCount++
		}
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

	// Capture any deferred error from the build scan before closing it, so a
	// build-side failure isn't lost (it propagates onto the join result).
	buildErr := buildIt.Error()

	// Close build iterator BEFORE probe phase begins.
	// The build relation may share underlying iterators with the probe relation
	// (e.g., OrFallbackRelation wraps a StreamingRelation that is also the probe).
	// Close() signals the CachingIterator, unblocking probe's Size()/Iterator().
	buildIt.Close()

	// Emit annotation for copy statistics if collector is available
	if opts.Collector != nil && (copyCount > 0 || passthruCount > 0) {
		opts.Collector.Add(annotations.Event{
			Name:  annotations.JoinBuildCopy,
			Start: time.Now(),
			End:   time.Now(),
			Data: map[string]interface{}{
				"copied":        copyCount,
				"passthru":      passthruCount,
				"requires_copy": needsCopy,
			},
		})
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
			hashTable:           hashTable,
			probeIt:             probeRel.Iterator(),
			buildErr:            buildErr,
			seen:                NewTupleKeyMapWithCapacity(expectedResults),
			buildIsLeft:         buildIsLeft,
			probeNeedsCopy:      probeRel.RequiresCopy(),
			probeIndices:        probeIndices,
			rightNonJoinIndices: rightNonJoinIndices,
			resultWidth:         resultWidth,
			options:             opts,
			matchIdx:            0,
		}
		if opts.EnableDebugLogging {
			iter.debug = &hashJoinDebug{}
		}

		// Return streaming result - no forced materialization
		// StreamingRelation enforces single-use semantics via panic if Iterator() called twice
		// Caller can explicitly call Materialize() if multiple iterations needed
		return &StreamingRelation{
			symbols:  outputSyms,
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
				// Combine tuples via the precomputed projection plan.
				var joined Tuple
				if buildIsLeft {
					joined = combineTuplesIndexed(buildTuple, probeTuple, rightNonJoinIndices, resultWidth)
				} else {
					joined = combineTuplesIndexed(probeTuple, buildTuple, rightNonJoinIndices, resultWidth)
				}

				// Create a key for deduplication based on all tuple values
				// (single bucket walk via PutIfAbsent)
				dedupKey := NewTupleKeyFull(joined)
				if !seen.PutIfAbsent(dedupKey, true) {
					results = append(results, joined)
				}
			}
		}
	}

	if opts.EnableDebugLogging {
		fmt.Printf("[HashJoin] Probe phase complete: probed %d tuples, found %d matches, produced %d results\n",
			probeCount, matchCount, len(results))
	}

	// We already deduplicated with 'seen', no need to do it again.
	// Carry any deferred build/probe error onto the result so a failed scan
	// isn't laundered into an empty/partial join.
	result := NewMaterializedRelationNoDedupeWithOptions(outputSyms, results, opts)
	if buildErr != nil {
		result.err = buildErr
	} else if pe := probeIt.Error(); pe != nil {
		result.err = pe
	}
	return result
}

// SemiJoin returns tuples from left that have matches in right
func SemiJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	// Build indices
	leftIndices := make([]int, len(joinSyms))
	rightIndices := make([]int, len(joinSyms))
	for i, sym := range joinSyms {
		leftIndices[i] = SymbolIndex(left, sym)
		rightIndices[i] = SymbolIndex(right, sym)
	}

	// Extract options from left relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}

	// Build set of keys from right relation using efficient TupleKeyMap.
	// If the right side fails, the key set is incomplete — every filter decision
	// becomes untrustworthy (semi-join would drop real matches), so surface the
	// error and trust no result rows.
	rightKeys := NewTupleKeyMapWithCapacity(right.Size())
	rightIt := right.Iterator()
	for rightIt.Next() {
		tuple := rightIt.Tuple()
		key := NewTupleKey(tuple, rightIndices)
		rightKeys.Put(key, true)
	}
	rerr := rightIt.Error()
	if cerr := rightIt.Close(); rerr == nil {
		rerr = cerr
	}
	if rerr != nil {
		res := NewMaterializedRelationWithOptions(left.Symbols(), nil, opts)
		res.err = rerr
		return res
	}

	// Filter left relation
	var results []Tuple
	leftIt := left.Iterator()
	for leftIt.Next() {
		tuple := leftIt.Tuple()
		key := NewTupleKey(tuple, leftIndices)
		if rightKeys.Exists(key) {
			results = append(results, tuple)
		}
	}
	lerr := leftIt.Error()
	if cerr := leftIt.Close(); lerr == nil {
		lerr = cerr
	}

	res := NewMaterializedRelationWithOptions(left.Symbols(), results, opts)
	res.err = lerr
	return res
}

// AntiJoin returns tuples from left that have no matches in right
func AntiJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	// Build indices
	leftIndices := make([]int, len(joinSyms))
	rightIndices := make([]int, len(joinSyms))
	for i, sym := range joinSyms {
		leftIndices[i] = SymbolIndex(left, sym)
		rightIndices[i] = SymbolIndex(right, sym)
	}

	// Extract options from left relation
	opts := left.Options()
	if opts == (ExecutorOptions{}) {
		opts = right.Options()
	}

	// Build set of keys from right relation using efficient TupleKeyMap.
	// If the right side fails, the key set is incomplete — an anti-join would then
	// report false "no match" rows (a missing key from a decode failure is
	// indistinguishable from a real absence), so surface the error and trust no
	// result rows.
	rightKeys := NewTupleKeyMapWithCapacity(right.Size())
	rightIt := right.Iterator()
	for rightIt.Next() {
		tuple := rightIt.Tuple()
		key := NewTupleKey(tuple, rightIndices)
		rightKeys.Put(key, true)
	}
	rerr := rightIt.Error()
	if cerr := rightIt.Close(); rerr == nil {
		rerr = cerr
	}
	if rerr != nil {
		res := NewMaterializedRelationWithOptions(left.Symbols(), nil, opts)
		res.err = rerr
		return res
	}

	// Filter left relation
	var results []Tuple
	leftIt := left.Iterator()
	for leftIt.Next() {
		tuple := leftIt.Tuple()
		key := NewTupleKey(tuple, leftIndices)
		if !rightKeys.Exists(key) {
			results = append(results, tuple)
		}
	}
	lerr := leftIt.Error()
	if cerr := leftIt.Close(); lerr == nil {
		lerr = cerr
	}

	res := NewMaterializedRelationWithOptions(left.Symbols(), results, opts)
	res.err = lerr
	return res
}

// Join utility functions

func isStreaming(rel Relation) bool {
	_, ok := rel.(*StreamingRelation)
	return ok
}

// combineTuplesIndexed builds a result tuple by copying `first` into the
// leading positions, then appending values from `second` at the indices
// listed in secondNonJoinIndices. `resultWidth` is len(first) +
// len(secondNonJoinIndices), passed in to avoid a redundant add per call.
//
// The projection plan (secondNonJoinIndices, resultWidth) is invariant
// for the lifetime of a hash join — it depends only on the join's
// symbols, not on any tuple — so it is computed once in
// HashJoinWithOptions and reused for every matched row. This replaces the
// previous combineTuples which allocated a joinSet map and walked
// rightSyms twice on every call.
func combineTuplesIndexed(first, second Tuple, secondNonJoinIndices []int, resultWidth int) Tuple {
	result := make(Tuple, resultWidth)
	copy(result, first)
	base := len(first)
	for k, idx := range secondNonJoinIndices {
		result[base+k] = second[idx]
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

	outputSyms := append(left.Symbols(), right.Symbols()...)
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

	return NewMaterializedRelationWithOptions(outputSyms, results, opts)
}
