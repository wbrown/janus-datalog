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
	buildIndex  *groupedTupleIndex
	probeIt     Iterator
	buildErr    error // deferred error captured from the (eagerly consumed) build relation
	seen        *TupleKeyMap
	buildIsLeft bool
	// probeNeedsCopy is true when the probe relation's iterator reuses its
	// tuple workspace (RequiresCopy()); only then must currentProbeTuple be
	// copied before use. Materialized probes return stable tuples and skip it.
	probeNeedsCopy bool
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

	// metrics is non-nil only when annotations are enabled, keeping counters
	// off the hot path otherwise.
	metrics *hashJoinMetrics
}

type hashJoinMetrics struct {
	probeCount  int
	matchCount  int
	resultCount int
	emitted     bool
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

			// A nil seen map means a derived candidate key already proves every
			// output tuple unique. Otherwise restore set semantics explicitly.
			if it.seen == nil || !it.seen.PutIfAbsent(NewTupleKeyFull(joined), true) {
				// combineTuplesIndexed returns a fresh slice on every call and
				// nothing mutates it, so no defensive copy is needed here. Any
				// downstream consumer that retains tuples copies at its own
				// boundary (this join's StreamingRelation has RequiresCopy()==true).
				it.currentJoined = joined // Store for Tuple() to return
				if it.metrics != nil {
					it.metrics.resultCount++
				}
				return true
			}
			// Duplicate, continue to next match
			continue
		}

		// Need next probe tuple
		if !it.probeIt.Next() {
			// metrics is allocated only when a collector exists, so its
			// presence is the existence guard on this path.
			if it.metrics != nil {
				it.emitProbeAnnotation()
			}
			return false
		}

		if it.metrics != nil {
			it.metrics.probeCount++
		}
		it.currentProbeTuple = it.probeIt.Tuple()

		// Only copy when the probe iterator reuses its tuple workspace. A
		// materialized probe returns stable tuples (RequiresCopy()==false), so
		// the copy is skipped — saving one alloc per probe tuple. Values read
		// from currentProbeTuple are consumed before the next probeIt.Next()
		// (combineTuplesIndexed copies them into fresh result slices), so a
		// reused buffer only needs copying when the iterator says so.
		if it.probeNeedsCopy {
			tupleCopy := make(Tuple, len(it.currentProbeTuple))
			copy(tupleCopy, it.currentProbeTuple)
			it.currentProbeTuple = tupleCopy
		}

		// Look up matches in the grouped build tuples — a positional probe, so
		// no key materializes, and a hit is a subslice of the shared backing.
		if matches := it.buildIndex.probe(it.currentProbeTuple); len(matches) > 0 {
			it.matches = matches
			it.matchIdx = 0
			if it.metrics != nil {
				it.metrics.matchCount++
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
		if it.metrics != nil {
			it.emitProbeAnnotation()
		}
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

// emitProbeAnnotation reports the probe's counters. Its callers guard on
// metrics, which is allocated only when a handler exists (see the construction
// below), so its presence is what "annotations are on" means on this path. What
// remains here is the once-only latch: exhaustion and Close both reach this, and
// the counters belong to whichever arrives first.
func (it *hashJoinIterator) emitProbeAnnotation() {
	if it.metrics.emitted {
		return
	}
	it.metrics.emitted = true
	it.options.Handler(annotations.Event{
		Name: annotations.JoinProbe,
		Data: map[string]interface{}{
			"tuple_count":   it.metrics.probeCount,
			"matched_count": it.metrics.matchCount,
			"result_count":  it.metrics.resultCount,
			"mode":          "streaming",
		},
	})
}

// emitJoinStrategyAnnotation reports which join shape was chosen and what it
// was chosen over. Callers guard on the handler being non-nil.
//
// left and right are rendered by type, not by value: the datum is which
// Relation implementation each side is, and a Relation is a live stream that
// must not be spent to describe itself.
func emitJoinStrategyAnnotation(
	handler annotations.Handler,
	left, right Relation,
	joinSymbols []query.Symbol,
	mode, buildSide string,
	buildKeyUnique bool,
) {
	handler(annotations.Event{
		Name: annotations.JoinStrategy,
		Data: map[string]interface{}{
			"mode":             mode,
			"build_side":       buildSide,
			"build_key_unique": buildKeyUnique,
			"left_type":        fmt.Sprintf("%T", left),
			"right_type":       fmt.Sprintf("%T", right),
			"left_size":        materializedSize(left),
			"right_size":       materializedSize(right),
			"join_symbols":     append([]query.Symbol(nil), joinSymbols...),
		},
	})
}

// HashJoin performs a hash join on specified symbols
// It attempts to get options from the input relations
func HashJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	// Try to get options from either relation
	opts := left.Options()
	if !opts.populated() {
		opts = right.Options()
	}
	return HashJoinWithOptions(left, right, joinSyms, opts)
}

// HashJoinWithOptions performs a hash join with explicit options, reporting its
// strategy and probe counters through opts.Handler when one is set.
func HashJoinWithOptions(
	left, right Relation,
	joinSyms []query.Symbol,
	opts ExecutorOptions,
) Relation {
	// Default capacity for unknown sizes (-1)
	const defaultCapacity = 1000

	// Check if we should use symmetric hash join for streaming relations
	if opts.EnableSymmetricHashJoin {
		strategy := ChooseJoinStrategy(left, right, joinSyms, opts)
		if strategy == "symmetric" {
			return SymmetricHashJoinWithOptions(left, right, joinSyms, opts)
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
			if !opts.populated() {
				opts = right.Options()
			}
			return NewMaterializedRelationWithOptions(nil, nil, opts)
		}
	}

	// Determine output symbols and the right-side projection plan in a
	// single pass: positions in right.Symbols() that are not join symbols
	// both append to outputSyms (the result relation's symbols) and to
	// rightNonJoinIndices (the gather indices used by combineTuplesIndexed
	// on every matched tuple). Precomputing these here turns the per-tuple
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
	resultProperties := joinProperties(left.Properties(), right.Properties(), joinSyms)

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
	} else if rightStreaming && !leftStreaming {
		// Right is streaming, left is materialized - use left as build
		buildRel, probeRel = left, right
		buildIndices, probeIndices = leftIndices, rightIndices
		buildIsLeft = true
	} else if leftStreaming && rightStreaming {
		// Both streaming - should have used symmetric join, but fallback to
		// arbitrarily choosing left as build (will force materialization)
		buildRel, probeRel = left, right
		buildIndices, probeIndices = leftIndices, rightIndices
		buildIsLeft = true
	} else {
		// Both materialized - use size-based optimization
		leftSize := left.Size()
		rightSize := right.Size()
		if leftSize >= 0 && rightSize >= 0 && leftSize < rightSize {
			buildRel, probeRel = left, right
			buildIndices, probeIndices = leftIndices, rightIndices
			buildIsLeft = true
		} else {
			buildRel, probeRel = right, left
			buildIndices, probeIndices = rightIndices, leftIndices
			buildIsLeft = false
		}
	}
	buildKeysUnique := hasKeyWithin(buildRel.Properties().Keys, joinSymSet)
	mode := "materialized"
	if opts.EnableStreamingJoins {
		mode = "streaming"
	}
	buildSide := "right"
	if buildIsLeft {
		buildSide = "left"
	}
	if opts.Handler != nil {
		emitJoinStrategyAnnotation(opts.Handler, left, right, joinSyms, mode, buildSide, buildKeysUnique)
	}

	// Build phase - collect the build tuples once, then group them by join-key
	// hash into contiguous spans of one shared backing (groupedTupleIndex).
	// This is a pure relational join: every build tuple is preserved. CRDT/temporal
	// resolution is the storage layer's responsibility (EATV ordering), never
	// inferred here from a symbol's name.
	// Pre-size based on build relation size to avoid slice growth
	collectCap := buildRel.Size()
	if collectCap < 0 {
		// Unknown size (streaming), use configurable default
		// 256 is a good balance: small enough for common cases (50-500 tuples),
		// large enough to avoid excessive rehashing for medium cases (500-2000 tuples)
		collectCap = opts.DefaultHashTableSize
		if collectCap == 0 {
			collectCap = 256 // Default if not configured
		}
	}
	buildTuples := make([]Tuple, 0, collectCap)

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

	// copyCount/passthruCount feed the JoinBuildCopy annotation below; only
	// track them when a handler will read them. Inlined into the build loop
	// (no closure) to avoid a per-join heap allocation.
	trackCopy := opts.Handler != nil
	var copyCount, passthruCount int
	// The interval the copies were made in: draining the build relation. It ends
	// at the loop rather than at the emit, which happens after grouping — a cost
	// the copy statistics do not describe.
	var buildStart, buildEnd time.Time
	if trackCopy {
		buildStart = time.Now()
	}

	// Create build iterator - single iteration only
	// Close explicitly after build loop, not deferred. The build relation may share
	// underlying iterators with the probe relation (e.g., OrFallbackRelation wraps the
	// same StreamingRelation that is the probe side). Deferring Close() causes deadlock.
	buildIt := buildRel.Iterator()

	// Check if we need to copy tuples from the build relation
	// This avoids unnecessary copies when the source guarantees stable tuples
	needsCopy := buildRel.RequiresCopy()

	for buildIt.Next() {
		tuple := buildIt.Tuple()
		// Copy only when the build relation reuses its tuple workspace; the
		// grouped index retains these tuples for the join's lifetime.
		if needsCopy {
			tuple = copyTuple(tuple)
			if trackCopy {
				copyCount++
			}
		} else if trackCopy {
			passthruCount++
		}
		buildTuples = append(buildTuples, tuple)
	}
	buildCount := len(buildTuples)

	// Capture any deferred error from the build scan before closing it, so a
	// build-side failure isn't lost (it propagates onto the join result).
	buildErr := buildIt.Error()

	// Close build iterator BEFORE probe phase begins.
	// The build relation may share underlying iterators with the probe relation
	// (e.g., OrFallbackRelation wraps a StreamingRelation that is also the probe).
	// Close() signals the CachingIterator, unblocking probe's Size()/Iterator().
	if closeErr := buildIt.Close(); buildErr == nil {
		buildErr = closeErr
	}
	if trackCopy {
		buildEnd = time.Now()
	}

	// Group the build tuples by join-key hash. The tuples carry their own key
	// values, so no key materializes per tuple and fanout needs no per-key
	// slices — probes verify against the tuples in place.
	buildIndex := groupTuples(buildTuples, probeIndices, buildIndices)
	if buildKeysUnique && !buildIndex.keysUnique() {
		panic("hash join build relation violated its candidate-key guarantee")
	}

	// Emit annotation for copy statistics if a handler is available
	if opts.Handler != nil && (copyCount > 0 || passthruCount > 0) {
		opts.Handler(annotations.Event{
			Name:    annotations.JoinBuildCopy,
			Start:   buildStart,
			End:     buildEnd,
			Latency: buildEnd.Sub(buildStart),
			Data: map[string]interface{}{
				"copied":        copyCount,
				"passthru":      passthruCount,
				"requires_copy": needsCopy,
			},
		})
	}
	if opts.Handler != nil {
		opts.Handler(annotations.Event{
			Name: annotations.JoinBuild,
			Data: map[string]interface{}{
				"tuple_count":     buildCount,
				"join_key_unique": buildKeysUnique,
				"build_side":      buildSide,
				"copied":          copyCount,
				"passthru":        passthruCount,
				"requires_copy":   needsCopy,
			},
		})
	}

	// Probe phase - find matches
	// Check if streaming mode is enabled
	if opts.EnableStreamingJoins {
		// Return streaming relation with lazy evaluation
		var resultSeen *TupleKeyMap
		if len(resultProperties.Keys) == 0 {
			// Handle unknown sizes (-1) with reasonable default.
			expectedResults := probeRel.Size()
			if expectedResults < 0 {
				expectedResults = defaultCapacity
			}
			buildSize := buildRel.Size()
			if buildSize > 0 && buildSize < expectedResults {
				expectedResults = buildSize
			}
			resultSeen = NewTupleKeyMapWithCapacity(expectedResults)
		}

		iter := &hashJoinIterator{
			buildIndex:          buildIndex,
			probeIt:             probeRel.Iterator(),
			buildErr:            buildErr,
			seen:                resultSeen,
			buildIsLeft:         buildIsLeft,
			probeNeedsCopy:      probeRel.RequiresCopy(),
			rightNonJoinIndices: rightNonJoinIndices,
			resultWidth:         resultWidth,
			options:             opts,
			matchIdx:            0,
		}
		if opts.Handler != nil {
			iter.metrics = &hashJoinMetrics{}
		}

		// Return streaming result - no forced materialization
		// StreamingRelation enforces single-use semantics via panic if Iterator() called twice
		// Caller can explicitly call Materialize() if multiple iterations needed
		return NewStreamingRelationWithProperties(outputSyms, iter, opts, resultProperties)
	}

	// Materialized mode (original implementation)
	// Use efficient TupleKeyMap for deduplication
	var seen *TupleKeyMap
	if len(resultProperties.Keys) == 0 {
		// Pre-size seen map - worst case is probe size, but likely smaller due
		// to filtering. Use min(probeSize, buildSize) as estimate.
		expectedResults := probeRel.Size()
		if expectedResults < 0 {
			expectedResults = defaultCapacity
		}
		probeBuildSize := buildRel.Size()
		if probeBuildSize > 0 && probeBuildSize < expectedResults {
			expectedResults = probeBuildSize
		}
		seen = NewTupleKeyMapWithCapacity(expectedResults)
	}
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

	probeCount := 0
	matchCount := 0
	for probeIt.Next() {
		probeTuple := probeIt.Tuple()
		probeCount++

		// Positional probe against the grouped build tuples — no key
		// materializes, and the hit is a subslice of the shared backing.
		matches := buildIndex.probe(probeTuple)
		if len(matches) == 0 {
			continue
		}
		matchCount++
		for _, buildTuple := range matches {
			// Combine tuples via the precomputed projection plan.
			var joined Tuple
			if buildIsLeft {
				joined = combineTuplesIndexed(buildTuple, probeTuple, rightNonJoinIndices, resultWidth)
			} else {
				joined = combineTuplesIndexed(probeTuple, buildTuple, rightNonJoinIndices, resultWidth)
			}

			// A nil seen map means the result candidate key proves this
			// joined tuple cannot duplicate any prior output.
			if seen == nil || !seen.PutIfAbsent(NewTupleKeyFull(joined), true) {
				results = append(results, joined)
			}
		}
	}
	probeErr := probeIt.Error()
	if closeErr := probeIt.Close(); probeErr == nil {
		probeErr = closeErr
	}
	if opts.Handler != nil {
		opts.Handler(annotations.Event{
			Name: annotations.JoinProbe,
			Data: map[string]interface{}{
				"tuple_count":   probeCount,
				"matched_count": matchCount,
				"result_count":  len(results),
				"mode":          "materialized",
			},
		})
	}

	// We already deduplicated with 'seen', no need to do it again.
	// Carry any deferred build/probe error onto the result so a failed scan
	// isn't laundered into an empty/partial join.
	result := newMaterializedRelationFromSet(outputSyms, results, opts, resultProperties)
	if buildErr != nil {
		result.err = buildErr
	} else if probeErr != nil {
		result.err = probeErr
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
	if !opts.populated() {
		opts = right.Options()
	}

	// Build set of keys from right relation using efficient TupleKeyMap.
	// If the right side fails, the key set is incomplete — every filter decision
	// becomes untrustworthy (semi-join would drop real matches), so surface the
	// error and trust no result tuples.
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

	// Filter left relation — positional membership probes materialize no key.
	var results []Tuple
	leftNeedsCopy := left.RequiresCopy()
	leftIt := left.Iterator()
	for leftIt.Next() {
		tuple := leftIt.Tuple()
		if _, ok := rightKeys.GetPositions(tuple, leftIndices); ok {
			if leftNeedsCopy {
				results = append(results, copyTuple(tuple))
			} else {
				results = append(results, tuple)
			}
		}
	}
	lerr := leftIt.Error()
	if cerr := leftIt.Close(); lerr == nil {
		lerr = cerr
	}

	res := materializeFilteredLeft(left, results, opts)
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
	if !opts.populated() {
		opts = right.Options()
	}

	// Build set of keys from right relation using efficient TupleKeyMap.
	// If the right side fails, the key set is incomplete — an anti-join would then
	// report false "no match" tuples (a missing key from a decode failure is
	// indistinguishable from a real absence), so surface the error and trust no
	// result tuples.
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

	// Filter left relation — positional membership probes materialize no key.
	var results []Tuple
	leftNeedsCopy := left.RequiresCopy()
	leftIt := left.Iterator()
	for leftIt.Next() {
		tuple := leftIt.Tuple()
		if _, ok := rightKeys.GetPositions(tuple, leftIndices); !ok {
			if leftNeedsCopy {
				results = append(results, copyTuple(tuple))
			} else {
				results = append(results, tuple)
			}
		}
	}
	lerr := leftIt.Error()
	if cerr := leftIt.Close(); lerr == nil {
		lerr = cerr
	}

	res := materializeFilteredLeft(left, results, opts)
	res.err = lerr
	return res
}

func materializeFilteredLeft(
	left Relation,
	tuples []Tuple,
	opts ExecutorOptions,
) *MaterializedRelation {
	properties := left.Properties()
	if len(properties.Keys) > 0 {
		return newMaterializedRelationFromSet(left.Symbols(), tuples, opts, properties)
	}
	return NewMaterializedRelationWithProperties(left.Symbols(), tuples, opts, properties)
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
// HashJoinWithOptions and reused for every matched tuple. This replaces the
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
	if !opts.populated() {
		opts = right.Options()
	}

	outputSyms := append(left.Symbols(), right.Symbols()...)
	var results []Tuple
	var scanErr error

	leftIt := left.Iterator()

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
		if err := rightIt.Error(); err != nil && scanErr == nil {
			scanErr = err
		}
		if closeErr := rightIt.Close(); closeErr != nil && scanErr == nil {
			scanErr = closeErr
		}
	}
	if err := leftIt.Error(); err != nil && scanErr == nil {
		scanErr = err
	}
	if closeErr := leftIt.Close(); closeErr != nil && scanErr == nil {
		scanErr = closeErr
	}

	// A failed scan is not an empty side — carry it as the result's
	// deferred error.
	result := NewMaterializedRelationWithOptions(outputSyms, results, opts)
	result.err = scanErr
	return result
}
