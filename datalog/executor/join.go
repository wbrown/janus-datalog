package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// hashJoinIterator is the join's iterator: the build index, the probe
// iterator, the deferred build error, the projection plan, the current probe
// tuple, the current match group and its position, and one workspace of the
// result's width that every pull fills and Tuple hands back
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, L1).
//
// CONCURRENCY: This iterator is NOT thread-safe. It maintains mutable state
// (currentProbeTuple, workspace, matches, matchIdx) that would be corrupted
// by concurrent access. Each goroutine must create its own iterator by calling
// Relation.Iterator(), which returns independent iterator instances.
type hashJoinIterator struct {
	buildIndex  *JoinIndex
	probeIt     Iterator
	buildErr    error // deferred error captured from the (eagerly consumed) build relation
	buildIsLeft bool
	// rightNonJoinIndices identifies positions in the right-side tuple
	// (per buildIsLeft, the probe side when buildIsLeft is true, otherwise
	// the build side) whose values are written after the left tuple in the
	// workspace. Computed once at iterator setup; the fill inner loop is a
	// pure indexed gather, never a per-call symbol scan.
	rightNonJoinIndices []int
	options             ExecutorOptions
	// workspace is the one slice of the result's width every pull fills and
	// Tuple hands back. The next pull overwrites it, so RequiresCopy answers
	// true and a consumer keeping a tuple copies it.
	workspace Tuple

	// Current state - NOT safe for concurrent access
	currentProbeTuple Tuple
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

			// Fill the workspace through the precomputed projection plan: the
			// left tuple whole, then the right tuple's values at the plan's
			// positions. Every combined tuple is one the join answers: the join
			// of two sets is a set (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T1).
			first, second := buildTuple, it.currentProbeTuple
			if !it.buildIsLeft {
				first, second = it.currentProbeTuple, buildTuple
			}
			base := copy(it.workspace, first)
			for k, idx := range it.rightNonJoinIndices {
				it.workspace[base+k] = second[idx]
			}
			if it.metrics != nil {
				it.metrics.resultCount++
			}
			return true
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
		// The probe tuple is read as the probe iterator hands it back. Its
		// values are read into the workspace at each pull of this iterator,
		// and the probe iterator is pulled again only once the match group is
		// exhausted, so a probe that reuses its buffer overwrites nothing this
		// iterator still reads (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, L2).
		it.currentProbeTuple = it.probeIt.Tuple()

		// Look up matches in the build index — a positional probe, so no key
		// materializes, and a hit is the key's group of held tuples.
		if matches := it.buildIndex.Probe(it.currentProbeTuple); len(matches) > 0 {
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
	return it.workspace
}

// RequiresCopy is true: Tuple hands back the workspace, which the next pull
// overwrites.
func (it *hashJoinIterator) RequiresCopy() bool { return true }

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
	// Check if we should use symmetric hash join for streaming relations
	if opts.EnableSymmetricHashJoin {
		strategy := ChooseJoinStrategy(left, right, joinSyms, opts)
		if strategy == "symmetric" {
			return SymmetricHashJoinWithOptions(left, right, joinSyms, opts)
		}
	}

	// A join symbol a side lacks is the query's error, carried by the result
	// and surfaced when it is driven.
	leftIndices, err := query.SymbolIndexTable(left.Symbols(), joinSyms)
	var rightIndices []int
	if err == nil {
		rightIndices, err = query.SymbolIndexTable(right.Symbols(), joinSyms)
	}
	if err != nil {
		if !opts.populated() {
			opts = left.Options()
		}
		if !opts.populated() {
			opts = right.Options()
		}
		result := NewMaterializedRelationWithOptions(left.Symbols(), nil, opts)
		result.err = fmt.Errorf("hash join: %w", err)
		return result
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
	buildSide := "right"
	if buildIsLeft {
		buildSide = "left"
	}
	if opts.Handler != nil {
		emitJoinStrategyAnnotation(opts.Handler, left, right, joinSyms, "streaming", buildSide, buildKeysUnique)
	}

	// Build phase - hold each build tuple in the index under its join key.
	// This is a pure relational join: every build tuple is preserved. CRDT/temporal
	// resolution is the storage layer's responsibility (EATV ordering), never
	// inferred here from a symbol's name.
	// Pre-size based on build relation size to avoid growth
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
	buildIndex := NewJoinIndex(probeIndices, buildIndices, collectCap)

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
	// to avoid a per-join heap allocation.
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
		buildIndex.Add(tuple)
	}
	buildCount := buildIndex.Len()

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

	if buildKeysUnique && !buildIndex.KeysUnique() {
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

	// Probe phase. An index holding no tuple matches no probe tuple
	// (JoinIndex.Probe answers nil for every key), so the join is empty
	// whatever the probe side holds and the probe side is never opened: a
	// stream among the probe sides keeps its one use
	// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T5). The build side was
	// drained, so its deferred error is in hand and the result carries it.
	if buildCount == 0 {
		if opts.Handler != nil {
			opts.Handler(annotations.Event{
				Name: annotations.JoinProbe,
				Data: map[string]interface{}{
					"tuple_count":   0,
					"matched_count": 0,
					"result_count":  0,
					"mode":          "streaming",
				},
			})
		}
		result := newMaterializedRelationFromSet(outputSyms, nil, opts, resultProperties)
		result.err = buildErr
		return result
	}

	// The result streams: every pull fills the iterator's workspace with one
	// combined tuple, so the join holds the build index and nothing of its
	// output (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T3 and T4).
	// StreamingRelation enforces single-use semantics via panic if Iterator()
	// is called twice; a caller reading the result twice materializes it.
	iter := &hashJoinIterator{
		buildIndex:          buildIndex,
		probeIt:             probeRel.Iterator(),
		buildErr:            buildErr,
		buildIsLeft:         buildIsLeft,
		rightNonJoinIndices: rightNonJoinIndices,
		options:             opts,
		workspace:           make(Tuple, resultWidth),
	}
	if opts.Handler != nil {
		iter.metrics = &hashJoinMetrics{}
	}
	return NewStreamingRelationWithProperties(outputSyms, iter, opts, resultProperties)
}

// SemiJoin returns tuples from left that have matches in right
func SemiJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	// Extract options from left relation
	opts := left.Options()
	if !opts.populated() {
		opts = right.Options()
	}

	// A join symbol a side lacks is the query's error, carried by the result
	// and surfaced when it is driven.
	leftIndices, err := query.SymbolIndexTable(left.Symbols(), joinSyms)
	var rightIndices []int
	if err == nil {
		rightIndices, err = query.SymbolIndexTable(right.Symbols(), joinSyms)
	}
	if err != nil {
		res := NewMaterializedRelationWithOptions(left.Symbols(), nil, opts)
		res.err = fmt.Errorf("semi join: %w", err)
		return res
	}

	// The right side's keys, probed by left tuples at the left's join
	// positions: each member is the right tuple itself at the right's join
	// positions when the right side's tuples are stable, and a copy of the
	// key's values at the positions 0 to k−1 when the right side reuses its
	// workspace (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, L2). If the right
	// side fails, the key set is incomplete — every filter decision becomes
	// untrustworthy (semi-join would drop real matches), so surface the error
	// and trust no result tuples.
	copyKeys := right.RequiresCopy()
	storedPos := rightIndices
	if copyKeys {
		storedPos = allPositions(len(rightIndices))
	}
	rightKeys := NewTupleSet(leftIndices, storedPos, right.Size())
	rightIt := right.Iterator()
	for rightIt.Next() {
		tuple := rightIt.Tuple()
		if _, held := rightKeys.FindAt(tuple, rightIndices); held {
			continue
		}
		if copyKeys {
			key := make(Tuple, len(rightIndices))
			for i, idx := range rightIndices {
				key[i] = tuple[idx]
			}
			tuple = key
		}
		rightKeys.Add(tuple)
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
		if rightKeys.Probe(tuple) {
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

	// A selection of a set is a set
	// (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T5).
	res := newMaterializedRelationFromSet(left.Symbols(), results, opts, left.Properties())
	res.err = lerr
	return res
}

// AntiJoin returns tuples from left that have no matches in right
func AntiJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	// Extract options from left relation
	opts := left.Options()
	if !opts.populated() {
		opts = right.Options()
	}

	// A join symbol a side lacks is the query's error, carried by the result
	// and surfaced when it is driven.
	leftIndices, err := query.SymbolIndexTable(left.Symbols(), joinSyms)
	var rightIndices []int
	if err == nil {
		rightIndices, err = query.SymbolIndexTable(right.Symbols(), joinSyms)
	}
	if err != nil {
		res := NewMaterializedRelationWithOptions(left.Symbols(), nil, opts)
		res.err = fmt.Errorf("anti join: %w", err)
		return res
	}

	// The right side's keys, probed by left tuples at the left's join
	// positions: each member is the right tuple itself at the right's join
	// positions when the right side's tuples are stable, and a copy of the
	// key's values at the positions 0 to k−1 when the right side reuses its
	// workspace (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, L2). If the right
	// side fails, the key set is incomplete — an anti-join would then report
	// false "no match" tuples (a missing key from a decode failure is
	// indistinguishable from a real absence), so surface the error and trust
	// no result tuples.
	copyKeys := right.RequiresCopy()
	storedPos := rightIndices
	if copyKeys {
		storedPos = allPositions(len(rightIndices))
	}
	rightKeys := NewTupleSet(leftIndices, storedPos, right.Size())
	rightIt := right.Iterator()
	for rightIt.Next() {
		tuple := rightIt.Tuple()
		if _, held := rightKeys.FindAt(tuple, rightIndices); held {
			continue
		}
		if copyKeys {
			key := make(Tuple, len(rightIndices))
			for i, idx := range rightIndices {
				key[i] = tuple[idx]
			}
			tuple = key
		}
		rightKeys.Add(tuple)
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
		if !rightKeys.Probe(tuple) {
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

	// A selection of a set is a set
	// (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T5).
	res := newMaterializedRelationFromSet(left.Symbols(), results, opts, left.Properties())
	res.err = lerr
	return res
}

// Join utility functions

func isStreaming(rel Relation) bool {
	_, ok := rel.(*StreamingRelation)
	return ok
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
