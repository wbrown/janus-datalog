package executor

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// UnionRelation streams results from multiple relations with deduplication.
// Key insight: Consumes relations from a channel as they're produced (by worker pool),
// iterating each one and discarding it before moving to the next.
//
// Memory profile: N workers producing + 1 relation being iterated + dedup map
// For OHLC with 8 workers: ~8 results in flight + 1 being iterated + ~200 unique tuples in dedup
//
// IMPORTANT: Channels can only be consumed once, but Relations must be reusable (multiple Iterator() calls).
// Solution: First Iterator() call consumes channel and caches results, subsequent calls replay from cache.
type UnionRelation struct {
	source     <-chan relationItem
	symbols    []query.Symbol
	opts       ExecutorOptions
	cached     []Tuple       // Final cache, published once cacheBuilt; immutable after
	cacheBuilt bool          // Build complete: cached is final
	building   bool          // A build is in progress (one builder owns the channel)
	buildDone  chan struct{} // Closed when the build completes; created when building starts
	buildErr   error         // First error from the build, surfaced to replay iterators
	cacheMutex sync.Mutex    // Guards building/cacheBuilt/cached/buildErr transitions
}

// relationItem holds either a relation or an error from subquery execution
type relationItem struct {
	relation Relation
	err      error
}

// NewUnionRelation creates a union relation that consumes from a channel
func NewUnionRelation(source <-chan relationItem, symbols []query.Symbol, opts ExecutorOptions) *UnionRelation {
	return &UnionRelation{
		source:  source,
		symbols: symbols,
		opts:    opts,
	}
}

// Symbols returns the symbol names of this relation
func (ur *UnionRelation) Symbols() []query.Symbol {
	return ur.symbols
}

func (ur *UnionRelation) Properties() RelationProperties {
	return deduplicatedProperties(ur.symbols)
}

// Iterator returns an iterator that consumes from the channel (first call) or cache (subsequent calls)
func (ur *UnionRelation) Iterator() Iterator {
	ur.cacheMutex.Lock()
	defer ur.cacheMutex.Unlock()

	// Build complete: replay the final cache. The slice iterator carries any
	// build error so Error() surfaces it on every replay.
	if ur.cacheBuilt {
		return &sliceIterator{tuples: ur.cached, pos: -1, err: ur.buildErr}
	}

	// A build is already in progress: this is a concurrent caller. It must NOT
	// touch the one-shot channel — only the sole builder consumes it. Return a
	// replay iterator that blocks until the build completes, then replays the
	// complete cache.
	if ur.building {
		return &unionReplayWaitIterator{done: ur.buildDone, ur: ur}
	}

	// First caller becomes the sole builder: it streams the channel, dedups, and
	// builds the cache, then publishes it via finishBuild.
	ur.building = true
	ur.buildDone = make(chan struct{})
	return newUnionBuildIterator(ur.source, ur)
}

// finishBuild publishes the completed cache and wakes any waiting replay
// iterators. Idempotent: only the first call (builder exhaustion or Close)
// publishes the cache and closes buildDone.
func (ur *UnionRelation) finishBuild(cache []Tuple, buildErr error) {
	ur.cacheMutex.Lock()
	if !ur.cacheBuilt {
		ur.cached = cache
		ur.buildErr = buildErr
		ur.cacheBuilt = true
		ur.building = false
		close(ur.buildDone)
	}
	ur.cacheMutex.Unlock()
}

// Size forces materialization to count tuples (expensive!)
func (ur *UnionRelation) Size() int {
	return ur.Materialize().Size()
}

// IsEmpty checks if there are any results
func (ur *UnionRelation) IsEmpty() bool {
	// Try to get first tuple
	it := ur.Iterator()
	defer it.Close()
	return !it.Next()
}

// Get forces materialization (expensive!)
func (ur *UnionRelation) Get(i int) Tuple {
	return ur.Materialize().Get(i)
}

// String returns a string representation
func (ur *UnionRelation) String() string {
	return ur.Materialize().String()
}

// Table returns a formatted table
func (ur *UnionRelation) Table() string {
	return ur.Materialize().Table()
}

// ProjectFromPattern projects symbols based on pattern
func (ur *UnionRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return ur.Materialize().ProjectFromPattern(pattern)
}

// Sorted returns sorted tuples (forces materialization)
func (ur *UnionRelation) Sorted() ([]Tuple, error) {
	return ur.Materialize().Sorted()
}

// Project returns a projection of this relation
func (ur *UnionRelation) Project(symbols []query.Symbol) (Relation, error) {
	return ur.Materialize().Project(symbols)
}

// Materialize forces consumption of all relations and returns a materialized result.
// collectTuplesInto captures both deferred iteration errors and the Close() error
// of the union iterator, and the first non-nil one is attached to the returned
// relation as mat.err so it replays via Iterator().Error() at the next public
// boundary. The Relation interface's lack of an (error) return doesn't drop
// errors — they ride the result.
func (ur *UnionRelation) Materialize() Relation {
	var allTuples []Tuple
	err := collectTuplesInto(&allTuples, ur)
	mat := newMaterializedRelationFromSet(
		ur.symbols,
		allTuples,
		ur.opts,
		ur.Properties(),
	)
	mat.err = err
	return mat
}

// Sort returns a sorted relation (forces materialization)
func (ur *UnionRelation) Sort(orderBy []query.OrderByClause) Relation {
	return ur.Materialize().Sort(orderBy)
}

// Filter returns a filtered relation
func (ur *UnionRelation) Filter(filter Filter) Relation {
	return ur.Materialize().Filter(filter)
}

// FilterWithPredicate returns a filtered relation
func (ur *UnionRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return ur.Materialize().FilterWithPredicate(pred)
}

// EvaluateFunction evaluates a function
func (ur *UnionRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	return ur.Materialize().EvaluateFunction(fn, outputSymbol)
}

// Select returns tuples matching predicate
func (ur *UnionRelation) Select(pred func(Tuple) bool) Relation {
	return ur.Materialize().Select(pred)
}

// Join performs a natural join
func (ur *UnionRelation) Join(other Relation) Relation {
	return ur.Materialize().Join(other)
}

// HashJoin performs a hash join
func (ur *UnionRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return ur.Materialize().HashJoin(other, joinSyms)
}

// SemiJoin performs a semi-join
func (ur *UnionRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return ur.Materialize().SemiJoin(other, joinSyms)
}

// AntiJoin performs an anti-join
func (ur *UnionRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return ur.Materialize().AntiJoin(other, joinSyms)
}

// Aggregate performs aggregation
func (ur *UnionRelation) Aggregate(findElements []query.FindElement) Relation {
	return ur.Materialize().Aggregate(findElements)
}

// Options returns executor options
func (ur *UnionRelation) Options() ExecutorOptions {
	return ur.opts
}

// RequiresCopy returns false because UnionIterator copies tuples from
// sources that have RequiresCopy() = true at the boundary.
func (ur *UnionRelation) RequiresCopy() bool {
	return false
}

// UnionIterator consumes relations from a channel and iterates with deduplication
// KEY: Only ONE relation held in memory at a time (plus dedup map)
// ALSO: Builds cache as a side effect for subsequent Iterator() calls
type UnionIterator struct {
	source          <-chan relationItem
	currentIter     Iterator
	currentRelation Relation     // Track current relation for RequiresCopy check
	seen            *TupleKeyMap // Deduplication without materialization
	currentTuple    Tuple
	exhausted       bool
	firstError      error          // Track first error encountered
	ur              *UnionRelation // Owner; receives the published cache on completion
	cache           []Tuple        // Cache built locally; published via ur.finishBuild
}

// newUnionBuildIterator creates the sole builder iterator: it consumes the
// channel, dedups, builds the cache locally, and publishes it to ur on
// completion (channel exhaustion or Close).
func newUnionBuildIterator(source <-chan relationItem, ur *UnionRelation) *UnionIterator {
	return &UnionIterator{
		source: source,
		seen:   NewTupleKeyMap(),
		ur:     ur,
	}
}

// Next advances to the next unique tuple
func (it *UnionIterator) Next() bool {
	if it.exhausted {
		return false
	}

	for {
		// Try to get next tuple from current relation
		if it.currentIter != nil && it.currentIter.Next() {
			tuple := it.currentIter.Tuple()

			// Copy tuple if source relation reuses workspace memory
			if it.currentRelation != nil && it.currentRelation.RequiresCopy() {
				tuple = copyTuple(tuple)
			}

			// Check if we've seen this tuple before (deduplication)
			key := NewTupleKeyFull(tuple)
			if !it.seen.PutIfAbsent(key, true) {
				it.currentTuple = tuple

				// Accumulate into the local cache; published on completion.
				it.cache = append(it.cache, tuple)

				return true
			}
			// Duplicate - keep searching
			continue
		}

		// Current iterator exhausted - capture any deferred error before
		// discarding it (otherwise UnionIterator.Error() loses it once
		// currentIter is nil), then close and get the next relation.
		if it.currentIter != nil {
			if e := it.currentIter.Error(); e != nil && it.firstError == nil {
				it.firstError = e
			}
			it.currentIter.Close()
			it.currentIter = nil
			it.currentRelation = nil
		}

		// Read next relation from channel
		item, ok := <-it.source
		if !ok {
			// Channel closed - all relations consumed. Publish the cache and
			// wake any replay iterators waiting on the build.
			it.exhausted = true
			it.ur.finishBuild(it.cache, it.firstError)
			return false
		}

		// Check for error
		if item.err != nil {
			// Track first error but continue processing other relations
			if it.firstError == nil {
				it.firstError = item.err
			}
			continue
		}

		// Skip nil relations
		if item.relation == nil {
			continue
		}

		// Set up iterator for this relation
		// Don't check IsEmpty() - it might consume the iterator!
		// If the relation is empty, Next() will return false and we'll move to the next one
		it.currentRelation = item.relation
		it.currentIter = item.relation.Iterator()
		// Loop back to get first tuple from this relation
	}
}

// Tuple returns the current tuple
func (it *UnionIterator) Tuple() Tuple {
	return it.currentTuple
}

// Close releases resources. If the builder is abandoned before the channel is
// exhausted, it drains the remainder INTO the cache (by running Next to
// completion) so the published cache is complete for replay/reuse and any
// waiting replay iterators do not block forever. Draining also unblocks
// producers still sending on the channel.
func (it *UnionIterator) Close() error {
	if !it.exhausted {
		for it.Next() {
			// Finish building the cache; the exhaustion path publishes it.
		}
	}
	if it.currentIter != nil {
		it.currentIter.Close()
		it.currentIter = nil
	}
	return it.firstError
}

func (it *UnionIterator) Error() error {
	if it.firstError != nil {
		return it.firstError
	}
	if it.currentIter != nil {
		return it.currentIter.Error()
	}
	return nil
}

// unionReplayWaitIterator is returned to a concurrent caller that arrives while
// the sole builder is still streaming the channel. On first use it blocks until
// the build completes, then replays the complete published cache. It never
// touches the channel, so the channel has exactly one consumer (the builder).
type unionReplayWaitIterator struct {
	done <-chan struct{}
	ur   *UnionRelation
	iter *sliceIterator // set once the build completes
}

func (it *unionReplayWaitIterator) ensure() {
	if it.iter == nil {
		<-it.done // wait for the builder to publish the cache
		it.ur.cacheMutex.Lock()
		it.iter = &sliceIterator{tuples: it.ur.cached, pos: -1, err: it.ur.buildErr}
		it.ur.cacheMutex.Unlock()
	}
}

func (it *unionReplayWaitIterator) Next() bool {
	it.ensure()
	return it.iter.Next()
}

func (it *unionReplayWaitIterator) Tuple() Tuple {
	return it.iter.Tuple()
}

func (it *unionReplayWaitIterator) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

func (it *unionReplayWaitIterator) Error() error {
	if it.iter != nil {
		return it.iter.Error()
	}
	return nil
}
