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
	source       <-chan relationItem
	columns      []query.Symbol
	opts         ExecutorOptions
	cached       []Tuple // Cache for reuse after first iteration
	cacheBuilt   bool    // Has cache been built?
	cacheMutex   sync.Mutex // Protect cache building
}

// relationItem holds either a relation or an error from subquery execution
type relationItem struct {
	relation Relation
	err      error
}

// NewUnionRelation creates a union relation that consumes from a channel
func NewUnionRelation(source <-chan relationItem, columns []query.Symbol, opts ExecutorOptions) *UnionRelation {
	return &UnionRelation{
		source:  source,
		columns: columns,
		opts:    opts,
	}
}

// Columns returns the column names
func (ur *UnionRelation) Columns() []query.Symbol {
	return ur.columns
}

// Symbols returns the symbols (same as Columns)
func (ur *UnionRelation) Symbols() []query.Symbol {
	return ur.columns
}

// Iterator returns an iterator that consumes from the channel (first call) or cache (subsequent calls)
func (ur *UnionRelation) Iterator() Iterator {
	ur.cacheMutex.Lock()
	defer ur.cacheMutex.Unlock()

	// If cache is already built, return a simple slice iterator over cached tuples
	if ur.cacheBuilt {
		return &sliceIterator{
			tuples: ur.cached,
			pos:    -1,
		}
	}

	// First call - need to consume channel and build cache

	// Create iterator that will build cache as a side effect
	return NewUnionIteratorWithCache(ur.source, &ur.cached, &ur.cacheBuilt)
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

// ProjectFromPattern projects columns based on pattern
func (ur *UnionRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return ur.Materialize().ProjectFromPattern(pattern)
}

// Sorted returns sorted tuples (forces materialization)
func (ur *UnionRelation) Sorted() []Tuple {
	return ur.Materialize().Sorted()
}

// Project returns a projection of this relation
func (ur *UnionRelation) Project(columns []query.Symbol) (Relation, error) {
	return ur.Materialize().Project(columns)
}

// Materialize forces consumption of all relations and returns a materialized result
// Note: This doesn't return errors because Relation.Materialize() doesn't have error return
// Errors from Close() are silently dropped (limitation of Relation interface)
func (ur *UnionRelation) Materialize() Relation {
	var allTuples []Tuple
	it := ur.Iterator()
	defer it.Close() // Errors silently dropped

	for it.Next() {
		allTuples = append(allTuples, it.Tuple())
	}

	return NewMaterializedRelation(ur.columns, allTuples)
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
func (ur *UnionRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	return ur.Materialize().EvaluateFunction(fn, outputColumn)
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
func (ur *UnionRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return ur.Materialize().HashJoin(other, joinCols)
}

// SemiJoin performs a semi-join
func (ur *UnionRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	return ur.Materialize().SemiJoin(other, joinCols)
}

// AntiJoin performs an anti-join
func (ur *UnionRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	return ur.Materialize().AntiJoin(other, joinCols)
}

// Aggregate performs aggregation
func (ur *UnionRelation) Aggregate(findElements []query.FindElement) Relation {
	return ur.Materialize().Aggregate(findElements)
}

// Options returns executor options
func (ur *UnionRelation) Options() ExecutorOptions {
	return ur.opts
}

// UnionIterator consumes relations from a channel and iterates with deduplication
// KEY: Only ONE relation held in memory at a time (plus dedup map)
// ALSO: Builds cache as a side effect for subsequent Iterator() calls
type UnionIterator struct {
	source       <-chan relationItem
	currentIter  Iterator
	seen         *TupleKeyMap // Deduplication without materialization
	currentTuple Tuple
	exhausted    bool
	firstError   error // Track first error encountered
	cache        *[]Tuple // Pointer to cache to build
	cacheBuilt   *bool    // Pointer to flag
}

// NewUnionIteratorWithCache creates a new union iterator that builds cache as it iterates
func NewUnionIteratorWithCache(source <-chan relationItem, cache *[]Tuple, cacheBuilt *bool) *UnionIterator {
	return &UnionIterator{
		source:     source,
		seen:       NewTupleKeyMap(),
		exhausted:  false,
		cache:      cache,
		cacheBuilt: cacheBuilt,
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

			// Check if we've seen this tuple before (deduplication)
			key := NewTupleKeyFull(tuple)
			if !it.seen.Exists(key) {
				// New unique tuple - mark as seen
				it.seen.Put(key, true)
				it.currentTuple = tuple

				// Add to cache if we're building it
				if it.cache != nil {
					*it.cache = append(*it.cache, tuple)
				}

				return true
			}
			// Duplicate - keep searching
			continue
		}

		// Current iterator exhausted - close it and get next relation
		if it.currentIter != nil {
			it.currentIter.Close()
			it.currentIter = nil
		}

		// Read next relation from channel
		item, ok := <-it.source
		if !ok {
			// Channel closed - all relations consumed
			it.exhausted = true

			// Mark cache as built
			if it.cacheBuilt != nil {
				*it.cacheBuilt = true
			}

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
		it.currentIter = item.relation.Iterator()
		// Loop back to get first tuple from this relation
	}
}

// Tuple returns the current tuple
func (it *UnionIterator) Tuple() Tuple {
	return it.currentTuple
}

// Close releases resources
func (it *UnionIterator) Close() error {
	if it.currentIter != nil {
		it.currentIter.Close()
	}
	// Drain remaining items from channel to unblock producers
	for range it.source {
		// Discard remaining items
	}
	return it.firstError
}
