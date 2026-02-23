package executor

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Tuple is an alias for query.Tuple to maintain backward compatibility
type Tuple = query.Tuple

// copyTuple returns a copy of the tuple. Required because iterator
// Tuple() returns a workspace that gets reused on each Next() call.
func copyTuple(t Tuple) Tuple {
	c := make(Tuple, len(t))
	copy(c, t)
	return c
}

// collectTuplesInto appends all tuples from a relation into the destination slice.
// It checks RequiresCopy() to avoid unnecessary copying when the relation
// guarantees stable tuple references (e.g., MaterializedRelation).
func collectTuplesInto(dest *[]Tuple, rel Relation) {
	needsCopy := rel.RequiresCopy()
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		if needsCopy {
			tuple = copyTuple(tuple)
		}
		*dest = append(*dest, tuple)
	}
	it.Close()
}

// CollectTuples materializes a Relation into [][]interface{}.
// Accepts the (Relation, error) pair returned by Query() directly:
//
//	results, err := executor.CollectTuples(db.Query(...))
func CollectTuples(rel Relation, err error) ([][]interface{}, error) {
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return [][]interface{}{}, nil
	}
	var tuples [][]interface{}
	it := rel.Iterator()
	defer it.Close()
	for it.Next() {
		src := it.Tuple()
		t := make([]interface{}, len(src))
		copy(t, src)
		tuples = append(tuples, t)
	}
	if tuples == nil {
		tuples = [][]interface{}{}
	}
	return tuples, nil
}

// Relation represents a set of tuples with named symbols
type Relation interface {
	// Symbols returns the symbols (attribute names) of this relation
	// In relational theory, a tuple is a map from symbols to values
	Symbols() []query.Symbol

	// Iterator returns an iterator over tuples
	Iterator() Iterator

	// Size returns the number of tuples (may be expensive for iterators)
	Size() int

	// IsEmpty returns true if the relation has no tuples
	IsEmpty() bool

	// Get returns a specific tuple by index (may be expensive for streaming relations)
	Get(i int) Tuple

	// String returns a compact string representation for annotations/logging
	String() string

	// Table returns a formatted markdown table representation
	Table() string

	// Project creates a new Relation with only the symbols from the pattern
	// that exist in this Relation, in the order they appear in the pattern
	ProjectFromPattern(pattern *query.DataPattern) Relation

	// Sorted returns tuples sorted by the relation's symbols
	// First symbol is primary sort key, second is secondary, etc.
	Sorted() []Tuple

	// Project returns a new relation with only the specified symbols
	// Returns an error if any requested symbol doesn't exist
	Project(symbols []query.Symbol) (Relation, error)

	// Materialize converts a streaming relation to a materialized one
	// For already-materialized relations, returns self
	Materialize() Relation

	// Sort returns a new relation sorted by the specified order-by clauses
	Sort(orderBy []query.OrderByClause) Relation

	// Filter returns a new relation with only tuples that satisfy the filter
	Filter(filter Filter) Relation

	// FilterWithPredicate returns a new relation filtered by a query.Predicate
	FilterWithPredicate(pred query.Predicate) Relation

	// EvaluateFunction evaluates a function and adds its result as a new symbol
	EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation

	// Select returns a new relation with only tuples that satisfy the predicate
	Select(pred func(Tuple) bool) Relation

	// Join performs a natural join with another relation
	Join(other Relation) Relation

	// HashJoin performs an equi-join on specified symbols
	HashJoin(other Relation, joinSyms []query.Symbol) Relation

	// SemiJoin returns tuples from this relation that have matches in the other
	SemiJoin(other Relation, joinSyms []query.Symbol) Relation

	// AntiJoin returns tuples from this relation that have no matches in the other
	AntiJoin(other Relation, joinSyms []query.Symbol) Relation

	// Aggregate performs aggregation operations
	Aggregate(findElements []query.FindElement) Relation

	// Options returns the executor options for this relation
	// Used by join operations to extract configuration
	Options() ExecutorOptions

	// RequiresCopy returns true if tuples from Iterator() must be copied
	// before storing, because the iterator reuses internal workspace memory.
	// MaterializedRelation returns false (tuples are independent).
	// StreamingRelation returns true (iterator may reuse workspace).
	RequiresCopy() bool

	// Note: Relations are IMMUTABLE and DEDUPLICATED at creation
	// All operations return NEW Relations
}

// Iterator provides streaming access to tuples
type Iterator interface {
	// Next advances to the next tuple
	Next() bool

	// Tuple returns the current tuple
	Tuple() Tuple

	// Close releases any resources
	Close() error
}

// CountingIterator wraps an iterator and tracks tuple count without buffering
type CountingIterator struct {
	inner Iterator
	count int
	done  bool
}

// NewCountingIterator creates a counting iterator wrapper
func NewCountingIterator(inner Iterator) *CountingIterator {
	return &CountingIterator{
		inner: inner,
		count: 0,
		done:  false,
	}
}

func (i *CountingIterator) Next() bool {
	hasNext := i.inner.Next()
	if hasNext {
		i.count++
	} else {
		i.done = true
	}
	return hasNext
}

func (i *CountingIterator) Tuple() Tuple {
	return i.inner.Tuple()
}

func (i *CountingIterator) Close() error {
	return i.inner.Close()
}

// Count returns the number of tuples seen so far
func (i *CountingIterator) Count() int {
	return i.count
}

// IsDone returns true if iteration has completed
func (i *CountingIterator) IsDone() bool {
	return i.done
}

// CachingIterator wraps an iterator and caches tuples as a side effect
// It signals completion via a channel when iteration finishes
// This implements lazy-seq semantics for concurrent access to streaming relations
type CachingIterator struct {
	inner             Iterator
	cache             *[]Tuple      // Pointer to cache in StreamingRelation
	cacheComplete     chan struct{} // Closed when caching finishes
	cachingInProgress *bool         // Pointer to flag in StreamingRelation
	cacheReady        *bool         // Pointer to ready flag in StreamingRelation
	mu                *sync.Mutex   // Protects state transitions
	done              bool
	signaled          bool // Ensure we only signal once
}

// NewCachingIterator creates a caching iterator that builds a cache as it iterates
func NewCachingIterator(inner Iterator, cachePtr *[]Tuple, completeChan chan struct{},
	cachingInProgress *bool, cacheReady *bool, mu *sync.Mutex) *CachingIterator {
	return &CachingIterator{
		inner:             inner,
		cache:             cachePtr,
		cacheComplete:     completeChan,
		cachingInProgress: cachingInProgress,
		cacheReady:        cacheReady,
		mu:                mu,
		done:              false,
		signaled:          false,
	}
}

func (ci *CachingIterator) Next() bool {
	if ci.done {
		return false
	}

	if ci.inner.Next() {
		tuple := ci.inner.Tuple()

		// CRITICAL: Always copy tuples when caching
		// The inner iterator may reuse tuple buffers (EnableTrueStreaming=true)
		// We must copy to ensure cached tuples are independent
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)

		// Thread-safe append to cache
		ci.mu.Lock()
		*ci.cache = append(*ci.cache, tupleCopy)
		ci.mu.Unlock()

		return true
	}

	// Iteration complete - signal waiting goroutines
	ci.done = true
	ci.signalComplete()
	return false
}

func (ci *CachingIterator) Tuple() Tuple {
	// Return most recently cached tuple
	if len(*ci.cache) > 0 {
		return (*ci.cache)[len(*ci.cache)-1]
	}
	return Tuple{}
}

func (ci *CachingIterator) Close() error {
	// Ensure we signal completion even if Close() called early
	ci.signalComplete()
	return ci.inner.Close()
}

func (ci *CachingIterator) signalComplete() {
	ci.mu.Lock()
	// Check if already signaled (must be inside lock to avoid race)
	if ci.signaled {
		ci.mu.Unlock()
		return
	}

	// Check if we need to close the channel (first iterator to finish)
	shouldClose := *ci.cachingInProgress
	if shouldClose {
		*ci.cachingInProgress = false
		*ci.cacheReady = true // Mark cache as ready to prevent double-iterator creation
	}
	ci.signaled = true
	ci.mu.Unlock()

	// Close channel OUTSIDE lock to avoid holding lock while unblocking waiters
	if shouldClose {
		close(ci.cacheComplete) // Unblock all waiting Iterator() calls
	}
}

// MaterializedRelation holds all tuples in memory
type MaterializedRelation struct {
	symbols []query.Symbol
	tuples  []Tuple
	options ExecutorOptions
}

func NewMaterializedRelation(symbols []query.Symbol, tuples []Tuple) *MaterializedRelation {
	// Deduplicate tuples at creation
	dedupedTuples := deduplicateTuples(tuples)

	return &MaterializedRelation{
		symbols: symbols,
		tuples:  dedupedTuples,
		options: ExecutorOptions{}, // Default options
	}
}

// NewMaterializedRelationWithOptions creates a materialized relation with specific options
func NewMaterializedRelationWithOptions(symbols []query.Symbol, tuples []Tuple, opts ExecutorOptions) *MaterializedRelation {
	// Deduplicate tuples at creation
	dedupedTuples := deduplicateTuples(tuples)

	return &MaterializedRelation{
		symbols: symbols,
		tuples:  dedupedTuples,
		options: opts,
	}
}

// NewMaterializedRelationNoDedupe creates a materialized relation without deduplication
// Use this when you know the tuples are already unique (e.g., from storage scans)
func NewMaterializedRelationNoDedupe(symbols []query.Symbol, tuples []Tuple) *MaterializedRelation {
	return &MaterializedRelation{
		symbols: symbols,
		tuples:  tuples,
		options: ExecutorOptions{}, // Default options
	}
}

// NewMaterializedRelationNoDedupeWithOptions creates a new relation without deduplication, with options
func NewMaterializedRelationNoDedupeWithOptions(symbols []query.Symbol, tuples []Tuple, opts ExecutorOptions) *MaterializedRelation {
	return &MaterializedRelation{
		symbols: symbols,
		tuples:  tuples,
		options: opts,
	}
}

// NewUnitRelation returns a relation with one empty tuple (identity for joins).
// Used when OR fallback needs a base case with no outer context.
// The unit relation has no symbols and one tuple with no values.
func NewUnitRelation(opts ExecutorOptions) *MaterializedRelation {
	return &MaterializedRelation{
		symbols: []query.Symbol{},
		tuples:  []Tuple{{}}, // One empty tuple
		options: opts,
	}
}

// deduplicateTuples removes duplicate tuples
func deduplicateTuples(tuples []Tuple) []Tuple {
	if len(tuples) == 0 {
		return tuples
	}

	// Pre-size seen map based on input size
	seen := NewTupleKeyMapWithCapacity(len(tuples))
	result := make([]Tuple, 0, len(tuples))

	for _, tuple := range tuples {
		key := NewTupleKeyFull(tuple)
		if !seen.Exists(key) {
			seen.Put(key, true)
			result = append(result, tuple)
		}
	}

	return result
}

func (r *MaterializedRelation) Symbols() []query.Symbol {
	return r.symbols
}

func (r *MaterializedRelation) Iterator() Iterator {
	return &sliceIterator{
		tuples: r.tuples,
		pos:    -1,
	}
}

func (r *MaterializedRelation) Size() int {
	return len(r.tuples)
}

func (r *MaterializedRelation) IsEmpty() bool {
	return len(r.tuples) == 0
}

// Options returns the executor options for this materialized relation
func (r *MaterializedRelation) Options() ExecutorOptions {
	return r.options
}

// RequiresCopy returns false because MaterializedRelation stores tuples
// in a slice - each tuple is independent and not reused across iterations.
func (r *MaterializedRelation) RequiresCopy() bool {
	return false
}

// Get returns a specific tuple by index
func (r *MaterializedRelation) Get(i int) Tuple {
	if i < 0 || i >= len(r.tuples) {
		return nil
	}
	return r.tuples[i]
}

// SymbolIndex returns the index of a symbol in this relation
func (r *MaterializedRelation) SymbolIndex(sym query.Symbol) int {
	for i, s := range r.symbols {
		if s == sym {
			return i
		}
	}
	return -1
}

// GetValue returns a specific value by tuple index and symbol
func (r *MaterializedRelation) GetValue(tupleIdx int, sym query.Symbol) (interface{}, bool) {
	tuple := r.Get(tupleIdx)
	if tuple == nil {
		return nil, false
	}

	idx := r.SymbolIndex(sym)
	if idx < 0 {
		return nil, false
	}

	return tuple[idx], true
}

// Tuples returns all tuples (for backward compatibility)
func (r *MaterializedRelation) Tuples() []Tuple {
	return r.tuples
}

// String returns a compact string representation for annotations
func (r *MaterializedRelation) String() string {
	// Format as: Relation([?x ?y], N Tuples) with colors
	var symStrs []string
	for _, sym := range r.symbols {
		symStrs = append(symStrs, sym.String())
	}

	// Color the tuple count based on size
	count := r.Size()
	var countStr string
	switch {
	case count == 0:
		countStr = color.RedString("%d", count)
	case count < 100:
		countStr = color.GreenString("%d", count)
	case count < 10000:
		countStr = color.YellowString("%d", count)
	default:
		countStr = color.RedString("%d", count)
	}

	return fmt.Sprintf("%s%s%s%s%s %s%s",
		color.BlueString("Relation(["),
		color.CyanString(strings.Join(symStrs, " ")),
		color.BlueString("]"),
		color.BlueString(", "),
		countStr,
		"Tuples",
		color.BlueString(")"))
}

// Table returns a formatted markdown table representation
func (r *MaterializedRelation) Table() string {
	formatter := NewTableFormatter()
	return formatter.FormatRelation(r)
}

// ProjectFromPattern creates a new Relation with only the symbols from the pattern
// that exist in this Relation, in the order they appear in the pattern
func (r *MaterializedRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	// Find which symbols from this relation are used in the pattern
	neededSymbols := []query.Symbol{}
	symbolIndices := make(map[query.Symbol]int)

	// Build index of our symbols
	for i, sym := range r.symbols {
		symbolIndices[sym] = i
	}

	// Check each position in the pattern (E, A, V, T)
	if sym, ok := pattern.GetE().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if sym, ok := pattern.GetA().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists && !contains(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if sym, ok := pattern.GetV().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists && !contains(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if len(pattern.Elements) > 3 {
		if sym, ok := pattern.GetT().(query.Variable); ok {
			if _, exists := symbolIndices[sym.Name]; exists && !contains(neededSymbols, sym.Name) {
				neededSymbols = append(neededSymbols, sym.Name)
			}
		}
	}

	// If no symbols needed, return empty relation
	if len(neededSymbols) == 0 {
		return NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{}, r.options)
	}

	// Project to needed symbols using method (preserves materialized state)
	result, _ := r.Project(neededSymbols)
	// Ignore error as neededSymbols are derived from the pattern elements
	// which must exist if we got this far
	return result
}

// Sorted returns tuples sorted by the relation's symbols
// First symbol is primary sort key, second is secondary, etc.
func (r *MaterializedRelation) Sorted() []Tuple {
	// Create a copy of tuples to sort (preserving immutability)
	sorted := make([]Tuple, len(r.tuples))
	copy(sorted, r.tuples)

	// Sort tuples lexicographically by symbols
	sort.Slice(sorted, func(i, j int) bool {
		for k := 0; k < len(r.symbols) && k < len(sorted[i]) && k < len(sorted[j]); k++ {
			cmp := datalog.CompareValues(sorted[i][k], sorted[j][k])
			if cmp < 0 {
				return true
			} else if cmp > 0 {
				return false
			}
		}
		return len(sorted[i]) < len(sorted[j])
	})

	return sorted
}

// Project returns a new relation with only the specified symbols
func (r *MaterializedRelation) Project(symbols []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(symbols) == 0 {
		return nil, fmt.Errorf("cannot project empty symbol list - invalid query")
	}

	// Find symbol indices
	indices := make([]int, len(symbols))
	for i, sym := range symbols {
		idx := -1
		for j, existing := range r.symbols {
			if existing == sym {
				idx = j
				break
			}
		}
		if idx < 0 {
			// Symbol not found - this is a query error in Datalog
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation (has symbols: %v)", sym, r.symbols)
		}
		indices[i] = idx
	}

	// Project tuples - directly access our tuples field
	projected := make([]Tuple, len(r.tuples))
	for i, tuple := range r.tuples {
		projTuple := make(Tuple, len(indices))
		for j, idx := range indices {
			projTuple[j] = tuple[idx]
		}
		projected[i] = projTuple
	}

	return NewMaterializedRelationWithOptions(symbols, projected, r.options), nil
}

// Materialize returns self since MaterializedRelation is already materialized
func (r *MaterializedRelation) Materialize() Relation {
	return r
}

// Sort returns a new relation sorted by the specified order-by clauses
func (r *MaterializedRelation) Sort(orderBy []query.OrderByClause) Relation {
	// Use the SortRelation function we created
	return SortRelation(r, orderBy)
}

// Filter returns a new relation with only tuples that satisfy the filter
func (r *MaterializedRelation) Filter(filter Filter) Relation {
	// Check if all required symbols are present
	for _, sym := range filter.RequiredSymbols() {
		found := false
		for _, s := range r.symbols {
			if s == sym {
				found = true
				break
			}
		}
		if !found {
			// Missing required symbol - return empty relation
			return NewMaterializedRelationWithOptions(r.symbols, nil, r.options)
		}
	}

	// Apply filter directly to our tuples
	var filtered []Tuple
	for _, tuple := range r.tuples {
		if filter.Evaluate(tuple, r.symbols) {
			filtered = append(filtered, tuple)
		}
	}

	return NewMaterializedRelationWithOptions(r.symbols, filtered, r.options)
}

// FilterWithPredicate filters the relation using a query.Predicate
func (r *MaterializedRelation) FilterWithPredicate(pred query.Predicate) Relation {
	// Build bindings map for each tuple
	var filtered []Tuple
	for _, tuple := range r.tuples {
		bindings := make(map[query.Symbol]interface{})
		for i, sym := range r.symbols {
			bindings[sym] = tuple[i]
		}

		// Apply the predicate
		if passes, err := pred.Eval(bindings); err == nil && passes {
			filtered = append(filtered, tuple)
		}
	}

	return NewMaterializedRelationWithOptions(r.symbols, filtered, r.options)
}

// Select returns a new relation with only tuples that satisfy the predicate
func (r *MaterializedRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

// Join performs a natural join with another relation
func (r *MaterializedRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		// No common symbols - cross product (expensive!)
		return crossProduct(r, other)
	}
	// Use hash join for efficiency
	return r.HashJoin(other, common)
}

// HashJoin performs an equi-join on specified symbols
func (r *MaterializedRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoin(r, other, joinSyms)
}

// SemiJoin returns tuples from this relation that have matches in the other
func (r *MaterializedRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return SemiJoin(r, other, joinSyms)
}

// AntiJoin returns tuples from this relation that have no matches in the other
func (r *MaterializedRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return AntiJoin(r, other, joinSyms)
}

// Aggregate performs aggregation operations
func (r *MaterializedRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

// EvaluateFunction evaluates a function and adds its result as a new symbol
func (r *MaterializedRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	// Add the output symbol
	newSymbols := append(r.symbols, outputSymbol)

	// Process each tuple
	var newTuples []Tuple
	for _, tuple := range r.tuples {
		// Create bindings from tuple
		bindings := make(map[query.Symbol]interface{})
		for i, sym := range r.symbols {
			bindings[sym] = tuple[i]
		}

		// Evaluate the function
		result, err := fn.Eval(bindings)
		if err != nil {
			// Skip tuples where function evaluation fails
			continue
		}

		// Create new tuple with function result
		newTuple := append(tuple, result)
		newTuples = append(newTuples, newTuple)
	}

	return NewMaterializedRelation(newSymbols, newTuples)
}

// contains checks if a symbol is in a slice
func contains(symbols []query.Symbol, sym query.Symbol) bool {
	for _, s := range symbols {
		if s == sym {
			return true
		}
	}
	return false
}

// sliceIterator iterates over a slice of tuples
type sliceIterator struct {
	tuples []Tuple
	pos    int
}

func (it *sliceIterator) Next() bool {
	it.pos++
	return it.pos < len(it.tuples)
}

func (it *sliceIterator) Tuple() Tuple {
	if it.pos >= 0 && it.pos < len(it.tuples) {
		return it.tuples[it.pos]
	}
	return nil
}

func (it *sliceIterator) Close() error {
	return nil
}

// StreamingRelation wraps an iterator as a relation
type StreamingRelation struct {
	symbols  []query.Symbol
	iterator Iterator
	size     int             // -1 if unknown
	options  ExecutorOptions // Options from the factory that created this relation

	// Lazy materialization: consume iterator once and cache result
	// sync.Once provides all necessary concurrency safety - ensures materialization
	// happens exactly once and all concurrent callers wait for completion
	materializeOnce sync.Once
	materialized    *MaterializedRelation

	// Lazy caching with concurrent access support (implements lazy-seq semantics)
	// Materialize() sets shouldCache=true, first Iterator() builds cache,
	// subsequent Iterator() calls block until cache is complete, then reuse cached data
	shouldCache       bool          // Flag set by Materialize() - indicates caching should happen
	cache             []Tuple       // Built on first iteration if shouldCache=true
	cachingInProgress bool          // True while first iterator is building cache
	cacheReady        bool          // True when caching has completed (prevents double-iterator creation)
	cacheComplete     chan struct{} // Closed when cache is ready (signals waiting goroutines)
	mu                sync.Mutex    // Protects cache state transitions

	// Lightweight size tracking: count tuples without buffering data
	counter        *CountingIterator // For tracking tuple count during iteration
	iteratorCalled bool              // Track if Iterator() was already called (for single-use enforcement)
}

func NewStreamingRelation(symbols []query.Symbol, iterator Iterator) *StreamingRelation {
	return &StreamingRelation{
		symbols:  symbols,
		iterator: iterator,
		size:     -1,
		options:  ExecutorOptions{}, // Default options - for backward compatibility
	}
}

// NewStreamingRelationWithOptions creates a streaming relation with specific options
func NewStreamingRelationWithOptions(symbols []query.Symbol, iterator Iterator, opts ExecutorOptions) *StreamingRelation {
	return &StreamingRelation{
		symbols:  symbols,
		iterator: iterator,
		size:     -1,
		options:  opts,
	}
}

func (r *StreamingRelation) Symbols() []query.Symbol {
	return r.symbols
}

func (r *StreamingRelation) Iterator() Iterator {
	r.mu.Lock()

	// Fast path: If we have a complete cache, return reusable iterator
	if r.cacheReady {
		r.mu.Unlock()
		return &sliceIterator{
			tuples: r.cache,
			pos:    -1,
		}
	}

	// If caching is in progress, BLOCK until it completes
	if r.cachingInProgress {
		completeChan := r.cacheComplete // Capture channel before unlocking
		r.mu.Unlock()

		// BLOCK: Wait for cache to complete
		<-completeChan

		// Cache is now ready, return iterator over cached data
		return &sliceIterator{
			tuples: r.cache,
			pos:    -1,
		}
	}

	// Check for illegal double-iteration without materialization
	// StreamingRelation is single-use unless Materialize() was called first
	if r.iteratorCalled && !r.shouldCache {
		r.mu.Unlock()
		panic("StreamingRelation.Iterator() called multiple times without Materialize(). " +
			"Streaming iterators are single-use only. " +
			"Call Materialize() first if you need multiple iterations.")
	}

	// First iterator call - mark as called
	r.iteratorCalled = true

	// If Materialize() was called, enable caching
	if r.shouldCache {
		r.cachingInProgress = true
		if r.options.EnableDebugLogging {
			fmt.Printf("[StreamingRelation.Iterator] First call with caching enabled\n")
		}
	}

	r.mu.Unlock()

	// Create base iterator
	baseIter := r.iterator

	// Wrap with counting iterator for lightweight size tracking
	if r.counter == nil {
		r.counter = NewCountingIterator(baseIter)
		baseIter = r.counter
	}

	// If caching enabled, wrap with CachingIterator
	if r.shouldCache {
		return NewCachingIterator(baseIter, &r.cache, r.cacheComplete, &r.cachingInProgress, &r.cacheReady, &r.mu)
	}

	// Pure streaming - single use
	return baseIter
}

func (r *StreamingRelation) Size() int {
	r.mu.Lock()

	// Fast path: If cache is complete, return its size
	if r.cacheReady {
		size := len(r.cache)
		r.mu.Unlock()
		return size
	}

	// BLOCK if caching is in progress - wait for completion
	// This ensures Size() and Iterator() have consistent semantics
	if r.cachingInProgress {
		completeChan := r.cacheComplete
		r.mu.Unlock()

		// BLOCK: Wait for cache to complete
		<-completeChan

		// Cache is now ready, return its size
		return len(r.cache)
	}

	r.mu.Unlock()

	if r.size >= 0 {
		return r.size
	}

	// If materialized (old path), return materialized size
	if r.materialized != nil {
		return r.materialized.Size()
	}

	// If iterator has been consumed, we can report the count
	if r.counter != nil && r.counter.IsDone() {
		r.size = r.counter.Count()
		return r.size
	}

	// Streaming behavior: return -1 to indicate unknown size
	// Callers should handle unknown sizes gracefully (e.g., use default capacity)
	// DO NOT call Iterator() here - that would break single-use semantics
	return -1
}

// Options returns the executor options for this streaming relation
func (r *StreamingRelation) Options() ExecutorOptions {
	return r.options
}

// RequiresCopy returns true because StreamingRelation wraps iterators
// that may reuse workspace memory for tuples across Next() calls.
func (r *StreamingRelation) RequiresCopy() bool {
	return true
}

func (r *StreamingRelation) IsEmpty() bool {
	// If materialized, check materialized relation
	if r.materialized != nil {
		return r.materialized.IsEmpty()
	}

	// If iterator has been consumed, check count
	if r.counter != nil && r.counter.IsDone() {
		return r.counter.Count() == 0
	}

	// With EnableTrueStreaming, we can't peek without consuming
	// Return false (assume not empty) to avoid consuming the iterator
	// Callers should handle empty results gracefully
	if r.options.EnableTrueStreaming {
		// Don't consume the iterator - assume not empty
		// If it IS empty, subsequent operations will discover that naturally
		return false
	}

	// Non-streaming mode: safe to peek
	if r.counter == nil {
		r.counter = NewCountingIterator(r.iterator)
	}

	// Check if there's at least one tuple
	hasOne := r.counter.Next()
	if !hasOne {
		return true // Empty
	}

	// Not empty - but we've consumed the first tuple
	return false
}

// Get returns a specific tuple by index
func (r *StreamingRelation) Get(i int) Tuple {
	// Trigger materialization, then delegate to materialized version
	_ = r.Iterator() // Triggers materializeOnce
	if r.materialized != nil {
		return r.materialized.Get(i)
	}
	return nil
}

// String returns a compact string representation for annotations
func (r *StreamingRelation) String() string {
	// Format as: Relation([?x ?y], N Tuples) with colors
	var symStrs []string
	for _, sym := range r.symbols {
		symStrs = append(symStrs, sym.String())
	}

	// For streaming relations, we might not know the size
	if r.size >= 0 {
		// Color the tuple count based on size
		var countStr string
		switch {
		case r.size == 0:
			countStr = color.RedString("%d", r.size)
		case r.size < 100:
			countStr = color.GreenString("%d", r.size)
		case r.size < 10000:
			countStr = color.YellowString("%d", r.size)
		default:
			countStr = color.RedString("%d", r.size)
		}

		return fmt.Sprintf("%s%s%s%s%s %s%s",
			color.BlueString("Relation(["),
			color.CyanString(strings.Join(symStrs, " ")),
			color.BlueString("]"),
			color.BlueString(", "),
			countStr,
			"Tuples",
			color.BlueString(")"))
	}

	// Size unknown
	return fmt.Sprintf("%s%s%s%s",
		color.BlueString("Relation(["),
		color.CyanString(strings.Join(symStrs, " ")),
		color.BlueString("]"),
		color.BlueString(", streaming)"))
}

// Table returns a formatted markdown table representation
func (r *StreamingRelation) Table() string {
	formatter := NewTableFormatter()
	return formatter.FormatRelation(r)
}

// ProjectFromPattern creates a new Relation with only the symbols from the pattern
// that exist in this Relation, in the order they appear in the pattern
func (r *StreamingRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	// Find which symbols from this relation are used in the pattern
	neededSymbols := []query.Symbol{}
	symbolIndices := make(map[query.Symbol]int)

	// Build index of our symbols
	for i, sym := range r.symbols {
		symbolIndices[sym] = i
	}

	// Check each position in the pattern (E, A, V, T)
	if sym, ok := pattern.GetE().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if sym, ok := pattern.GetA().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists && !contains(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if sym, ok := pattern.GetV().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists && !contains(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if len(pattern.Elements) > 3 {
		if sym, ok := pattern.GetT().(query.Variable); ok {
			if _, exists := symbolIndices[sym.Name]; exists && !contains(neededSymbols, sym.Name) {
				neededSymbols = append(neededSymbols, sym.Name)
			}
		}
	}

	// If no symbols needed, return empty relation
	if len(neededSymbols) == 0 {
		return NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{}, r.options)
	}

	// Use the StreamingRelation's Project method which creates a streaming projection iterator
	// instead of the global Project() function which materializes
	result, _ := r.Project(neededSymbols)
	// Ignore error as neededSymbols are derived from the pattern elements
	// which must exist if we got this far
	return result
}

// Sorted returns tuples sorted by the relation's symbols
func (r *StreamingRelation) Sorted() []Tuple {
	// Sorted() requires all data in memory
	// Set shouldCache flag so iteration builds the cache
	r.Materialize()

	// Now consume iterator to build cache
	var tuples []Tuple
	collectTuplesInto(&tuples, r)

	// Sort tuples lexicographically by symbols
	sort.Slice(tuples, func(i, j int) bool {
		for k := 0; k < len(r.symbols) && k < len(tuples[i]) && k < len(tuples[j]); k++ {
			cmp := datalog.CompareValues(tuples[i][k], tuples[j][k])
			if cmp < 0 {
				return true
			} else if cmp > 0 {
				return false
			}
		}
		return len(tuples[i]) < len(tuples[j])
	})

	return tuples
}

// Project returns a new relation with only the specified symbols
func (r *StreamingRelation) Project(symbols []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(symbols) == 0 {
		return nil, fmt.Errorf("cannot project empty symbol list - invalid query")
	}

	// Streaming is now the default behavior
	// Validate symbols exist
	for _, sym := range symbols {
		found := false
		for _, existing := range r.symbols {
			if existing == sym {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation", sym)
		}
	}
	// CRITICAL FIX: Pass the relation itself to ProjectIterator, not the raw iterator
	// This allows ProjectIterator to call r.Iterator(), which respects caching/materialization
	// When r.shouldCache=true, the first Iterator() call builds the cache, and both the
	// original relation and the projection can iterate from cached data
	projIter := NewProjectIterator(r, r.symbols, symbols)
	// SET SEMANTICS: Wrap with DedupIterator to ensure projection maintains set semantics.
	// Projection can produce duplicates when multiple distinct input tuples map to the same
	// projected tuple (e.g., [(1,a), (2,a)] projected to symbol 2 yields [a, a]).
	dedupIter := NewDedupIterator(projIter, 0)
	// BUGFIX: Preserve options (especially EnableTrueStreaming) to prevent re-scanning
	return NewStreamingRelationWithOptions(symbols, dedupIter, r.options), nil
}

// Materialize converts this streaming relation to a materialized one
func (r *StreamingRelation) Materialize() Relation {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If already cached, return self (idempotent)
	if r.cache != nil {
		return r
	}

	// If already materialized (old path), return that
	if r.materialized != nil {
		return r.materialized
	}

	// CRITICAL: Must be called BEFORE iteration starts
	if r.iteratorCalled {
		panic("StreamingRelation.Materialize() called after iteration began. " +
			"Materialize() must be called before first Iterator() call.")
	}

	// Set flag - actual caching happens on first Iterator() call
	r.shouldCache = true
	r.cacheComplete = make(chan struct{}) // Create completion signal
	return r                              // Return self, NOT a new MaterializedRelation
}

// Sort returns a new relation sorted by the specified order-by clauses
// Warning: This materializes the streaming relation
func (r *StreamingRelation) Sort(orderBy []query.OrderByClause) Relation {
	// Collect all tuples (can't sort without materializing)
	var tuples []Tuple
	collectTuplesInto(&tuples, r)

	// Create MaterializedRelation and delegate to its Sort
	mat := NewMaterializedRelationWithOptions(r.symbols, tuples, r.options)
	return mat.Sort(orderBy)
}

// Filter returns a new relation with only tuples that satisfy the filter
func (r *StreamingRelation) Filter(filter Filter) Relation {
	if r.options.EnableIteratorComposition {
		// Use iterator composition for true streaming
		filterIter := NewFilterIterator(r.iterator, r.symbols, filter)
		return NewStreamingRelationWithOptions(r.symbols, filterIter, r.options)
	}
	// Fall back to current behavior
	return FilterRelation(r, filter)
}

// FilterWithPredicate filters the relation using a query.Predicate
func (r *StreamingRelation) FilterWithPredicate(pred query.Predicate) Relation {
	if r.options.EnableIteratorComposition {
		// Use iterator composition for true streaming
		predIter := NewPredicateFilterIterator(r.iterator, r.symbols, pred)
		return NewStreamingRelationWithOptions(r.symbols, predIter, r.options)
	}
	// Fall back to current behavior - materialize then filter
	materialized := r.Materialize()
	return materialized.FilterWithPredicate(pred)
}

// EvaluateFunction evaluates a function and adds its result as a new symbol
func (r *StreamingRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	if r.options.EnableIteratorComposition {
		// Use iterator composition for true streaming
		evalIter := NewFunctionEvaluatorIterator(r.iterator, r.symbols, fn, outputSymbol)
		newSymbols := append(r.symbols, outputSymbol)
		return NewStreamingRelationWithOptions(newSymbols, evalIter, r.options)
	}
	// Fall back to current behavior - materialize then evaluate
	materialized := r.Materialize()
	return materialized.EvaluateFunction(fn, outputSymbol)
}

// Select returns a new relation with only tuples that satisfy the predicate
func (r *StreamingRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

// Join performs a natural join with another relation
func (r *StreamingRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		// No common symbols - cross product (expensive!)
		return crossProduct(r, other)
	}
	// Use hash join for efficiency
	return r.HashJoin(other, common)
}

// HashJoin performs an equi-join on specified symbols
func (r *StreamingRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoin(r, other, joinSyms)
}

// SemiJoin returns tuples from this relation that have matches in the other
func (r *StreamingRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return SemiJoin(r, other, joinSyms)
}

// AntiJoin returns tuples from this relation that have no matches in the other
func (r *StreamingRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return AntiJoin(r, other, joinSyms)
}

// Aggregate performs aggregation operations
// Warning: This materializes the streaming relation
func (r *StreamingRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

// PatternBinding describes how to extract values from a datom
type PatternBinding struct {
	EntitySym    *query.Symbol // Variable in E position
	AttributeSym *query.Symbol // Variable in A position
	ValueSym     *query.Symbol // Variable in V position
	TxSym        *query.Symbol // Variable in T position
}

// Utility functions for working with relations

// SymbolIndex returns the index of a symbol in a relation, or -1 if not found
func SymbolIndex(rel Relation, sym query.Symbol) int {
	syms := rel.Symbols()
	for i, s := range syms {
		if s == sym {
			return i
		}
	}
	return -1
}

// CommonSymbols returns symbols that appear in both relations
func CommonSymbols(r1, r2 Relation) []query.Symbol {
	syms1 := r1.Symbols()
	syms2Set := make(map[query.Symbol]bool)
	for _, sym := range r2.Symbols() {
		syms2Set[sym] = true
	}

	var common []query.Symbol
	for _, sym := range syms1 {
		if syms2Set[sym] {
			common = append(common, sym)
		}
	}
	return common
}

// Select filters a relation based on a predicate
func Select(rel Relation, pred func(Tuple) bool) Relation {
	var selected []Tuple
	needsCopy := rel.RequiresCopy()
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		if pred(tuple) {
			if needsCopy {
				tuple = copyTuple(tuple)
			}
			selected = append(selected, tuple)
		}
	}

	return NewMaterializedRelation(rel.Symbols(), selected)
}

// ProductRelation represents a streaming Cartesian product of multiple relations
// Used for expressions/predicates that reference symbols from disjoint relations
type ProductRelation struct {
	relations []Relation
	symbols   []query.Symbol
	options   ExecutorOptions
}

// NewProductRelation creates a new ProductRelation
func NewProductRelation(relations []Relation) *ProductRelation {
	if len(relations) == 0 {
		return &ProductRelation{
			relations: relations,
			symbols:   nil,
			options:   ExecutorOptions{},
		}
	}

	// Combine symbols from all relations
	var allSymbols []query.Symbol
	for _, rel := range relations {
		allSymbols = append(allSymbols, rel.Symbols()...)
	}

	// Extract options from first relation
	opts := relations[0].Options()

	return &ProductRelation{
		relations: relations,
		symbols:   allSymbols,
		options:   opts,
	}
}

func (p *ProductRelation) Symbols() []query.Symbol {
	return p.symbols
}

func (p *ProductRelation) Iterator() Iterator {
	return &ProductIterator{
		relations: p.relations,
		iterators: make([]Iterator, len(p.relations)),
		current:   make([]Tuple, len(p.relations)),
		first:     true,
	}
}

func (p *ProductRelation) Size() int {
	// Product size is product of all relation sizes
	if len(p.relations) == 0 {
		return 0
	}
	size := 1
	for _, rel := range p.relations {
		relSize := rel.Size()
		if relSize == 0 {
			return 0
		}
		if relSize < 0 {
			return -1 // Unknown size
		}
		size *= relSize
	}
	return size
}

func (p *ProductRelation) IsEmpty() bool {
	for _, rel := range p.relations {
		if rel.IsEmpty() {
			return true
		}
	}
	return len(p.relations) == 0
}

func (p *ProductRelation) Get(i int) Tuple {
	// Materialize for random access
	return p.Materialize().Get(i)
}

func (p *ProductRelation) String() string {
	return fmt.Sprintf("Product(%d relations, %d symbols)", len(p.relations), len(p.symbols))
}

func (p *ProductRelation) Table() string {
	return p.Materialize().Table()
}

func (p *ProductRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	// Materialize then project
	return p.Materialize().ProjectFromPattern(pattern)
}

func (p *ProductRelation) Sorted() []Tuple {
	return p.Materialize().Sorted()
}

func (p *ProductRelation) Project(symbols []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(symbols) == 0 {
		return nil, fmt.Errorf("cannot project empty symbol list - invalid query")
	}

	// Validate symbols exist
	for _, sym := range symbols {
		found := false
		for _, existing := range p.Symbols() {
			if existing == sym {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation", sym)
		}
	}
	// Product relations are streaming - use iterator composition
	// Pass the relation itself so ProjectIterator can call Iterator() when needed
	projIter := NewProjectIterator(p, p.Symbols(), symbols)
	// Use default options since ProductRelation is a wrapper
	return NewStreamingRelation(symbols, projIter), nil
}

func (p *ProductRelation) Materialize() Relation {
	var tuples []Tuple
	collectTuplesInto(&tuples, p)
	return NewMaterializedRelationWithOptions(p.symbols, tuples, p.options)
}

func (p *ProductRelation) Sort(orderBy []query.OrderByClause) Relation {
	return p.Materialize().Sort(orderBy)
}

func (p *ProductRelation) Filter(filter Filter) Relation {
	// Materialize then filter
	return p.Materialize().Filter(filter)
}

func (p *ProductRelation) FilterWithPredicate(pred query.Predicate) Relation {
	// Materialize then filter
	return p.Materialize().FilterWithPredicate(pred)
}

func (p *ProductRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	// Materialize then evaluate
	return p.Materialize().EvaluateFunction(fn, outputSymbol)
}

func (p *ProductRelation) Select(pred func(Tuple) bool) Relation {
	return Select(p, pred)
}

func (p *ProductRelation) Join(other Relation) Relation {
	// Materialize then join
	return p.Materialize().Join(other)
}

func (p *ProductRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoinWithOptions(p, other, joinSyms, p.options)
}

func (p *ProductRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	// Materialize then semi-join
	return p.Materialize().SemiJoin(other, joinSyms)
}

func (p *ProductRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	// Materialize then anti-join
	return p.Materialize().AntiJoin(other, joinSyms)
}

func (p *ProductRelation) Aggregate(findElements []query.FindElement) Relation {
	// Materialize then aggregate
	return p.Materialize().Aggregate(findElements)
}

func (p *ProductRelation) Options() ExecutorOptions {
	return p.options
}

// RequiresCopy returns false because ProductIterator.Tuple() creates a fresh
// tuple on each call using append (var result Tuple; result = append(result, ...)).
// The tuple is not reused across Next() calls.
func (p *ProductRelation) RequiresCopy() bool {
	return false
}

// ProductIterator implements streaming nested-loop iteration over multiple relations
type ProductIterator struct {
	relations []Relation
	iterators []Iterator
	current   []Tuple
	first     bool
	done      bool
}

func (pi *ProductIterator) Next() bool {
	if pi.done {
		return false
	}

	// Initialize all iterators on first call
	if pi.first {
		pi.first = false

		// Create iterators for all relations
		for i, rel := range pi.relations {
			pi.iterators[i] = rel.Iterator()
		}

		// Get first tuple from each relation
		for i := range pi.iterators {
			if !pi.iterators[i].Next() {
				// Empty relation - product is empty
				pi.done = true
				return false
			}
			pi.current[i] = pi.iterators[i].Tuple()
		}
		return true
	}

	// Advance rightmost iterator (nested loop)
	for i := len(pi.iterators) - 1; i >= 0; i-- {
		if pi.iterators[i].Next() {
			pi.current[i] = pi.iterators[i].Tuple()
			return true
		}

		// This iterator exhausted - reset it and advance previous
		if i == 0 {
			// Leftmost iterator exhausted - we're done
			pi.done = true
			return false
		}

		// Reset this iterator
		pi.iterators[i].Close()
		pi.iterators[i] = pi.relations[i].Iterator()
		if !pi.iterators[i].Next() {
			// Should not happen - relation became empty
			pi.done = true
			return false
		}
		pi.current[i] = pi.iterators[i].Tuple()
	}

	return false
}

func (pi *ProductIterator) Tuple() Tuple {
	// Concatenate all current tuples
	var result Tuple
	for _, tuple := range pi.current {
		result = append(result, tuple...)
	}
	return result
}

func (pi *ProductIterator) Close() error {
	for _, it := range pi.iterators {
		if it != nil {
			it.Close()
		}
	}
	return nil
}
