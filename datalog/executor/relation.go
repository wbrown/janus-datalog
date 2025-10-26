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

// Relation represents a set of tuples with named columns
type Relation interface {
	// Columns returns the column names (symbols) in order
	// Deprecated: Use Symbols() instead for consistency with relational theory
	Columns() []query.Symbol

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

	// Project returns a new relation with only the specified columns
	// Returns an error if any requested column doesn't exist
	Project(columns []query.Symbol) (Relation, error)

	// Materialize converts a streaming relation to a materialized one
	// For already-materialized relations, returns self
	Materialize() Relation

	// Sort returns a new relation sorted by the specified order-by clauses
	Sort(orderBy []query.OrderByClause) Relation

	// Filter returns a new relation with only tuples that satisfy the filter
	Filter(filter Filter) Relation

	// FilterWithPredicate returns a new relation filtered by a query.Predicate
	FilterWithPredicate(pred query.Predicate) Relation

	// EvaluateFunction evaluates a function and adds its result as a new column
	EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation

	// Select returns a new relation with only tuples that satisfy the predicate
	Select(pred func(Tuple) bool) Relation

	// Join performs a natural join with another relation
	Join(other Relation) Relation

	// HashJoin performs an equi-join on specified columns
	HashJoin(other Relation, joinCols []query.Symbol) Relation

	// SemiJoin returns tuples from this relation that have matches in the other
	SemiJoin(other Relation, joinCols []query.Symbol) Relation

	// AntiJoin returns tuples from this relation that have no matches in the other
	AntiJoin(other Relation, joinCols []query.Symbol) Relation

	// Aggregate performs aggregation operations
	Aggregate(findElements []query.FindElement) Relation

	// Options returns the executor options for this relation
	// Used by join operations to extract configuration
	Options() ExecutorOptions

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
	cache             *[]Tuple       // Pointer to cache in StreamingRelation
	cacheComplete     chan struct{}  // Closed when caching finishes
	cachingInProgress *bool          // Pointer to flag in StreamingRelation
	cacheReady        *bool          // Pointer to ready flag in StreamingRelation
	mu                *sync.Mutex    // Protects state transitions
	done              bool
	signaled          bool           // Ensure we only signal once
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
		*ci.cacheReady = true  // Mark cache as ready to prevent double-iterator creation
	}
	ci.signaled = true
	ci.mu.Unlock()

	// Close channel OUTSIDE lock to avoid holding lock while unblocking waiters
	if shouldClose {
		close(ci.cacheComplete)  // Unblock all waiting Iterator() calls
	}
}

// MaterializedRelation holds all tuples in memory
type MaterializedRelation struct {
	columns []query.Symbol
	tuples  []Tuple
	options ExecutorOptions
}

func NewMaterializedRelation(columns []query.Symbol, tuples []Tuple) *MaterializedRelation {
	// Deduplicate tuples at creation
	dedupedTuples := deduplicateTuples(tuples)

	return &MaterializedRelation{
		columns: columns,
		tuples:  dedupedTuples,
		options: ExecutorOptions{}, // Default options
	}
}

// NewMaterializedRelationWithOptions creates a materialized relation with specific options
func NewMaterializedRelationWithOptions(columns []query.Symbol, tuples []Tuple, opts ExecutorOptions) *MaterializedRelation {
	// Deduplicate tuples at creation
	dedupedTuples := deduplicateTuples(tuples)

	return &MaterializedRelation{
		columns: columns,
		tuples:  dedupedTuples,
		options: opts,
	}
}

// NewMaterializedRelationNoDedupe creates a materialized relation without deduplication
// Use this when you know the tuples are already unique (e.g., from storage scans)
func NewMaterializedRelationNoDedupe(columns []query.Symbol, tuples []Tuple) *MaterializedRelation {
	return &MaterializedRelation{
		columns: columns,
		tuples:  tuples,
		options: ExecutorOptions{}, // Default options
	}
}

// NewMaterializedRelationNoDedupeWithOptions creates a new relation without deduplication, with options
func NewMaterializedRelationNoDedupeWithOptions(columns []query.Symbol, tuples []Tuple, opts ExecutorOptions) *MaterializedRelation {
	return &MaterializedRelation{
		columns: columns,
		tuples:  tuples,
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

func (r *MaterializedRelation) Columns() []query.Symbol {
	return r.columns
}

func (r *MaterializedRelation) Symbols() []query.Symbol {
	return r.columns
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

// Get returns a specific tuple by index
func (r *MaterializedRelation) Get(i int) Tuple {
	if i < 0 || i >= len(r.tuples) {
		return nil
	}
	return r.tuples[i]
}

// ColumnIndex returns the index of a column by symbol
func (r *MaterializedRelation) ColumnIndex(sym query.Symbol) int {
	for i, col := range r.columns {
		if col == sym {
			return i
		}
	}
	return -1
}

// GetValue returns a specific value by row and column symbol
func (r *MaterializedRelation) GetValue(row int, sym query.Symbol) (interface{}, bool) {
	tuple := r.Get(row)
	if tuple == nil {
		return nil, false
	}

	col := r.ColumnIndex(sym)
	if col < 0 {
		return nil, false
	}

	return tuple[col], true
}

// Tuples returns all tuples (for backward compatibility)
func (r *MaterializedRelation) Tuples() []Tuple {
	return r.tuples
}

// String returns a compact string representation for annotations
func (r *MaterializedRelation) String() string {
	// Format as: Relation([?x ?y], N Tuples) with colors
	var symbols []string
	for _, col := range r.columns {
		symbols = append(symbols, string(col))
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
		color.CyanString(strings.Join(symbols, " ")),
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

	// Build index of our columns
	for i, col := range r.columns {
		symbolIndices[col] = i
	}

	// Check each position in the pattern
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

	// Sort tuples lexicographically by columns
	sort.Slice(sorted, func(i, j int) bool {
		for k := 0; k < len(r.columns) && k < len(sorted[i]) && k < len(sorted[j]); k++ {
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

// Project returns a new relation with only the specified columns
func (r *MaterializedRelation) Project(columns []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(columns) == 0 {
		return nil, fmt.Errorf("cannot project empty column list - invalid query")
	}

	// Find column indices
	indices := make([]int, len(columns))
	for i, col := range columns {
		idx := -1
		for j, existing := range r.columns {
			if existing == col {
				idx = j
				break
			}
		}
		if idx < 0 {
			// Column not found - this is a query error in Datalog
			return nil, fmt.Errorf("cannot project: column %s not found in relation (has columns: %v)", col, r.columns)
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

	return NewMaterializedRelationWithOptions(columns, projected, r.options), nil
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
		for _, col := range r.columns {
			if col == sym {
				found = true
				break
			}
		}
		if !found {
			// Missing required symbol - return empty relation
			return NewMaterializedRelationWithOptions(r.columns, nil, r.options)
		}
	}

	// Apply filter directly to our tuples
	var filtered []Tuple
	for _, tuple := range r.tuples {
		if filter.Evaluate(tuple, r.columns) {
			filtered = append(filtered, tuple)
		}
	}

	return NewMaterializedRelationWithOptions(r.columns, filtered, r.options)
}

// FilterWithPredicate filters the relation using a query.Predicate
func (r *MaterializedRelation) FilterWithPredicate(pred query.Predicate) Relation {
	// Build bindings map for each tuple
	var filtered []Tuple
	for _, tuple := range r.tuples {
		bindings := make(map[query.Symbol]interface{})
		for i, col := range r.columns {
			bindings[col] = tuple[i]
		}

		// Apply the predicate
		if passes, err := pred.Eval(bindings); err == nil && passes {
			filtered = append(filtered, tuple)
		}
	}

	return NewMaterializedRelationWithOptions(r.columns, filtered, r.options)
}

// Select returns a new relation with only tuples that satisfy the predicate
func (r *MaterializedRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

// Join performs a natural join with another relation
func (r *MaterializedRelation) Join(other Relation) Relation {
	common := CommonColumns(r, other)
	if len(common) == 0 {
		// No common columns - cross product (expensive!)
		return crossProduct(r, other)
	}
	// Use hash join for efficiency
	return r.HashJoin(other, common)
}

// HashJoin performs an equi-join on specified columns
func (r *MaterializedRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return HashJoin(r, other, joinCols)
}

// SemiJoin returns tuples from this relation that have matches in the other
func (r *MaterializedRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	return SemiJoin(r, other, joinCols)
}

// AntiJoin returns tuples from this relation that have no matches in the other
func (r *MaterializedRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	return AntiJoin(r, other, joinCols)
}

// Aggregate performs aggregation operations
func (r *MaterializedRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

// EvaluateFunction evaluates a function and adds its result as a new column
func (r *MaterializedRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	// Add the output column
	newColumns := append(r.columns, outputColumn)

	// Process each tuple
	var newTuples []Tuple
	for _, tuple := range r.tuples {
		// Create bindings from tuple
		bindings := make(map[query.Symbol]interface{})
		for i, col := range r.columns {
			bindings[col] = tuple[i]
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

	return NewMaterializedRelation(newColumns, newTuples)
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
	columns  []query.Symbol
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
	counter         *CountingIterator // For tracking tuple count during iteration
	iteratorCalled  bool              // Track if Iterator() was already called (for single-use enforcement)
}

func NewStreamingRelation(columns []query.Symbol, iterator Iterator) *StreamingRelation {
	return &StreamingRelation{
		columns:  columns,
		iterator: iterator,
		size:     -1,
		options:  ExecutorOptions{}, // Default options - for backward compatibility
	}
}

// NewStreamingRelationWithOptions creates a streaming relation with specific options
func NewStreamingRelationWithOptions(columns []query.Symbol, iterator Iterator, opts ExecutorOptions) *StreamingRelation {
	return &StreamingRelation{
		columns:  columns,
		iterator: iterator,
		size:     -1,
		options:  opts,
	}
}

func (r *StreamingRelation) Columns() []query.Symbol {
	return r.columns
}

func (r *StreamingRelation) Symbols() []query.Symbol {
	return r.columns
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
		completeChan := r.cacheComplete  // Capture channel before unlocking
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
	var symbols []string
	for _, col := range r.columns {
		symbols = append(symbols, string(col))
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
			color.CyanString(strings.Join(symbols, " ")),
			color.BlueString("]"),
			color.BlueString(", "),
			countStr,
			"Tuples",
			color.BlueString(")"))
	}

	// Size unknown
	return fmt.Sprintf("%s%s%s%s",
		color.BlueString("Relation(["),
		color.CyanString(strings.Join(symbols, " ")),
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

	// Build index of our columns
	for i, col := range r.columns {
		symbolIndices[col] = i
	}

	// Check each position in the pattern
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
	it := r.Iterator()
	defer it.Close()
	for it.Next() {
		// Tuples are already copied by CachingIterator if shouldCache=true
		tuples = append(tuples, it.Tuple())
	}

	// Sort tuples lexicographically by columns
	sort.Slice(tuples, func(i, j int) bool {
		for k := 0; k < len(r.columns) && k < len(tuples[i]) && k < len(tuples[j]); k++ {
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

// Project returns a new relation with only the specified columns
func (r *StreamingRelation) Project(columns []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(columns) == 0 {
		return nil, fmt.Errorf("cannot project empty column list - invalid query")
	}

	// Streaming is now the default behavior
	// Validate columns exist
	for _, col := range columns {
		found := false
		for _, existing := range r.columns {
			if existing == col {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("cannot project: column %s not found in relation", col)
		}
	}
	// CRITICAL FIX: Pass the relation itself to ProjectIterator, not the raw iterator
	// This allows ProjectIterator to call r.Iterator(), which respects caching/materialization
	// When r.shouldCache=true, the first Iterator() call builds the cache, and both the
	// original relation and the projection can iterate from cached data
	projIter := NewProjectIterator(r, r.columns, columns)
	// BUGFIX: Preserve options (especially EnableTrueStreaming) to prevent re-scanning
	return NewStreamingRelationWithOptions(columns, projIter, r.options), nil
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
	r.cacheComplete = make(chan struct{})  // Create completion signal
	return r  // Return self, NOT a new MaterializedRelation
}

// Sort returns a new relation sorted by the specified order-by clauses
// Warning: This materializes the streaming relation
func (r *StreamingRelation) Sort(orderBy []query.OrderByClause) Relation {
	// First materialize, then sort
	materialized := r.Materialize()
	return materialized.Sort(orderBy)
}

// Filter returns a new relation with only tuples that satisfy the filter
func (r *StreamingRelation) Filter(filter Filter) Relation {
	if r.options.EnableIteratorComposition {
		// Use iterator composition for true streaming
		filterIter := NewFilterIterator(r.iterator, r.columns, filter)
		return NewStreamingRelationWithOptions(r.columns, filterIter, r.options)
	}
	// Fall back to current behavior
	return FilterRelation(r, filter)
}

// FilterWithPredicate filters the relation using a query.Predicate
func (r *StreamingRelation) FilterWithPredicate(pred query.Predicate) Relation {
	if r.options.EnableIteratorComposition {
		// Use iterator composition for true streaming
		predIter := NewPredicateFilterIterator(r.iterator, r.columns, pred)
		return NewStreamingRelationWithOptions(r.columns, predIter, r.options)
	}
	// Fall back to current behavior - materialize then filter
	materialized := r.Materialize()
	return materialized.FilterWithPredicate(pred)
}

// EvaluateFunction evaluates a function and adds its result as a new column
func (r *StreamingRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	if r.options.EnableIteratorComposition {
		// Use iterator composition for true streaming
		evalIter := NewFunctionEvaluatorIterator(r.iterator, r.columns, fn, outputColumn)
		newColumns := append(r.columns, outputColumn)
		return NewStreamingRelationWithOptions(newColumns, evalIter, r.options)
	}
	// Fall back to current behavior - materialize then evaluate
	materialized := r.Materialize()
	return materialized.EvaluateFunction(fn, outputColumn)
}

// Select returns a new relation with only tuples that satisfy the predicate
func (r *StreamingRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

// Join performs a natural join with another relation
func (r *StreamingRelation) Join(other Relation) Relation {
	common := CommonColumns(r, other)
	if len(common) == 0 {
		// No common columns - cross product (expensive!)
		return crossProduct(r, other)
	}
	// Use hash join for efficiency
	return r.HashJoin(other, common)
}

// HashJoin performs an equi-join on specified columns
func (r *StreamingRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return HashJoin(r, other, joinCols)
}

// SemiJoin returns tuples from this relation that have matches in the other
func (r *StreamingRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	return SemiJoin(r, other, joinCols)
}

// AntiJoin returns tuples from this relation that have no matches in the other
func (r *StreamingRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	return AntiJoin(r, other, joinCols)
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

// ColumnIndex returns the index of a column, or -1 if not found
func ColumnIndex(rel Relation, sym query.Symbol) int {
	cols := rel.Columns()
	for i, col := range cols {
		if col == sym {
			return i
		}
	}
	return -1
}

// CommonColumns returns columns that appear in both relations
func CommonColumns(r1, r2 Relation) []query.Symbol {
	cols1 := r1.Columns()
	cols2Set := make(map[query.Symbol]bool)
	for _, col := range r2.Columns() {
		cols2Set[col] = true
	}

	var common []query.Symbol
	for _, col := range cols1 {
		if cols2Set[col] {
			common = append(common, col)
		}
	}
	return common
}

// Select filters a relation based on a predicate
func Select(rel Relation, pred func(Tuple) bool) Relation {
	var selected []Tuple
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		if pred(tuple) {
			selected = append(selected, tuple)
		}
	}

	return NewMaterializedRelation(rel.Columns(), selected)
}

// ProductRelation represents a streaming Cartesian product of multiple relations
// Used for expressions/predicates that reference symbols from disjoint relations
type ProductRelation struct {
	relations []Relation
	columns   []query.Symbol
	options   ExecutorOptions
}

// NewProductRelation creates a new ProductRelation
func NewProductRelation(relations []Relation) *ProductRelation {
	if len(relations) == 0 {
		return &ProductRelation{
			relations: relations,
			columns:   nil,
			options:   ExecutorOptions{},
		}
	}

	// Combine columns from all relations
	var allColumns []query.Symbol
	for _, rel := range relations {
		allColumns = append(allColumns, rel.Columns()...)
	}

	// Extract options from first relation
	opts := relations[0].Options()

	return &ProductRelation{
		relations: relations,
		columns:   allColumns,
		options:   opts,
	}
}

func (p *ProductRelation) Columns() []query.Symbol {
	return p.columns
}

func (p *ProductRelation) Symbols() []query.Symbol {
	return p.columns
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
	return fmt.Sprintf("Product(%d relations, %d columns)", len(p.relations), len(p.columns))
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

func (p *ProductRelation) Project(columns []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(columns) == 0 {
		return nil, fmt.Errorf("cannot project empty column list - invalid query")
	}

	// Validate columns exist
	for _, col := range columns {
		found := false
		for _, existing := range p.Columns() {
			if existing == col {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("cannot project: column %s not found in relation", col)
		}
	}
	// Product relations are streaming - use iterator composition
	// Pass the relation itself so ProjectIterator can call Iterator() when needed
	projIter := NewProjectIterator(p, p.Columns(), columns)
	// Use default options since ProductRelation is a wrapper
	return NewStreamingRelation(columns, projIter), nil
}

func (p *ProductRelation) Materialize() Relation {
	var tuples []Tuple
	it := p.Iterator()
	defer it.Close()

	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}

	return NewMaterializedRelationWithOptions(p.columns, tuples, p.options)
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

func (p *ProductRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	// Materialize then evaluate
	return p.Materialize().EvaluateFunction(fn, outputColumn)
}

func (p *ProductRelation) Select(pred func(Tuple) bool) Relation {
	return Select(p, pred)
}

func (p *ProductRelation) Join(other Relation) Relation {
	// Materialize then join
	return p.Materialize().Join(other)
}

func (p *ProductRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return HashJoinWithOptions(p, other, joinCols, p.options)
}

func (p *ProductRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	// Materialize then semi-join
	return p.Materialize().SemiJoin(other, joinCols)
}

func (p *ProductRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	// Materialize then anti-join
	return p.Materialize().AntiJoin(other, joinCols)
}

func (p *ProductRelation) Aggregate(findElements []query.FindElement) Relation {
	// Materialize then aggregate
	return p.Materialize().Aggregate(findElements)
}

func (p *ProductRelation) Options() ExecutorOptions {
	return p.options
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
