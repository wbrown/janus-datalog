package executor

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Tuple is an alias for query.Tuple to maintain backward compatibility
type Tuple = query.Tuple

var errIncompleteMaterialization = errors.New("streaming relation materialization closed before exhaustion")

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
// collectTuplesInto appends all tuples from rel into dest and returns any error
// the source iterator deferred to Error() (per the Iterator contract), plus a
// Close() error if iteration was otherwise clean. Callers that build a cached
// relation from the result must carry this error onto that relation so the
// failure isn't laundered by materialization.
func collectTuplesInto(dest *[]Tuple, rel Relation) error {
	needsCopy := rel.RequiresCopy()
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		if needsCopy {
			tuple = copyTuple(tuple)
		}
		*dest = append(*dest, tuple)
	}
	err := it.Error()
	if cerr := it.Close(); err == nil {
		err = cerr
	}
	return err
}

// ForEach drives rel's iterator per the documented Iterator contract: it yields
// each tuple to fn, then resolves the outcome whether the loop ended by
// exhaustion, an fn error, or an iterator failure. It returns, in priority
// order: fn's error (iteration stops on the first), then it.Error() (iteration
// aborted), then any Close() error. An iteration or fn error always wins over a
// Close() error so a cleanup failure cannot mask the real cause.
//
// fn must copy the tuple if it retains it: streaming iterators may reuse the
// tuple's backing memory across calls.
func ForEach(rel Relation, fn func(Tuple) error) (err error) {
	it := rel.Iterator()
	// Close() is deferred (panic-safe) and is a separate signal: it only
	// surfaces when nothing else failed, so a cleanup error can't mask the
	// real iteration/fn error.
	defer func() {
		if cerr := it.Close(); err == nil {
			err = cerr
		}
	}()
	for it.Next() {
		if e := fn(it.Tuple()); e != nil {
			// fn error is the more specific cause; return it without consulting
			// the iterator's deferred Error().
			return e
		}
	}
	return it.Error()
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
	tuples := [][]interface{}{}
	if ferr := ForEach(rel, func(src Tuple) error {
		t := make([]interface{}, len(src))
		copy(t, src)
		tuples = append(tuples, t)
		return nil
	}); ferr != nil {
		return nil, ferr
	}
	return tuples, nil
}

// Relation represents a set of tuples with named symbols
type Relation interface {
	// Symbols returns the symbols (attribute names) of this relation
	// In relational theory, a tuple is a map from symbols to values
	Symbols() []query.Symbol

	// Properties returns ordering and candidate-key guarantees. Callers must
	// treat the returned value as immutable.
	Properties() RelationProperties

	// Iterator returns an iterator over tuples
	Iterator() Iterator

	// Size returns the number of tuples (may be expensive for iterators)
	Size() int


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
	// Returns the source iterator's deferred error if iteration failed, so a
	// failed sort source isn't laundered into clean sorted tuples.
	Sorted() ([]Tuple, error)

	// Project returns a new relation with only the specified symbols
	// Returns an error if any requested symbol doesn't exist
	Project(symbols []query.Symbol) (Relation, error)

	// Materialize returns a replayable Relation. Implementations may establish
	// replay through lazy caching; a Relation that is already replayable returns
	// itself without forcing source iteration.
	Materialize() Relation

	// Sort returns a new relation sorted by the specified order-by clauses
	Sort(orderBy []query.OrderByClause) Relation

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

// materializedSize returns a relation's tuple count only when reading it cannot
// advance or cache an iterator. Unknown and streaming sizes are reported as -1.
func materializedSize(rel Relation) int {
	if materialized, ok := rel.(*MaterializedRelation); ok {
		return materialized.Size()
	}
	return -1
}

// Iterator provides streaming access to tuples
type Iterator interface {
	// Next advances to the next tuple
	Next() bool

	// Tuple returns the current tuple
	Tuple() Tuple

	// Close releases any resources
	Close() error

	// Error returns any error encountered during iteration.
	// Callers must check Error() after Next() returns false to
	// distinguish normal exhaustion from execution failure.
	Error() error
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

func (i *CountingIterator) Error() error { return i.inner.Error() }

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
	errPtr            *error        // Pointer to err in StreamingRelation (captured on completion)
	cacheComplete     chan struct{} // Closed when caching finishes
	cachingInProgress *bool         // Pointer to flag in StreamingRelation
	cacheReady        *bool         // Pointer to ready flag in StreamingRelation
	mu                *sync.Mutex   // Protects state transitions
	done              bool
	signaled          bool // Ensure we only signal once
}

// NewCachingIterator creates a caching iterator that builds a cache as it iterates
func NewCachingIterator(inner Iterator, cachePtr *[]Tuple, errPtr *error, completeChan chan struct{},
	cachingInProgress *bool, cacheReady *bool, mu *sync.Mutex) *CachingIterator {
	return &CachingIterator{
		inner:             inner,
		cache:             cachePtr,
		errPtr:            errPtr,
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

	// Iteration complete - capture any deferred source error for cache replay,
	// then signal waiting goroutines. First error wins: never overwrite an
	// already-recorded error (possibly with nil).
	ci.done = true
	if ci.errPtr != nil {
		ci.mu.Lock()
		if *ci.errPtr == nil {
			*ci.errPtr = ci.inner.Error()
		}
		ci.mu.Unlock()
	}
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
	if !ci.done {
		ci.done = true
		if ci.errPtr != nil {
			ci.mu.Lock()
			*ci.errPtr = errIncompleteMaterialization
			ci.mu.Unlock()
		}
		closeErr := ci.inner.Close()
		ci.signalComplete()
		if closeErr != nil {
			return errors.Join(errIncompleteMaterialization, closeErr)
		}
		return errIncompleteMaterialization
	}
	ci.signalComplete()
	return ci.inner.Close()
}

func (ci *CachingIterator) Error() error { return ci.inner.Error() }

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
	symbols    []query.Symbol
	tuples     []Tuple
	options    ExecutorOptions
	properties RelationProperties
	// err is the deferred error from the source iteration that produced these
	// tuples (e.g., a stream that failed partway while being cached). It is
	// replayed by Iterator().Error() so a failure isn't laundered by
	// materialization. nil when the source iterated cleanly.
	err error
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

// NewMaterializedRelationWithProperties creates a deduplicated materialized
// relation with explicit ordering and candidate-key guarantees.
func NewMaterializedRelationWithProperties(
	symbols []query.Symbol,
	tuples []Tuple,
	opts ExecutorOptions,
	properties RelationProperties,
) *MaterializedRelation {
	return &MaterializedRelation{
		symbols:    symbols,
		tuples:     deduplicateTuples(tuples),
		options:    opts,
		properties: properties.clone(),
	}
}

// newMaterializedRelationFromSet constructs a Relation from tuples already
// proven duplicate-free by the producing relational operator.
func newMaterializedRelationFromSet(
	symbols []query.Symbol,
	tuples []Tuple,
	opts ExecutorOptions,
	properties RelationProperties,
) *MaterializedRelation {
	return &MaterializedRelation{
		symbols:    symbols,
		tuples:     tuples,
		options:    opts,
		properties: properties.clone(),
	}
}

// NewMaterializedRelationFromSet constructs a Relation at the package
// boundary from a tuple stream the caller warrants is already a set — each
// complete tuple appears at most once, as produced by a distinct scan or a
// set-preserving operator. The warranty replaces the deduplication pass;
// the value-domain admission check still runs because boundary callers
// inject raw Go values into relational flow. Interior operators, whose
// tuples never left relational flow and which carry derived
// RelationProperties, construct through newMaterializedRelationFromSet
// instead.
func NewMaterializedRelationFromSet(symbols []query.Symbol, tuples []Tuple, opts ExecutorOptions) *MaterializedRelation {
	validateTupleValueDomain(tuples)
	return &MaterializedRelation{
		symbols: symbols,
		tuples:  tuples,
		options: opts,
	}
}

func validateTupleValueDomain(tuples []Tuple) {
	for _, tuple := range tuples {
		for _, value := range tuple {
			hashValue(value)
		}
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
		if !seen.PutIfAbsent(key, true) {
			result = append(result, tuple)
		}
	}

	return result
}

func (r *MaterializedRelation) Symbols() []query.Symbol {
	return r.symbols
}

func (r *MaterializedRelation) Properties() RelationProperties {
	return r.properties
}

func (r *MaterializedRelation) Iterator() Iterator {
	return &sliceIterator{
		tuples: r.tuples,
		pos:    -1,
		err:    r.err,
	}
}

// carryErr propagates this relation's deferred (taint) error onto a relation
// derived from it, so a transform of incomplete data stays marked incomplete.
// Used by the unary transforms, which build a new relation from r's tuples.
func (r *MaterializedRelation) carryErr(derived Relation) Relation {
	if r.err == nil {
		return derived
	}
	if m, ok := derived.(*MaterializedRelation); ok && m.err == nil {
		m.err = r.err
	}
	return derived
}

// EmptyRelationError returns the deferred (taint) error of a relation that
// reports zero tuples. An errored relation that materialized empty is not an
// empty relation — its zero rows mean "the scan failed", not "no data" — so
// every consumer that branches on emptiness must consult this before
// treating absence of tuples as absence of data. Laundering the distinction
// turned a mandated loud failure into a silent empty
// (docs/bugs/BUG_MISSING_ON_LOOKUPLESS_MATCHER_SILENTLY_EMPTY.md).
//
// Call only on relations reporting Size() == 0: probing iterates one step,
// which is destructive on a single-use stream (streaming relations report
// Size() -1 and never take emptiness branches).
func EmptyRelationError(rel Relation) error {
	it := rel.Iterator()
	_ = it.Next()
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (r *MaterializedRelation) Size() int {
	return len(r.tuples)
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
	return query.SymbolIndex(r.symbols, sym)
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
		if _, exists := symbolIndices[sym.Name]; exists && !query.ContainsSymbol(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if sym, ok := pattern.GetV().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists && !query.ContainsSymbol(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if len(pattern.Elements) > 3 {
		if sym, ok := pattern.GetT().(query.Variable); ok {
			if _, exists := symbolIndices[sym.Name]; exists && !query.ContainsSymbol(neededSymbols, sym.Name) {
				neededSymbols = append(neededSymbols, sym.Name)
			}
		}
	}

	// If no symbols needed, return empty relation
	if len(neededSymbols) == 0 {
		return NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{}, r.options)
	}

	// Project to needed symbols. neededSymbols are derived from the pattern
	// elements above and intersected with `symbolIndices`, so they must exist
	// in the relation. If Project errors anyway, that's a contract violation —
	// surface it loudly instead of silently returning a half-broken result.
	result, err := r.Project(neededSymbols)
	if err != nil {
		panic(fmt.Sprintf("ProjectFromPattern: Project of derived symbols failed: %v", err))
	}
	return result
}

// Sorted returns tuples sorted by the relation's symbols
// First symbol is primary sort key, second is secondary, etc.
func (r *MaterializedRelation) Sorted() ([]Tuple, error) {
	// Surface a deferred source error rather than returning sorted partial data.
	if r.err != nil {
		return nil, r.err
	}
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

	return sorted, nil
}

// Project returns a new relation with only the specified symbols
func (r *MaterializedRelation) Project(symbols []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(symbols) == 0 {
		return nil, fmt.Errorf("cannot project empty symbol list - invalid query")
	}

	// Find symbol indices
	indices := query.SymbolIndexTable(r.symbols, symbols)
	for i, idx := range indices {
		if idx < 0 {
			// Symbol not found - this is a query error in Datalog
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation (has symbols: %v)", symbols[i], r.symbols)
		}
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

	properties := r.properties.project(symbols)
	var result *MaterializedRelation
	if projectionPreservesSet(r.symbols, symbols, properties) {
		result = newMaterializedRelationFromSet(
			symbols,
			projected,
			r.options,
			properties,
		)
	} else {
		result = NewMaterializedRelationWithProperties(
			symbols,
			projected,
			r.options,
			properties,
		)
	}
	result.err = r.err // a projection of tainted data is still tainted
	return result, nil
}

// Materialize returns self since MaterializedRelation is already materialized
func (r *MaterializedRelation) Materialize() Relation {
	return r
}

// Sort returns a new relation sorted by the specified order-by clauses
func (r *MaterializedRelation) Sort(orderBy []query.OrderByClause) Relation {
	// Use the SortRelation function we created
	return r.carryErr(SortRelation(r, orderBy))
}

// FilterWithPredicate filters the relation using a query.Predicate. The
// first evaluation error stops the loop and surfaces as the result's
// deferred error — an eval failure must not silently drop the tuple. The
// source relation's own deferred error carries through first.
func (r *MaterializedRelation) FilterWithPredicate(pred query.Predicate) Relation {
	var filtered []Tuple
	var evalErr error
	for _, tuple := range r.tuples {
		bindings := make(map[query.Symbol]interface{})
		bindTuple(bindings, r.symbols, tuple)

		passes, err := pred.Eval(bindings)
		if err != nil {
			evalErr = err
			break
		}
		if passes {
			filtered = append(filtered, tuple)
		}
	}

	mat := newMaterializedRelationFromSet(
		r.symbols,
		filtered,
		r.options,
		r.properties,
	)
	if r.err != nil {
		mat.err = r.err
	} else if evalErr != nil {
		mat.err = evalErr
	}
	return mat
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

	// Process each tuple. Fail-fast on eval errors and propagate them via
	// result.err — silently `continue`ing past a failed Eval would launder
	// real errors into a clean (truncated) materialized relation.
	var newTuples []Tuple
	var evalErr error
	for _, tuple := range r.tuples {
		// Create bindings from tuple
		bindings := make(map[query.Symbol]interface{})
		bindTuple(bindings, r.symbols, tuple)

		// Evaluate the function
		result, err := fn.Eval(bindings)
		if err != nil {
			evalErr = err
			break
		}

		// get-some signals "no attribute matched" via Found=false (not via
		// error). Skip the tuple in that case, not the eval-error path.
		if gsr, ok := result.(*query.GetSomeResult); ok {
			if !gsr.Found {
				continue
			}
			result = gsr.Value
		}

		if err := admitExpressionResult(fn, result); err != nil {
			evalErr = err
			break
		}

		// Create new tuple with function result
		newTuple := append(tuple, result)
		newTuples = append(newTuples, newTuple)
	}

	mat := newMaterializedRelationFromSet(
		newSymbols,
		newTuples,
		r.options,
		r.properties.addSymbol(outputSymbol),
	)
	if evalErr != nil {
		mat.err = evalErr
	} else if r.err != nil {
		// Inherit the source relation's deferred error.
		mat.err = r.err
	}
	return mat
}

// sliceIterator iterates over a slice of tuples
type sliceIterator struct {
	tuples []Tuple
	pos    int
	// err is the deferred error replayed by a cached/materialized relation that
	// was built from a source whose iteration failed. nil for clean caches.
	err error
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

func (it *sliceIterator) Error() error { return it.err }

// StreamingRelation wraps an iterator as a relation
type StreamingRelation struct {
	symbols    []query.Symbol
	iterator   Iterator
	size       int             // -1 if unknown
	options    ExecutorOptions // Options from the factory that created this relation
	properties RelationProperties

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

	// err holds the source iterator's deferred error, captured when the cache
	// finishes building. Replayed by cache-replay iterators so a failure during
	// the first (caching) pass isn't lost on subsequent iterations.
	err error
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

// NewStreamingRelationWithProperties creates a streaming relation with explicit
// ordering and candidate-key guarantees.
func NewStreamingRelationWithProperties(
	symbols []query.Symbol,
	iterator Iterator,
	opts ExecutorOptions,
	properties RelationProperties,
) *StreamingRelation {
	return &StreamingRelation{
		symbols:    symbols,
		iterator:   iterator,
		size:       -1,
		options:    opts,
		properties: properties.clone(),
	}
}

func (r *StreamingRelation) Symbols() []query.Symbol {
	return r.symbols
}

func (r *StreamingRelation) Properties() RelationProperties {
	return r.properties
}

func (r *StreamingRelation) Iterator() Iterator {
	r.mu.Lock()

	// Fast path: If we have a complete cache, return reusable iterator
	if r.cacheReady {
		err := r.err
		r.mu.Unlock()
		return &sliceIterator{
			tuples: r.cache,
			pos:    -1,
			err:    err, // replay the source failure captured during cache build
		}
	}

	// If caching is in progress, BLOCK until it completes
	if r.cachingInProgress {
		completeChan := r.cacheComplete // Capture channel before unlocking
		r.mu.Unlock()

		// BLOCK: Wait for cache to complete
		<-completeChan

		// Cache is now ready, return iterator over cached data
		r.mu.Lock()
		err := r.err
		r.mu.Unlock()
		return &sliceIterator{
			tuples: r.cache,
			pos:    -1,
			err:    err,
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
	var cacheCollector *annotations.Collector
	var cacheEvent annotations.Event
	if r.shouldCache {
		r.cachingInProgress = true
		if r.options.Collector != nil {
			cacheCollector = r.options.Collector
			cacheEvent = annotations.Event{
				Name: annotations.RelationCacheEnabled,
				Data: map[string]interface{}{
					"symbols":            append([]query.Symbol(nil), r.symbols...),
					"symbol_count":       len(r.symbols),
					"cached_tuple_count": len(r.cache),
				},
			}
		}
	}

	r.mu.Unlock()
	if cacheCollector != nil {
		cacheCollector.Add(cacheEvent)
	}

	// Create base iterator
	baseIter := r.iterator

	// Wrap with counting iterator for lightweight size tracking
	if r.counter == nil {
		r.counter = NewCountingIterator(baseIter)
		baseIter = r.counter
	}

	// If caching enabled, wrap with CachingIterator
	if r.shouldCache {
		return NewCachingIterator(baseIter, &r.cache, &r.err, r.cacheComplete, &r.cachingInProgress, &r.cacheReady, &r.mu)
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

// Get returns a specific tuple by index
func (r *StreamingRelation) Get(i int) Tuple {
	if i < 0 {
		return nil
	}
	r.Materialize()
	it := r.Iterator()
	for it.Next() {
	}
	iterErr := it.Error()
	if closeErr := it.Close(); iterErr == nil {
		iterErr = closeErr
	}
	if iterErr != nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.cache) {
		return nil
	}
	return r.cache[i]
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
		if _, exists := symbolIndices[sym.Name]; exists && !query.ContainsSymbol(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if sym, ok := pattern.GetV().(query.Variable); ok {
		if _, exists := symbolIndices[sym.Name]; exists && !query.ContainsSymbol(neededSymbols, sym.Name) {
			neededSymbols = append(neededSymbols, sym.Name)
		}
	}
	if len(pattern.Elements) > 3 {
		if sym, ok := pattern.GetT().(query.Variable); ok {
			if _, exists := symbolIndices[sym.Name]; exists && !query.ContainsSymbol(neededSymbols, sym.Name) {
				neededSymbols = append(neededSymbols, sym.Name)
			}
		}
	}

	// If no symbols needed, return empty relation
	if len(neededSymbols) == 0 {
		return NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{}, r.options)
	}

	// Use the StreamingRelation's Project method which creates a streaming
	// projection iterator instead of the global Project() function which
	// materializes. neededSymbols are derived from pattern elements above and
	// intersected with the relation's symbols, so they must exist. A Project
	// error here is a contract violation — surface it loudly.
	result, err := r.Project(neededSymbols)
	if err != nil {
		panic(fmt.Sprintf("ProjectFromPattern: Project of derived symbols failed: %v", err))
	}
	return result
}

// Sorted returns tuples sorted by the relation's symbols
func (r *StreamingRelation) Sorted() ([]Tuple, error) {
	// Sorted() requires all data in memory
	// Set shouldCache flag so iteration builds the cache
	r.Materialize()

	// Now consume iterator to build cache; surface a deferred source error
	// rather than returning sorted partial data.
	var tuples []Tuple
	if err := collectTuplesInto(&tuples, r); err != nil {
		return nil, err
	}

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

	return tuples, nil
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
		if !query.ContainsSymbol(r.symbols, sym) {
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation", sym)
		}
	}
	// CRITICAL FIX: Pass the relation itself to ProjectIterator, not the raw iterator
	// This allows ProjectIterator to call r.Iterator(), which respects caching/materialization
	// When r.shouldCache=true, the first Iterator() call builds the cache, and both the
	// original relation and the projection can iterate from cached data
	projIter := NewProjectIterator(r, r.symbols, symbols)
	properties := r.properties.project(symbols)
	var resultIterator Iterator = projIter
	if !projectionPreservesSet(r.symbols, symbols, properties) {
		// A reducing projection can map distinct input tuples to the same
		// output tuple and must restore set semantics. ProjectIterator yields
		// a fresh tuple per Next, so the seen-keys need no copy.
		resultIterator = NewDedupIterator(projIter, 0, false)
	}
	// BUGFIX: Preserve options (especially EnableTrueStreaming) to prevent re-scanning
	return NewStreamingRelationWithProperties(
		symbols,
		resultIterator,
		r.options,
		properties,
	), nil
}

// Materialize converts this streaming relation to a materialized one
func (r *StreamingRelation) Materialize() Relation {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If already cached or cache completed (empty result), return self (idempotent)
	if r.cache != nil || r.cacheReady {
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
	err := collectTuplesInto(&tuples, r)

	mat := NewMaterializedRelationWithProperties(r.symbols, tuples, r.options, r.properties)
	if err != nil {
		// Source failed mid-iteration: carry the error so the boundary sees it
		// and discards the (incomplete) data. Sorting tainted data is moot.
		mat.err = err
		return mat
	}
	return mat.Sort(orderBy)
}

// FilterWithPredicate filters the relation using a query.Predicate.
// Filtering is a pure streaming transform — one pass, nothing retained —
// so the result is always a composed stream. Consumers that need replay
// call Materialize at their point of need.
func (r *StreamingRelation) FilterWithPredicate(pred query.Predicate) Relation {
	predIter := NewPredicateFilterIterator(r.Iterator(), r.symbols, pred)
	return NewStreamingRelationWithProperties(r.symbols, predIter, r.options, r.properties)
}

// EvaluateFunction evaluates a function and adds its result as a new symbol.
// Like filtering, this is a pure streaming transform: always a composed
// stream, never a buffer.
func (r *StreamingRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	evalIter := NewFunctionEvaluatorIterator(r.Iterator(), r.symbols, fn, outputSymbol)
	newSymbols := append(r.symbols, outputSymbol)
	return NewStreamingRelationWithProperties(
		newSymbols,
		evalIter,
		r.options,
		r.properties.addSymbol(outputSymbol),
	)
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
	return query.SymbolIndex(rel.Symbols(), sym)
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

	for it.Next() {
		tuple := it.Tuple()
		if pred(tuple) {
			if needsCopy {
				tuple = copyTuple(tuple)
			}
			selected = append(selected, tuple)
		}
	}
	scanErr := it.Error()
	if closeErr := it.Close(); scanErr == nil {
		scanErr = closeErr
	}

	// A failed scan is not an empty selection — carry it as the result's
	// deferred error.
	result := newMaterializedRelationFromSet(
		rel.Symbols(),
		selected,
		rel.Options(),
		rel.Properties(),
	)
	result.err = scanErr
	return result
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

	// The nested-loop ProductIterator consumes the leftmost operand once but
	// rewinds every other operand (reopening its Iterator()) once per outer
	// tuple. StreamingRelation is single-use, so make relations[1:] re-iterable
	// via Materialize() — the existing CachingIterator lazily caches each on its
	// first pass and replays on rewind. The leftmost still streams, and the
	// product output still streams (only the rewound operands are cached).
	reiterable := make([]Relation, len(relations))
	reiterable[0] = relations[0]
	for i := 1; i < len(relations); i++ {
		reiterable[i] = relations[i].Materialize()
	}

	// Combine symbols from all relations
	var allSymbols []query.Symbol
	for _, rel := range reiterable {
		allSymbols = append(allSymbols, rel.Symbols()...)
	}

	// Extract options from first relation
	opts := reiterable[0].Options()

	return &ProductRelation{
		relations: reiterable,
		symbols:   allSymbols,
		options:   opts,
	}
}

func (p *ProductRelation) Symbols() []query.Symbol {
	return p.symbols
}

func (p *ProductRelation) Properties() RelationProperties {
	return RelationProperties{}
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

func (p *ProductRelation) Sorted() ([]Tuple, error) {
	return p.Materialize().Sorted()
}

func (p *ProductRelation) Project(symbols []query.Symbol) (Relation, error) {
	// Empty projection is invalid in Datalog - must have at least one find element
	if len(symbols) == 0 {
		return nil, fmt.Errorf("cannot project empty symbol list - invalid query")
	}

	// Validate symbols exist
	relSymbols := p.Symbols()
	for _, sym := range symbols {
		if !query.ContainsSymbol(relSymbols, sym) {
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation", sym)
		}
	}
	// Product relations are streaming - use iterator composition
	// Pass the relation itself so ProjectIterator can call Iterator() when needed
	projIter := NewProjectIterator(p, p.Symbols(), symbols)
	properties := p.Properties().project(symbols)
	var resultIterator Iterator = projIter
	if !projectionPreservesSet(p.symbols, symbols, properties) {
		// A reducing projection can map distinct input tuples to the same
		// output tuple and must restore set semantics. ProjectIterator yields
		// a fresh tuple per Next, so the seen-keys need no copy.
		resultIterator = NewDedupIterator(projIter, 0, false)
	}
	return NewStreamingRelationWithProperties(
		symbols,
		resultIterator,
		p.options,
		properties,
	), nil
}

func (p *ProductRelation) Materialize() Relation {
	var tuples []Tuple
	err := collectTuplesInto(&tuples, p)
	result := NewMaterializedRelationWithOptions(p.symbols, tuples, p.options)
	result.err = err
	return result
}

func (p *ProductRelation) Sort(orderBy []query.OrderByClause) Relation {
	return p.Materialize().Sort(orderBy)
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

func (pi *ProductIterator) Error() error {
	for _, it := range pi.iterators {
		if it != nil {
			if err := it.Error(); err != nil {
				return err
			}
		}
	}
	return nil
}
