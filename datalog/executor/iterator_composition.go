package executor

import (
	"fmt"
	"math"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// SimpleFilter is a simple filter function that implements the Filter interface
type SimpleFilter struct {
	filterFunc func(Tuple) bool
	symbols    []query.Symbol
}

// NewSimpleFilter creates a filter from a function
func NewSimpleFilter(filterFunc func(Tuple) bool) *SimpleFilter {
	return &SimpleFilter{filterFunc: filterFunc}
}

// RequiredSymbols returns empty since this is a simple filter
func (f *SimpleFilter) RequiredSymbols() []query.Symbol {
	return []query.Symbol{}
}

// Evaluate applies the filter function
func (f *SimpleFilter) Evaluate(tuple Tuple, symbols []query.Symbol) bool {
	return f.filterFunc(tuple)
}

// String returns a string representation
func (f *SimpleFilter) String() string {
	return "SimpleFilter"
}

// FilterIterator wraps another iterator and only returns tuples that match a filter predicate
type FilterIterator struct {
	source  Iterator
	filter  Filter
	current Tuple
	symbols []query.Symbol
}

// NewFilterIterator creates a new filtering iterator
func NewFilterIterator(source Iterator, symbols []query.Symbol, filter Filter) *FilterIterator {
	return &FilterIterator{
		source:  source,
		filter:  filter,
		symbols: symbols,
	}
}

// Next advances to the next tuple that matches the filter
func (it *FilterIterator) Next() bool {
	for it.source.Next() {
		it.current = it.source.Tuple()
		if it.filter.Evaluate(it.current, it.symbols) {
			return true
		}
	}
	return false
}

// Tuple returns the current tuple
func (it *FilterIterator) Tuple() Tuple {
	return it.current
}

// Close releases any resources
func (it *FilterIterator) Close() error {
	return it.source.Close()
}

func (it *FilterIterator) Error() error { return it.source.Error() }

// ProjectIterator projects specific symbols from the source relation
type ProjectIterator struct {
	relation   Relation // Source relation (may be cached/materialized)
	source     Iterator // Lazily obtained from relation.Iterator()
	indices    []int    // Indices of symbols to keep from source
	current    Tuple
	newSymbols []query.Symbol
}

// NewProjectIterator creates a new projection iterator
func NewProjectIterator(relation Relation, sourceSymbols []query.Symbol, targetSymbols []query.Symbol) *ProjectIterator {
	// Compute indices for projection
	indices := make([]int, len(targetSymbols))
	for i, targetSym := range targetSymbols {
		for j, sourceSym := range sourceSymbols {
			if sourceSym == targetSym {
				indices[i] = j
				break
			}
		}
	}

	return &ProjectIterator{
		relation:   relation,
		indices:    indices,
		newSymbols: targetSymbols,
	}
}

// Next advances to the next tuple and projects it
func (it *ProjectIterator) Next() bool {
	// Lazily get iterator from relation on first call
	// This allows the relation to handle caching/materialization
	if it.source == nil {
		it.source = it.relation.Iterator()
	}

	if !it.source.Next() {
		return false
	}

	sourceTuple := it.source.Tuple()
	it.current = make(Tuple, len(it.indices))
	for i, idx := range it.indices {
		if idx < len(sourceTuple) {
			it.current[i] = sourceTuple[idx]
		}
	}
	return true
}

// Tuple returns the current projected tuple
func (it *ProjectIterator) Tuple() Tuple {
	return it.current
}

// Close releases any resources
func (it *ProjectIterator) Close() error {
	if it.source != nil {
		return it.source.Close()
	}
	return nil
}

func (it *ProjectIterator) Error() error {
	if it.source != nil {
		return it.source.Error()
	}
	return nil
}

// TransformIterator applies a transformation function to each tuple
type TransformIterator struct {
	source    Iterator
	transform func(Tuple) Tuple
	current   Tuple
}

// NewTransformIterator creates a new transform iterator
func NewTransformIterator(source Iterator, transform func(Tuple) Tuple) *TransformIterator {
	return &TransformIterator{
		source:    source,
		transform: transform,
	}
}

// Next advances to the next tuple and transforms it
func (it *TransformIterator) Next() bool {
	if !it.source.Next() {
		return false
	}
	it.current = it.transform(it.source.Tuple())
	return true
}

// Tuple returns the current transformed tuple
func (it *TransformIterator) Tuple() Tuple {
	return it.current
}

// Close releases any resources
func (it *TransformIterator) Close() error {
	return it.source.Close()
}

func (it *TransformIterator) Error() error { return it.source.Error() }

// ConcatIterator concatenates multiple iterators sequentially
type ConcatIterator struct {
	iterators []Iterator
	current   int
	tuple     Tuple
}

// NewConcatIterator creates a new concatenating iterator
func NewConcatIterator(iterators ...Iterator) *ConcatIterator {
	return &ConcatIterator{
		iterators: iterators,
		current:   0,
	}
}

// Next advances to the next tuple across all iterators
func (it *ConcatIterator) Next() bool {
	for it.current < len(it.iterators) {
		if it.iterators[it.current].Next() {
			it.tuple = it.iterators[it.current].Tuple()
			return true
		}
		// Current iterator exhausted, move to next
		it.iterators[it.current].Close()
		it.current++
	}
	return false
}

// Tuple returns the current tuple
func (it *ConcatIterator) Tuple() Tuple {
	return it.tuple
}

// Close releases all resources
func (it *ConcatIterator) Close() error {
	var lastErr error
	// Close any remaining iterators
	for i := it.current; i < len(it.iterators); i++ {
		if err := it.iterators[i].Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (it *ConcatIterator) Error() error {
	if it.current < len(it.iterators) {
		return it.iterators[it.current].Error()
	}
	return nil
}

// PredicateFilterIterator wraps another iterator and filters based on a query.Predicate
type PredicateFilterIterator struct {
	source    Iterator
	predicate query.Predicate
	symbols   []query.Symbol
	current   Tuple
	err       error
}

// NewPredicateFilterIterator creates a new predicate-based filtering iterator
func NewPredicateFilterIterator(source Iterator, symbols []query.Symbol, predicate query.Predicate) *PredicateFilterIterator {
	return &PredicateFilterIterator{
		source:    source,
		predicate: predicate,
		symbols:   symbols,
	}
}

// Next advances to the next tuple that matches the predicate
func (it *PredicateFilterIterator) Next() bool {
	for it.source.Next() {
		it.current = it.source.Tuple()

		// Create bindings for predicate evaluation
		bindings := make(map[query.Symbol]interface{})
		for i, sym := range it.symbols {
			if i < len(it.current) {
				bindings[sym] = it.current[i]
			}
		}

		// Evaluate predicate
		result, err := it.predicate.Eval(bindings)
		if err != nil {
			it.err = err
			return false
		}
		if result {
			return true
		}
	}
	return false
}

// Tuple returns the current tuple
func (it *PredicateFilterIterator) Tuple() Tuple {
	return it.current
}

// Close releases any resources
func (it *PredicateFilterIterator) Close() error {
	return it.source.Close()
}

func (it *PredicateFilterIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.source.Error()
}

// admitExpressionResult checks a function result entering relational flow.
// Expression evaluation is the one producer of NaN inside the engine —
// arithmetic over Inf operands (Inf - Inf, 0 * Inf, Inf / Inf), where Inf is
// itself a value and reachable from finite data by overflow — so NaN fails
// loudly here rather than entering joins and sorts. The write and input
// boundaries exclude it everywhere else.
func admitExpressionResult(fn query.Function, result interface{}) error {
	if f, ok := result.(float64); ok && math.IsNaN(f) {
		return fmt.Errorf("expression %v produced NaN, which is not a datalog value", fn)
	}
	return nil
}

// FunctionEvaluatorIterator adds a new symbol by evaluating a function
type FunctionEvaluatorIterator struct {
	source       Iterator
	function     query.Function
	outputSymbol query.Symbol
	symbols      []query.Symbol // Original symbols
	newSymbols   []query.Symbol // Symbols after adding function output (or same if unifying)
	current      Tuple
	existingIdx  int   // >=0 if outputSymbol already in symbols (unification mode)
	err          error // First eval error; replayed via Error() at iteration end
}

// NewFunctionEvaluatorIterator creates an iterator that adds a symbol via function evaluation.
// If outputSymbol already exists in the source symbols, the iterator unifies
// (filters to tuples where the function result matches the existing value)
// instead of appending a duplicate.
func NewFunctionEvaluatorIterator(source Iterator, symbols []query.Symbol, function query.Function, outputSymbol query.Symbol) *FunctionEvaluatorIterator {
	// Check if the output symbol already exists (unification case)
	existingIdx := -1
	for i, sym := range symbols {
		if sym == outputSymbol {
			existingIdx = i
			break
		}
	}

	var newSymbols []query.Symbol
	if existingIdx >= 0 {
		// Symbol already exists — output symbols unchanged (unification, not extension)
		newSymbols = symbols
	} else {
		newSymbols = append(symbols, outputSymbol)
	}

	return &FunctionEvaluatorIterator{
		source:       source,
		function:     function,
		outputSymbol: outputSymbol,
		symbols:      symbols,
		newSymbols:   newSymbols,
		existingIdx:  existingIdx,
	}
}

// Next advances to the next tuple and evaluates the function
func (it *FunctionEvaluatorIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for it.source.Next() {
		sourceTuple := it.source.Tuple()

		// Create bindings for function evaluation
		bindings := make(map[query.Symbol]interface{})
		for i, sym := range it.symbols {
			if i < len(sourceTuple) {
				bindings[sym] = sourceTuple[i]
			}
		}

		// Evaluate function. Fail-fast on eval errors — store the deferred
		// error so Error() returns it after Next() reports exhaustion.
		// Silently `continue`ing here laundered real Eval failures into a
		// clean iteration end indistinguishable from "no more tuples."
		result, err := it.function.Eval(bindings)
		if err != nil {
			it.err = err
			return false
		}

		// get-some signals "no attribute matched" via Found=false (not via
		// error). Skip the tuple in that case — that's a soft no-match, not
		// the error-as-signal misuse the swallowing loop existed to absorb.
		if gsr, ok := result.(*query.GetSomeResult); ok {
			if !gsr.Found {
				continue
			}
			result = gsr.Value
		}

		if err := admitExpressionResult(it.function, result); err != nil {
			it.err = err
			return false
		}

		if it.existingIdx >= 0 {
			// Unification: check that function result matches existing binding
			if it.existingIdx < len(sourceTuple) && !datalog.ValuesEqual(sourceTuple[it.existingIdx], result) {
				continue // Mismatch — filter this tuple
			}
			// Match — pass through unchanged (no new symbol added)
			it.current = sourceTuple
		} else {
			// Extension: append function result as new symbol
			it.current = make(Tuple, len(sourceTuple)+1)
			copy(it.current, sourceTuple)
			it.current[len(sourceTuple)] = result
		}

		return true
	}
	return false
}

// Tuple returns the current tuple with function result
func (it *FunctionEvaluatorIterator) Tuple() Tuple {
	return it.current
}

// Close releases any resources
func (it *FunctionEvaluatorIterator) Close() error {
	return it.source.Close()
}

func (it *FunctionEvaluatorIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.source.Error()
}

// DedupIterator removes duplicate tuples based on full tuple equality
type DedupIterator struct {
	source  Iterator
	seen    *TupleKeyMap
	current Tuple
}

// NewDedupIterator creates an iterator that removes duplicates
func NewDedupIterator(source Iterator, expectedSize int) *DedupIterator {
	return &DedupIterator{
		source: source,
		seen:   NewTupleKeyMapWithCapacity(expectedSize),
	}
}

// Next advances to the next unique tuple
func (it *DedupIterator) Next() bool {
	for it.source.Next() {
		it.current = it.source.Tuple()
		key := NewTupleKeyFull(it.current)
		if !it.seen.PutIfAbsent(key, true) {
			return true
		}
	}
	return false
}

// Tuple returns the current tuple
func (it *DedupIterator) Tuple() Tuple {
	return it.current
}

// Close releases any resources
func (it *DedupIterator) Close() error {
	return it.source.Close()
}

func (it *DedupIterator) Error() error { return it.source.Error() }
