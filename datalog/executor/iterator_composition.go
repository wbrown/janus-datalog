package executor

import (
	"fmt"
	"math"

	"github.com/wbrown/janus-datalog/datalog/query"
)

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
	// Compute indices for projection. An absent target reads position 0 —
	// long-standing behavior; callers validate presence before constructing.
	indices := make([]int, len(targetSymbols))
	for i, targetSym := range targetSymbols {
		if j := query.SymbolIndex(sourceSymbols, targetSym); j >= 0 {
			indices[i] = j
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

// RequiresCopy is false: Next builds a fresh slice per projected tuple.
func (it *ProjectIterator) RequiresCopy() bool { return false }

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

// failedTupleIterator is an iterator that answers the tuple its own failure
// arose on. After Next answers false with a failure, failedTuple answers that
// tuple over the iterator's symbols, with no value at the positions the
// failing evaluation would have filled, which it answers second, and false
// when the failure did not arise on a tuple it read; resume drops the
// failure, and Next reads on from the next tuple. A windowed form's reader
// carries such a tuple into its window, so the form decides the failure at
// the tuple's key (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, the failure
// contract).
type failedTupleIterator interface {
	Iterator
	failedTuple() (Tuple, []int, bool)
	resume()
}

// PredicateFilterIterator wraps another iterator and filters based on a query.Predicate
type PredicateFilterIterator struct {
	source    Iterator
	predicate query.Predicate
	symbols   []query.Symbol
	// lookupPredicate is the predicate when it is a database-function
	// predicate given a lookup, which it evaluates through.
	lookupPredicate *query.DatabaseFunctionPredicate
	lookup          query.EntityLookup
	envSymbols      []query.Symbol
	envTuple        Tuple
	bindings        map[query.Symbol]interface{}
	current         Tuple
	err             error
	// failed is the source tuple err arose on, nil when err is the source's.
	failed Tuple
}

// NewPredicateFilterIterator creates an iterator filtering source, whose
// tuples carry symbols, by predicate. lookup is the lookup a database-function
// predicate evaluates through, and env is the scope's environment relation,
// whose one tuple binds into every evaluation, a tuple's symbol of the same
// name taking its place; either may be nil.
func NewPredicateFilterIterator(source Iterator, symbols []query.Symbol, predicate query.Predicate, lookup query.EntityLookup, env Relation) *PredicateFilterIterator {
	envSymbols, envTuple := environmentTuple(env)
	it := &PredicateFilterIterator{
		source:     source,
		predicate:  predicate,
		symbols:    symbols,
		lookup:     lookup,
		envSymbols: envSymbols,
		envTuple:   envTuple,
		bindings:   make(map[query.Symbol]interface{}, len(symbols)+len(envSymbols)),
	}
	if dbPred, ok := predicate.(*query.DatabaseFunctionPredicate); ok && lookup != nil {
		it.lookupPredicate = dbPred
	}
	return it
}

// Next advances to the next tuple that matches the predicate
func (it *PredicateFilterIterator) Next() bool {
	for it.source.Next() {
		it.current = it.source.Tuple()

		for k := range it.bindings {
			delete(it.bindings, k)
		}
		for i := range it.envSymbols {
			it.bindings[it.envSymbols[i]] = it.envTuple[i]
		}
		bindTuple(it.bindings, it.symbols, it.current)

		var result bool
		var err error
		if it.lookupPredicate != nil {
			result, err = it.lookupPredicate.EvalWithLookup(it.bindings, it.lookup)
		} else {
			result, err = it.predicate.Eval(it.bindings)
		}
		if err != nil {
			it.err = err
			it.failed = it.current
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

// RequiresCopy is the source's: Tuple hands back the source's own tuple.
func (it *PredicateFilterIterator) RequiresCopy() bool { return it.source.RequiresCopy() }

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

// failedTuple answers the source tuple the iterator's own failure arose on,
// a copy, with a value at every position: the filter answers its source's
// tuples. It answers false when the failure is the source's or there is none.
func (it *PredicateFilterIterator) failedTuple() (Tuple, []int, bool) {
	if it.failed == nil {
		return nil, nil, false
	}
	return copyTuple(it.failed), nil, true
}

// resume drops the iterator's own failure, and Next reads on from the next
// source tuple.
func (it *PredicateFilterIterator) resume() {
	it.err = nil
	it.failed = nil
}

// admitExpressionResult resolves a function result entering relational flow.
// It consumes the get-some absence sentinel — found=false means the function
// produced no binding, and the caller drops the affected tuples per its own
// semantics; the sentinel itself never escapes this boundary — and admits a
// found get-some as Datomic's pair, the attribute it found, as its keyword,
// and that attribute's value, which the tuple form [?attr ?val] destructures.
// It validates the resolved value against the datalog domain. Expression
// evaluation is the one producer of NaN inside the engine — arithmetic over
// Inf operands (Inf - Inf, 0 * Inf, Inf / Inf), where Inf is itself a value
// and reachable from finite data by overflow — so NaN fails loudly here
// rather than entering joins and sorts. The write and input boundaries
// exclude it everywhere else.
func admitExpressionResult(fn query.Function, result interface{}) (interface{}, bool, error) {
	if gsr, ok := result.(*query.GetSomeResult); ok {
		if !gsr.Found {
			return nil, false, nil
		}
		return []interface{}{gsr.Attr, gsr.Value}, true, nil
	}
	if f, ok := result.(float64); ok && math.IsNaN(f) {
		return nil, false, fmt.Errorf("expression %v produced NaN, which is not a datalog value", fn)
	}
	return result, true, nil
}

// FunctionEvaluatorIterator answers its source's tuples each joined with what
// an expression's binding reads from the expression's value on that tuple. A
// binding symbol the source lacks extends the tuple, one it carries unifies,
// and a collection or relation binding answers a tuple per element it reads;
// an expression with no binding passes the tuple through. It evaluates as it
// is pulled, so an evaluation failure surfaces from Error when the iteration
// that reached it ends, and a tuple nothing pulls is never evaluated.
type FunctionEvaluatorIterator struct {
	source  Iterator
	expr    *query.Expression
	lookup  query.EntityLookup // the lookup a database function evaluates through
	symbols []query.Symbol     // the source's symbols
	align   bindingAlignment
	bound   bool // the expression binds a symbol
	// expands is true for the collection and relation forms, which read any
	// number of tuples from a value; the scalar and tuple forms read one.
	expands bool
	// invariant is true when the expression reads no symbol of the source, so
	// it has one value over the source, and evaluated once that value is in
	// hand: found, whether the function found one, and values, the binding's
	// tuples read from it.
	invariant  bool
	evaluated  bool
	found      bool
	values     []Tuple
	envSymbols []query.Symbol
	envTuple   Tuple
	bindings   map[query.Symbol]interface{}
	input      Tuple   // the source tuple being joined
	pending    []Tuple // the binding's tuples for input not yet joined
	current    Tuple
	err        error // First eval error; replayed via Error() at iteration end
	failed     Tuple // the source tuple err arose on, nil when err is the source's
}

// NewFunctionEvaluatorIterator creates an iterator evaluating expr over
// source, whose tuples carry symbols. lookup is the lookup a database function
// evaluates through, and env is the scope's environment relation, whose one
// tuple binds into every evaluation; either may be nil.
func NewFunctionEvaluatorIterator(source Iterator, symbols []query.Symbol, expr *query.Expression, lookup query.EntityLookup, env Relation) *FunctionEvaluatorIterator {
	envSymbols, envTuple := environmentTuple(env)
	bindingSymbols := expr.BoundVariables()
	var expands bool
	switch expr.Binding.(type) {
	case query.CollectionBinding, query.RelationBinding:
		expands = true
	}
	invariant := true
	for _, sym := range expr.Function.RequiredSymbols() {
		if query.ContainsSymbol(symbols, sym) {
			invariant = false
			break
		}
	}
	return &FunctionEvaluatorIterator{
		source:     source,
		expr:       expr,
		lookup:     lookup,
		symbols:    symbols,
		align:      alignBinding(symbols, bindingSymbols),
		bound:      len(bindingSymbols) > 0,
		expands:    expands,
		invariant:  invariant,
		envSymbols: envSymbols,
		envTuple:   envTuple,
		bindings:   make(map[query.Symbol]interface{}, len(symbols)+len(envSymbols)),
	}
}

// Symbols returns the symbols of the tuples the iterator answers: the
// source's, then each binding symbol that extends them.
func (it *FunctionEvaluatorIterator) Symbols() []query.Symbol {
	return it.align.symbols
}

// Next advances to the next joined tuple, evaluating the expression on each
// source tuple it reads. Fail-fast on eval errors: the error is stored so
// Error() returns it after Next() reports exhaustion.
func (it *FunctionEvaluatorIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		for len(it.pending) > 0 {
			boundTuple := it.pending[0]
			it.pending = it.pending[1:]
			if out, ok := it.align.apply(it.input, boundTuple); ok {
				it.current = out
				return true
			}
		}
		if !it.source.Next() {
			return false
		}
		it.input = it.source.Tuple()
		if !it.evaluated {
			value, found, err := it.evaluate(it.input)
			if err != nil {
				it.err = err
				it.failed = it.input
				return false
			}
			it.found, it.values = found, nil
			if found && it.bound {
				if it.values, err = bindingTuples(it.expr.Binding, value); err != nil {
					it.err = err
					it.failed = it.input
					return false
				}
			}
			it.evaluated = it.invariant
		}
		// Absence (get-some found no attribute) is a soft no-match: skip the
		// tuple.
		if !it.found {
			continue
		}
		if !it.bound {
			it.current = it.input
			return true
		}
		it.pending = it.values
	}
}

// evaluate evaluates the expression on tuple, with the environment's tuple
// bound beside it: its value, and whether the function found one.
func (it *FunctionEvaluatorIterator) evaluate(tuple Tuple) (interface{}, bool, error) {
	for k := range it.bindings {
		delete(it.bindings, k)
	}
	// A relation symbol of an environment symbol's name overwrites it: the
	// relation's data wins locally.
	for i := range it.envSymbols {
		it.bindings[it.envSymbols[i]] = it.envTuple[i]
	}
	bindTuple(it.bindings, it.symbols, tuple)
	var result interface{}
	var err error
	if dbFunc, ok := it.expr.Function.(query.DatabaseFunction); ok && it.lookup != nil {
		result, err = dbFunc.EvalWithLookup(it.bindings, it.lookup)
	} else {
		result, err = it.expr.Function.Eval(it.bindings)
	}
	if err != nil {
		return nil, false, err
	}
	return admitExpressionResult(it.expr.Function, result)
}

// Tuple returns the current joined tuple.
func (it *FunctionEvaluatorIterator) Tuple() Tuple {
	return it.current
}

// RequiresCopy is the source's when the iterator answers the source's tuple
// itself, which it does when no binding symbol extends it, and false when a
// binding symbol extends it and the iterator answers a fresh tuple.
func (it *FunctionEvaluatorIterator) RequiresCopy() bool {
	return !it.align.extendsTuple() && it.source.RequiresCopy()
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

// failedTuple answers the source tuple the iterator's own failure arose on,
// over the iterator's symbols: the source's values, then no value at each
// binding symbol that extends them, the positions it answers second. It
// answers false when the failure is the source's or there is none.
func (it *FunctionEvaluatorIterator) failedTuple() (Tuple, []int, bool) {
	if it.failed == nil {
		return nil, nil, false
	}
	tuple := make(Tuple, len(it.align.symbols))
	copy(tuple, it.failed)
	unknown := make([]int, 0, len(tuple)-len(it.failed))
	for pos := len(it.failed); pos < len(tuple); pos++ {
		unknown = append(unknown, pos)
	}
	return tuple, unknown, true
}

// resume drops the iterator's own failure, and Next reads on from the next
// source tuple.
func (it *FunctionEvaluatorIterator) resume() {
	it.err = nil
	it.failed = nil
}

// DedupIterator answers each distinct tuple of its source once, by whole
// tuple equality. Each admitted tuple is held in the set under its
// whole-tuple key (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T4).
type DedupIterator struct {
	source     Iterator
	seen       *TupleSet // built at the first tuple, whose width is the key's
	hint       int
	current    Tuple
	copyTuples bool
}

// NewDedupIterator creates an iterator that removes duplicates, its set sized
// at its first tuple for expectedSize tuples when that is above zero. The set
// retains each admitted tuple by reference, so copyTuples must be true when
// the source iterator reuses workspace memory across Next() calls (storage
// scan iterators); it may be false only when the source yields a fresh tuple
// per call (ProjectIterator and other composing transforms).
func NewDedupIterator(source Iterator, expectedSize int, copyTuples bool) *DedupIterator {
	return &DedupIterator{
		source:     source,
		hint:       expectedSize,
		copyTuples: copyTuples,
	}
}

// Next advances to the next unique tuple. A tuple is probed as the source
// hands it, so a duplicate is rejected without a copy, and an admitted tuple
// is copied once, on admission, from a reusing source.
func (it *DedupIterator) Next() bool {
	for it.source.Next() {
		tuple := it.source.Tuple()
		if it.seen == nil {
			positions := allPositions(len(tuple))
			it.seen = NewTupleSet(positions, positions, it.hint)
		}
		_, hash, held := it.seen.Find(tuple)
		if held {
			continue
		}
		if it.copyTuples {
			tuple = copyTuple(tuple)
		}
		it.seen.Add(tuple, hash)
		it.current = tuple
		return true
	}
	return false
}

// Tuple returns the current tuple
func (it *DedupIterator) Tuple() Tuple {
	return it.current
}

// RequiresCopy is false when the iterator copies each admitted tuple, and
// the source's otherwise.
func (it *DedupIterator) RequiresCopy() bool {
	return !it.copyTuples && it.source.RequiresCopy()
}

// Close releases any resources
func (it *DedupIterator) Close() error {
	return it.source.Close()
}

func (it *DedupIterator) Error() error { return it.source.Error() }
