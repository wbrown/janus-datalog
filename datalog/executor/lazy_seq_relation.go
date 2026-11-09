package executor

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// LazySeqRelation implements Relation backed by a shared LazySeq.
// Multiple Iterator() calls create independent cursors over the same
// shared cells — the first consumer realizes cells by advancing the
// underlying storage iterator, subsequent consumers read cached cells.
//
// shared marks a sequence the scan registry serves to several readers. A
// cursor over it never closes its source; the registry does.
type LazySeqRelation struct {
	seq        *LazySeq
	symbols    []query.Symbol
	options    ExecutorOptions
	properties RelationProperties
	shared     bool
}

// NewLazySeqRelation creates a Relation backed by a LazySeq with the
// given symbols. The seq may be shared across multiple relations (with
// different symbols for symbol remapping).
func NewLazySeqRelation(seq *LazySeq, symbols []query.Symbol) *LazySeqRelation {
	return &LazySeqRelation{seq: seq, symbols: symbols}
}

// WrapStreamingAsLazy wraps a StreamingRelation's iterator in a LazySeqRelation
// so it can be safely re-iterated. Non-streaming relations are returned as-is.
// Use this when a streaming result will participate in a join or collapse that
// may call Iterator() multiple times.
func WrapStreamingAsLazy(rel Relation) Relation {
	sr, ok := rel.(*StreamingRelation)
	if !ok {
		return rel
	}
	seq := NewTupleSeq(sr.Iterator(), sr.RequiresCopy())
	return &LazySeqRelation{
		seq:        seq,
		symbols:    sr.Symbols(),
		options:    sr.Options(),
		properties: sr.Properties(),
	}
}

func (r *LazySeqRelation) Symbols() []query.Symbol { return r.symbols }
func (r *LazySeqRelation) Properties() RelationProperties {
	return r.properties
}

// Rename is the relation under the substitution, over the same shared seq.
func (r *LazySeqRelation) Rename(subst query.Substitution) (Relation, error) {
	symbols, err := subst.Rename(r.symbols)
	if err != nil {
		return nil, err
	}
	return &LazySeqRelation{
		seq:        r.seq,
		symbols:    symbols,
		options:    r.options,
		properties: r.properties.renameSymbols(subst),
		shared:     r.shared,
	}, nil
}
func (r *LazySeqRelation) Size() int                { return -1 } // streaming
func (r *LazySeqRelation) RequiresCopy() bool       { return false }
func (r *LazySeqRelation) Options() ExecutorOptions { return r.options }

func (r *LazySeqRelation) Get(i int) Tuple {
	if i < 0 {
		return nil
	}
	it := r.Iterator()
	for position := 0; it.Next(); position++ {
		if position == i {
			return it.Tuple()
		}
	}
	return nil
}

// Iterator returns a new cursor over the shared LazySeq.
// Each call creates an independent cursor starting from the head.
func (r *LazySeqRelation) Iterator() Iterator {
	return &lazySeqIterator{cur: r.seq, shared: r.shared}
}

func (r *LazySeqRelation) Materialize() Relation {
	return r
}

func (r *LazySeqRelation) realizeAll() *MaterializedRelation {
	var tuples []Tuple
	err := collectTuplesInto(&tuples, r)
	result := newMaterializedRelationFromSet(r.symbols, tuples, r.options, r.properties)
	result.err = err
	return result
}

func (r *LazySeqRelation) String() string {
	var symStrs []string
	for _, s := range r.symbols {
		symStrs = append(symStrs, s.String())
	}
	return fmt.Sprintf("LazySeqRelation([%s])", strings.Join(symStrs, " "))
}

func (r *LazySeqRelation) Table() string                         { return r.realizeAll().Table() }
func (r *LazySeqRelation) Sorted() ([]Tuple, error)              { return r.realizeAll().Sorted() }
func (r *LazySeqRelation) Sort(o []query.OrderByClause) Relation { return r.realizeAll().Sort(o) }
func (r *LazySeqRelation) Select(pred func(Tuple) bool) Relation {
	return r.fromIterator(
		&selectionIterator{source: r.Iterator(), predicate: pred},
		r.symbols,
		r.properties,
	)
}

func (r *LazySeqRelation) Project(symbols []query.Symbol) (Relation, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("cannot project to zero symbols")
	}
	// The identity projection, the relation's own symbols in its own order,
	// is the relation (docs/proposals/BINDING_MATCH_PROOF.md, T1).
	if symbolsMatch(r.symbols, symbols) {
		return r, nil
	}
	for _, symbol := range symbols {
		if SymbolIndex(r, symbol) < 0 {
			return nil, fmt.Errorf("cannot project: symbol %s not found in relation (has symbols: %v)", symbol, r.symbols)
		}
	}
	properties := r.properties.project(symbols)
	var iterator Iterator = NewProjectIterator(r, r.symbols, symbols)
	if !projectionPreservesSet(r.symbols, symbols, properties) {
		iterator = NewDedupIterator(iterator, 0, false)
	}
	return r.fromIterator(iterator, symbols, properties), nil
}

func (r *LazySeqRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.realizeAll().ProjectFromPattern(pattern)
}

// FilterWithPredicate is the relation's tuples the predicate admits, a stream
// that evaluates the predicate as it is pulled. It evaluates as the relation
// does, and also when the predicate can fail on a value of the relation
// (canFailOnAValueOver).
func (r *LazySeqRelation) FilterWithPredicate(pred query.Predicate) Relation {
	properties := r.properties.clone()
	properties.Evaluates = properties.Evaluates || canFailOnAValueOver(pred, r.properties.Identities)
	return r.fromIterator(
		NewPredicateFilterIterator(r.Iterator(), r.symbols, pred, nil, nil),
		r.symbols,
		properties,
	)
}

// EvaluateFunction is the relation's tuples each extended with the
// function's value, a stream that evaluates the function as it is pulled. It
// evaluates as the relation does, and also when the function can fail on a
// value of the relation (canFailOnAValueOver).
func (r *LazySeqRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	expr := &query.Expression{Function: fn, Binding: outputSymbol}
	evalIter := NewFunctionEvaluatorIterator(r.Iterator(), r.symbols, expr, nil, nil)
	properties := r.properties.addSymbol(outputSymbol)
	properties.Evaluates = properties.Evaluates || canFailOnAValueOver(expr, r.properties.Identities)
	return r.fromIterator(
		evalIter,
		evalIter.Symbols(),
		properties,
	)
}

func (r *LazySeqRelation) fromIterator(
	iterator Iterator,
	symbols []query.Symbol,
	properties RelationProperties,
) *LazySeqRelation {
	return &LazySeqRelation{
		seq:        NewTupleSeq(iterator, true),
		symbols:    append([]query.Symbol(nil), symbols...),
		options:    r.options,
		properties: properties.clone(),
	}
}

func (r *LazySeqRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		return crossProduct(r, other)
	}
	return r.HashJoin(other, common)
}

func (r *LazySeqRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoin(r, other, joinSyms)
}

func (r *LazySeqRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return SemiJoin(r, other, joinSyms)
}

func (r *LazySeqRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return AntiJoin(r, other, joinSyms)
}

func (r *LazySeqRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

// lazySeqIterator walks a LazySeq chain, producing one Tuple per Next() call.
// The chain's end carries the source's read failure joined with its close
// failure, which Error answers once the iterator reaches it. Close closes the
// source when the iterator stopped before the end and the sequence has one
// owner, and answers that close's failure. A cursor over a shared sequence
// closes nothing: the sequence's other readers read on, and the scan registry
// closes its source.
type lazySeqIterator struct {
	cur     *LazySeq
	current Tuple
	done    bool
	err     error
	shared  bool
}

func (it *lazySeqIterator) Next() bool {
	if it.done || it.cur == nil {
		return false
	}
	if it.cur.Empty() {
		it.done = true
		return false
	}
	v, err := it.cur.First()
	if err != nil {
		// The chain's end: the source is closed and its failure is err.
		it.err = err
		it.done = true
		it.cur = nil
		return false
	}
	if v == nil {
		it.done = true
		return false
	}
	tuple, ok := v.(Tuple)
	if !ok {
		it.done = true
		return false
	}
	it.current = tuple

	rest, err := it.cur.Rest()
	if err != nil {
		// The current tuple is valid — emit it; the realization failure
		// surfaces on the next advance via Error().
		it.err = err
		it.done = true
		it.cur = nil
		return true
	}
	if rest == nil {
		it.cur = nil
	} else {
		it.cur = rest.(*LazySeq)
	}
	return true
}

func (it *lazySeqIterator) Tuple() Tuple {
	return it.current
}

// RequiresCopy is false: Tuple hands back the tuple a realized cell keeps
// for every cursor, which no pull writes.
func (it *lazySeqIterator) RequiresCopy() bool { return false }

func (it *lazySeqIterator) Close() error {
	it.done = true
	if it.cur != nil && !it.shared {
		return it.cur.Close()
	}
	it.cur = nil
	return nil
}

func (it *lazySeqIterator) Error() error { return it.err }

type selectionIterator struct {
	source    Iterator
	predicate func(Tuple) bool
	current   Tuple
}

func (it *selectionIterator) Next() bool {
	for it.source.Next() {
		tuple := it.source.Tuple()
		if it.predicate(tuple) {
			it.current = tuple
			return true
		}
	}
	return false
}

func (it *selectionIterator) Tuple() Tuple       { return it.current }
func (it *selectionIterator) Close() error       { return it.source.Close() }
func (it *selectionIterator) Error() error       { return it.source.Error() }
func (it *selectionIterator) RequiresCopy() bool { return it.source.RequiresCopy() }
