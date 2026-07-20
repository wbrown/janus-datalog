package executor

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// PrependedRelation wraps a relation with a pre-peeked first tuple.
// This allows checking if a relation has results without losing the first tuple.
type PrependedRelation struct {
	symbols      []query.Symbol
	firstTuple   Tuple
	restRelation Relation
	options      ExecutorOptions
	iterStarted  bool
}

// NewPrependedRelation creates a streaming relation that yields firstTuple first,
// then continues with the rest of the relation.
func NewPrependedRelation(symbols []query.Symbol, firstTuple Tuple, restRelation Relation, options ExecutorOptions) *PrependedRelation {
	return &PrependedRelation{
		symbols:      symbols,
		firstTuple:   firstTuple,
		restRelation: restRelation,
		options:      options,
	}
}

func (r *PrependedRelation) Iterator() Iterator {
	if r.iterStarted {
		// Already consumed - materialize for re-iteration
		return r.Materialize().Iterator()
	}
	r.iterStarted = true

	return &PrependedIterator{
		firstTuple:   r.firstTuple,
		restIter:     r.restRelation.Iterator(),
		restRelation: r.restRelation,
	}
}

func (r *PrependedRelation) Symbols() []query.Symbol {
	return r.symbols
}

func (r *PrependedRelation) Properties() RelationProperties {
	return r.restRelation.Properties()
}

func (r *PrependedRelation) Size() int {
	return -1 // Streaming
}

func (r *PrependedRelation) IsEmpty() bool {
	return false // We know we have at least firstTuple
}

func (r *PrependedRelation) Get(i int) Tuple {
	return nil // Not supported for streaming
}

func (r *PrependedRelation) String() string {
	var symStrs []string
	for _, sym := range r.symbols {
		symStrs = append(symStrs, sym.String())
	}
	return fmt.Sprintf("PrependedRelation([%s], streaming)", strings.Join(symStrs, " "))
}

func (r *PrependedRelation) Table() string {
	return r.Materialize().Table()
}

func (r *PrependedRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.Materialize().ProjectFromPattern(pattern)
}

func (r *PrependedRelation) Sorted() ([]Tuple, error) {
	return r.Materialize().Sorted()
}

func (r *PrependedRelation) Project(symbols []query.Symbol) (Relation, error) {
	return r.Materialize().Project(symbols)
}

func (r *PrependedRelation) Materialize() Relation {
	tuples := []Tuple{r.firstTuple}

	needsCopy := r.restRelation.RequiresCopy()
	it := r.restRelation.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		if needsCopy {
			tuple = copyTuple(tuple)
		}
		tuples = append(tuples, tuple)
	}
	scanErr := it.Error()
	if closeErr := it.Close(); scanErr == nil {
		scanErr = closeErr
	}

	// A failed rest-relation scan must not materialize as a clean result —
	// carry it as the materialization's deferred error.
	result := newMaterializedRelationFromSet(r.symbols, tuples, r.options, r.Properties())
	result.err = scanErr
	return result
}

func (r *PrependedRelation) Sort(orderBy []query.OrderByClause) Relation {
	return r.Materialize().Sort(orderBy)
}

func (r *PrependedRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.Materialize().FilterWithPredicate(pred)
}

func (r *PrependedRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	return r.Materialize().EvaluateFunction(fn, outputSymbol)
}

func (r *PrependedRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

func (r *PrependedRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		return crossProduct(r, other)
	}
	return r.HashJoin(other, common)
}

func (r *PrependedRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoin(r, other, joinSyms)
}

func (r *PrependedRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return SemiJoin(r, other, joinSyms)
}

func (r *PrependedRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return AntiJoin(r, other, joinSyms)
}

func (r *PrependedRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

func (r *PrependedRelation) Options() ExecutorOptions {
	return r.options
}

// RequiresCopy returns false because PrependedIterator copies tuples from
// sources that have RequiresCopy() = true at the boundary.
func (r *PrependedRelation) RequiresCopy() bool {
	return false
}

// PrependedIterator yields a pre-peeked tuple first, then continues with the rest.
type PrependedIterator struct {
	firstTuple    Tuple
	restIter      Iterator
	restRelation  Relation // For RequiresCopy check
	returnedFirst bool
	currentTuple  Tuple
	done          bool
}

func (it *PrependedIterator) Next() bool {
	if it.done {
		return false
	}

	if !it.returnedFirst {
		it.returnedFirst = true
		it.currentTuple = it.firstTuple
		return true
	}

	if it.restIter != nil && it.restIter.Next() {
		tuple := it.restIter.Tuple()

		// Copy if rest relation requires it
		if it.restRelation.RequiresCopy() {
			tuple = copyTuple(tuple)
		}

		it.currentTuple = tuple
		return true
	}

	it.done = true
	return false
}

func (it *PrependedIterator) Tuple() Tuple {
	return it.currentTuple
}

func (it *PrependedIterator) Close() error {
	if it.restIter != nil {
		it.restIter.Close()
	}
	it.done = true
	return nil
}

func (it *PrependedIterator) Error() error {
	if it.restIter != nil {
		return it.restIter.Error()
	}
	return nil
}
