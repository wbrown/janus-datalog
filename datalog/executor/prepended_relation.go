package executor

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// PrependedRelation wraps an iterator with a pre-peeked first tuple.
// This allows checking if an iterator has results without losing the first tuple.
type PrependedRelation struct {
	columns     []query.Symbol
	firstTuple  Tuple
	restIter    Iterator
	options     ExecutorOptions
	iterStarted bool
}

// NewPrependedRelation creates a streaming relation that yields firstTuple first,
// then continues with the rest of the iterator.
func NewPrependedRelation(columns []query.Symbol, firstTuple Tuple, restIter Iterator, options ExecutorOptions) *PrependedRelation {
	return &PrependedRelation{
		columns:    columns,
		firstTuple: firstTuple,
		restIter:   restIter,
		options:    options,
	}
}

func (r *PrependedRelation) Iterator() Iterator {
	if r.iterStarted {
		// Already consumed - materialize for re-iteration
		return r.Materialize().Iterator()
	}
	r.iterStarted = true
	return &PrependedIterator{
		firstTuple:    r.firstTuple,
		restIter:      r.restIter,
		returnedFirst: false,
	}
}

func (r *PrependedRelation) Columns() []query.Symbol {
	return r.columns
}

func (r *PrependedRelation) Symbols() []query.Symbol {
	return r.columns
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
	var symbols []string
	for _, col := range r.columns {
		symbols = append(symbols, string(col))
	}
	return fmt.Sprintf("PrependedRelation([%s], streaming)", strings.Join(symbols, " "))
}

func (r *PrependedRelation) Table() string {
	return r.Materialize().Table()
}

func (r *PrependedRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.Materialize().ProjectFromPattern(pattern)
}

func (r *PrependedRelation) Sorted() []Tuple {
	return r.Materialize().Sorted()
}

func (r *PrependedRelation) Project(columns []query.Symbol) (Relation, error) {
	return r.Materialize().Project(columns)
}

func (r *PrependedRelation) Materialize() Relation {
	tuples := []Tuple{r.firstTuple}
	if r.restIter != nil {
		for r.restIter.Next() {
			tuples = append(tuples, r.restIter.Tuple())
		}
		r.restIter.Close()
		r.restIter = nil
	}
	return NewMaterializedRelationWithOptions(r.columns, tuples, r.options)
}

func (r *PrependedRelation) Sort(orderBy []query.OrderByClause) Relation {
	return r.Materialize().Sort(orderBy)
}

func (r *PrependedRelation) Filter(filter Filter) Relation {
	return FilterRelation(r, filter)
}

func (r *PrependedRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.Materialize().FilterWithPredicate(pred)
}

func (r *PrependedRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	return r.Materialize().EvaluateFunction(fn, outputColumn)
}

func (r *PrependedRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

func (r *PrependedRelation) Join(other Relation) Relation {
	common := CommonColumns(r, other)
	if len(common) == 0 {
		return crossProduct(r, other)
	}
	return r.HashJoin(other, common)
}

func (r *PrependedRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return HashJoin(r, other, joinCols)
}

func (r *PrependedRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	return SemiJoin(r, other, joinCols)
}

func (r *PrependedRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	return AntiJoin(r, other, joinCols)
}

func (r *PrependedRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

func (r *PrependedRelation) Options() ExecutorOptions {
	return r.options
}

// PrependedIterator yields a pre-peeked tuple first, then continues with the rest.
type PrependedIterator struct {
	firstTuple    Tuple
	restIter      Iterator
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
		it.currentTuple = it.restIter.Tuple()
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
