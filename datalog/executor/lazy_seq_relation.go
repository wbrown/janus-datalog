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
type LazySeqRelation struct {
	seq     *LazySeq
	symbols []query.Symbol
	options ExecutorOptions
}

// NewLazySeqRelation creates a Relation backed by a LazySeq with the
// given symbols. The seq may be shared across multiple relations (with
// different symbols for symbol remapping).
func NewLazySeqRelation(seq *LazySeq, symbols []query.Symbol) *LazySeqRelation {
	return &LazySeqRelation{seq: seq, symbols: symbols}
}

func (r *LazySeqRelation) Symbols() []query.Symbol { return r.symbols }
func (r *LazySeqRelation) Size() int               { return -1 } // streaming
func (r *LazySeqRelation) Get(i int) Tuple          { return nil }
func (r *LazySeqRelation) RequiresCopy() bool       { return false }
func (r *LazySeqRelation) Options() ExecutorOptions  { return r.options }

func (r *LazySeqRelation) IsEmpty() bool {
	return r.seq.Empty()
}

// Iterator returns a new cursor over the shared LazySeq.
// Each call creates an independent cursor starting from the head.
func (r *LazySeqRelation) Iterator() Iterator {
	return &lazySeqIterator{cur: r.seq}
}

func (r *LazySeqRelation) Materialize() Relation {
	var tuples []Tuple
	it := r.Iterator()
	for it.Next() {
		cp := make(Tuple, len(it.Tuple()))
		copy(cp, it.Tuple())
		tuples = append(tuples, cp)
	}
	it.Close()
	return NewMaterializedRelationWithOptions(r.symbols, tuples, r.options)
}

func (r *LazySeqRelation) String() string {
	var symStrs []string
	for _, s := range r.symbols {
		symStrs = append(symStrs, s.String())
	}
	return fmt.Sprintf("LazySeqRelation([%s])", strings.Join(symStrs, " "))
}

func (r *LazySeqRelation) Table() string            { return r.Materialize().Table() }
func (r *LazySeqRelation) Sorted() []Tuple          { return r.Materialize().Sorted() }
func (r *LazySeqRelation) Sort(o []query.OrderByClause) Relation { return r.Materialize().Sort(o) }
func (r *LazySeqRelation) Filter(f Filter) Relation  { return FilterRelation(r, f) }
func (r *LazySeqRelation) Select(pred func(Tuple) bool) Relation { return Select(r, pred) }

func (r *LazySeqRelation) Project(symbols []query.Symbol) (Relation, error) {
	return r.Materialize().Project(symbols)
}

func (r *LazySeqRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.Materialize().ProjectFromPattern(pattern)
}

func (r *LazySeqRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.Materialize().FilterWithPredicate(pred)
}

func (r *LazySeqRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	return r.Materialize().EvaluateFunction(fn, outputColumn)
}

func (r *LazySeqRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		return crossProduct(r, other)
	}
	return r.HashJoin(other, common)
}

func (r *LazySeqRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return HashJoin(r, other, joinCols)
}

func (r *LazySeqRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	return SemiJoin(r, other, joinCols)
}

func (r *LazySeqRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	return AntiJoin(r, other, joinCols)
}

func (r *LazySeqRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

// lazySeqIterator walks a LazySeq chain, producing one Tuple per Next() call.
type lazySeqIterator struct {
	cur     *LazySeq
	current Tuple
	done    bool
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
	if err != nil || v == nil {
		it.done = true
		return false
	}
	tuple, ok := v.(Tuple)
	if !ok {
		it.done = true
		return false
	}
	it.current = tuple

	rest, _ := it.cur.Rest()
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

func (it *lazySeqIterator) Close() error {
	it.done = true
	return nil
}
