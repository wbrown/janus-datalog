package executor

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// LimitRelation caps a relation to at most N tuples. It materializes lazily,
// pulling the source iterator no more than N times — so a :limit with no
// :order-by stops the underlying scan early instead of draining it. When the
// source is already materialized (e.g. after :order-by sorts), the cap is a
// cheap truncation of the first N tuples.
//
// All relational operations delegate to the bounded materialization, so the
// source iterator is consumed exactly once regardless of access pattern.
type LimitRelation struct {
	source Relation
	limit  int

	once sync.Once
	mat  *MaterializedRelation
}

// NewLimitRelation wraps source so that at most limit tuples are produced.
// A negative limit is treated as zero (empty result).
func NewLimitRelation(source Relation, limit int) *LimitRelation {
	if limit < 0 {
		limit = 0
	}
	return &LimitRelation{source: source, limit: limit}
}

// ensure materializes at most limit tuples from the source exactly once.
func (r *LimitRelation) ensure() *MaterializedRelation {
	r.once.Do(func() {
		tuples := make([]Tuple, 0, r.limit)
		var err error

		if r.limit > 0 {
			needsCopy := r.source.RequiresCopy()
			it := r.source.Iterator()
			// Pull no more than limit tuples — this is the early termination:
			// once we have N, we stop advancing the source.
			for len(tuples) < r.limit && it.Next() {
				tuple := it.Tuple()
				if needsCopy {
					tuple = copyTuple(tuple)
				}
				tuples = append(tuples, tuple)
			}
			err = it.Error()
			if cerr := it.Close(); err == nil {
				err = cerr
			}
		}

		r.mat = newMaterializedRelationFromSet(
			r.source.Symbols(),
			tuples,
			r.source.Options(),
			r.source.Properties(),
		)
		// Carry any deferred source error so it isn't laundered by materialization.
		r.mat.err = err
	})
	return r.mat
}

func (r *LimitRelation) Symbols() []query.Symbol { return r.source.Symbols() }
func (r *LimitRelation) Properties() RelationProperties {
	return r.source.Properties()
}
func (r *LimitRelation) Iterator() Iterator { return r.ensure().Iterator() }
func (r *LimitRelation) Size() int          { return r.ensure().Size() }
func (r *LimitRelation) Get(i int) Tuple    { return r.ensure().Get(i) }
func (r *LimitRelation) String() string     { return r.ensure().String() }
func (r *LimitRelation) Table() string      { return r.ensure().Table() }

func (r *LimitRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.ensure().ProjectFromPattern(pattern)
}
func (r *LimitRelation) Sorted() ([]Tuple, error) { return r.ensure().Sorted() }
func (r *LimitRelation) Project(symbols []query.Symbol) (Relation, error) {
	return r.ensure().Project(symbols)
}
func (r *LimitRelation) Materialize() Relation { return r.ensure() }
func (r *LimitRelation) Sort(orderBy []query.OrderByClause) Relation {
	return r.ensure().Sort(orderBy)
}
func (r *LimitRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.ensure().FilterWithPredicate(pred)
}
func (r *LimitRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	return r.ensure().EvaluateFunction(fn, outputSymbol)
}
func (r *LimitRelation) Select(pred func(Tuple) bool) Relation { return r.ensure().Select(pred) }
func (r *LimitRelation) Join(other Relation) Relation          { return r.ensure().Join(other) }
func (r *LimitRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return r.ensure().HashJoin(other, joinSyms)
}
func (r *LimitRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return r.ensure().SemiJoin(other, joinSyms)
}
func (r *LimitRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return r.ensure().AntiJoin(other, joinSyms)
}
func (r *LimitRelation) Aggregate(findElements []query.FindElement) Relation {
	return r.ensure().Aggregate(findElements)
}
func (r *LimitRelation) Options() ExecutorOptions { return r.source.Options() }
func (r *LimitRelation) RequiresCopy() bool       { return false }
