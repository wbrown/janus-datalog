package executor

import (
	"container/heap"
	"sort"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TopNRelation returns the first limit tuples under orderBy without
// materializing and sorting the complete relation. It still consumes the full
// source: unlike index-order pushdown, this operator reduces CPU and retained
// memory but does not reduce storage scanning.
func TopNRelation(rel Relation, orderBy []query.OrderByClause, limit int) Relation {
	if len(orderBy) == 0 {
		return NewLimitRelation(rel, limit)
	}

	symbols := rel.Symbols()
	indices, err := orderBySymbolIndices(symbols, orderBy)
	if err != nil {
		result := NewMaterializedRelationWithOptions(symbols, nil, rel.Options())
		result.err = err
		return result
	}
	if limit <= 0 {
		return NewMaterializedRelationWithOptions(symbols, nil, rel.Options())
	}
	if size := rel.Size(); size >= 0 && limit >= size {
		return rel.Sort(orderBy)
	}

	selected := &topNTupleHeap{
		orderBy: orderBy,
		indices: indices,
		tuples:  make([]Tuple, 0, limit),
	}
	heap.Init(selected)

	needsCopy := rel.RequiresCopy()
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		if selected.Len() < limit {
			if needsCopy {
				tuple = copyTuple(tuple)
			}
			heap.Push(selected, tuple)
			continue
		}

		// The heap root is the worst retained tuple. Replace it only when the
		// current tuple sorts before it.
		if compareTuplesByOrder(tuple, selected.tuples[0], orderBy, indices) < 0 {
			if needsCopy {
				tuple = copyTuple(tuple)
			}
			selected.tuples[0] = tuple
			heap.Fix(selected, 0)
		}
	}
	iterErr := it.Error()
	if closeErr := it.Close(); iterErr == nil {
		iterErr = closeErr
	}

	sort.Slice(selected.tuples, func(i, j int) bool {
		return compareTuplesByOrder(selected.tuples[i], selected.tuples[j], orderBy, indices) < 0
	})

	result := NewMaterializedRelationWithOptions(symbols, selected.tuples, rel.Options())
	result.err = iterErr
	return result
}

// topNTupleHeap keeps the worst retained tuple at index zero.
type topNTupleHeap struct {
	orderBy []query.OrderByClause
	indices []int
	tuples  []Tuple
}

func (h topNTupleHeap) Len() int { return len(h.tuples) }

func (h topNTupleHeap) Less(i, j int) bool {
	return compareTuplesByOrder(h.tuples[i], h.tuples[j], h.orderBy, h.indices) > 0
}

func (h topNTupleHeap) Swap(i, j int) {
	h.tuples[i], h.tuples[j] = h.tuples[j], h.tuples[i]
}

func (h *topNTupleHeap) Push(value interface{}) {
	h.tuples = append(h.tuples, value.(Tuple))
}

func (h *topNTupleHeap) Pop() interface{} {
	last := len(h.tuples) - 1
	value := h.tuples[last]
	h.tuples = h.tuples[:last]
	return value
}
