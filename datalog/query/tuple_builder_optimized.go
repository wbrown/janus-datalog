package query

import (
	"github.com/wbrown/janus-datalog/datalog"
	"sync"
)

// NOTE: OptimizedTupleBuilder is UNUSED in production code. Only InternedTupleBuilder is used.
// This implementation exists solely for benchmark comparisons (see tuple_builder_bench_test.go).
// CANDIDATE FOR REMOVAL: Consider removing this file to reduce maintenance burden.

// TuplePool manages a pool of reusable tuples to avoid allocations
var tuplePool = &sync.Pool{
	New: func() interface{} {
		// Create slices of different sizes
		return make(Tuple, 0, 4) // Most patterns have 2-4 variables
	},
}

// GetTuple gets a tuple from the pool with the specified size
func GetTuple(size int) Tuple {
	t := tuplePool.Get().(Tuple)
	if cap(t) < size {
		// Need a bigger tuple
		tuplePool.Put(t) // Return the small one
		return make(Tuple, size)
	}
	return t[:size]
}

// PutTuple returns a tuple to the pool for reuse
func PutTuple(t Tuple) {
	if cap(t) <= 8 { // Only pool small tuples
		t = t[:0] // Reset length but keep capacity
		tuplePool.Put(t)
	}
}

// OptimizedTupleBuilder is a highly optimized tuple builder
type OptimizedTupleBuilder struct {
	columns []Symbol

	// Pre-computed indexes for each position (-1 means not captured)
	eIndex int
	aIndex int
	vIndex int
	tIndex int

	// Number of variables actually captured
	numVars int

	// Reusable workspace for building tuples
	workspace Tuple
}

// NewOptimizedTupleBuilder creates an optimized tuple builder
func NewOptimizedTupleBuilder(pattern *DataPattern, columns []Symbol) *OptimizedTupleBuilder {
	// Use shared indexer to compute indices
	indexer := NewTupleIndexer(pattern, columns)

	return &OptimizedTupleBuilder{
		columns:   columns,
		eIndex:    indexer.EIndex,
		aIndex:    indexer.AIndex,
		vIndex:    indexer.VIndex,
		tIndex:    indexer.TIndex,
		numVars:   indexer.NumVars,
		workspace: make(Tuple, len(columns)), // Pre-allocate workspace
	}
}

// BuildTupleInto fills a pre-allocated tuple with datom values
// This avoids allocation by using the provided tuple
func (tb *OptimizedTupleBuilder) BuildTupleInto(datom *datalog.Datom, tuple Tuple) {
	// Direct assignment using pre-computed indexes
	if tb.eIndex >= 0 {
		tuple[tb.eIndex] = datom.E
	}
	if tb.aIndex >= 0 {
		tuple[tb.aIndex] = datom.A
	}
	if tb.vIndex >= 0 {
		tuple[tb.vIndex] = datom.V
	}
	if tb.tIndex >= 0 {
		tuple[tb.tIndex] = datom.Tx
	}
}

// BuildTuplePooled builds a tuple using the pool to avoid allocation
func (tb *OptimizedTupleBuilder) BuildTuplePooled(datom *datalog.Datom) Tuple {
	tuple := GetTuple(len(tb.columns))
	tb.BuildTupleInto(datom, tuple)
	return tuple
}

// BuildTupleCopy builds a tuple and returns a copy
// Use this when you need to store the tuple long-term
func (tb *OptimizedTupleBuilder) BuildTupleCopy(datom *datalog.Datom) Tuple {
	// Use workspace to build, then copy
	tb.BuildTupleInto(datom, tb.workspace)

	// Make a copy for storage
	result := make(Tuple, len(tb.columns))
	copy(result, tb.workspace)
	return result
}

// EstimateResultSize estimates the number of results based on binding size
// and typical fanout for this pattern
func EstimateResultSize(bindingSize int, pattern *DataPattern) int {
	// Heuristics based on pattern type

	// If we're binding on E (entity), typically 1-10 attributes per entity
	if _, ok := pattern.GetE().(Variable); ok {
		if _, ok := pattern.GetA().(Constant); ok {
			// [?e :specific-attr ?v] - usually 1 value per entity
			return bindingSize
		}
		// [?e ?a ?v] - multiple attributes per entity
		return bindingSize * 10
	}

	// If we're binding on A (attribute)
	if _, ok := pattern.GetA().(Variable); ok {
		// Usually many entities have each attribute
		return bindingSize * 100
	}

	// If we're binding on V (value)
	if _, ok := pattern.GetV().(Variable); ok {
		if _, ok := pattern.GetA().(Constant); ok {
			// [?e :specific-attr ?v] with ?v bound - varies widely
			return bindingSize * 50
		}
		// [?e ?a ?v] with ?v bound - could match many
		return bindingSize * 100
	}

	// Default conservative estimate
	return bindingSize * 20
}
