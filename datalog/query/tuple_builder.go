package query

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// NOTE: TupleBuilder is UNUSED in production code. Only InternedTupleBuilder is used.
// This implementation exists solely for benchmark comparisons (see tuple_builder_bench_test.go).
// CANDIDATE FOR REMOVAL: Consider removing this file to reduce maintenance burden.

// TupleBuilder is an optimized builder for converting datoms to tuples
// It pre-computes the mapping from pattern variables to tuple positions
type TupleBuilder struct {
	columns []Symbol

	// Pre-computed indexes for each position (-1 means not captured)
	eIndex int
	aIndex int
	vIndex int
	tIndex int

	// Whether each position is a variable (vs constant)
	eIsVar bool
	aIsVar bool
	vIsVar bool
	tIsVar bool
}

// NewTupleBuilder creates an optimized tuple builder for a pattern and columns
func NewTupleBuilder(pattern *DataPattern, columns []Symbol) *TupleBuilder {
	// Use shared indexer to compute indices
	indexer := NewTupleIndexer(pattern, columns)

	return &TupleBuilder{
		columns: columns,
		eIndex:  indexer.EIndex,
		aIndex:  indexer.AIndex,
		vIndex:  indexer.VIndex,
		tIndex:  indexer.TIndex,
		eIsVar:  indexer.EIndex >= 0,
		aIsVar:  indexer.AIndex >= 0,
		vIsVar:  indexer.VIndex >= 0,
		tIsVar:  indexer.TIndex >= 0,
	}
}

// BuildTuple efficiently converts a datom to a tuple using pre-computed indexes
func (tb *TupleBuilder) BuildTuple(datom *datalog.Datom) Tuple {
	// Allocate tuple once
	tuple := make(Tuple, len(tb.columns))

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

	return tuple
}

// DatomToTupleOptimized is a drop-in replacement for DatomToTuple
// For one-off conversions where creating a TupleBuilder isn't worth it
func DatomToTupleOptimized(datom datalog.Datom, pattern *DataPattern, columns []Symbol) Tuple {
	if len(columns) == 0 {
		return nil
	}

	// For small column counts, use direct approach
	if len(columns) <= 4 {
		tuple := make(Tuple, len(columns))

		// Check each pattern element and find its column index
		if v, ok := pattern.GetE().(Variable); ok {
			for i, col := range columns {
				if col == v.Name {
					tuple[i] = datom.E
					break
				}
			}
		}

		if v, ok := pattern.GetA().(Variable); ok {
			for i, col := range columns {
				if col == v.Name {
					tuple[i] = datom.A
					break
				}
			}
		}

		if v, ok := pattern.GetV().(Variable); ok {
			for i, col := range columns {
				if col == v.Name {
					tuple[i] = datom.V
					break
				}
			}
		}

		if len(pattern.Elements) > 3 {
			if v, ok := pattern.GetT().(Variable); ok {
				for i, col := range columns {
					if col == v.Name {
						tuple[i] = datom.Tx
						break
					}
				}
			}
		}

		return tuple
	}

	// For larger column counts, fall back to map approach
	// (though this shouldn't happen in practice with EAVT patterns)
	values := make(map[Symbol]interface{}, 4)

	if v, ok := pattern.GetE().(Variable); ok {
		values[v.Name] = datom.E
	}
	if v, ok := pattern.GetA().(Variable); ok {
		values[v.Name] = datom.A
	}
	if v, ok := pattern.GetV().(Variable); ok {
		values[v.Name] = datom.V
	}
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(Variable); ok {
			values[v.Name] = datom.Tx
		}
	}

	tuple := make(Tuple, len(columns))
	for i, col := range columns {
		if val, found := values[col]; found {
			tuple[i] = val
		}
	}

	return tuple
}
