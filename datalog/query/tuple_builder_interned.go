package query

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// InternedTupleBuilder uses interning to minimize allocations
type InternedTupleBuilder struct {
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

	// Cache for common uint64 values (transaction IDs)
	txCache map[uint64]*uint64
}

// NewInternedTupleBuilder creates a tuple builder that uses interning
func NewInternedTupleBuilder(pattern *DataPattern, columns []Symbol) *InternedTupleBuilder {
	// Use shared indexer to compute indices
	indexer := NewTupleIndexer(pattern, columns)

	return &InternedTupleBuilder{
		columns:   columns,
		eIndex:    indexer.EIndex,
		aIndex:    indexer.AIndex,
		vIndex:    indexer.VIndex,
		tIndex:    indexer.TIndex,
		numVars:   indexer.NumVars,
		workspace: make(Tuple, len(columns)),
		txCache:   make(map[uint64]*uint64, 128), // Pre-allocate for common tx IDs
	}
}

// getTxPtr returns a cached pointer to a uint64 value
func (tb *InternedTupleBuilder) getTxPtr(tx uint64) *uint64 {
	if ptr, found := tb.txCache[tx]; found {
		return ptr
	}
	// Create new pointer and cache it
	ptr := new(uint64)
	*ptr = tx
	tb.txCache[tx] = ptr
	return ptr
}

// BuildTupleInterned builds a tuple using interned values to minimize allocations
func (tb *InternedTupleBuilder) BuildTupleInterned(datom *datalog.Datom) Tuple {
	result := make(Tuple, len(tb.columns))

	// Use interned values where possible - store pointers to avoid interface boxing
	if tb.eIndex >= 0 {
		// Intern the identity
		result[tb.eIndex] = datalog.InternIdentity(datom.E)
	}
	if tb.aIndex >= 0 {
		// Intern the keyword
		result[tb.aIndex] = datalog.InternKeyword(datom.A.String())
	}
	if tb.vIndex >= 0 {
		// Values are more varied, harder to intern effectively
		// But if it's an Identity, we can intern it
		if id, ok := datom.V.(datalog.Identity); ok {
			result[tb.vIndex] = datalog.InternIdentity(id)
		} else {
			result[tb.vIndex] = datom.V
		}
	}
	if tb.tIndex >= 0 {
		// Use cached pointer for common transaction IDs
		result[tb.tIndex] = tb.getTxPtr(datom.Tx)
	}

	return result
}

// BuildTupleInternedInto fills a pre-allocated tuple with interned values
func (tb *InternedTupleBuilder) BuildTupleInternedInto(datom *datalog.Datom, tuple Tuple) {
	// Use interned values where possible - store pointers to avoid interface boxing
	if tb.eIndex >= 0 {
		// Intern the identity
		tuple[tb.eIndex] = datalog.InternIdentity(datom.E)
	}
	if tb.aIndex >= 0 {
		// Intern the keyword
		tuple[tb.aIndex] = datalog.InternKeyword(datom.A.String())
	}
	if tb.vIndex >= 0 {
		// Values are more varied, harder to intern effectively
		// But if it's an Identity, we can intern it
		if id, ok := datom.V.(datalog.Identity); ok {
			tuple[tb.vIndex] = datalog.InternIdentity(id)
		} else {
			tuple[tb.vIndex] = datom.V
		}
	}
	if tb.tIndex >= 0 {
		// Use cached pointer for common transaction IDs
		tuple[tb.tIndex] = tb.getTxPtr(datom.Tx)
	}
}
