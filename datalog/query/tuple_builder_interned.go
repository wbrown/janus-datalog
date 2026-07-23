package query

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
)

// InternedTupleBuilder uses interning to minimize allocations
type InternedTupleBuilder struct {
	symbols []Symbol

	// Pre-computed indexes for each position (-1 means not captured)
	eIndex int
	aIndex int
	vIndex int
	tIndex int

	// Number of variables actually captured
	numVars int

	// Reusable workspace for building tuples
	workspace Tuple

	// Cache for common ElementID values (transaction IDs)
	txCache map[datalog.ElementID]*datalog.ElementID
	txMu    sync.Mutex
}

// NewInternedTupleBuilder creates a tuple builder that uses interning
func NewInternedTupleBuilder(pattern *DataPattern, symbols []Symbol) *InternedTupleBuilder {
	// Use shared indexer to compute indices
	indexer := NewTupleIndexer(pattern, symbols)

	return &InternedTupleBuilder{
		symbols:   symbols,
		eIndex:    indexer.EIndex,
		aIndex:    indexer.AIndex,
		vIndex:    indexer.VIndex,
		tIndex:    indexer.TIndex,
		numVars:   indexer.NumVars,
		workspace: make(Tuple, len(symbols)),
		txCache:   make(map[datalog.ElementID]*datalog.ElementID),
	}
}

// getTxPtr returns a cached pointer to an ElementID value
func (tb *InternedTupleBuilder) getTxPtr(tx datalog.ElementID) *datalog.ElementID {
	tb.txMu.Lock()
	if ptr, found := tb.txCache[tx]; found {
		tb.txMu.Unlock()
		return ptr
	}
	// Create new pointer and cache it
	ptr := new(datalog.ElementID)
	*ptr = tx
	tb.txCache[tx] = ptr
	tb.txMu.Unlock()
	return ptr
}

// BuildTupleInterned builds a tuple using interned values to minimize allocations
func (tb *InternedTupleBuilder) BuildTupleInterned(datom *datalog.Datom) Tuple {
	result := make(Tuple, len(tb.symbols))

	// Datoms decoded from storage carry interned identities and keywords
	// (datom_decoder.go constructs E via InternIdentityFromHash and A via
	// InternKeywordFromBytes), so positions copy the canonical pointers.
	if tb.eIndex >= 0 {
		result[tb.eIndex] = datom.E
	}
	if tb.aIndex >= 0 {
		// Keywords are interned at construction; datom.A is already canonical
		result[tb.aIndex] = datom.A
	}
	if tb.vIndex >= 0 {
		// Identity values decode through NewIdentityFromHash (interned);
		// other value types are not interned. Copy as-is either way.
		result[tb.vIndex] = datom.V
	}
	if tb.tIndex >= 0 {
		// Use cached pointer for common transaction IDs
		result[tb.tIndex] = tb.getTxPtr(datom.Tx)
	}

	return result
}

// BuildTupleInternedInto fills a pre-allocated tuple with interned values
func (tb *InternedTupleBuilder) BuildTupleInternedInto(datom *datalog.Datom, tuple Tuple) {
	// Datoms decoded from storage carry interned identities and keywords
	// (datom_decoder.go constructs E via InternIdentityFromHash and A via
	// InternKeywordFromBytes), so positions copy the canonical pointers.
	if tb.eIndex >= 0 {
		tuple[tb.eIndex] = datom.E
	}
	if tb.aIndex >= 0 {
		// Keywords are interned at construction; datom.A is already canonical
		tuple[tb.aIndex] = datom.A
	}
	if tb.vIndex >= 0 {
		// Identity values decode through NewIdentityFromHash (interned);
		// other value types are not interned. Copy as-is either way.
		tuple[tb.vIndex] = datom.V
	}
	if tb.tIndex >= 0 {
		// Use cached pointer for common transaction IDs
		tuple[tb.tIndex] = tb.getTxPtr(datom.Tx)
	}
}
