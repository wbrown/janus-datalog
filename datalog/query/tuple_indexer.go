package query

// TupleIndexer computes and stores the mapping from pattern variables to tuple positions.
// This logic was duplicated across TupleBuilder, InternedTupleBuilder, and OptimizedTupleBuilder.
type TupleIndexer struct {
	// Pre-computed indexes for each position (-1 means not captured)
	EIndex int
	AIndex int
	VIndex int
	TIndex int

	// Number of variables actually captured
	NumVars int

	// Symbol index map for quick lookups
	SymIndex map[Symbol]int
}

// NewTupleIndexer computes the index mapping for a pattern and symbols.
// This consolidates the constructor logic that was duplicated ~240 lines across three builders.
func NewTupleIndexer(pattern *DataPattern, symbols []Symbol) *TupleIndexer {
	indexer := &TupleIndexer{
		EIndex:   -1,
		AIndex:   -1,
		VIndex:   -1,
		TIndex:   -1,
		NumVars:  0,
		SymIndex: make(map[Symbol]int, len(symbols)),
	}

	// Build symbol index map for quick lookups
	for i, sym := range symbols {
		indexer.SymIndex[sym] = i
	}

	// Check E position
	if v, ok := pattern.GetE().(Variable); ok {
		if idx, found := indexer.SymIndex[v.Name]; found {
			indexer.EIndex = idx
			indexer.NumVars++
		}
	}

	// Check A position
	if v, ok := pattern.GetA().(Variable); ok {
		if idx, found := indexer.SymIndex[v.Name]; found {
			indexer.AIndex = idx
			indexer.NumVars++
		}
	}

	// Check V position
	if v, ok := pattern.GetV().(Variable); ok {
		if idx, found := indexer.SymIndex[v.Name]; found {
			indexer.VIndex = idx
			indexer.NumVars++
		}
	}

	// Check T position
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(Variable); ok {
			if idx, found := indexer.SymIndex[v.Name]; found {
				indexer.TIndex = idx
				indexer.NumVars++
			}
		}
	}

	return indexer
}
