package query

// PatternExtractor provides utilities for extracting bound values from patterns.
// This consolidates logic that was previously duplicated across multiple iterators.
type PatternExtractor struct {
	pattern *DataPattern
	columns []Symbol
	colMap  map[Symbol]int
}

// NewPatternExtractor creates a new pattern extractor for the given pattern and binding columns.
func NewPatternExtractor(pattern *DataPattern, columns []Symbol) *PatternExtractor {
	colMap := make(map[Symbol]int, len(columns))
	for i, col := range columns {
		colMap[col] = i
	}

	return &PatternExtractor{
		pattern: pattern,
		columns: columns,
		colMap:  colMap,
	}
}

// BoundValues contains the extracted E, A, V, T values from a pattern.
type BoundValues struct {
	E, A, V, T interface{}
}

// Extract extracts bound values from the pattern using the given binding tuple.
// For each position in the pattern (E, A, V, T):
// - If it's a Constant, returns the constant value
// - If it's a Variable, looks up the value in the binding tuple
// - If it's a Blank or variable not in binding, returns nil
func (pe *PatternExtractor) Extract(bindingTuple Tuple) BoundValues {
	return BoundValues{
		E:  pe.extractElement(pe.pattern.GetE(), bindingTuple),
		A:  pe.extractElement(pe.pattern.GetA(), bindingTuple),
		V:  pe.extractElement(pe.pattern.GetV(), bindingTuple),
		T:  pe.extractElement(pe.pattern.GetT(), bindingTuple),
	}
}

// extractElement extracts a single pattern element value.
func (pe *PatternExtractor) extractElement(elem PatternElement, bindingTuple Tuple) interface{} {
	if elem == nil {
		return nil
	}

	// Constants return their value directly
	if c, ok := elem.(Constant); ok {
		return c.Value
	}

	// Variables look up the value in the binding tuple
	if v, ok := elem.(Variable); ok {
		if idx, found := pe.colMap[v.Name]; found && idx < len(bindingTuple) {
			return bindingTuple[idx]
		}
	}

	// Blanks or variables not in binding return nil
	return nil
}

// ExtractE extracts just the E value from the pattern.
func (pe *PatternExtractor) ExtractE(bindingTuple Tuple) interface{} {
	return pe.extractElement(pe.pattern.GetE(), bindingTuple)
}

// ExtractA extracts just the A value from the pattern.
func (pe *PatternExtractor) ExtractA(bindingTuple Tuple) interface{} {
	return pe.extractElement(pe.pattern.GetA(), bindingTuple)
}

// ExtractV extracts just the V value from the pattern.
func (pe *PatternExtractor) ExtractV(bindingTuple Tuple) interface{} {
	return pe.extractElement(pe.pattern.GetV(), bindingTuple)
}

// ExtractT extracts just the T value from the pattern.
func (pe *PatternExtractor) ExtractT(bindingTuple Tuple) interface{} {
	return pe.extractElement(pe.pattern.GetT(), bindingTuple)
}

// BuildColumnIndexMap creates a map from symbols to their column indices.
// This is a utility function for code that needs to build column maps independently.
func BuildColumnIndexMap(columns []Symbol) map[Symbol]int {
	colMap := make(map[Symbol]int, len(columns))
	for i, col := range columns {
		colMap[col] = i
	}
	return colMap
}

// ExtractPatternValues is a convenience function that extracts E, A, V, T values
// from a pattern without creating a PatternExtractor.
// Use this for one-off extractions. For repeated extractions on the same pattern,
// create a PatternExtractor and reuse it.
func ExtractPatternValues(pattern *DataPattern, columns []Symbol, bindingTuple Tuple) (e, a, v, t interface{}) {
	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)
	return values.E, values.A, values.V, values.T
}
