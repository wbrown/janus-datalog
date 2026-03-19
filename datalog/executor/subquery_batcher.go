package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// SubqueryBatcher builds batched input relations for subquery execution
// When a subquery accepts RelationInput (:in $ [[?sym ?hr] ...]), we can
// pass all input combinations as a single relation instead of executing
// the subquery multiple times.
type SubqueryBatcher struct{}

// NewSubqueryBatcher creates a new SubqueryBatcher
func NewSubqueryBatcher() *SubqueryBatcher {
	return &SubqueryBatcher{}
}

// BuildBatchedInput creates a single relation containing all input combinations
// This relation is passed as RelationInput to the subquery for batched execution.
//
// Parameters:
// - combinations: Map of symbol -> value for each input combination
// - inputSymbols: List of symbols to extract (may include "$")
//
// Returns: Relation with symbols matching inputSymbols (excluding "$")
func (b *SubqueryBatcher) BuildBatchedInput(
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
) Relation {
	if len(combinations) == 0 {
		// Filter source markers from symbols
		var symbols []query.Symbol
		for _, sym := range inputSymbols {
			if !sym.IsSource() {
				symbols = append(symbols, sym)
			}
		}
		return NewMaterializedRelation(symbols, []Tuple{})
	}

	// Filter to only the symbols we're passing (exclude source markers)
	var symbols []query.Symbol
	for _, sym := range inputSymbols {
		if !sym.IsSource() {
			symbols = append(symbols, sym)
		}
	}

	// Build tuples from all combinations
	var tuples []Tuple
	for _, values := range combinations {
		tuple := make(Tuple, len(symbols))
		for i, sym := range symbols {
			if val, ok := values[sym]; ok {
				tuple[i] = val
			}
			// Note: If value not found, tuple[i] remains nil
			// This is intentional for optional bindings
		}
		tuples = append(tuples, tuple)
	}

	return NewMaterializedRelation(symbols, tuples)
}

// ExtractInputSymbols extracts variable symbols from subquery inputs
// Filters out constants and the database marker ($)
//
// Parameters:
// - inputs: Subquery input specifications from :in clause
//
// Returns: List of variable symbols (excludes $ and other constants)
func (b *SubqueryBatcher) ExtractInputSymbols(inputs []query.InputSpec) []query.Symbol {
	var symbols []query.Symbol
	for _, input := range inputs {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Include source marker for filtering later
			symbols = append(symbols, inp.Name)
		case query.ScalarInput:
			// Single variable like ?sym
			symbols = append(symbols, inp.Symbol)
		case query.CollectionInput:
			// Collection like [?sym ...]
			symbols = append(symbols, inp.Symbol)
		case query.TupleInput:
			// Tuple like [[?sym ?hr]]
			symbols = append(symbols, inp.Symbols...)
		case query.RelationInput:
			// Relation like [[?sym ?hr] ...]
			symbols = append(symbols, inp.Symbols...)
		}
	}
	return symbols
}

// ExtractRelationSymbols extracts symbols from RelationInput only
// This is useful when you need to know what symbols the batched relation will have.
//
// Parameters:
// - inputs: Subquery input specifications from :in clause
//
// Returns: Symbols from RelationInput (empty if no RelationInput found)
func (b *SubqueryBatcher) ExtractRelationSymbols(inputs []query.InputSpec) []query.Symbol {
	for _, input := range inputs {
		if rel, ok := input.(query.RelationInput); ok {
			return rel.Symbols
		}
	}
	return nil
}
