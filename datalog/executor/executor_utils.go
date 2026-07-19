package executor

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// emptyRelationForQuery returns an empty MaterializedRelation with the correct
// symbols for the given query's :find clause, plus any retained :order-by
// symbols: every path that can reach the finalization sort must produce the
// same shape, or sorting an empty result would error on a missing sort symbol.
// This ensures Query never returns nil — callers always get a valid (possibly
// empty) Relation.
func emptyRelationForQuery(q *query.Query) Relation {
	symbols := extractFindSymbols(q.Find)
	symbols = append(symbols, query.RetainedSortSymbols(q)...)
	return NewMaterializedRelation(symbols, nil)
}

func extractFindSymbols(findElements []query.FindElement) []query.Symbol {
	var symbols []query.Symbol
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			symbols = append(symbols, e.Symbol)
		case query.FindAggregate:
			symbols = append(symbols, datalog.NewSymbol(e.String()))
		case query.FindPull:
			symbols = append(symbols, e.Variable)
		}
	}
	return symbols
}

// MaterializeResult converts a streaming relation to a materialized result with the specified symbols.
// This is a pure function that collects all tuples from the iterator into memory.
func MaterializeResult(rel Relation, symbols []query.Symbol) Relation {
	var tuples []Tuple
	err := collectTuplesInto(&tuples, rel)

	// Note: an earlier debug assertion here panicked with
	// "BUG DETECTED" when the first and last tuples were
	// interface-equal on every element. It was intended to catch a
	// historical iterator-workspace-reuse bug. Under the current
	// interning and BufferedIterator invariants, the original bug is
	// fixed; the assertion became a false-positive trap for
	// legitimate queries whose results happened to contain repeated
	// interned values (two entities with the same name, for example).
	// Removed in the EXTERNAL_REVIEW_2026_04 item-5 fix pass.

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	mat := NewMaterializedRelationWithOptions(symbols, tuples, opts)
	if err != nil {
		// Carry a deferred source error so it isn't laundered by materialization.
		mat.err = err
	}
	return mat
}

// Result is deprecated - use Relation instead.
// This type alias exists for backward compatibility.
type Result = MaterializedRelation

// SortRelation sorts a relation according to the order-by clauses.
// This is a pure function that performs multi-symbol sorting with configurable direction.
// It materializes the relation if not already materialized.
//
// Every sort variable must be a symbol of the relation; one that is not
// resolves to a deferred error, never a silent skip — a stated ordering the
// engine cannot honor must not report success (see
// docs/bugs/resolved/BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md). Parsed
// queries cannot reach the error: the parser validates sort keys and the
// planner retains them through the final projection. It guards
// API-constructed queries.
func SortRelation(rel Relation, orderBy []query.OrderByClause) Relation {
	// Materialize if not already materialized
	var tuples []Tuple
	err := collectTuplesInto(&tuples, rel)

	// Get symbol indices for sort variables
	symbols := rel.Symbols()
	sortIndices, indexErr := orderBySymbolIndices(symbols, orderBy)
	if err == nil {
		err = indexErr
	}

	// Sort tuples (skipped when erroring — an arbitrary partial order must
	// not masquerade as the requested one)
	if err == nil {
		sort.Slice(tuples, func(i, j int) bool {
			return compareTuplesByOrder(tuples[i], tuples[j], orderBy, sortIndices) < 0
		})
	}

	opts := rel.Options()
	properties := rel.Properties()
	properties.Ordering = append([]query.OrderByClause(nil), orderBy...)
	mat := newMaterializedRelationFromSet(symbols, tuples, opts, properties)
	if err != nil {
		// Carry a deferred source error so it isn't laundered by materialization.
		mat.err = err
	}
	return mat
}

func orderBySymbolIndices(symbols []query.Symbol, orderBy []query.OrderByClause) ([]int, error) {
	indices := make([]int, len(orderBy))
	for i, clause := range orderBy {
		indices[i] = -1
		for j, sym := range symbols {
			if sym == clause.Variable {
				indices[i] = j
				break
			}
		}
		if indices[i] < 0 {
			return indices, fmt.Errorf("order-by variable %s is not a symbol of the relation (symbols: %v)",
				clause.Variable, symbols)
		}
	}
	return indices, nil
}

// compareTuplesByOrder returns -1 when left precedes right, 1 when left follows
// right, and 0 when all requested sort keys compare equal.
func compareTuplesByOrder(left, right Tuple, orderBy []query.OrderByClause, indices []int) int {
	for i, clause := range orderBy {
		cmp := datalog.CompareValues(left[indices[i]], right[indices[i]])
		if clause.Direction == query.OrderDesc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// BindQueryInputs binds input relations to a query's :in clause specifications.
// This processes the query's input specifications (ScalarInput, TupleInput, RelationInput, etc.)
// and creates a unified relation containing all bound input variables.
// This is a pure function that bridges the gap between query syntax and runtime values.
func BindQueryInputs(q *query.Query, inputRelations []Relation) Relation {
	// If no input relations, return empty relation
	if len(inputRelations) == 0 {
		return NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{{}}, ExecutorOptions{})
	}

	// Process :in clause to bind relations
	var boundRelations []Relation
	relationIndex := 0

	for _, input := range q.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Skip database input
			continue

		case query.ScalarInput:
			// Single value input - expect a relation with one symbol and one tuple
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				capacity := rel.Size()
				if capacity < 0 {
					capacity = 0
				}
				tuples := make([]Tuple, 0, capacity)
				it := rel.Iterator()
				for it.Next() {
					tuple := it.Tuple()
					if len(tuple) > 0 {
						tuples = append(tuples, Tuple{tuple[0]})
					}
				}
				iterErr := it.Error()
				if closeErr := it.Close(); iterErr == nil {
					iterErr = closeErr
				}
				bound := NewMaterializedRelationWithOptions(
					[]query.Symbol{inp.Symbol},
					tuples,
					rel.Options(),
				)
				bound.err = iterErr
				boundRelations = append(boundRelations, bound)
				relationIndex++
			}

		case query.RelationInput:
			// Multiple tuples input - use the relation directly with renamed symbols
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if len(inp.Symbols) == len(rel.Symbols()) {
					// Create a new relation with the input variables as symbol names
					capacity := rel.Size()
					if capacity < 0 {
						capacity = 0
					}
					tuples := make([]Tuple, 0, capacity)
					err := collectTuplesInto(&tuples, rel)

					opts := rel.Options()
					bound := NewMaterializedRelationWithOptions(inp.Symbols, tuples, opts)
					if err != nil {
						bound.err = err
					}
					boundRelations = append(boundRelations, bound)
				}
				relationIndex++
			}

		case query.TupleInput:
			// Single tuple input - expect a relation with one tuple
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if len(inp.Symbols) == len(rel.Symbols()) {
					var sourceTuples []Tuple
					iterErr := collectTuplesInto(&sourceTuples, rel)
					var tuples []Tuple
					if len(sourceTuples) > 0 {
						tuples = []Tuple{sourceTuples[0]}
					}
					bound := NewMaterializedRelationWithOptions(inp.Symbols, tuples, rel.Options())
					bound.err = iterErr
					boundRelations = append(boundRelations, bound)
				}
				relationIndex++
			}

		case query.CollectionInput:
			// Collection input - all values in one symbol
			// IMPORTANT: Always add the collection relation, even if empty.
			// An empty collection should produce 0 results when joined.
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				symbols := []query.Symbol{inp.Symbol}
				capacity := rel.Size()
				if capacity < 0 {
					capacity = 0
				}
				tuples := make([]Tuple, 0, capacity)

				it := rel.Iterator()
				for it.Next() {
					tuple := it.Tuple()
					if len(tuple) > 0 {
						// Take first value from each tuple
						tuples = append(tuples, Tuple{tuple[0]})
					}
				}
				iterErr := it.Error()
				if closeErr := it.Close(); iterErr == nil {
					iterErr = closeErr
				}

				opts := rel.Options()
				bound := NewMaterializedRelationWithOptions(symbols, tuples, opts)
				bound.err = iterErr
				boundRelations = append(boundRelations, bound)
				relationIndex++
			}
		}
	}

	// Join all bound relations to create the input context
	if len(boundRelations) == 0 {
		return NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{{}}, ExecutorOptions{})
	}

	result := boundRelations[0]
	for i := 1; i < len(boundRelations); i++ {
		result = result.Join(boundRelations[i])
	}

	return result
}
