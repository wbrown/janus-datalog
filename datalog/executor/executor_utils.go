package executor

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// injectConditionalAggregates replaces find variables with conditional aggregates
// from the query rewriter.
func injectConditionalAggregates(findClause []query.FindElement, condAggs []planner.ConditionalAggregate) []query.FindElement {
	// Build a map from variable symbols to conditional aggregates
	varToAgg := make(map[query.Symbol]query.FindAggregate)

	for _, condAgg := range condAggs {
		// Extract variable(s) from the binding
		switch b := condAgg.Binding.(type) {
		case query.TupleBinding:
			// For tuple bindings, we expect exactly one variable for single aggregates
			if len(b.Variables) == 1 {
				varToAgg[b.Variables[0]] = condAgg.Aggregate
			}
			// If multiple variables, we'd need to handle differently (not yet implemented)
		case query.CollectionBinding:
			varToAgg[b.Variable] = condAgg.Aggregate
		case query.RelationBinding:
			// For relation bindings with single variable (rare for aggregates)
			if len(b.Variables) == 1 {
				varToAgg[b.Variables[0]] = condAgg.Aggregate
			}
		}
	}

	// Replace find variables with conditional aggregates
	result := make([]query.FindElement, len(findClause))
	for i, elem := range findClause {
		if v, ok := elem.(query.FindVariable); ok {
			if agg, exists := varToAgg[v.Symbol]; exists {
				result[i] = agg
			} else {
				result[i] = elem
			}
		} else {
			result[i] = elem
		}
	}

	return result
}

func extractFindColumns(findElements []query.FindElement) []query.Symbol {
	var columns []query.Symbol
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			columns = append(columns, e.Symbol)
		case query.FindAggregate:
			columns = append(columns, query.Symbol(e.String()))
		}
	}
	return columns
}

// MaterializeResult converts a streaming relation to a materialized result with the specified columns.
// This is a pure function that collects all tuples from the iterator into memory.
func MaterializeResult(rel Relation, columns []query.Symbol) Relation {
	tuples := []Tuple{}

	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}

	// DEBUG: Check for tuple copying bug
	if len(tuples) > 1 {
		first := tuples[0]
		last := tuples[len(tuples)-1]
		allSame := true
		for i := range first {
			if first[i] != last[i] {
				allSame = false
				break
			}
		}
		if allSame {
			panic(fmt.Sprintf("BUG DETECTED in MaterializeResult: All %d tuples identical! First=%v Last=%v", len(tuples), first, last))
		}
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	return NewMaterializedRelationWithOptions(columns, tuples, opts)
}

// Result is deprecated - use Relation instead.
// This type alias exists for backward compatibility.
type Result = MaterializedRelation

// SortRelation sorts a relation according to the order-by clauses.
// This is a pure function that performs multi-column sorting with configurable direction.
// It materializes the relation if not already materialized.
func SortRelation(rel Relation, orderBy []query.OrderByClause) Relation {
	// Materialize if not already materialized
	tuples := []Tuple{}
	it := rel.Iterator()
	defer it.Close()
	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}

	// Get column indices for sort variables
	columns := rel.Columns()
	sortIndices := make([]int, len(orderBy))
	for i, clause := range orderBy {
		idx := -1
		for j, col := range columns {
			if col == clause.Variable {
				idx = j
				break
			}
		}
		sortIndices[i] = idx
	}

	// Sort tuples
	sort.Slice(tuples, func(i, j int) bool {
		for k, clause := range orderBy {
			if sortIndices[k] < 0 {
				// Variable not in results, skip
				continue
			}

			cmp := datalog.CompareValues(
				tuples[i][sortIndices[k]],
				tuples[j][sortIndices[k]],
			)

			if cmp < 0 {
				return clause.Direction != query.OrderDesc
			} else if cmp > 0 {
				return clause.Direction == query.OrderDesc
			}
			// Equal, continue to next sort key
		}
		return false
	})

	opts := rel.Options()
	return NewMaterializedRelationWithOptions(columns, tuples, opts)
}

// computeAggregate computes an aggregate over all values in a column
func computeAggregate(rel Relation, colIdx int, function string) interface{} {
	var values []interface{}

	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		if colIdx < len(tuple) {
			values = append(values, tuple[colIdx])
		}
	}

	return computeAggregateValues(values, function)
}

// computeAggregateValues computes an aggregate over a slice of values
func computeAggregateValues(values []interface{}, function string) interface{} {
	switch function {
	case "count":
		return int64(len(values))

	case "sum":
		if len(values) == 0 {
			return nil
		}
		var sum float64
		for _, v := range values {
			if num, ok := toFloat64(v); ok {
				sum += num
			}
		}
		return sum

	case "avg":
		if len(values) == 0 {
			return nil
		}
		var sum float64
		count := 0
		for _, v := range values {
			if num, ok := toFloat64(v); ok {
				sum += num
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "min":
		if len(values) == 0 {
			return nil
		}
		var min interface{}
		for _, v := range values {
			if v == nil {
				continue
			}
			if min == nil || datalog.CompareValues(v, min) < 0 {
				min = v
			}
		}
		return min

	case "max":
		if len(values) == 0 {
			return nil
		}
		var max interface{}
		for _, v := range values {
			if v == nil {
				continue
			}
			if max == nil || datalog.CompareValues(v, max) > 0 {
				max = v
			}
		}
		return max

	default:
		return nil
	}
}

// toFloat64 converts a value to float64 if possible
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
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
			// Single value input - expect a relation with one column and one row
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if rel.Size() > 0 {
					// Create a new relation with the input symbol as column name
					columns := []query.Symbol{inp.Symbol}
					tuples := make([]Tuple, 0, rel.Size())

					it := rel.Iterator()
					for it.Next() {
						tuple := it.Tuple()
						if len(tuple) > 0 {
							// Take first value from each tuple
							tuples = append(tuples, Tuple{tuple[0]})
						}
					}
					it.Close()

					opts := rel.Options()
					boundRelations = append(boundRelations, NewMaterializedRelationWithOptions(columns, tuples, opts))
				}
				relationIndex++
			}

		case query.RelationInput:
			// Multiple tuples input - use the relation directly with renamed columns
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if rel.Size() > 0 && len(inp.Symbols) == len(rel.Columns()) {
					// Create a new relation with the input variables as column names
					tuples := make([]Tuple, 0, rel.Size())

					it := rel.Iterator()
					for it.Next() {
						tuples = append(tuples, it.Tuple())
					}
					it.Close()

					opts := rel.Options()
					boundRelations = append(boundRelations, NewMaterializedRelationWithOptions(inp.Symbols, tuples, opts))
				}
				relationIndex++
			}

		case query.TupleInput:
			// Single tuple input - expect a relation with one row
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if rel.Size() > 0 && len(inp.Symbols) == len(rel.Columns()) {
					// Take the first tuple and bind to variables
					it := rel.Iterator()
					if it.Next() {
						tuple := it.Tuple()
						opts := rel.Options()
						boundRelations = append(boundRelations,
							NewMaterializedRelationWithOptions(inp.Symbols, []Tuple{tuple}, opts))
					}
					it.Close()
				}
				relationIndex++
			}

		case query.CollectionInput:
			// Collection input - all values in one column
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if rel.Size() > 0 {
					// Create a relation with the collection variable
					columns := []query.Symbol{inp.Symbol}
					tuples := make([]Tuple, 0, rel.Size())

					it := rel.Iterator()
					for it.Next() {
						tuple := it.Tuple()
						if len(tuple) > 0 {
							// Take first value from each tuple
							tuples = append(tuples, Tuple{tuple[0]})
						}
					}
					it.Close()

					opts := rel.Options()
					boundRelations = append(boundRelations, NewMaterializedRelationWithOptions(columns, tuples, opts))
				}
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
