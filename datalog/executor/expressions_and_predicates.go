package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// applyExpressionsAndPredicates applies expressions and predicates to collapsed relations
// It handles multiple disjoint relation groups by attempting to join them after expressions
// add new symbols that might connect them.
func (e *Executor) applyExpressionsAndPredicates(ctx Context, phase *planner.Phase, relations Relations) (Relation, error) {

	// If we have no relations, return empty
	// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!
	// Empty detection happens naturally through subsequent operations
	if len(relations) == 0 {
		return NewMaterializedRelationWithOptions(phase.Provides, []Tuple{}, e.options), nil
	}

	// Keep track of relation groups - they might join after expressions add symbols
	groups := relations

	// Apply expressions to each group
	for _, exprPlan := range phase.Expressions {
		// Skip expressions that were optimized by semantic rewriting
		if exprPlan.Metadata != nil {
			if optimized, ok := exprPlan.Metadata["optimized_by_constraint"].(bool); ok && optimized {
				continue
			}
		}

		var newGroups Relations

		for _, group := range groups {
			// Check if this group has all required symbols for the expression
			hasAllInputs := true
			groupCols := group.Columns()
			for _, input := range exprPlan.Inputs {
				found := false
				for _, col := range groupCols {
					if col == input {
						found = true
						break
					}
				}
				if !found {
					hasAllInputs = false
					break
				}
			}

			if !hasAllInputs {
				// This group doesn't have the required inputs, keep as-is
				if collector := ctx.Collector(); collector != nil {
					collector.Add(annotations.Event{
						Name: "expression/skipped",
						Data: map[string]interface{}{
							"expression":     exprPlan.Expression.String(),
							"required":       exprPlan.Inputs,
							"available":      groupCols,
							"group_size":     group.Size(),
							"group_columns":  group.Columns(),
						},
					})
				}
				newGroups = append(newGroups, group)
				continue
			}

			// Annotate before evaluation
			if collector := ctx.Collector(); collector != nil {
				collector.Add(annotations.Event{
					Name: "expression/begin",
					Data: map[string]interface{}{
						"expression":    exprPlan.Expression.String(),
						"is_equality":   exprPlan.IsEquality,
						"output":        exprPlan.Output,
						"inputs":        exprPlan.Inputs,
						"input_size":    group.Size(),
						"input_columns": group.Columns(),
					},
				})
			}

			// Apply the expression to this group
			var result Relation
			if exprPlan.IsEquality {
				// This is an equality expression - apply as a filter using Eval
				result = ctx.FilterRelation(group, exprPlan.Expression.String(), func() Relation {
					return filterWithExpression(group, exprPlan.Expression)
				})
			} else {
				// This is a binding expression - evaluate and add column
				result = ctx.EvaluateExpressionRelation(group, exprPlan.Expression.String(), func() Relation {
					return evaluateExpressionNew(group, exprPlan.Expression)
				})
			}

			// Annotate after evaluation
			if collector := ctx.Collector(); collector != nil {
				collector.Add(annotations.Event{
					Name: "expression/complete",
					Data: map[string]interface{}{
						"expression":     exprPlan.Expression.String(),
						"output_size":    result.Size(),
						"output_columns": result.Columns(),
						"reduction":      float64(result.Size()) / float64(group.Size()),
					},
				})
			}

			newGroups = append(newGroups, result)
		}

		groups = newGroups

		// After each expression, try to collapse groups again
		// Expressions might have added symbols that allow joining
		groups = groups.Collapse(ctx)
	}

	// Apply predicates to each group
	for _, predPlan := range phase.Predicates {
		// Skip predicates that were optimized by semantic rewriting
		if predPlan.Metadata != nil {
			if optimized, ok := predPlan.Metadata["optimized_by_constraint"].(bool); ok && optimized {
				continue
			}
		}

		// Use the new Predicate interface directly
		var newGroups Relations

		for _, group := range groups {
			// Check if this group has all required symbols for the predicate
			hasAllSymbols := true
			groupCols := group.Columns()
			for _, sym := range predPlan.Predicate.RequiredSymbols() {
				found := false
				for _, col := range groupCols {
					if col == sym {
						found = true
						break
					}
				}
				if !found {
					hasAllSymbols = false
					break
				}
			}

			if !hasAllSymbols {
				// This group doesn't have the required symbols - skip the predicate
				// This can happen when expressions that generate required symbols were
				// skipped because their inputs weren't available in this specific group.
				// Keep the group as-is without applying the predicate.
				newGroups = append(newGroups, group)
				continue
			}

			result := ctx.FilterRelation(group, predPlan.Predicate.String(), func() Relation {
				return filterWithPredicate(group, predPlan.Predicate)
			})

			// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!
			// Just keep all results; empty detection happens naturally
			newGroups = append(newGroups, result)
		}

		groups = newGroups

		// If all groups are now empty, return early
		if len(groups) == 0 {
			return NewMaterializedRelationWithOptions(phase.Provides, []Tuple{}, e.options), nil
		}
	}

	// Try one final collapse after all expressions and predicates
	groups = groups.Collapse(ctx)

	// Execute subqueries if any
	if len(phase.Subqueries) > 0 || len(phase.DecorrelatedSubqueries) > 0 {
		// For now, we only support subqueries with a single group
		if len(groups) != 1 {
			return nil, fmt.Errorf("subqueries with disjoint relations not yet supported")
		}

		result := groups[0]

		// Execute decorrelated subqueries first (if any)
		if len(phase.DecorrelatedSubqueries) > 0 {
			for _, decorPlan := range phase.DecorrelatedSubqueries {
				decorResult, err := executeDecorrelatedSubqueries(ctx, e, &decorPlan, result)
				if err != nil {
					return nil, fmt.Errorf("decorrelated subquery execution failed: %w", err)
				}

				// Join the decorrelated result with the phase result
				result = ctx.JoinRelations(result, decorResult, func() Relation {
					return result.Join(decorResult)
				})

				// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!
				// Empty detection happens naturally in subsequent operations
			}
		}

		// Execute remaining non-decorrelated subqueries (if any)
		for _, subqPlan := range phase.Subqueries {
			// Skip subqueries that were decorrelated (already executed above)
			if subqPlan.Decorrelated {
				continue
			}

			// CRITICAL: Materialize result before subquery if it's streaming and we'll need to join with it
			// The subquery will iterate over result to extract input combinations,
			// and then we need to iterate it again for the join
			if sr, ok := result.(*StreamingRelation); ok {
			result = sr.Materialize()
			}

			// Execute the subquery for each unique combination of input values
			subqResult, err := e.executeSubquery(ctx, subqPlan, result)
			if err != nil {
				return nil, fmt.Errorf("subquery execution failed: %w", err)
			}

			// Join the subquery result with the phase result
			result = ctx.JoinRelations(result, subqResult, func() Relation {
				return result.Join(subqResult)
			})

			// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!
			// Empty detection happens naturally in subsequent operations
		}

		// Update groups with the result
		groups = Relations{result}
	}

	// At this point, we should ideally have a single group
	if len(groups) == 1 {
		result := groups[0]

		// Check if there are aggregate required columns in metadata (from conditional aggregate rewriting)
		var keepCols []query.Symbol
		if phase.Metadata != nil {
			if reqCols, ok := phase.Metadata["aggregate_required_columns"].([]query.Symbol); ok && len(reqCols) > 0 {
				// Use the aggregate required columns PLUS the normal Keep columns
				keepCols = append([]query.Symbol{}, phase.Keep...)
				for _, reqCol := range reqCols {
					found := false
					for _, k := range keepCols {
						if k == reqCol {
							found = true
							break
						}
					}
					if !found {
						keepCols = append(keepCols, reqCol)
					}
				}
			} else {
				keepCols = phase.Keep
			}
		} else {
			keepCols = phase.Keep
		}

		// Project to keep only required symbols
		// CRITICAL FIX: Only project symbols that actually exist in the relation
		// The planner may include expression outputs in Keep, but expressions are
		// evaluated on a per-group basis and may be skipped if inputs aren't available
		// in that specific group. Filter Keep to only include symbols in the relation.
		if len(keepCols) > 0 && result != nil {
			resultCols := result.Columns()
			colSet := make(map[query.Symbol]bool)
			for _, col := range resultCols {
				colSet[col] = true
			}

			// Filter Keep to only include symbols that exist in the relation
			var actualKeep []query.Symbol
			for _, sym := range keepCols {
				if colSet[sym] {
					actualKeep = append(actualKeep, sym)
				}
			}

			// Only project if we have symbols to keep and they're different from current columns
			if len(actualKeep) > 0 && !equalSymbols(actualKeep, resultCols) {
				projected, err := result.Project(actualKeep)
				if err != nil {
					return nil, fmt.Errorf("projection failed: %w", err)
				}
				result = projected
			}
		}

		return result, nil
	}

	// We still have multiple disjoint groups
	// This means the query is asking for a Cartesian product
	// For now, return an error
	if len(groups) > 1 {
		return nil, fmt.Errorf("phase resulted in %d disjoint relation groups - Cartesian products not supported", len(groups))
	}

	// No groups left (all filtered out)
	return NewMaterializedRelationWithOptions(phase.Provides, []Tuple{}, e.options), nil
}

// filterWithPredicate filters a relation using a Predicate's Eval method
func filterWithPredicate(rel Relation, pred query.Predicate) Relation {
	columns := rel.Columns()

	// Pre-allocate filtered only for materialized relations to avoid forcing materialization
	var filtered []Tuple
	if _, ok := rel.(*MaterializedRelation); ok {
		if size := rel.Size(); size >= 0 {
			filtered = make([]Tuple, 0, size)
		}
	}

	// Reuse single bindings map to avoid repeated allocations
	bindings := make(map[query.Symbol]interface{}, len(columns))

	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()

		// Clear and populate bindings map
		for k := range bindings {
			delete(bindings, k)
		}
		for i, col := range columns {
			bindings[col] = tuple[i]
		}

		// Evaluate the predicate
		passes, err := pred.Eval(bindings)
		if err != nil {
			// Log error but continue processing
			// TODO: Consider better error handling strategy
			continue
		}

		if passes {
			filtered = append(filtered, tuple)
		}
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	return NewMaterializedRelationWithOptions(columns, filtered, opts)
}

// filterWithExpression filters a relation using an Expression that acts as a predicate (IsEquality = true)
func filterWithExpression(rel Relation, expr *query.Expression) Relation {
	columns := rel.Columns()

	// Pre-allocate filtered only for materialized relations to avoid forcing materialization
	var filtered []Tuple
	if _, ok := rel.(*MaterializedRelation); ok {
		if size := rel.Size(); size >= 0 {
			filtered = make([]Tuple, 0, size)
		}
	}

	// Reuse single bindings map to avoid repeated allocations
	bindings := make(map[query.Symbol]interface{}, len(columns))

	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()

		// Clear and populate bindings map
		for k := range bindings {
			delete(bindings, k)
		}
		for i, col := range columns {
			bindings[col] = tuple[i]
		}

		// Evaluate the expression (should return a boolean)
		result, err := expr.Function.Eval(bindings)
		if err != nil {
			continue
		}

		// Check if the result is true
		if passes, ok := result.(bool); ok && passes {
			filtered = append(filtered, tuple)
		}
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	return NewMaterializedRelationWithOptions(columns, filtered, opts)
}

// evaluateExpressionNew evaluates an expression and adds the result as a new column
func evaluateExpressionNew(rel Relation, expr *query.Expression) Relation {
	columns := rel.Columns()

	// Add the binding column if it doesn't exist
	hasBinding := false
	for _, col := range columns {
		if col == expr.Binding {
			hasBinding = true
			break
		}
	}

	newColumns := columns
	if !hasBinding && expr.Binding != "" {
		newColumns = append([]query.Symbol{}, columns...)
		newColumns = append(newColumns, expr.Binding)
	}

	// Reuse single bindings map to avoid repeated allocations
	bindings := make(map[query.Symbol]interface{}, len(columns))

	// Pre-allocate newTuples only for materialized relations to avoid forcing materialization
	var newTuples []Tuple
	if _, ok := rel.(*MaterializedRelation); ok {
		if size := rel.Size(); size >= 0 {
			newTuples = make([]Tuple, 0, size)
		}
	}

	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()

		// Clear and populate bindings map
		for k := range bindings {
			delete(bindings, k)
		}
		for i, col := range columns {
			bindings[col] = tuple[i]
		}

		// Evaluate the expression
		result, err := expr.Function.Eval(bindings)
		if err != nil {
			// Skip tuples where expression fails
			continue
		}

		// Create new tuple with result
		if hasBinding {
			// Update existing column
			newTuple := make(Tuple, len(tuple))
			copy(newTuple, tuple)
			for i, col := range columns {
				if col == expr.Binding {
					newTuple[i] = result
					break
				}
			}
			newTuples = append(newTuples, newTuple)
		} else if expr.Binding != "" {
			// Add new column
			newTuple := make(Tuple, len(tuple)+1)
			copy(newTuple, tuple)
			newTuple[len(tuple)] = result
			newTuples = append(newTuples, newTuple)
		} else {
			// No binding, just keep original tuple (shouldn't happen)
			newTuples = append(newTuples, tuple)
		}
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	return NewMaterializedRelationWithOptions(newColumns, newTuples, opts)
}
