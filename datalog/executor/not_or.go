package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// executeNotClause performs anti-join filtering
// For each input tuple, the tuple is EXCLUDED if the inner clauses match
func (e *Executor) executeNotClause(ctx Context, clause *query.NotClause, input Relation) (Relation, error) {
	if input == nil {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Materialize input since we need to iterate multiple times
	input = input.Materialize()

	// Collect all variables from inner clauses to determine join keys
	joinVars := collectInnerVars(clause.Clauses)
	if len(joinVars) == 0 {
		return nil, fmt.Errorf("NOT clause has no variables to join on")
	}

	// Filter to only variables present in input relation
	inputCols := input.Columns()
	inputColSet := make(map[query.Symbol]bool)
	for _, col := range inputCols {
		inputColSet[col] = true
	}

	var actualJoinVars []query.Symbol
	for _, v := range joinVars {
		if inputColSet[v] {
			actualJoinVars = append(actualJoinVars, v)
		}
	}

	if len(actualJoinVars) == 0 {
		return nil, fmt.Errorf("NOT clause variables not found in input relation")
	}

	// Get unique combinations of join variables from input
	uniqueCombos := getUniqueCombinations(input, actualJoinVars)

	// Track which key combinations matched the inner clauses
	matchedKeys := make(map[string]bool)

	// For each unique combination, execute inner clauses
	for _, combo := range uniqueCombos {
		// Create a single-tuple relation with the combo values for binding
		bindingRel := NewMaterializedRelationWithOptions(actualJoinVars, []Tuple{combo}, e.options)

		// Execute inner clauses with this binding
		// We need to match each inner clause and join results
		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("NOT inner clause execution failed: %w", err)
		}

		// If inner produced any results, this combo is "matched" and should be excluded
		if innerResult != nil && innerResult.Size() > 0 {
			key := notOrTupleKey(combo)
			matchedKeys[key] = true
		}
	}

	// Build join key column indices for input
	keyIndices := make([]int, len(actualJoinVars))
	for i, v := range actualJoinVars {
		for j, col := range inputCols {
			if col == v {
				keyIndices[i] = j
				break
			}
		}
	}

	// Filter input: keep tuples whose join key is NOT in matchedKeys
	var filtered []Tuple
	iter := input.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		// Extract key values
		keyVals := make(Tuple, len(keyIndices))
		for i, idx := range keyIndices {
			keyVals[i] = tuple[idx]
		}
		key := notOrTupleKey(keyVals)

		if !matchedKeys[key] {
			filtered = append(filtered, tuple)
		}
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(inputCols, filtered, e.options), nil
}

// executeNotJoinClause performs anti-join with explicit join variables
func (e *Executor) executeNotJoinClause(ctx Context, clause *query.NotJoinClause, input Relation) (Relation, error) {
	if input == nil {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Materialize input since we need to iterate multiple times
	input = input.Materialize()

	joinVars := clause.JoinVars
	if len(joinVars) == 0 {
		return nil, fmt.Errorf("NOT-JOIN clause has no join variables")
	}

	// Verify join vars are in input
	inputCols := input.Columns()
	inputColSet := make(map[query.Symbol]bool)
	for _, col := range inputCols {
		inputColSet[col] = true
	}

	for _, v := range joinVars {
		if !inputColSet[v] {
			return nil, fmt.Errorf("NOT-JOIN variable %s not found in input relation", v)
		}
	}

	// Same logic as executeNotClause but with explicit join vars
	uniqueCombos := getUniqueCombinations(input, joinVars)
	matchedKeys := make(map[string]bool)

	for _, combo := range uniqueCombos {
		bindingRel := NewMaterializedRelationWithOptions(joinVars, []Tuple{combo}, e.options)
		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("NOT-JOIN inner clause execution failed: %w", err)
		}

		if innerResult != nil && innerResult.Size() > 0 {
			key := notOrTupleKey(combo)
			matchedKeys[key] = true
		}
	}

	// Build key indices
	keyIndices := make([]int, len(joinVars))
	for i, v := range joinVars {
		for j, col := range inputCols {
			if col == v {
				keyIndices[i] = j
				break
			}
		}
	}

	// Filter
	var filtered []Tuple
	iter := input.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		keyVals := make(Tuple, len(keyIndices))
		for i, idx := range keyIndices {
			keyVals[i] = tuple[idx]
		}
		key := notOrTupleKey(keyVals)

		if !matchedKeys[key] {
			filtered = append(filtered, tuple)
		}
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(inputCols, filtered, e.options), nil
}

// executeOrClause performs union of branches, or fallback semantics for expression branches
func (e *Executor) executeOrClause(ctx Context, clause *query.OrClause, available Relations) (Relation, error) {
	if len(clause.Branches) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Check if any branch has expressions - use fallback semantics if so
	if query.OrHasExpressions(clause.Branches) {
		return e.executeOrClauseFallback(ctx, clause, available)
	}

	// Standard union semantics for pattern-only OR
	return e.executeOrClauseUnion(ctx, clause, available)
}

// executeOrClauseFallback implements Clojure-style fallback semantics:
// Try each branch in order, return first non-empty result
func (e *Executor) executeOrClauseFallback(ctx Context, clause *query.OrClause, available Relations) (Relation, error) {
	for i, branch := range clause.Branches {
		// Execute branch with available bindings
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d execution failed: %w", i+1, err)
		}

		// Check if result is non-empty by collecting tuples
		// We materialize here because fallback semantics require knowing if a branch
		// produced results before trying the next branch
		if branchResult != nil {
			var tuples []Tuple
			iter := branchResult.Iterator()
			for iter.Next() {
				tuples = append(tuples, iter.Tuple())
			}
			iter.Close()

			if len(tuples) > 0 {
				return NewMaterializedRelationWithOptions(branchResult.Columns(), tuples, e.options), nil
			}
		}
	}

	// All branches empty
	return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
}

// executeOrClauseUnion implements standard Datalog union semantics:
// Execute all branches and merge results
func (e *Executor) executeOrClauseUnion(ctx Context, clause *query.OrClause, available Relations) (Relation, error) {
	// Execute each branch and collect results
	var branchResults []Relation
	var commonCols []query.Symbol

	for i, branch := range clause.Branches {
		// Execute this branch's clauses
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d execution failed: %w", i+1, err)
		}

		if branchResult == nil {
			continue
		}

		// Track columns for intersection
		if i == 0 {
			commonCols = branchResult.Columns()
		} else {
			// Intersect columns
			branchColSet := make(map[query.Symbol]bool)
			for _, col := range branchResult.Columns() {
				branchColSet[col] = true
			}
			var newCommon []query.Symbol
			for _, col := range commonCols {
				if branchColSet[col] {
					newCommon = append(newCommon, col)
				}
			}
			commonCols = newCommon
		}

		branchResults = append(branchResults, branchResult)
	}

	if len(branchResults) == 0 || len(commonCols) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Union all branch results, projecting to common columns
	return unionRelations(branchResults, commonCols, e.options), nil
}

// executeOrJoinClause performs union with explicit join variables, or fallback for expressions
func (e *Executor) executeOrJoinClause(ctx Context, clause *query.OrJoinClause, available Relations) (Relation, error) {
	if len(clause.Branches) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	joinVars := clause.JoinVars
	if len(joinVars) == 0 {
		return nil, fmt.Errorf("OR-JOIN clause has no join variables")
	}

	// Check if any branch has expressions - use fallback semantics if so
	if query.OrHasExpressions(clause.Branches) {
		return e.executeOrJoinClauseFallback(ctx, clause, available)
	}

	var branchResults []Relation

	for i, branch := range clause.Branches {
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR-JOIN branch %d execution failed: %w", i+1, err)
		}

		if branchResult != nil {
			branchResults = append(branchResults, branchResult)
		}
	}

	if len(branchResults) == 0 {
		return NewMaterializedRelationWithOptions(joinVars, nil, e.options), nil
	}

	// Union all branch results, projecting to join vars
	return unionRelations(branchResults, joinVars, e.options), nil
}

// executeOrJoinClauseFallback implements fallback semantics for or-join with expressions
func (e *Executor) executeOrJoinClauseFallback(ctx Context, clause *query.OrJoinClause, available Relations) (Relation, error) {
	joinVars := clause.JoinVars

	for i, branch := range clause.Branches {
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR-JOIN branch %d execution failed: %w", i+1, err)
		}

		// Return first non-empty result, projected to join vars
		if branchResult != nil && branchResult.Size() > 0 {
			// Project to join vars only
			return projectToColumns(branchResult, joinVars, e.options), nil
		}
	}

	// All branches empty
	return NewMaterializedRelationWithOptions(joinVars, nil, e.options), nil
}

// projectToColumns projects a relation to specified columns
func projectToColumns(rel Relation, cols []query.Symbol, opts ExecutorOptions) Relation {
	relCols := rel.Columns()

	// Build column index mapping
	colIndices := make([]int, len(cols))
	for i, col := range cols {
		found := false
		for j, relCol := range relCols {
			if relCol == col {
				colIndices[i] = j
				found = true
				break
			}
		}
		if !found {
			// Column not found - return empty relation
			return NewMaterializedRelationWithOptions(cols, nil, opts)
		}
	}

	var projected []Tuple
	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		newTuple := make(Tuple, len(cols))
		for i, idx := range colIndices {
			newTuple[i] = tuple[idx]
		}
		projected = append(projected, newTuple)
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(cols, projected, opts)
}

// executeInnerClauses executes a list of clauses and returns the result
// Used by NOT and OR to execute their inner clauses
func (e *Executor) executeInnerClauses(ctx Context, clauses []query.Clause, binding Relation) (Relation, error) {
	if len(clauses) == 0 {
		return binding, nil
	}

	// Start with binding relation (or empty)
	var result Relations
	if binding != nil {
		result = Relations{binding}
	}

	for _, clause := range clauses {
		switch c := clause.(type) {
		case *query.DataPattern:
			// Match the pattern
			rel, err := e.matcher.Match(c, result)
			if err != nil {
				return nil, err
			}
			if rel == nil {
				return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
			}
			// Join with existing result
			if len(result) > 0 {
				result = append(result, rel)
				result = result.Collapse(ctx)
			} else {
				result = Relations{rel}
			}

		case *query.NotClause:
			// Nested NOT
			if len(result) == 0 {
				return nil, fmt.Errorf("NOT clause requires prior bindings")
			}
			collapsed := result.Collapse(ctx)
			if len(collapsed) != 1 {
				return nil, fmt.Errorf("NOT clause requires single relation")
			}
			notResult, err := e.executeNotClause(ctx, c, collapsed[0])
			if err != nil {
				return nil, err
			}
			result = Relations{notResult}

		case *query.OrClause:
			// Nested OR
			orResult, err := e.executeOrClause(ctx, c, result)
			if err != nil {
				return nil, err
			}
			if len(result) > 0 {
				result = append(result, orResult)
				result = result.Collapse(ctx)
			} else {
				result = Relations{orResult}
			}

		case *query.Expression:
			// Expression evaluation - supports ground, arithmetic, etc.
			exprResult, err := e.executeInnerExpression(ctx, c, result)
			if err != nil {
				return nil, fmt.Errorf("expression evaluation failed: %w", err)
			}
			result = Relations{exprResult}

		case query.Predicate:
			// Predicate filtering
			if len(result) == 0 {
				return nil, fmt.Errorf("predicate requires prior bindings")
			}
			collapsed := result.Collapse(ctx)
			if len(collapsed) != 1 {
				return nil, fmt.Errorf("predicate requires single relation")
			}
			predResult := filterWithPredicate(collapsed[0], c)
			result = Relations{predResult}

		default:
			return nil, fmt.Errorf("unsupported clause type in NOT/OR: %T", clause)
		}
	}

	if len(result) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Collapse to single relation
	collapsed := result.Collapse(ctx)
	if len(collapsed) != 1 {
		return nil, fmt.Errorf("inner clauses resulted in %d disjoint groups", len(collapsed))
	}

	return collapsed[0], nil
}

// executeInnerExpression evaluates an expression within an OR/NOT branch
func (e *Executor) executeInnerExpression(ctx Context, expr *query.Expression, groups Relations) (Relation, error) {
	// If no input relations, create a single empty tuple to evaluate against
	// This is needed for ground expressions like [(ground 0) ?x]
	if len(groups) == 0 {
		// Create a single-tuple relation with just the binding
		result, err := expr.Function.Eval(make(map[query.Symbol]interface{}))
		if err != nil {
			return nil, err
		}
		columns := []query.Symbol{expr.Binding}
		tuples := []Tuple{{result}}
		return NewMaterializedRelationWithOptions(columns, tuples, e.options), nil
	}

	// Collapse groups to single relation for expression evaluation
	collapsed := groups.Collapse(ctx)
	if len(collapsed) != 1 {
		return nil, fmt.Errorf("expression requires single relation, got %d disjoint groups", len(collapsed))
	}

	// Evaluate expression over the relation
	return evaluateExpressionWithLookup(collapsed[0], expr, nil), nil
}

// collectInnerVars collects all variables from inner clauses
func collectInnerVars(clauses []query.Clause) []query.Symbol {
	seen := make(map[query.Symbol]bool)
	var vars []query.Symbol

	for _, clause := range clauses {
		switch c := clause.(type) {
		case *query.DataPattern:
			for _, sym := range c.Symbols() {
				if !seen[sym] {
					seen[sym] = true
					vars = append(vars, sym)
				}
			}
		case *query.NotClause:
			for _, sym := range collectInnerVars(c.Clauses) {
				if !seen[sym] {
					seen[sym] = true
					vars = append(vars, sym)
				}
			}
		case *query.OrClause:
			for _, branch := range c.Branches {
				for _, sym := range collectInnerVars(branch) {
					if !seen[sym] {
						seen[sym] = true
						vars = append(vars, sym)
					}
				}
			}
		}
	}

	return vars
}

// getUniqueCombinations extracts unique value combinations for the given columns
func getUniqueCombinations(rel Relation, cols []query.Symbol) []Tuple {
	if rel == nil || len(cols) == 0 {
		return nil
	}

	relCols := rel.Columns()
	colIndices := make([]int, len(cols))
	for i, col := range cols {
		found := false
		for j, relCol := range relCols {
			if relCol == col {
				colIndices[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	seen := make(map[string]bool)
	var combos []Tuple

	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		combo := make(Tuple, len(colIndices))
		for i, idx := range colIndices {
			combo[i] = tuple[idx]
		}
		key := notOrTupleKey(combo)
		if !seen[key] {
			seen[key] = true
			combos = append(combos, combo)
		}
	}
	iter.Close()

	return combos
}

// notOrTupleKey creates a string key from a tuple for deduplication in NOT/OR execution
func notOrTupleKey(tuple Tuple) string {
	if len(tuple) == 0 {
		return ""
	}
	key := fmt.Sprintf("%v", tuple[0])
	for i := 1; i < len(tuple); i++ {
		key += fmt.Sprintf("|%v", tuple[i])
	}
	return key
}

// unionRelations creates a union of multiple relations, projecting to common columns
func unionRelations(relations []Relation, cols []query.Symbol, opts ExecutorOptions) Relation {
	if len(relations) == 0 {
		return NewMaterializedRelationWithOptions(cols, nil, opts)
	}

	seen := make(map[string]bool)
	var allTuples []Tuple

	for _, rel := range relations {
		// Build column index mapping
		relCols := rel.Columns()
		colIndices := make([]int, len(cols))
		valid := true
		for i, col := range cols {
			found := false
			for j, relCol := range relCols {
				if relCol == col {
					colIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				valid = false
				break
			}
		}

		if !valid {
			continue
		}

		iter := rel.Iterator()
		for iter.Next() {
			tuple := iter.Tuple()
			projected := make(Tuple, len(cols))
			for i, idx := range colIndices {
				projected[i] = tuple[idx]
			}
			key := notOrTupleKey(projected)
			if !seen[key] {
				seen[key] = true
				allTuples = append(allTuples, projected)
			}
		}
		iter.Close()
	}

	return NewMaterializedRelationWithOptions(cols, allTuples, opts)
}
