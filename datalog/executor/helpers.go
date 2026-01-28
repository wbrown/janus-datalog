package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// hasRelationInput checks if a query has a RelationInput in its :in clause
func hasRelationInput(q *query.Query) bool {
	for _, input := range q.In {
		if _, ok := input.(query.RelationInput); ok {
			return true
		}
	}
	return false
}

// materializeRelationsForPattern materializes relations that share symbols with a pattern.
// This is needed for binding-based filtering during pattern matching.
func materializeRelationsForPattern(pattern *query.DataPattern, relations Relations) Relations {
	// Extract pattern symbols - what variables does this pattern bind?
	patternSymbols := pattern.Symbols()
	if len(patternSymbols) == 0 {
		// No variables in pattern - no bindings needed
		return relations
	}

	// Build set for fast lookup
	patternSymbolSet := make(map[query.Symbol]bool)
	for _, sym := range patternSymbols {
		patternSymbolSet[sym] = true
	}

	// Materialize relations that share symbols with the pattern
	result := make(Relations, len(relations))
	for i, rel := range relations {
		hasSharedSymbol := false
		for _, sym := range rel.Symbols() {
			if patternSymbolSet[sym] {
				hasSharedSymbol = true
				break
			}
		}

		if hasSharedSymbol {
			// This relation shares symbols with the pattern - materialize it
			// It will be used for binding-based filtering AND joining
			result[i] = rel.Materialize()
		} else {
			// No shared symbols - keep as-is (pure streaming)
			result[i] = rel
		}
	}

	return result
}

// filterWithPredicateAndLookup filters a relation using a predicate with optional database lookup.
func filterWithPredicateAndLookup(rel Relation, pred query.Predicate, lookup query.EntityLookup) Relation {
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

	// Check if this is a DatabaseFunctionPredicate that needs lookup
	dbFuncPred, isDbFuncPred := pred.(*query.DatabaseFunctionPredicate)

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
		var passes bool
		var err error
		if isDbFuncPred && lookup != nil {
			passes, err = dbFuncPred.EvalWithLookup(bindings, lookup)
		} else {
			passes, err = pred.Eval(bindings)
		}
		if err != nil {
			// Log error but continue processing
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

// evaluateExpressionWithLookup evaluates an expression with optional database lookup support.
// If lookup is non-nil and the expression is a DatabaseFunction, it uses EvalWithLookup.
// Otherwise, it falls back to the standard Eval method.
func evaluateExpressionWithLookup(rel Relation, expr *query.Expression, lookup query.EntityLookup) Relation {
	columns := rel.Columns()

	// Determine binding symbols and whether they already exist
	var bindingSymbols []query.Symbol
	switch b := expr.Binding.(type) {
	case query.Symbol:
		if b != nil {
			bindingSymbols = []query.Symbol{b}
		}
	case query.TupleBinding:
		bindingSymbols = b.Variables
	}

	// Check which binding columns already exist
	hasAllBindings := len(bindingSymbols) > 0
	existingBindingIndices := make(map[query.Symbol]int)
	for _, bindSym := range bindingSymbols {
		found := false
		for i, col := range columns {
			if col == bindSym {
				existingBindingIndices[bindSym] = i
				found = true
				break
			}
		}
		if !found {
			hasAllBindings = false
		}
	}

	newColumns := columns
	if !hasAllBindings && len(bindingSymbols) > 0 {
		newColumns = append([]query.Symbol{}, columns...)
		for _, bindSym := range bindingSymbols {
			if _, exists := existingBindingIndices[bindSym]; !exists {
				newColumns = append(newColumns, bindSym)
			}
		}
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
		// Check if this is a database function that needs lookup access
		var result interface{}
		var err error
		if dbFunc, ok := expr.Function.(query.DatabaseFunction); ok && lookup != nil {
			result, err = dbFunc.EvalWithLookup(bindings, lookup)
		} else {
			result, err = expr.Function.Eval(bindings)
		}
		if err != nil {
			// Skip tuples where expression fails
			continue
		}

		// Extract value from GetSomeResult if needed
		// get-some returns a struct with Attr and Value; we just want the Value for binding
		if gsr, ok := result.(*query.GetSomeResult); ok {
			result = gsr.Value
		}

		// Create new tuple with result
		if len(bindingSymbols) == 0 {
			// No binding, just keep original tuple
			newTuples = append(newTuples, tuple)
		} else if hasAllBindings {
			// Update existing columns
			newTuple := make(Tuple, len(tuple))
			copy(newTuple, tuple)
			// Handle tuple binding - result should be []interface{}
			if tb, ok := expr.Binding.(query.TupleBinding); ok {
				values, ok := result.([]interface{})
				if ok && len(values) == len(tb.Variables) {
					for i, bindSym := range tb.Variables {
						if idx, exists := existingBindingIndices[bindSym]; exists {
							newTuple[idx] = values[i]
						}
					}
				}
			} else {
				// Scalar binding
				for i, col := range columns {
					if bindSym, ok := expr.Binding.(query.Symbol); ok && col == bindSym {
						newTuple[i] = result
						break
					}
				}
			}
			newTuples = append(newTuples, newTuple)
		} else {
			// Add new columns
			newTuple := make(Tuple, len(newColumns))
			copy(newTuple, tuple)
			// Handle tuple binding - result should be []interface{}
			if tb, ok := expr.Binding.(query.TupleBinding); ok {
				values, ok := result.([]interface{})
				if ok && len(values) == len(tb.Variables) {
					for i, bindSym := range tb.Variables {
						// Find position of this symbol in newColumns
						for j := len(columns); j < len(newColumns); j++ {
							if newColumns[j] == bindSym {
								newTuple[j] = values[i]
								break
							}
						}
					}
				}
			} else {
				// Scalar binding - add to end
				newTuple[len(tuple)] = result
			}
			newTuples = append(newTuples, newTuple)
		}
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	return NewMaterializedRelationWithOptions(newColumns, newTuples, opts)
}

// projectToColumns projects a relation to the specified columns
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

// countOverlap counts how many symbols from syms are present in cols
func countOverlap(cols, syms []query.Symbol) int {
	colSet := make(map[query.Symbol]bool, len(cols))
	for _, col := range cols {
		colSet[col] = true
	}
	count := 0
	for _, sym := range syms {
		if colSet[sym] {
			count++
		}
	}
	return count
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

// convertPlannerConstraints converts planner-level storage constraints to executor-level constraints
func convertPlannerConstraints(pattern *query.DataPattern, plannerConstraints []planner.StorageConstraint) []StorageConstraint {
	var result []StorageConstraint

	// Map pattern variables to positions
	varPositions := make(map[query.Symbol]int)
	if v, ok := pattern.GetE().(query.Variable); ok {
		varPositions[v.Name] = 0
	}
	if v, ok := pattern.GetA().(query.Variable); ok {
		varPositions[v.Name] = 1
	}
	if v, ok := pattern.GetV().(query.Variable); ok {
		varPositions[v.Name] = 2
	}
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(query.Variable); ok {
			varPositions[v.Name] = 3
		}
	}

	for _, pc := range plannerConstraints {
		switch pc.Type {
		case planner.ConstraintEquality:
			// Position 2 (value) is the common case
			result = append(result, &equalityConstraint{
				position: 2,
				value:    pc.Value,
			})

		case planner.ConstraintRange:
			// Build range constraint based on operator
			rc := &rangeConstraint{position: 2}
			switch pc.Operator {
			case query.OpGT:
				rc.min = pc.Value
				rc.includeMin = false
			case query.OpGTE:
				rc.min = pc.Value
				rc.includeMin = true
			case query.OpLT:
				rc.max = pc.Value
				rc.includeMax = false
			case query.OpLTE:
				rc.max = pc.Value
				rc.includeMax = true
			}
			result = append(result, rc)

		case planner.ConstraintTimeExtraction:
			// Time extraction with expected value
			result = append(result, &timeExtractionConstraint{
				position:  2, // Value position for time
				extractFn: pc.TimeField,
				expected:  pc.Value,
			})
		}
	}

	return result
}

// equalSymbols checks if two symbol slices are equal
func equalSymbols(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
