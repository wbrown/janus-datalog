package executor

import (
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
// constantBindings are pre-resolved scalar values that are not present as relation symbols.
func filterWithPredicateAndLookup(rel Relation, pred query.Predicate, lookup query.EntityLookup, constantBindings map[query.Symbol]interface{}) (result Relation) {
	symbols := rel.Symbols()
	needsCopy := rel.RequiresCopy()

	// Pre-allocate filtered only for materialized relations to avoid forcing materialization
	var filtered []Tuple
	if _, ok := rel.(*MaterializedRelation); ok {
		if size := rel.Size(); size >= 0 {
			filtered = make([]Tuple, 0, size)
		}
	}

	// Reuse single bindings map to avoid repeated allocations
	bindings := make(map[query.Symbol]interface{}, len(symbols)+len(constantBindings))

	// Check if this is a DatabaseFunctionPredicate that needs lookup
	dbFuncPred, isDbFuncPred := pred.(*query.DatabaseFunctionPredicate)

	// Failed iteration or evaluation surfaces via result.err — replayed at the
	// next public boundary by Iterator().Error(). Named return + closure so the
	// deferred Close() also runs on panic (predicate Eval is user-supplied code
	// and can panic), without losing the Close error.
	var iterErr error
	iter := rel.Iterator()
	defer func() {
		if closeErr := iter.Close(); closeErr != nil && iterErr == nil {
			iterErr = closeErr
		}
		// Panic path: result is nil; iter.Close above still ran.
		if iterErr == nil || result == nil {
			return
		}
		if m, ok := result.(*MaterializedRelation); ok && m.err == nil {
			m.err = iterErr
		}
	}()

	for iter.Next() {
		tuple := iter.Tuple()

		// Clear and populate bindings map
		for k := range bindings {
			delete(bindings, k)
		}
		// Pre-populate with constant bindings
		for sym, val := range constantBindings {
			bindings[sym] = val
		}
		for i, sym := range symbols {
			bindings[sym] = tuple[i]
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
			// Fail fast — predicate eval errors are real errors, not
			// "treat as false." Surface to the consumer; do not silently
			// drop the tuple as if the predicate had said no.
			iterErr = err
			break
		}

		if passes {
			if needsCopy {
				tuple = copyTuple(tuple)
			}
			filtered = append(filtered, tuple)
		}
	}
	if iterErr == nil {
		iterErr = iter.Error()
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	result = NewMaterializedRelationWithProperties(symbols, filtered, opts, rel.Properties())
	return
}

// evaluateExpressionWithLookup evaluates an expression with optional database lookup support.
// If lookup is non-nil and the expression is a DatabaseFunction, it uses EvalWithLookup.
// Otherwise, it falls back to the standard Eval method.
// constantBindings are pre-resolved scalar values that are not present as relation symbols.
func evaluateExpressionWithLookup(rel Relation, expr *query.Expression, lookup query.EntityLookup, constantBindings map[query.Symbol]interface{}) (result Relation) {
	symbols := rel.Symbols()

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

	// Check which binding symbols already exist
	hasAllBindings := len(bindingSymbols) > 0
	existingBindingIndices := make(map[query.Symbol]int)
	for _, bindSym := range bindingSymbols {
		if i := query.SymbolIndex(symbols, bindSym); i >= 0 {
			existingBindingIndices[bindSym] = i
		} else {
			hasAllBindings = false
		}
	}

	newSymbols := symbols
	if !hasAllBindings && len(bindingSymbols) > 0 {
		newSymbols = append([]query.Symbol{}, symbols...)
		for _, bindSym := range bindingSymbols {
			if _, exists := existingBindingIndices[bindSym]; !exists {
				newSymbols = append(newSymbols, bindSym)
			}
		}
	}

	// Reuse single bindings map to avoid repeated allocations
	bindings := make(map[query.Symbol]interface{}, len(symbols)+len(constantBindings))

	// Pre-allocate newTuples only for materialized relations to avoid forcing materialization
	var newTuples []Tuple
	if _, ok := rel.(*MaterializedRelation); ok {
		if size := rel.Size(); size >= 0 {
			newTuples = make([]Tuple, 0, size)
		}
	}

	// Failed iteration or evaluation surfaces via result.err — replayed at the
	// next public boundary by Iterator().Error(). Named return + closure so the
	// deferred Close() also runs on panic (expression Function.Eval is
	// user-supplied code and can panic), without losing the Close error.
	var iterErr error
	expanded := false
	iter := rel.Iterator()
	defer func() {
		if closeErr := iter.Close(); closeErr != nil && iterErr == nil {
			iterErr = closeErr
		}
		if iterErr == nil || result == nil {
			return
		}
		if m, ok := result.(*MaterializedRelation); ok && m.err == nil {
			m.err = iterErr
		}
	}()

	for iter.Next() {
		tuple := iter.Tuple()

		// Clear and populate bindings map
		for k := range bindings {
			delete(bindings, k)
		}
		// Pre-populate with constant bindings
		for sym, val := range constantBindings {
			bindings[sym] = val
		}
		for i, sym := range symbols {
			bindings[sym] = tuple[i]
		}

		// Evaluate the expression
		// Check if this is a database function that needs lookup access
		var evalResult interface{}
		var err error
		if dbFunc, ok := expr.Function.(query.DatabaseFunction); ok && lookup != nil {
			evalResult, err = dbFunc.EvalWithLookup(bindings, lookup)
		} else {
			evalResult, err = expr.Function.Eval(bindings)
		}
		if err != nil {
			// Fail fast — expression eval errors are real errors. Surface
			// to the consumer; do not silently drop the tuple.
			iterErr = err
			break
		}

		// Extract value from GetSomeResult if needed
		// get-some returns a struct with Attr, Value, and Found; we just want
		// the Value for binding. Found=false signals "no attribute matched":
		// drop this tuple without surfacing an error.
		if gsr, ok := evalResult.(*query.GetSomeResult); ok {
			if !gsr.Found {
				continue
			}
			evalResult = gsr.Value
		}

		if err := admitExpressionResult(expr.Function, evalResult); err != nil {
			iterErr = err
			break
		}

		// Handle multi-tuple expansion (e.g., enumerate returns [][]interface{})
		if multiRows, ok := evalResult.([][]interface{}); ok {
			if tb, ok := expr.Binding.(query.TupleBinding); ok {
				expanded = true
				for _, subTuple := range multiRows {
					if len(subTuple) != len(tb.Variables) {
						continue
					}
					if hasAllBindings {
						newTuple := make(Tuple, len(tuple))
						copy(newTuple, tuple)
						for i, bindSym := range tb.Variables {
							if idx, exists := existingBindingIndices[bindSym]; exists {
								newTuple[idx] = subTuple[i]
							}
						}
						newTuples = append(newTuples, newTuple)
					} else {
						newTuple := make(Tuple, len(newSymbols))
						copy(newTuple, tuple)
						for i, bindSym := range tb.Variables {
							for j := len(symbols); j < len(newSymbols); j++ {
								if newSymbols[j] == bindSym {
									newTuple[j] = subTuple[i]
									break
								}
							}
						}
						newTuples = append(newTuples, newTuple)
					}
				}
				continue
			}
		}

		// Create new tuple with result
		if len(bindingSymbols) == 0 {
			// No binding, just keep original tuple
			newTuples = append(newTuples, tuple)
		} else if hasAllBindings {
			// Update existing symbols
			newTuple := make(Tuple, len(tuple))
			copy(newTuple, tuple)
			// Handle tuple binding - evalResult should be []interface{}
			if tb, ok := expr.Binding.(query.TupleBinding); ok {
				values, ok := evalResult.([]interface{})
				if ok && len(values) == len(tb.Variables) {
					for i, bindSym := range tb.Variables {
						if idx, exists := existingBindingIndices[bindSym]; exists {
							newTuple[idx] = values[i]
						}
					}
				}
			} else {
				// Scalar binding
				for i, sym := range symbols {
					if bindSym, ok := expr.Binding.(query.Symbol); ok && sym == bindSym {
						newTuple[i] = evalResult
						break
					}
				}
			}
			newTuples = append(newTuples, newTuple)
		} else {
			// Add new symbols
			newTuple := make(Tuple, len(newSymbols))
			copy(newTuple, tuple)
			// Handle tuple binding - evalResult should be []interface{}
			if tb, ok := expr.Binding.(query.TupleBinding); ok {
				values, ok := evalResult.([]interface{})
				if ok && len(values) == len(tb.Variables) {
					for i, bindSym := range tb.Variables {
						// Find the position of this symbol in newSymbols.
						for j := len(symbols); j < len(newSymbols); j++ {
							if newSymbols[j] == bindSym {
								newTuple[j] = values[i]
								break
							}
						}
					}
				}
			} else {
				// Scalar binding - add to end
				newTuple[len(tuple)] = evalResult
			}
			newTuples = append(newTuples, newTuple)
		}
	}
	if iterErr == nil {
		iterErr = iter.Error()
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	properties := rel.Properties()
	if expanded {
		properties = expansionProperties(properties, bindingSymbols, newSymbols)
	} else {
		for _, symbol := range bindingSymbols {
			properties = properties.addSymbol(symbol)
		}
	}
	result = NewMaterializedRelationWithProperties(newSymbols, newTuples, opts, properties)
	return
}

// projectToSymbols projects a relation to the specified symbols
func projectToSymbols(rel Relation, syms []query.Symbol, opts ExecutorOptions) (result Relation) {
	relSyms := rel.Symbols()

	// Build symbol index mapping
	symIndices := query.SymbolIndexTable(relSyms, syms)
	for _, idx := range symIndices {
		if idx < 0 {
			// Symbol not found - return empty relation
			return NewMaterializedRelationWithOptions(syms, nil, opts)
		}
	}

	// Failed iteration surfaces via result.err — replayed at the next public
	// boundary by Iterator().Error(). Named return + closure so iter.Close runs
	// on panic without losing the Close error.
	var iterErr error
	var projected []Tuple
	iter := rel.Iterator()
	defer func() {
		if closeErr := iter.Close(); closeErr != nil && iterErr == nil {
			iterErr = closeErr
		}
		if iterErr == nil || result == nil {
			return
		}
		if m, ok := result.(*MaterializedRelation); ok && m.err == nil {
			m.err = iterErr
		}
	}()

	for iter.Next() {
		tuple := iter.Tuple()
		newTuple := make(Tuple, len(syms))
		for i, idx := range symIndices {
			newTuple[i] = tuple[idx]
		}
		projected = append(projected, newTuple)
	}
	if e := iter.Error(); iterErr == nil {
		iterErr = e
	}

	result = NewMaterializedRelationWithOptions(syms, projected, opts)
	return
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
		case *query.OrDefaultClause:
			for _, branch := range c.Branches {
				for _, sym := range collectInnerVars(branch) {
					if !seen[sym] {
						seen[sym] = true
						vars = append(vars, sym)
					}
				}
			}
		case *query.OrDefaultJoinClause:
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

// getUniqueCombinations extracts unique value combinations for the given symbols.
func getUniqueCombinations(rel Relation, syms []query.Symbol) (combos []Tuple, resultErr error) {
	if rel == nil || len(syms) == 0 {
		return nil, nil
	}

	relSyms := rel.Symbols()
	symIndices := query.SymbolIndexTable(relSyms, syms)
	for _, idx := range symIndices {
		if idx < 0 {
			return nil, nil
		}
	}

	seen := NewTupleKeyMap()
	iter := rel.Iterator()
	defer func() {
		if closeErr := iter.Close(); resultErr == nil {
			resultErr = closeErr
		}
	}()
	for iter.Next() {
		tuple := iter.Tuple()
		combo := make(Tuple, len(symIndices))
		for i, idx := range symIndices {
			combo[i] = tuple[idx]
		}
		key := NewTupleKeyFull(combo)
		if !seen.PutIfAbsent(key, struct{}{}) {
			combos = append(combos, combo)
		}
	}
	if e := iter.Error(); resultErr == nil {
		resultErr = e
	}
	return combos, resultErr
}

// countOverlap counts how many symbols from targetSyms are present in refSyms
func countOverlap(refSyms, targetSyms []query.Symbol) int {
	symSet := make(map[query.Symbol]bool, len(refSyms))
	for _, sym := range refSyms {
		symSet[sym] = true
	}
	count := 0
	for _, sym := range targetSyms {
		if symSet[sym] {
			count++
		}
	}
	return count
}

// unionRelations creates a union of multiple relations, projecting to common symbols
func unionRelations(relations []Relation, syms []query.Symbol, opts ExecutorOptions) Relation {
	if len(relations) == 0 {
		return NewMaterializedRelationWithOptions(syms, nil, opts)
	}

	seen := NewTupleKeyMap()
	var allTuples []Tuple
	var firstErr error

	for _, rel := range relations {
		// Build symbol index mapping
		symIndices := query.SymbolIndexTable(rel.Symbols(), syms)
		valid := true
		for _, idx := range symIndices {
			if idx < 0 {
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
			projected := make(Tuple, len(syms))
			for i, idx := range symIndices {
				projected[i] = tuple[idx]
			}
			key := NewTupleKeyFull(projected)
			if !seen.PutIfAbsent(key, struct{}{}) {
				allTuples = append(allTuples, projected)
			}
		}
		// Capture a branch's deferred scan error so the union doesn't drop it.
		branchErr := iter.Error()
		if closeErr := iter.Close(); branchErr == nil {
			branchErr = closeErr
		}
		if branchErr != nil && firstErr == nil {
			firstErr = branchErr
		}
	}

	result := newMaterializedRelationFromSet(
		syms,
		allTuples,
		opts,
		deduplicatedProperties(syms),
	)
	result.err = firstErr
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
