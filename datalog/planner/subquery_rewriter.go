// Conditional Aggregate Rewriting
//
// This module implements an optimization that rewrites correlated aggregate subqueries
// as conditional aggregates to eliminate subquery execution overhead.
//
// WARNING: This optimization currently causes a 22.5x performance regression!
//
// The optimization is semantically correct but performs worse because:
//  1. It adds extra patterns to retrieve aggregate input values upfront
//  2. These patterns execute even when already bound (adding overhead)
//  3. The conditional filtering happens AFTER retrieving all data
//  4. For N subquery executions, targeted retrieval is often more efficient
//     than retrieving all data upfront and filtering
//
// Performance journey during implementation:
// - Initial: 174x slower (unbound patterns caused full table scans)
// - After deduplication: 20x slower (removed duplicate patterns)
// - After ordering fix: 22.5x slower (patterns execute when bound)
//
// Key lessons learned:
// - Not all SQL optimizations translate to Datalog
// - Pattern ordering by selectivity is critical
// - Retrieving all data upfront can be worse than targeted subqueries
// - The planner cache must account for optimization options (major bug fixed)
//
// The code is kept for reference and future improvements but should remain disabled.
package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// renamePatternVariables renames variables in a pattern according to a mapping
func renamePatternVariables(pat *query.DataPattern, varMap map[query.Symbol]query.Symbol) *query.DataPattern {
	renamed := &query.DataPattern{
		Elements: make([]query.PatternElement, len(pat.Elements)),
	}

	for i, elem := range pat.Elements {
		switch e := elem.(type) {
		case query.Variable:
			// Check if this variable should be renamed
			if newSym, ok := varMap[e.Name]; ok {
				renamed.Elements[i] = query.Variable{Name: newSym}
			} else {
				renamed.Elements[i] = elem
			}
		default:
			// Constants, etc. - keep as is
			renamed.Elements[i] = elem
		}
	}

	return renamed
}

// renameExpressionVariables renames variables in an expression according to a mapping
func renameExpressionVariables(expr *query.Expression, varMap map[query.Symbol]query.Symbol) *query.Expression {
	// Deep copy and rename
	renamed := &query.Expression{
		Function: expr.Function, // Will need to recursively rename inside functions
		Binding:  expr.Binding,
	}

	// Recursively rename variables in the function
	renamed.Function = renameFunctionVariables(expr.Function, varMap)

	return renamed
}

// renameFunctionVariables renames variables in a function
func renameFunctionVariables(fn query.Function, varMap map[query.Symbol]query.Symbol) query.Function {
	switch f := fn.(type) {
	case *query.TimeExtractionFunction:
		// Rename the argument
		return &query.TimeExtractionFunction{
			Field:    f.Field,
			TimeTerm: renameTermVariables(f.TimeTerm, varMap),
		}
	case query.TimeExtractionFunction:
		// Handle value type as well
		return query.TimeExtractionFunction{
			Field:    f.Field,
			TimeTerm: renameTermVariables(f.TimeTerm, varMap),
		}
	case *query.ComparisonFunction:
		return &query.ComparisonFunction{
			Comparison: &query.Comparison{
				Op:    f.Comparison.Op,
				Left:  renameTermVariables(f.Comparison.Left, varMap),
				Right: renameTermVariables(f.Comparison.Right, varMap),
			},
		}
	default:
		// For other function types, return as-is (may need to add more cases)
		return fn
	}
}

// renameTermVariables renames variables in a term
func renameTermVariables(term query.Term, varMap map[query.Symbol]query.Symbol) query.Term {
	switch t := term.(type) {
	case query.VariableTerm:
		if newSym, ok := varMap[t.Symbol]; ok {
			return query.VariableTerm{Symbol: newSym}
		}
		return t
	default:
		return term
	}
}

// extractExpressionInputs extracts all variables used in an expression
func extractExpressionInputs(expr *query.Expression) []query.Symbol {
	var inputs []query.Symbol
	seen := make(map[query.Symbol]bool)

	// Extract from function
	extractFromFunction(expr.Function, &inputs, seen)

	return inputs
}

// extractFromFunction extracts variables from a function
func extractFromFunction(fn query.Function, inputs *[]query.Symbol, seen map[query.Symbol]bool) {
	switch f := fn.(type) {
	case *query.TimeExtractionFunction:
		extractFromTerm(f.TimeTerm, inputs, seen)
	case query.TimeExtractionFunction:
		extractFromTerm(f.TimeTerm, inputs, seen)
	case *query.ComparisonFunction:
		extractFromTerm(f.Comparison.Left, inputs, seen)
		extractFromTerm(f.Comparison.Right, inputs, seen)
		// Add other function types as needed
	}
}

// extractFromTerm extracts variables from a term
func extractFromTerm(term query.Term, inputs *[]query.Symbol, seen map[query.Symbol]bool) {
	switch t := term.(type) {
	case query.VariableTerm:
		if !seen[t.Symbol] {
			*inputs = append(*inputs, t.Symbol)
			seen[t.Symbol] = true
		}
	}
}

// ConditionalAggregate pairs a subquery binding with its rewritten conditional aggregate
// This is used to pass rewriting information from planner to executor via phase metadata
type ConditionalAggregate struct {
	Binding   query.BindingForm
	Aggregate query.FindAggregate
}

// CorrelatedAggregatePattern represents a detected correlated aggregate subquery
// that can be rewritten as a conditional aggregate
type CorrelatedAggregatePattern struct {
	SubqueryIndex     int                  // Index in Phase.Subqueries
	SubqueryPlan      *SubqueryPlan        // The subquery plan
	Aggregate         query.FindAggregate  // The aggregate function
	InputParams       []query.Symbol       // :in parameters from outer query
	FilterPredicates  []FilterPredicate    // Equality predicates linking inner to outer
	FilterExpressions []int                // Indices of expressions producing filter vars
	SubqueryPatterns  []*query.DataPattern // Patterns from subquery to merge
}

// FilterPredicate represents an equality predicate that filters subquery results
// Example: [(= ?inner-var ?outer-param)]
type FilterPredicate struct {
	InnerVar     query.Symbol // Variable in subquery (e.g., ?py from [(year ?t) ?py])
	OuterParam   query.Symbol // Parameter from outer query (e.g., ?year)
	PredicateIdx int          // Index in subquery predicates
}

// detectCorrelatedAggregates finds correlated aggregate subqueries eligible for rewriting
func detectCorrelatedAggregates(phase *Phase) []CorrelatedAggregatePattern {
	var patterns []CorrelatedAggregatePattern

	// Find Subquery plans
	for subqIdx, subqPlan := range phase.Subqueries {
		// Check if eligible for rewriting
		if pattern, eligible := analyzeSubqueryForRewriting(subqIdx, &subqPlan); eligible {
			patterns = append(patterns, pattern)
		}
	}

	return patterns
}

// analyzeSubqueryForRewriting checks if a subquery matches the rewriting pattern
func analyzeSubqueryForRewriting(subqIdx int, subqPlan *SubqueryPlan) (CorrelatedAggregatePattern, bool) {
	pattern := CorrelatedAggregatePattern{
		SubqueryIndex: subqIdx,
		SubqueryPlan:  subqPlan,
	}

	q := subqPlan.Subquery.Query

	// 1. Must have exactly one element in :find clause
	if len(q.Find) != 1 {
		return pattern, false
	}

	// 2. Must be an aggregate function
	agg, ok := q.Find[0].(query.FindAggregate)
	if !ok {
		return pattern, false
	}
	pattern.Aggregate = agg

	// 3. Must have :in parameters (correlation)
	if len(q.In) <= 1 {
		// Only database input, no correlation
		return pattern, false
	}

	// Extract input parameters (skip first element which is database)
	for _, input := range q.In[1:] {
		if scalarInput, ok := input.(query.ScalarInput); ok {
			pattern.InputParams = append(pattern.InputParams, scalarInput.Symbol)
		} else {
			// Non-scalar inputs not supported for now
			return pattern, false
		}
	}

	// 4. Analyze WHERE clause to find filter predicates and patterns
	// We need to find equality predicates that compare subquery variables to input params
	filterPredicates, filterExpressions := findFilterPredicates(q, pattern.InputParams)
	if len(filterPredicates) == 0 {
		// No filter predicates means not a filterable correlation
		return pattern, false
	}
	pattern.FilterPredicates = filterPredicates
	pattern.FilterExpressions = filterExpressions

	// 5. Extract data patterns from subquery
	for _, clause := range q.Where {
		if dataPat, ok := clause.(*query.DataPattern); ok {
			pattern.SubqueryPatterns = append(pattern.SubqueryPatterns, dataPat)
		}
	}

	return pattern, true
}

// findFilterPredicates finds equality predicates that filter based on input parameters
// Example: [(= ?py ?y)] where ?y is an input parameter
func findFilterPredicates(q *query.Query, inputParams []query.Symbol) ([]FilterPredicate, []int) {
	var predicates []FilterPredicate
	var exprIndices []int

	// Build map of input parameters for quick lookup
	inputParamSet := make(map[query.Symbol]bool)
	for _, param := range inputParams {
		inputParamSet[param] = true
	}

	// Build map of expression bindings (e.g., [(year ?t) ?py] maps ?py to year expression)
	exprBindings := make(map[query.Symbol]int)
	for exprIdx, clause := range q.Where {
		if expr, ok := clause.(*query.Expression); ok {
			exprBindings[expr.Binding] = exprIdx
		}
	}

	// Find equality predicates comparing to input params
	for predIdx, clause := range q.Where {
		comp, ok := clause.(*query.Comparison)
		if !ok || comp.Op != query.OpEQ {
			continue
		}

		// Check if one side is a variable and other is an input parameter
		leftVar, leftIsVar := comp.Left.(query.VariableTerm)
		rightVar, rightIsVar := comp.Right.(query.VariableTerm)

		if leftIsVar && rightIsVar {
			// Both are variables - check if one is an input param
			if inputParamSet[rightVar.Symbol] {
				// Right side is input param, left side is inner var
				predicates = append(predicates, FilterPredicate{
					InnerVar:     leftVar.Symbol,
					OuterParam:   rightVar.Symbol,
					PredicateIdx: predIdx,
				})
				// Track expression that produces this inner var
				if exprIdx, exists := exprBindings[leftVar.Symbol]; exists {
					exprIndices = append(exprIndices, exprIdx)
				}
			} else if inputParamSet[leftVar.Symbol] {
				// Left side is input param, right side is inner var
				predicates = append(predicates, FilterPredicate{
					InnerVar:     rightVar.Symbol,
					OuterParam:   leftVar.Symbol,
					PredicateIdx: predIdx,
				})
				// Track expression that produces this inner var
				if exprIdx, exists := exprBindings[rightVar.Symbol]; exists {
					exprIndices = append(exprIndices, exprIdx)
				}
			}
		}
	}

	return predicates, exprIndices
}

// rewriteCorrelatedAggregates transforms detected patterns into conditional aggregates
// This is the main entry point for query rewriting
func rewriteCorrelatedAggregates(plan *QueryPlan, options PlannerOptions) error {
	// TODO: Remove DEBUG output once conditional aggregate rewriting is stable
	// (Keeping for now as feature was just completed - Oct 2025)
	fmt.Printf("DEBUG rewriteCorrelatedAggregates: called, enabled=%v, phases=%d\n", options.EnableConditionalAggregateRewriting, len(plan.Phases))

	// Process each phase
	for phaseIdx := range plan.Phases {
		phase := &plan.Phases[phaseIdx]

		// Detect patterns
		patterns := detectCorrelatedAggregates(phase)
		fmt.Printf("DEBUG rewriteCorrelatedAggregates: Phase %d has %d correlated aggregate patterns, %d subqueries\n", phaseIdx, len(patterns), len(phase.Subqueries))
		if len(patterns) == 0 {
			continue
		}

		// Rewrite each pattern (in reverse order to maintain indices)
		for i := len(patterns) - 1; i >= 0; i-- {
			fmt.Printf("DEBUG rewriteCorrelatedAggregates: Rewriting pattern %d in phase %d\n", i, phaseIdx)
			if err := rewritePattern(phase, &patterns[i]); err != nil {
				return err
			}
		}
	}

	// After rewriting, move expressions to correct phases based on dependencies
	// This may create new phases if needed
	fixedPhases, err := fixExpressionPlacement(plan.Phases)
	if err != nil {
		return err
	}
	plan.Phases = fixedPhases

	return nil
}

// fixExpressionPlacement moves expressions to the earliest phase where all their dependencies are satisfied
// This is needed after conditional aggregate rewriting because the rewriter may add expressions to phases
// before their input dependencies are available. Returns the modified phases slice (may have new phases added).
func fixExpressionPlacement(phases []Phase) ([]Phase, error) {
	// Build a map of which phase provides each symbol
	symbolPhase := make(map[query.Symbol]int)

	// Track which symbols are available at the start of each phase
	for phaseIdx := range phases {
		// Add symbols from Available (input parameters + previous phase outputs)
		for _, sym := range phases[phaseIdx].Available {
			if _, exists := symbolPhase[sym]; !exists {
				symbolPhase[sym] = -1 // Available from start (input parameter)
			}
		}

		// Add symbols from pattern Provides
		for _, sym := range phases[phaseIdx].Provides {
			symbolPhase[sym] = phaseIdx
		}

		// Add ALL variables from patterns (E, A, V positions)
		for _, patternPlan := range phases[phaseIdx].Patterns {
			if dp, ok := patternPlan.Pattern.(*query.DataPattern); ok {
				for _, elem := range dp.Elements {
					if v, ok := elem.(query.Variable); ok {
						if _, exists := symbolPhase[v.Name]; !exists {
							symbolPhase[v.Name] = phaseIdx
						}
					}
				}
			}
		}

		// Add symbols from expressions (in phase order)
		for _, expr := range phases[phaseIdx].Expressions {
			if expr.Output != "" {
				symbolPhase[expr.Output] = phaseIdx
			}
		}
	}

	// Now check each expression and move it if needed
	moved := true
	for moved {
		moved = false

		for phaseIdx := 0; phaseIdx < len(phases); phaseIdx++ {
			i := 0
			for i < len(phases[phaseIdx].Expressions) {
				expr := phases[phaseIdx].Expressions[i]

				// Find the minimum phase where all inputs are available
				minPhase := 0
				for _, input := range expr.Inputs {
					if providerPhase, ok := symbolPhase[input]; ok {
						if providerPhase >= minPhase {
							minPhase = providerPhase + 1 // Need to be AFTER provider phase
						}
					} else {
						// Input not provided by any phase - might be an input parameter
						// Check if it's in Available at start
						found := false
						for _, availSym := range phases[0].Available {
							if availSym == input {
								found = true
								break
							}
						}
						if !found {
							return nil, fmt.Errorf("expression output %s depends on unavailable symbol %s", expr.Output, input)
						}
					}
				}

				// If expression is in earlier phase than its dependencies, move it
				if minPhase > phaseIdx {
					// Remove from current phase
					phases[phaseIdx].Expressions = append(
						phases[phaseIdx].Expressions[:i],
						phases[phaseIdx].Expressions[i+1:]...,
					)

					// Create new phases if needed
					for len(phases) <= minPhase {
						newPhase := Phase{
							Patterns:    []PatternPlan{},
							Predicates:  []PredicatePlan{},
							Expressions: []ExpressionPlan{},
							Subqueries:  []SubqueryPlan{},
							Provides:    []query.Symbol{},
							Keep:        []query.Symbol{},
							Available:   []query.Symbol{},
						}
						phases = append(phases, newPhase)
					}

					// Add to target phase
					phases[minPhase].Expressions = append(phases[minPhase].Expressions, expr)
					// Update symbol tracking
					symbolPhase[expr.Output] = minPhase
					moved = true
				} else {
					i++
				}
			}
		}
	}

	// After moving expressions, recalculate Provides from patterns and expressions
	for phaseIdx := range phases {
		providesSet := make(map[query.Symbol]bool)

		// Add variables from ALL pattern elements (E, A, V positions)
		for _, patternPlan := range phases[phaseIdx].Patterns {
			if dp, ok := patternPlan.Pattern.(*query.DataPattern); ok {
				for _, elem := range dp.Elements {
					if v, ok := elem.(query.Variable); ok {
						providesSet[v.Name] = true
					}
				}
			}
		}

		// Add expression outputs
		for _, expr := range phases[phaseIdx].Expressions {
			if expr.Output != "" {
				providesSet[expr.Output] = true
			}
		}

		// Convert back to slice
		provides := make([]query.Symbol, 0, len(providesSet))
		for sym := range providesSet {
			provides = append(provides, sym)
		}
		phases[phaseIdx].Provides = provides
	}

	// After recalculating Provides, move metadata to the correct phase
	// The aggregate_required_columns metadata needs to be on the phase that actually
	// produces all the required columns, not the original phase where rewriting happened
	if err := fixAggregateMetadata(phases, symbolPhase); err != nil {
		return nil, err
	}

	return phases, nil
}

// fixAggregateMetadata moves aggregate_required_columns metadata to the phase
// that actually produces all the required columns
func fixAggregateMetadata(phases []Phase, symbolPhase map[query.Symbol]int) error {
	// Find phases with aggregate_required_columns metadata
	for phaseIdx := range phases {
		if phases[phaseIdx].Metadata == nil {
			continue
		}

		reqCols, ok := phases[phaseIdx].Metadata["aggregate_required_columns"].([]query.Symbol)
		if !ok || len(reqCols) == 0 {
			continue
		}

		// Find the latest phase where any required column is produced
		// The metadata should be on the phase that produces the last required column
		latestPhase := phaseIdx
		for _, col := range reqCols {
			if providerPhase, exists := symbolPhase[col]; exists {
				// Symbol is provided by a phase - metadata should be on that phase or later
				if providerPhase > latestPhase {
					latestPhase = providerPhase
				}
			} else {
				// Check if it's an input parameter (phase -1)
				found := false
				if len(phases) > 0 {
					for _, availSym := range phases[0].Available {
						if availSym == col {
							found = true
							break
						}
					}
				}
				if !found {
					return fmt.Errorf("aggregate required column %s not found in any phase", col)
				}
			}
		}

		// If metadata needs to move to a later phase, move it
		if latestPhase > phaseIdx {
			// Ensure target phase has metadata map
			if phases[latestPhase].Metadata == nil {
				phases[latestPhase].Metadata = make(map[string]interface{})
			}

			// Move the metadata
			phases[latestPhase].Metadata["aggregate_required_columns"] = reqCols

			// Remove from original phase
			delete(phases[phaseIdx].Metadata, "aggregate_required_columns")
		}
	}

	return nil
}

// rewritePattern transforms a single correlated aggregate pattern
// This modifies the query plan to eliminate the subquery and add a conditional aggregate
func rewritePattern(phase *Phase, pattern *CorrelatedAggregatePattern) error {
	q := pattern.SubqueryPlan.Subquery.Query

	// Step 0: Create mapping from subquery input params to outer query variables
	// The subquery has `:in $ ?person ?d` and the pattern uses `q` with inputs `$ ?p ?day`
	// We need to map ?person -> ?p, ?d -> ?day
	varMap := make(map[query.Symbol]query.Symbol)

	// Extract subquery parameter names from :in clause (skip database input)
	// Also skip database in the pattern inputs
	subqInParams := []query.Symbol{}
	for _, input := range q.In[1:] { // Skip database input in subquery
		if scalarInput, ok := input.(query.ScalarInput); ok {
			subqInParams = append(subqInParams, scalarInput.Symbol)
		}
	}

	// Pattern.SubqueryPlan.Inputs might include $ - filter it out
	outerInputs := []query.Symbol{}
	for _, sym := range pattern.SubqueryPlan.Inputs {
		if sym != "$" {
			outerInputs = append(outerInputs, sym)
		}
	}

	// Now map subquery params to outer inputs
	for i, subqParam := range subqInParams {
		if i < len(outerInputs) {
			varMap[subqParam] = outerInputs[i]
		}
	}

	// Additionally, we need to unify subquery entity variables with outer query entity variables
	// If the subquery accesses the same relationship (e.g., [?ev :event/person ?p]) as the outer query
	// (e.g., [?e :event/person ?p]), then ?ev should map to ?e
	//
	// Look for patterns in the phase that have the same attribute and bound value as subquery patterns
	for _, sqPat := range pattern.SubqueryPatterns {
		// Check if this is [?subq-entity :attr ?bound-value] where ?bound-value is already mapped
		if len(sqPat.Elements) == 3 {
			if sqEntity, ok := sqPat.Elements[0].(query.Variable); ok {
				if attr, ok := sqPat.Elements[1].(query.Constant); ok {
					if sqValue, ok := sqPat.Elements[2].(query.Variable); ok {
						// Check if sqValue is mapped (it's a correlated parameter)
						if outerValue, isMapped := varMap[sqValue.Name]; isMapped {
							// Now find outer pattern with same attribute and value
							for _, outerPat := range phase.Patterns {
								if dp, ok := outerPat.Pattern.(*query.DataPattern); ok && len(dp.Elements) == 3 {
									if outerAttr, ok := dp.Elements[1].(query.Constant); ok && outerAttr.Value == attr.Value {
										if outerVal, ok := dp.Elements[2].(query.Variable); ok && outerVal.Name == outerValue {
											if outerEntity, ok := dp.Elements[0].(query.Variable); ok {
												// Found match! Map subquery entity to outer entity
												if _, alreadyMapped := varMap[sqEntity.Name]; !alreadyMapped {
													varMap[sqEntity.Name] = outerEntity.Name
												}
											}
										}
									}
								}
							}
						} else {
							// sqValue is NOT mapped (it's a subquery-local variable)
							// Check if there's an outer pattern with the same entity and attribute
							// If so, map the subquery value variable to the outer value variable
							for _, outerPat := range phase.Patterns {
								if dp, ok := outerPat.Pattern.(*query.DataPattern); ok && len(dp.Elements) == 3 {
									if outerAttr, ok := dp.Elements[1].(query.Constant); ok && outerAttr.Value == attr.Value {
										if outerEntity, ok := dp.Elements[0].(query.Variable); ok {
											// Check if sqEntity will be mapped to outerEntity
											finalSqEntity := sqEntity.Name
											if mapped, ok := varMap[sqEntity.Name]; ok {
												finalSqEntity = mapped
											}
											if finalSqEntity == outerEntity.Name {
												// Same entity and attribute! Map values
												if outerVal, ok := dp.Elements[2].(query.Variable); ok {
													if _, alreadyMapped := varMap[sqValue.Name]; !alreadyMapped {
														varMap[sqValue.Name] = outerVal.Name
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Identify unmapped variables from subquery patterns
	// These are LOCAL to the inlined subquery and don't need outer mappings
	unmappedVars := make(map[query.Symbol]bool)
	for _, sqPat := range pattern.SubqueryPatterns {
		for _, elem := range sqPat.Elements {
			if v, ok := elem.(query.Variable); ok {
				// If this variable isn't mapped, it's subquery-local
				if _, mapped := varMap[v.Name]; !mapped {
					unmappedVars[v.Name] = true
				}
			}
		}
	}

	// Step 1: Merge ONLY NEW patterns from subquery into outer phase
	// Build set of existing patterns for quick lookup
	existingPatterns := make(map[string]bool)
	for _, pp := range phase.Patterns {
		existingPatterns[pp.Pattern.String()] = true
	}

	// Only add patterns that don't already exist
	newPatterns := []PatternPlan{}
	for _, dataPat := range pattern.SubqueryPatterns {
		renamedPat := renamePatternVariables(dataPat, varMap)
		patternStr := renamedPat.String()
		if !existingPatterns[patternStr] {
			// This is a genuinely new pattern needed for the aggregate
			newPatterns = append(newPatterns, PatternPlan{
				Pattern:     renamedPat,
				Selectivity: 999999, // Very high selectivity so it executes AFTER bound patterns
			})
		}
	}

	// Append new patterns AFTER existing ones so they execute when variables are bound
	phase.Patterns = append(phase.Patterns, newPatterns...)

	// Step 2: Merge filter expressions into outer phase
	// These are expressions like [(year ?t) ?py] that produce the filter variables
	for _, exprIdx := range pattern.FilterExpressions {
		if exprIdx >= 0 && exprIdx < len(q.Where) {
			if expr, ok := q.Where[exprIdx].(*query.Expression); ok {
				// Rename variables in the expression according to the mapping
				renamedExpr := renameExpressionVariables(expr, varMap)

				// Extract inputs from the expression
				inputs := extractExpressionInputs(renamedExpr)

				phase.Expressions = append(phase.Expressions, ExpressionPlan{
					Expression: renamedExpr,
					Inputs:     inputs,
					Output:     renamedExpr.Binding, // CRITICAL: Set the output symbol so planner knows what this expression produces
				})
			}
		}
	}

	// Step 3: Synthesize filter predicate from equality comparisons
	// Create [(= ?inner ?outer) ?condN] for each filter predicate
	filterVars := []query.Symbol{}

	for _, filterPred := range pattern.FilterPredicates {
		// Apply variable mapping to inner and outer variables
		innerVar := filterPred.InnerVar
		outerVar := filterPred.OuterParam
		if mapped, ok := varMap[innerVar]; ok {
			innerVar = mapped
		}
		if mapped, ok := varMap[outerVar]; ok {
			outerVar = mapped
		}

		// Create a unique condition variable name based on the RENAMED inner variable
		condVar := query.Symbol("?__cond_" + string(innerVar))
		filterVars = append(filterVars, condVar)

		// Create the equality comparison with renamed variables
		comparison := &query.Comparison{
			Op:    query.OpEQ,
			Left:  query.VariableTerm{Symbol: innerVar},
			Right: query.VariableTerm{Symbol: outerVar},
		}

		// Wrap comparison as a function so it can be used in expression binding
		compFunc := &query.ComparisonFunction{Comparison: comparison}

		expr := &query.Expression{
			Function: compFunc,
			Binding:  condVar,
		}

		// Extract inputs
		inputs := extractExpressionInputs(expr)

		// Add as an expression binding
		phase.Expressions = append(phase.Expressions, ExpressionPlan{
			Expression: expr,
			Inputs:     inputs,
			Output:     condVar, // CRITICAL: Set the output symbol so planner knows this expression produces condVar
		})
	}

	// Step 4: Combine multiple conditions with AND (if needed)
	var filterSymbol query.Symbol
	if len(filterVars) == 0 {
		// No filter predicates - this shouldn't happen if detection worked correctly
		return fmt.Errorf("no filter predicates found for pattern")
	} else if len(filterVars) == 1 {
		// Single condition - use it directly
		filterSymbol = filterVars[0]
	} else {
		// Multiple conditions - combine with AND
		filterSymbol = query.Symbol("?__filter_combined")
		andFunc := &query.AndFunction{Terms: filterVars}

		phase.Expressions = append(phase.Expressions, ExpressionPlan{
			Expression: &query.Expression{
				Function: andFunc,
				Binding:  filterSymbol,
			},
			Output: filterSymbol, // CRITICAL: Set the output symbol so planner knows this expression produces filterSymbol
		})
	}

	// Step 5: Create the conditional aggregate
	// Mark the aggregate with the filter predicate
	conditionalAgg := pattern.Aggregate
	conditionalAgg.Predicate = filterSymbol

	// Store the conditional aggregate in phase metadata
	// The executor will use this when processing aggregations
	if phase.Metadata == nil {
		phase.Metadata = make(map[string]interface{})
	}

	// Map the subquery's result binding to the conditional aggregate
	// Store as a slice since we need to preserve order and handle multiple aggregates
	var condAggs []ConditionalAggregate
	if existing, ok := phase.Metadata["conditional_aggregates"]; ok {
		condAggs = existing.([]ConditionalAggregate)
	}

	condAgg := ConditionalAggregate{
		Binding:   pattern.SubqueryPlan.Subquery.Binding,
		Aggregate: conditionalAgg,
	}
	condAggs = append(condAggs, condAgg)

	phase.Metadata["conditional_aggregates"] = condAggs

	// Step 6: Mark the subquery as rewritten (to prevent execution)
	phase.Subqueries[pattern.SubqueryIndex].Decorrelated = true

	// Step 7: Update phase.Provides
	// Remove subquery binding variables (these will be produced by aggregation)
	// But ADD aggregate input variables (these are needed for aggregation)

	var bindingVars []query.Symbol
	switch b := pattern.SubqueryPlan.Subquery.Binding.(type) {
	case query.TupleBinding:
		bindingVars = b.Variables
	case query.CollectionBinding:
		bindingVars = []query.Symbol{b.Variable}
	case query.RelationBinding:
		bindingVars = b.Variables
	}

	// Collect aggregate input variables (e.g., ?v from (max ?v))
	// CRITICAL: Use mapped name if mapped, otherwise keep original (subquery-local) name
	aggregateInputs := make(map[query.Symbol]bool)
	if pattern.Aggregate.Arg != "" {
		aggVar := pattern.Aggregate.Arg
		if mapped, ok := varMap[aggVar]; ok {
			// Variable was mapped to outer query variable
			aggVar = mapped
		}
		aggregateInputs[aggVar] = true

		// Update the conditional aggregate to use the correct variable name
		conditionalAgg.Arg = aggVar
	}

	// Also need the predicate variable (use filterSymbol which was computed earlier)
	if filterSymbol != "" {
		aggregateInputs[filterSymbol] = true
	}

	// Remove binding variables, add aggregate inputs
	newProvides := make([]query.Symbol, 0, len(phase.Provides))
	for _, sym := range phase.Provides {
		keep := true
		for _, bindingVar := range bindingVars {
			if sym == bindingVar {
				keep = false
				break
			}
		}
		if keep {
			newProvides = append(newProvides, sym)
		}
	}

	// Add aggregate inputs if not already present
	for aggInput := range aggregateInputs {
		found := false
		for _, sym := range newProvides {
			if sym == aggInput {
				found = true
				break
			}
		}
		if !found {
			newProvides = append(newProvides, aggInput)
		}
	}

	phase.Provides = newProvides

	// ALSO update phase.Keep to include aggregate inputs
	// Keep is used for projection, and we need the aggregate variables to be available

	// Build new Keep (start from existing or from Provides if empty)
	newKeep := make([]query.Symbol, 0)
	if len(phase.Keep) > 0 {
		for _, sym := range phase.Keep {
			// Remove binding variables from Keep
			keep := true
			for _, bindingVar := range bindingVars {
				if sym == bindingVar {
					keep = false
					break
				}
			}
			if keep {
				newKeep = append(newKeep, sym)
			}
		}
	} else {
		// Keep is empty, use Provides as base (excluding binding vars)
		for _, sym := range newProvides {
			newKeep = append(newKeep, sym)
		}
	}

	// Add aggregate inputs to Keep
	for aggInput := range aggregateInputs {
		found := false
		for _, sym := range newKeep {
			if sym == aggInput {
				found = true
				break
			}
		}
		if !found {
			newKeep = append(newKeep, aggInput)
		}
	}

	phase.Keep = newKeep

	// Store the required columns for aggregation in metadata
	// The executor will use this to override Keep projection
	requiredCols := make([]query.Symbol, 0)
	for aggInput := range aggregateInputs {
		requiredCols = append(requiredCols, aggInput)
	}
	phase.Metadata["aggregate_required_columns"] = requiredCols

	return nil
}
