package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// assignExpressionsToPhases assigns expressions to the earliest phase where their inputs are available
func (p *Planner) assignExpressionsToPhases(phases []Phase, expressions []*query.Expression, predicates []query.Predicate) {
	if len(phases) == 0 {
		return
	}

	// Clear existing expression assignments (needed when called after phase reordering)
	// Also remove expression outputs from Provides since we'll recalculate them
	for i := range phases {
		phases[i].Expressions = nil

		// Remove expression-generated symbols from Provides
		// Keep only symbols from patterns
		var newProvides []query.Symbol
		for _, sym := range phases[i].Provides {
			// Check if this symbol is an expression output
			isExprOutput := false
			for _, expr := range expressions {
				if expr.Binding == sym {
					isExprOutput = true
					break
				}
			}
			if !isExprOutput {
				newProvides = append(newProvides, sym)
			}
		}
		phases[i].Provides = newProvides
	}

	// Track which expressions have been assigned
	assigned := make(map[*query.Expression]bool)

	// For each expression, find the earliest phase where all inputs are available
	for _, expr := range expressions {
		// Extract input variables from the expression
		inputs := p.extractExpressionInputs(expr)

		// Find the earliest phase where all inputs are available
		for i := range phases {
			// Check what symbols are available at this phase
			available := make(map[query.Symbol]bool)

			// Add symbols from previous phases
			for _, sym := range phases[i].Available {
				available[sym] = true
			}

			// Add symbols provided by this phase
			for _, sym := range phases[i].Provides {
				available[sym] = true
			}

			// Check if all inputs are available
			allAvailable := true
			for _, input := range inputs {
				if !available[input] {
					allAvailable = false
					break
				}
			}

			if allAvailable {
				// This expression can be evaluated in this phase
				isEquality := expr.Binding == "" // No binding means equality check

				phases[i].Expressions = append(phases[i].Expressions, ExpressionPlan{
					Expression: expr,
					Inputs:     inputs,
					Output:     expr.Binding,
					IsEquality: isEquality,
				})

				// If this expression produces a binding, add it to the phase's provides
				if expr.Binding != "" {
					phases[i].Provides = append(phases[i].Provides, expr.Binding)
				}

				assigned[expr] = true
				break
			}
		}

		// If we couldn't assign the expression to any phase, add it to the last phase
		if !assigned[expr] {
			lastIdx := len(phases) - 1
			isEquality := expr.Binding == ""

			phases[lastIdx].Expressions = append(phases[lastIdx].Expressions, ExpressionPlan{
				Expression: expr,
				Inputs:     inputs,
				Output:     expr.Binding,
				IsEquality: isEquality,
			})

			if expr.Binding != "" {
				phases[lastIdx].Provides = append(phases[lastIdx].Provides, expr.Binding)
			}
		}
	}

	// Update Available lists for phases to include expression outputs from previous phases
	allAvailable := make(map[query.Symbol]bool)
	for i := range phases {
		// Add symbols from this phase's original Available
		for _, sym := range phases[i].Available {
			allAvailable[sym] = true
		}

		// Add symbols provided by this phase (including expressions)
		for _, sym := range phases[i].Provides {
			allAvailable[sym] = true
		}

		// Update next phase's Available if there is one
		if i+1 < len(phases) {
			phases[i+1].Available = p.getResolvedSymbols(allAvailable)
		}
	}

	// Clear existing predicates from all phases and reassign them with expression outputs considered
	allPredicates := make([]query.Predicate, 0)

	// Collect all predicates from phases
	for i := range phases {
		for _, predPlan := range phases[i].Predicates {
			allPredicates = append(allPredicates, predPlan.Predicate)
		}
		// Clear predicates - we'll reassign them
		phases[i].Predicates = nil
	}

	// Add any predicates that weren't assigned to phases
	allPredicates = append(allPredicates, predicates...)

	// Remove duplicates
	seen := make(map[string]bool)
	uniquePredicates := make([]query.Predicate, 0)
	for _, pred := range allPredicates {
		key := pred.String()
		if !seen[key] {
			seen[key] = true
			uniquePredicates = append(uniquePredicates, pred)
		}
	}

	// Now reassign all predicates to phases with expression outputs considered
	remainingPredicates := uniquePredicates

	for i := range phases {
		// Build complete symbol set for this phase
		available := make(map[query.Symbol]bool)

		// Add all symbols from this phase's Available
		// (includes input parameters + symbols from previous phases)
		for _, sym := range phases[i].Available {
			available[sym] = true
		}

		// Debug: print what's available for last phase
		if i == len(phases)-1 {
		}

		// Add symbols provided by this phase (including expression outputs)
		for _, sym := range phases[i].Provides {
			available[sym] = true
		}

		// Debug: print final available for last phase
		if i == len(phases)-1 {
		}

		// Check which predicates can now be evaluated
		var stillRemaining []query.Predicate
		for _, pred := range remainingPredicates {
			// Check if all variables are available
			canEval := p.canEvaluatePredicate(pred, available)

			if canEval {
				// Check if this is a join predicate (equality between previous and current phase symbols)
				if i > 0 {
					// Check if this is an equality comparison that might be a join
					if comp, ok := pred.(*query.Comparison); ok && comp.Op == query.OpEQ {
						var leftSym, rightSym query.Symbol
						var leftIsVar, rightIsVar bool
						if v, ok := comp.Left.(query.VariableTerm); ok {
							leftSym = v.Symbol
							leftIsVar = true
						}
						if v, ok := comp.Right.(query.VariableTerm); ok {
							rightSym = v.Symbol
							rightIsVar = true
						}

						if leftIsVar && rightIsVar {
							// Determine which symbol comes from previous phases vs current phase
							leftFromPrev := false
							rightFromPrev := false

							// Check if symbols are from previous phases
							for j := 0; j < i; j++ {
								for _, sym := range phases[j].Provides {
									if sym == leftSym {
										leftFromPrev = true
									}
									if sym == rightSym {
										rightFromPrev = true
									}
								}
							}

							// Check if symbols are from current phase
							leftFromCurrent := false
							rightFromCurrent := false
							for _, sym := range phases[i].Provides {
								if sym == leftSym {
									leftFromCurrent = true
								}
								if sym == rightSym {
									rightFromCurrent = true
								}
							}

							// It's a join predicate ONLY if one symbol is EXCLUSIVELY from previous phases
							// and one is EXCLUSIVELY from current phase, AND they will be used for joining
							// between phases (not supported yet in executor).
							//
							// For now, if both symbols are available in the current phase (even if one
							// came from a previous phase), treat it as a regular filtering predicate.
							// This handles cases like [(= ?year ?year2)] where ?year is from Phase 1
							// (but available in Phase 2) and ?year2 is from Phase 2.

							// Check if both symbols are available in the current phase
							bothAvailable := available[leftSym] && available[rightSym]

							if bothAvailable {
								// Both symbols are available - this is a filtering predicate
								phases[i].Predicates = append(phases[i].Predicates, p.createPredicatePlan(pred))
							} else if leftFromPrev && rightFromCurrent && !leftFromCurrent {
								// This would be a join predicate, but executor doesn't support them yet
								// For now, add as regular predicate (will fail at execution if symbols missing)
								phases[i].Predicates = append(phases[i].Predicates, p.createPredicatePlan(pred))
							} else if rightFromPrev && leftFromCurrent && !rightFromCurrent {
								// This would be a join predicate, but executor doesn't support them yet
								// For now, add as regular predicate (will fail at execution if symbols missing)
								phases[i].Predicates = append(phases[i].Predicates, p.createPredicatePlan(pred))
							} else {
								// Regular predicate
								phases[i].Predicates = append(phases[i].Predicates, p.createPredicatePlan(pred))
							}
						} else {
							// Not a variable equality, could be ground predicate
							predPlan := p.createPredicatePlan(pred)
							phases[i].Predicates = append(phases[i].Predicates, predPlan)

							// Check if this is a ground predicate that binds a variable
							// (equality with one variable and one constant)
							if predPlan.Type == PredicateEquality && predPlan.Variable != "" && predPlan.Value != nil && !available[predPlan.Variable] {
								phases[i].Provides = append(phases[i].Provides, predPlan.Variable)
								available[predPlan.Variable] = true
							}
						}
					} else {
						// Not an equality predicate in phase > 0, just add it normally
						phases[i].Predicates = append(phases[i].Predicates, p.createPredicatePlan(pred))
					}
				} else {
					// Phase 0 - just add the predicate
					phases[i].Predicates = append(phases[i].Predicates, p.createPredicatePlan(pred))
				}
			} else {
				stillRemaining = append(stillRemaining, pred)
			}
		}
		remainingPredicates = stillRemaining
	}

	// If any predicates remain unassigned, this is a critical error
	if len(remainingPredicates) > 0 {
		// Debug: print what predicates couldn't be assigned
		var unassigned []string
		for _, pred := range remainingPredicates {
			unassigned = append(unassigned, pred.String())
		}
		// This is a critical error - the query cannot be executed correctly
		panic(fmt.Sprintf("CRITICAL: %d predicates could not be assigned to any phase: %v. Query cannot be executed correctly.", len(remainingPredicates), unassigned))
	}
}

// extractExpressionInputs extracts all variable inputs from an expression
func (p *Planner) extractExpressionInputs(expr *query.Expression) []query.Symbol {
	var inputs []query.Symbol
	seen := make(map[query.Symbol]bool)

	// Extract variables from the function
	if expr.Function != nil {
		// Get required symbols from the function
		switch fn := expr.Function.(type) {
		case *query.ArithmeticFunction:
			if v, ok := fn.Left.(query.VariableTerm); ok && !seen[v.Symbol] {
				inputs = append(inputs, v.Symbol)
				seen[v.Symbol] = true
			}
			if v, ok := fn.Right.(query.VariableTerm); ok && !seen[v.Symbol] {
				inputs = append(inputs, v.Symbol)
				seen[v.Symbol] = true
			}
		case *query.StringConcatFunction:
			for _, term := range fn.Terms {
				if v, ok := term.(query.VariableTerm); ok && !seen[v.Symbol] {
					inputs = append(inputs, v.Symbol)
					seen[v.Symbol] = true
				}
			}
		case *query.GroundFunction:
			// Ground functions don't require any input symbols
		case *query.IdentityFunction:
			if v, ok := fn.Arg.(query.VariableTerm); ok && !seen[v.Symbol] {
				inputs = append(inputs, v.Symbol)
				seen[v.Symbol] = true
			}
		case *query.TimeExtractionFunction:
			if v, ok := fn.TimeTerm.(query.VariableTerm); ok && !seen[v.Symbol] {
				inputs = append(inputs, v.Symbol)
				seen[v.Symbol] = true
			}
		}
	}

	return inputs
}
