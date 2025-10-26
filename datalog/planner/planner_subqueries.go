package planner

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// assignSubqueriesToPhases assigns subqueries to the earliest phase where their inputs are available.
// This function is re-entrant and can be called multiple times (e.g., after phase reordering).
func (p *Planner) assignSubqueriesToPhases(phases []Phase, subqueries []*query.SubqueryPattern) {
	if len(phases) == 0 {
		return
	}

	// First, remove all existing subquery assignments
	// This makes the function re-entrant (can be called after reordering)
	for i := range phases {
		phases[i].Subqueries = []SubqueryPlan{}
	}

	// Track which subqueries have been assigned
	assigned := make(map[*query.SubqueryPattern]bool)

	// For each subquery, find the earliest phase where all inputs are available
	for _, subq := range subqueries {
		// Extract input variables from the subquery
		inputs := p.extractSubqueryInputs(subq)

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
				// Create initial bindings for the subquery based on its DECLARED PARAMETERS
				// NOT the outer query's arguments
				params := p.extractSubqueryParameters(subq)
				subqueryBindings := make(map[query.Symbol]bool)
				for _, param := range params {
					subqueryBindings[param] = true
				}

				// Recursively plan the nested query with input bindings
				nestedPlan, err := p.PlanWithBindings(subq.Query, subqueryBindings)
				if err != nil {
					// For now, we'll skip subqueries that fail to plan
					// In production, we should propagate this error
					continue
				}

				// This subquery can be evaluated in this phase
				phases[i].Subqueries = append(phases[i].Subqueries, SubqueryPlan{
					Subquery:   subq,
					Inputs:     inputs,
					NestedPlan: nestedPlan,
				})

				// Add symbols provided by this subquery to the phase's provides
				switch b := subq.Binding.(type) {
				case query.TupleBinding:
					for _, v := range b.Variables {
						phases[i].Provides = append(phases[i].Provides, v)
					}
				case query.CollectionBinding:
					phases[i].Provides = append(phases[i].Provides, b.Variable)
				case query.RelationBinding:
					for _, v := range b.Variables {
						phases[i].Provides = append(phases[i].Provides, v)
					}
				}

				assigned[subq] = true
				break
			}
		}

		// If we couldn't assign the subquery to any phase, add it to the last phase
		if !assigned[subq] {
			lastIdx := len(phases) - 1

			// Create initial bindings for the subquery based on its DECLARED PARAMETERS
			// NOT the outer query's arguments
			subqueryBindings := make(map[query.Symbol]bool)
			for _, param := range p.extractSubqueryParameters(subq) {
				subqueryBindings[param] = true
			}

			// Recursively plan the nested query with input bindings
			nestedPlan, err := p.PlanWithBindings(subq.Query, subqueryBindings)
			if err != nil {
				// For now, we'll skip subqueries that fail to plan
				// In production, we should propagate this error
				continue
			}

			phases[lastIdx].Subqueries = append(phases[lastIdx].Subqueries, SubqueryPlan{
				Subquery:   subq,
				Inputs:     inputs,
				NestedPlan: nestedPlan,
			})

			// Add symbols provided by this subquery
			switch b := subq.Binding.(type) {
			case query.TupleBinding:
				for _, v := range b.Variables {
					phases[lastIdx].Provides = append(phases[lastIdx].Provides, v)
				}
			case query.CollectionBinding:
				phases[lastIdx].Provides = append(phases[lastIdx].Provides, b.Variable)
			case query.RelationBinding:
				for _, v := range b.Variables {
					phases[lastIdx].Provides = append(phases[lastIdx].Provides, v)
				}
			}
		}
	}

	// Update Available lists for phases to include subquery outputs from previous phases
	allAvailable := make(map[query.Symbol]bool)
	for i := range phases {
		// Add symbols from this phase's original Available
		for _, sym := range phases[i].Available {
			allAvailable[sym] = true
		}

		// Add symbols provided by this phase (including subqueries)
		for _, sym := range phases[i].Provides {
			allAvailable[sym] = true
		}

		// Update next phase's Available if there is one
		if i+1 < len(phases) {
			phases[i+1].Available = p.getResolvedSymbols(allAvailable)
		}
	}

	// NOTE: Conditional aggregate rewriting is now done in planner.go after all phase
	// creation and reordering is complete. It was previously done here but that caused
	// duplicate expressions when reassigning subqueries after reordering.

	// Detect and plan decorrelation opportunities
	// This is a more general optimization for complex subqueries
	for i := range phases {
		if err := p.detectAndPlanDecorrelation(&phases[i]); err != nil {
			// Log warning but don't fail - fall back to sequential execution
			// fmt.Printf("Warning: decorrelation planning failed: %v\n", err)
		}
	}
}

// extractSubqueryParameters extracts the parameter names declared by the subquery
// These are the symbols from the subquery's :in clause (e.g., :in $ ?symbol ?d)
func (p *Planner) extractSubqueryParameters(subq *query.SubqueryPattern) []query.Symbol {
	var params []query.Symbol
	seen := make(map[query.Symbol]bool)

	// Extract parameter names from the subquery's :in clause
	for _, input := range subq.Query.In {
		switch inp := input.(type) {
		case query.ScalarInput:
			if !seen[inp.Symbol] {
				params = append(params, inp.Symbol)
				seen[inp.Symbol] = true
			}
		case query.CollectionInput:
			if !seen[inp.Symbol] {
				params = append(params, inp.Symbol)
				seen[inp.Symbol] = true
			}
		case query.TupleInput:
			for _, sym := range inp.Symbols {
				if !seen[sym] {
					params = append(params, sym)
					seen[sym] = true
				}
			}
		case query.RelationInput:
			for _, sym := range inp.Symbols {
				if !seen[sym] {
					params = append(params, sym)
					seen[sym] = true
				}
			}
		case query.DatabaseInput:
			// Database $ is handled specially - include it as a parameter
			if !seen["$"] {
				params = append(params, "$")
				seen["$"] = true
			}
		}
	}

	return params
}

// extractSubqueryInputs extracts all variable inputs from a subquery
func (p *Planner) extractSubqueryInputs(subq *query.SubqueryPattern) []query.Symbol {
	// IMPORTANT: We need to extract which OUTER QUERY symbols need to be available
	// to pass as arguments to the subquery. This means we look at subq.Inputs (the
	// argument list), NOT subq.Query.In (the parameter list).
	//
	// Example:
	//   Subquery definition: [:find (max ?h) :in $ ?symbol ?d :where ...]
	//   Outer query invocation: $ ?s ?target-day
	//
	// We need ?s and ?target-day to be available in the outer query, not ?symbol and ?d.

	var inputs []query.Symbol
	seen := make(map[query.Symbol]bool)

	// Extract variables and database markers from input arguments
	for _, input := range subq.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			if !seen[inp.Name] {
				inputs = append(inputs, inp.Name)
				seen[inp.Name] = true
			}
		case query.Constant:
			// Handle database marker $ as a special constant
			if sym, ok := inp.Value.(query.Symbol); ok && sym == "$" {
				if !seen[sym] {
					inputs = append(inputs, sym)
					seen[sym] = true
				}
			}
			// Other constants (like 100.0) don't need to be extracted
			// They're passed directly and don't need to be "available" from outer query
		}
	}

	return inputs
}
