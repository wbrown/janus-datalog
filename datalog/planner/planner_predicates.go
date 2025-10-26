package planner

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

func (p *Planner) selectPredicatesForPhase(predicates []query.Predicate, resolved map[query.Symbol]bool) ([]query.Predicate, []query.Predicate) {
	var selected, unselected []query.Predicate

	for _, pred := range predicates {
		if p.canEvaluatePredicate(pred, resolved) {
			selected = append(selected, pred)
		} else {
			unselected = append(unselected, pred)
		}
	}

	return selected, unselected
}

func (p *Planner) canEvaluatePredicate(pred query.Predicate, resolved map[query.Symbol]bool) bool {
	// Get required variables for this predicate
	requiredVars := p.extractPredicateVars(pred)

	// Check if it's a ground predicate (binds a variable)
	if comp, ok := pred.(*query.Comparison); ok && comp.Op == query.OpEQ {
		// Check for ground predicate pattern: one unresolved variable, one constant
		var unboundVar query.Symbol
		hasConstant := false

		if varTerm, ok := comp.Left.(query.VariableTerm); ok {
			if !resolved[varTerm.Symbol] {
				unboundVar = varTerm.Symbol
			}
		}
		if _, ok := comp.Right.(query.ConstantTerm); ok {
			hasConstant = true
		}

		if unboundVar == "" && hasConstant {
			// Try the other way around
			if varTerm, ok := comp.Right.(query.VariableTerm); ok {
				if !resolved[varTerm.Symbol] {
					unboundVar = varTerm.Symbol
				}
			}
			if _, ok := comp.Left.(query.ConstantTerm); ok {
				hasConstant = true
			}
		}

		if unboundVar != "" && hasConstant {
			// Check if this variable will be provided by an expression
			if p.expressionOutputs != nil && p.expressionOutputs[unboundVar] {
				// This variable will be provided by an expression
				// Don't treat it as a ground predicate - wait for the expression
				return false
			}
			// This is a ground predicate that binds a variable
			return true
		}
	}

	// Regular case: all required variables must be resolved
	for _, varSym := range requiredVars {
		if !resolved[varSym] {
			return false
		}
	}
	return true
}

// createPredicatePlan creates a PredicatePlan with analyzed metadata
func (p *Planner) createPredicatePlan(pred query.Predicate) PredicatePlan {
	plan := PredicatePlan{
		Predicate:    pred,
		RequiredVars: p.extractPredicateVars(pred),
	}

	// Analyze the predicate type and populate metadata
	switch p := pred.(type) {
	case *query.Comparison:
		// Handle comparison predicates
		actualOp := p.Op

		// Check if we need to flip the operator (constant on left, variable on right)
		if _, leftIsConstant := p.Left.(query.ConstantTerm); leftIsConstant {
			if _, rightIsVariable := p.Right.(query.VariableTerm); rightIsVariable {
				// Flip the operator when constant is on left
				// e.g., [(< 100 ?v)] becomes ?v > 100
				switch p.Op {
				case query.OpLT:
					actualOp = query.OpGT
				case query.OpLTE:
					actualOp = query.OpGTE
				case query.OpGT:
					actualOp = query.OpLT
				case query.OpGTE:
					actualOp = query.OpLTE
					// OpEQ and OpNE don't need flipping
				}
			}
		}

		if actualOp == query.OpEQ {
			plan.Type = PredicateEquality
			plan.Operator = query.OpEQ
		} else {
			plan.Type = PredicateComparison
			plan.Operator = actualOp
		}

		// Extract variable and value info
		if v, ok := p.Left.(query.VariableTerm); ok {
			plan.Variable = v.Symbol
			if c, ok := p.Right.(query.ConstantTerm); ok {
				plan.Value = c.Value
			}
		} else if v, ok := p.Right.(query.VariableTerm); ok {
			plan.Variable = v.Symbol
			if c, ok := p.Left.(query.ConstantTerm); ok {
				plan.Value = c.Value
			}
		}

	case *query.ChainedComparison:
		// Chained comparisons need special handling
		plan.Type = PredicateChainedComparison

	case *query.NotEqualPredicate:
		plan.Type = PredicateNotEqual
		plan.Operator = query.OpNE

	case *query.GroundPredicate:
		plan.Type = PredicateGround

	case *query.MissingPredicate:
		plan.Type = PredicateMissing

	case *query.FunctionPredicate:
		plan.Type = PredicateFunction

	default:
		// Unknown predicate type
		plan.Type = PredicateUnknown
	}

	// Collect all required variables if not already set
	if len(plan.RequiredVars) == 0 {
		plan.RequiredVars = pred.RequiredSymbols()
	}

	return plan
}

func (p *Planner) extractPredicateVars(pred query.Predicate) []query.Symbol {
	var vars []query.Symbol
	varSet := make(map[query.Symbol]bool)

	// Extract variables based on predicate type
	switch p := pred.(type) {
	case *query.Comparison:
		if v, ok := p.Left.(query.VariableTerm); ok && !varSet[v.Symbol] {
			vars = append(vars, v.Symbol)
			varSet[v.Symbol] = true
		}
		if v, ok := p.Right.(query.VariableTerm); ok && !varSet[v.Symbol] {
			vars = append(vars, v.Symbol)
			varSet[v.Symbol] = true
		}

	case *query.ChainedComparison:
		for _, term := range p.Terms {
			if v, ok := term.(query.VariableTerm); ok && !varSet[v.Symbol] {
				vars = append(vars, v.Symbol)
				varSet[v.Symbol] = true
			}
		}

	case *query.NotEqualPredicate:
		if v, ok := p.Left.(query.VariableTerm); ok && !varSet[v.Symbol] {
			vars = append(vars, v.Symbol)
			varSet[v.Symbol] = true
		}
		if v, ok := p.Right.(query.VariableTerm); ok && !varSet[v.Symbol] {
			vars = append(vars, v.Symbol)
			varSet[v.Symbol] = true
		}

	case *query.GroundPredicate:
		for _, sym := range p.Variables {
			if !varSet[sym] {
				vars = append(vars, sym)
				varSet[sym] = true
			}
		}

	case *query.MissingPredicate:
		for _, sym := range p.Variables {
			if !varSet[sym] {
				vars = append(vars, sym)
				varSet[sym] = true
			}
		}

	default:
		// For any predicate types not explicitly handled above (e.g. FunctionPredicates),
		// use the predicate's RequiredSymbols() method as a fallback
		for _, sym := range pred.RequiredSymbols() {
			if !varSet[sym] {
				vars = append(vars, sym)
				varSet[sym] = true
			}
		}
	}

	return vars
}
