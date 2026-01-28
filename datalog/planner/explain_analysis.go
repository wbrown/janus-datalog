package planner

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// analyzeClausesForExplain analyzes clauses and populates explain fields in RealizedPhase
func analyzeClausesForExplain(phase *RealizedPhase, clauses []query.Clause, available map[query.Symbol]bool, stats *Statistics) {
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *query.DataPattern:
			plan := analyzePatternForExplain(c, available, stats)
			phase.Patterns = append(phase.Patterns, plan)
			// Add pattern's bindings to available for subsequent clause analysis
			for sym := range plan.Bindings {
				available[sym] = true
			}

		case *query.Expression:
			plan := analyzeExprForExplain(c, available)
			phase.Expressions = append(phase.Expressions, plan)
			// Add expression output to available
			switch binding := c.Binding.(type) {
			case query.Symbol:
				if binding != nil {
					available[binding] = true
				}
			case query.TupleBinding:
				for _, sym := range binding.Variables {
					available[sym] = true
				}
			}

		case query.Predicate:
			plan := analyzePredicateForExplain(c, available)
			phase.Predicates = append(phase.Predicates, plan)

		case *query.SubqueryPattern:
			plan := analyzeSubqueryForExplain(c, available)
			phase.Subqueries = append(phase.Subqueries, plan)
			// Add subquery outputs to available
			syms := getBindingSymbols(c.Binding)
			for _, sym := range syms {
				available[sym] = true
			}
		}
	}
}

// getBindingSymbols extracts symbols from a BindingForm
func getBindingSymbols(bf query.BindingForm) []query.Symbol {
	switch b := bf.(type) {
	case query.TupleBinding:
		return b.Variables
	case query.CollectionBinding:
		return []query.Symbol{b.Variable}
	case query.ScalarBinding:
		return []query.Symbol{b.Variable}
	case query.RelationBinding:
		return b.Variables
	}
	return nil
}

// analyzePatternForExplain creates a PatternPlan with index selection and selectivity
func analyzePatternForExplain(pattern *query.DataPattern, resolved map[query.Symbol]bool, stats *Statistics) PatternPlan {
	if stats == nil {
		stats = &Statistics{
			AttributeCardinality: make(map[string]int),
			EntityCount:          1000000,
		}
	}

	plan := PatternPlan{
		Pattern:  pattern,
		Bindings: make(map[query.Symbol]bool),
	}

	// Determine which elements are bound
	if elem := pattern.GetE(); elem != nil {
		plan.BoundMask.E = isElementBound(elem, resolved)
	}
	if elem := pattern.GetA(); elem != nil {
		plan.BoundMask.A = isElementBound(elem, resolved)
	}
	if elem := pattern.GetV(); elem != nil {
		plan.BoundMask.V = isElementBound(elem, resolved)
	}
	if elem := pattern.GetT(); elem != nil {
		plan.BoundMask.T = isElementBound(elem, resolved)
	}

	// Select index based on bound mask
	plan.Index = selectIndexForMask(plan.BoundMask)

	// Calculate selectivity (lower is more selective)
	plan.Selectivity = scorePatternSelectivity(pattern, resolved, stats)

	// Extract variables that will be bound
	for _, elem := range pattern.Elements {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok {
				plan.Bindings[v.Name] = true
			}
		}
	}

	return plan
}

// isElementBound checks if a pattern element is bound (constant or resolved variable)
func isElementBound(elem query.PatternElement, resolved map[query.Symbol]bool) bool {
	if elem == nil {
		return false
	}
	if !elem.IsVariable() {
		return true // Constants and blanks are bound
	}
	if v, ok := elem.(query.Variable); ok {
		return resolved[v.Name]
	}
	return false
}

// selectIndexForMask chooses the best index based on bound elements
func selectIndexForMask(mask BoundMask) IndexType {
	switch {
	case mask.E && mask.A && mask.V:
		return EAVT // All bound - most selective
	case mask.E && mask.A:
		return EAVT // Entity + attribute
	case mask.A && mask.V:
		return AVET // Attribute + value (for reverse lookups)
	case mask.A && mask.E:
		return AEVT // Attribute + entity
	case mask.E:
		return EAVT // Entity only
	case mask.A:
		return AEVT // Attribute only
	case mask.V:
		return VAET // Value only (for ref lookups)
	default:
		return EAVT // Full scan - try to avoid
	}
}

// scorePatternSelectivity estimates the selectivity of a pattern (lower is more selective)
func scorePatternSelectivity(pattern *query.DataPattern, resolved map[query.Symbol]bool, stats *Statistics) int {
	score := 0
	boundCount := 0

	// Check entity
	if elem := pattern.GetE(); elem != nil {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
				boundCount++
			} else {
				score += 1000 // Unbound entity is least selective
			}
		} else {
			boundCount++
			score -= 800 // Huge bonus for constant entity
		}
	}

	// Check attribute
	if elem := pattern.GetA(); elem != nil {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
				boundCount++
				score += 10
			} else {
				score += 100 // Unbound attribute is moderately unselective
			}
		} else {
			boundCount++
			// Use cardinality statistics if available
			if constant, ok := elem.(query.Constant); ok {
				if attr, ok := constant.Value.(datalog.Keyword); ok {
					if card, exists := stats.AttributeCardinality[attr.String()]; exists {
						score += card / 100 // Higher cardinality = less selective
					}
				}
			}
		}
	}

	// Check value
	if elem := pattern.GetV(); elem != nil {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok {
				if resolved[v.Name] {
					boundCount++
					score -= 500 // Bound value is as selective as constant
				} else {
					score += 500 // Very unselective
				}
			} else {
				score += 50
			}
		} else {
			boundCount++
			score -= 500 // Big bonus for constant value
		}
	}

	// Patterns with no bound elements can't be executed yet
	if boundCount == 0 && len(resolved) > 0 {
		return -1
	}

	// Bonus for patterns that bind new variables
	newBindings := 0
	for _, elem := range pattern.Elements {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && !resolved[v.Name] {
				newBindings++
			}
		}
	}
	score -= newBindings * 10 // Patterns that bind more variables are preferred

	return score
}

// analyzeExprForExplain creates an ExpressionPlan from an Expression
// Named differently to avoid conflict with existing extractExpressionInputs
func analyzeExprForExplain(expr *query.Expression, available map[query.Symbol]bool) ExpressionPlan {
	inputs := extractExprInputs(expr)
	// Check if this is an equality check (no binding)
	var isEquality bool
	switch b := expr.Binding.(type) {
	case query.Symbol:
		isEquality = b == nil
	case query.TupleBinding:
		isEquality = len(b.Variables) == 0
	default:
		isEquality = expr.Binding == nil
	}
	return ExpressionPlan{
		Expression: expr,
		Inputs:     inputs,
		Output:     expr.Binding,
		IsEquality: isEquality,
	}
}

// extractExprInputs extracts input symbols from an expression
// Named differently to avoid conflict with existing extractExpressionInputs
func extractExprInputs(expr *query.Expression) []query.Symbol {
	var inputs []query.Symbol
	seen := make(map[query.Symbol]bool)

	// Extract from function arguments using RequiredSymbols if available
	if expr.Function != nil {
		for _, sym := range expr.Function.RequiredSymbols() {
			if !seen[sym] {
				inputs = append(inputs, sym)
				seen[sym] = true
			}
		}
	}

	return inputs
}

// analyzePredicateForExplain creates a PredicatePlan from a Predicate
func analyzePredicateForExplain(pred query.Predicate, available map[query.Symbol]bool) PredicatePlan {
	plan := PredicatePlan{
		Predicate:    pred,
		RequiredVars: extractPredicateVars(pred),
		Type:         classifyPredicate(pred),
	}
	return plan
}

// extractPredicateVars extracts all variable symbols from a predicate
func extractPredicateVars(pred query.Predicate) []query.Symbol {
	// Use the predicate's RequiredSymbols method if available
	if p, ok := pred.(*query.Comparison); ok {
		return p.RequiredSymbols()
	}
	return nil
}

// classifyPredicate determines the type of a predicate
func classifyPredicate(pred query.Predicate) PredicatePlanType {
	switch p := pred.(type) {
	case *query.Comparison:
		switch p.Op {
		case query.OpEQ:
			return PredicateEquality
		case query.OpNE:
			return PredicateNotEqual
		default:
			return PredicateComparison
		}
	}
	return PredicateUnknown
}

// analyzeSubqueryForExplain creates a SubqueryPlan from a SubqueryPattern
func analyzeSubqueryForExplain(subq *query.SubqueryPattern, available map[query.Symbol]bool) SubqueryPlan {
	// Extract input symbols from the subquery's :in clause
	var inputs []query.Symbol
	for _, inp := range subq.Query.In {
		switch i := inp.(type) {
		case query.ScalarInput:
			if available[i.Symbol] {
				inputs = append(inputs, i.Symbol)
			}
		case query.CollectionInput:
			if available[i.Symbol] {
				inputs = append(inputs, i.Symbol)
			}
		case query.TupleInput:
			for _, sym := range i.Symbols {
				if available[sym] {
					inputs = append(inputs, sym)
				}
			}
		case query.RelationInput:
			for _, sym := range i.Symbols {
				if available[sym] {
					inputs = append(inputs, sym)
				}
			}
		}
	}

	return SubqueryPlan{
		Subquery: subq,
		Inputs:   inputs,
	}
}
