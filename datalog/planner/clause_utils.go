package planner

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ClauseSymbols represents the symbol dependencies of a clause
type ClauseSymbols struct {
	Requires []query.Symbol // Symbols this clause needs to execute
	Provides []query.Symbol // Symbols this clause produces
}

// extractClauseSymbols analyzes a clause to determine its symbol dependencies
func extractClauseSymbols(clause query.Clause) ClauseSymbols {
	switch c := clause.(type) {
	case *query.DataPattern:
		return extractPatternSymbols(c)
	case *query.Expression:
		return extractExpressionSymbols(c)
	case *query.Comparison:
		return extractComparisonSymbols(c)
	case *query.ChainedComparison:
		return extractChainedComparisonSymbols(c)
	case *query.GroundPredicate:
		return extractGroundPredicateSymbols(c)
	case *query.NotEqualPredicate:
		return extractNotEqualSymbols(c)
	case *query.MissingPredicate:
		return extractMissingPredicateSymbols(c)
	case *query.Subquery:
		return extractSubquerySymbols(c)
	default:
		// Unknown clause type - conservative: requires and provides nothing
		return ClauseSymbols{}
	}
}

// extractPatternSymbols extracts symbols from a data pattern
func extractPatternSymbols(p *query.DataPattern) ClauseSymbols {
	var provides []query.Symbol
	seen := make(map[query.Symbol]bool)

	// Check each element in the pattern
	for _, elem := range p.Elements {
		if v, ok := elem.(query.Variable); ok {
			if !seen[v.Name] {
				provides = append(provides, v.Name)
				seen[v.Name] = true
			}
		}
	}

	return ClauseSymbols{
		Requires: nil, // Patterns don't require symbols
		Provides: provides,
	}
}

// extractExpressionSymbols extracts symbols from an expression
func extractExpressionSymbols(e *query.Expression) ClauseSymbols {
	requires := e.Function.RequiredSymbols()

	var provides []query.Symbol
	if e.Binding != "" {
		provides = append(provides, e.Binding)
	}

	return ClauseSymbols{
		Requires: requires,
		Provides: provides,
	}
}

// extractComparisonSymbols extracts symbols from a comparison predicate
func extractComparisonSymbols(c *query.Comparison) ClauseSymbols {
	requires := c.RequiredSymbols()

	return ClauseSymbols{
		Requires: requires,
		Provides: nil, // Predicates don't produce symbols
	}
}

// extractChainedComparisonSymbols extracts symbols from a chained comparison
func extractChainedComparisonSymbols(cc *query.ChainedComparison) ClauseSymbols {
	requires := cc.RequiredSymbols()

	return ClauseSymbols{
		Requires: requires,
		Provides: nil,
	}
}

// extractGroundPredicateSymbols extracts symbols from a ground predicate
func extractGroundPredicateSymbols(gp *query.GroundPredicate) ClauseSymbols {
	return ClauseSymbols{
		Requires: gp.Variables, // Ground checks these variables
		Provides: nil,
	}
}

// extractNotEqualSymbols extracts symbols from a not-equal predicate
func extractNotEqualSymbols(ne *query.NotEqualPredicate) ClauseSymbols {
	requires := ne.RequiredSymbols()

	return ClauseSymbols{
		Requires: requires,
		Provides: nil,
	}
}

// extractMissingPredicateSymbols extracts symbols from a missing predicate
func extractMissingPredicateSymbols(mp *query.MissingPredicate) ClauseSymbols {
	return ClauseSymbols{
		Requires: mp.Variables, // Missing checks these variables
		Provides: nil,
	}
}

// extractSubquerySymbols extracts symbols from a subquery
func extractSubquerySymbols(sq *query.Subquery) ClauseSymbols {
	requires := sq.Inputs

	var provides []query.Symbol
	switch binding := sq.Binding.(type) {
	case query.Symbol:
		provides = append(provides, binding)
	case query.TupleBinding:
		provides = append(provides, binding.Variables...)
	case query.RelationBinding:
		provides = append(provides, binding.Variables...)
	}

	return ClauseSymbols{
		Requires: requires,
		Provides: provides,
	}
}

// canExecuteClause determines if a clause can be executed given available symbols
func canExecuteClause(clause query.Clause, available map[query.Symbol]bool) bool {
	symbols := extractClauseSymbols(clause)

	// Check if all required symbols are available
	for _, req := range symbols.Requires {
		if !available[req] {
			return false
		}
	}

	return true
}

// scoreClause assigns a score to a clause for greedy selection
// Higher score = better to execute now
func scoreClause(clause query.Clause, available map[query.Symbol]bool) int {
	symbols := extractClauseSymbols(clause)

	score := 0

	// Patterns are preferred (data sources)
	if _, ok := clause.(*query.DataPattern); ok {
		score += 100
	}

	// Expressions that produce new symbols are valuable
	if len(symbols.Provides) > 0 {
		score += 50 * len(symbols.Provides)
	}

	// Predicates that filter are less valuable (should come after data loading)
	if len(symbols.Requires) > 0 && len(symbols.Provides) == 0 {
		score += 10
	}

	// Subqueries are expensive - defer if possible
	if _, ok := clause.(*query.Subquery); ok {
		score -= 50
	}

	return score
}
