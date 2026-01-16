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
	case *query.SubqueryPattern:
		return extractSubqueryPatternSymbols(c)
	case *query.NotClause:
		return extractNotClauseSymbols(c)
	case *query.NotJoinClause:
		return extractNotJoinClauseSymbols(c)
	case *query.OrClause:
		return extractOrClauseSymbols(c)
	case *query.OrJoinClause:
		return extractOrJoinClauseSymbols(c)
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

// extractSubqueryPatternSymbols extracts symbols from a subquery pattern
// SubqueryPattern is used in OR branches: [(q [...] $ ?in) [[?out]]]
func extractSubqueryPatternSymbols(sp *query.SubqueryPattern) ClauseSymbols {
	// Inputs that are variables are required
	var requires []query.Symbol
	for _, input := range sp.Inputs {
		if v, ok := input.(query.Variable); ok {
			requires = append(requires, v.Name)
		}
	}

	// Binding provides symbols
	var provides []query.Symbol
	switch binding := sp.Binding.(type) {
	case query.TupleBinding:
		provides = append(provides, binding.Variables...)
	case query.RelationBinding:
		provides = append(provides, binding.Variables...)
	case query.ScalarBinding:
		provides = append(provides, binding.Variable)
	case query.CollectionBinding:
		provides = append(provides, binding.Variable)
	}

	return ClauseSymbols{
		Requires: requires,
		Provides: provides,
	}
}

// extractNotClauseSymbols extracts symbols from a NOT clause
// NOT requires ALL variables from inner clauses to be bound before it executes
// NOT provides nothing - it filters, doesn't produce new bindings
func extractNotClauseSymbols(n *query.NotClause) ClauseSymbols {
	requires := make(map[query.Symbol]bool)

	// Collect all variables from inner clauses
	for _, innerClause := range n.Clauses {
		innerSymbols := extractClauseSymbols(innerClause)
		// All variables in inner clauses must be bound before NOT
		for _, sym := range innerSymbols.Requires {
			requires[sym] = true
		}
		for _, sym := range innerSymbols.Provides {
			requires[sym] = true
		}
	}

	var requiresSlice []query.Symbol
	for sym := range requires {
		requiresSlice = append(requiresSlice, sym)
	}

	return ClauseSymbols{
		Requires: requiresSlice,
		Provides: nil, // NOT does not provide new bindings
	}
}

// extractNotJoinClauseSymbols extracts symbols from a NOT-JOIN clause
// NOT-JOIN only requires the explicit JoinVars to be bound
func extractNotJoinClauseSymbols(n *query.NotJoinClause) ClauseSymbols {
	return ClauseSymbols{
		Requires: n.JoinVars,
		Provides: nil, // NOT-JOIN does not provide new bindings
	}
}

// extractOrClauseSymbols extracts symbols from an OR clause
// - For union semantics (pattern-only): provides intersection of all branches
// - For fallback semantics (has expressions): provides union of all branches
func extractOrClauseSymbols(o *query.OrClause) ClauseSymbols {
	if len(o.Branches) == 0 {
		return ClauseSymbols{}
	}

	// Collect symbols from each branch
	branchSymbols := make([]map[query.Symbol]bool, len(o.Branches))
	for i, branch := range o.Branches {
		branchSymbols[i] = make(map[query.Symbol]bool)
		for _, clause := range branch {
			clauseSyms := extractClauseSymbols(clause)
			for _, sym := range clauseSyms.Provides {
				branchSymbols[i][sym] = true
			}
		}
	}

	var provides []query.Symbol

	// Check if fallback semantics apply (any branch has expressions)
	if query.OrHasExpressions(o.Branches) {
		// Fallback semantics: only one branch executes, so use UNION
		// Any symbol that any branch provides will be provided
		allSymbols := make(map[query.Symbol]bool)
		for _, syms := range branchSymbols {
			for sym := range syms {
				allSymbols[sym] = true
			}
		}
		for sym := range allSymbols {
			provides = append(provides, sym)
		}
	} else {
		// Union semantics: all branches execute, so use INTERSECTION
		// Only symbols that ALL branches provide are guaranteed
		for sym := range branchSymbols[0] {
			inAll := true
			for i := 1; i < len(branchSymbols); i++ {
				if !branchSymbols[i][sym] {
					inAll = false
					break
				}
			}
			if inAll {
				provides = append(provides, sym)
			}
		}
	}

	return ClauseSymbols{
		Requires: nil, // OR branches provide data, don't require prior bindings
		Provides: provides,
	}
}

// extractOrJoinClauseSymbols extracts symbols from an OR-JOIN clause
// OR-JOIN provides exactly the JoinVars
func extractOrJoinClauseSymbols(o *query.OrJoinClause) ClauseSymbols {
	return ClauseSymbols{
		Requires: nil, // OR-JOIN provides data, doesn't require prior bindings
		Provides: o.JoinVars,
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

	// OR clauses that provide symbols are data sources too
	if _, ok := clause.(*query.OrClause); ok {
		score += 80
	}
	if _, ok := clause.(*query.OrJoinClause); ok {
		score += 80
	}

	// Expressions that produce new symbols are valuable
	if len(symbols.Provides) > 0 {
		score += 50 * len(symbols.Provides)
	}

	// Predicates that filter are less valuable (should come after data loading)
	if len(symbols.Requires) > 0 && len(symbols.Provides) == 0 {
		score += 10
	}

	// NOT clauses filter - defer until all required symbols are available
	if _, ok := clause.(*query.NotClause); ok {
		score += 5 // Lower than regular predicates
	}
	if _, ok := clause.(*query.NotJoinClause); ok {
		score += 5
	}

	// Subqueries are expensive - defer if possible
	if _, ok := clause.(*query.Subquery); ok {
		score -= 50
	}

	return score
}
