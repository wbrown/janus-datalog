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
		// Same symbol extraction as OrClause — join vars are planner scheduling
		// metadata, not additional requirements. The algebra bridge infers join
		// vars that may include branch-produced symbols; treating those as
		// Requires would break phasing.
		return extractOrClauseSymbols(&query.OrClause{Branches: c.Branches})
	case *query.OrDefaultClause:
		return extractOrDefaultClauseSymbols(c)
	case *query.OrDefaultJoinClause:
		return extractOrDefaultJoinClauseSymbols(c)
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
	switch binding := e.Binding.(type) {
	case query.Symbol:
		if binding != nil {
			provides = append(provides, binding)
		}
	case query.TupleBinding:
		provides = append(provides, binding.Variables...)
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

// extractNotClauseSymbols extracts symbols from a NOT clause.
// Collects all variables from inner clauses as Requires. This is conservative:
// when the algebra bridge is enabled, NOT is converted to NOT-JOIN with
// explicit join vars before the planner sees it. This function is only used
// when the algebra bridge is disabled.
// NOT provides nothing — it filters, doesn't produce new bindings.
func extractNotClauseSymbols(n *query.NotClause) ClauseSymbols {
	requires := make(map[query.Symbol]bool)

	for _, innerClause := range n.Clauses {
		innerSymbols := extractClauseSymbols(innerClause)
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
		Provides: nil,
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

// extractOrClauseSymbols extracts symbols from an OR clause.
// Always uses INTERSECTION for provides (all branches execute in union mode).
func extractOrClauseSymbols(o *query.OrClause) ClauseSymbols {
	return extractOrBranchSymbolsIntersection(o.Branches)
}

// extractOrDefaultClauseSymbols extracts symbols from an OR-DEFAULT clause.
// Uses UNION for provides (only one branch executes in fallback mode).
// Requires correlation symbols from pattern branches.
func extractOrDefaultClauseSymbols(o *query.OrDefaultClause) ClauseSymbols {
	return extractOrBranchSymbolsFallback(o.Branches)
}

// extractOrBranchSymbolsIntersection computes symbols for union semantics.
// Provides = intersection of all branch provides. Requires = any branch's unmet needs.
func extractOrBranchSymbolsIntersection(branches [][]query.Clause) ClauseSymbols {
	if len(branches) == 0 {
		return ClauseSymbols{}
	}

	branchProvides, branchRequires := collectBranchSymbols(branches)

	// INTERSECTION: only symbols ALL branches provide
	var provides []query.Symbol
	for sym := range branchProvides[0] {
		inAll := true
		for i := 1; i < len(branchProvides); i++ {
			if !branchProvides[i][sym] {
				inAll = false
				break
			}
		}
		if inAll {
			provides = append(provides, sym)
		}
	}

	requires := collectBranchRequires(branchProvides, branchRequires)

	return ClauseSymbols{
		Requires: requires,
		Provides: provides,
	}
}

// extractOrBranchSymbolsFallback computes symbols for fallback semantics.
// Provides = union of all branch provides (any branch might execute).
// Requires correlation symbols from pattern branches for per-tuple evaluation.
func extractOrBranchSymbolsFallback(branches [][]query.Clause) ClauseSymbols {
	if len(branches) == 0 {
		return ClauseSymbols{}
	}

	branchProvides, branchRequires := collectBranchSymbols(branches)

	// UNION: any symbol any branch provides
	var provides []query.Symbol
	allSymbols := make(map[query.Symbol]bool)
	for _, syms := range branchProvides {
		for sym := range syms {
			allSymbols[sym] = true
		}
	}
	for sym := range allSymbols {
		provides = append(provides, sym)
	}

	requires := collectBranchRequires(branchProvides, branchRequires)

	// Add correlation symbols from pattern branches (entity variable of first pattern)
	for _, branch := range branches {
		for _, clause := range branch {
			if pattern, ok := clause.(*query.DataPattern); ok {
				if len(pattern.Elements) > 0 {
					if v, ok := pattern.Elements[0].(query.Variable); ok {
						found := false
						for _, r := range requires {
							if r == v.Name {
								found = true
								break
							}
						}
						if !found {
							requires = append(requires, v.Name)
						}
					}
				}
				break
			}
		}
	}

	return ClauseSymbols{
		Requires: requires,
		Provides: provides,
	}
}

// collectBranchSymbols extracts provides/requires from each branch.
func collectBranchSymbols(branches [][]query.Clause) ([]map[query.Symbol]bool, []map[query.Symbol]bool) {
	branchProvides := make([]map[query.Symbol]bool, len(branches))
	branchRequires := make([]map[query.Symbol]bool, len(branches))
	for i, branch := range branches {
		branchProvides[i] = make(map[query.Symbol]bool)
		branchRequires[i] = make(map[query.Symbol]bool)
		for _, clause := range branch {
			clauseSyms := extractClauseSymbols(clause)
			for _, sym := range clauseSyms.Provides {
				branchProvides[i][sym] = true
			}
			for _, sym := range clauseSyms.Requires {
				branchRequires[i][sym] = true
			}
		}
	}
	return branchProvides, branchRequires
}

// collectBranchRequires collects symbols required by any branch but not self-provided.
func collectBranchRequires(branchProvides, branchRequires []map[query.Symbol]bool) []query.Symbol {
	allRequires := make(map[query.Symbol]bool)
	for i, reqs := range branchRequires {
		for sym := range reqs {
			if !branchProvides[i][sym] {
				allRequires[sym] = true
			}
		}
	}
	var requires []query.Symbol
	for sym := range allRequires {
		requires = append(requires, sym)
	}
	return requires
}

// extractOrJoinClauseSymbols extracts symbols from an OR-JOIN clause
// OR-JOIN provides exactly the JoinVars and requires external symbols used by branches
func extractOrJoinClauseSymbols(o *query.OrJoinClause) ClauseSymbols {
	// Collect requires from each branch
	branchProvides := make([]map[query.Symbol]bool, len(o.Branches))
	branchRequires := make([]map[query.Symbol]bool, len(o.Branches))
	for i, branch := range o.Branches {
		branchProvides[i] = make(map[query.Symbol]bool)
		branchRequires[i] = make(map[query.Symbol]bool)
		for _, clause := range branch {
			clauseSyms := extractClauseSymbols(clause)
			for _, sym := range clauseSyms.Provides {
				branchProvides[i][sym] = true
			}
			for _, sym := range clauseSyms.Requires {
				branchRequires[i][sym] = true
			}
		}
	}

	// Collect required symbols: any symbol required by any branch
	// that isn't provided within that branch needs to come from outside
	allRequires := make(map[query.Symbol]bool)
	for i, reqs := range branchRequires {
		for sym := range reqs {
			// Only require if this branch doesn't self-provide it
			if !branchProvides[i][sym] {
				allRequires[sym] = true
			}
		}
	}

	var requires []query.Symbol
	for sym := range allRequires {
		// Source symbols ($) are always available — never a dependency
		if sym.IsSource() {
			continue
		}
		requires = append(requires, sym)
	}

	// Provides: symbols provided by ALL branches (intersection).
	// Join vars are only in provides if every branch produces them;
	// otherwise they're required from the outer context.
	providesSet := make(map[query.Symbol]bool)
	if len(branchProvides) > 0 {
		// Start with first branch's provides, intersect with others
		common := make(map[query.Symbol]bool)
		for sym := range branchProvides[0] {
			common[sym] = true
		}
		for i := 1; i < len(branchProvides); i++ {
			for sym := range common {
				if !branchProvides[i][sym] {
					delete(common, sym)
				}
			}
		}
		for sym := range common {
			providesSet[sym] = true
		}
	}

	// Join vars not produced by all branches are required from outside
	for _, jv := range o.JoinVars {
		if !providesSet[jv] && !allRequires[jv] {
			allRequires[jv] = true
			requires = append(requires, jv)
		}
	}

	var provides []query.Symbol
	for sym := range providesSet {
		provides = append(provides, sym)
	}

	return ClauseSymbols{
		Requires: requires,
		Provides: provides,
	}
}

// extractOrDefaultJoinClauseSymbols extracts symbols from an OR-DEFAULT-JOIN clause.
// Uses fallback semantics (UNION of provides) with explicit join variables.
func extractOrDefaultJoinClauseSymbols(o *query.OrDefaultJoinClause) ClauseSymbols {
	if len(o.Branches) == 0 {
		return ClauseSymbols{}
	}

	branchProvides, branchRequires := collectBranchSymbols(o.Branches)
	requires := collectBranchRequires(branchProvides, branchRequires)

	// Filter source symbols from requires
	var filteredRequires []query.Symbol
	for _, sym := range requires {
		if !sym.IsSource() {
			filteredRequires = append(filteredRequires, sym)
		}
	}

	// UNION provides (fallback: any branch might execute)
	providesSet := make(map[query.Symbol]bool)
	for _, syms := range branchProvides {
		for sym := range syms {
			providesSet[sym] = true
		}
	}

	// Join vars not produced by any branch are required from outside
	allRequiresSet := make(map[query.Symbol]bool)
	for _, r := range filteredRequires {
		allRequiresSet[r] = true
	}
	for _, jv := range o.JoinVars {
		if !providesSet[jv] && !allRequiresSet[jv] {
			allRequiresSet[jv] = true
			filteredRequires = append(filteredRequires, jv)
		}
	}

	var provides []query.Symbol
	for sym := range providesSet {
		provides = append(provides, sym)
	}

	return ClauseSymbols{
		Requires: filteredRequires,
		Provides: provides,
	}
}

// patternDependsOnPendingExpression checks if a data pattern uses a variable
// that a pending (unselected) expression provides but that isn't yet available.
// This prevents the planner from reordering data patterns before the expressions
// that provide variables they need for correct join semantics.
//
// Without this check, the greedy scorer picks high-scoring data patterns (~210)
// before low-scoring expressions (~20). When a pattern like [?item :item/color ?color]
// runs before [(enumerate ?vec) [?idx ?item]], ?item isn't in the accumulated
// relation, so the join is on ?color alone — producing a cross-product.
func patternDependsOnPendingExpression(p *query.DataPattern, available map[query.Symbol]bool, remaining []query.Clause, selected map[int]bool) bool {
	// Collect variables this pattern references that aren't yet available
	needed := make(map[query.Symbol]bool)
	for _, elem := range p.Elements {
		if v, ok := elem.(query.Variable); ok {
			if !available[v.Name] {
				needed[v.Name] = true
			}
		}
	}
	if len(needed) == 0 {
		return false
	}

	// Check if any pending expression provides one of these variables
	for i, clause := range remaining {
		if selected[i] {
			continue
		}
		if _, ok := clause.(*query.Expression); ok {
			symbols := extractClauseSymbols(clause)
			for _, sym := range symbols.Provides {
				if needed[sym] {
					return true
				}
			}
		}
	}
	return false
}

// computeOtherProvidable computes what symbols could be provided by OTHER clauses
// (excluding the clause at excludeIdx and already selected clauses)
func computeOtherProvidable(clauses []query.Clause, selected map[int]bool, excludeIdx int) map[query.Symbol]bool {
	result := make(map[query.Symbol]bool)
	for i, clause := range clauses {
		if i == excludeIdx || selected[i] {
			continue
		}
		symbols := extractClauseSymbols(clause)
		for _, sym := range symbols.Provides {
			result[sym] = true
		}
	}
	return result
}

// canExecuteClause determines if a clause can be executed given available symbols
// This is the simple version without context about other clauses.
func canExecuteClause(clause query.Clause, available map[query.Symbol]bool) bool {
	// Delegate to context-aware version with empty providable set
	return canExecuteClauseWithContext(clause, available, nil)
}

// canExecuteClauseWithContext determines if a clause can be executed given:
// - available: symbols already bound
// - potentiallyProvidable: symbols that OTHER clauses could provide (used for OR correlation)
func canExecuteClauseWithContext(clause query.Clause, available map[query.Symbol]bool, potentiallyProvidable map[query.Symbol]bool) bool {
	symbols := extractClauseSymbols(clause)

	// OR clauses with fallback semantics need special handling for correlation symbols.
	// They should wait for correlation symbols IF those symbols will become available
	// (i.e., some other clause provides them). If no other clause provides them,
	// the OR can execute with global fallback.
	// Or-default clauses with fallback semantics need special handling for correlation symbols.
	// They should wait for correlation symbols IF those symbols will become available.
	if _, ok := clause.(*query.OrDefaultClause); ok {
		for _, req := range symbols.Requires {
			if available[req] {
				continue
			}
			if potentiallyProvidable != nil && potentiallyProvidable[req] {
				return false
			}
		}
		return true
	}
	if _, ok := clause.(*query.OrDefaultJoinClause); ok {
		for _, req := range symbols.Requires {
			if available[req] {
				continue
			}
			if potentiallyProvidable != nil && potentiallyProvidable[req] {
				return false
			}
		}
		return true
	}

	// Standard check: all required symbols must be available
	for _, req := range symbols.Requires {
		if !available[req] {
			return false
		}
	}

	return true
}

// scoreClause assigns a score to a clause for greedy selection
// Higher score = better to execute now
//
// For DataPatterns, we follow the greedy join ordering approach from the paper:
// "When Greedy Beats Optimal: Join Ordering for Pattern-Based Datalog Queries"
//
// Key insight: Selectivity is VISIBLE in pattern syntax, no statistics needed.
// Priority ordering:
//  1. Patterns with concrete values (constants) - visible selectivity, run first
//  2. Patterns with available variable bindings - join hints, run next
//  3. Unbound patterns - run last
//
// Constants are weighted 10× more than available variables because:
// - Constants FILTER data (selectivity is visible in the pattern)
// - Available variables ENABLE JOINS (don't filter, just connect)
func scoreClause(clause query.Clause, available map[query.Symbol]bool) int {
	symbols := extractClauseSymbols(clause)

	score := 0

	// Patterns are data sources - use priority-based selectivity scoring
	if p, ok := clause.(*query.DataPattern); ok {
		score += 100

		// Count constants (visible selectivity) and available vars (join hints)
		constants, availableVars := countSelectivityFactors(p, available)

		// Priority ordering: constants >> available variables
		// Constants have 10× the weight because they actually filter data
		score += (constants * 100) + (availableVars * 10)
	}

	// OR clauses that provide symbols are data sources too
	switch clause.(type) {
	case *query.OrClause, *query.OrJoinClause, *query.OrDefaultClause, *query.OrDefaultJoinClause:
		score += 80
	}

	// Expressions that produce new symbols are valuable but less than selective patterns
	if len(symbols.Provides) > 0 {
		score += 10 * len(symbols.Provides)
	}

	// Predicates that filter are less valuable (should come after data loading)
	if len(symbols.Requires) > 0 && len(symbols.Provides) == 0 {
		score += 5
	}

	// NOT clauses filter - defer until all required symbols are available
	if _, ok := clause.(*query.NotClause); ok {
		score += 2
	}
	if _, ok := clause.(*query.NotJoinClause); ok {
		score += 2
	}

	// Subqueries are expensive - defer if possible
	if _, ok := clause.(*query.Subquery); ok {
		score -= 50
	}

	return score
}

// countSelectivityFactors separates constants from available variables in a pattern.
// This implements the paper's insight that selectivity is visible in pattern syntax:
// - Constants = visible selectivity (they filter data)
// - Available variables = join hints (they enable joins, don't filter)
//
// The distinction matters: [?e :name "Alice"] has 2 constants (A and V positions),
// while [?e :skills ?skills] has 1 constant (A only). The former is more selective
// regardless of what variables are available.
func countSelectivityFactors(p *query.DataPattern, available map[query.Symbol]bool) (constants, availableVars int) {
	for _, elem := range p.Elements {
		switch e := elem.(type) {
		case query.Constant:
			constants++ // Constants provide visible selectivity
		case query.Variable:
			if available[e.Name] {
				availableVars++ // Available vars are join hints, not selectivity
			}
			// Blanks are unbound (match anything) - neither constants nor join hints
		}
	}
	return constants, availableVars
}

// findConstantBindableScalars identifies scalar input symbols that only appear
// in predicates/expressions, not in any data pattern. These can be resolved as
// constant bindings rather than creating separate relation groups.
func findConstantBindableScalars(scalarInputs []query.Symbol, clauses []query.Clause) []query.Symbol {
	// Collect all symbols that appear in data patterns
	patternSyms := make(map[query.Symbol]bool)
	collectDataPatternSymbols(clauses, patternSyms)

	// Return scalar inputs not found in any data pattern
	var result []query.Symbol
	for _, sym := range scalarInputs {
		if !patternSyms[sym] {
			result = append(result, sym)
		}
	}
	return result
}

// collectDataPatternSymbols recursively walks clauses and collects all variable
// symbols that appear in data patterns.
func collectDataPatternSymbols(clauses []query.Clause, out map[query.Symbol]bool) {
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *query.DataPattern:
			for _, elem := range c.Elements {
				if v, ok := elem.(query.Variable); ok {
					out[v.Name] = true
				}
			}
		case *query.NotClause:
			collectDataPatternSymbols(c.Clauses, out)
		case *query.NotJoinClause:
			collectDataPatternSymbols(c.Clauses, out)
		case *query.OrClause:
			for _, branch := range c.Branches {
				collectDataPatternSymbols(branch, out)
			}
		case *query.OrJoinClause:
			for _, branch := range c.Branches {
				collectDataPatternSymbols(branch, out)
			}
		case *query.OrDefaultClause:
			for _, branch := range c.Branches {
				collectDataPatternSymbols(branch, out)
			}
		case *query.OrDefaultJoinClause:
			for _, branch := range c.Branches {
				collectDataPatternSymbols(branch, out)
			}
		case *query.SubqueryPattern:
			collectDataPatternSymbols(c.Query.Where, out)
		case *query.Subquery:
			collectDataPatternSymbols(c.Query.Where, out)
		}
	}
}
