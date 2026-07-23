package planner

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
			for _, sym := range query.ScopeOf(clause).Provides {
				if needed[sym] {
					return true
				}
			}
		}
	}
	return false
}

// clauseSelectable is the single definition of "the greedy selection loop
// may choose this clause this iteration": every scheduling gate admits it
// and it is ready. The selection loop and the subquery deferral gate both
// consume this function — a gate that waits on a clause the loop itself
// would skip deadlocks the phase, which is exactly the divergence that
// occurs when the gate checks readiness alone (a DataPattern can be ready
// yet skipped because it depends on a pending expression).
func clauseSelectable(
	clause query.Clause,
	available map[query.Symbol]bool,
	inputs map[query.Symbol]bool,
	providerCount map[query.Symbol]int,
	remaining []query.Clause,
	selected map[int]bool,
) bool {
	if p, ok := clause.(*query.DataPattern); ok {
		if patternDependsOnPendingExpression(p, available, remaining, selected) {
			return false
		}
	}
	if sp, ok := clause.(*query.SubqueryPattern); ok {
		if subqueryDependsOnPendingProvider(sp, available, inputs, providerCount, remaining, selected) {
			return false
		}
	}
	return query.ClauseReady(clause, available, inputs, providerCount)
}

// subqueryDependsOnPendingProvider checks if a subquery binds a variable
// that a pending (unselected), currently-SELECTABLE, non-subquery clause
// also provides but that isn't yet available. A subquery's result relation
// joins the accumulated relation on whichever of its binding variables are
// already bound; selecting it while a join key's provider is still pending
// joins on a subset of the keys — an under-keyed join that inflates the
// intermediate relation. This is patternDependsOnPendingExpression's
// invariant applied to the subquery relation's interface (a decorrelated
// grouped subquery binds its group keys to outer names precisely so they
// join).
//
// Both restrictions on the provider scan make deferral unable to block
// progress, by construction rather than by convention:
//
//   - Non-subquery only: two subqueries providing the same symbol (e.g. two
//     decorrelated grouped subqueries binding the same group keys) must not
//     defer on each other; once every other provider has run, whichever
//     executes first supplies the keys the next one joins on.
//   - Selectable only — via clauseSelectable, the same predicate the
//     selection loop applies, not bare readiness: a deferring subquery
//     therefore implies the loop can select some provider this iteration,
//     so the phase always advances. Readiness alone is not enough: a ready
//     DataPattern that the pattern gate skips (it depends on a pending
//     expression that in turn needs this subquery's output) is not
//     selectable, and deferring on it deadlocks a valid query.
func subqueryDependsOnPendingProvider(
	sp *query.SubqueryPattern,
	available map[query.Symbol]bool,
	inputs map[query.Symbol]bool,
	providerCount map[query.Symbol]int,
	remaining []query.Clause,
	selected map[int]bool,
) bool {
	needed := make(map[query.Symbol]bool)
	for _, sym := range query.ScopeOf(sp).Provides {
		if !available[sym] {
			needed[sym] = true
		}
	}
	if len(needed) == 0 {
		return false
	}

	for i, clause := range remaining {
		if selected[i] {
			continue
		}
		if _, ok := clause.(*query.SubqueryPattern); ok {
			continue
		}
		if !clauseSelectable(clause, available, inputs, providerCount, remaining, selected) {
			continue
		}
		for _, sym := range query.ScopeOf(clause).Provides {
			if needed[sym] {
				return true
			}
		}
	}
	return false
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
	scope := query.ScopeOf(clause)

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
	if len(scope.Provides) > 0 {
		score += 10 * len(scope.Provides)
	}

	// A predicate is eligible only after every required symbol is available.
	// Once ready, apply it before loading unrelated data so later scans and
	// joins receive the smallest relation possible.
	if len(scope.Correlates) > 0 && len(scope.Provides) == 0 {
		if _, ok := clause.(query.Predicate); ok {
			score += 1000
		} else {
			score += 5
		}
	}

	// NOT clauses filter - defer until all required symbols are available
	if _, ok := clause.(*query.NotClause); ok {
		score += 2
	}
	if _, ok := clause.(*query.NotJoinClause); ok {
		score += 2
	}

	// Correlated subqueries — any variable among the inputs — execute the
	// nested query once per input combination, so they must schedule after
	// every simultaneously-ready clause that can narrow that input;
	// dependency ordering cannot separate them when both need only the same
	// bound symbol. The magnitude is -1000, not an incremental nudge: the
	// deferral must dominate the provides bonus above (+10 per binding
	// variable) at any arity — an additive -50 flips back above a NOT
	// clause at six binding variables — and -1000 is already this scorer's
	// dominance constant for ready predicates. Uncorrelated subqueries are
	// exempt: they execute exactly once wherever placed, so deferral cannot
	// reduce their cost and would only withhold their bindings from earlier
	// joins. Derived 2026-07 on BenchmarkSubqueryDeferralScheduling (~8×
	// time and allocations on the tie-break shape; plan-neutral across the
	// existing benchmark corpus).
	if sp, ok := clause.(*query.SubqueryPattern); ok {
		for _, input := range sp.Inputs {
			if _, isVar := input.(query.Variable); isVar {
				score -= 1000
				break
			}
		}
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

	// Also collect symbols used as subquery inputs — these must remain in the
	// relation so executeSubquery can project the outer relation onto them
	// (the projection's set semantics is the input-combination dedup).
	subqueryInputSyms := make(map[query.Symbol]bool)
	collectSubqueryInputSymbols(clauses, subqueryInputSyms)

	// Return scalar inputs not found in any data pattern or subquery input
	var result []query.Symbol
	for _, sym := range scalarInputs {
		if !patternSyms[sym] && !subqueryInputSyms[sym] {
			result = append(result, sym)
		}
	}
	return result
}

// collectSubqueryInputSymbols recursively walks clauses and collects all variable
// symbols that appear as subquery inputs (the arguments passed to nested queries).
func collectSubqueryInputSymbols(clauses []query.Clause, out map[query.Symbol]bool) {
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *query.SubqueryPattern:
			for _, input := range c.Inputs {
				if v, ok := input.(query.Variable); ok {
					out[v.Name] = true
				}
			}
		case *query.NotClause:
			collectSubqueryInputSymbols(c.Clauses, out)
		case *query.NotJoinClause:
			collectSubqueryInputSymbols(c.Clauses, out)
		case *query.OrClause:
			for _, branch := range c.Branches {
				collectSubqueryInputSymbols(branch, out)
			}
		case *query.OrJoinClause:
			for _, branch := range c.Branches {
				collectSubqueryInputSymbols(branch, out)
			}
		}
	}
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
		}
	}
}
