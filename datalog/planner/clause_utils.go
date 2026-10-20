package planner

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A selection is one phase's greedy pick over the clauses still to place:
// which of them the phase has taken, the symbols bound so far, and the rule
// set the clauses are read under. Its gates decide which pending clause the
// loop may take next.
type selection struct {
	remaining []query.Clause
	selected  map[int]bool
	available map[query.Symbol]bool
	rules     *query.RuleSet
}

// blockers returns the correlates holding a clause back now, per
// query.ClauseBlockers, under the selection's state: the one readiness view
// every gate reads.
func (s selection) blockers(clause query.Clause) []query.Symbol {
	return query.ClauseBlockers(clause, s.available, s.rules)
}

// patternDependsOnPendingExpression reports whether a pending (unselected)
// expression binds a variable the pattern reads unbound and can run before
// the pattern. The greedy scorer ranks a data pattern (~210) far above an
// expression (~20), and a pattern like [?item :item/color ?color] run before
// [(enumerate ?vec) [?idx ?item]] joins the accumulated relation on ?color
// alone, a cross-product; held back, it seeks on the ?item the expression
// bound. An expression whose own needs reach the pattern through the pending
// clauses binding them (pendingBlockers) cannot run before it: the pattern
// runs first, and the expression unifies with the variable the pattern
// bound.
func (s selection) patternDependsOnPendingExpression(p *query.DataPattern) bool {
	needed := make(map[query.Symbol]bool)
	for _, elem := range p.Elements {
		if v, ok := elem.(query.Variable); ok {
			if !s.available[v.Name] {
				needed[v.Name] = true
			}
		}
	}
	if len(needed) == 0 {
		return false
	}

	for i, clause := range s.remaining {
		if s.selected[i] {
			continue
		}
		if _, ok := clause.(*query.Expression); !ok {
			continue
		}
		binds := false
		for _, sym := range query.ScopeOf(clause, s.rules).Provides {
			if needed[sym] {
				binds = true
				break
			}
		}
		if !binds {
			continue
		}
		waits := s.pendingBlockers(clause)
		reachesPattern := false
		for sym := range needed {
			if waits[sym] {
				reachesPattern = true
				break
			}
		}
		if !reachesPattern {
			return true
		}
	}
	return false
}

// pendingBlockers returns the symbols a clause waits on through the pending
// (unselected) clauses: its own blockers, and, for each pending clause
// binding a symbol of the set, what that clause waits on in turn. A pattern,
// the clause asked about or one reached, waits on its unavailable variables
// that a pending expression binds, since patternDependsOnPendingExpression
// holds it behind that expression.
func (s selection) pendingBlockers(clause query.Clause) map[query.Symbol]bool {
	expressionBound := make(map[query.Symbol]bool)
	for i, c := range s.remaining {
		if s.selected[i] {
			continue
		}
		if _, ok := c.(*query.Expression); ok {
			for _, sym := range query.ScopeOf(c, s.rules).Provides {
				expressionBound[sym] = true
			}
		}
	}
	waits := make(map[query.Symbol]bool)
	queue := s.blockers(clause)
	if p, ok := clause.(*query.DataPattern); ok {
		for _, v := range query.ScopeOf(p, s.rules).Provides {
			if !s.available[v] && expressionBound[v] {
				queue = append(queue, v)
			}
		}
	}
	for len(queue) > 0 {
		sym := queue[0]
		queue = queue[1:]
		if waits[sym] {
			continue
		}
		waits[sym] = true
		for i, c := range s.remaining {
			if s.selected[i] {
				continue
			}
			provides := query.ScopeOf(c, s.rules).Provides
			if !query.ContainsSymbol(provides, sym) {
				continue
			}
			queue = append(queue, s.blockers(c)...)
			if _, ok := c.(*query.DataPattern); ok {
				for _, v := range provides {
					if !s.available[v] && expressionBound[v] {
						queue = append(queue, v)
					}
				}
			}
		}
	}
	return waits
}

// clauseSelectable is the single definition of "the greedy selection loop
// may choose this clause this iteration": every scheduling gate admits it
// and it is ready. The selection loop and the subquery and invocation
// deferral gates all consume it — a gate that waits on a clause the loop
// itself would skip deadlocks the phase, which is exactly the divergence
// that occurs when the gate checks readiness alone (a DataPattern can be
// ready yet skipped because it depends on a pending expression).
func (s selection) clauseSelectable(clause query.Clause) bool {
	switch c := clause.(type) {
	case *query.DataPattern:
		if s.patternDependsOnPendingExpression(c) {
			return false
		}
	case *query.SubqueryPattern:
		if s.subqueryDependsOnPendingProvider(c) {
			return false
		}
	case *query.RuleInvocation:
		if s.invocationDependsOnPendingProvider(c) {
			return false
		}
	}
	return len(s.blockers(clause)) == 0
}

// scansAWholeExtent reports whether a clause reads nothing bound under the
// symbols available now: a data pattern with no constant at a position other
// than the attribute and no available variable, an invocation with no
// constant argument, no available argument and a predicate the seed does not
// restrict, or any other clause none of whose variables is available, a form
// whose header nothing binds, a subquery with no available input, or an
// expression over constants alone. The selection takes such a clause only
// when no other clause is selectable, since a scan placed before an
// invocation the seed's bindings reach multiplies that invocation's magic set
// by the scan's extent, and a form placed before the pattern binding its
// header runs over the unit relation (REWRITE_ORDER_PROOF.md T2, T3, F7).
func scansAWholeExtent(clause query.Clause, available map[query.Symbol]bool, rules *query.RuleSet) bool {
	switch c := clause.(type) {
	case *query.DataPattern:
		for i, elem := range c.Elements {
			switch e := elem.(type) {
			case query.Constant, query.VectorConstant:
				if i != 1 {
					return false
				}
			case query.Variable:
				if available[e.Name] {
					return false
				}
			}
		}
		return true
	case *query.RuleInvocation:
		if query.SeedRestricted(c.Name) {
			return false
		}
		for _, arg := range c.Args {
			switch a := arg.(type) {
			case query.Constant, query.VectorConstant:
				return false
			case query.Variable:
				if available[a.Name] {
					return false
				}
			}
		}
		return true
	}
	scope := query.ScopeOf(clause, rules)
	for _, sym := range scope.Provides {
		if available[sym] {
			return false
		}
	}
	for _, sym := range scope.Correlates {
		if available[sym] {
			return false
		}
	}
	return true
}

// invocationDependsOnPendingProvider checks if a rule invocation has a
// variable argument that a pending (unselected), currently-SELECTABLE clause
// other than a subquery or an invocation binds but that isn't yet available.
// An invocation with no available argument derives the whole predicate when
// selected before the clause that binds its argument, so it waits on any
// such provider. An invocation with an available argument derives what that
// argument reaches, and waits on a provider that does not scan a whole
// extent: a scan placed before it would bind its other argument to the
// scan's every value, the product REWRITE_ORDER_PROOF.md T2 names. An
// invocation of a predicate the seed restricts derives nothing at its clause:
// it reads a relation holding only what the seed reaches, and scoreClause
// gives it the lead over every pattern, so it waits on no provider.
//
// The provider scan carries subqueryDependsOnPendingProvider's restrictions,
// so deferral cannot block progress:
//
//   - Neither an invocation nor a subquery is a provider here: two
//     invocations binding the same variable never wait on each other, and
//     the subquery gate already asks whether an invocation is selectable, so
//     neither gate's scan reaches the other.
//   - Selectable only, via clauseSelectable: a deferring invocation implies
//     the loop can select some provider this iteration, so the phase always
//     advances.
func (s selection) invocationDependsOnPendingProvider(inv *query.RuleInvocation) bool {
	if query.SeedRestricted(inv.Name) {
		return false
	}
	bound := false
	for _, arg := range inv.Args {
		if v, ok := arg.(query.Variable); ok && s.available[v.Name] {
			bound = true
		}
	}
	needed := make(map[query.Symbol]bool)
	for _, sym := range query.ScopeOf(inv, s.rules).Provides {
		if !s.available[sym] {
			needed[sym] = true
		}
	}
	if len(needed) == 0 {
		return false
	}

	for i, clause := range s.remaining {
		if s.selected[i] {
			continue
		}
		switch clause.(type) {
		case *query.SubqueryPattern, *query.RuleInvocation:
			continue
		}
		if !s.clauseSelectable(clause) {
			continue
		}
		if bound && scansAWholeExtent(clause, s.available, s.rules) {
			continue
		}
		for _, sym := range query.ScopeOf(clause, s.rules).Provides {
			if needed[sym] {
				return true
			}
		}
	}
	return false
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
//   - A provider that is selectable this iteration holds the subquery, and
//     so does one that is not selectable but waits, through the pending
//     clauses (pendingBlockers), on no output of the subquery: the clauses
//     it waits on are held by no gate of the subquery's, so the phase always
//     advances. A provider waiting on the subquery's own output never holds
//     it: a DataPattern the pattern gate skips because it depends on a
//     pending expression that in turn needs this subquery's output would
//     deadlock a valid query.
func (s selection) subqueryDependsOnPendingProvider(sp *query.SubqueryPattern) bool {
	outputs := query.ScopeOf(sp, s.rules).Provides
	needed := make(map[query.Symbol]bool)
	for _, sym := range outputs {
		if !s.available[sym] {
			needed[sym] = true
		}
	}
	if len(needed) == 0 {
		return false
	}

	for i, clause := range s.remaining {
		if s.selected[i] {
			continue
		}
		if _, ok := clause.(*query.SubqueryPattern); ok {
			continue
		}
		if !s.clauseSelectable(clause) {
			waits := s.pendingBlockers(clause)
			waitsOnSubquery := false
			for _, sym := range outputs {
				if waits[sym] {
					waitsOnSubquery = true
					break
				}
			}
			if waitsOnSubquery {
				continue
			}
		}
		for _, sym := range query.ScopeOf(clause, s.rules).Provides {
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
//
// bindings is what the caller holds when it asks for the plan; the rule set
// among them declares an invocation's scope.
func scoreClause(clause query.Clause, available map[query.Symbol]bool, bindings query.Bindings) int {
	scope := query.ScopeOf(clause, bindings.Rules)

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
	case *query.OrJoinClause, *query.OrDefaultJoinClause:
		score += 80
	}

	// A rule invocation carries the visible selectivity a pattern's positions
	// do and schedules beside the patterns that bind its arguments. An
	// invocation of a predicate the seed restricts, a magic or adorned
	// predicate or the delta of one, reads a relation holding only what the
	// seed reaches, so it schedules before every pattern, which then seeks on
	// what it binds.
	if inv, ok := clause.(*query.RuleInvocation); ok {
		score += 80
		constants, availableVars := 0, 0
		for _, arg := range inv.Args {
			switch a := arg.(type) {
			case query.Constant, query.VectorConstant:
				constants++
			case query.Variable:
				if available[a.Name] {
					availableVars++
				}
			}
		}
		score += (constants * 100) + (availableVars * 10)
		if query.SeedRestricted(inv.Name) {
			score += 500
		}
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
	// joins.
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
