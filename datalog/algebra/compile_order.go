package algebra

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// orderClausesForCompile returns the clauses in a dependency-honoring order:
// repeatedly the first (source-order) ready clause, per query.ClauseReady —
// the same readiness the planner's phasing consumes, so the bridge and the
// planner agree on which queries are schedulable. Clause order in query text
// carries no meaning (the language contract), so a NOT, predicate,
// expression, or subquery written before the clauses that bind its
// correlates compiles after them.
//
// The first pick prefers generators (non-empty Provides) among ready
// clauses. Readiness alone does not capture the fold's other requirement:
// NOT, not-join, and database-function predicates need a prior relation to
// filter, and a correlate bound by :in makes them ready from iteration zero
// with nothing yet folded. Once anything is folded a relation exists, so
// every later pick is plain first-ready source order — deferring ready
// consumers any further would push Selects above Joins and defeat the
// selective-child lowerings. The fold's requires-prior-relation guards
// remain for clause lists containing no generator at all.
//
// scope's symbols seed the bound set: what is bound around the clause list,
// the query's :in symbols and the caller's at the top level, a header's bound
// variables inside a header form, so a correlate the scope binds is available
// from the first pick. scope's rule set is read by every clause scope, so an
// invocation consumes the variables at its predicate's required positions
// and waits for their binders.
//
// A stall — no remaining clause ready — means some clause's correlates are
// bound by no input and no clause, or by a clause no order places before it:
// an invalid query, rejected loudly naming the stuck clauses and, for a
// variable no input and no clause binds, that nothing binds it (the
// planner's phasing loop rejects the same queries with its no-progress
// error).
func orderClausesForCompile(clauses []query.Clause, scope query.Bindings) ([]query.Clause, error) {
	inputs := scope.Symbols
	bound := make(map[query.Symbol]bool, len(inputs))
	for sym := range inputs {
		bound[sym] = true
	}

	ordered := make([]query.Clause, 0, len(clauses))
	placed := make([]bool, len(clauses))
	for len(ordered) < len(clauses) {
		picked := -1
		for i, clause := range clauses {
			if placed[i] {
				continue
			}
			if !query.ClauseReady(clause, bound, scope.Rules) {
				continue
			}
			if len(ordered) > 0 || len(query.ScopeOf(clause, scope.Rules).Provides) > 0 {
				picked = i
				break
			}
			if picked < 0 {
				picked = i
			}
		}
		if picked < 0 {
			var remaining []query.Clause
			for i, clause := range clauses {
				if !placed[i] {
					remaining = append(remaining, clause)
				}
			}
			return nil, query.NewNoOrderError(remaining, clauses, bound, inputs, scope.Rules)
		}
		placed[picked] = true
		ordered = append(ordered, clauses[picked])
		for _, sym := range query.ScopeOf(clauses[picked], nil).Provides {
			bound[sym] = true
		}
	}
	return ordered, nil
}
