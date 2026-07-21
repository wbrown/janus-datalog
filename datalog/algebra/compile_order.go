package algebra

import (
	"fmt"
	"strings"

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
// available seeds the bound set: the query's :in symbols plus whatever the
// enclosing context supplies (an outer relation for NOT bodies, a schema
// placeholder for or-branches). inputs is the :in symbol set alone —
// ClauseReady distinguishes a correlate an input can bind from one nothing
// can bind.
//
// A stall — no remaining clause ready — means some clause's mandatory
// correlates are bindable by no input and no clause: an invalid query,
// rejected loudly naming the stuck clauses (the planner's phasing loop
// rejects the same queries with its no-progress error).
func orderClausesForCompile(clauses []query.Clause, available, inputs map[query.Symbol]bool) ([]query.Clause, error) {
	if len(clauses) < 2 {
		return clauses, nil
	}
	providerCount := query.CountProviders(clauses)
	bound := make(map[query.Symbol]bool, len(available))
	for sym := range available {
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
			if !query.ClauseReady(clause, bound, inputs, providerCount) {
				continue
			}
			if len(ordered) > 0 || len(query.ScopeOf(clause).Provides) > 0 {
				picked = i
				break
			}
			if picked < 0 {
				picked = i
			}
		}
		if picked < 0 {
			var stuck []string
			for i, clause := range clauses {
				if placed[i] {
					continue
				}
				var waits []string
				for _, sym := range query.ClauseBlockers(clause, bound, inputs, providerCount) {
					if !inputs[sym] && providerCount[sym] == 0 {
						waits = append(waits, fmt.Sprintf("%s (no input or clause binds it)", sym))
					} else {
						waits = append(waits, sym.String())
					}
				}
				stuck = append(stuck, fmt.Sprintf("%v waits on %s", clause, strings.Join(waits, ", ")))
			}
			return nil, fmt.Errorf(
				"cannot order clauses for compilation: %s",
				strings.Join(stuck, "; "))
		}
		placed[picked] = true
		ordered = append(ordered, clauses[picked])
		for _, sym := range query.ScopeOf(clauses[picked]).Provides {
			bound[sym] = true
		}
	}
	return ordered, nil
}
