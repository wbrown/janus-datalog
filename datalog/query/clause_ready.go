package query

// ClauseBlockers returns the correlates preventing a clause from executing
// now: every non-source correlate that is not yet available. One that can
// never become available blocks forever, and the caller's scheduling loop
// surfaces the invalid query loudly. rules is the rule set the query runs
// with, which declares an invocation's correlates.
//
// This is the single readiness definition — ClauseReady is its emptiness
// test. The planner's phasing (clauseSelectable / createPhasesGreedy) and the
// algebra bridge's compile-order pass both consume it; a second
// implementation would diverge exactly where the scope taxonomy is subtle
// (source markers, an invocation's required positions).
func ClauseBlockers(clause Clause, available map[Symbol]bool, rules *RuleSet) []Symbol {
	var blockers []Symbol
	for _, sym := range ScopeOf(clause, rules).Correlates {
		if sym.IsSource() || available[sym] {
			continue
		}
		blockers = append(blockers, sym)
	}
	return blockers
}

// ClauseReady reports whether a clause can execute now: no correlate blocks
// it (see ClauseBlockers).
func ClauseReady(clause Clause, available map[Symbol]bool, rules *RuleSet) bool {
	return len(ClauseBlockers(clause, available, rules)) == 0
}
