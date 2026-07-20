package query

// ClauseBlockers returns the correlates preventing a clause from executing
// now: every correlate that an input or another clause can bind but that is
// not yet available. For optional-correlate forms, a correlate nothing else
// can bind never blocks — it is existential (plain NOT) or global-fallback
// (or-default), and the clause must not wait on a symbol only it provides.
// Mandatory-correlate forms wait unconditionally; one that can never become
// available blocks forever, and the caller's scheduling loop surfaces the
// invalid query loudly.
//
// This is the single readiness definition — ClauseReady is its emptiness
// test. The planner's phasing (clauseSelectable / createPhasesGreedy) and the
// algebra bridge's compile-order pass both consume it; a second
// implementation would diverge exactly where the scope taxonomy is subtle
// (optional correlates, self-provided symbols, source markers).
func ClauseBlockers(clause Clause, available, inputs map[Symbol]bool, providerCount map[Symbol]int) []Symbol {
	scope := ScopeOf(clause)
	var selfProvides map[Symbol]bool
	var blockers []Symbol
	for _, sym := range scope.Correlates {
		if sym.IsSource() || available[sym] {
			continue
		}
		if scope.CorrelatesOptional {
			if selfProvides == nil {
				selfProvides = make(map[Symbol]bool, len(scope.Provides))
				for _, provided := range scope.Provides {
					selfProvides[provided] = true
				}
			}
			others := providerCount[sym]
			if selfProvides[sym] {
				others--
			}
			if !inputs[sym] && others <= 0 {
				continue
			}
		}
		blockers = append(blockers, sym)
	}
	return blockers
}

// ClauseReady reports whether a clause can execute now: no correlate blocks
// it (see ClauseBlockers).
func ClauseReady(clause Clause, available, inputs map[Symbol]bool, providerCount map[Symbol]int) bool {
	return len(ClauseBlockers(clause, available, inputs, providerCount)) == 0
}

// CountProviders returns how many clauses can bind each symbol. An optional
// correlate blocks scheduling only while an input or some OTHER clause can
// still bind it; counting providers (deduplicated per clause) lets
// ClauseBlockers subtract the clause's own contribution, so a clause never
// waits on itself.
func CountProviders(clauses []Clause) map[Symbol]int {
	providerCount := make(map[Symbol]int)
	for _, clause := range clauses {
		provided := make(map[Symbol]bool)
		for _, sym := range ScopeOf(clause).Provides {
			if !provided[sym] {
				provided[sym] = true
				providerCount[sym]++
			}
		}
	}
	return providerCount
}
