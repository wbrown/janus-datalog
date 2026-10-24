package query

import (
	"fmt"
	"strings"
)

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

// NoOrderError refuses a clause list no order places every clause of
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3): each clause left
// unplaced with the variables it waits on, and among them those no input and
// no clause of the list binds, whose repair is binding the variable, where
// the repair for a variable a clause binds is the order. A clause waiting on
// no variable is one the planner's gates hold back. Both planner paths raise
// it, so one text is refused with one message.
type NoOrderError struct {
	Stuck []StuckClause
}

// StuckClause is one clause a stalled order leaves unplaced: the variables
// it waits on, in its scope's order, and those among them no input and no
// clause of the list binds.
type StuckClause struct {
	Clause  Clause
	Waits   []Symbol
	Unbound []Symbol
}

// NewNoOrderError is the refusal of a stall: for each clause of remaining,
// the variables it waits on under available, and those among them that no
// symbol of inputs and no clause of all other than the stuck clause itself
// provides, since a clause binding the variable it reads is no provider of
// it. remaining's clauses are members of all. rules declares an invocation's
// correlates and provisions.
func NewNoOrderError(remaining, all []Clause, available, inputs map[Symbol]bool, rules *RuleSet) *NoOrderError {
	providers := make(map[Symbol]int)
	for _, clause := range all {
		for _, sym := range ScopeOf(clause, rules).Provides {
			providers[sym]++
		}
	}
	e := &NoOrderError{}
	for _, clause := range remaining {
		stuck := StuckClause{Clause: clause, Waits: ClauseBlockers(clause, available, rules)}
		own := ScopeOf(clause, rules).Provides
		for _, sym := range stuck.Waits {
			others := providers[sym]
			if ContainsSymbol(own, sym) {
				others--
			}
			if !inputs[sym] && others == 0 {
				stuck.Unbound = append(stuck.Unbound, sym)
			}
		}
		e.Stuck = append(e.Stuck, stuck)
	}
	return e
}

// Error renders each stuck clause with the variables it waits on, the suffix
// on a variable no input and no clause binds, and a clause waiting on no
// variable as such.
func (e *NoOrderError) Error() string {
	stuck := make([]string, len(e.Stuck))
	for i, s := range e.Stuck {
		if len(s.Waits) == 0 {
			stuck[i] = fmt.Sprintf("%v waits on no variable", s.Clause)
			continue
		}
		waits := make([]string, len(s.Waits))
		for j, sym := range s.Waits {
			if ContainsSymbol(s.Unbound, sym) {
				waits[j] = sym.String() + " (no input or clause binds it)"
			} else {
				waits[j] = sym.String()
			}
		}
		stuck[i] = fmt.Sprintf("%v waits on %s", s.Clause, strings.Join(waits, ", "))
	}
	return "cannot order clauses: " + strings.Join(stuck, "; ")
}
