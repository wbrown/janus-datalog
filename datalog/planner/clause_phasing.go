package planner

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ClausePhase represents a group of clauses that can execute together
type ClausePhase struct {
	Clauses   []query.Clause
	Available []query.Symbol // Symbols available at phase start
	Provides  []query.Symbol // Symbols this phase produces
}

// createPhasesGreedy groups clauses into phases using a greedy algorithm
// This operates on the optimized clause list and phases it ONCE
func createPhasesGreedy(clauses []query.Clause, findSymbols []query.Symbol, inputSymbols map[query.Symbol]bool) ([]ClausePhase, error) {
	if len(clauses) == 0 {
		return nil, nil
	}

	// Initialize available symbols with inputs
	available := make(map[query.Symbol]bool)
	for sym := range inputSymbols {
		available[sym] = true
	}

	// How many clauses can bind each symbol — see query.CountProviders and
	// query.ClauseReady for the self-provider subtraction this feeds.
	providerCount := query.CountProviders(clauses)

	// A NOT body must unify with the enclosing query through at least one
	// variable the query can bind — by input or by some clause. With zero
	// unifiable variables the anti-join has no keys and the clause's
	// quantification would silently turn global; reject here, before any
	// execution, naming the clause (Datomic's insufficient-binding rule).
	for _, clause := range clauses {
		nc, ok := clause.(*query.NotClause)
		if !ok {
			continue
		}
		unifiable := false
		for _, sym := range query.FreeVariables(nc.Clauses) {
			if sym.IsSource() {
				continue
			}
			if available[sym] || providerCount[sym] > 0 {
				unifiable = true
				break
			}
		}
		if !unifiable {
			return nil, fmt.Errorf(
				"NOT clause %s shares no variable the enclosing query can bind; a NOT body must unify with the enclosing query through at least one variable",
				nc,
			)
		}
	}

	var phases []ClausePhase
	remaining := clauses

	// Keep creating phases until all clauses are assigned
	for len(remaining) > 0 {
		phase, newRemaining, err := selectPhaseClauses(remaining, available, findSymbols, inputSymbols, providerCount)
		if err != nil {
			return nil, err
		}

		if len(phase.Clauses) == 0 {
			// No progress - we have clauses that can't execute with available symbols
			// This indicates a problem with the query
			return nil, fmt.Errorf("cannot create phase: %d clauses remaining but none can execute with available symbols", len(remaining))
		}

		// Add symbols this phase provides to available set
		for _, sym := range phase.Provides {
			available[sym] = true
		}

		phases = append(phases, phase)
		remaining = newRemaining
	}

	return phases, nil
}

// selectPhaseClauses greedily selects clauses for the next phase
func selectPhaseClauses(remaining []query.Clause, available map[query.Symbol]bool, findSymbols []query.Symbol, inputs map[query.Symbol]bool, providerCount map[query.Symbol]int) (ClausePhase, []query.Clause, error) {
	var selectedClauses []query.Clause
	var providedSymbols []query.Symbol
	providedSet := make(map[query.Symbol]bool)
	availableAtStart := make(map[query.Symbol]bool, len(available))
	for sym := range available {
		availableAtStart[sym] = true
	}

	// Track which clauses we've selected
	selected := make(map[int]bool)

	// Greedy selection: keep selecting best clause until no more can execute
	for {
		bestIdx := -1
		bestScore := -1000

		// Find the best clause that can execute
		for i, clause := range remaining {
			if selected[i] {
				continue
			}

			// Selectability is defined once, in clauseSelectable: the
			// pattern gate (patterns wait for pending expressions they
			// depend on), the subquery gate (subqueries wait for selectable
			// providers of their binding variables), and readiness. The
			// subquery gate consumes the same predicate, so it can never
			// wait on a clause this loop would skip.
			if !clauseSelectable(clause, available, inputs, providerCount, remaining, selected) {
				continue
			}

			score := scoreClause(clause, available)
			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}

		// No more executable clauses
		if bestIdx == -1 {
			break
		}

		// Select this clause
		selected[bestIdx] = true
		clause := remaining[bestIdx]
		selectedClauses = append(selectedClauses, clause)

		// Add symbols this clause provides to our local available set and tracking
		for _, sym := range query.ScopeOf(clause).Provides {
			if !providedSet[sym] {
				providedSymbols = append(providedSymbols, sym)
				providedSet[sym] = true
			}
			// Make it available for subsequent clauses in this phase
			available[sym] = true
		}
	}

	// Build remaining clause list
	var newRemaining []query.Clause
	for i, clause := range remaining {
		if !selected[i] {
			newRemaining = append(newRemaining, clause)
		}
	}

	// Capture symbols that were available at phase start. A bound input can also
	// appear in a pattern position and therefore be reported as provided; it
	// remains an input to the phase and must not disappear from its Datalog :in.
	var availableList []query.Symbol
	for sym := range availableAtStart {
		availableList = append(availableList, sym)
	}
	// Sort for deterministic ordering. Available is used only as a set
	// (membership) and to build the phase's :in clause. Sorting keeps
	// EXPLAIN/plan output stable across runs.
	sort.Slice(availableList, func(i, j int) bool {
		return availableList[i].Compare(availableList[j]) < 0
	})

	phase := ClausePhase{
		Clauses:   selectedClauses,
		Available: availableList,
		Provides:  providedSymbols,
	}

	return phase, newRemaining, nil
}

// computeKeepSymbols determines which symbols to pass to the next phase
func computeKeepSymbols(currentPhase ClausePhase, remainingClauses []query.Clause, findSymbols []query.Symbol) []query.Symbol {
	needed := make(map[query.Symbol]bool)

	// Symbols needed by find clause
	for _, sym := range findSymbols {
		needed[sym] = true
	}

	// Symbols needed by remaining clauses
	for _, clause := range remainingClauses {
		for _, sym := range query.ScopeOf(clause).Correlates {
			needed[sym] = true
		}
	}

	// Filter to symbols that are actually available (from current or previous phases)
	allAvailable := make(map[query.Symbol]bool)
	for _, sym := range currentPhase.Available {
		allAvailable[sym] = true
	}
	for _, sym := range currentPhase.Provides {
		allAvailable[sym] = true
	}

	var keep []query.Symbol
	for sym := range needed {
		if allAvailable[sym] {
			keep = append(keep, sym)
		}
	}
	// Sort for deterministic ordering. Downstream phases match Keep symbols by
	// name (Project / joins look up by symbol, not position), so order does not
	// affect results — this only stabilizes EXPLAIN/plan output across runs.
	sort.Slice(keep, func(i, j int) bool {
		return keep[i].Compare(keep[j]) < 0
	})

	return keep
}
