package planner

import (
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
// This operates on the optimized clause list and phases it ONCE. The
// bindings are what the caller holds when it asks for the plan; the rule set
// among them declares an invocation's scope.
func createPhasesGreedy(clauses []query.Clause, findSymbols []query.Symbol, inputSymbols map[query.Symbol]bool, bindings query.Bindings) ([]ClausePhase, error) {
	if len(clauses) == 0 {
		return nil, nil
	}

	// Initialize available symbols with inputs
	available := make(map[query.Symbol]bool)
	for sym := range inputSymbols {
		available[sym] = true
	}

	var phases []ClausePhase
	remaining := clauses

	// Keep creating phases until all clauses are assigned
	for len(remaining) > 0 {
		phase, newRemaining, err := selectPhaseClauses(remaining, available, findSymbols, bindings)
		if err != nil {
			return nil, err
		}

		if len(phase.Clauses) == 0 {
			// No clause can execute with the symbols available: the refusal
			// names each remaining clause with the variables it waits on and
			// says of each whether no input and no clause binds it.
			return nil, query.NewNoOrderError(remaining, clauses, available, inputSymbols, bindings.Rules)
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
func selectPhaseClauses(remaining []query.Clause, available map[query.Symbol]bool, findSymbols []query.Symbol, bindings query.Bindings) (ClausePhase, []query.Clause, error) {
	var selectedClauses []query.Clause
	var providedSymbols []query.Symbol
	providedSet := make(map[query.Symbol]bool)
	availableAtStart := make(map[query.Symbol]bool, len(available))
	for sym := range available {
		availableAtStart[sym] = true
	}

	// Track which clauses we've selected
	selected := make(map[int]bool)
	sel := selection{
		remaining: remaining,
		selected:  selected,
		available: available,
		rules:     bindings.Rules,
	}

	// Greedy selection: keep selecting best clause until no more can execute
	for {
		bestIdx := -1
		bestRank, bestScore := -1, -1000

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
			if !sel.clauseSelectable(clause) {
				continue
			}

			// A clause scanning a whole extent ranks below every other
			// clause, and the score decides within a rank, the first in
			// list order on a tie (REWRITE_ORDER_PROOF.md §6).
			rank := 1
			if scansAWholeExtent(clause, available, bindings.Rules) {
				rank = 0
			}
			score := scoreClause(clause, available, bindings)
			if rank > bestRank || (rank == bestRank && score > bestScore) {
				bestRank, bestScore = rank, score
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
		for _, sym := range query.ScopeOf(clause, bindings.Rules).Provides {
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
func computeKeepSymbols(currentPhase ClausePhase, remainingClauses []query.Clause, findSymbols []query.Symbol, rules *query.RuleSet) []query.Symbol {
	needed := make(map[query.Symbol]bool)

	// Symbols needed by find clause
	for _, sym := range findSymbols {
		needed[sym] = true
	}

	// Symbols needed by remaining clauses
	for _, clause := range remainingClauses {
		for _, sym := range query.ScopeOf(clause, rules).Correlates {
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
