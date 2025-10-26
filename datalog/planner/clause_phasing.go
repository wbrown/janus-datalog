package planner

import (
	"fmt"
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

	var phases []ClausePhase
	remaining := clauses

	// Keep creating phases until all clauses are assigned
	for len(remaining) > 0 {
		phase, newRemaining, err := selectPhaseClauses(remaining, available, findSymbols)
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
func selectPhaseClauses(remaining []query.Clause, available map[query.Symbol]bool, findSymbols []query.Symbol) (ClausePhase, []query.Clause, error) {
	var selectedClauses []query.Clause
	var providedSymbols []query.Symbol
	providedSet := make(map[query.Symbol]bool)

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

			if !canExecuteClause(clause, available) {
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
		symbols := extractClauseSymbols(clause)
		for _, sym := range symbols.Provides {
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

	// Capture symbols that were available at phase start (before we added provides)
	var availableList []query.Symbol
	for sym := range available {
		if !providedSet[sym] {
			availableList = append(availableList, sym)
		}
	}

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
		symbols := extractClauseSymbols(clause)
		for _, sym := range symbols.Requires {
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

	return keep
}
