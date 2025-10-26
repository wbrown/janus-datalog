package planner

import (
	"sort"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// PhaseScore tracks scoring information for phase ordering decisions
type PhaseScore struct {
	Phase              *Phase
	IntersectionCount  int            // Number of symbols that overlap with resolved
	BoundIntersections int            // Number of intersecting symbols that are bound
	Intersections      []query.Symbol // Which symbols actually intersect
	Score              int            // Total score (higher = more related)
}

// reorderPhasesByRelations reorders phases to maximize symbol connectivity between consecutive phases.
// This implements the Clojure planner's reorder-plan-by-relations algorithm.
//
// The algorithm uses a greedy approach:
// 1. Start with initial resolved symbols (from input parameters or first phase)
// 2. For each iteration, find the phase with the most symbol intersections with resolved symbols
// 3. Add that phase to the result and update resolved symbols
// 4. Continue until all phases are ordered
//
// This maximizes "information flow" - phases that share many symbols execute consecutively,
// reducing intermediate result sizes and improving join efficiency.
func (p *Planner) reorderPhasesByRelations(phases []Phase, initialSymbols map[query.Symbol]bool) []Phase {
	// No reordering needed for 0 or 1 phases
	if len(phases) <= 1 {
		return phases
	}

	// Initialize resolved symbols from input parameters
	resolvedSymbols := make(map[query.Symbol]bool)
	for sym := range initialSymbols {
		resolvedSymbols[sym] = true
	}

	// If no initial symbols, bootstrap with the first phase's provides
	// (following Clojure's behavior)
	if len(resolvedSymbols) == 0 && len(phases) > 0 {
		for _, sym := range phases[0].Provides {
			resolvedSymbols[sym] = true
		}
	}

	var result []Phase
	remaining := phases

	for len(remaining) > 0 {
		// Separate phases into executable (dependencies satisfied), related, and unrelated
		var executable, unexecutable []Phase
		for _, phase := range remaining {
			// A phase is executable if ALL its required symbols are available
			if canExecutePhase(phase, resolvedSymbols) {
				executable = append(executable, phase)
			} else {
				unexecutable = append(unexecutable, phase)
			}
		}

		// Further separate executable phases by connectivity
		var related, unrelated []Phase
		for _, phase := range executable {
			if hasSymbolIntersection(phase, resolvedSymbols) {
				related = append(related, phase)
			} else {
				unrelated = append(unrelated, phase)
			}
		}

		var selectedPhase Phase
		var newRemaining []Phase

		if len(related) > 0 {
			// Score related phases and select the best one
			scored := orderPhasesByScore(related, resolvedSymbols)
			selectedPhase = *scored[0].Phase

			// Remaining phases = rest of related + unrelated + unexecutable
			for i := 1; i < len(scored); i++ {
				newRemaining = append(newRemaining, *scored[i].Phase)
			}
			newRemaining = append(newRemaining, unrelated...)
			newRemaining = append(newRemaining, unexecutable...)
		} else if len(unrelated) > 0 {
			// No related executable phases - take the first unrelated executable one
			// This handles disjoint query groups
			selectedPhase = unrelated[0]
			newRemaining = append(unrelated[1:], unexecutable...)
		} else {
			// No executable phases at all - this means we have circular dependencies
			// or phases that require symbols not yet provided. Take the first one anyway.
			selectedPhase = remaining[0]
			newRemaining = remaining[1:]
		}

		// Add selected phase to result
		result = append(result, selectedPhase)

		// Update resolved symbols with what this phase provides
		for _, sym := range selectedPhase.Provides {
			resolvedSymbols[sym] = true
		}

		// Also add expression outputs to resolved symbols
		// This is critical for subqueries that depend on expression outputs
		for _, expr := range selectedPhase.Expressions {
			if expr.Output != "" {
				resolvedSymbols[expr.Output] = true
			}
		}

		remaining = newRemaining
	}

	return result
}

// canExecutePhase checks if a phase can be executed given the current resolved symbols.
// A phase is executable if ALL symbols in its Available list are in the resolved set.
// This ensures we don't execute a phase before its dependencies are ready.
func canExecutePhase(phase Phase, resolvedSymbols map[query.Symbol]bool) bool {
	// Check that ALL required symbols in Available are provided
	for _, sym := range phase.Available {
		if !resolvedSymbols[sym] {
			return false // Missing a required symbol
		}
	}

	// Check subquery requirements
	// Subqueries need their input symbols to be available from previous phases
	for _, subq := range phase.Subqueries {
		for _, input := range subq.Inputs {
			// Skip the database parameter "$" - it's always available
			if input == "$" {
				continue
			}
			// All other inputs must be in resolved symbols
			if !resolvedSymbols[input] {
				return false // Subquery input not yet available
			}
		}
	}

	// Check decorrelated subquery requirements
	for _, decorSubq := range phase.DecorrelatedSubqueries {
		// Decorrelated subqueries have merged queries that need correlation keys
		for _, corrKey := range decorSubq.CorrelationKeys {
			if !resolvedSymbols[corrKey] {
				return false // Correlation key not yet available
			}
		}
	}

	return true // All required symbols and subquery inputs are available
}

// hasSymbolIntersection checks if a phase shares any symbols with the resolved set
func hasSymbolIntersection(phase Phase, resolvedSymbols map[query.Symbol]bool) bool {
	// Check symbols in Available (what the phase needs)
	for _, sym := range phase.Available {
		if resolvedSymbols[sym] {
			return true
		}
	}

	// Check symbols in Provides (what the phase outputs)
	for _, sym := range phase.Provides {
		if resolvedSymbols[sym] {
			return true
		}
	}

	// Check symbols in Keep (what the phase carries forward)
	for _, sym := range phase.Keep {
		if resolvedSymbols[sym] {
			return true
		}
	}

	return false
}

// scorePhase calculates a score for a phase based on symbol connectivity
//
// Score formula (from Clojure):
//
//	score = intersection_count + bound_intersections + (1 if not assertion else 0)
//
// Where:
// - intersection_count: number of symbols shared with resolved
// - bound_intersections: number of those symbols that are actually bound
// - assertion patterns get penalized (though we don't have those yet in Go)
func scorePhase(phase Phase, resolvedSymbols map[query.Symbol]bool) PhaseScore {
	var intersections []query.Symbol
	boundCount := 0

	// Find intersecting symbols
	symbolSet := make(map[query.Symbol]bool)

	// Collect all symbols from the phase
	for _, sym := range phase.Available {
		symbolSet[sym] = true
	}
	for _, sym := range phase.Provides {
		symbolSet[sym] = true
	}
	for _, sym := range phase.Keep {
		symbolSet[sym] = true
	}

	// Check which are in resolved set
	for sym := range symbolSet {
		if resolvedSymbols[sym] {
			intersections = append(intersections, sym)
			// If symbol is in Available, it's bound (can be used)
			for _, availSym := range phase.Available {
				if availSym == sym {
					boundCount++
					break
				}
			}
		}
	}

	intersectionCount := len(intersections)

	// Calculate score
	// Note: We don't currently have "assertion" patterns in Go,
	// so we don't apply the assertion penalty yet
	score := intersectionCount + boundCount

	return PhaseScore{
		Phase:              &phase,
		IntersectionCount:  intersectionCount,
		BoundIntersections: boundCount,
		Intersections:      intersections,
		Score:              score,
	}
}

// orderPhasesByScore sorts phases by their connectivity scores (descending)
func orderPhasesByScore(phases []Phase, resolvedSymbols map[query.Symbol]bool) []PhaseScore {
	scored := make([]PhaseScore, len(phases))
	for i := range phases {
		scored[i] = scorePhase(phases[i], resolvedSymbols)
	}

	// Sort by score descending (higher score = more related = better)
	// Using bubble sort for simplicity and stability
	for i := 0; i < len(scored); i++ {
		for j := i + 1; j < len(scored); j++ {
			if scored[j].Score > scored[i].Score {
				scored[i], scored[j] = scored[j], scored[i]
			}
		}
	}

	return scored
}

// updatePhaseSymbols recalculates Available, Keep, and Find fields after reordering.
// After reordering, we must determine which symbols each phase receives and needs to carry forward.
// This is critical for correctness - without it, phases won't have access to required symbols.
func updatePhaseSymbols(phases []Phase, findElements []query.FindElement, inputSymbols map[query.Symbol]bool) []Phase {
	if len(phases) == 0 {
		return phases
	}

	// Extract symbols from FindElements for Keep calculation
	var findSymbols []query.Symbol
	findSymbolSet := make(map[query.Symbol]bool)
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			if !findSymbolSet[e.Symbol] {
				findSymbols = append(findSymbols, e.Symbol)
				findSymbolSet[e.Symbol] = true
			}
		case query.FindAggregate:
			if !findSymbolSet[e.Arg] {
				findSymbols = append(findSymbols, e.Arg)
				findSymbolSet[e.Arg] = true
			}
		}
	}

	// First pass: Recalculate Provides from scratch based on patterns, expressions, and subqueries
	// This ensures we don't carry over stale Provides from before reordering
	for i := range phases {
		providesSet := make(map[query.Symbol]bool)

		// Add variables from pattern elements (E, A, V positions)
		for _, patternPlan := range phases[i].Patterns {
			if dp, ok := patternPlan.Pattern.(*query.DataPattern); ok {
				for _, elem := range dp.Elements {
					if v, ok := elem.(query.Variable); ok {
						providesSet[v.Name] = true
					}
				}
			}
		}

		// Add expression outputs
		for _, expr := range phases[i].Expressions {
			if expr.Output != "" {
				providesSet[expr.Output] = true
			}
		}

		// Add subquery outputs (skip decorrelated subqueries - their outputs come from aggregates)
		for _, subq := range phases[i].Subqueries {
			// Skip decorrelated subqueries - they don't execute, so they don't provide their outputs
			// Instead, their outputs will be computed by conditional aggregates
			if subq.Decorrelated {
				continue
			}

			switch b := subq.Subquery.Binding.(type) {
			case query.TupleBinding:
				for _, v := range b.Variables {
					providesSet[v] = true
				}
			case query.CollectionBinding:
				providesSet[b.Variable] = true
			case query.RelationBinding:
				for _, v := range b.Variables {
					providesSet[v] = true
				}
			}
		}

		// Convert back to slice
		provides := make([]query.Symbol, 0, len(providesSet))
		for sym := range providesSet {
			provides = append(provides, sym)
		}
		phases[i].Provides = provides
	}

	// Second pass: recalculate Available for each phase based on new ordering
	// Available[i] = inputSymbols + union of all Provides from phases 0..i-1
	// NOTE: This is a temporary calculation - we'll refine it after Keep is calculated
	providedSoFar := make(map[query.Symbol]bool)
	// Start with input symbols
	for sym := range inputSymbols {
		providedSoFar[sym] = true
	}

	for i := range phases {
		// Available = all symbols provided so far (inputs + previous phases)
		var availableSlice []query.Symbol
		for sym := range providedSoFar {
			availableSlice = append(availableSlice, sym)
		}
		phases[i].Available = availableSlice

		// Update providedSoFar with what this phase provides (now includes expressions)
		for _, sym := range phases[i].Provides {
			providedSoFar[sym] = true
		}
	}

	// Third pass: calculate Keep for each phase
	for i := range phases {
		keep := make(map[query.Symbol]bool)

		// Get all symbols available after this phase
		available := make(map[query.Symbol]bool)
		for _, sym := range phases[i].Available {
			available[sym] = true
		}
		for _, sym := range phases[i].Provides {
			available[sym] = true
		}
		// IMPORTANT: Also include expression outputs
		// These are symbols created by expressions (like ?__cond_?pd from conditional aggregates)
		for _, expr := range phases[i].Expressions {
			if expr.Output != "" {
				available[expr.Output] = true
			}
		}

		// 1. Keep symbols in the find clause
		for sym := range findSymbolSet {
			if available[sym] {
				keep[sym] = true
			}
		}

		// 2. Keep symbols needed by future phases
		for j := i + 1; j < len(phases); j++ {
			// Check pattern requirements
			for _, pattern := range phases[j].Patterns {
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
					for _, elem := range dp.Elements {
						if v, ok := elem.(query.Variable); ok {
							if available[v.Name] {
								keep[v.Name] = true
							}
						}
					}
				}
			}

			// Keep symbols needed for join predicates
			for _, jp := range phases[j].JoinPredicates {
				if available[jp.LeftSymbol] {
					keep[jp.LeftSymbol] = true
				}
			}

			// Keep symbols needed for subqueries
			for _, sq := range phases[j].Subqueries {
				for _, input := range sq.Inputs {
					if available[input] {
						keep[input] = true
					}
				}
			}

			// Keep symbols needed for predicates
			for _, pred := range phases[j].Predicates {
				// Extract variables from predicate
				for _, sym := range pred.RequiredVars {
					if available[sym] {
						keep[sym] = true
					}
				}
			}

			// Keep symbols needed for expressions
			for _, expr := range phases[j].Expressions {
				for _, sym := range expr.Inputs {
					if available[sym] {
						keep[sym] = true
					}
				}
			}
		}

		// 2b. Keep symbols needed for conditional aggregates in ANY phase
		// These are stored in phase metadata by the conditional aggregate rewriter
		// IMPORTANT: Aggregate required columns must be carried through ALL later phases,
		// not just the phase that has the metadata! We need to check ALL phases (including future ones)
		// because the metadata might be in a later phase but we need to keep the columns from earlier phases.
		for j := 0; j < len(phases); j++ {
			if phases[j].Metadata != nil {
				if aggCols, ok := phases[j].Metadata["aggregate_required_columns"]; ok {
					if cols, ok := aggCols.([]query.Symbol); ok {
						for _, sym := range cols {
							if available[sym] {
								keep[sym] = true
							}
						}
					}
				}
			}
		}

		// 3. For non-first phases, ensure we keep at least one join symbol
		if i > 0 && len(phases[i].Available) > 0 {
			hasJoinSymbol := false
			for _, sym := range phases[i].Available {
				if keep[sym] {
					hasJoinSymbol = true
					break
				}
			}

			// If no join symbol is kept, keep the first available symbol that's also in Provides
			// IMPORTANT: Don't keep input parameters that aren't in the relation!
			if !hasJoinSymbol {
				// Find first symbol that's in both Available and Provides
				providesSet := make(map[query.Symbol]bool)
				for _, sym := range phases[i].Provides {
					providesSet[sym] = true
				}

				for _, sym := range phases[i].Available {
					if providesSet[sym] {
						keep[sym] = true
						break
					}
				}
			}
		}

		// Convert to slice, preserving findSymbols order
		// First add findSymbols that are in keep (in original order)
		// Then add other symbols needed by future phases
		var keepSlice []query.Symbol
		for _, sym := range findSymbols {
			if keep[sym] {
				keepSlice = append(keepSlice, sym)
				delete(keep, sym)
			}
		}
		// Add remaining symbols (alphabetically for determinism)
		var remaining []query.Symbol
		for sym := range keep {
			remaining = append(remaining, sym)
		}
		sort.Slice(remaining, func(i, j int) bool {
			return remaining[i] < remaining[j]
		})
		keepSlice = append(keepSlice, remaining...)
		phases[i].Keep = keepSlice

		// Find is the query's find elements (preserves aggregates, same for all phases)
		phases[i].Find = findElements
	}

	// Fourth pass: Update Available for each phase based on actual Keep values
	// Now that we know what each phase keeps, we can correctly calculate what's available to future phases
	// Available[i] = input symbols + (Keep[0] ∪ ... ∪ Keep[i-1]) ∪ (Provides[0] ∪ ... ∪ Provides[i-1])
	// where Provides are symbols created by this phase (expressions), and Keep are symbols carried forward
	availableSoFar := make(map[query.Symbol]bool)
	// Start with input symbols
	for sym := range inputSymbols {
		availableSoFar[sym] = true
	}

	for i := range phases {
		// Available = input symbols + what's been kept/created so far
		var availableSlice []query.Symbol
		for sym := range availableSoFar {
			availableSlice = append(availableSlice, sym)
		}
		phases[i].Available = availableSlice

		// Update availableSoFar for next phase:
		// Only what this phase keeps flows forward (projection happens)
		// Keep includes both: (a) symbols carried from previous phases, and (b) newly created symbols
		// So we reset to only Keep, not Keep ∪ Provides
		newAvailable := make(map[query.Symbol]bool)
		for _, sym := range phases[i].Keep {
			newAvailable[sym] = true
		}

		availableSoFar = newAvailable
	}

	return phases
}
