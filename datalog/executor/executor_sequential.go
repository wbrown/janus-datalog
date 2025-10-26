package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// executePhaseSequential executes patterns in a phase using the Relations-based interface
func (e *Executor) executePhaseSequential(ctx Context, phase *planner.Phase, phaseIndex int, previousResult Relation) (Relation, error) {
	phaseName := fmt.Sprintf("Phase %d", phaseIndex+1)
	if len(phase.Patterns) > 0 {
		// Add first pattern info for clarity
		if dp, ok := phase.Patterns[0].Pattern.(*query.DataPattern); ok && len(dp.Elements) >= 2 {
			if attr, ok := dp.Elements[1].(query.Constant); ok {
				phaseName = fmt.Sprintf("Phase %d (%v)", phaseIndex+1, attr.Value)
			}
		}
	}

	return ctx.ExecutePhase(phaseName, phase, func() (Relation, error) {
		// Collect all available relations for this phase
		var availableRelations Relations

		// If we have results from previous phases, include them
		// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!
		if previousResult != nil {
			availableRelations = append(availableRelations, previousResult)
		}

		// Execute patterns and progressively collapse relations
		var independentGroups Relations

		for i, patternPlan := range phase.Patterns {
			pattern, ok := patternPlan.Pattern.(*query.DataPattern)
			if !ok {
				continue
			}

			// Check if pattern has pre-computed constraints from planner
			var constraints []StorageConstraint
			if patternPlan.Metadata != nil {
				// Try new constraints.StorageConstraint interface type first
				if newConstraints, ok := patternPlan.Metadata["storage_constraints"].([]StorageConstraint); ok && len(newConstraints) > 0 {
					constraints = newConstraints
				} else if plannerConstraints, ok := patternPlan.Metadata["storage_constraints"].([]planner.StorageConstraint); ok && len(plannerConstraints) > 0 {
					// Fallback to old planner.StorageConstraint struct type
					constraints = convertPlannerConstraints(pattern, plannerConstraints)
				}
			}

			// Materialize relations that share symbols with the pattern
			// These relations will be: (1) used for binding-based filtering, (2) joined with the result
			// Materializing allows them to be iterated multiple times without consuming the iterator
			availableRelations = materializeRelationsForPattern(pattern, availableRelations)

			// Match pattern with all available relations
			var rel Relation
			var err error

			if collector := ctx.Collector(); collector != nil {
				start := time.Now()

				// Use predicate-aware matching if constraints exist
				if len(constraints) > 0 {
					if pam, ok := e.matcher.(PredicateAwareMatcher); ok {
						rel, err = pam.MatchWithConstraints(pattern, availableRelations, constraints)
					} else {
						rel, err = e.matchPatternWithRelations(ctx, pattern, availableRelations)
					}
				} else {
					rel, err = e.matchPatternWithRelations(ctx, pattern, availableRelations)
				}

				// Build annotation data
				annotData := map[string]interface{}{
					"pattern":        pattern.String(),
					"bindings.count": len(availableRelations),
				}

				if len(constraints) > 0 {
					annotData["constraints.pushed"] = len(constraints)
				}

				// Add result info if we have a result
				if rel != nil {
					// Convert columns to string array for output formatter
					symbols := make([]string, len(rel.Columns()))
					for i, col := range rel.Columns() {
						symbols[i] = string(col)
					}
					annotData["symbol.order"] = symbols

					// Don't call Size() - it consumes streaming iterators
					// Size tracking moved to collapse events
				}

				collector.AddTiming("pattern/match", start, annotData)
			} else {
				// No collector - still use constraints if available
				if len(constraints) > 0 {
					if pam, ok := e.matcher.(PredicateAwareMatcher); ok {
						rel, err = pam.MatchWithConstraints(pattern, availableRelations, constraints)
					} else {
						rel, err = e.matchPatternWithRelations(ctx, pattern, availableRelations)
					}
				} else {
					rel, err = e.matchPatternWithRelations(ctx, pattern, availableRelations)
				}
			}

			if err != nil {
				return nil, fmt.Errorf("pattern %d failed: %w", i, err)
			}

			// Don't call IsEmpty() - it consumes streaming iterators
			// Collapse() will handle empty relations naturally
			// Don't materialize - pattern result will be joined as a streaming relation

			// Add to independent groups and try to collapse
			if len(independentGroups) == 0 {
				// First pattern in phase - initialize with availableRelations + new result
				independentGroups = append(Relations{}, availableRelations...)
				independentGroups = append(independentGroups, rel)
				independentGroups = independentGroups.Collapse(ctx)
			} else {
				// Subsequent patterns - add new result and collapse
				independentGroups = append(independentGroups, rel)
				independentGroups = independentGroups.Collapse(ctx)
			}

			// Don't check IsEmpty() - even after Collapse(), results might be streaming
			// Empty detection happens naturally in subsequent operations

			// Update available relations to include all independent groups
			// Clear and rebuild to avoid index confusion
			availableRelations = Relations{}
			availableRelations = append(availableRelations, independentGroups...)


		}

		// Use the collapsed groups for expressions/predicates
		//
		// CRITICAL: Handle expression-only phases (no patterns)
		// If a phase has zero patterns, the pattern loop (lines 35-147) never executes,
		// leaving independentGroups empty. In this case, we must use availableRelations
		// which contains the previous phase's results.
		//
		// Without this check, expression-only phases receive empty relations and produce
		// zero tuples, even when the previous phase had data.
		//
		// This commonly occurs with:
		// - Conditional aggregate rewriting (creates pure expression phases)
		// - Phase reordering (may group expressions together)
		// - CSE optimization (may create dedicated expression phases)
		//
		// See: docs/bugs/resolved/BUG_EXPRESSION_ONLY_PHASES.md
		collapsed := independentGroups
		if len(phase.Patterns) == 0 && len(collapsed) == 0 {
			collapsed = availableRelations
		}

		// Handle expressions and predicates
		return e.applyExpressionsAndPredicates(ctx, phase, collapsed)
	})
}

// matchPatternWithRelations matches a pattern using the new interface
func (e *Executor) matchPatternWithRelations(ctx Context, pattern *query.DataPattern, bindings Relations) (Relation, error) {
	// Pass time ranges if available and matcher supports them
	if tra, ok := e.matcher.(TimeRangeAware); ok {
		if rangesVal, ok := ctx.GetMetadata("time_ranges"); ok {
			if ranges, ok := rangesVal.([]TimeRange); ok {
				tra.WithTimeRanges(ranges)
			}
		}
	}

	// Use the new Match interface
	// NOTE: bindings are used for pattern selection heuristics (FindBestForPattern)
	// but should NOT be consumed - they will be joined with the result later
	return e.matcher.Match(pattern, bindings)
}

// convertPlannerConstraints converts planner storage constraints to executor constraints
func convertPlannerConstraints(pattern *query.DataPattern, plannerConstraints []planner.StorageConstraint) []StorageConstraint {
	var result []StorageConstraint

	// Map pattern variables to positions
	varPositions := make(map[query.Symbol]int)
	if v, ok := pattern.GetE().(query.Variable); ok {
		varPositions[v.Name] = 0
	}
	if v, ok := pattern.GetA().(query.Variable); ok {
		varPositions[v.Name] = 1
	}
	if v, ok := pattern.GetV().(query.Variable); ok {
		varPositions[v.Name] = 2
	}
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(query.Variable); ok {
			varPositions[v.Name] = 3
		}
	}

	for _, pc := range plannerConstraints {
		switch pc.Type {
		case planner.ConstraintEquality:
			// Position 2 (value) is the common case
			result = append(result, &equalityConstraint{
				position: 2,
				value:    pc.Value,
			})

		case planner.ConstraintRange:
			// Build range constraint based on operator
			rc := &rangeConstraint{position: 2}
			switch pc.Operator {
			case query.OpGT:
				rc.min = pc.Value
				rc.includeMin = false
			case query.OpGTE:
				rc.min = pc.Value
				rc.includeMin = true
			case query.OpLT:
				rc.max = pc.Value
				rc.includeMax = false
			case query.OpLTE:
				rc.max = pc.Value
				rc.includeMax = true
			}
			result = append(result, rc)

		case planner.ConstraintTimeExtraction:
			// Time extraction with expected value
			result = append(result, &timeExtractionConstraint{
				position:  2, // Value position for time
				extractFn: pc.TimeField,
				expected:  pc.Value,
			})
		}
	}

	return result
}

// equalSymbols checks if two symbol slices are equal
func equalSymbols(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// materializeRelationsForPattern materializes only relations that share symbols with the pattern
// These relations will be used for binding-based filtering AND joined with the result,
// requiring them to be iterated multiple times
func materializeRelationsForPattern(pattern *query.DataPattern, relations Relations) Relations {
	// Extract pattern symbols - what variables does this pattern bind?
	patternSymbols := pattern.Symbols()
	if len(patternSymbols) == 0 {
		// No variables in pattern - no bindings needed
		return relations
	}

	// Build set for fast lookup
	patternSymbolSet := make(map[query.Symbol]bool)
	for _, sym := range patternSymbols {
		patternSymbolSet[sym] = true
	}

	// Materialize relations that share symbols with the pattern
	result := make(Relations, len(relations))
	for i, rel := range relations {
		hasSharedSymbol := false
		for _, sym := range rel.Symbols() {
			if patternSymbolSet[sym] {
				hasSharedSymbol = true
				break
			}
		}

		if hasSharedSymbol {
			// This relation shares symbols with the pattern - materialize it
			// It will be used for binding-based filtering AND joining
			result[i] = rel.Materialize()
		} else {
			// No shared symbols - keep as-is (pure streaming)
			result[i] = rel
		}
	}

	return result
}
