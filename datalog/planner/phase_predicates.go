package planner

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PushPredicates analyzes predicates in this phase and pushes them to storage
func (p *Phase) PushPredicates() {
	// First, combine time extractions with equality/comparison predicates
	p.combineTimeExtractions()

	// Group patterns by shared entity variable
	entityGroups := groupPatternsByEntity(p.Patterns)

	// For each group of patterns sharing an entity
	for entityVar, patterns := range entityGroups {
		// Collect predicates that involve this entity
		predicates := collectEntityPredicates(*p, entityVar, patterns)

		// Apply constraints to each pattern
		for i := range patterns {
			patterns[i].ApplyConstraints(predicates, *p)
			// Update the pattern in the phase
			for j := range p.Patterns {
				if patternsEqual(p.Patterns[j], patterns[i]) {
					p.Patterns[j] = patterns[i]
					break
				}
			}
		}
	}

	// Remove predicates that have been pushed to storage
	p.Predicates = removePushedPredicates(p.Predicates, p.Patterns)
}

// patternsEqual checks if two pattern plans are the same
func patternsEqual(a, b PatternPlan) bool {
	// Simple equality check using string representation
	return fmt.Sprintf("%v", a.Pattern) == fmt.Sprintf("%v", b.Pattern)
}

// Helper functions

// groupPatternsByEntity groups patterns by their entity variable (first element)
func groupPatternsByEntity(patterns []PatternPlan) map[query.Symbol][]PatternPlan {
	groups := make(map[query.Symbol][]PatternPlan)

	for _, pattern := range patterns {
		if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
			// Get the entity variable (first element)
			if len(dp.Elements) > 0 {
				if varElem, ok := dp.Elements[0].(query.Variable); ok {
					entityVar := query.Symbol(varElem.Name)
					groups[entityVar] = append(groups[entityVar], pattern)
				}
			}
		}
	}

	return groups
}

// collectEntityPredicates finds all predicates related to an entity
func collectEntityPredicates(phase Phase, entityVar query.Symbol, patterns []PatternPlan) []PredicatePlan {
	// Collect all variables provided by patterns with this entity
	entityVars := make(map[query.Symbol]bool)
	entityVars[entityVar] = true

	for _, pattern := range patterns {
		if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
			for _, elem := range dp.Elements {
				if v, ok := elem.(query.Variable); ok {
					entityVars[query.Symbol(v.Name)] = true
				}
			}
		}
	}

	// Return ALL predicates for now - let each pattern decide what it can use
	// This ensures we don't miss time extraction predicates that need specific attributes
	return phase.Predicates
}

// predicateUsesVars checks if a predicate uses any of the given variables
func predicateUsesVars(pred PredicatePlan, vars map[query.Symbol]bool) bool {
	// Check the main variable
	if vars[pred.Variable] {
		return true
	}

	// Check any referenced variables in the predicate
	for _, v := range pred.RequiredVars {
		if vars[v] {
			return true
		}
	}

	return false
}

// removePushedPredicates removes predicates that have been pushed to storage
func removePushedPredicates(predicates []PredicatePlan, patterns []PatternPlan) []PredicatePlan {
	// Collect all constraints that were pushed
	var pushedConstraints []StorageConstraint
	for _, pattern := range patterns {
		if pattern.Metadata != nil {
			if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
				pushedConstraints = append(pushedConstraints, constraints...)
			}
		}
	}

	// Keep predicates that weren't converted to storage constraints
	var remaining []PredicatePlan
	for _, pred := range predicates {
		pushed := false
		for _, constraint := range pushedConstraints {
			if constraintMatchesPredicate(constraint, pred) {
				pushed = true
				break
			}
		}
		if !pushed {
			remaining = append(remaining, pred)
		}
	}

	return remaining
}

// constraintMatchesPredicate checks if a constraint was derived from a predicate
func constraintMatchesPredicate(constraint StorageConstraint, pred PredicatePlan) bool {
	if constraint.Type == ConstraintTimeExtraction && pred.Type == PredicateTimeExtraction {
		return constraint.TimeField == pred.TimeField &&
			constraint.Value == pred.Value &&
			constraint.Operator == pred.Operator
	}

	if constraint.Type == ConstraintRange && pred.Type == PredicateComparison {
		return constraint.Value == pred.Value &&
			constraint.Operator == pred.Operator
	}

	if constraint.Type == ConstraintEquality && pred.Type == PredicateEquality {
		return constraint.Value == pred.Value
	}

	return false
}

// analyzeSelectivity estimates the selectivity of pushing predicates to a pattern
func analyzeSelectivity(pattern PatternPlan) float64 {
	selectivity := 1.0

	if pattern.Metadata == nil {
		return selectivity
	}

	constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint)
	if !ok {
		return selectivity
	}

	for _, constraint := range constraints {
		switch constraint.Type {
		case ConstraintEquality:
			selectivity *= 0.01 // Exact match is very selective
		case ConstraintRange:
			selectivity *= 0.2 // Range typically covers 20% of values
		case ConstraintTimeExtraction:
			if constraint.TimeField == "day" {
				selectivity *= 1.0 / 30 // ~30 days per month
			} else if constraint.TimeField == "hour" {
				selectivity *= 1.0 / 24 // 24 hours per day
			} else if constraint.TimeField == "month" {
				selectivity *= 1.0 / 12 // 12 months per year
			}
		}
	}

	return selectivity
}
