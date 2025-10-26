package planner

import (
	"github.com/wbrown/janus-datalog/datalog/constraints"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TimeExtractionPattern represents a detected time extraction + equality pattern
// Example: [(year ?time) ?py] followed by [(= ?py 2025)]
type TimeExtractionPattern struct {
	Function      string       // "year", "month", "day", "hour", "minute", "second"
	SourceVar     query.Symbol // ?time (the time value being extracted from)
	ResultVar     query.Symbol // ?py (the variable holding the extracted value)
	ComparedValue interface{}  // The constant it's compared to (e.g., 2025)
	PatternIndex  int          // Which pattern binds the source variable
	Position      int          // Position in pattern (E=0, A=1, V=2, Tx=3)
	ExprIndex     int          // Index of expression in phase
	PredIndex     int          // Index of comparison predicate in phase
}

// detectTimeExtractionPatterns finds time extraction + equality patterns in a phase
func detectTimeExtractionPatterns(phase *Phase) []TimeExtractionPattern {
	var patterns []TimeExtractionPattern

	// Build map of which pattern binds each variable
	varToPattern := make(map[query.Symbol]int)
	varToPosition := make(map[query.Symbol]int)

	for patIdx, patPlan := range phase.Patterns {
		// Only look at data patterns
		dataPat, ok := patPlan.Pattern.(*query.DataPattern)
		if !ok || len(dataPat.Elements) < 3 {
			continue
		}

		// Elements are [E, A, V, Tx?]
		// Check each position in the pattern
		if v, ok := dataPat.Elements[0].(query.Variable); ok {
			varToPattern[v.Name] = patIdx
			varToPosition[v.Name] = 0
		}
		if v, ok := dataPat.Elements[2].(query.Variable); ok {
			varToPattern[v.Name] = patIdx
			varToPosition[v.Name] = 2
		}
	}

	// Map expression result variables to their function and source
	exprResults := make(map[query.Symbol]*TimeExtractionInfo)

	for exprIdx, exprPlan := range phase.Expressions {
		if timeFunc, ok := exprPlan.Expression.Function.(*query.TimeExtractionFunction); ok {
			// Check if source is a variable bound by a pattern
			if sourceVar, isVar := getVariableFromTerm(timeFunc.TimeTerm); isVar {
				if _, bound := varToPattern[sourceVar]; bound {
					exprResults[exprPlan.Expression.Binding] = &TimeExtractionInfo{
						Function:  timeFunc.Field,
						SourceVar: sourceVar,
						ExprIndex: exprIdx,
					}
				}
			}
		}
	}

	// Find predicates that compare extracted values to constants
	for predIdx, predPlan := range phase.Predicates {
		// Look for [(= ?resultVar constant)]
		if comp, ok := predPlan.Predicate.(*query.Comparison); ok {
			if comp.Op == query.OpEQ {
				// Check both sides of equality
				leftVar, leftIsVar := comp.Left.(query.VariableTerm)
				rightVar, rightIsVar := comp.Right.(query.VariableTerm)

				var resultVar query.Symbol
				var constValue interface{}
				var found bool

				if leftIsVar {
					if info, exists := exprResults[leftVar.Symbol]; exists {
						resultVar = leftVar.Symbol
						constValue, found = getConstantValue(comp.Right)
						if found {
							patterns = append(patterns, TimeExtractionPattern{
								Function:      info.Function,
								SourceVar:     info.SourceVar,
								ResultVar:     resultVar,
								ComparedValue: constValue,
								PatternIndex:  varToPattern[info.SourceVar],
								Position:      varToPosition[info.SourceVar],
								ExprIndex:     info.ExprIndex,
								PredIndex:     predIdx,
							})
						}
					}
				}

				if rightIsVar {
					if info, exists := exprResults[rightVar.Symbol]; exists {
						resultVar = rightVar.Symbol
						constValue, found = getConstantValue(comp.Left)
						if found {
							patterns = append(patterns, TimeExtractionPattern{
								Function:      info.Function,
								SourceVar:     info.SourceVar,
								ResultVar:     resultVar,
								ComparedValue: constValue,
								PatternIndex:  varToPattern[info.SourceVar],
								Position:      varToPosition[info.SourceVar],
								ExprIndex:     info.ExprIndex,
								PredIndex:     predIdx,
							})
						}
					}
				}
			}
		}
	}

	return patterns
}

type TimeExtractionInfo struct {
	Function  string
	SourceVar query.Symbol
	ExprIndex int
}

// groupTimePatternsBySource groups patterns by their source variable
func groupTimePatternsBySource(patterns []TimeExtractionPattern) map[query.Symbol][]TimeExtractionPattern {
	grouped := make(map[query.Symbol][]TimeExtractionPattern)
	for _, pat := range patterns {
		grouped[pat.SourceVar] = append(grouped[pat.SourceVar], pat)
	}
	return grouped
}

// composeTimeConstraint combines multiple time predicates into a single constraint
func composeTimeConstraint(patterns []TimeExtractionPattern) constraints.StorageConstraint {
	if len(patterns) == 0 {
		return nil
	}

	var year, month, day, hour, minute, second *int

	for _, pat := range patterns {
		val, ok := pat.ComparedValue.(int64)
		if !ok {
			continue
		}
		intVal := int(val)

		switch pat.Function {
		case "year":
			year = &intVal
		case "month":
			month = &intVal
		case "day":
			day = &intVal
		case "hour":
			hour = &intVal
		case "minute":
			minute = &intVal
		case "second":
			second = &intVal
		}
	}

	// Use the position from the first pattern (they should all be the same source)
	position := patterns[0].Position

	return constraints.ComposeTimeConstraint(year, month, day, hour, minute, second, position)
}

// rewriteTimePredicates applies semantic rewriting to time extraction predicates
func rewriteTimePredicates(plan *QueryPlan, opts PlannerOptions) {
	if !opts.EnableSemanticRewriting {
		return
	}

	for phaseIdx := range plan.Phases {
		phase := &plan.Phases[phaseIdx]

		// Detect time extraction patterns
		patterns := detectTimeExtractionPatterns(phase)
		if len(patterns) == 0 {
			continue
		}

		// Group by source variable
		grouped := groupTimePatternsBySource(patterns)

		// For each group, compose and inject constraint
		for _, group := range grouped {
			constraint := composeTimeConstraint(group)
			if constraint == nil {
				continue
			}

			// Inject constraint into the pattern that binds the source variable
			patternIdx := group[0].PatternIndex
			pattern := &phase.Patterns[patternIdx]

			// Store constraints in metadata
			if pattern.Metadata == nil {
				pattern.Metadata = make(map[string]interface{})
			}

			// Get existing constraints or create new slice
			var existingConstraints []constraints.StorageConstraint
			if existing, ok := pattern.Metadata["storage_constraints"]; ok {
				existingConstraints = existing.([]constraints.StorageConstraint)
			}

			// Append new constraint
			pattern.Metadata["storage_constraints"] = append(existingConstraints, constraint)

			// Mark expressions and predicates as handled by constraint
			for _, pat := range group {
				// Mark the time extraction expression as optimized
				exprPlan := &phase.Expressions[pat.ExprIndex]
				if exprPlan.Metadata == nil {
					exprPlan.Metadata = make(map[string]interface{})
				}
				exprPlan.Metadata["optimized_by_constraint"] = true

				// Mark the equality predicate as optimized
				predPlan := &phase.Predicates[pat.PredIndex]
				if predPlan.Metadata == nil {
					predPlan.Metadata = make(map[string]interface{})
				}
				predPlan.Metadata["optimized_by_constraint"] = true
			}
		}
	}
}

// Helper functions

func isVariable(term query.Term) bool {
	_, ok := term.(query.VariableTerm)
	return ok
}

func getVariableFromTerm(term query.Term) (query.Symbol, bool) {
	if v, ok := term.(query.VariableTerm); ok {
		return v.Symbol, true
	}
	return "", false
}

func getConstantValue(term query.Term) (interface{}, bool) {
	if c, ok := term.(query.ConstantTerm); ok {
		return c.Value, true
	}
	return nil, false
}
