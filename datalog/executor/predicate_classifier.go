package executor

import (
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PredicateClassifier analyzes predicates and determines which can be pushed to storage
type PredicateClassifier struct {
	pattern *query.DataPattern
	phase   *planner.Phase
}

// NewPredicateClassifier creates a new classifier for a pattern and phase
func NewPredicateClassifier(pattern *query.DataPattern, phase *planner.Phase) *PredicateClassifier {
	return &PredicateClassifier{
		pattern: pattern,
		phase:   phase,
	}
}

// ClassifyAndConvert analyzes predicates and converts pushable ones to storage constraints
func (pc *PredicateClassifier) ClassifyAndConvert() (pushable []StorageConstraint, remaining []planner.PredicatePlan) {
	patternVars := pc.extractPatternVariables()

	for _, predPlan := range pc.phase.Predicates {
		pred := predPlan.Predicate

		// Check if predicate references only variables from this pattern
		predVars := pc.extractPredicateVariables(pred)
		if !pc.isSubset(predVars, patternVars) {
			// Predicate needs variables from other patterns
			remaining = append(remaining, predPlan)
			continue
		}

		// Try to convert to storage constraint
		if constraint := pc.convertToConstraint(pred, patternVars); constraint != nil {
			pushable = append(pushable, constraint)
		} else {
			remaining = append(remaining, predPlan)
		}
	}

	return pushable, remaining
}

// extractPatternVariables gets all variables from the pattern
func (pc *PredicateClassifier) extractPatternVariables() map[query.Symbol]int {
	vars := make(map[query.Symbol]int)

	// Map variables to their positions
	if v, ok := pc.pattern.GetE().(query.Variable); ok {
		vars[v.Name] = 0 // Entity position
	}
	if v, ok := pc.pattern.GetA().(query.Variable); ok {
		vars[v.Name] = 1 // Attribute position
	}
	if v, ok := pc.pattern.GetV().(query.Variable); ok {
		vars[v.Name] = 2 // Value position
	}
	if len(pc.pattern.Elements) > 3 {
		if v, ok := pc.pattern.GetT().(query.Variable); ok {
			vars[v.Name] = 3 // Transaction position
		}
	}

	return vars
}

// extractPredicateVariables gets all variables referenced in a predicate
func (pc *PredicateClassifier) extractPredicateVariables(pred query.Predicate) []query.Symbol {
	// Use the Predicate interface's RequiredSymbols method
	return pred.RequiredSymbols()
}

// isSubset checks if all elements in 'subset' are in 'set'
func (pc *PredicateClassifier) isSubset(subset []query.Symbol, set map[query.Symbol]int) bool {
	for _, sym := range subset {
		if _, found := set[sym]; !found {
			return false
		}
	}
	return true
}

// convertToConstraint attempts to convert a predicate to a storage constraint
func (pc *PredicateClassifier) convertToConstraint(pred query.Predicate, patternVars map[query.Symbol]int) StorageConstraint {
	// Type switch on the predicate type
	switch p := pred.(type) {
	case *query.Comparison:
		if p.Op == query.OpEQ {
			// Equality predicate: [(= ?var value)] or [(= ?var1 ?var2)]
			// Check if one side is a variable from our pattern and other is constant
			var position int
			var value interface{}
			var foundVar bool

			// Check left term
			if vt, ok := p.Left.(query.VariableTerm); ok {
				if pos, found := patternVars[vt.Symbol]; found {
					position = pos
					foundVar = true
					// Right should be constant
					if ct, ok := p.Right.(query.ConstantTerm); ok {
						value = ct.Value
					} else {
						return nil // Both are variables
					}
				}
			}

			// Check right term if left wasn't a pattern variable
			if !foundVar {
				if vt, ok := p.Right.(query.VariableTerm); ok {
					if pos, found := patternVars[vt.Symbol]; found {
						position = pos
						// Left should be constant
						if ct, ok := p.Left.(query.ConstantTerm); ok {
							value = ct.Value
						} else {
							return nil
						}
					}
				}
			}

			if value != nil {
				return &equalityConstraint{position: position, value: value}
			}
		} else {
			// Range comparison: [(< ?var value)], [(>= ?var value)], etc.
			return pc.convertRangeComparison(p, patternVars)
		}

	case *query.ChainedComparison:
		// Handle chained comparisons like [(< 0 ?x 100)]
		if p.CanPushToStorage() {
			// Middle term should be variable, outer terms constants
			if len(p.Terms) == 3 {
				if vt, ok := p.Terms[1].(query.VariableTerm); ok {
					if pos, found := patternVars[vt.Symbol]; found {
						if ct1, ok1 := p.Terms[0].(query.ConstantTerm); ok1 {
							if ct2, ok2 := p.Terms[2].(query.ConstantTerm); ok2 {
								// Convert to range constraint
								var min, max interface{}
								var includeMin, includeMax bool

								switch p.Op {
								case query.OpLT:
									min = ct1.Value
									max = ct2.Value
									includeMin = false
									includeMax = false
								case query.OpLTE:
									min = ct1.Value
									max = ct2.Value
									includeMin = true
									includeMax = true
								}

								return &rangeConstraint{
									position:   pos,
									min:        min,
									max:        max,
									includeMin: includeMin,
									includeMax: includeMax,
								}
							}
						}
					}
				}
			}
		}

	default:
		// Check if it's a FunctionPredicate for time extraction
		if fp, ok := pred.(*query.FunctionPredicate); ok {
			return pc.convertFunctionPredicate(fp, patternVars)
		}
	}

	// Predicate type not supported for pushdown
	return nil
}

// convertRangeComparison converts a range comparison to a storage constraint
func (pc *PredicateClassifier) convertRangeComparison(comp *query.Comparison, patternVars map[query.Symbol]int) StorageConstraint {
	// Find which element is the variable
	var position int
	var value interface{}
	var isVarFirst bool

	if vt, ok := comp.Left.(query.VariableTerm); ok {
		if pos, found := patternVars[vt.Symbol]; found {
			position = pos
			isVarFirst = true
			if ct, ok := comp.Right.(query.ConstantTerm); ok {
				value = ct.Value
			} else {
				return nil
			}
		}
	} else if vt, ok := comp.Right.(query.VariableTerm); ok {
		if pos, found := patternVars[vt.Symbol]; found {
			position = pos
			isVarFirst = false
			if ct, ok := comp.Left.(query.ConstantTerm); ok {
				value = ct.Value
			} else {
				return nil
			}
		}
	}

	if value == nil {
		return nil
	}

	// Create range constraint based on operator and variable position
	var min, max interface{}
	var includeMin, includeMax bool

	switch comp.Op {
	case query.OpGT:
		if isVarFirst {
			min = value
			includeMin = false
		} else {
			max = value
			includeMax = false
		}
	case query.OpGTE:
		if isVarFirst {
			min = value
			includeMin = true
		} else {
			max = value
			includeMax = true
		}
	case query.OpLT:
		if isVarFirst {
			max = value
			includeMax = false
		} else {
			min = value
			includeMin = false
		}
	case query.OpLTE:
		if isVarFirst {
			max = value
			includeMax = true
		} else {
			min = value
			includeMin = true
		}
	default:
		return nil
	}

	return &rangeConstraint{
		position:   position,
		min:        min,
		max:        max,
		includeMin: includeMin,
		includeMax: includeMax,
	}
}

// convertFunctionPredicate converts function predicates like time extraction
func (pc *PredicateClassifier) convertFunctionPredicate(fp *query.FunctionPredicate, patternVars map[query.Symbol]int) StorageConstraint {
	fnName := fp.Fn

	switch fnName {
	case "day", "month", "year", "hour", "minute", "second":
		// Time extraction functions: [(day ?t) ?d] where ?d is bound
		if len(fp.Args) != 2 {
			return nil
		}

		// First element should be a variable from our pattern
		timeVar, ok := fp.Args[0].(query.Variable)
		if !ok {
			return nil
		}

		position, found := patternVars[query.Symbol(timeVar.Name)]
		if !found {
			return nil
		}

		// Second element determines if this is pushable
		// If it's a variable, check if it's bound in Available symbols
		var expectedValue interface{}

		switch e := fp.Args[1].(type) {
		case query.Constant:
			expectedValue = e.Value
		case query.Variable:
			// Check if this variable is bound (in Available from previous phases)
			for _, sym := range pc.phase.Available {
				if sym == query.Symbol(e.Name) {
					// Variable is bound from previous phase
					// We'd need the actual value at execution time
					// For now, we can't push this down statically
					return nil
				}
			}
			// Variable is not bound - can't push down
			return nil
		default:
			return nil
		}

		if expectedValue != nil {
			return &timeExtractionConstraint{
				position:  position,
				extractFn: fnName,
				expected:  expectedValue,
			}
		}
	}

	return nil
}

// SplitPredicatesForPattern separates predicates into pushable and remaining for a specific pattern
func SplitPredicatesForPattern(pattern *query.DataPattern, predicates []planner.PredicatePlan, phase *planner.Phase) (pushable []StorageConstraint, remaining []planner.PredicatePlan) {
	classifier := NewPredicateClassifier(pattern, phase)
	classifier.phase.Predicates = predicates
	return classifier.ClassifyAndConvert()
}
