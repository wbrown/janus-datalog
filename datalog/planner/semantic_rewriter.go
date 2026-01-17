package planner

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// SemanticRewriter performs clause-to-clause transformations for optimization.
// Currently implements time extraction rewriting: [(year ?t) ?py] + [(= ?py 2025)]
// becomes [(>= ?t start)] + [(< ?t end)] for efficient range-based filtering.
type SemanticRewriter struct {
	options PlannerOptions
}

// NewSemanticRewriter creates a new semantic rewriter
func NewSemanticRewriter(options PlannerOptions) *SemanticRewriter {
	return &SemanticRewriter{options: options}
}

// Rewrite applies semantic transformations to a clause list
func (r *SemanticRewriter) Rewrite(clauses []query.Clause) []query.Clause {
	if !r.options.EnableSemanticRewriting {
		return clauses
	}

	return r.rewriteTimeExtractions(clauses)
}

// timeExtractionInfo holds info about a time extraction expression
type timeExtractionInfo struct {
	field     string       // "year", "month", "day", "hour", "minute", "second"
	sourceVar query.Symbol // The variable holding the time value
	resultVar query.Symbol // The variable holding the extracted component
	exprIndex int          // Index in clause list
}

// matchedPattern represents a time extraction + equality pattern that can be rewritten
type matchedPattern struct {
	info          *timeExtractionInfo
	comparedValue int64
	predIndex     int
}

// rewriteTimeExtractions transforms time extraction + equality patterns into range predicates
func (r *SemanticRewriter) rewriteTimeExtractions(clauses []query.Clause) []query.Clause {
	// Step 1: Find all time extraction expressions
	timeExprs := make(map[query.Symbol]*timeExtractionInfo) // resultVar -> info

	for i, clause := range clauses {
		expr, ok := clause.(*query.Expression)
		if !ok {
			continue
		}

		timeFunc, ok := expr.Function.(*query.TimeExtractionFunction)
		if !ok {
			continue
		}

		// Get the source variable from the time term
		sourceVar, isVar := getVariableFromTerm(timeFunc.TimeTerm)
		if !isVar {
			continue
		}

		// Time extraction only supports scalar binding
		bindingSym, ok := expr.Binding.(query.Symbol)
		if !ok {
			continue
		}

		timeExprs[bindingSym] = &timeExtractionInfo{
			field:     timeFunc.Field,
			sourceVar: sourceVar,
			resultVar: bindingSym,
			exprIndex: i,
		}
	}

	if len(timeExprs) == 0 {
		return clauses
	}

	// Step 2: Find equality comparisons that use time extraction results
	var matches []matchedPattern

	for i, clause := range clauses {
		comp, ok := clause.(*query.Comparison)
		if !ok {
			continue
		}

		if comp.Op != query.OpEQ {
			continue
		}

		// Check both sides of equality
		var info *timeExtractionInfo
		var constVal int64
		var found bool

		// Try left as variable
		if leftVar, ok := comp.Left.(query.VariableTerm); ok {
			if i, exists := timeExprs[leftVar.Symbol]; exists {
				info = i
				constVal, found = getConstantInt(comp.Right)
			}
		}

		// Try right as variable
		if !found {
			if rightVar, ok := comp.Right.(query.VariableTerm); ok {
				if i, exists := timeExprs[rightVar.Symbol]; exists {
					info = i
					constVal, found = getConstantInt(comp.Left)
				}
			}
		}

		if found {
			matches = append(matches, matchedPattern{
				info:          info,
				comparedValue: constVal,
				predIndex:     i,
			})
		}
	}

	if len(matches) == 0 {
		return clauses
	}

	// Step 3: Group matches by source variable
	grouped := make(map[query.Symbol][]matchedPattern)
	for _, m := range matches {
		grouped[m.info.sourceVar] = append(grouped[m.info.sourceVar], m)
	}

	// Step 4: Build replacement clauses and indices to remove
	indicesToRemove := make(map[int]bool)
	var replacements []query.Clause

	for _, patterns := range grouped {
		// Compose time range from all components
		start, end := composeTimeRange(patterns)

		// Add range predicates
		sourceVar := patterns[0].info.sourceVar

		// Create [(>= ?source start)]
		replacements = append(replacements, &query.Comparison{
			Op:    query.OpGTE,
			Left:  query.VariableTerm{Symbol: sourceVar},
			Right: query.ConstantTerm{Value: start},
		})

		// Create [(< ?source end)]
		replacements = append(replacements, &query.Comparison{
			Op:    query.OpLT,
			Left:  query.VariableTerm{Symbol: sourceVar},
			Right: query.ConstantTerm{Value: end},
		})

		// Mark original clauses for removal
		for _, p := range patterns {
			indicesToRemove[p.info.exprIndex] = true
			indicesToRemove[p.predIndex] = true
		}
	}

	// Step 5: Build result clause list
	result := make([]query.Clause, 0, len(clauses)-len(indicesToRemove)+len(replacements))

	for i, clause := range clauses {
		if !indicesToRemove[i] {
			result = append(result, clause)
		}
	}

	// Add replacement range predicates
	result = append(result, replacements...)

	return result
}

// composeTimeRange computes start and end times from matched patterns
func composeTimeRange(patterns []matchedPattern) (time.Time, time.Time) {
	var year, month, day, hour, minute, second *int

	for _, p := range patterns {
		val := int(p.comparedValue)
		switch p.info.field {
		case "year":
			year = &val
		case "month":
			month = &val
		case "day":
			day = &val
		case "hour":
			hour = &val
		case "minute":
			minute = &val
		case "second":
			second = &val
		}
	}

	// Default values
	y := 1970
	m := 1
	d := 1
	h := 0
	min := 0
	sec := 0

	if year != nil {
		y = *year
	}
	if month != nil {
		m = *month
	}
	if day != nil {
		d = *day
	}
	if hour != nil {
		h = *hour
	}
	if minute != nil {
		min = *minute
	}
	if second != nil {
		sec = *second
	}

	start := time.Date(y, time.Month(m), d, h, min, sec, 0, time.UTC)

	// Calculate end based on least specific component
	var end time.Time
	if second != nil {
		end = start.Add(time.Second)
	} else if minute != nil {
		end = start.Add(time.Minute)
	} else if hour != nil {
		end = start.Add(time.Hour)
	} else if day != nil {
		end = start.AddDate(0, 0, 1)
	} else if month != nil {
		end = start.AddDate(0, 1, 0)
	} else if year != nil {
		end = start.AddDate(1, 0, 0)
	} else {
		// No constraints
		end = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	}

	return start, end
}

// getVariableFromTerm extracts a symbol from a term if it's a variable
func getVariableFromTerm(term query.Term) (query.Symbol, bool) {
	if v, ok := term.(query.VariableTerm); ok {
		return v.Symbol, true
	}
	return "", false
}

// getConstantInt extracts an integer constant from a term
func getConstantInt(term query.Term) (int64, bool) {
	c, ok := term.(query.ConstantTerm)
	if !ok {
		return 0, false
	}

	switch v := c.Value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}
