package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// MockPatternMatcher implements PatternMatcher for testing
type MockPatternMatcher struct {
	data map[string][]datalog.Datom
}

// Match implements the new PatternMatcher interface
func (m *MockPatternMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	// First get all matching datoms
	var allDatoms []datalog.Datom

	// Check all stored datoms
	for _, datoms := range m.data {
		for _, d := range datoms {
			if matchesDatomWithPattern(d, pattern) {
				allDatoms = append(allDatoms, d)
			}
		}
	}

	// If no bindings, return all matches as a relation
	if bindings == nil || len(bindings) == 0 {
		return PatternToRelation(allDatoms, pattern), nil
	}

	// Find best binding relation for this pattern
	bindingRel := bindings.FindBestForPattern(pattern)
	if bindingRel == nil || bindingRel.IsEmpty() {
		return PatternToRelation(allDatoms, pattern), nil
	}

	// Filter datoms based on bindings
	var filteredDatoms []datalog.Datom

	// For each tuple in the binding relation
	it := bindingRel.Iterator()
	for it.Next() {
		tuple := it.Tuple()

		// Create a map of bound values
		boundValues := make(map[query.Symbol]interface{})
		cols := bindingRel.Columns()
		for i, col := range cols {
			if i < len(tuple) {
				boundValues[col] = tuple[i]
			}
		}

		// Check each datom against the bound values
		for _, d := range allDatoms {
			if matchesBoundPattern(d, pattern, boundValues) {
				filteredDatoms = append(filteredDatoms, d)
			}
		}
	}
	it.Close()

	return PatternToRelation(filteredDatoms, pattern), nil
}

// Helper to check if a datom matches a pattern with bound values
func matchesBoundPattern(d datalog.Datom, pattern *query.DataPattern, boundValues map[query.Symbol]interface{}) bool {
	// Check E position
	if v, ok := pattern.GetE().(query.Variable); ok {
		if boundVal, hasBound := boundValues[v.Name]; hasBound {
			if !matchesConstant(d.E, boundVal) {
				return false
			}
		}
	}

	// Check A position
	if v, ok := pattern.GetA().(query.Variable); ok {
		if boundVal, hasBound := boundValues[v.Name]; hasBound {
			if !matchesConstant(d.A, boundVal) {
				return false
			}
		}
	}

	// Check V position
	if v, ok := pattern.GetV().(query.Variable); ok {
		if boundVal, hasBound := boundValues[v.Name]; hasBound {
			if !matchesConstant(d.V, boundVal) {
				return false
			}
		}
	}

	// Check T position if present
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(query.Variable); ok {
			if boundVal, hasBound := boundValues[v.Name]; hasBound {
				if !matchesConstant(d.Tx, boundVal) {
					return false
				}
			}
		}
	}

	return true
}
