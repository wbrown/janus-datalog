package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// MockPatternMatcher implements PatternMatcher for testing.
//
// It carries options and attaches them to every relation it builds, exactly as
// the production matchers do. A fixture that discarded them would hand its
// consumers a relation with no configuration and no handler — nil is a valid
// handler, but a fixture that silently drops the one it was given makes the
// engine look like it lost it.
type MockPatternMatcher struct {
	data    map[string][]datalog.Datom
	options ExecutorOptions
}

// Match implements the new PatternMatcher interface
func (m *MockPatternMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
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
		return datomsToRelationWithOptions(allDatoms, pattern, pattern.Symbols(), m.options), nil
	}

	// Find best binding relation for this pattern
	bindingRel := bindings.FindBestForPattern(pattern)
	if bindingRel == nil {
		return datomsToRelationWithOptions(allDatoms, pattern, pattern.Symbols(), m.options), nil
	}
	if bindingRel.Size() == 0 {
		// An errored relation that materialized empty is not an empty
		// binding — the fixture honors the same contract as the production
		// matchers.
		if err := EmptyRelationError(bindingRel); err != nil {
			return nil, err
		}
		return datomsToRelationWithOptions(allDatoms, pattern, pattern.Symbols(), m.options), nil
	}

	// Filter datoms based on bindings
	var filteredDatoms []datalog.Datom

	// For each tuple in the binding relation
	it := bindingRel.Iterator()
	for it.Next() {
		tuple := it.Tuple()

		// Create a map of bound values
		boundValues := make(map[query.Symbol]interface{})
		bindTuple(boundValues, bindingRel.Symbols(), tuple)

		// Check each datom against the bound values
		for _, d := range allDatoms {
			if matchesBoundPattern(d, pattern, boundValues) {
				filteredDatoms = append(filteredDatoms, d)
			}
		}
	}
	scanErr := it.Error()
	if closeErr := it.Close(); scanErr == nil {
		scanErr = closeErr
	}
	if scanErr != nil {
		return nil, scanErr
	}

	return datomsToRelationWithOptions(filteredDatoms, pattern, pattern.Symbols(), m.options), nil
}

// matchesDatomPattern checks if a datom matches a pattern with bound values
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
