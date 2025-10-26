package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// MockPatternMatcherWithStats tracks match calls for verification
type MockPatternMatcherWithStats struct {
	datoms     []datalog.Datom
	matchCalls int
}

func (m *MockPatternMatcherWithStats) Match(pattern *query.DataPattern) ([]datalog.Datom, error) {
	m.matchCalls++
	return m.matchWithoutBindings(pattern)
}

func (m *MockPatternMatcherWithStats) MatchWithRelation(pattern *query.DataPattern, bindingRel Relation) ([]datalog.Datom, error) {
	if bindingRel == nil {
		return m.Match(pattern)
	}

	if bindingRel.Size() == 0 {
		return []datalog.Datom{}, nil
	}

	// This is the key: ONE call handles MULTIPLE bindings
	m.matchCalls++

	// Simulate the actual matching logic
	projected := bindingRel.ProjectFromPattern(pattern)
	if projected.Size() == 0 {
		return []datalog.Datom{}, nil
	}

	var results []datalog.Datom
	seen := make(map[string]bool)

	// Process each tuple in one go
	for _, tuple := range projected.Sorted() {
		// Simulate matching with this tuple's bindings
		for _, d := range m.datoms {
			if m.matchesWithTuple(d, pattern, tuple, projected) {
				key := fmt.Sprintf("%v:%v:%v:%v", d.E, d.A, d.V, d.Tx)
				if !seen[key] {
					seen[key] = true
					results = append(results, d)
				}
			}
		}
	}

	return results, nil
}

func (m *MockPatternMatcherWithStats) matchWithoutBindings(pattern *query.DataPattern) ([]datalog.Datom, error) {
	var results []datalog.Datom
	for _, d := range m.datoms {
		// Simple pattern matching
		match := true
		if pattern.GetE() != nil {
			if c, ok := pattern.GetE().(query.Constant); ok {
				if !d.E.Equal(c.Value.(datalog.Identity)) {
					match = false
				}
			}
		}
		if pattern.GetA() != nil {
			if c, ok := pattern.GetA().(query.Constant); ok {
				if d.A != c.Value.(datalog.Keyword) {
					match = false
				}
			}
		}
		if match {
			results = append(results, d)
		}
	}
	return results, nil
}

func (m *MockPatternMatcherWithStats) matchesWithTuple(d datalog.Datom, pattern *query.DataPattern, tuple Tuple, rel Relation) bool {
	cols := rel.Columns()

	// Check each pattern element
	elements := []struct {
		patternElem query.PatternElement
		datomValue  interface{}
	}{
		{pattern.GetE(), d.E},
		{pattern.GetA(), d.A},
		{pattern.GetV(), d.V},
	}

	for _, elem := range elements {
		if elem.patternElem == nil {
			continue
		}

		if v, ok := elem.patternElem.(query.Variable); ok {
			// Find this variable in the relation columns
			for i, col := range cols {
				if col == v.Name && i < len(tuple) {
					// Check if the binding matches
					if !matchValuesEqual(tuple[i], elem.datomValue) {
						return false
					}
					break
				}
			}
		} else if c, ok := elem.patternElem.(query.Constant); ok {
			if !matchValuesEqual(c.Value, elem.datomValue) {
				return false
			}
		}
	}

	return true
}

func matchValuesEqual(a, b interface{}) bool {
	if id1, ok1 := a.(datalog.Identity); ok1 {
		if id2, ok2 := b.(datalog.Identity); ok2 {
			return id1.Equal(id2)
		}
	}
	return a == b
}

func TestMultiMatchOptimization(t *testing.T) {
	// Create test data: 1000 entities with attributes
	var datoms []datalog.Datom
	nameAttr := datalog.NewKeyword(":entity/name")
	valueAttr := datalog.NewKeyword(":entity/value")

	for i := 0; i < 1000; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: fmt.Sprintf("Entity%d", i), Tx: 1},
			datalog.Datom{E: e, A: valueAttr, V: int64(i * 10), Tx: 1},
		)
	}

	t.Run("OldApproach_ManyBindings", func(t *testing.T) {
		matcher := &MockPatternMatcherWithStats{datoms: datoms}

		// Create 100 entity bindings
		var entities []datalog.Identity
		for i := 0; i < 100; i++ {
			entities = append(entities, datalog.NewIdentity(fmt.Sprintf("entity:%d", i)))
		}

		// Pattern: [?e :entity/value ?v]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Constant{Value: valueAttr},
				query.Variable{Name: "?v"},
			},
		}

		// Old approach: call MatchWithRelation once per entity
		start := time.Now()
		var allResults []datalog.Datom
		seen := make(map[string]bool)

		for _, entity := range entities {
			// Create single-row relation
			singleRel := NewMaterializedRelation([]query.Symbol{"?e"}, []Tuple{{entity}})

			results, err := matcher.MatchWithRelation(pattern, singleRel)
			if err != nil {
				t.Fatal(err)
			}

			// Deduplicate
			for _, r := range results {
				key := fmt.Sprintf("%v:%v:%v", r.E, r.A, r.V)
				if !seen[key] {
					seen[key] = true
					allResults = append(allResults, r)
				}
			}
		}
		oldDuration := time.Since(start)

		t.Logf("Old approach: %d calls, %d results, %v", matcher.matchCalls, len(allResults), oldDuration)
		if matcher.matchCalls != 100 {
			t.Errorf("Expected 100 match calls, got %d", matcher.matchCalls)
		}
	})

	t.Run("NewApproach_MultiRowRelation", func(t *testing.T) {
		matcher := &MockPatternMatcherWithStats{datoms: datoms}

		// Create 100 entity bindings
		var tuples []Tuple
		for i := 0; i < 100; i++ {
			entity := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
			tuples = append(tuples, Tuple{entity})
		}

		// Create multi-row relation with all entities
		multiRel := NewMaterializedRelation([]query.Symbol{"?e"}, tuples)

		// Pattern: [?e :entity/value ?v]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Constant{Value: valueAttr},
				query.Variable{Name: "?v"},
			},
		}

		// New approach: ONE call with multi-row relation
		start := time.Now()
		results, err := matcher.MatchWithRelation(pattern, multiRel)
		if err != nil {
			t.Fatal(err)
		}
		newDuration := time.Since(start)

		t.Logf("New approach: %d calls, %d results, %v", matcher.matchCalls, len(results), newDuration)
		if matcher.matchCalls != 1 {
			t.Errorf("Expected 1 match call, got %d", matcher.matchCalls)
		}

		if len(results) != 100 {
			t.Errorf("Expected 100 results, got %d", len(results))
		}
	})

	t.Run("ProofOfCorrectness", func(t *testing.T) {
		// Verify that multi-row relations produce the same results as individual calls
		matcher := &MockPatternMatcherWithStats{datoms: datoms}

		// Test entities
		entities := []datalog.Identity{
			datalog.NewIdentity("entity:5"),
			datalog.NewIdentity("entity:10"),
			datalog.NewIdentity("entity:15"),
		}

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Constant{Value: valueAttr},
				query.Variable{Name: "?v"},
			},
		}

		// Method 1: Individual calls
		var individualResults []datalog.Datom
		for _, e := range entities {
			rel := NewMaterializedRelation([]query.Symbol{"?e"}, []Tuple{{e}})
			results, _ := matcher.MatchWithRelation(pattern, rel)
			individualResults = append(individualResults, results...)
		}

		// Reset matcher
		matcher.matchCalls = 0

		// Method 2: Multi-row relation
		var tuples []Tuple
		for _, e := range entities {
			tuples = append(tuples, Tuple{e})
		}
		multiRel := NewMaterializedRelation([]query.Symbol{"?e"}, tuples)
		multiResults, _ := matcher.MatchWithRelation(pattern, multiRel)

		// Compare results
		if len(individualResults) != len(multiResults) {
			t.Errorf("Result count mismatch: individual=%d, multi=%d",
				len(individualResults), len(multiResults))
		}

		// Verify all individual results are in multi results
		for _, ir := range individualResults {
			found := false
			for _, mr := range multiResults {
				if ir.E.Equal(mr.E) && ir.A == mr.A && ir.V == mr.V {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Result %v from individual calls not found in multi results", ir)
			}
		}

		t.Logf("✅ Correctness verified: both methods produce identical results")
		t.Logf("   Individual calls: %d results", len(individualResults))
		t.Logf("   Multi-row call: %d results", len(multiResults))
	})
}
