package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestIndexedMatcher_IndexBuilding verifies that indices are built correctly
func TestIndexedMatcher_IndexBuilding(t *testing.T) {
	// Create test datoms
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("age"), V: int64(30), Tx: 1},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("name"), V: "Bob", Tx: 1},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("age"), V: int64(25), Tx: 1},
	}

	matcher := NewIndexedMemoryMatcher(datoms)
	matcher.buildIndices()

	// Verify entity index
	e1 := datalog.NewIdentity("e1")
	e1Key := e1.L85()
	if positions := matcher.entityIndex[e1Key]; len(positions) != 2 {
		t.Errorf("Entity index for e1: expected 2 datoms, got %d", len(positions))
	}

	e2 := datalog.NewIdentity("e2")
	e2Key := e2.L85()
	if positions := matcher.entityIndex[e2Key]; len(positions) != 2 {
		t.Errorf("Entity index for e2: expected 2 datoms, got %d", len(positions))
	}

	// Verify attribute index
	nameKw := datalog.NewKeyword("name")
	nameKey := nameKw.String()
	if positions := matcher.attributeIndex[nameKey]; len(positions) != 2 {
		t.Errorf("Attribute index for :name: expected 2 datoms, got %d", len(positions))
	}

	ageKw := datalog.NewKeyword("age")
	ageKey := ageKw.String()
	if positions := matcher.attributeIndex[ageKey]; len(positions) != 2 {
		t.Errorf("Attribute index for :age: expected 2 datoms, got %d", len(positions))
	}

	// Verify EA index
	e1NameKey := e1Key + "|" + nameKey
	if _, ok := matcher.eavIndex[e1NameKey]; !ok {
		t.Errorf("EA index missing entry for (e1, :name)")
	}

	// Verify value index (by hash)
	aliceHash := hashDatomValue("Alice")
	if positions := matcher.valueIndex[aliceHash]; len(positions) != 1 {
		t.Errorf("Value index for 'Alice': expected 1 datom, got %d", len(positions))
	}
}

// TestIndexedMatcher_StrategySelection verifies correct index selection
func TestIndexedMatcher_StrategySelection(t *testing.T) {
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: 1},
	}

	matcher := NewIndexedMemoryMatcher(datoms)

	tests := []struct {
		pattern      *query.DataPattern
		expectedType string
	}{
		{
			// [e1 :name ?v] - EA bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("e1")},
					query.Constant{Value: datalog.NewKeyword("name")},
					query.Variable{Name: "?v"},
				},
			},
			expectedType: "EA-index",
		},
		{
			// [e1 ?a ?v] - E bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("e1")},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
			expectedType: "E-index",
		},
		{
			// [?e :name ?v] - A bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: datalog.NewKeyword("name")},
					query.Variable{Name: "?v"},
				},
			},
			expectedType: "A-index",
		},
		{
			// [?e ?a "Alice"] - V bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Constant{Value: "Alice"},
				},
			},
			expectedType: "V-index",
		},
		{
			// [?e ?a ?v] - Nothing bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
			expectedType: "linear-scan",
		},
	}

	for _, tt := range tests {
		strategy := matcher.chooseStrategy(tt.pattern)
		if strategy.String() != tt.expectedType {
			t.Errorf("Pattern %v: expected strategy %s, got %s",
				tt.pattern, tt.expectedType, strategy.String())
		}
	}
}

// TestIndexedMatcher_CorrectnessVsLinear compares indexed matcher against linear scan
func TestIndexedMatcher_CorrectnessVsLinear(t *testing.T) {
	// Create diverse test datoms
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword("age"), V: int64(30), Tx: 1},
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword("active"), V: true, Tx: 1},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword("name"), V: "Bob", Tx: 1},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword("age"), V: int64(25), Tx: 1},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword("active"), V: false, Tx: 1},
		{E: datalog.NewIdentity("person3"), A: datalog.NewKeyword("name"), V: "Charlie", Tx: 2},
		{E: datalog.NewIdentity("person3"), A: datalog.NewKeyword("age"), V: int64(35), Tx: 2},
	}

	linear := NewMemoryPatternMatcher(datoms)
	indexed := NewIndexedMemoryMatcher(datoms)

	testPatterns := []struct {
		name    string
		pattern *query.DataPattern
	}{
		{
			name: "EA bound",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("person1")},
					query.Constant{Value: datalog.NewKeyword("name")},
					query.Variable{Name: "?v"},
				},
			},
		},
		{
			name: "E bound",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("person1")},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
		},
		{
			name: "A bound",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: datalog.NewKeyword("name")},
					query.Variable{Name: "?v"},
				},
			},
		},
		{
			name: "V bound (string)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Constant{Value: "Alice"},
				},
			},
		},
		{
			name: "V bound (int64)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Constant{Value: int64(30)},
				},
			},
		},
		{
			name: "V bound (bool)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Constant{Value: true},
				},
			},
		},
		{
			name: "Nothing bound (full scan)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
		},
	}

	for _, tt := range testPatterns {
		t.Run(tt.name, func(t *testing.T) {
			linearResult, err := linear.Match(tt.pattern, nil)
			if err != nil {
				t.Fatalf("Linear match failed: %v", err)
			}

			indexedResult, err := indexed.Match(tt.pattern, nil)
			if err != nil {
				t.Fatalf("Indexed match failed: %v", err)
			}

			// Compare sizes
			if linearResult.Size() != indexedResult.Size() {
				t.Errorf("Size mismatch: linear=%d, indexed=%d",
					linearResult.Size(), indexedResult.Size())
			}

			// Compare contents (sort for comparison)
			linearTuples := linearResult.Sorted()
			indexedTuples := indexedResult.Sorted()

			if len(linearTuples) != len(indexedTuples) {
				t.Fatalf("Tuple count mismatch: linear=%d, indexed=%d",
					len(linearTuples), len(indexedTuples))
			}

			for i := range linearTuples {
				if !tupleEqualityCheck(linearTuples[i], indexedTuples[i]) {
					t.Errorf("Tuple %d mismatch:\n  linear:  %v\n  indexed: %v",
						i, linearTuples[i], indexedTuples[i])
				}
			}
		})
	}
}

// TestIndexedMatcher_EdgeCases tests edge cases
func TestIndexedMatcher_EdgeCases(t *testing.T) {
	t.Run("Empty dataset", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher([]datalog.Datom{})
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Variable{Name: "?a"},
				query.Variable{Name: "?v"},
			},
		}

		result, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}

		if !result.IsEmpty() {
			t.Errorf("Expected empty result, got %d tuples", result.Size())
		}
	})

	t.Run("Single datom", func(t *testing.T) {
		datoms := []datalog.Datom{
			{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: 1},
		}

		matcher := NewIndexedMemoryMatcher(datoms)
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Constant{Value: datalog.NewKeyword("name")},
				query.Variable{Name: "?v"},
			},
		}

		result, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}

		count := 0
		it := result.Iterator()
		defer it.Close()
		for it.Next() {
			count++
		}
		if count != 1 {
			t.Errorf("Expected 1 result, got %d", count)
		}
	})

	t.Run("Hash collision handling", func(t *testing.T) {
		// Create values that might have hash collisions
		// We can't control hash function, but we can test that different values
		// are correctly distinguished even if they hash to the same value
		datoms := []datalog.Datom{
			{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("value"), V: "test1", Tx: 1},
			{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("value"), V: "test2", Tx: 1},
			{E: datalog.NewIdentity("e3"), A: datalog.NewKeyword("value"), V: "test3", Tx: 1},
		}

		matcher := NewIndexedMemoryMatcher(datoms)

		// Search for specific value - should only match exact value
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Variable{Name: "?a"},
				query.Constant{Value: "test1"},
			},
		}

		result, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}

		count := 0
		var firstTuple Tuple
		it := result.Iterator()
		defer it.Close()
		for it.Next() {
			if count == 0 {
				firstTuple = it.Tuple()
			}
			count++
		}

		if count != 1 {
			t.Errorf("Expected exactly 1 match for 'test1', got %d", count)
		}

		// Verify it's the correct entity
		tuple := firstTuple
		if tuple == nil {
			t.Fatal("Expected non-nil tuple")
		}
		entity := tuple[0].(datalog.Identity)
		if entity.String() != "e1" {
			t.Errorf("Expected entity e1, got %s", entity.String())
		}
	})
}

// TestIndexedMatcher_WithBindings tests pattern matching with binding relations
func TestIndexedMatcher_WithBindings(t *testing.T) {
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("p1"), A: datalog.NewKeyword("age"), V: int64(30), Tx: 1},
		{E: datalog.NewIdentity("p2"), A: datalog.NewKeyword("name"), V: "Bob", Tx: 1},
		{E: datalog.NewIdentity("p2"), A: datalog.NewKeyword("age"), V: int64(25), Tx: 1},
	}

	matcher := NewIndexedMemoryMatcher(datoms)

	// Create binding relation that filters to only p1
	bindingTuples := []Tuple{
		{datalog.NewIdentity("p1")},
	}
	bindings := Relations{
		NewMaterializedRelation([]query.Symbol{"?e"}, bindingTuples),
	}

	// Pattern: [?e :age ?v]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword("age")},
			query.Variable{Name: "?v"},
		},
	}

	result, err := matcher.Match(pattern, bindings)
	if err != nil {
		t.Fatalf("Match with bindings failed: %v", err)
	}

	// Should only match p1's age
	count := 0
	var firstTuple Tuple
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		if count == 0 {
			firstTuple = it.Tuple()
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}

	if firstTuple != nil {
		age := firstTuple[1].(int64)
		if age != 30 {
			t.Errorf("Expected age 30, got %d", age)
		}
	}
}

// TestIndexedMatcher_WithConstraints tests constraint filtering
func TestIndexedMatcher_WithConstraints(t *testing.T) {
	now := time.Now()
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("timestamp"), V: now, Tx: 1},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("timestamp"), V: now.Add(-time.Hour), Tx: 1},
		{E: datalog.NewIdentity("e3"), A: datalog.NewKeyword("timestamp"), V: now.Add(-2 * time.Hour), Tx: 1},
	}

	matcher := NewIndexedMemoryMatcher(datoms)

	// Create time extraction constraint
	constraint := &timeExtractionConstraint{
		position:  2, // Value position
		extractFn: "hour",
		expected:  int64(now.Hour()),
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword("timestamp")},
			query.Variable{Name: "?v"},
		},
	}

	result, err := matcher.MatchWithConstraints(pattern, nil, []StorageConstraint{constraint})
	if err != nil {
		t.Fatalf("Match with constraints failed: %v", err)
	}

	// Should only match datoms where hour(timestamp) == now.Hour()
	count := 0
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 result after constraint filtering, got %d", count)
	}
}

// Helper function to compare tuples for equality (test-local version)
func tupleEqualityCheck(t1, t2 Tuple) bool {
	if len(t1) != len(t2) {
		return false
	}
	for i := range t1 {
		if !datalog.ValuesEqual(t1[i], t2[i]) {
			return false
		}
	}
	return true
}
