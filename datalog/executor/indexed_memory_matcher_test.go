package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestIndexedMatcher_IndexBuilding verifies that indices are built correctly
func TestIndexedMatcher_IndexBuilding(t *testing.T) {
	// Create test datoms
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("age"), V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("name"), V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("age"), V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewIndexedMemoryMatcher(datoms)
	matcher.buildIndices()

	// Verify entity index
	e1 := datalog.NewIdentity("e1")
	if positions := matcher.entityIndex[e1]; len(positions) != 2 {
		t.Errorf("Entity index for e1: expected 2 datoms, got %d", len(positions))
	}

	e2 := datalog.NewIdentity("e2")
	if positions := matcher.entityIndex[e2]; len(positions) != 2 {
		t.Errorf("Entity index for e2: expected 2 datoms, got %d", len(positions))
	}

	// Verify attribute index
	nameKw := datalog.NewKeyword("name")
	if positions := matcher.attributeIndex[nameKw]; len(positions) != 2 {
		t.Errorf("Attribute index for :name: expected 2 datoms, got %d", len(positions))
	}

	ageKw := datalog.NewKeyword("age")
	if positions := matcher.attributeIndex[ageKw]; len(positions) != 2 {
		t.Errorf("Attribute index for :age: expected 2 datoms, got %d", len(positions))
	}

	// Verify EA index
	if _, ok := matcher.eavIndex[eaIndexKey{e: e1, a: nameKw}]; !ok {
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
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
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
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			expectedType: "EA-index",
		},
		{
			// [e1 ?a ?v] - E bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("e1")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			expectedType: "E-index",
		},
		{
			// [?e :name ?v] - A bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword("name")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			expectedType: "A-index",
		},
		{
			// [?e ?a "Alice"] - V bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Constant{Value: "Alice"},
				},
			},
			expectedType: "V-index",
		},
		{
			// [?e ?a ?v] - Nothing bound
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
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
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword("age"), V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword("active"), V: true, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword("name"), V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword("age"), V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword("active"), V: false, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("person3"), A: datalog.NewKeyword("name"), V: "Charlie", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: datalog.NewIdentity("person3"), A: datalog.NewKeyword("age"), V: int64(35), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
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
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
		},
		{
			name: "E bound",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("person1")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
		},
		{
			name: "A bound",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword("name")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
		},
		{
			name: "V bound (string)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Constant{Value: "Alice"},
				},
			},
		},
		{
			name: "V bound (int64)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Constant{Value: int64(30)},
				},
			},
		},
		{
			name: "V bound (bool)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Constant{Value: true},
				},
			},
		},
		{
			name: "Nothing bound (full scan)",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
		},
	}

	for _, tt := range testPatterns {
		t.Run(tt.name, func(t *testing.T) {
			linearResult, err := linear.Match(query.PatternQuery(tt.pattern), nil)
			if err != nil {
				t.Fatalf("Linear match failed: %v", err)
			}

			indexedResult, err := indexed.Match(query.PatternQuery(tt.pattern), nil)
			if err != nil {
				t.Fatalf("Indexed match failed: %v", err)
			}

			// Compare sizes
			if linearResult.Size() != indexedResult.Size() {
				t.Errorf("Size mismatch: linear=%d, indexed=%d",
					linearResult.Size(), indexedResult.Size())
			}

			// Compare contents (sort for comparison)
			linearTuples, err := linearResult.Sorted()
			if err != nil {
				t.Fatalf("linear Sorted() failed: %v", err)
			}
			indexedTuples, err := indexedResult.Sorted()
			if err != nil {
				t.Fatalf("indexed Sorted() failed: %v", err)
			}

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
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Variable{Name: datalog.NewSymbol("?a")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		result, err := matcher.Match(query.PatternQuery(pattern), nil)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}

		if size := result.Materialize().Size(); size != 0 {
			t.Errorf("Expected empty result, got %d tuples", size)
		}
	})

	t.Run("Single datom", func(t *testing.T) {
		datoms := []datalog.Datom{
			{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		}

		matcher := NewIndexedMemoryMatcher(datoms)
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: datalog.NewKeyword("name")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		result, err := matcher.Match(query.PatternQuery(pattern), nil)
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
			{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("value"), V: "test1", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("value"), V: "test2", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			{E: datalog.NewIdentity("e3"), A: datalog.NewKeyword("value"), V: "test3", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		}

		matcher := NewIndexedMemoryMatcher(datoms)

		// Search for specific value - should only match exact value
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Variable{Name: datalog.NewSymbol("?a")},
				query.Constant{Value: "test1"},
			},
		}

		result, err := matcher.Match(query.PatternQuery(pattern), nil)
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
		if !entity.Equal(datalog.NewIdentity("e1")) {
			t.Errorf("Expected entity e1, got %s", entity.String())
		}
	})
}

// TestIndexedMatcher_WithBindings tests pattern matching with binding relations
func TestIndexedMatcher_WithBindings(t *testing.T) {
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: datalog.NewKeyword("name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p1"), A: datalog.NewKeyword("age"), V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p2"), A: datalog.NewKeyword("name"), V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p2"), A: datalog.NewKeyword("age"), V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewIndexedMemoryMatcher(datoms)

	// Create binding relation that filters to only p1
	bindingTuples := []Tuple{
		{datalog.NewIdentity("p1")},
	}
	bindings := Relations{
		NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?e")}, bindingTuples),
	}

	// Pattern: [?e :age ?v]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword("age")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	result, err := matcher.Match(query.PatternQuery(pattern), bindings)
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

// timeWindowConstraint is a test-local StorageConstraint: V is a time.Time
// within [start, end). The subject under test is the matcher's constraint
// filtering, not any particular constraint's semantics — production
// constraint implementations live in the constraints package.
type timeWindowConstraint struct {
	start, end time.Time
}

func (c *timeWindowConstraint) Evaluate(d *datalog.Datom) bool {
	t, ok := d.V.(time.Time)
	return ok && !t.Before(c.start) && t.Before(c.end)
}

func (c *timeWindowConstraint) String() string {
	return fmt.Sprintf("V ∈ [%s, %s)", c.start, c.end)
}

// TestIndexedMatcher_WithConstraints tests constraint filtering
func TestIndexedMatcher_WithConstraints(t *testing.T) {
	now := time.Now()
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword("timestamp"), V: now, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword("timestamp"), V: now.Add(-time.Hour), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("e3"), A: datalog.NewKeyword("timestamp"), V: now.Add(-2 * time.Hour), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewIndexedMemoryMatcher(datoms)

	// A half-hour window around now matches exactly e1.
	constraint := &timeWindowConstraint{
		start: now.Add(-30 * time.Minute),
		end:   now.Add(30 * time.Minute),
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword("timestamp")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	result, err := matcher.MatchWithConstraints(query.PatternQuery(pattern), nil, []StorageConstraint{constraint})
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

// tupleEqualityCheck compares tuples for equality (test-local version)
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
