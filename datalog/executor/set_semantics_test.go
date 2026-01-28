package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// Set Semantics Tests
//
// Relations in Datalog are SETS, not BAGS. This means:
// - No duplicate tuples should ever appear in a relation
// - Projection must deduplicate (projecting away columns can create duplicates)
// - All relation operations must maintain set semantics
//
// These tests verify set semantics are maintained throughout the pipeline.
// =============================================================================

// Helper: check that a relation has no duplicate tuples
// Uses datalog.ValuesEqual for comparison - same semantics as production code
func assertNoDuplicates(t *testing.T, name string, rel Relation) {
	t.Helper()
	var seen []Tuple // Use slice + equality check instead of map with string keys
	iter := rel.Iterator()
	defer iter.Close()

	idx := 0
	for iter.Next() {
		tuple := iter.Tuple()
		if firstIdx := setTestFindDuplicate(seen, tuple); firstIdx >= 0 {
			t.Errorf("%s: duplicate tuple at index %d (first seen at %d): %v", name, idx, firstIdx, tuple)
		}
		seen = append(seen, tuple)
		idx++
	}
}

// Helper: check if two tuples are equal using the same logic as production code
func setTestTuplesEqual(a, b Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !datalog.ValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Helper: find duplicate index using production equality semantics
// Returns -1 if no duplicate found
func setTestFindDuplicate(tuples []Tuple, newTuple Tuple) int {
	for i, existing := range tuples {
		if setTestTuplesEqual(existing, newTuple) {
			return i
		}
	}
	return -1
}

// Helper: collect all tuples from a relation
func setTestCollectTuples(rel Relation) []Tuple {
	var tuples []Tuple
	iter := rel.Iterator()
	defer iter.Close()
	for iter.Next() {
		tuples = append(tuples, iter.Tuple())
	}
	return tuples
}

// =============================================================================
// Test 1: MaterializedRelation Constructors
// =============================================================================

func TestSetSemantics_MaterializedRelation(t *testing.T) {
	t.Run("NewMaterializedRelation deduplicates", func(t *testing.T) {
		columns := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
		tuples := []Tuple{
			{1, "a"},
			{2, "b"},
			{1, "a"}, // duplicate
			{3, "c"},
			{2, "b"}, // duplicate
		}

		rel := NewMaterializedRelation(columns, tuples)

		if rel.Size() != 3 {
			t.Errorf("expected 3 unique tuples, got %d", rel.Size())
		}
		assertNoDuplicates(t, "NewMaterializedRelation", rel)
	})

	t.Run("NewMaterializedRelationWithOptions deduplicates", func(t *testing.T) {
		columns := []query.Symbol{datalog.NewSymbol("?x")}
		tuples := []Tuple{
			{"foo"},
			{"bar"},
			{"foo"}, // duplicate
			{"foo"}, // duplicate
		}

		rel := NewMaterializedRelationWithOptions(columns, tuples, ExecutorOptions{})

		if rel.Size() != 2 {
			t.Errorf("expected 2 unique tuples, got %d", rel.Size())
		}
		assertNoDuplicates(t, "NewMaterializedRelationWithOptions", rel)
	})
}

// =============================================================================
// Test 2: Projection Must Deduplicate
// =============================================================================

func TestSetSemantics_Projection(t *testing.T) {
	t.Run("MaterializedRelation.Project deduplicates", func(t *testing.T) {
		// Create relation with distinct full tuples that become duplicates after projection
		columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
		tuples := []Tuple{
			{"entity1", "same_value"},
			{"entity2", "same_value"}, // Different entity, same value
			{"entity3", "different"},
			{"entity4", "same_value"}, // Another with same value
		}

		rel := NewMaterializedRelation(columns, tuples)

		// Project to just ?v - should deduplicate
		projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?v")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		// Should have only 2 unique values: "same_value" and "different"
		if projected.Size() != 2 {
			t.Errorf("expected 2 unique values after projection, got %d", projected.Size())
		}
		assertNoDuplicates(t, "MaterializedRelation.Project", projected)
	})

	t.Run("StreamingRelation.Project deduplicates", func(t *testing.T) {
		// Create streaming relation with tuples that become duplicates after projection
		columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
		tuples := []Tuple{
			{"entity1", "value_a"},
			{"entity2", "value_a"}, // Same value, different entity
			{"entity3", "value_b"},
			{"entity4", "value_a"}, // Same value again
			{"entity5", "value_b"}, // Same value as entity3
		}

		// Create a streaming relation
		iter := &sliceIterator{tuples: tuples, pos: -1}
		rel := NewStreamingRelation(columns, iter)

		// Project to just ?v - should deduplicate
		projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?v")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		// Materialize to check results
		materialized := projected.Materialize()
		collected := setTestCollectTuples(materialized)

		// Should have only 2 unique values: "value_a" and "value_b"
		if len(collected) != 2 {
			t.Errorf("expected 2 unique values after projection, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "StreamingRelation.Project", materialized)
	})

	t.Run("Multiple column projection deduplicates", func(t *testing.T) {
		// Project from 3 columns to 2 columns
		columns := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")}
		tuples := []Tuple{
			{1, "x", 100},
			{1, "x", 200}, // Same ?a, ?b - different ?c
			{2, "y", 300},
			{1, "x", 300}, // Same ?a, ?b again
		}

		rel := NewMaterializedRelation(columns, tuples)
		projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		// Should have only 2 unique (?a, ?b) pairs
		if projected.Size() != 2 {
			t.Errorf("expected 2 unique tuples after projection, got %d", projected.Size())
		}
		assertNoDuplicates(t, "Multi-column projection", projected)
	})
}

// =============================================================================
// Test 3: Join Operations Must Maintain Set Semantics
// =============================================================================

func TestSetSemantics_Joins(t *testing.T) {
	t.Run("HashJoin produces no duplicates", func(t *testing.T) {
		// Create relations where join could produce duplicates
		leftCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
		leftTuples := []Tuple{
			{1, "a"},
			{1, "b"}, // Same ?x, different ?y
			{2, "c"},
		}
		left := NewMaterializedRelation(leftCols, leftTuples)

		rightCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")}
		rightTuples := []Tuple{
			{1, 100},
			{1, 100}, // Duplicate in input (should be deduped by constructor)
			{2, 200},
		}
		right := NewMaterializedRelation(rightCols, rightTuples)

		joined := HashJoin(left, right, []query.Symbol{datalog.NewSymbol("?x")})
		assertNoDuplicates(t, "HashJoin", joined)
	})

	t.Run("SemiJoin produces no duplicates", func(t *testing.T) {
		leftCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
		leftTuples := []Tuple{
			{1, "a"},
			{1, "b"},
			{2, "c"},
		}
		left := NewMaterializedRelation(leftCols, leftTuples)

		rightCols := []query.Symbol{datalog.NewSymbol("?x")}
		rightTuples := []Tuple{
			{1},
			{1}, // Duplicate key
		}
		right := NewMaterializedRelation(rightCols, rightTuples)

		joined := SemiJoin(left, right, []query.Symbol{datalog.NewSymbol("?x")})
		assertNoDuplicates(t, "SemiJoin", joined)
	})

	t.Run("AntiJoin produces no duplicates", func(t *testing.T) {
		leftCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
		leftTuples := []Tuple{
			{1, "a"},
			{2, "b"},
			{3, "c"},
		}
		left := NewMaterializedRelation(leftCols, leftTuples)

		rightCols := []query.Symbol{datalog.NewSymbol("?x")}
		rightTuples := []Tuple{
			{1},
		}
		right := NewMaterializedRelation(rightCols, rightTuples)

		joined := AntiJoin(left, right, []query.Symbol{datalog.NewSymbol("?x")})
		assertNoDuplicates(t, "AntiJoin", joined)
	})

	t.Run("Natural Join (via Join method) produces no duplicates", func(t *testing.T) {
		leftCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
		leftTuples := []Tuple{
			{1, "a"},
			{2, "b"},
		}
		left := NewMaterializedRelation(leftCols, leftTuples)

		rightCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")}
		rightTuples := []Tuple{
			{1, 100},
			{2, 200},
		}
		right := NewMaterializedRelation(rightCols, rightTuples)

		joined := left.Join(right)
		assertNoDuplicates(t, "Natural Join", joined)
	})
}

// =============================================================================
// Test 4: Union Operations Must Deduplicate
// =============================================================================

func TestSetSemantics_Union(t *testing.T) {
	t.Run("UnionRelation deduplicates across relations", func(t *testing.T) {
		cols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

		// Create channel with relations that have overlapping tuples
		ch := make(chan relationItem, 3)

		rel1 := NewMaterializedRelation(cols, []Tuple{
			{1, "a"},
			{2, "b"},
		})
		rel2 := NewMaterializedRelation(cols, []Tuple{
			{2, "b"}, // Duplicate from rel1
			{3, "c"},
		})
		rel3 := NewMaterializedRelation(cols, []Tuple{
			{1, "a"}, // Duplicate from rel1
			{3, "c"}, // Duplicate from rel2
			{4, "d"},
		})

		ch <- relationItem{relation: rel1}
		ch <- relationItem{relation: rel2}
		ch <- relationItem{relation: rel3}
		close(ch)

		union := NewUnionRelation(ch, cols, ExecutorOptions{})
		materialized := union.Materialize()

		// Should have 4 unique tuples
		if materialized.Size() != 4 {
			t.Errorf("expected 4 unique tuples, got %d", materialized.Size())
		}
		assertNoDuplicates(t, "UnionRelation", materialized)
	})
}

// =============================================================================
// Test 5: Filter Operations Preserve Set Semantics
// =============================================================================

func TestSetSemantics_Filter(t *testing.T) {
	t.Run("Filter preserves set semantics", func(t *testing.T) {
		columns := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
		tuples := []Tuple{
			{1, "a"},
			{2, "b"},
			{3, "c"},
		}

		rel := NewMaterializedRelation(columns, tuples)
		filtered := rel.Select(func(t Tuple) bool {
			return t[0].(int) > 1
		})

		assertNoDuplicates(t, "Filter", filtered)
	})
}

// =============================================================================
// Test 6: Iterator Wrappers Maintain Set Semantics
// =============================================================================

func TestSetSemantics_Iterators(t *testing.T) {
	t.Run("DedupIterator removes duplicates", func(t *testing.T) {
		tuples := []Tuple{
			{1, "a"},
			{2, "b"},
			{1, "a"}, // duplicate
			{3, "c"},
			{2, "b"}, // duplicate
		}

		source := &sliceIterator{tuples: tuples, pos: -1}
		dedupIter := NewDedupIterator(source, 10)

		var results []Tuple
		for dedupIter.Next() {
			results = append(results, dedupIter.Tuple())
		}
		dedupIter.Close()

		if len(results) != 3 {
			t.Errorf("expected 3 unique tuples, got %d", len(results))
		}

		// Check no duplicates in results using production equality semantics
		for i, tuple := range results {
			if dupIdx := setTestFindDuplicate(results[:i], tuple); dupIdx >= 0 {
				t.Errorf("DedupIterator produced duplicate at index %d (same as %d): %v", i, dupIdx, tuple)
			}
		}
	})

	t.Run("ProjectIterator should work with DedupIterator", func(t *testing.T) {
		// This tests the fix: ProjectIterator output wrapped with DedupIterator
		columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
		tuples := []Tuple{
			{"e1", "same"},
			{"e2", "same"},
			{"e3", "different"},
		}

		source := &sliceIterator{tuples: tuples, pos: -1}
		rel := NewStreamingRelation(columns, source)

		// Create project iterator
		projIter := NewProjectIterator(rel, columns, []query.Symbol{datalog.NewSymbol("?v")})

		// Wrap with dedup (this is what the fix should do)
		dedupIter := NewDedupIterator(projIter, 10)

		var results []Tuple
		for dedupIter.Next() {
			results = append(results, dedupIter.Tuple())
		}
		dedupIter.Close()

		if len(results) != 2 {
			t.Errorf("expected 2 unique values, got %d: %v", len(results), results)
		}
	})
}

// =============================================================================
// Test 7: End-to-End Query Set Semantics
// =============================================================================

func TestSetSemantics_EndToEnd(t *testing.T) {
	t.Run("Query projection deduplicates final result", func(t *testing.T) {
		// Simulate the query: [:find ?v :where [_ :attr ?v]]
		// Multiple entities with the same attribute value should produce one result

		// This is what comes from storage: multiple datoms with same value
		columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?a"), datalog.NewSymbol("?v")}
		tuples := []Tuple{
			{"entity1", ":attr", "shared_value"},
			{"entity2", ":attr", "shared_value"},
			{"entity3", ":attr", "unique_value"},
			{"entity4", ":attr", "shared_value"},
		}

		rel := NewMaterializedRelation(columns, tuples)

		// Project to just ?v (like :find ?v)
		projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?v")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		collected := setTestCollectTuples(projected)

		// Should have only 2 unique values
		if len(collected) != 2 {
			t.Errorf("expected 2 unique values, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Query projection", projected)
	})

	t.Run("Streaming query projection deduplicates", func(t *testing.T) {
		// Same test but with streaming relation
		columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
		tuples := []Tuple{
			{"e1", "val1"},
			{"e2", "val1"}, // duplicate value
			{"e3", "val2"},
			{"e4", "val1"}, // duplicate value
			{"e5", "val2"}, // duplicate value
		}

		iter := &sliceIterator{tuples: tuples, pos: -1}
		rel := NewStreamingRelation(columns, iter)

		// Project and materialize
		projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?v")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}
		materialized := projected.Materialize()

		collected := setTestCollectTuples(materialized)

		// Should have only 2 unique values
		if len(collected) != 2 {
			t.Errorf("expected 2 unique values, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Streaming query projection", materialized)
	})
}

// =============================================================================
// Test 8: Aggregation Input Set Semantics
// =============================================================================

func TestSetSemantics_Aggregation(t *testing.T) {
	t.Run("Aggregation receives deduplicated input", func(t *testing.T) {
		// When aggregating, the input should be deduplicated
		// This matters for COUNT - counting duplicates would be wrong
		columns := []query.Symbol{datalog.NewSymbol("?group"), datalog.NewSymbol("?value")}
		tuples := []Tuple{
			{"A", int64(10)},
			{"A", int64(20)},
			{"A", int64(10)}, // Duplicate - should not be counted twice
			{"B", int64(30)},
		}

		rel := NewMaterializedRelation(columns, tuples)

		// The relation should already be deduplicated
		if rel.Size() != 3 {
			t.Errorf("expected 3 unique tuples before aggregation, got %d", rel.Size())
		}
		assertNoDuplicates(t, "Aggregation input", rel)
	})
}

// =============================================================================
// Test 9: Relation Operations Chain
// =============================================================================

func TestSetSemantics_OperationChain(t *testing.T) {
	t.Run("Filter then Project maintains set semantics", func(t *testing.T) {
		columns := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
		tuples := []Tuple{
			{1, "a", 100},
			{2, "a", 200}, // Same ?y as above
			{3, "b", 300},
			{4, "a", 400}, // Same ?y as first two
		}

		rel := NewMaterializedRelation(columns, tuples)

		// Filter to x > 1
		filtered := rel.Select(func(t Tuple) bool {
			return t[0].(int) > 1
		})

		// Project to just ?y
		projected, err := filtered.Project([]query.Symbol{datalog.NewSymbol("?y")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		// Should have 2 unique ?y values: "a" and "b"
		if projected.Size() != 2 {
			t.Errorf("expected 2 unique values, got %d", projected.Size())
		}
		assertNoDuplicates(t, "Filter+Project chain", projected)
	})

	t.Run("Join then Project maintains set semantics", func(t *testing.T) {
		left := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			[]Tuple{
				{1, "a"},
				{2, "a"}, // Same ?y
			},
		)

		right := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")},
			[]Tuple{
				{1, 100},
				{2, 200},
			},
		)

		joined := left.Join(right)

		// Project to just ?y, ?z
		projected, err := joined.Project([]query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		assertNoDuplicates(t, "Join+Project chain", projected)
	})
}

// =============================================================================
// Test 10: Edge Cases
// =============================================================================

func TestSetSemantics_EdgeCases(t *testing.T) {
	t.Run("Empty relation has no duplicates", func(t *testing.T) {
		rel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, []Tuple{})
		assertNoDuplicates(t, "Empty relation", rel)
	})

	t.Run("Single tuple relation has no duplicates", func(t *testing.T) {
		rel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, []Tuple{{1}})
		assertNoDuplicates(t, "Single tuple", rel)
	})

	t.Run("All duplicate input produces single tuple", func(t *testing.T) {
		tuples := []Tuple{
			{"same"},
			{"same"},
			{"same"},
			{"same"},
		}
		rel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, tuples)

		if rel.Size() != 1 {
			t.Errorf("expected 1 unique tuple, got %d", rel.Size())
		}
	})

	t.Run("Nil values handled correctly", func(t *testing.T) {
		tuples := []Tuple{
			{nil, "a"},
			{nil, "a"}, // duplicate with nil
			{nil, "b"},
		}
		rel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}, tuples)

		if rel.Size() != 2 {
			t.Errorf("expected 2 unique tuples, got %d", rel.Size())
		}
		assertNoDuplicates(t, "Nil values", rel)
	})

	t.Run("Different types with same string representation", func(t *testing.T) {
		// "1" (string) vs 1 (int) should be different tuples
		tuples := []Tuple{
			{"1"},
			{1},
			{int64(1)},
		}
		rel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, tuples)

		// These should all be considered different (type matters)
		// Note: This depends on how TupleKey handles types
		assertNoDuplicates(t, "Different types", rel)
	})
}

// =============================================================================
// Test 11: Semantic Contract Tests
//
// These tests document the SEMANTIC CONTRACT that Relations are sets.
// They should pass regardless of implementation details (streaming vs materialized).
// If future optimizations break these, the optimization is wrong.
// =============================================================================

func TestSetSemantics_Contract_ProjectionAlwaysDeduplicates(t *testing.T) {
	// SEMANTIC CONTRACT: Projection MUST deduplicate, regardless of input relation type.
	// This is not an implementation detail - it's fundamental to relational algebra.

	columns := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")}
	// Create tuples that are unique in full form but duplicate after projection
	inputTuples := []Tuple{
		{1, "x", 100},
		{2, "x", 200}, // Same ?b
		{3, "y", 300},
		{4, "x", 400}, // Same ?b again
		{5, "y", 500}, // Same ?b as row 3
	}

	testCases := []struct {
		name       string
		createRel  func() Relation
		projectTo  []query.Symbol
		expectSize int
	}{
		{
			name: "MaterializedRelation project to single column",
			createRel: func() Relation {
				return NewMaterializedRelation(columns, inputTuples)
			},
			projectTo:  []query.Symbol{datalog.NewSymbol("?b")},
			expectSize: 2, // "x" and "y"
		},
		{
			name: "StreamingRelation project to single column",
			createRel: func() Relation {
				iter := &sliceIterator{tuples: inputTuples, pos: -1}
				return NewStreamingRelation(columns, iter)
			},
			projectTo:  []query.Symbol{datalog.NewSymbol("?b")},
			expectSize: 2, // "x" and "y"
		},
		{
			name: "MaterializedRelation project to two columns",
			createRel: func() Relation {
				return NewMaterializedRelation(columns, inputTuples)
			},
			projectTo:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			expectSize: 5, // All unique (a,b) pairs
		},
		{
			name: "StreamingRelation project to two columns",
			createRel: func() Relation {
				iter := &sliceIterator{tuples: inputTuples, pos: -1}
				return NewStreamingRelation(columns, iter)
			},
			projectTo:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			expectSize: 5, // All unique (a,b) pairs
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rel := tc.createRel()
			projected, err := rel.Project(tc.projectTo)
			if err != nil {
				t.Fatalf("projection failed: %v", err)
			}

			// Force materialization to get final result
			materialized := projected.Materialize()
			collected := setTestCollectTuples(materialized)

			if len(collected) != tc.expectSize {
				t.Errorf("expected %d unique tuples, got %d: %v", tc.expectSize, len(collected), collected)
			}
			assertNoDuplicates(t, tc.name, materialized)
		})
	}
}

func TestSetSemantics_Contract_JoinThenProjectDeduplicates(t *testing.T) {
	// SEMANTIC CONTRACT: Join followed by projection must deduplicate.
	// Even if join is optimized to be streaming in the future.

	t.Run("Hash join then project creates duplicates that must be removed", func(t *testing.T) {
		// Two people in same city - joining then projecting to city should dedupe
		left := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?city")},
			[]Tuple{
				{"alice", "NYC"},
				{"bob", "NYC"}, // Same city
				{"carol", "LA"},
			},
		)

		right := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?city"), datalog.NewSymbol("?population")},
			[]Tuple{
				{"NYC", 8000000},
				{"LA", 4000000},
			},
		)

		// Join on ?city
		joined := HashJoin(left, right, []query.Symbol{datalog.NewSymbol("?city")})

		// Project to just ?city - should have 2 unique values
		projected, err := joined.Project([]query.Symbol{datalog.NewSymbol("?city")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		collected := setTestCollectTuples(projected.Materialize())
		if len(collected) != 2 {
			t.Errorf("expected 2 unique cities, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Join then project", projected.Materialize())
	})

	t.Run("Natural join then project", func(t *testing.T) {
		left := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			[]Tuple{
				{1, "a"},
				{2, "a"}, // Same ?y
				{3, "b"},
			},
		)

		right := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")},
			[]Tuple{
				{1, 100},
				{2, 200},
				{3, 300},
			},
		)

		joined := left.Join(right)
		projected, err := joined.Project([]query.Symbol{datalog.NewSymbol("?y")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		collected := setTestCollectTuples(projected.Materialize())
		if len(collected) != 2 {
			t.Errorf("expected 2 unique ?y values, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Natural join then project", projected.Materialize())
	})
}

func TestSetSemantics_Contract_FilterPreservesSetSemantics(t *testing.T) {
	// SEMANTIC CONTRACT: Filter cannot introduce duplicates.
	// Input is a set, output must be a set.

	columns := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	tuples := []Tuple{
		{1, "a"},
		{2, "b"},
		{3, "a"}, // Same ?y as first
		{4, "c"},
	}

	t.Run("Filter then project must deduplicate", func(t *testing.T) {
		rel := NewMaterializedRelation(columns, tuples)

		// Filter to x > 1
		filtered := rel.Select(func(t Tuple) bool {
			return t[0].(int) > 1
		})

		// Project to ?y - should dedupe "a" (from row 3) and "b", "c"
		projected, err := filtered.Project([]query.Symbol{datalog.NewSymbol("?y")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		collected := setTestCollectTuples(projected.Materialize())
		// Rows 2,3,4 pass filter -> ?y values are "b", "a", "c" -> 3 unique
		if len(collected) != 3 {
			t.Errorf("expected 3 unique values, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Filter then project", projected.Materialize())
	})

	t.Run("Streaming filter then project must deduplicate", func(t *testing.T) {
		iter := &sliceIterator{tuples: tuples, pos: -1}
		rel := NewStreamingRelation(columns, iter)

		filtered := rel.Select(func(t Tuple) bool {
			return t[0].(int) > 1
		})

		projected, err := filtered.Project([]query.Symbol{datalog.NewSymbol("?y")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		collected := setTestCollectTuples(projected.Materialize())
		if len(collected) != 3 {
			t.Errorf("expected 3 unique values, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Streaming filter then project", projected.Materialize())
	})
}

func TestSetSemantics_Contract_ChainedOperationsPreserveSetSemantics(t *testing.T) {
	// SEMANTIC CONTRACT: Any chain of relational operations must produce a set.
	// Filter -> Join -> Project -> Filter -> Project must still be a set.

	t.Run("Complex operation chain", func(t *testing.T) {
		rel1 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			[]Tuple{
				{1, "x"},
				{2, "x"},
				{3, "y"},
				{4, "x"},
			},
		)

		rel2 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?c")},
			[]Tuple{
				{1, 100},
				{2, 200},
				{3, 300},
				{4, 400},
			},
		)

		// Chain: filter -> join -> project
		filtered := rel1.Select(func(t Tuple) bool {
			return t[0].(int) > 1
		})

		joined := filtered.Join(rel2)

		projected, err := joined.Project([]query.Symbol{datalog.NewSymbol("?b")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		collected := setTestCollectTuples(projected.Materialize())
		// After filter: rows with a=2,3,4 -> b values "x", "y", "x"
		// After project to ?b: should be 2 unique ("x", "y")
		if len(collected) != 2 {
			t.Errorf("expected 2 unique values, got %d: %v", len(collected), collected)
		}
		assertNoDuplicates(t, "Chained operations", projected.Materialize())
	})
}

func TestSetSemantics_Contract_IteratorMultiplePassesSameResults(t *testing.T) {
	// SEMANTIC CONTRACT: Multiple iterations over a relation must produce identical results.
	// This ensures the relation behaves as a stable set, not a stream that changes.

	columns := []query.Symbol{datalog.NewSymbol("?x")}
	tuples := []Tuple{{1}, {2}, {3}, {2}, {1}} // With duplicates

	t.Run("MaterializedRelation multiple iterations", func(t *testing.T) {
		rel := NewMaterializedRelation(columns, tuples)

		// First iteration
		var first []Tuple
		iter1 := rel.Iterator()
		for iter1.Next() {
			first = append(first, iter1.Tuple())
		}
		iter1.Close()

		// Second iteration
		var second []Tuple
		iter2 := rel.Iterator()
		for iter2.Next() {
			second = append(second, iter2.Tuple())
		}
		iter2.Close()

		if len(first) != len(second) {
			t.Errorf("iterations produced different sizes: %d vs %d", len(first), len(second))
		}

		// Both should have 3 unique tuples (deduped)
		if len(first) != 3 {
			t.Errorf("expected 3 unique tuples, got %d", len(first))
		}
	})
}

// =============================================================================
// Test 12: Storage Integration (requires mock or real storage)
// =============================================================================

func TestSetSemantics_StoragePattern(t *testing.T) {
	t.Run("Pattern match with multiple entities same value", func(t *testing.T) {
		// Simulate what comes from storage for pattern [_ :attr ?v]
		// This is the core bug scenario

		// Create datoms as they would come from storage
		e1 := datalog.NewIdentity("entity:1")
		e2 := datalog.NewIdentity("entity:2")
		e3 := datalog.NewIdentity("entity:3")
		attr := datalog.NewKeyword(":test/value")

		datoms := []datalog.Datom{
			{E: e1, A: attr, V: "shared", Tx: 1},
			{E: e2, A: attr, V: "shared", Tx: 2}, // Same value
			{E: e3, A: attr, V: "unique", Tx: 3},
		}

		// Convert to tuples as pattern matcher would
		columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
		tuples := make([]Tuple, len(datoms))
		for i, d := range datoms {
			tuples[i] = Tuple{d.E, d.V}
		}

		rel := NewMaterializedRelation(columns, tuples)

		// Project to just ?v (the find clause)
		projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?v")})
		if err != nil {
			t.Fatalf("projection failed: %v", err)
		}

		// Should have 2 unique values
		if projected.Size() != 2 {
			collected := setTestCollectTuples(projected)
			t.Errorf("expected 2 unique values, got %d: %v", projected.Size(), collected)
		}
		assertNoDuplicates(t, "Storage pattern projection", projected)
	})
}
