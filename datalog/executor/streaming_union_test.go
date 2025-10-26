package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestUnionBuilder_Streaming(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: true,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Create test relations
	rel1 := NewMaterializedRelation(
		[]query.Symbol{"?x", "?y"},
		[]Tuple{
			{1, 2},
			{3, 4},
		},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{"?x", "?y"},
		[]Tuple{
			{5, 6},
			{7, 8},
		},
	)

	rel3 := NewMaterializedRelation(
		[]query.Symbol{"?x", "?y"},
		[]Tuple{
			{9, 10},
		},
	)

	// Union them
	result := builder.Union([]Relation{rel1, rel2, rel3})

	// Verify it's a UnionRelation (streaming)
	if _, ok := result.(*UnionRelation); !ok {
		t.Errorf("Expected UnionRelation (streaming), got %T", result)
	}

	// Verify columns
	expectedColumns := []query.Symbol{"?x", "?y"}
	if !symbolsEqual(result.Columns(), expectedColumns) {
		t.Errorf("Expected columns %v, got %v", expectedColumns, result.Columns())
	}

	// Collect all tuples
	var allTuples []Tuple
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		allTuples = append(allTuples, it.Tuple())
	}

	// Verify all tuples present (may be in any order due to streaming)
	if len(allTuples) != 5 {
		t.Fatalf("Expected 5 tuples, got %d", len(allTuples))
	}

	// Check that all expected values are present
	expectedValues := []int{1, 3, 5, 7, 9}
	foundValues := make(map[int]bool)
	for _, tuple := range allTuples {
		foundValues[tuple[0].(int)] = true
	}

	for _, expected := range expectedValues {
		if !foundValues[expected] {
			t.Errorf("Missing value %d in first column", expected)
		}
	}
}

func TestUnionBuilder_Materialized(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Create test relations
	rel1 := NewMaterializedRelation(
		[]query.Symbol{"?x"},
		[]Tuple{{1}, {2}},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{"?x"},
		[]Tuple{{3}, {4}},
	)

	// Union them
	result := builder.Union([]Relation{rel1, rel2})

	// Verify it's a MaterializedRelation
	if _, ok := result.(*MaterializedRelation); !ok {
		t.Errorf("Expected MaterializedRelation, got %T", result)
	}

	// Verify size
	if result.Size() != 4 {
		t.Errorf("Expected 4 tuples, got %d", result.Size())
	}

	// Verify all values present
	expected := []int{1, 2, 3, 4}
	for i, exp := range expected {
		tuple := result.Get(i)
		if tuple[0].(int) != exp {
			t.Errorf("Tuple %d: expected %d, got %d", i, exp, tuple[0].(int))
		}
	}
}

func TestUnionBuilder_SingleRelation(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: true,
	}
	builder := NewStreamingUnionBuilder(opts)

	rel := NewMaterializedRelation(
		[]query.Symbol{"?x"},
		[]Tuple{{1}, {2}},
	)

	// Union single relation should return it unchanged
	result := builder.Union([]Relation{rel})

	if result != rel {
		t.Error("Expected single relation to be returned unchanged")
	}
}

func TestUnionBuilder_Empty(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: true,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Empty relations list
	result := builder.Union([]Relation{})

	if result != nil {
		t.Errorf("Expected nil for empty relations, got %v", result)
	}
}

func TestUnionBuilder_WithColumns_Matching(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Relations with matching columns
	rel1 := NewMaterializedRelation(
		[]query.Symbol{"?x", "?y"},
		[]Tuple{{1, 2}},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{"?x", "?y"},
		[]Tuple{{3, 4}},
	)

	// Union with matching columns
	result, err := builder.UnionWithColumns(
		[]Relation{rel1, rel2},
		[]query.Symbol{"?x", "?y"},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Size() != 2 {
		t.Errorf("Expected 2 tuples, got %d", result.Size())
	}

	// Verify columns
	if !symbolsEqual(result.Columns(), []query.Symbol{"?x", "?y"}) {
		t.Errorf("Columns mismatch: %v", result.Columns())
	}
}

func TestUnionBuilder_WithColumns_NeedProjection(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Relations with different column order
	rel1 := NewMaterializedRelation(
		[]query.Symbol{"?x", "?y"},
		[]Tuple{{1, 2}},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{"?y", "?x"},  // Different order
		[]Tuple{{4, 3}},
	)

	// Union with specific column order
	result, err := builder.UnionWithColumns(
		[]Relation{rel1, rel2},
		[]query.Symbol{"?x", "?y"},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Size() != 2 {
		t.Errorf("Expected 2 tuples, got %d", result.Size())
	}

	// Verify columns are in requested order
	if !symbolsEqual(result.Columns(), []query.Symbol{"?x", "?y"}) {
		t.Errorf("Columns mismatch: %v", result.Columns())
	}

	// Verify values are correctly ordered
	tuple0 := result.Get(0)
	if tuple0[0] != 1 || tuple0[1] != 2 {
		t.Errorf("Tuple 0: expected [1, 2], got %v", tuple0)
	}

	tuple1 := result.Get(1)
	if tuple1[0] != 3 || tuple1[1] != 4 {
		t.Errorf("Tuple 1: expected [3, 4], got %v", tuple1)
	}
}

func TestUnionBuilder_WithColumns_Empty(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Empty relations with column spec
	result, err := builder.UnionWithColumns(
		[]Relation{},
		[]query.Symbol{"?x", "?y"},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Size() != 0 {
		t.Errorf("Expected 0 tuples, got %d", result.Size())
	}

	// Should have correct columns
	if !symbolsEqual(result.Columns(), []query.Symbol{"?x", "?y"}) {
		t.Errorf("Columns mismatch: %v", result.Columns())
	}
}

func TestUnionBuilder_WithColumns_SingleRelation(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	rel := NewMaterializedRelation(
		[]query.Symbol{"?y", "?x"},  // Different order
		[]Tuple{{2, 1}},
	)

	// Should project single relation to match columns
	result, err := builder.UnionWithColumns(
		[]Relation{rel},
		[]query.Symbol{"?x", "?y"},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !symbolsEqual(result.Columns(), []query.Symbol{"?x", "?y"}) {
		t.Errorf("Columns mismatch: %v", result.Columns())
	}

	tuple := result.Get(0)
	if tuple[0] != 1 || tuple[1] != 2 {
		t.Errorf("Expected [1, 2], got %v", tuple)
	}
}

func TestSymbolsEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []query.Symbol
		b        []query.Symbol
		expected bool
	}{
		{
			name:     "equal",
			a:        []query.Symbol{"?x", "?y"},
			b:        []query.Symbol{"?x", "?y"},
			expected: true,
		},
		{
			name:     "different_order",
			a:        []query.Symbol{"?x", "?y"},
			b:        []query.Symbol{"?y", "?x"},
			expected: false,
		},
		{
			name:     "different_length",
			a:        []query.Symbol{"?x", "?y"},
			b:        []query.Symbol{"?x"},
			expected: false,
		},
		{
			name:     "empty_both",
			a:        []query.Symbol{},
			b:        []query.Symbol{},
			expected: true,
		},
		{
			name:     "nil_vs_empty",
			a:        nil,
			b:        []query.Symbol{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := symbolsEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v for %v vs %v", tt.expected, result, tt.a, tt.b)
			}
		})
	}
}
