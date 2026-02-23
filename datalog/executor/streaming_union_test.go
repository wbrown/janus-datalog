package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestUnionBuilder_Streaming(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: true,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Create test relations
	rel1 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		[]Tuple{
			{1, 2},
			{3, 4},
		},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		[]Tuple{
			{5, 6},
			{7, 8},
		},
	)

	rel3 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
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

	// Verify symbols
	expectedColumns := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	if !symbolsEqual(result.Symbols(), expectedColumns) {
		t.Errorf("Expected symbols %v, got %v", expectedColumns, result.Symbols())
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
			t.Errorf("Missing value %d in first symbol", expected)
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
		[]query.Symbol{datalog.NewSymbol("?x")},
		[]Tuple{{1}, {2}},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x")},
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
		[]query.Symbol{datalog.NewSymbol("?x")},
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

	// Relations with matching symbols
	rel1 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		[]Tuple{{1, 2}},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		[]Tuple{{3, 4}},
	)

	// Union with matching symbols
	result, err := builder.UnionWithColumns(
		[]Relation{rel1, rel2},
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Size() != 2 {
		t.Errorf("Expected 2 tuples, got %d", result.Size())
	}

	// Verify symbols
	if !symbolsEqual(result.Symbols(), []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}) {
		t.Errorf("Symbols mismatch: %v", result.Symbols())
	}
}

func TestUnionBuilder_WithColumns_NeedProjection(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	// Relations with different symbol order
	rel1 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		[]Tuple{{1, 2}},
	)

	rel2 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?x")}, // Different order
		[]Tuple{{4, 3}},
	)

	// Union with specific symbol order
	result, err := builder.UnionWithColumns(
		[]Relation{rel1, rel2},
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Size() != 2 {
		t.Errorf("Expected 2 tuples, got %d", result.Size())
	}

	// Verify symbols are in requested order
	if !symbolsEqual(result.Symbols(), []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}) {
		t.Errorf("Symbols mismatch: %v", result.Symbols())
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

	// Empty relations with symbol spec
	result, err := builder.UnionWithColumns(
		[]Relation{},
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Size() != 0 {
		t.Errorf("Expected 0 tuples, got %d", result.Size())
	}

	// Should have correct symbols
	if !symbolsEqual(result.Symbols(), []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}) {
		t.Errorf("Symbols mismatch: %v", result.Symbols())
	}
}

func TestUnionBuilder_WithColumns_SingleRelation(t *testing.T) {
	opts := ExecutorOptions{
		UseStreamingSubqueryUnion: false,
	}
	builder := NewStreamingUnionBuilder(opts)

	rel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?x")}, // Different order
		[]Tuple{{2, 1}},
	)

	// Should project single relation to match symbols
	result, err := builder.UnionWithColumns(
		[]Relation{rel},
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !symbolsEqual(result.Symbols(), []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}) {
		t.Errorf("Symbols mismatch: %v", result.Symbols())
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
			a:        []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			b:        []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			expected: true,
		},
		{
			name:     "different_order",
			a:        []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			b:        []query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?x")},
			expected: false,
		},
		{
			name:     "different_length",
			a:        []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			b:        []query.Symbol{datalog.NewSymbol("?x")},
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
