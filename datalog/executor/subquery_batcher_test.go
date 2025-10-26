package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBatcher_BuildBatchedInput(t *testing.T) {
	batcher := NewSubqueryBatcher()

	// Create input combinations
	combinations := []map[query.Symbol]interface{}{
		{
			"?sym":  "AAPL",
			"?hour": int64(10),
		},
		{
			"?sym":  "GOOGL",
			"?hour": int64(11),
		},
		{
			"?sym":  "MSFT",
			"?hour": int64(12),
		},
	}

	inputSymbols := []query.Symbol{"$", "?sym", "?hour"}

	// Build batched input
	rel := batcher.BuildBatchedInput(combinations, inputSymbols)

	// Verify columns (should exclude $)
	expectedColumns := []query.Symbol{"?sym", "?hour"}
	columns := rel.Columns()
	if len(columns) != len(expectedColumns) {
		t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(columns))
	}
	for i, col := range columns {
		if col != expectedColumns[i] {
			t.Errorf("Column %d: expected %v, got %v", i, expectedColumns[i], col)
		}
	}

	// Verify tuples
	if rel.Size() != 3 {
		t.Fatalf("Expected 3 tuples, got %d", rel.Size())
	}

	// Check first tuple
	tuple0 := rel.Get(0)
	if tuple0[0] != "AAPL" {
		t.Errorf("Tuple 0, col 0: expected AAPL, got %v", tuple0[0])
	}
	if tuple0[1] != int64(10) {
		t.Errorf("Tuple 0, col 1: expected 10, got %v", tuple0[1])
	}

	// Check second tuple
	tuple1 := rel.Get(1)
	if tuple1[0] != "GOOGL" {
		t.Errorf("Tuple 1, col 0: expected GOOGL, got %v", tuple1[0])
	}
	if tuple1[1] != int64(11) {
		t.Errorf("Tuple 1, col 1: expected 11, got %v", tuple1[1])
	}
}

func TestBatcher_BuildBatchedInput_SingleColumn(t *testing.T) {
	batcher := NewSubqueryBatcher()

	combinations := []map[query.Symbol]interface{}{
		{"?sym": "AAPL"},
		{"?sym": "GOOGL"},
	}

	inputSymbols := []query.Symbol{"$", "?sym"}

	rel := batcher.BuildBatchedInput(combinations, inputSymbols)

	// Verify single column (excluding $)
	if len(rel.Columns()) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(rel.Columns()))
	}
	if rel.Columns()[0] != "?sym" {
		t.Errorf("Expected column ?sym, got %v", rel.Columns()[0])
	}

	// Verify tuples
	if rel.Size() != 2 {
		t.Fatalf("Expected 2 tuples, got %d", rel.Size())
	}
}

func TestBatcher_BuildBatchedInput_Empty(t *testing.T) {
	batcher := NewSubqueryBatcher()

	// Empty combinations
	combinations := []map[query.Symbol]interface{}{}
	inputSymbols := []query.Symbol{"$", "?sym"}

	rel := batcher.BuildBatchedInput(combinations, inputSymbols)

	// Should return empty relation with correct columns
	if len(rel.Columns()) != 1 {
		t.Errorf("Expected 1 column, got %d", len(rel.Columns()))
	}
	if rel.Size() != 0 {
		t.Errorf("Expected 0 tuples, got %d", rel.Size())
	}
}

func TestBatcher_BuildBatchedInput_MissingValue(t *testing.T) {
	batcher := NewSubqueryBatcher()

	// Combination missing ?hour value
	combinations := []map[query.Symbol]interface{}{
		{
			"?sym": "AAPL",
			// ?hour is missing
		},
	}

	inputSymbols := []query.Symbol{"$", "?sym", "?hour"}

	rel := batcher.BuildBatchedInput(combinations, inputSymbols)

	// Should still create tuple with nil for missing value
	if rel.Size() != 1 {
		t.Fatalf("Expected 1 tuple, got %d", rel.Size())
	}

	tuple := rel.Get(0)
	if tuple[0] != "AAPL" {
		t.Errorf("Col 0: expected AAPL, got %v", tuple[0])
	}
	if tuple[1] != nil {
		t.Errorf("Col 1: expected nil for missing value, got %v", tuple[1])
	}
}

func TestBatcher_ExtractInputSymbols_ScalarInputs(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.ScalarInput{Symbol: "?sym"},
		query.ScalarInput{Symbol: "?hour"},
	}

	symbols := batcher.ExtractInputSymbols(inputs)

	expected := []query.Symbol{"$", "?sym", "?hour"}
	if len(symbols) != len(expected) {
		t.Fatalf("Expected %d symbols, got %d", len(expected), len(symbols))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("Symbol %d: expected %v, got %v", i, expected[i], sym)
		}
	}
}

func TestBatcher_ExtractInputSymbols_RelationInput(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.RelationInput{
			Symbols: []query.Symbol{"?sym", "?hour"},
		},
	}

	symbols := batcher.ExtractInputSymbols(inputs)

	expected := []query.Symbol{"$", "?sym", "?hour"}
	if len(symbols) != len(expected) {
		t.Fatalf("Expected %d symbols, got %d", len(expected), len(symbols))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("Symbol %d: expected %v, got %v", i, expected[i], sym)
		}
	}
}

func TestBatcher_ExtractInputSymbols_TupleInput(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.TupleInput{
			Symbols: []query.Symbol{"?x", "?y"},
		},
	}

	symbols := batcher.ExtractInputSymbols(inputs)

	expected := []query.Symbol{"$", "?x", "?y"}
	if len(symbols) != len(expected) {
		t.Fatalf("Expected %d symbols, got %d", len(expected), len(symbols))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("Symbol %d: expected %v, got %v", i, expected[i], sym)
		}
	}
}

func TestBatcher_ExtractInputSymbols_CollectionInput(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.CollectionInput{Symbol: "?values"},
	}

	symbols := batcher.ExtractInputSymbols(inputs)

	expected := []query.Symbol{"$", "?values"}
	if len(symbols) != len(expected) {
		t.Fatalf("Expected %d symbols, got %d", len(expected), len(symbols))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("Symbol %d: expected %v, got %v", i, expected[i], sym)
		}
	}
}

func TestBatcher_ExtractInputSymbols_Mixed(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.ScalarInput{Symbol: "?x"},
		query.CollectionInput{Symbol: "?ys"},
		query.TupleInput{Symbols: []query.Symbol{"?a", "?b"}},
	}

	symbols := batcher.ExtractInputSymbols(inputs)

	expected := []query.Symbol{"$", "?x", "?ys", "?a", "?b"}
	if len(symbols) != len(expected) {
		t.Fatalf("Expected %d symbols, got %d", len(expected), len(symbols))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("Symbol %d: expected %v, got %v", i, expected[i], sym)
		}
	}
}

func TestBatcher_ExtractInputSymbols_Empty(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{}

	symbols := batcher.ExtractInputSymbols(inputs)

	if len(symbols) != 0 {
		t.Errorf("Expected empty symbols, got %d symbols", len(symbols))
	}
}

func TestBatcher_ExtractRelationSymbols_WithRelationInput(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.RelationInput{
			Symbols: []query.Symbol{"?sym", "?hour"},
		},
	}

	symbols := batcher.ExtractRelationSymbols(inputs)

	expected := []query.Symbol{"?sym", "?hour"}
	if len(symbols) != len(expected) {
		t.Fatalf("Expected %d symbols, got %d", len(expected), len(symbols))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("Symbol %d: expected %v, got %v", i, expected[i], sym)
		}
	}
}

func TestBatcher_ExtractRelationSymbols_WithoutRelationInput(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{
		query.DatabaseInput{},
		query.ScalarInput{Symbol: "?sym"},
	}

	symbols := batcher.ExtractRelationSymbols(inputs)

	if symbols != nil {
		t.Errorf("Expected nil for no RelationInput, got %v", symbols)
	}
}

func TestBatcher_ExtractRelationSymbols_Empty(t *testing.T) {
	batcher := NewSubqueryBatcher()

	inputs := []query.InputSpec{}

	symbols := batcher.ExtractRelationSymbols(inputs)

	if symbols != nil {
		t.Errorf("Expected nil for empty inputs, got %v", symbols)
	}
}
