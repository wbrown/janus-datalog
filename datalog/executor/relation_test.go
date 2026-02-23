package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestMaterializedRelation(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?city")}
	tuples := []Tuple{
		{"Alice", 30, "NYC"},
		{"Bob", 25, "LA"},
		{"Charlie", 35, "NYC"},
	}

	rel := NewMaterializedRelation(symbols, tuples)

	// Test basic properties
	if rel.Size() != 3 {
		t.Errorf("expected size 3, got %d", rel.Size())
	}

	if rel.IsEmpty() {
		t.Error("expected non-empty relation")
	}

	// Test symbols
	cols := rel.Symbols()
	if len(cols) != 3 || cols[0] != datalog.NewSymbol("?name") || cols[1] != datalog.NewSymbol("?age") || cols[2] != datalog.NewSymbol("?city") {
		t.Errorf("unexpected symbols: %v", cols)
	}

	// Test iteration
	it := rel.Iterator()
	defer it.Close()

	count := 0
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) != 3 {
			t.Errorf("expected tuple length 3, got %d", len(tuple))
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 tuples, got %d", count)
	}
}

func TestEmptyRelation(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	rel := NewMaterializedRelation(symbols, nil)

	if !rel.IsEmpty() {
		t.Error("expected empty relation")
	}

	if rel.Size() != 0 {
		t.Errorf("expected size 0, got %d", rel.Size())
	}

	it := rel.Iterator()
	if it.Next() {
		t.Error("expected no tuples in empty relation")
	}
}

func TestSymbolIndex(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")}
	rel := NewMaterializedRelation(symbols, nil)

	tests := []struct {
		symbol   query.Symbol
		expected int
	}{
		{datalog.NewSymbol("?a"), 0},
		{datalog.NewSymbol("?b"), 1},
		{datalog.NewSymbol("?c"), 2},
		{datalog.NewSymbol("?d"), -1}, // not found
	}

	for _, tt := range tests {
		idx := SymbolIndex(rel, tt.symbol)
		if idx != tt.expected {
			t.Errorf("SymbolIndex(%s) = %d, want %d", tt.symbol, idx, tt.expected)
		}
	}
}

func TestCommonSymbols(t *testing.T) {
	rel1 := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")}, nil)
	rel2 := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?b"), datalog.NewSymbol("?c"), datalog.NewSymbol("?d")}, nil)

	common := CommonSymbols(rel1, rel2)

	if len(common) != 2 {
		t.Errorf("expected 2 common symbols, got %d", len(common))
	}

	// Check that ?b and ?c are in common
	commonSet := make(map[query.Symbol]bool)
	for _, col := range common {
		commonSet[col] = true
	}

	if !commonSet[datalog.NewSymbol("?b")] || !commonSet[datalog.NewSymbol("?c")] {
		t.Errorf("expected ?b and ?c in common symbols, got %v", common)
	}
}

func TestProject(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?city")}
	tuples := []Tuple{
		{"Alice", 30, "NYC"},
		{"Bob", 25, "LA"},
	}

	rel := NewMaterializedRelation(symbols, tuples)

	// Project to subset of symbols using method
	projected, err := rel.Project([]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?city")})
	if err != nil {
		t.Fatalf("Project failed: %v", err)
	}

	// Check symbols
	projCols := projected.Symbols()
	if len(projCols) != 2 || projCols[0] != datalog.NewSymbol("?name") || projCols[1] != datalog.NewSymbol("?city") {
		t.Errorf("unexpected projected symbols: %v", projCols)
	}

	// Check tuples
	it := projected.Iterator()
	defer it.Close()

	expected := []Tuple{
		{"Alice", "NYC"},
		{"Bob", "LA"},
	}

	i := 0
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) != 2 {
			t.Errorf("expected tuple length 2, got %d", len(tuple))
		}
		if tuple[0] != expected[i][0] || tuple[1] != expected[i][1] {
			t.Errorf("tuple %d: expected %v, got %v", i, expected[i], tuple)
		}
		i++
	}

	// Test projecting non-existent symbol using method
	_, err = rel.Project([]query.Symbol{datalog.NewSymbol("?nonexistent")})
	if err == nil {
		t.Fatal("Expected error when projecting non-existent symbol")
	}
}

func TestSelect(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?city")}
	tuples := []Tuple{
		{"Alice", 30, "NYC"},
		{"Bob", 25, "LA"},
		{"Charlie", 35, "NYC"},
		{"David", 28, "NYC"},
	}

	rel := NewMaterializedRelation(symbols, tuples)

	// Select people in NYC
	nycOnly := Select(rel, func(tuple Tuple) bool {
		return tuple[2] == "NYC"
	})

	if nycOnly.Size() != 3 {
		t.Errorf("expected 3 NYC residents, got %d", nycOnly.Size())
	}

	// Select people over 30
	over30 := Select(rel, func(tuple Tuple) bool {
		age, ok := tuple[1].(int)
		return ok && age > 30
	})

	if over30.Size() != 1 {
		t.Errorf("expected 1 person over 30, got %d", over30.Size())
	}

	// Verify the right person
	it := over30.Iterator()
	if it.Next() {
		tuple := it.Tuple()
		if tuple[0] != "Charlie" {
			t.Errorf("expected Charlie, got %v", tuple[0])
		}
	}
	it.Close()
}

func TestStreamingRelation(t *testing.T) {
	// Create a simple iterator that yields 3 tuples
	tuples := []Tuple{
		{"A", 1},
		{"B", 2},
		{"C", 3},
	}

	it := &sliceIterator{tuples: tuples, pos: -1}
	symbols := []query.Symbol{datalog.NewSymbol("?letter"), datalog.NewSymbol("?number")}

	rel := NewStreamingRelation(symbols, it)

	// Test symbols
	if len(rel.Symbols()) != 2 {
		t.Errorf("expected 2 symbols, got %d", len(rel.Symbols()))
	}

	// Test iteration
	relIt := rel.Iterator()
	count := 0
	for relIt.Next() {
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 tuples, got %d", count)
	}
}
