package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestMaterializedRelation(t *testing.T) {
	columns := []query.Symbol{"?name", "?age", "?city"}
	tuples := []Tuple{
		{"Alice", 30, "NYC"},
		{"Bob", 25, "LA"},
		{"Charlie", 35, "NYC"},
	}

	rel := NewMaterializedRelation(columns, tuples)

	// Test basic properties
	if rel.Size() != 3 {
		t.Errorf("expected size 3, got %d", rel.Size())
	}

	if rel.IsEmpty() {
		t.Error("expected non-empty relation")
	}

	// Test columns
	cols := rel.Columns()
	if len(cols) != 3 || cols[0] != "?name" || cols[1] != "?age" || cols[2] != "?city" {
		t.Errorf("unexpected columns: %v", cols)
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
	columns := []query.Symbol{"?x", "?y"}
	rel := NewMaterializedRelation(columns, nil)

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

func TestColumnIndex(t *testing.T) {
	columns := []query.Symbol{"?a", "?b", "?c"}
	rel := NewMaterializedRelation(columns, nil)

	tests := []struct {
		symbol   query.Symbol
		expected int
	}{
		{"?a", 0},
		{"?b", 1},
		{"?c", 2},
		{"?d", -1}, // not found
	}

	for _, tt := range tests {
		idx := ColumnIndex(rel, tt.symbol)
		if idx != tt.expected {
			t.Errorf("ColumnIndex(%s) = %d, want %d", tt.symbol, idx, tt.expected)
		}
	}
}

func TestCommonColumns(t *testing.T) {
	rel1 := NewMaterializedRelation([]query.Symbol{"?a", "?b", "?c"}, nil)
	rel2 := NewMaterializedRelation([]query.Symbol{"?b", "?c", "?d"}, nil)

	common := CommonColumns(rel1, rel2)

	if len(common) != 2 {
		t.Errorf("expected 2 common columns, got %d", len(common))
	}

	// Check that ?b and ?c are in common
	commonSet := make(map[query.Symbol]bool)
	for _, col := range common {
		commonSet[col] = true
	}

	if !commonSet["?b"] || !commonSet["?c"] {
		t.Errorf("expected ?b and ?c in common columns, got %v", common)
	}
}

func TestProject(t *testing.T) {
	columns := []query.Symbol{"?name", "?age", "?city"}
	tuples := []Tuple{
		{"Alice", 30, "NYC"},
		{"Bob", 25, "LA"},
	}

	rel := NewMaterializedRelation(columns, tuples)

	// Project to subset of columns using method
	projected, err := rel.Project([]query.Symbol{"?name", "?city"})
	if err != nil {
		t.Fatalf("Project failed: %v", err)
	}

	// Check columns
	projCols := projected.Columns()
	if len(projCols) != 2 || projCols[0] != "?name" || projCols[1] != "?city" {
		t.Errorf("unexpected projected columns: %v", projCols)
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

	// Test projecting non-existent column using method
	_, err = rel.Project([]query.Symbol{"?nonexistent"})
	if err == nil {
		t.Fatal("Expected error when projecting non-existent column")
	}
}

func TestSelect(t *testing.T) {
	columns := []query.Symbol{"?name", "?age", "?city"}
	tuples := []Tuple{
		{"Alice", 30, "NYC"},
		{"Bob", 25, "LA"},
		{"Charlie", 35, "NYC"},
		{"David", 28, "NYC"},
	}

	rel := NewMaterializedRelation(columns, tuples)

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
	columns := []query.Symbol{"?letter", "?number"}

	rel := NewStreamingRelation(columns, it)

	// Test columns
	if len(rel.Columns()) != 2 {
		t.Errorf("expected 2 columns, got %d", len(rel.Columns()))
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
