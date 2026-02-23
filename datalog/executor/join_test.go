package executor

import (
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestHashJoin(t *testing.T) {
	// Left relation: people and their departments
	leftCols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")}
	leftTuples := []Tuple{
		{"Alice", "Engineering"},
		{"Bob", "Sales"},
		{"Charlie", "Engineering"},
	}
	left := NewMaterializedRelation(leftCols, leftTuples)

	// Right relation: departments and their locations
	rightCols := []query.Symbol{datalog.NewSymbol("?dept"), datalog.NewSymbol("?location")}
	rightTuples := []Tuple{
		{"Engineering", "Building A"},
		{"Sales", "Building B"},
		{"Marketing", "Building C"},
	}
	right := NewMaterializedRelation(rightCols, rightTuples)

	// Join on ?dept
	joined := left.HashJoin(right, []query.Symbol{datalog.NewSymbol("?dept")})

	// Expected: 3 results (no Marketing people)
	if joined.Size() != 3 {
		t.Errorf("expected 3 joined tuples, got %d", joined.Size())
	}

	// Check symbols
	expectedCols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept"), datalog.NewSymbol("?location")}
	if !reflect.DeepEqual(joined.Symbols(), expectedCols) {
		t.Errorf("expected symbols %v, got %v", expectedCols, joined.Symbols())
	}

	// Collect results
	results := collectTuples(joined)

	// Verify specific joins
	expected := []Tuple{
		{"Alice", "Engineering", "Building A"},
		{"Bob", "Sales", "Building B"},
		{"Charlie", "Engineering", "Building A"},
	}

	if !tuplesEqual(results, expected) {
		t.Errorf("unexpected join results:\ngot:  %v\nwant: %v", results, expected)
	}
}

func TestJoinMultipleColumns(t *testing.T) {
	// Test joining on multiple symbols
	leftCols := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")}
	leftTuples := []Tuple{
		{1, 2, "x"},
		{1, 3, "y"},
		{2, 2, "z"},
	}
	left := NewMaterializedRelation(leftCols, leftTuples)

	rightCols := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?d")}
	rightTuples := []Tuple{
		{1, 2, "foo"},
		{1, 3, "bar"},
		{2, 3, "baz"},
	}
	right := NewMaterializedRelation(rightCols, rightTuples)

	// Join on both ?a and ?b
	joined := left.HashJoin(right, []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")})

	// Should match: (1,2) and (1,3)
	if joined.Size() != 2 {
		t.Errorf("expected 2 joined tuples, got %d", joined.Size())
	}

	results := collectTuples(joined)
	expected := []Tuple{
		{1, 2, "x", "foo"},
		{1, 3, "y", "bar"},
	}

	if !tuplesEqual(results, expected) {
		t.Errorf("unexpected join results:\ngot:  %v\nwant: %v", results, expected)
	}
}

func TestEmptyJoin(t *testing.T) {
	// Join with no common values
	leftCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	leftTuples := []Tuple{{1, 2}, {3, 4}}
	left := NewMaterializedRelation(leftCols, leftTuples)

	rightCols := []query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	rightTuples := []Tuple{{5, 6}, {7, 8}}
	right := NewMaterializedRelation(rightCols, rightTuples)

	joined := left.HashJoin(right, []query.Symbol{datalog.NewSymbol("?y")})

	if !joined.IsEmpty() {
		t.Error("expected empty join result")
	}
}

func TestCrossProduct(t *testing.T) {
	// Test cross product (no common symbols)
	leftCols := []query.Symbol{datalog.NewSymbol("?a")}
	leftTuples := []Tuple{{"x"}, {"y"}}
	left := NewMaterializedRelation(leftCols, leftTuples)

	rightCols := []query.Symbol{datalog.NewSymbol("?b")}
	rightTuples := []Tuple{{1}, {2}}
	right := NewMaterializedRelation(rightCols, rightTuples)

	joined := left.Join(right)

	// Should be 2x2 = 4 tuples
	if joined.Size() != 4 {
		t.Errorf("expected 4 tuples in cross product, got %d", joined.Size())
	}

	results := collectTuples(joined)
	expected := []Tuple{
		{"x", 1},
		{"x", 2},
		{"y", 1},
		{"y", 2},
	}

	if !tuplesEqual(results, expected) {
		t.Errorf("unexpected cross product:\ngot:  %v\nwant: %v", results, expected)
	}
}

func TestSemiJoin(t *testing.T) {
	// Left: all people
	leftCols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")}
	leftTuples := []Tuple{
		{"Alice", "Engineering"},
		{"Bob", "Sales"},
		{"Charlie", "Engineering"},
		{"David", "HR"},
	}
	left := NewMaterializedRelation(leftCols, leftTuples)

	// Right: active departments
	rightCols := []query.Symbol{datalog.NewSymbol("?dept")}
	rightTuples := []Tuple{
		{"Engineering"},
		{"Sales"},
	}
	right := NewMaterializedRelation(rightCols, rightTuples)

	// Semi-join: people in active departments
	result := left.SemiJoin(right, []query.Symbol{datalog.NewSymbol("?dept")})

	if result.Size() != 3 {
		t.Errorf("expected 3 people in active departments, got %d", result.Size())
	}

	// David from HR should be filtered out
	results := collectTuples(result)
	for _, tuple := range results {
		if tuple[0] == "David" {
			t.Error("David should have been filtered out")
		}
	}
}

func TestAntiJoin(t *testing.T) {
	// Same setup as SemiJoin
	leftCols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")}
	leftTuples := []Tuple{
		{"Alice", "Engineering"},
		{"Bob", "Sales"},
		{"Charlie", "Engineering"},
		{"David", "HR"},
	}
	left := NewMaterializedRelation(leftCols, leftTuples)

	rightCols := []query.Symbol{datalog.NewSymbol("?dept")}
	rightTuples := []Tuple{
		{"Engineering"},
		{"Sales"},
	}
	right := NewMaterializedRelation(rightCols, rightTuples)

	// Anti-join: people NOT in active departments
	result := left.AntiJoin(right, []query.Symbol{datalog.NewSymbol("?dept")})

	if result.Size() != 1 {
		t.Errorf("expected 1 person not in active departments, got %d", result.Size())
	}

	// Only David from HR should remain
	it := result.Iterator()
	if it.Next() {
		tuple := it.Tuple()
		if tuple[0] != "David" || tuple[1] != "HR" {
			t.Errorf("expected David from HR, got %v", tuple)
		}
	}
	it.Close()
}

// Helper functions

func collectTuples(rel Relation) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}
	return tuples
}

func tuplesEqual(a, b []Tuple) bool {
	if len(a) != len(b) {
		return false
	}

	// For simplicity, assume order matters
	// In real implementation might want set comparison
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestJoinBuildCopyAnnotation(t *testing.T) {
	// Test that the copy tracking annotation is emitted when a collector is provided

	// Create a collector to capture events
	var events []annotations.Event
	handler := func(e annotations.Event) {
		events = append(events, e)
	}
	collector := annotations.NewCollector(handler)

	// Create relations with options that include the collector
	opts := ExecutorOptions{
		Collector: collector,
	}

	// MaterializedRelation doesn't require copying (RequiresCopy() = false)
	leftCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	leftTuples := []Tuple{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	}
	left := NewMaterializedRelationWithOptions(leftCols, leftTuples, opts)

	rightCols := []query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	rightTuples := []Tuple{
		{"a", 100},
		{"b", 200},
		{"d", 400},
	}
	right := NewMaterializedRelationWithOptions(rightCols, rightTuples, opts)

	// Perform the join
	joined := HashJoinWithOptions(left, right, []query.Symbol{datalog.NewSymbol("?y")}, opts)

	// Materialize to ensure build phase runs
	_ = collectTuples(joined)

	// Check that we got the copy tracking annotation
	var foundCopyAnnotation bool
	var copyCount, skipCount int
	var requiresCopy bool
	for _, e := range events {
		if e.Name == annotations.JoinBuildCopy {
			foundCopyAnnotation = true
			if c, ok := e.Data["copied"].(int); ok {
				copyCount = c
			}
			if s, ok := e.Data["passthru"].(int); ok {
				skipCount = s
			}
			if rc, ok := e.Data["requires_copy"].(bool); ok {
				requiresCopy = rc
			}
		}
	}

	if !foundCopyAnnotation {
		t.Error("expected JoinBuildCopy annotation to be emitted")
	}

	// MaterializedRelation doesn't require copying, so we should see passthru > 0, copied = 0
	if requiresCopy {
		t.Error("expected requires_copy=false for MaterializedRelation")
	}
	if copyCount != 0 {
		t.Errorf("expected 0 copies for MaterializedRelation, got %d", copyCount)
	}
	if skipCount == 0 {
		t.Errorf("expected some passthru copies for MaterializedRelation, got %d", skipCount)
	}

	t.Logf("Copy stats: copied=%d, passthru=%d, requires_copy=%v", copyCount, skipCount, requiresCopy)
}
