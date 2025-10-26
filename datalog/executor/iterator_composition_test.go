package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// mockIterator provides test data for iterator composition tests
type mockIterator struct {
	tuples []Tuple
	pos    int
}

func newMockIterator(tuples []Tuple) *mockIterator {
	return &mockIterator{
		tuples: tuples,
		pos:    -1,
	}
}

func (it *mockIterator) Next() bool {
	it.pos++
	return it.pos < len(it.tuples)
}

func (it *mockIterator) Tuple() Tuple {
	if it.pos >= 0 && it.pos < len(it.tuples) {
		return it.tuples[it.pos]
	}
	return nil
}

func (it *mockIterator) Close() error {
	return nil
}

func TestFilterIterator(t *testing.T) {
	// Create test data
	tuples := []Tuple{
		{1, "alice", 25},
		{2, "bob", 30},
		{3, "charlie", 25},
		{4, "diana", 35},
		{5, "eve", 30},
	}
	columns := []query.Symbol{"?id", "?name", "?age"}

	// Test filtering by age
	source := newMockIterator(tuples)
	filter := NewSimpleFilter(func(t Tuple) bool {
		return t[2].(int) == 30 // Age == 30
	})
	filterIter := NewFilterIterator(source, columns, filter)

	// Collect filtered results
	var results []Tuple
	for filterIter.Next() {
		tuple := filterIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	filterIter.Close()

	// Verify results
	assert.Len(t, results, 2)
	assert.Equal(t, "bob", results[0][1])
	assert.Equal(t, "eve", results[1][1])
}

func TestProjectIterator(t *testing.T) {
	// Create test data
	tuples := []Tuple{
		{1, "alice", 25, "NYC"},
		{2, "bob", 30, "LA"},
		{3, "charlie", 25, "Chicago"},
	}
	sourceColumns := []query.Symbol{"?id", "?name", "?age", "?city"}
	targetColumns := []query.Symbol{"?name", "?city"} // Project name and city only

	// Create projection iterator
	// Wrap tuples in a relation for ProjectIterator
	sourceRel := NewMaterializedRelation(sourceColumns, tuples)
	projIter := NewProjectIterator(sourceRel, sourceColumns, targetColumns)

	// Collect projected results
	var results []Tuple
	for projIter.Next() {
		tuple := projIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	projIter.Close()

	// Verify results
	assert.Len(t, results, 3)
	assert.Len(t, results[0], 2) // Only 2 columns
	assert.Equal(t, "alice", results[0][0])
	assert.Equal(t, "NYC", results[0][1])
	assert.Equal(t, "bob", results[1][0])
	assert.Equal(t, "LA", results[1][1])
}

func TestTransformIterator(t *testing.T) {
	// Create test data
	tuples := []Tuple{
		{1, 10},
		{2, 20},
		{3, 30},
	}

	// Transform function: double the second value
	transform := func(t Tuple) Tuple {
		return Tuple{t[0], t[1].(int) * 2}
	}

	// Create transform iterator
	source := newMockIterator(tuples)
	transformIter := NewTransformIterator(source, transform)

	// Collect transformed results
	var results []Tuple
	for transformIter.Next() {
		tuple := transformIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	transformIter.Close()

	// Verify results
	assert.Len(t, results, 3)
	assert.Equal(t, 20, results[0][1])
	assert.Equal(t, 40, results[1][1])
	assert.Equal(t, 60, results[2][1])
}

func TestConcatIterator(t *testing.T) {
	// Create multiple iterators with different data
	iter1 := newMockIterator([]Tuple{{1, "a"}, {2, "b"}})
	iter2 := newMockIterator([]Tuple{{3, "c"}})
	iter3 := newMockIterator([]Tuple{{4, "d"}, {5, "e"}})

	// Create concat iterator
	concatIter := NewConcatIterator(iter1, iter2, iter3)

	// Collect all results
	var results []Tuple
	for concatIter.Next() {
		tuple := concatIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	concatIter.Close()

	// Verify results
	assert.Len(t, results, 5)
	assert.Equal(t, 1, results[0][0])
	assert.Equal(t, "a", results[0][1])
	assert.Equal(t, 5, results[4][0])
	assert.Equal(t, "e", results[4][1])
}

func TestDedupIterator(t *testing.T) {
	// Create test data with duplicates
	tuples := []Tuple{
		{1, "alice"},
		{2, "bob"},
		{1, "alice"}, // Duplicate
		{3, "charlie"},
		{2, "bob"}, // Duplicate
		{4, "diana"},
	}

	// Create dedup iterator
	source := newMockIterator(tuples)
	dedupIter := NewDedupIterator(source, 10)

	// Collect deduplicated results
	var results []Tuple
	for dedupIter.Next() {
		tuple := dedupIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	dedupIter.Close()

	// Verify results
	assert.Len(t, results, 4) // Only unique tuples
	assert.Equal(t, "alice", results[0][1])
	assert.Equal(t, "bob", results[1][1])
	assert.Equal(t, "charlie", results[2][1])
	assert.Equal(t, "diana", results[3][1])
}

func TestComposedIterators(t *testing.T) {
	// Test chaining multiple iterators together
	// Filter -> Project -> Transform
	tuples := []Tuple{
		{1, "alice", 25, 1000},
		{2, "bob", 30, 1500},
		{3, "charlie", 25, 1200},
		{4, "diana", 35, 2000},
	}
	columns := []query.Symbol{"?id", "?name", "?age", "?salary"}

	// Step 1: Filter by age >= 30
	source := newMockIterator(tuples)
	filter := NewSimpleFilter(func(t Tuple) bool {
		return t[2].(int) >= 30
	})
	filterIter := NewFilterIterator(source, columns, filter)

	// Step 2: Project name and salary only
	projectedColumns := []query.Symbol{"?name", "?salary"}
	projIndices := []int{1, 3} // Manually specify indices for test
	projIter := &ProjectIterator{
		source:     filterIter,
		indices:    projIndices,
		newColumns: projectedColumns,
	}

	// Step 3: Transform to add bonus (10% of salary)
	transform := func(t Tuple) Tuple {
		salary := t[1].(int)
		bonus := salary / 10
		return Tuple{t[0], salary, bonus}
	}
	transformIter := NewTransformIterator(projIter, transform)

	// Collect final results
	var results []Tuple
	for transformIter.Next() {
		tuple := transformIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	transformIter.Close()

	// Verify results
	assert.Len(t, results, 2) // Only bob and diana (age >= 30)
	assert.Equal(t, "bob", results[0][0])
	assert.Equal(t, 1500, results[0][1])
	assert.Equal(t, 150, results[0][2]) // 10% bonus
	assert.Equal(t, "diana", results[1][0])
	assert.Equal(t, 2000, results[1][1])
	assert.Equal(t, 200, results[1][2]) // 10% bonus
}

func TestStreamingRelationWithComposition(t *testing.T) {
	// Test the StreamingRelation with iterator composition enabled
	opts := ExecutorOptions{
		EnableIteratorComposition: true,
		EnableTrueStreaming:       true,
	}

	tuples := []Tuple{
		{1, "alice", 25},
		{2, "bob", 30},
		{3, "charlie", 25},
		{4, "diana", 35},
	}
	columns := []query.Symbol{"?id", "?name", "?age"}

	// Create a StreamingRelation
	source := newMockIterator(tuples)
	rel := NewStreamingRelationWithOptions(columns, source, opts)

	// Test that Size() doesn't materialize when TrueStreaming is enabled
	size := rel.Size()
	assert.Equal(t, -1, size) // Unknown size

	// Test filter operation returns streaming relation
	filter := NewSimpleFilter(func(t Tuple) bool {
		return t[2].(int) >= 30
	})
	filtered := rel.Filter(filter)

	// Verify it's still streaming (not materialized)
	assert.IsType(t, &StreamingRelation{}, filtered)

	// Project columns
	projected, err := filtered.Project([]query.Symbol{"?name", "?age"})
	assert.NoError(t, err)
	assert.IsType(t, &StreamingRelation{}, projected)

	// Finally iterate and check results
	it := projected.Iterator()
	var results []Tuple
	for it.Next() {
		tuple := it.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	it.Close()

	// Verify final results
	assert.Len(t, results, 2)
	assert.Equal(t, "bob", results[0][0])
	assert.Equal(t, 30, results[0][1])
	assert.Equal(t, "diana", results[1][0])
	assert.Equal(t, 35, results[1][1])
}

func TestPredicateFilterIterator(t *testing.T) {
	// Test filtering with query.Predicate
	tuples := []Tuple{
		{1, 10},
		{2, 20},
		{3, 30},
		{4, 40},
	}
	columns := []query.Symbol{"?x", "?y"}

	// Create a comparison predicate (y > 20)
	pred := &query.Comparison{
		Op:    query.OpGT,
		Left:  query.VariableTerm{Symbol: "?y"},
		Right: query.ConstantTerm{Value: 20},
	}

	source := newMockIterator(tuples)
	predIter := NewPredicateFilterIterator(source, columns, pred)

	// Collect filtered results
	var results []Tuple
	for predIter.Next() {
		tuple := predIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	predIter.Close()

	// Verify results
	assert.Len(t, results, 2)
	assert.Equal(t, 30, results[0][1])
	assert.Equal(t, 40, results[1][1])
}

func TestFunctionEvaluatorIterator(t *testing.T) {
	// Test function evaluation that adds a new column
	tuples := []Tuple{
		{10, 20},
		{30, 40},
		{50, 60},
	}
	columns := []query.Symbol{"?x", "?y"}

	// Create an addition function (x + y)
	fn := query.ArithmeticFunction{
		Op:    query.OpAdd,
		Left:  query.VariableTerm{Symbol: "?x"},
		Right: query.VariableTerm{Symbol: "?y"},
	}

	source := newMockIterator(tuples)
	evalIter := NewFunctionEvaluatorIterator(source, columns, fn, "?sum")

	// Collect results with new column
	var results []Tuple
	for evalIter.Next() {
		tuple := evalIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	evalIter.Close()

	// Verify results
	assert.Len(t, results, 3)
	assert.Len(t, results[0], 3)               // Original 2 columns + 1 new
	assert.Equal(t, int64(30), results[0][2])  // 10 + 20 (ArithmeticFunction returns int64)
	assert.Equal(t, int64(70), results[1][2])  // 30 + 40
	assert.Equal(t, int64(110), results[2][2]) // 50 + 60
}

// BenchmarkIteratorComposition benchmarks composed iterators vs materialized operations
func BenchmarkIteratorComposition(b *testing.B) {
	// Create large test dataset
	var tuples []Tuple
	for i := 0; i < 10000; i++ {
		tuples = append(tuples, Tuple{i, i * 2, i * 3})
	}
	columns := []query.Symbol{"?x", "?y", "?z"}

	b.Run("Composed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			source := newMockIterator(tuples)

			// Filter -> Project -> Transform
			filter := NewSimpleFilter(func(t Tuple) bool {
				return t[0].(int)%2 == 0 // Even numbers
			})
			filterIter := NewFilterIterator(source, columns, filter)

			// Wrap in a relation for projection
			filteredRel := NewStreamingRelation(columns, filterIter)
			projIter := NewProjectIterator(filteredRel, columns, []query.Symbol{"?x", "?z"})

			transform := func(t Tuple) Tuple {
				return Tuple{t[0], t[1].(int) * 10}
			}
			transformIter := NewTransformIterator(projIter, transform)

			// Consume results
			count := 0
			for transformIter.Next() {
				count++
			}
			transformIter.Close()
		}
	})

	b.Run("Materialized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate materialized approach
			rel := NewMaterializedRelation(columns, tuples)

			// Filter
			var filtered []Tuple
			it := rel.Iterator()
			for it.Next() {
				t := it.Tuple()
				if t[0].(int)%2 == 0 {
					filtered = append(filtered, t)
				}
			}
			it.Close()

			// Project
			var projected []Tuple
			for _, t := range filtered {
				projected = append(projected, Tuple{t[0], t[2]})
			}

			// Transform
			var transformed []Tuple
			for _, t := range projected {
				transformed = append(transformed, Tuple{t[0], t[1].(int) * 10})
			}

			count := len(transformed)
			_ = count
		}
	})
}
