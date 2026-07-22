package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// mockIterator provides test data for iterator composition tests
type mockIterator struct {
	tuples []Tuple
	pos    int
	err    error
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

func (it *mockIterator) Error() error { return it.err }

func TestProjectIterator(t *testing.T) {
	// Create test data
	tuples := []Tuple{
		{1, "alice", 25, "NYC"},
		{2, "bob", 30, "LA"},
		{3, "charlie", 25, "Chicago"},
	}
	sourceSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?city")}
	targetSymbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?city")} // Project name and city only

	// Create projection iterator
	// Wrap tuples in a relation for ProjectIterator
	sourceRel := NewMaterializedRelation(sourceSymbols, tuples)
	projIter := NewProjectIterator(sourceRel, sourceSymbols, targetSymbols)

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
	assert.Len(t, results[0], 2) // Only 2 symbols
	assert.Equal(t, "alice", results[0][0])
	assert.Equal(t, "NYC", results[0][1])
	assert.Equal(t, "bob", results[1][0])
	assert.Equal(t, "LA", results[1][1])
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
	dedupIter := NewDedupIterator(source, 10, false)

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
	// Test chaining multiple iterators together: Filter -> Project
	tuples := []Tuple{
		{1, "alice", 25, 1000},
		{2, "bob", 30, 1500},
		{3, "charlie", 25, 1200},
		{4, "diana", 35, 2000},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?salary")}

	// Step 1: Filter by age >= 30
	source := newMockIterator(tuples)
	filterIter := NewPredicateFilterIterator(source, symbols, &query.Comparison{
		Op:    datalog.SymGTE,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
		Right: query.ConstantTerm{Value: int64(30)},
	})

	// Step 2: Project name and salary only
	projectedSymbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?salary")}
	projIndices := []int{1, 3} // Manually specify indices for test
	projIter := &ProjectIterator{
		source:     filterIter,
		indices:    projIndices,
		newSymbols: projectedSymbols,
	}

	// Collect final results
	var results []Tuple
	for projIter.Next() {
		tuple := projIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	projIter.Close()

	// Verify results
	assert.Len(t, results, 2) // Only bob and diana (age >= 30)
	assert.Equal(t, "bob", results[0][0])
	assert.Equal(t, 1500, results[0][1])
	assert.Equal(t, "diana", results[1][0])
	assert.Equal(t, 2000, results[1][1])
}

func TestStreamingRelationWithComposition(t *testing.T) {
	// Test the StreamingRelation with true streaming enabled
	opts := ExecutorOptions{
		EnableTrueStreaming: true,
	}

	tuples := []Tuple{
		{1, "alice", 25},
		{2, "bob", 30},
		{3, "charlie", 25},
		{4, "diana", 35},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?age")}

	// Create a StreamingRelation
	source := newMockIterator(tuples)
	rel := NewStreamingRelationWithOptions(symbols, source, opts)

	// Test that Size() doesn't materialize when TrueStreaming is enabled
	size := rel.Size()
	assert.Equal(t, -1, size) // Unknown size

	// Test filter operation returns streaming relation
	filtered := rel.FilterWithPredicate(&query.Comparison{
		Op:    datalog.SymGTE,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
		Right: query.ConstantTerm{Value: int64(30)},
	})

	// Verify it's still streaming (not materialized)
	assert.IsType(t, &StreamingRelation{}, filtered)

	// Project symbols
	projected, err := filtered.Project([]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age")})
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
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// Create a comparison predicate (y > 20)
	pred := &query.Comparison{
		Op:    datalog.SymGT,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
		Right: query.ConstantTerm{Value: 20},
	}

	source := newMockIterator(tuples)
	predIter := NewPredicateFilterIterator(source, symbols, pred)

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
	// Test function evaluation that adds a new symbol
	tuples := []Tuple{
		{10, 20},
		{30, 40},
		{50, 60},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// Create an addition function (x + y)
	fn := query.ArithmeticFunction{
		Op: datalog.SymAdd,
		Args: []query.Term{
			query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
			query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
		},
	}

	source := newMockIterator(tuples)
	evalIter := NewFunctionEvaluatorIterator(source, symbols, fn, datalog.NewSymbol("?sum"))

	// Collect results with new symbol
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
	assert.Len(t, results[0], 3)               // Original 2 symbols + 1 new
	assert.Equal(t, int64(30), results[0][2])  // 10 + 20 (ArithmeticFunction returns int64)
	assert.Equal(t, int64(70), results[1][2])  // 30 + 40
	assert.Equal(t, int64(110), results[2][2]) // 50 + 60
}

// TestFunctionEvaluatorIterator_UnifiesExistingSymbol verifies that when the
// output symbol already exists in the source, the iterator filters (unifies)
// instead of appending a duplicate. In Datalog, [(+ ?x 0) ?x] means "keep
// tuples where ?x + 0 == ?x", not "add a second ?x binding."
func TestFunctionEvaluatorIterator_UnifiesExistingSymbol(t *testing.T) {
	tuples := []Tuple{
		{int64(10), int64(20)},
		{int64(30), int64(40)},
		{int64(50), int64(60)},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// identity function on ?x, binding back to ?x — should unify (all pass)
	fn := query.ArithmeticFunction{
		Op: datalog.SymAdd,
		Args: []query.Term{
			query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
			query.ConstantTerm{Value: int64(0)},
		},
	}

	source := newMockIterator(tuples)
	evalIter := NewFunctionEvaluatorIterator(source, symbols, fn, datalog.NewSymbol("?x"))

	var results []Tuple
	for evalIter.Next() {
		tuple := evalIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	evalIter.Close()

	// All 3 tuples should pass (identity unification)
	assert.Len(t, results, 3, "all tuples should pass identity unification")
	// Tuple width should be 2 (not 3) — no duplicate symbol
	assert.Len(t, results[0], 2, "should not duplicate existing symbol")
	assert.Equal(t, int64(10), results[0][0])
	assert.Equal(t, int64(20), results[0][1])
}

// TestFunctionEvaluatorIterator_UnifiesFilters verifies that unification
// filters tuples where the function result doesn't match the existing binding.
func TestFunctionEvaluatorIterator_UnifiesFilters(t *testing.T) {
	tuples := []Tuple{
		{int64(10), int64(20)}, // ?x=10, ?y=20, ?x+?y=30 != ?x → filtered
		{int64(0), int64(0)},   // ?x=0, ?y=0, ?x+?y=0 == ?x → passes
		{int64(5), int64(-5)},  // ?x=5, ?y=-5, ?x+?y=0 != ?x → filtered
	}
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// (+ ?x ?y) binding to ?x — unifies: keep only where ?x + ?y == ?x
	fn := query.ArithmeticFunction{
		Op: datalog.SymAdd,
		Args: []query.Term{
			query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
			query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
		},
	}

	source := newMockIterator(tuples)
	evalIter := NewFunctionEvaluatorIterator(source, symbols, fn, datalog.NewSymbol("?x"))

	var results []Tuple
	for evalIter.Next() {
		tuple := evalIter.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}
	evalIter.Close()

	// Only tuple {0, 0} passes: 0 + 0 == 0 == ?x
	assert.Len(t, results, 1, "only tuples where function result matches existing binding should pass")
	assert.Equal(t, int64(0), results[0][0])
	assert.Equal(t, int64(0), results[0][1])
}

// BenchmarkIteratorComposition benchmarks composed iterators vs materialized operations
func BenchmarkIteratorComposition(b *testing.B) {
	// Create large test dataset. ?y alternates so the filter below selects
	// every other tuple (scattered matches, ~50% selectivity).
	var tuples []Tuple
	for i := 0; i < 10000; i++ {
		tuples = append(tuples, Tuple{i, i % 2, i * 3})
	}
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}

	b.Run("Composed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			source := newMockIterator(tuples)

			// Filter -> Project
			filterIter := NewPredicateFilterIterator(source, symbols, &query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
				Right: query.ConstantTerm{Value: int64(0)},
			})

			// Wrap in a relation for projection
			filteredRel := NewStreamingRelation(symbols, filterIter)
			projIter := NewProjectIterator(filteredRel, symbols, []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")})

			// Consume results
			count := 0
			for projIter.Next() {
				count++
			}
			projIter.Close()
		}
	})

	b.Run("Materialized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate materialized approach
			rel := NewMaterializedRelation(symbols, tuples)

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
