package storage

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestMultiPositionBindingCorrectness verifies correctness when both E and V are bound
// This is the core regression test for the fix
func TestMultiPositionBindingCorrectness(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: 10 entities, each with :attr/code attribute
			// Entities 0,1,2,3,4 have code="A", entities 5,6,7,8,9 have code="B"
			tx := db.NewTransaction()
			entities := make([]datalog.Identity, 10)

			for i := 0; i < 10; i++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
				entities[i] = entityID

				code := "A"
				if i >= 5 {
					code = "B"
				}
				tx.Add(entityID, datalog.NewKeyword(":attr/code"), code)
			}

			_, err := tx.Commit()
			require.NoError(t, err)

			// Create pattern: [?e :attr/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":attr/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			// Create binding relation with entities 0,2,4,6,8 with code="A"
			// Entities 0,2,4 have code="A" in DB, entities 6,8 have code="B" in DB
			// So only entities 0,2,4 should match (correct entity+value combo)
			bindingTuples := []executor.Tuple{
				{entities[0], "A"},
				{entities[2], "A"},
				{entities[4], "A"},
				{entities[6], "A"}, // Has code="B" in DB, should NOT match
				{entities[8], "A"}, // Has code="B" in DB, should NOT match
			}
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				bindingTuples,
			)

			// Create matcher and execute
			matcher := NewPatternMatcher(db.Store())
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
			require.NoError(t, err)

			// Collect results
			var results []executor.Tuple
			it := result.Iterator()
			for it.Next() {
				tuple := it.Tuple()
				tupleCopy := make(executor.Tuple, len(tuple))
				copy(tupleCopy, tuple)
				results = append(results, tupleCopy)
			}
			it.Close()

			// Expected: Only entities 0, 2, 4 (those with code="A" AND in binding set)
			// NOT entities 6, 8 (have code="B" in DB, even though binding says "A")
			require.Equal(t, 3, len(results), "Expected 3 results (entities with code='A'), got %d", len(results))

			// Verify the correct entities are returned
			// Use L85() for comparison since storage-returned entities don't have original strings
			resultEntities := make(map[string]bool)
			for _, tuple := range results {
				// Identity is always a pointer type
				if id, ok := tuple[0].(datalog.Identity); ok {
					resultEntities[id.L85()] = true
				}
			}

			require.True(t, resultEntities[entities[0].L85()], "Entity 0 should be in results")
			require.True(t, resultEntities[entities[2].L85()], "Entity 2 should be in results")
			require.True(t, resultEntities[entities[4].L85()], "Entity 4 should be in results")
			require.False(t, resultEntities[entities[6].L85()], "Entity 6 should NOT be in results")
			require.False(t, resultEntities[entities[8].L85()], "Entity 8 should NOT be in results")
		})
	}
}

// TestMultiPositionAllCombinations tests E+A, E+V, A+V, E+A+V bound cases
func TestMultiPositionAllCombinations(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: Create entities with multiple attributes
			tx := db.NewTransaction()

			entity1 := datalog.NewIdentity("entity:1")
			entity2 := datalog.NewIdentity("entity:2")
			entity3 := datalog.NewIdentity("entity:3")

			// entity1: :attr/name="Alice", :attr/age=25
			tx.Add(entity1, datalog.NewKeyword(":attr/name"), "Alice")
			tx.Add(entity1, datalog.NewKeyword(":attr/age"), int64(25))

			// entity2: :attr/name="Bob", :attr/age=30
			tx.Add(entity2, datalog.NewKeyword(":attr/name"), "Bob")
			tx.Add(entity2, datalog.NewKeyword(":attr/age"), int64(30))

			// entity3: :attr/name="Alice", :attr/age=35
			tx.Add(entity3, datalog.NewKeyword(":attr/name"), "Alice")
			tx.Add(entity3, datalog.NewKeyword(":attr/age"), int64(35))

			_, err := tx.Commit()
			require.NoError(t, err)

			matcher := NewPatternMatcher(db.Store())

			t.Run("E_and_V_bound", func(t *testing.T) {
				// Pattern: [?e :attr/name ?name] with ?e and ?name bound
				pattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":attr/name")},
						query.Variable{Name: datalog.NewSymbol("?name")},
					},
				}

				// Bindings: entity1 with name="Alice", entity2 with name="Alice" (wrong!)
				bindingRel := executor.NewMaterializedRelation(
					[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?name")},
					[]executor.Tuple{
						{entity1, "Alice"}, // Matches
						{entity2, "Alice"}, // entity2 has name="Bob", should NOT match
					},
				)

				result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
				require.NoError(t, err)

				count := countResults(result)
				require.Equal(t, 1, count, "Only entity1 should match (has name='Alice')")
			})

			t.Run("E_and_A_bound", func(t *testing.T) {
				// Pattern: [?e ?a ?v] with ?e and ?a bound
				// This tests attribute binding (less common but possible)
				pattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Variable{Name: datalog.NewSymbol("?a")},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				}

				// Bindings: entity1 with :attr/name, entity1 with :attr/age
				bindingRel := executor.NewMaterializedRelation(
					[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?a")},
					[]executor.Tuple{
						{entity1, datalog.NewKeyword(":attr/name")},
						{entity1, datalog.NewKeyword(":attr/age")},
					},
				)

				result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
				require.NoError(t, err)

				count := countResults(result)
				require.Equal(t, 2, count, "entity1 has 2 attributes")
			})

			t.Run("A_and_V_bound", func(t *testing.T) {
				// Pattern: [?e ?a ?v] with ?a and ?v bound
				// Find all entities with :attr/name="Alice"
				pattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Variable{Name: datalog.NewSymbol("?a")},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				}

				// Bindings: :attr/name with value "Alice"
				bindingRel := executor.NewMaterializedRelation(
					[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?v")},
					[]executor.Tuple{
						{datalog.NewKeyword(":attr/name"), "Alice"},
					},
				)

				result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
				require.NoError(t, err)

				count := countResults(result)
				require.Equal(t, 2, count, "entity1 and entity3 both have name='Alice'")
			})
		})
	}
}

// TestMultiPositionAsymmetricCardinality reproduces the slow query scenario
// V has 1 distinct value, E has many distinct values
func TestMultiPositionAsymmetricCardinality(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: 100 entities
			// - 80 entities have code="common"
			// - 20 entities have code="rare"
			tx := db.NewTransaction()
			entities := make([]datalog.Identity, 100)

			for i := 0; i < 100; i++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
				entities[i] = entityID

				code := "common"
				if i < 20 {
					code = "rare"
				}
				tx.Add(entityID, datalog.NewKeyword(":entity/code"), code)
			}

			_, err := tx.Commit()
			require.NoError(t, err)

			// Pattern: [?e :entity/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":entity/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			// Bindings: All 100 entities with code="rare"
			// Only entities 0-19 actually have code="rare"
			bindingTuples := make([]executor.Tuple, 100)
			for i := 0; i < 100; i++ {
				bindingTuples[i] = executor.Tuple{entities[i], "rare"}
			}
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				bindingTuples,
			)

			matcher := NewPatternMatcher(db.Store())
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
			require.NoError(t, err)

			// Expected: 20 results (only entities with code="rare")
			count := countResults(result)
			require.Equal(t, 20, count, "Expected 20 entities with code='rare', got %d", count)
		})
	}
}

// TestMultiPositionNoMatches ensures empty results are handled correctly
func TestMultiPositionNoMatches(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: A few entities
			tx := db.NewTransaction()
			entity1 := datalog.NewIdentity("entity:1")
			tx.Add(entity1, datalog.NewKeyword(":attr/code"), "exists")
			_, err := tx.Commit()
			require.NoError(t, err)

			// Pattern: [?e :attr/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":attr/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			// Bindings with values that don't match
			nonExistentEntity := datalog.NewIdentity("entity:nonexistent")
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				[]executor.Tuple{
					{nonExistentEntity, "missing"},
					{entity1, "missing"}, // entity1 exists but has code="exists", not "missing"
				},
			)

			matcher := NewPatternMatcher(db.Store())
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
			require.NoError(t, err)

			count := countResults(result)
			require.Equal(t, 0, count, "Expected 0 results for non-matching bindings")
		})
	}
}

// TestMultiPositionSingleBinding ensures single-tuple binding works
func TestMultiPositionSingleBinding(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup
			tx := db.NewTransaction()
			entity1 := datalog.NewIdentity("entity:1")
			tx.Add(entity1, datalog.NewKeyword(":attr/code"), "A")
			_, err := tx.Commit()
			require.NoError(t, err)

			// Pattern: [?e :attr/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":attr/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			// Single binding tuple
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				[]executor.Tuple{
					{entity1, "A"},
				},
			)

			matcher := NewPatternMatcher(db.Store())
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
			require.NoError(t, err)

			count := countResults(result)
			require.Equal(t, 1, count, "Expected 1 result for single binding tuple")
		})
	}
}

// TestMultiPositionResultOrdering verifies results are same regardless of binding order
func TestMultiPositionResultOrdering(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: 5 entities all with same code
			tx := db.NewTransaction()
			entities := make([]datalog.Identity, 5)

			for i := 0; i < 5; i++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
				entities[i] = entityID
				tx.Add(entityID, datalog.NewKeyword(":attr/code"), "same")
			}

			_, err := tx.Commit()
			require.NoError(t, err)

			// Pattern: [?e :attr/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":attr/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			matcher := NewPatternMatcher(db.Store())

			// Run with bindings in order 0,1,2,3,4
			bindingTuples1 := make([]executor.Tuple, 5)
			for i := 0; i < 5; i++ {
				bindingTuples1[i] = executor.Tuple{entities[i], "same"}
			}
			bindingRel1 := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				bindingTuples1,
			)

			result1, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel1})
			require.NoError(t, err)
			results1 := collectEntityIDs(result1)

			// Run with bindings in reverse order 4,3,2,1,0
			bindingTuples2 := make([]executor.Tuple, 5)
			for i := 0; i < 5; i++ {
				bindingTuples2[i] = executor.Tuple{entities[4-i], "same"}
			}
			bindingRel2 := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				bindingTuples2,
			)

			result2, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel2})
			require.NoError(t, err)
			results2 := collectEntityIDs(result2)

			// Results should be same set (order may differ)
			sort.Strings(results1)
			sort.Strings(results2)
			require.Equal(t, results1, results2, "Results should be identical regardless of binding order")
			require.Equal(t, 5, len(results1), "All 5 entities should be in results")
		})
	}
}

// TestMultiPositionPerformance benchmarks the multi-position binding fix
// This reproduces the slow query scenario: 81 entities, 1 code value
func TestMultiPositionPerformance(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: 100 entities, simulating the real-world scenario
			// 81 entities have code="target", rest have other codes
			tx := db.NewTransaction()
			entities := make([]datalog.Identity, 100)

			for i := 0; i < 100; i++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
				entities[i] = entityID

				code := "target"
				if i >= 81 {
					code = fmt.Sprintf("other-%d", i)
				}
				tx.Add(entityID, datalog.NewKeyword(":entity/code"), code)
			}

			_, err := tx.Commit()
			require.NoError(t, err)

			// Pattern: [?e :entity/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":entity/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			// Bindings: All 81 "target" entities with code="target"
			// This is the problematic case: 81 distinct E values, 1 distinct V value
			bindingTuples := make([]executor.Tuple, 81)
			for i := 0; i < 81; i++ {
				bindingTuples[i] = executor.Tuple{entities[i], "target"}
			}
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				bindingTuples,
			)

			matcher := NewPatternMatcher(db.Store())

			// Warm up
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
			require.NoError(t, err)
			count := countResults(result)
			require.Equal(t, 81, count, "Expected 81 results")

			// Benchmark: Run multiple iterations
			const iterations = 20
			var totalDuration time.Duration

			for i := 0; i < iterations; i++ {
				// Need fresh binding relation each time since iterator is consumed
				bindingRel := executor.NewMaterializedRelation(
					[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
					bindingTuples,
				)

				start := time.Now()
				result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
				require.NoError(t, err)

				// Must consume the result to measure full execution
				count := countResults(result)
				require.Equal(t, 81, count)

				totalDuration += time.Since(start)
			}

			avgDuration := totalDuration / iterations
			t.Logf("Average query time: %v (total: %v over %d iterations)", avgDuration, totalDuration, iterations)

			// A ceiling well above the expected time, so this catches a return to
			// per-binding scanning rather than ordinary machine-to-machine variance.
			maxAllowed := 5 * time.Millisecond
			if avgDuration > maxAllowed {
				t.Errorf("Query too slow: %v average (expected <%v)", avgDuration, maxAllowed)
			}
		})
	}
}

// BenchmarkMultiPositionBinding provides Go benchmark for the multi-position case
func BenchmarkMultiPositionBinding(b *testing.B) {
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup: 100 entities
	tx := db.NewTransaction()
	entities := make([]datalog.Identity, 100)

	for i := 0; i < 100; i++ {
		entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		entities[i] = entityID
		code := "target"
		if i >= 81 {
			code = fmt.Sprintf("other-%d", i)
		}
		tx.Add(entityID, datalog.NewKeyword(":entity/code"), code)
	}

	_, err = tx.Commit()
	if err != nil {
		b.Fatal(err)
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":entity/code")},
			query.Variable{Name: datalog.NewSymbol("?code")},
		},
	}

	bindingTuples := make([]executor.Tuple, 81)
	for i := 0; i < 81; i++ {
		bindingTuples[i] = executor.Tuple{entities[i], "target"}
	}

	matcher := NewPatternMatcher(db.Store())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
			bindingTuples,
		)
		result, _ := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
		// Consume iterator
		it := result.Iterator()
		for it.Next() {
		}
		it.Close()
	}
}

// Test utilities

func countResults(result executor.Relation) int {
	count := 0
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		count++
	}
	return count
}

func collectEntityIDs(result executor.Relation) []string {
	var ids []string
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) > 0 {
			// Identity is always a pointer type; a single assertion covers
			// every case. Use L85() for consistent comparison since storage-
			// returned entities don't have original strings.
			if id, ok := tuple[0].(datalog.Identity); ok {
				ids = append(ids, id.L85())
			}
		}
	}
	return ids
}

// TestMultiPositionWithStreamingBinding verifies that streaming relations work correctly
// as binding inputs with multi-position binding. This tests the potential panic issue
// where chooseBestMultiPositionStrategy iterates the relation, then the binding-driven
// scan iterates it again — which a StreamingRelation refuses without Materialize().
func TestMultiPositionWithStreamingBinding(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Setup: 10 entities with codes
			tx := db.NewTransaction()
			entities := make([]datalog.Identity, 10)

			for i := 0; i < 10; i++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
				entities[i] = entityID

				code := "A"
				if i >= 5 {
					code = "B"
				}
				tx.Add(entityID, datalog.NewKeyword(":attr/code"), code)
			}

			_, err := tx.Commit()
			require.NoError(t, err)

			// Create pattern: [?e :attr/code ?code]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":attr/code")},
					query.Variable{Name: datalog.NewSymbol("?code")},
				},
			}

			// Create binding tuples
			bindingTuples := []executor.Tuple{
				{entities[0], "A"},
				{entities[2], "A"},
				{entities[4], "A"},
			}

			// Create a STREAMING relation instead of materialized
			// This simulates what happens when binding comes from a previous pattern match
			tupleIter := &sliceTupleIterator{tuples: bindingTuples, idx: -1}
			streamingBindingRel := executor.NewStreamingRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?code")},
				tupleIter,
			)

			// Create matcher and execute - this should NOT panic
			matcher := NewPatternMatcher(db.Store())
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{streamingBindingRel})
			require.NoError(t, err, "Match should not error with streaming binding relation")

			// Collect results
			count := countResults(result)
			require.Equal(t, 3, count, "Expected 3 results")
		})
	}
}

// sliceTupleIterator is a simple iterator over a slice of tuples for testing
type sliceTupleIterator struct {
	tuples []executor.Tuple
	idx    int
	err    error
}

func (it *sliceTupleIterator) Next() bool {
	it.idx++
	return it.idx < len(it.tuples)
}

func (it *sliceTupleIterator) Tuple() executor.Tuple {
	if it.idx < 0 || it.idx >= len(it.tuples) {
		return nil
	}
	return it.tuples[it.idx]
}

func (it *sliceTupleIterator) Close() error {
	return nil
}

func (it *sliceTupleIterator) Error() error { return it.err }

// TestMultiPositionWithBytesValue verifies that chooseBestMultiPositionStrategy
// does not panic when binding relation tuples contain []byte values.
// []byte is not comparable in Go and cannot be used as map keys.
// This reproduces the panic: "hash of unhashable type []uint8"
func TestMultiPositionWithBytesValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create entities with []byte values (TypeBytes)
			e1 := datalog.NewIdentity("entity:1")
			e2 := datalog.NewIdentity("entity:2")

			tx := db.NewTransaction()
			tx.Add(e1, datalog.NewKeyword(":item/data"), []byte{0x01, 0x02, 0x03})
			tx.Add(e2, datalog.NewKeyword(":item/data"), []byte{0x04, 0x05, 0x06})
			tx.Add(e1, datalog.NewKeyword(":item/name"), "alpha")
			tx.Add(e2, datalog.NewKeyword(":item/name"), "beta")
			_, err := tx.Commit()
			require.NoError(t, err)

			// Pattern [?e :item/data ?v] with both E and V bound in the binding relation.
			// This forces chooseBestMultiPositionStrategy which counts distinct values
			// using map[interface{}]bool — panics on []byte.
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":item/data")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			}

			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")},
				[]executor.Tuple{
					{e1, []byte{0x01, 0x02, 0x03}},
					{e2, []byte{0x04, 0x05, 0x06}},
				},
			)

			matcher := NewPatternMatcher(db.store)

			// This should not panic
			results, err := executor.CollectTuples(matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel}))
			require.NoError(t, err)
			require.Len(t, results, 2, "should match both entities")
		})
	}
}
