package executor

import (
	"fmt"
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestJoinCorrectness validates that asymmetric and symmetric joins produce identical, correct results
func TestJoinCorrectness(t *testing.T) {
	sizes := []int{100, 1000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			leftCols := []query.Symbol{"?x", "?name"}
			rightCols := []query.Symbol{"?x", "?value"}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)

			// Create test data with known join results
			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
				rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
			}

			// Get results from asymmetric
			asymmetricResults := getJoinResults(t, leftTuples, rightTuples, leftCols, rightCols, false)

			// Get results from symmetric
			symmetricResults := getJoinResults(t, leftTuples, rightTuples, leftCols, rightCols, true)

			// 1. Check count matches
			if len(asymmetricResults) != size {
				t.Errorf("Asymmetric: expected %d results, got %d", size, len(asymmetricResults))
			}
			if len(symmetricResults) != size {
				t.Errorf("Symmetric: expected %d results, got %d", size, len(symmetricResults))
			}

			// 2. Check no duplicates
			checkNoDuplicates(t, "Asymmetric", asymmetricResults)
			checkNoDuplicates(t, "Symmetric", symmetricResults)

			// 3. Check join correctness (join key matches)
			checkJoinCorrectness(t, "Asymmetric", asymmetricResults)
			checkJoinCorrectness(t, "Symmetric", symmetricResults)

			// 4. Check both produce same results (order-independent)
			checkResultsEqual(t, asymmetricResults, symmetricResults)
		})
	}
}

// TestJoinCorrectnessWithMismatches tests partial joins where not all tuples match
func TestJoinCorrectnessWithMismatches(t *testing.T) {
	leftCols := []query.Symbol{"?x", "?name"}
	rightCols := []query.Symbol{"?x", "?value"}

	// Left has IDs 0-99, Right has IDs 50-149
	// Expected matches: 50-99 (50 results)
	leftTuples := make([]Tuple, 100)
	rightTuples := make([]Tuple, 100)

	for i := 0; i < 100; i++ {
		leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
		rightTuples[i] = Tuple{int64(i + 50), fmt.Sprintf("value%d", i+50)}
	}

	asymmetricResults := getJoinResults(t, leftTuples, rightTuples, leftCols, rightCols, false)
	symmetricResults := getJoinResults(t, leftTuples, rightTuples, leftCols, rightCols, true)

	expectedCount := 50
	if len(asymmetricResults) != expectedCount {
		t.Errorf("Asymmetric: expected %d results, got %d", expectedCount, len(asymmetricResults))
	}
	if len(symmetricResults) != expectedCount {
		t.Errorf("Symmetric: expected %d results, got %d", expectedCount, len(symmetricResults))
	}

	// Verify only IDs 50-99 present
	for _, result := range asymmetricResults {
		id := result[0].(int64)
		if id < 50 || id >= 100 {
			t.Errorf("Asymmetric: unexpected ID %d (expected 50-99)", id)
		}
	}

	for _, result := range symmetricResults {
		id := result[0].(int64)
		if id < 50 || id >= 100 {
			t.Errorf("Symmetric: unexpected ID %d (expected 50-99)", id)
		}
	}

	checkResultsEqual(t, asymmetricResults, symmetricResults)
}

// TestJoinCorrectnessEarlyTermination validates correctness with LIMIT
func TestJoinCorrectnessEarlyTermination(t *testing.T) {
	size := 10000
	limit := 100

	leftCols := []query.Symbol{"?x", "?name"}
	rightCols := []query.Symbol{"?x", "?value"}

	leftTuples := make([]Tuple, size)
	rightTuples := make([]Tuple, size)

	for i := 0; i < size; i++ {
		leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
		rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
	}

	// Get limited results
	asymmetricResults := getJoinResultsWithLimit(t, leftTuples, rightTuples, leftCols, rightCols, false, limit)
	symmetricResults := getJoinResultsWithLimit(t, leftTuples, rightTuples, leftCols, rightCols, true, limit)

	// Both should return exactly 'limit' results
	if len(asymmetricResults) != limit {
		t.Errorf("Asymmetric with LIMIT: expected %d results, got %d", limit, len(asymmetricResults))
	}
	if len(symmetricResults) != limit {
		t.Errorf("Symmetric with LIMIT: expected %d results, got %d", limit, len(symmetricResults))
	}

	// Check no duplicates
	checkNoDuplicates(t, "Asymmetric (LIMIT)", asymmetricResults)
	checkNoDuplicates(t, "Symmetric (LIMIT)", symmetricResults)

	// Check join correctness
	checkJoinCorrectness(t, "Asymmetric (LIMIT)", asymmetricResults)
	checkJoinCorrectness(t, "Symmetric (LIMIT)", symmetricResults)
}

// Helper: Get all join results
func getJoinResults(t *testing.T, leftTuples, rightTuples []Tuple, leftCols, rightCols []query.Symbol, symmetric bool) []Tuple {
	left := &StreamingRelation{
		columns:  leftCols,
		iterator: &sliceIterator{tuples: leftTuples, pos: -1},
		size:     -1,
		options: ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: symmetric,
			DefaultHashTableSize:    256,
		},
	}
	right := &StreamingRelation{
		columns:  rightCols,
		iterator: &sliceIterator{tuples: rightTuples, pos: -1},
		size:     -1,
		options: ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: symmetric,
			DefaultHashTableSize:    256,
		},
	}

	result := left.Join(right)
	it := result.Iterator()
	defer it.Close()

	var results []Tuple
	for it.Next() {
		tuple := it.Tuple()
		// CRITICAL: Copy the tuple to avoid reuse bugs
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
	}

	return results
}

// Helper: Get limited join results
func getJoinResultsWithLimit(t *testing.T, leftTuples, rightTuples []Tuple, leftCols, rightCols []query.Symbol, symmetric bool, limit int) []Tuple {
	left := &StreamingRelation{
		columns:  leftCols,
		iterator: &sliceIterator{tuples: leftTuples, pos: -1},
		size:     -1,
		options: ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: symmetric,
			DefaultHashTableSize:    256,
		},
	}
	right := &StreamingRelation{
		columns:  rightCols,
		iterator: &sliceIterator{tuples: rightTuples, pos: -1},
		size:     -1,
		options: ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: symmetric,
			DefaultHashTableSize:    256,
		},
	}

	result := left.Join(right)
	it := result.Iterator()
	defer it.Close()

	var results []Tuple
	count := 0
	for count < limit && it.Next() {
		tuple := it.Tuple()
		// CRITICAL: Copy the tuple to avoid reuse bugs
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		results = append(results, tupleCopy)
		count++
	}

	return results
}

// Check for duplicate tuples
func checkNoDuplicates(t *testing.T, name string, results []Tuple) {
	seen := make(map[string]bool)
	for i, tuple := range results {
		key := tupleToString(tuple)
		if seen[key] {
			t.Errorf("%s: duplicate tuple at index %d: %v", name, i, tuple)
		}
		seen[key] = true
	}
}

// Check join correctness: ID in position 0 must match ID in position 2
func checkJoinCorrectness(t *testing.T, name string, results []Tuple) {
	for i, tuple := range results {
		if len(tuple) != 3 {
			t.Errorf("%s: tuple %d has wrong length %d (expected 3): %v", name, i, len(tuple), tuple)
			continue
		}

		// Expected format: [?x, ?name, ?value]
		// Original left: [?x, ?name]
		// Original right: [?x, ?value]
		// Join should have: [?x from left, ?name, ?value]

		leftID, ok1 := tuple[0].(int64)
		if !ok1 {
			t.Errorf("%s: tuple %d has non-int64 ID: %v", name, i, tuple[0])
			continue
		}

		// Verify name matches expected pattern
		expectedName := fmt.Sprintf("name%d", leftID)
		if name, ok := tuple[1].(string); !ok || name != expectedName {
			t.Errorf("%s: tuple %d has wrong name: got %v, expected %s", name, i, tuple[1], expectedName)
		}

		// Verify value matches expected pattern
		expectedValue := fmt.Sprintf("value%d", leftID)
		if value, ok := tuple[2].(string); !ok || value != expectedValue {
			t.Errorf("%s: tuple %d has wrong value: got %v, expected %s", name, i, tuple[2], expectedValue)
		}
	}
}

// Check that two result sets are equal (order-independent)
func checkResultsEqual(t *testing.T, results1, results2 []Tuple) {
	if len(results1) != len(results2) {
		t.Errorf("Result count mismatch: %d vs %d", len(results1), len(results2))
		return
	}

	// Convert to sorted string representations
	strs1 := make([]string, len(results1))
	strs2 := make([]string, len(results2))

	for i := range results1 {
		strs1[i] = tupleToString(results1[i])
		strs2[i] = tupleToString(results2[i])
	}

	sort.Strings(strs1)
	sort.Strings(strs2)

	for i := range strs1 {
		if strs1[i] != strs2[i] {
			t.Errorf("Result mismatch at position %d:\n  Got:      %s\n  Expected: %s",
				i, strs1[i], strs2[i])
		}
	}
}

// Helper to convert tuple to string for comparison
func tupleToString(tuple Tuple) string {
	return fmt.Sprintf("%v", tuple)
}
