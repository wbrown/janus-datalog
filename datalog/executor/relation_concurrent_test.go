package executor

import (
	"fmt"
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestConcurrentIteratorAccess verifies that multiple goroutines can safely
// call Iterator() on the same Relation and iterate independently
func TestConcurrentIteratorAccess(t *testing.T) {
	columns := []query.Symbol{"?x", "?y"}
	tuples := []Tuple{
		{1, "a"},
		{2, "b"},
		{3, "c"},
		{4, "d"},
		{5, "e"},
	}

	testCases := []struct {
		name     string
		relation Relation
	}{
		{
			name:     "MaterializedRelation",
			relation: NewMaterializedRelation(columns, tuples),
		},
		{
			name: "StreamingRelation (materialized)",
			relation: NewStreamingRelation(columns, &sliceIterator{
				tuples: append([]Tuple{}, tuples...), // Copy to avoid sharing
				pos:    -1,
			}).Materialize(), // CRITICAL: Must materialize before concurrent access
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numGoroutines := 10
			var wg sync.WaitGroup
			errors := make(chan error, numGoroutines)

			// Spawn multiple goroutines that iterate the same relation
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					// Each goroutine gets its own iterator
					iter := tc.relation.Iterator()
					defer iter.Close()

					// Collect all tuples
					var collected []Tuple
					for iter.Next() {
						tuple := iter.Tuple()
						// Make a copy to verify tuple isn't corrupted by concurrent access
						tupleCopy := make(Tuple, len(tuple))
						copy(tupleCopy, tuple)
						collected = append(collected, tupleCopy)
					}

					// Verify we got all tuples
					if len(collected) != len(tuples) {
						errors <- &testError{
							goroutine: id,
							message:   "wrong number of tuples",
							expected:  len(tuples),
							got:       len(collected),
						}
						return
					}

					// Verify tuples match (order might differ for streaming)
					counts := make(map[string]int)
					for _, tuple := range collected {
						key := tupleKey(tuple)
						counts[key]++
					}

					for _, tuple := range tuples {
						key := tupleKey(tuple)
						if counts[key] != 1 {
							errors <- &testError{
								goroutine: id,
								message:   "tuple missing or duplicated",
								expected:  tuple,
								got:       counts[key],
							}
							return
						}
					}
				}(i)
			}

			// Wait for all goroutines to complete
			wg.Wait()
			close(errors)

			// Check for errors
			for err := range errors {
				t.Error(err)
			}
		})
	}
}

// TestConcurrentStreamingMaterialization verifies that concurrent calls to
// Iterator() on a StreamingRelation all see the same materialized data
func TestConcurrentStreamingMaterialization(t *testing.T) {
	columns := []query.Symbol{"?n"}
	tuples := []Tuple{{1}, {2}, {3}, {4}, {5}}

	// Create streaming relation and materialize it for concurrent access
	sr := NewStreamingRelation(columns, &sliceIterator{
		tuples: append([]Tuple{}, tuples...),
		pos:    -1,
	}).Materialize() // CRITICAL: Must materialize before concurrent Iterator() calls

	numGoroutines := 20
	var wg sync.WaitGroup
	results := make([][]Tuple, numGoroutines)

	// Spawn many goroutines simultaneously to trigger race conditions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			iter := sr.Iterator()
			defer iter.Close()

			var collected []Tuple
			for iter.Next() {
				tuple := iter.Tuple()
				tupleCopy := make(Tuple, len(tuple))
				copy(tupleCopy, tuple)
				collected = append(collected, tupleCopy)
			}

			results[id] = collected
		}(i)
	}

	wg.Wait()

	// All goroutines should see the same tuples
	for i, result := range results {
		if len(result) != len(tuples) {
			t.Errorf("goroutine %d: expected %d tuples, got %d", i, len(tuples), len(result))
		}
	}
}

// Helper types and functions

type testError struct {
	goroutine int
	message   string
	expected  interface{}
	got       interface{}
}

func (e *testError) Error() string {
	return fmt.Sprintf("goroutine %d: %s (expected %v, got %v)",
		e.goroutine, e.message, e.expected, e.got)
}

func tupleKey(t Tuple) string {
	return fmt.Sprintf("%v", t)
}
