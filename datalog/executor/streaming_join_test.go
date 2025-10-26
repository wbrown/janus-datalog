package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestStreamingHashJoin(t *testing.T) {
	// Test with streaming enabled - create relations with options
	opts := ExecutorOptions{EnableStreamingJoins: true}
	left := NewMaterializedRelationWithOptions(
		[]query.Symbol{"?user", "?name"},
		[]Tuple{
			{"u1", "Alice"},
			{"u2", "Bob"},
		},
		opts,
	)

	right := NewMaterializedRelationWithOptions(
		[]query.Symbol{"?user", "?age"},
		[]Tuple{
			{"u1", 25},
			{"u2", 30},
		},
		opts,
	)

	result := HashJoin(left, right, []query.Symbol{"?user"})

	t.Logf("Result type: %T", result)
	t.Logf("Result size: %d", result.Size())

	// Materialize to check contents
	iter := result.Iterator()
	count := 0
	for iter.Next() {
		tuple := iter.Tuple()
		t.Logf("Tuple %d: %v", count, tuple)
		count++
	}
	iter.Close()

	if count != 2 {
		t.Errorf("Expected 2 results, got %d", count)
	}
}

func TestStreamingHashJoinChain(t *testing.T) {
	// Test with streaming enabled - create relations with options
	opts := ExecutorOptions{EnableStreamingJoins: true}
	users := NewMaterializedRelationWithOptions(
		[]query.Symbol{"?user", "?name"},
		[]Tuple{
			{"u1", "Alice"},
			{"u2", "Bob"},
		},
		opts,
	)

	ages := NewMaterializedRelationWithOptions(
		[]query.Symbol{"?user", "?age"},
		[]Tuple{
			{"u1", 25},
			{"u2", 30},
		},
		opts,
	)

	depts := NewMaterializedRelationWithOptions(
		[]query.Symbol{"?user", "?dept"},
		[]Tuple{
			{"u1", "Engineering"},
			{"u2", "Sales"},
		},
		opts,
	)

	// First join
	result1 := HashJoin(users, ages, []query.Symbol{"?user"})
	t.Logf("After first join: type=%T, size=%d", result1, result1.Size())

	// Second join
	result2 := HashJoin(result1, depts, []query.Symbol{"?user"})
	t.Logf("After second join: type=%T, size=%d", result2, result2.Size())

	// Materialize final result
	iter := result2.Iterator()
	count := 0
	for iter.Next() {
		tuple := iter.Tuple()
		t.Logf("Tuple %d: %v", count, tuple)
		count++
	}
	iter.Close()

	if count != 2 {
		t.Errorf("Expected 2 results, got %d", count)
	}
}
