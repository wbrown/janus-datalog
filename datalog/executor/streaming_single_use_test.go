package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestStreamingRelationSingleUse verifies that StreamingRelation with EnableTrueStreaming
// panics on second Iterator() call as expected
func TestStreamingRelationSingleUse(t *testing.T) {
	cols := []query.Symbol{query.Symbol("?x")}
	tuples := []Tuple{
		Tuple{1},
		Tuple{2},
		Tuple{3},
	}

	// Create a materialized relation then convert to streaming
	matRel := NewMaterializedRelation(cols, tuples)
	iter := matRel.Iterator()
	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(cols, iter, opts)

	// First iteration should work
	it1 := rel.Iterator()
	count := 0
	for it1.Next() {
		count++
	}
	it1.Close()

	if count != 3 {
		t.Fatalf("Expected 3 tuples, got %d", count)
	}

	// Second iteration should panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic on second Iterator() call, but didn't panic")
		}
	}()

	_ = rel.Iterator() // Should panic
}

// TestStreamingRelationSingleUseInJoin tests that joins don't call Iterator() twice
func TestStreamingRelationSingleUseInJoin(t *testing.T) {
	// Create two simple streaming relations
	cols1 := []query.Symbol{query.Symbol("?x"), query.Symbol("?y")}
	tuples1 := []Tuple{
		Tuple{1, "a"},
		Tuple{2, "b"},
	}
	matRel1 := NewMaterializedRelation(cols1, tuples1)
	iter1 := matRel1.Iterator()
	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel1 := NewStreamingRelationWithOptions(cols1, iter1, opts)

	cols2 := []query.Symbol{query.Symbol("?y"), query.Symbol("?z")}
	tuples2 := []Tuple{
		Tuple{"a", 10},
		Tuple{"b", 20},
	}
	matRel2 := NewMaterializedRelation(cols2, tuples2)
	iter2 := matRel2.Iterator()
	rel2 := NewStreamingRelationWithOptions(cols2, iter2, opts)

	// Join should work without panicking (single Iterator() call per relation)
	joinCols := []query.Symbol{query.Symbol("?y")}
	result := HashJoin(rel1, rel2, joinCols)

	// Verify result
	if result.Size() != 2 {
		t.Fatalf("Expected 2 joined tuples, got %d", result.Size())
	}
}
