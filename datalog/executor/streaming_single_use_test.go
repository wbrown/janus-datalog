package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestStreamingRelationSingleUse verifies that a StreamingRelation panics on
// a second Iterator() call
func TestStreamingRelationSingleUse(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	tuples := []Tuple{
		Tuple{1},
		Tuple{2},
		Tuple{3},
	}

	// Create a materialized relation then convert to streaming
	matRel := NewMaterializedRelation(symbols, tuples)
	iter := matRel.Iterator()
	rel := NewStreamingRelation(symbols, iter)

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
	cols1 := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	tuples1 := []Tuple{
		Tuple{1, "a"},
		Tuple{2, "b"},
	}
	matRel1 := NewMaterializedRelation(cols1, tuples1)
	iter1 := matRel1.Iterator()
	rel1 := NewStreamingRelation(cols1, iter1)

	cols2 := []query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	tuples2 := []Tuple{
		Tuple{"a", 10},
		Tuple{"b", 20},
	}
	matRel2 := NewMaterializedRelation(cols2, tuples2)
	iter2 := matRel2.Iterator()
	rel2 := NewStreamingRelation(cols2, iter2)

	// Join should work without panicking (single Iterator() call per relation)
	joinSymbols := []query.Symbol{datalog.NewSymbol("?y")}
	result := HashJoin(rel1, rel2, joinSymbols)

	// Verify result
	if tuples := collectTuples(result); len(tuples) != 2 {
		t.Fatalf("Expected 2 joined tuples, got %d", len(tuples))
	}
}
