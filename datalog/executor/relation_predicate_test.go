package executor

import (
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestFilterWithPredicate(t *testing.T) {
	// Create a test relation
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	tuples := []Tuple{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
		{10, 11, 12},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Test with a comparison predicate
	pred := &query.Comparison{
		Op:    query.OpGT,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
		Right: query.ConstantTerm{Value: int64(5)},
	}

	filtered := rel.FilterWithPredicate(pred)

	// Should have 2 tuples (7,8,9) and (10,11,12)
	if filtered.Size() != 2 {
		t.Errorf("Expected 2 tuples after filter, got %d", filtered.Size())
	}

	// Check the actual values
	tuple1 := filtered.Get(0)
	if tuple1[0] != 7 {
		t.Errorf("Expected first tuple to have x=7, got %v", tuple1[0])
	}

	tuple2 := filtered.Get(1)
	if tuple2[0] != 10 {
		t.Errorf("Expected second tuple to have x=10, got %v", tuple2[0])
	}
}

func TestEvaluateFunction(t *testing.T) {
	// Create a test relation
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	tuples := []Tuple{
		{int64(10), int64(20)},
		{int64(5), int64(15)},
		{int64(3), int64(7)},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Test with an arithmetic function
	fn := &query.ArithmeticFunction{
		Op:    query.OpAdd,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
		Right: query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
	}

	result := rel.EvaluateFunction(fn, datalog.NewSymbol("?sum"))

	// Should have 3 symbols now
	if len(result.Symbols()) != 3 {
		t.Errorf("Expected 3 symbols after evaluation, got %d", len(result.Symbols()))
	}

	// Check the computed values
	tuple1 := result.Get(0)
	if tuple1[2] != int64(30) { // 10 + 20
		t.Errorf("Expected sum=30, got %v", tuple1[2])
	}

	tuple2 := result.Get(1)
	if tuple2[2] != int64(20) { // 5 + 15
		t.Errorf("Expected sum=20, got %v", tuple2[2])
	}

	tuple3 := result.Get(2)
	if tuple3[2] != int64(10) { // 3 + 7
		t.Errorf("Expected sum=10, got %v", tuple3[2])
	}
}

func TestChainedComparison(t *testing.T) {
	// Test the variadic comparison with FilterWithPredicate
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	tuples := []Tuple{
		{1, 5, 10},
		{2, 3, 4}, // This one satisfies 2 < 3 < 4 < 5
		{6, 7, 8},
		{3, 4, 2}, // Not in order
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Test: [(< ?x ?y ?z 5)]
	pred := &query.ChainedComparison{
		Op: query.OpLT,
		Terms: []query.Term{
			query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
			query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
			query.VariableTerm{Symbol: datalog.NewSymbol("?z")},
			query.ConstantTerm{Value: int64(5)},
		},
	}

	filtered := rel.FilterWithPredicate(pred)

	// Should have 1 tuple (2,3,4)
	if filtered.Size() != 1 {
		t.Errorf("Expected 1 tuple after chained comparison, got %d", filtered.Size())
	}

	tuple := filtered.Get(0)
	if tuple[0] != 2 || tuple[1] != 3 || tuple[2] != 4 {
		t.Errorf("Expected tuple (2,3,4), got %v", tuple)
	}
}
