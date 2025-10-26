package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestStrategySelector_Batched(t *testing.T) {
	selector := NewSubqueryStrategySelector(100)

	// Query with RelationInput should use batched strategy
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.RelationInput{
				Symbols: []query.Symbol{"?sym"},
			},
		},
	}

	opts := ExecutorOptions{
		EnableParallelSubqueries: true,
	}

	strategy := selector.SelectStrategy(q, 200, opts)
	if strategy != StrategyBatched {
		t.Errorf("Expected StrategyBatched, got %v", strategy)
	}
}

func TestStrategySelector_Parallel(t *testing.T) {
	selector := NewSubqueryStrategySelector(100)

	// Query without RelationInput but with many inputs should use parallel
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.ScalarInput{Symbol: "?sym"},
		},
	}

	opts := ExecutorOptions{
		EnableParallelSubqueries: true,
	}

	strategy := selector.SelectStrategy(q, 150, opts)
	if strategy != StrategyParallel {
		t.Errorf("Expected StrategyParallel, got %v", strategy)
	}
}

func TestStrategySelector_Sequential(t *testing.T) {
	selector := NewSubqueryStrategySelector(100)

	// Query without RelationInput and few inputs should use sequential
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.ScalarInput{Symbol: "?sym"},
		},
	}

	opts := ExecutorOptions{
		EnableParallelSubqueries: true,
	}

	strategy := selector.SelectStrategy(q, 50, opts)
	if strategy != StrategySequential {
		t.Errorf("Expected StrategySequential, got %v", strategy)
	}
}

func TestStrategySelector_ParallelDisabled(t *testing.T) {
	selector := NewSubqueryStrategySelector(100)

	// Many inputs but parallel disabled should use sequential
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.ScalarInput{Symbol: "?sym"},
		},
	}

	opts := ExecutorOptions{
		EnableParallelSubqueries: false,
	}

	strategy := selector.SelectStrategy(q, 200, opts)
	if strategy != StrategySequential {
		t.Errorf("Expected StrategySequential (parallel disabled), got %v", strategy)
	}
}

func TestStrategySelector_CustomThreshold(t *testing.T) {
	selector := NewSubqueryStrategySelector(50)

	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.ScalarInput{Symbol: "?sym"},
		},
	}

	opts := ExecutorOptions{
		EnableParallelSubqueries: true,
	}

	// 60 inputs with threshold of 50 should trigger parallel
	strategy := selector.SelectStrategy(q, 60, opts)
	if strategy != StrategyParallel {
		t.Errorf("Expected StrategyParallel (60 >= 50), got %v", strategy)
	}

	// 40 inputs with threshold of 50 should use sequential
	strategy = selector.SelectStrategy(q, 40, opts)
	if strategy != StrategySequential {
		t.Errorf("Expected StrategySequential (40 < 50), got %v", strategy)
	}
}

func TestStrategySelector_DefaultThreshold(t *testing.T) {
	// Zero or negative threshold should use default of 100
	selector := NewSubqueryStrategySelector(0)
	if selector.GetParallelThreshold() != 100 {
		t.Errorf("Expected default threshold of 100, got %d", selector.GetParallelThreshold())
	}

	selector = NewSubqueryStrategySelector(-50)
	if selector.GetParallelThreshold() != 100 {
		t.Errorf("Expected default threshold of 100 for negative input, got %d", selector.GetParallelThreshold())
	}
}

func TestCanBatchSubquery_WithRelationInput(t *testing.T) {
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.RelationInput{
				Symbols: []query.Symbol{"?sym", "?hr"},
			},
		},
	}

	if !CanBatchSubquery(q) {
		t.Error("Expected CanBatchSubquery to return true for query with RelationInput")
	}
}

func TestCanBatchSubquery_WithoutDatabase(t *testing.T) {
	// RelationInput without DatabaseInput should return false
	q := &query.Query{
		In: []query.InputSpec{
			query.RelationInput{
				Symbols: []query.Symbol{"?sym"},
			},
		},
	}

	if CanBatchSubquery(q) {
		t.Error("Expected CanBatchSubquery to return false when DatabaseInput missing")
	}
}

func TestCanBatchSubquery_OnlyVariables(t *testing.T) {
	// Query with only scalar variable inputs cannot be batched
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.ScalarInput{Symbol: "?sym"},
			query.ScalarInput{Symbol: "?hr"},
		},
	}

	if CanBatchSubquery(q) {
		t.Error("Expected CanBatchSubquery to return false for scalar variable inputs")
	}
}

func TestCanBatchSubquery_EmptyIn(t *testing.T) {
	// Query with no :in clause cannot be batched
	q := &query.Query{
		In: []query.InputSpec{},
	}

	if CanBatchSubquery(q) {
		t.Error("Expected CanBatchSubquery to return false for empty :in clause")
	}
}

func TestCanBatchSubquery_NilQuery(t *testing.T) {
	// Nil query should return false safely
	if CanBatchSubquery(nil) {
		t.Error("Expected CanBatchSubquery to return false for nil query")
	}
}

func TestStrategyString(t *testing.T) {
	tests := []struct {
		strategy SubqueryExecutionStrategy
		expected string
	}{
		{StrategyBatched, "batched"},
		{StrategyParallel, "parallel"},
		{StrategySequential, "sequential"},
		{SubqueryExecutionStrategy(99), "unknown"},
	}

	for _, tt := range tests {
		result := tt.strategy.String()
		if result != tt.expected {
			t.Errorf("Strategy %d: expected string %q, got %q", tt.strategy, tt.expected, result)
		}
	}
}

func TestStrategySelector_BatchedTakesPrecedence(t *testing.T) {
	selector := NewSubqueryStrategySelector(100)

	// Query with RelationInput should use batched even if parallel conditions met
	q := &query.Query{
		In: []query.InputSpec{
			query.DatabaseInput{},
			query.RelationInput{
				Symbols: []query.Symbol{"?sym"},
			},
		},
	}

	opts := ExecutorOptions{
		EnableParallelSubqueries: true,
	}

	// Even with 1000 inputs, batched should take precedence
	strategy := selector.SelectStrategy(q, 1000, opts)
	if strategy != StrategyBatched {
		t.Errorf("Expected StrategyBatched to take precedence over parallel, got %v", strategy)
	}
}
