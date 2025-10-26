package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// SubqueryExecutionStrategy represents different ways to execute subqueries
type SubqueryExecutionStrategy int

const (
	// StrategyBatched executes subquery once with all inputs as RelationInput
	// Used when subquery has :in $ [[?sym ?hr] ...] format
	StrategyBatched SubqueryExecutionStrategy = iota

	// StrategyParallel executes subquery iterations in parallel using worker pool
	// Used when input count >= threshold and parallel execution is enabled
	StrategyParallel

	// StrategySequential executes subquery iterations sequentially in a loop
	// Used when input count < threshold or parallel execution is disabled
	StrategySequential
)

// String returns human-readable strategy name
func (s SubqueryExecutionStrategy) String() string {
	switch s {
	case StrategyBatched:
		return "batched"
	case StrategyParallel:
		return "parallel"
	case StrategySequential:
		return "sequential"
	default:
		return "unknown"
	}
}

// SubqueryStrategySelector chooses optimal execution strategy based on query structure and configuration
type SubqueryStrategySelector struct {
	parallelThreshold int
}

// NewSubqueryStrategySelector creates a new strategy selector
// parallelThreshold: minimum number of inputs to trigger parallel execution (0 = use default of 100)
func NewSubqueryStrategySelector(parallelThreshold int) *SubqueryStrategySelector {
	if parallelThreshold <= 0 {
		parallelThreshold = 100 // Default threshold
	}
	return &SubqueryStrategySelector{
		parallelThreshold: parallelThreshold,
	}
}

// SelectStrategy chooses the optimal execution strategy for a subquery
// Returns:
// - StrategyBatched if subquery accepts RelationInput
// - StrategyParallel if parallel enabled and inputCount >= threshold
// - StrategySequential otherwise
func (s *SubqueryStrategySelector) SelectStrategy(
	subq *query.Query,
	inputCount int,
	opts ExecutorOptions,
) SubqueryExecutionStrategy {
	// Check if subquery can be batched (has RelationInput in :in clause)
	if CanBatchSubquery(subq) {
		return StrategyBatched
	}

	// Check if parallel execution is enabled and threshold is met
	if opts.EnableParallelSubqueries && inputCount >= s.parallelThreshold {
		return StrategyParallel
	}

	return StrategySequential
}

// GetParallelThreshold returns the configured parallel threshold
func (s *SubqueryStrategySelector) GetParallelThreshold() int {
	return s.parallelThreshold
}

// CanBatchSubquery checks if a query accepts RelationInput (:in $ [[?sym] ...])
// This requires:
// 1. Query has :in clause
// 2. :in clause has DatabaseInput ($) before RelationInput
// 3. :in clause has RelationInput ([[...]])
func CanBatchSubquery(q *query.Query) bool {
	if q == nil || len(q.In) == 0 {
		return false
	}

	hasDatabase := false
	for _, input := range q.In {
		switch input.(type) {
		case query.DatabaseInput:
			hasDatabase = true
		case query.RelationInput:
			// Found RelationInput - can batch only if database came first
			return hasDatabase
		}
	}
	return false
}
