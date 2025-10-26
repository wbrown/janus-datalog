package executor

// ExecutorOptions is a lightweight struct for internal use within executor
// The main configuration comes from PlannerOptions which includes both planner and executor settings
type ExecutorOptions struct {
	// Execution engine selection
	UseQueryExecutor bool // If true, use new QueryExecutor (Stage B); if false, use legacy executor

	// Streaming options - control memory vs performance tradeoffs
	EnableIteratorComposition bool
	EnableTrueStreaming       bool
	EnableSymmetricHashJoin   bool

	// Parallel execution options
	EnableParallelSubqueries bool
	MaxSubqueryWorkers       int

	// Subquery optimization options
	EnableSubqueryDecorrelation bool // If true, batch identical subqueries for efficiency
	UseStreamingSubqueryUnion   bool // If true, use streaming union for subquery results (default: true)
	UseComponentizedSubquery    bool // If true, use component-based subquery execution (strategy selector, batcher, worker pool)

	// Join options
	EnableStreamingJoins bool
	EnableDebugLogging   bool
	DefaultHashTableSize int // Default hash table size for streaming relations (Size() = -1). If 0, uses 256.

	// Aggregation options
	EnableStreamingAggregation      bool
	EnableStreamingAggregationDebug bool
}
