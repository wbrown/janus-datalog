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

	// Storage join strategy: IndexNestedLoop threshold
	// For bindingSize <= threshold: use IndexNestedLoop (iterator reuse with seeks)
	// For bindingSize > threshold: continue to HashJoinScan/MergeJoin selection
	// Default: 0 (benchmarks show HashJoinScan is faster even for size 1 due to Sorted() overhead)
	// Set high (e.g. 999999) to force IndexNestedLoop for testing
	IndexNestedLoopThreshold int

	// Aggregation options
	EnableStreamingAggregation      bool
	EnableStreamingAggregationDebug bool
}
