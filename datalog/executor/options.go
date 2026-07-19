package executor

import "github.com/wbrown/janus-datalog/datalog/annotations"

// ExecutorOptions is a lightweight struct for internal use within executor
// The main configuration comes from PlannerOptions which includes both planner and executor settings
type ExecutorOptions struct {
	// Annotation collector for tracking execution metrics
	// If nil, no annotations are emitted (zero overhead)
	Collector *annotations.Collector

	// Streaming options - control memory vs performance tradeoffs
	EnableTrueStreaming     bool
	EnableSymmetricHashJoin bool

	// Parallel execution options
	EnableParallelSubqueries bool
	MaxSubqueryWorkers       int

	// Join options
	EnableStreamingJoins bool
	DefaultHashTableSize int // Default hash table size for streaming relations (Size() = -1). If 0, uses 256.

	// Storage join strategy: IndexNestedLoop threshold
	// For bindingSize <= threshold: use IndexNestedLoop (iterator reuse with seeks)
	// For bindingSize > threshold: continue to HashJoinScan/MergeJoin selection
	// Default: 0 (benchmarks show HashJoinScan is faster even for size 1 due to Sorted() overhead)
	// Set high (e.g. 999999) to force IndexNestedLoop for testing
	IndexNestedLoopThreshold int

	// Aggregation options
	EnableStreamingAggregation bool

	// Scan sharing: deduplicate unbound scans across subqueries via LazySeq
	EnableScanSharing bool

	// Entity prefetch: warm EA cache after first DataPattern
	EnableEntityPrefetch bool

	// Attribute-fetch fusion: execute a same-entity [?e :const-attr ?fresh]
	// pattern as a per-tuple LookupAttribute binding instead of a
	// separate relation match + hash join. CardinalityOne, latest/as-of only.
	EnableAttributeFetchFusion bool
}
