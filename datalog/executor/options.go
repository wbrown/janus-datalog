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
