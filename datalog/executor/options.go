package executor

import "github.com/wbrown/janus-datalog/datalog/annotations"

// ExecutorOptions is a lightweight struct for internal use within executor.
// The main configuration comes from PlannerOptions which includes both planner
// and executor settings.
//
// Options travel with the relation: Relation.Options() is how a relation built
// during one query hands its settings — and its observer — to the operators that
// consume it. The handler rides here because a relation that emits after its
// construction has no other route to one: it is built in one call and consumed
// in another, long after any context went out of scope.
type ExecutorOptions struct {
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

	// Handler receives this relation's annotation events. Nil is annotations-off;
	// every emit site guards on it, so there is no no-op handler to install.
	// Stamped once per query from the execution context, then inherited by every
	// relation built under those options.
	Handler annotations.Handler
}

// populated reports whether these options carry anything at all — any
// configuration field set, or a handler. Relations built through a constructor
// that takes no options hold the zero value, and an operator joining such a
// relation to a configured one takes the configured side's options rather than
// pass an empty configuration (and a lost observer) down to its result.
//
// Written field by field because Handler is a func type: a struct containing one
// cannot be compared with ==, nil field or not. TestExecutorOptionsPopulated
// pins this against a field being added to the struct and not accounted for here.
func (o ExecutorOptions) populated() bool {
	return o.EnableTrueStreaming ||
		o.EnableSymmetricHashJoin ||
		o.EnableParallelSubqueries ||
		o.MaxSubqueryWorkers != 0 ||
		o.EnableStreamingJoins ||
		o.DefaultHashTableSize != 0 ||
		o.EnableStreamingAggregation ||
		o.EnableScanSharing ||
		o.EnableEntityPrefetch ||
		o.EnableAttributeFetchFusion ||
		o.Handler != nil
}
