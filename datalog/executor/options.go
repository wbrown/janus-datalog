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

	// Rule evaluation bounds. MaxRuleIterations caps the fixpoint iterations
	// of one stratum, MaxDerivedTuples the tuples one rule set derives; zero
	// is unbounded.
	MaxRuleIterations int
	MaxDerivedTuples  int

	// The window a form's branches run over
	// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2): every subject,
	// sized or streaming, is read into windows of one tuple, then
	// ScopeWindowGrowth times the last, up to ScopeWindowCap tuples. Zero
	// takes the default windowCap and windowGrowth read.
	ScopeWindowCap    int
	ScopeWindowGrowth int

	// Handler receives this relation's annotation events. Nil is annotations-off;
	// every emit site guards on it, so there is no no-op handler to install.
	// Stamped once per query from the execution context, then inherited by every
	// relation built under those options.
	Handler annotations.Handler
}

// SmallBindingBoundary is the size at or below which a binding set is small
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.6). The matcher seeks
// a small set's distinct keys one by one in one scan of the pattern's extent,
// and joins a large set against one scan of the extent, by a hash join or a
// merge join (chooseJoinStrategy in datalog/storage/hash_join_matcher.go).
// Declared here rather than beside the matcher because the window's cap
// defaults to it and storage imports this package.
const SmallBindingBoundary = 1000

// The window's defaults: the cap is SmallBindingBoundary, so a full window's
// keys are sought one by one, and the growth doubles each window.
const (
	defaultScopeWindowCap    = SmallBindingBoundary
	defaultScopeWindowGrowth = 2
)

// windowCap is the cap a form's windows grow to.
func (o ExecutorOptions) windowCap() int {
	if o.ScopeWindowCap > 0 {
		return o.ScopeWindowCap
	}
	return defaultScopeWindowCap
}

// windowGrowth is the factor each window grows by over the last.
func (o ExecutorOptions) windowGrowth() int {
	if o.ScopeWindowGrowth > 1 {
		return o.ScopeWindowGrowth
	}
	return defaultScopeWindowGrowth
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
		o.EnableStreamingJoins ||
		o.DefaultHashTableSize != 0 ||
		o.EnableStreamingAggregation ||
		o.EnableScanSharing ||
		o.EnableEntityPrefetch ||
		o.EnableAttributeFetchFusion ||
		o.MaxRuleIterations != 0 ||
		o.MaxDerivedTuples != 0 ||
		o.ScopeWindowCap != 0 ||
		o.ScopeWindowGrowth != 0 ||
		o.Handler != nil
}
