// Package annotations provides a clean, low-overhead annotation system for
// tracking query execution metrics and debugging information.
package annotations

import (
	"sync"
	"time"
)

// Event names. Every event the engine emits is named here, and every producer
// emits through one of these constants rather than writing the string.
//
// The two halves have to stay together or neither sweep can see the whole set.
// Nine formatter arms for events with no producer survived a sweep that deleted
// seven others, because the seven were written `case "pattern/match":` and the
// nine `case PatternFiltering:` — a search keyed on names could not see the
// second kind. Simultaneously, five live storage events were emitted as literals
// and three had no constant at all, so a search keyed on constants could not see
// them either. One vocabulary, declared in one place, is what makes "does
// anything emit this?" a question with an answer.
const (
	// Query lifecycle
	QueryInvoked                      = "query/invoked"
	QueryPlanCreated                  = "query/plan.created"
	QueryComplete                     = "query/completed"
	QueryRewriteConditionalAggregates = "query/rewrite.conditional-aggregates"

	// Phase execution
	PhaseBegin    = "phase/begin"
	PhaseComplete = "phase/complete"

	// Pattern matching
	MatchesToRelations = "matches->relations"

	// Detailed pattern matching timing
	PatternIndexSelection = "pattern/index-selection"
	PatternStorageScan    = "pattern/storage-scan"

	// Cache-resolved patterns. Carries no index: the cache picks one by
	// cardinality inside resolution, and a hit reads no index at all.
	PatternCacheResolveComplete = "pattern/cache-resolve-complete"

	// Binding-driven scan completion, one per strategy chooseJoinStrategy picks
	PatternHashJoinComplete       = "pattern/hash-join-complete"
	PatternMergeJoinComplete      = "pattern/merge-join-complete"
	PatternPerBindingScanComplete = "pattern/per-binding-scan-complete"

	// Storage strategy selection
	StorageReuseStrategy = "storage/reuse-strategy"
	StorageJoinStrategy  = "storage/join-strategy"
	StorageNoReusePath   = "storage/no-reuse-path"

	// V-bound candidate validation
	VValidationEntry         = "v-validation/entry"
	VValidationCandidate     = "v-validation/candidate"
	VValidationCacheResolved = "v-validation/cache-resolved"
	VValidationResult        = "v-validation/result"
	VValidationNoWinner      = "v-validation/no-winner"
	VValidationOpenScan      = "v-validation/open-scan"
	VValidationScanOpened    = "v-validation/scan-opened"

	// Join operations
	JoinHash      = "join/hash"
	JoinBuildCopy = "join/build.copy" // Tuple copy statistics during hash join build phase
	JoinStrategy  = "join/strategy.selected"
	JoinBuild     = "join/build.complete"
	JoinProbe     = "join/probe.complete"

	// OR clause operations
	OrClauseBegin          = "or/begin"
	OrClauseComplete       = "or/complete"
	OrClauseBranchComplete = "or/branch.complete"
	OrClauseUnion          = "or/union"
	OrPropertiesDerived    = "or/properties.derived"
	OrOuterReplaced        = "or/outer-replaced"

	// OR fallback evaluation
	OrFallbackOuterMaterialized = "or-fallback/outer.materialized"
	OrFallbackIteratorCreated   = "or-fallback/iterator.created"
	OrFallbackOuterExhausted    = "or-fallback/outer.exhausted"
	OrFallbackOuterTuple        = "or-fallback/outer.tuple"
	OrFallbackBranchNarrowed    = "or-fallback/branch.narrowed"
	OrFallbackBranchSuccess     = "or-fallback/branch.success"
	OrFallbackCacheBuild        = "or-fallback/cache-build"

	// Subquery execution
	SubqueryExecutorPath      = "subquery/executor-path"
	SubqueryInputRelation     = "subquery/input-relation"
	SubqueryInputCombinations = "subquery/input-combinations"

	// Algebra bridge and rewrites
	AlgebraBridgeBegin               = "algebra/bridge-begin"
	AlgebraBridgeComplete            = "algebra/bridge-complete"
	AlgebraCompiled                  = "algebra/compiled"
	AlgebraCompileError              = "algebra/compile-error"
	AlgebraOptimized                 = "algebra/optimized"
	AlgebraOptimizeError             = "algebra/optimize-error"
	AlgebraDecompileError            = "algebra/decompile-error"
	AlgebraDecorrelateInnerOptimized = "algebra/decorrelate-inner-optimized"

	// Aggregation operations
	AggregationExecuted     = "aggregation/executed"
	AggregationStrategy     = "aggregation/strategy.selected"
	AggregationMaterialized = "aggregation/materialized"

	// Relation lifecycle
	RelationCacheEnabled = "relation/cache.enabled"
	CollapseSuccess      = "collapse/success"

	// Result ordering
	SortConstantKeysDropped = "sort/constant-keys-dropped"

	// Unique-attribute ownership lookup
	UniqueLookupComplete = "unique/lookup-complete"

	// Prefetch
	PrefetchTrigger = "prefetch/trigger"

	// EA cache resolution
	CacheRebuild      = "cache/rebuild"
	CacheCheck        = "cache/check"
	CacheMatchHandled = "cache/match-handled"

	// Pull API operations
	PullBegin           = "pull/begin"
	PullComplete        = "pull/complete"
	PullBatchBegin      = "pull/batch.begin"
	PullBatchComplete   = "pull/batch.complete"
	PullEntityBegin     = "pull/entity.begin"
	PullEntityComplete  = "pull/entity.complete"
	PullAttributeLookup = "pull/attr.lookup"
	PullAllAttributes   = "pull/attr.wildcard"
	PullManyValues      = "pull/attr.many"
	PullNestedBegin     = "pull/nested.begin"
	PullNestedComplete  = "pull/nested.complete"
	PullCycleDetected   = "pull/cycle.detected"

	// Reflection operations
	ReflectReadBegin      = "reflect/read.begin"
	ReflectReadComplete   = "reflect/read.complete"
	ReflectWriteBegin     = "reflect/write.begin"
	ReflectWriteComplete  = "reflect/write.complete"
	ReflectUpdateBegin    = "reflect/update.begin"
	ReflectUpdateComplete = "reflect/update.complete"
)

// Event represents a single annotation event during query execution.
type Event struct {
	Name    string                 // Event name using hierarchical constants above
	Start   time.Time              // Start timestamp
	End     time.Time              // End timestamp
	Latency time.Duration          // Duration (End - Start)
	Data    map[string]interface{} // Additional event-specific data with grouped metrics
	Caller  string                 // Optional: file:line where event occurred
}

// Handler processes annotation events as they occur.
type Handler func(event Event)

// Synchronized wraps a Handler so its invocations are serialized through a
// mutex, making it safe to call from multiple goroutines. The query engine
// emits annotations from parallel workers (and from several collectors plus the
// storage matcher, all sharing one handler), so a handler installed on a
// Database is wrapped with this — handler authors don't need their own locking.
// Returns nil for a nil handler.
func Synchronized(h Handler) Handler {
	if h == nil {
		return nil
	}
	var mu sync.Mutex
	return func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		h(e)
	}
}

// Collector accumulates events during query execution.
type Collector struct {
	enabled bool
	handler Handler
	events  []Event

	// Pre-allocated buffers to minimize allocations
	dataPool []map[string]interface{}
	poolIdx  int
	mu       sync.Mutex // Protects dataPool and poolIdx for concurrent access
}

// NewCollector creates a new annotation collector.
func NewCollector(handler Handler) *Collector {
	const poolSize = 32
	c := &Collector{
		enabled:  handler != nil,
		handler:  handler,
		events:   make([]Event, 0, 128), // Pre-size for typical query
		dataPool: make([]map[string]interface{}, poolSize),
	}

	// Pre-allocate data maps
	for i := range c.dataPool {
		c.dataPool[i] = make(map[string]interface{}, 8)
	}

	return c
}

// Handler returns the underlying event handler.
// This is used by the decorator pattern to wrap matchers.
func (c *Collector) Handler() Handler {
	return c.handler
}

// Add records a new event.
// Thread-safe for concurrent access.
func (c *Collector) Add(event Event) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()

	// Call handler outside the lock to avoid deadlocks
	if c.handler != nil {
		c.handler(event)
	}
}

// AddTiming records an event with timing information.
func (c *Collector) AddTiming(name string, start time.Time, data map[string]interface{}) {
	if !c.enabled {
		return
	}

	end := time.Now()
	event := Event{
		Name:    name,
		Start:   start,
		End:     end,
		Latency: end.Sub(start),
		Data:    data,
	}

	c.Add(event)
}

// GetDataMap returns a pooled map for event data.
// This reduces allocations in hot paths.
// Thread-safe for concurrent access.
func (c *Collector) GetDataMap() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.poolIdx >= len(c.dataPool) {
		// Fallback to allocation if pool exhausted
		return make(map[string]interface{}, 4)
	}

	m := c.dataPool[c.poolIdx]
	c.poolIdx++

	// Clear the map for reuse
	for k := range m {
		delete(m, k)
	}

	return m
}

// Events returns all collected events.
func (c *Collector) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Return a copy to avoid race conditions
	eventsCopy := make([]Event, len(c.events))
	copy(eventsCopy, c.events)
	return eventsCopy
}

// Reset clears the collector for reuse.
// Thread-safe for concurrent access.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = c.events[:0]
	c.poolIdx = 0
	// Don't clear handler or enabled status
}
