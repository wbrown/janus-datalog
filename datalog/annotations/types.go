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
// Both halves matter. A producer writing `Emit("pattern/match", …)` and a
// consumer writing `case PatternFiltering:` are invisible to each other: a
// search for the name misses the constant and a search for the constant misses
// the name, so neither can answer "does anything emit this?" or "does anything
// read this?". One vocabulary, declared here, referenced everywhere, is what
// makes both questions answerable at all.
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

// Payload keys shared by the scan events — the index-selection announcement and
// the completion events. Declared here for the same reason the event names above
// are: a key spelled at the producer is invisible to the formatter that reads it
// and to any other producer that must spell it the same way, so neither "who
// writes this?" nor "who reads this?" is answerable from either side alone.
//
// Deliberately no count of the producers. One was written here and had gone
// stale within the round, which is the same rot the event names were centralised
// to end — a number in prose is a claim about the tree that nothing rechecks.
//
// Only shared keys belong here, where shared includes the formatter: a key one
// producer writes and nothing else reads is that producer's business, and
// answering "who else uses this?" for it is a one-file question.
//
// Values travel typed: KeyIndex carries an IndexType, KeyPattern a
// *query.DataPattern, KeyCardinality a datalog.Keyword. The formatter renders —
// it is the renderer, and a producer that flattens a value to a string spends an
// allocation on every emit to hand its consumer something to parse instead of
// something to compare. The storage types are not nameable here (storage imports
// this package), so consumers read them through fmt.Stringer.
const (
	// The scan's subject and the run it addressed.
	KeyPattern     = "pattern"
	KeyIndex       = "index"
	KeyBound       = "bound"
	KeyCardinality = "cardinality"

	// What a binding-driven scan was driven by.
	KeyBindingSize = "binding.size"
	KeyScansOpened = "scans.opened"

	// The funnel, narrowest last: intake from the index, what CRDT resolution
	// produced from it, what survived the pattern and its constraints.
	//
	// Intake bounds the middle term — a scan cannot resolve datoms it never
	// read — and that ordering is the funnel's whole claim. The last two are
	// not so ordered: a merge join emits a tuple per (datom, binding tuple) pair,
	// so it can match more than it resolved.
	KeyDatomsScanned  = "datoms.scanned"
	KeyDatomsResolved = "datoms.resolved"
	KeyDatomsMatched  = "datoms.matched"

	// What a cache-resolved pattern took from the entry it read. Its own key
	// rather than the funnel's middle term, because a hit resolves nothing: it
	// reads an entry an earlier call built, so the values it serves sit under
	// no intake of this call's and would invert the ordering above. The two
	// terms it does share — intake and matched — mean there exactly what they
	// mean everywhere.
	KeyValuesServed = "values.served"
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

// A handler needs no serializing wrapper, and this package deliberately offers
// none. The engine emits from parallel workers through one handler, so a
// handler that remembers the last event renders one worker's line from
// another's leftovers — and a mutex does not repair that, it only hides the
// race while the pairing stays wrong. An event therefore carries everything its
// line reports, and state lives where the facts are produced.
//
// A wrapper existed here and was removed once nothing called it. Its presence
// was the problem: it made "serialize the consumer" look like the answer to a
// consumer holding state it should never have had.

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
