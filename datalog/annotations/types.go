// Package annotations provides a clean, low-overhead annotation system for
// tracking query execution metrics and debugging information.
package annotations

import (
	"sync"
	"time"
)

// Event name constants following hierarchical naming pattern
const (
	// Query lifecycle
	QueryInvoked           = "query/invoked"
	QueryPlanCreated       = "query/plan.created"
	QueryComplete          = "query/completed"
	QueryTuplesTransmitted = "query/tuples.transmitted"

	// Phase execution
	PhaseBegin    = "phase/begin"
	PhaseComplete = "phase/complete"
	PhaseScore    = "phase/score"

	// Relation operations
	RelationIndexing     = "relation/indexing"
	RelationIndexed      = "relation/indexed"
	CombineRelsBegin     = "combine-rels/begin"
	CombineRelsCollapsed = "combine-rels/collapsed"

	// Pattern matching
	PatternsToRelationsBegin    = "patterns->relations/begin"
	PatternsToRelationsRealized = "patterns->relations/realized"
	MatchesToRelations          = "matches->relations"

	// Detailed pattern matching timing
	PatternIndexSelection = "pattern/index-selection"
	PatternStorageScan    = "pattern/storage-scan"
	PatternFiltering      = "pattern/filtering"
	PatternToRelation     = "pattern/to-relation"

	// Join operations
	JoinHash   = "join/hash"
	JoinNested = "join/nested"
	JoinMerge  = "join/merge"

	// Aggregation operations
	AggregationExecuted = "aggregation/executed"

	// Errors
	ErrorQueryParsing  = "error/query.parsing"
	ErrorQueryBinding  = "error/query.binding"
	ErrorQueryInternal = "error/query.internal"
	ErrorBackend       = "error/backend"
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
