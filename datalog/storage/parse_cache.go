package storage

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ParseCache caches the parsed form of the text a query is given: a query
// string's *query.Query and rule text's *query.RuleSet. Unlike PlanCache, no
// TTL is needed — the mapping from text to its parsed form is immutable. Each
// arm holds at most maxSize entries and evicts its oldest beyond that.
type ParseCache struct {
	queries textCache[*query.Query]
	rules   textCache[*query.RuleSet]
	mu      sync.RWMutex

	// Statistics
	hits   int64
	misses int64

	// Configuration
	maxSize int
}

// textCache maps text to the parsed value it names, each entry stamped with
// the time it was stored for eviction of the oldest.
type textCache[T any] map[string]*parsed[T]

type parsed[T any] struct {
	value     T
	timestamp time.Time // used for LRU eviction only
}

// get returns the value stored for text and whether one is.
func (c textCache[T]) get(text string) (T, bool) {
	entry, ok := c[text]
	if !ok {
		var zero T
		return zero, false
	}
	return entry.value, true
}

// put stores value for text, evicting the oldest entry first when the cache
// already holds maxSize entries.
func (c textCache[T]) put(text string, value T, maxSize int) {
	if len(c) >= maxSize {
		c.evictOldest()
	}
	c[text] = &parsed[T]{value: value, timestamp: time.Now()}
}

// evictOldest removes the oldest entry from the cache.
func (c textCache[T]) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c {
		if oldestKey == "" || entry.timestamp.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.timestamp
		}
	}

	if oldestKey != "" {
		delete(c, oldestKey)
	}
}

// NewParseCache creates a new parse cache.
func NewParseCache(maxSize int) *ParseCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &ParseCache{
		queries: textCache[*query.Query]{},
		rules:   textCache[*query.RuleSet]{},
		maxSize: maxSize,
	}
}

// Get retrieves a cached *query.Query for the given query string.
func (c *ParseCache) Get(queryString string) (*query.Query, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	q, ok := c.queries.get(queryString)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&c.hits, 1)
	return q, true
}

// Set stores a parsed *query.Query for the given query string.
func (c *ParseCache) Set(queryString string, q *query.Query) {
	if c == nil || q == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.queries.put(queryString, q, c.maxSize)
}

// GetRules retrieves the cached *query.RuleSet for the given rule text.
func (c *ParseCache) GetRules(text string) (*query.RuleSet, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	rs, ok := c.rules.get(text)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&c.hits, 1)
	return rs, true
}

// SetRules stores a parsed *query.RuleSet for the given rule text.
func (c *ParseCache) SetRules(text string, rs *query.RuleSet) {
	if c == nil || rs == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.rules.put(text, rs, c.maxSize)
}

// Clear removes every cached query and rule set.
func (c *ParseCache) Clear() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.queries = textCache[*query.Query]{}
	c.rules = textCache[*query.RuleSet]{}
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Stats returns cache statistics: the hits and misses across both arms, and
// the entries both hold.
func (c *ParseCache) Stats() (hits, misses int64, size int) {
	if c == nil {
		return 0, 0, 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses), len(c.queries) + len(c.rules)
}
