package storage

import (
	"sync"
	"sync/atomic"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ParseCache caches the parsed form of the text a query is given: a query
// string's *query.Query and rule text's *query.RuleSet. Unlike PlanCache, no
// TTL is needed — the mapping from text to its parsed form is immutable. The
// two arms hold at most maxSize entries together, and a store into a full
// cache evicts the oldest entry of either arm, the one stored earliest in the
// cache's sequence of stores.
type ParseCache struct {
	queries textCache[*query.Query]
	rules   textCache[*query.RuleSet]
	mu      sync.RWMutex

	// stored counts the stores into either arm, under mu. An entry carries the
	// count at its store, so the oldest entry is the one with the least.
	stored uint64

	// Statistics
	hits   int64
	misses int64

	// Configuration
	maxSize int
}

// textCache maps text to the parsed value it names, each entry carrying the
// cache's store count at its store.
type textCache[T any] map[string]*parsed[T]

type parsed[T any] struct {
	value  T
	stored uint64
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

// oldest returns the key of the entry stored earliest and its store count,
// and false when the cache is empty.
func (c textCache[T]) oldest() (string, uint64, bool) {
	var oldestKey string
	var oldestStored uint64
	found := false
	for key, entry := range c {
		if !found || entry.stored < oldestStored {
			oldestKey, oldestStored, found = key, entry.stored, true
		}
	}
	return oldestKey, oldestStored, found
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

// makeRoom evicts the entry stored earliest of either arm when the two arms
// together hold maxSize entries, so the store that follows keeps the cache at
// its bound. Called with mu held.
func (c *ParseCache) makeRoom() {
	if len(c.queries)+len(c.rules) < c.maxSize {
		return
	}
	queryKey, queryStored, queryFound := c.queries.oldest()
	ruleKey, ruleStored, ruleFound := c.rules.oldest()
	switch {
	case queryFound && (!ruleFound || queryStored < ruleStored):
		delete(c.queries, queryKey)
	case ruleFound:
		delete(c.rules, ruleKey)
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

// Set stores a parsed *query.Query for the given query string, under the
// cache's one bound.
func (c *ParseCache) Set(queryString string, q *query.Query) {
	if c == nil || q == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.makeRoom()
	c.stored++
	c.queries[queryString] = &parsed[*query.Query]{value: q, stored: c.stored}
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

// SetRules stores a parsed *query.RuleSet for the given rule text, under the
// cache's one bound.
func (c *ParseCache) SetRules(text string, rs *query.RuleSet) {
	if c == nil || rs == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.makeRoom()
	c.stored++
	c.rules[text] = &parsed[*query.RuleSet]{value: rs, stored: c.stored}
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
