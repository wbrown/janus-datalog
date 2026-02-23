package storage

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ParseCache caches parsed *query.Query objects to avoid re-parsing identical
// query strings. Unlike PlanCache, no TTL is needed — the mapping from a query
// string to its parsed form is immutable.
type ParseCache struct {
	cache map[string]*cachedQuery
	mu    sync.RWMutex

	// Statistics
	hits   int64
	misses int64

	// Configuration
	maxSize int
}

type cachedQuery struct {
	query     *query.Query
	timestamp time.Time // used for LRU eviction only
}

// NewParseCache creates a new query parse cache.
func NewParseCache(maxSize int) *ParseCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &ParseCache{
		cache:   make(map[string]*cachedQuery),
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

	cached, ok := c.cache[queryString]
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&c.hits, 1)
	return cached.query, true
}

// Set stores a parsed *query.Query for the given query string.
func (c *ParseCache) Set(queryString string, q *query.Query) {
	if c == nil || q == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict oldest if full
	if len(c.cache) >= c.maxSize {
		c.evictOldest()
	}

	c.cache[queryString] = &cachedQuery{
		query:     q,
		timestamp: time.Now(),
	}
}

// Clear removes all cached queries.
func (c *ParseCache) Clear() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*cachedQuery)
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Stats returns cache statistics.
func (c *ParseCache) Stats() (hits, misses int64, size int) {
	if c == nil {
		return 0, 0, 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses), len(c.cache)
}

// evictOldest removes the oldest entry from the cache. Must be called with mu held.
func (c *ParseCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, cached := range c.cache {
		if oldestKey == "" || cached.timestamp.Before(oldestTime) {
			oldestKey = key
			oldestTime = cached.timestamp
		}
	}

	if oldestKey != "" {
		delete(c.cache, oldestKey)
	}
}
