package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// PlanCache caches query plans to avoid re-planning identical queries
type PlanCache struct {
	cache map[string]*cachedPlan
	mu    sync.RWMutex

	// Statistics
	hits   int64
	misses int64

	// Configuration
	maxSize int
	ttl     time.Duration
}

type cachedPlan struct {
	plan      *QueryPlan
	timestamp time.Time
}

// NewPlanCache creates a new query plan cache
func NewPlanCache(maxSize int, ttl time.Duration) *PlanCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default to 1000 cached plans
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute // Default to 5 minute TTL
	}

	return &PlanCache{
		cache:   make(map[string]*cachedPlan),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// Get retrieves a cached plan if it exists and is not expired
func (c *PlanCache) GetWithOptions(q *query.Query, opts PlannerOptions) (*QueryPlan, bool) {
	if c == nil {
		return nil, false
	}

	key := c.computeKeyWithOptions(q, opts)

	c.mu.RLock()
	defer c.mu.RUnlock()

	cached, ok := c.cache[key]
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	// Check if expired
	if time.Since(cached.timestamp) > c.ttl {
		// Note: We don't delete here to avoid write lock, lazy deletion happens on Set
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&c.hits, 1)
	return cached.plan, true
}

// Get retrieves a cached plan - deprecated, use GetWithOptions
func (c *PlanCache) Get(q *query.Query) (*QueryPlan, bool) {
	// For backward compatibility, use default options
	return c.GetWithOptions(q, PlannerOptions{})
}

// Set stores a plan in the cache
func (c *PlanCache) SetWithOptions(q *query.Query, plan *QueryPlan, opts PlannerOptions) {
	if c == nil || plan == nil {
		return
	}

	key := c.computeKeyWithOptions(q, opts)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict expired entries if cache is full
	if len(c.cache) >= c.maxSize {
		c.evictExpired()

		// If still full, evict oldest
		if len(c.cache) >= c.maxSize {
			c.evictOldest()
		}
	}

	c.cache[key] = &cachedPlan{
		plan:      plan,
		timestamp: time.Now(),
	}
}

// Set stores a plan in the cache - deprecated, use SetWithOptions
func (c *PlanCache) Set(q *query.Query, plan *QueryPlan) {
	// For backward compatibility, use default options
	c.SetWithOptions(q, plan, PlannerOptions{})
}

// Clear removes all cached plans
func (c *PlanCache) Clear() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*cachedPlan)
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Stats returns cache statistics
func (c *PlanCache) Stats() (hits, misses int64, size int) {
	if c == nil {
		return 0, 0, 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses), len(c.cache)
}

// computeKeyWithOptions generates a deterministic key for a query with planner options
func (c *PlanCache) computeKeyWithOptions(q *query.Query, opts PlannerOptions) string {
	// Create a string representation that captures the query structure AND options
	// This needs to be deterministic and capture all relevant aspects

	h := sha256.New()

	// Hash find clause
	fmt.Fprintf(h, "FIND:")
	for _, elem := range q.Find {
		fmt.Fprintf(h, "%v;", elem)
	}

	// Hash where clause
	fmt.Fprintf(h, "WHERE:")
	for _, clause := range q.Where {
		fmt.Fprintf(h, "%v;", clause)
	}

	// Hash in clause (but not the actual values, just the structure)
	fmt.Fprintf(h, "IN:")
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			fmt.Fprintf(h, "DB;")
		case query.ScalarInput:
			fmt.Fprintf(h, "SCALAR:%v;", inp.Symbol)
		case query.CollectionInput:
			fmt.Fprintf(h, "COLLECTION:%v;", inp.Symbol)
		case query.TupleInput:
			fmt.Fprintf(h, "TUPLE:%v;", inp.Symbols)
		case query.RelationInput:
			fmt.Fprintf(h, "RELATION:%v;", inp.Symbols)
		}
	}

	// Hash order-by clause
	if q.OrderBy != nil {
		fmt.Fprintf(h, "ORDERBY:")
		for _, order := range q.OrderBy {
			fmt.Fprintf(h, "%v:%v;", order.Variable, order.Direction)
		}
	}

	// Hash planner options that affect the plan
	fmt.Fprintf(h, "OPTIONS:")
	fmt.Fprintf(h, "DynamicReorder:%v;", opts.EnableDynamicReordering)
	fmt.Fprintf(h, "PredicatePush:%v;", opts.EnablePredicatePushdown)
	fmt.Fprintf(h, "CondAggRewrite:%v;", opts.EnableConditionalAggregateRewriting)
	fmt.Fprintf(h, "SubqueryDecorr:%v;", opts.EnableSubqueryDecorrelation)

	return hex.EncodeToString(h.Sum(nil))
}

// computeKey generates a deterministic key for a query - deprecated
func (c *PlanCache) computeKey(q *query.Query) string {
	// For backward compatibility
	return c.computeKeyWithOptions(q, PlannerOptions{})
}

// evictExpired removes expired entries from the cache
func (c *PlanCache) evictExpired() {
	now := time.Now()
	for key, cached := range c.cache {
		if now.Sub(cached.timestamp) > c.ttl {
			delete(c.cache, key)
		}
	}
}

// evictOldest removes the oldest entry from the cache
func (c *PlanCache) evictOldest() {
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
