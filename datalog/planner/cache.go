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

// PlanCache caches RealizedPlan to avoid re-planning identical queries
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
	plan      *RealizedPlan
	timestamp time.Time
}

// NewPlanCache creates a new query plan cache holding at most maxSize plans,
// a thousand when given no bound. A positive ttl expires an entry of that
// age; zero or less is no expiry, which is how the database makes its cache.
func NewPlanCache(maxSize int, ttl time.Duration) *PlanCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default to 1000 cached plans
	}
	if ttl < 0 {
		ttl = 0
	}

	return &PlanCache{
		cache:   make(map[string]*cachedPlan),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// GetWithOptions retrieves a cached RealizedPlan for a query under a rule
// set if it exists and, under a TTL, is not expired
func (c *PlanCache) GetWithOptions(q *query.Query, rules *query.RuleSet, opts PlannerOptions) (*RealizedPlan, bool) {
	if c == nil {
		return nil, false
	}

	key := c.computeKeyWithOptions(q, rules, opts)

	c.mu.RLock()
	defer c.mu.RUnlock()

	cached, ok := c.cache[key]
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	// Check if expired
	if c.ttl > 0 && time.Since(cached.timestamp) > c.ttl {
		// Note: We don't delete here to avoid write lock, lazy deletion happens on Set
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&c.hits, 1)
	return cached.plan, true
}

// SetWithOptions stores a RealizedPlan for a query under a rule set in the
// cache
func (c *PlanCache) SetWithOptions(q *query.Query, rules *query.RuleSet, plan *RealizedPlan, opts PlannerOptions) {
	if c == nil || plan == nil {
		return
	}

	key := c.computeKeyWithOptions(q, rules, opts)

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

// computeKeyWithOptions generates a deterministic key for a query under a
// rule set with planner options
func (c *PlanCache) computeKeyWithOptions(q *query.Query, rules *query.RuleSet, opts PlannerOptions) string {
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

	// The positions the rule set requires of an invoked predicate decide the
	// invocation's correlates, and so its phase, and the clause text does not
	// carry them; each invocation, at any depth, contributes its predicate's
	// required positions beside the text.
	if rules != nil {
		for _, inv := range query.Invocations(q.Where) {
			fmt.Fprintf(h, "REQUIRED:%v:%v;", inv.Name, rules.Required(inv.Name))
		}
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
		case query.RulesInput:
			fmt.Fprintf(h, "RULES;")
		}
	}

	// Hash order-by clause
	if q.OrderBy != nil {
		fmt.Fprintf(h, "ORDERBY:")
		for _, order := range q.OrderBy {
			fmt.Fprintf(h, "%v:%v;", order.Variable, order.Descending)
		}
	}

	// Hash limit clause. The realized plan carries the query and the executor
	// reads the limit from it, so two queries differing only by :limit must not
	// share a cache entry (else the wrong limit would be applied).
	if q.Limit != nil {
		fmt.Fprintf(h, "LIMIT:%d;", *q.Limit)
	}

	// Hash planner options that affect the plan, the roster it carries
	// among them
	fmt.Fprintf(h, "OPTIONS:")
	fmt.Fprintf(h, "AlgebraOptimizer:%v;", opts.EnableAlgebraOptimizer)
	fmt.Fprintf(h, "JoinProjectInsertion:%v;", opts.EnableJoinProjectInsertion)
	fmt.Fprintf(h, "CollectRosters:%v;", opts.CollectRosters)

	return hex.EncodeToString(h.Sum(nil))
}

// evictExpired removes the entries older than the TTL; under no TTL nothing
// has expired and the cache is left as it is.
func (c *PlanCache) evictExpired() {
	if c.ttl <= 0 {
		return
	}
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
