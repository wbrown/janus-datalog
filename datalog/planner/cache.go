package planner

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PlanCache caches RealizedPlan to avoid re-planning identical queries. A
// plan is keyed on the structural hash of the parts it was made under: the
// query, the rule set, the symbols bound when the plan was made, the
// invocations held and the planner options that shape a plan. A hit compares
// those parts in full, so a hash collision costs a miss, never a wrong plan.
type PlanCache struct {
	cache map[uint64]*cachedPlan
	mu    sync.RWMutex

	// Statistics
	hits   int64
	misses int64

	// Configuration
	maxSize int
	ttl     time.Duration
}

// cachedPlan is a plan with the parts it was made under, compared on a hit.
type cachedPlan struct {
	plan      *RealizedPlan
	timestamp time.Time
	query     *query.Query
	rules     *query.RuleSet
	bound     []query.Symbol
	held      query.Held
	options   planOptions
}

// planOptions is the planner options that shape a plan: the algebra
// optimizer, join-project insertion for the algebra bridge, and the roster a
// plan carries.
type planOptions struct {
	algebraOptimizer     bool
	joinProjectInsertion bool
	collectRosters       bool
}

func planOptionsOf(opts PlannerOptions) planOptions {
	return planOptions{
		algebraOptimizer:     opts.EnableAlgebraOptimizer,
		joinProjectInsertion: opts.EnableJoinProjectInsertion,
		collectRosters:       opts.CollectRosters,
	}
}

// madeUnder reports whether the plan was made under a request's query, rule
// set, bound symbols, held invocations and options: the parts are compared
// in full, a query and a rule set structurally and the bound symbols as a
// set.
func (p *cachedPlan) madeUnder(q *query.Query, bindings query.Bindings, opts PlannerOptions) bool {
	if p.held != bindings.Held || p.options != planOptionsOf(opts) || len(p.bound) != len(bindings.Symbols) {
		return false
	}
	for _, sym := range p.bound {
		if _, bound := bindings.Symbols[sym]; !bound {
			return false
		}
	}
	if (p.rules == nil) != (bindings.Rules == nil) {
		return false
	}
	if p.rules != nil && !p.rules.Equal(bindings.Rules) {
		return false
	}
	return p.query.Equal(q)
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
		cache:   make(map[uint64]*cachedPlan),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// GetWithOptions retrieves a cached RealizedPlan for a query under the
// caller's bindings, the rule set and the symbols bound when the plan is
// made, if it exists and, under a TTL, is not expired
func (c *PlanCache) GetWithOptions(q *query.Query, bindings query.Bindings, opts PlannerOptions) (*RealizedPlan, bool) {
	if c == nil {
		return nil, false
	}

	key := c.computeKeyWithOptions(q, bindings, opts)

	c.mu.RLock()
	defer c.mu.RUnlock()

	cached, ok := c.cache[key]
	if !ok || !cached.madeUnder(q, bindings, opts) {
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

// SetWithOptions stores a RealizedPlan for a query under the caller's
// bindings in the cache
func (c *PlanCache) SetWithOptions(q *query.Query, bindings query.Bindings, plan *RealizedPlan, opts PlannerOptions) {
	if c == nil || plan == nil {
		return
	}

	key := c.computeKeyWithOptions(q, bindings, opts)
	bound := make([]query.Symbol, 0, len(bindings.Symbols))
	for sym := range bindings.Symbols {
		bound = append(bound, sym)
	}

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
		query:     q,
		rules:     bindings.Rules,
		bound:     bound,
		held:      bindings.Held,
		options:   planOptionsOf(opts),
	}
}

// Clear removes all cached plans
func (c *PlanCache) Clear() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[uint64]*cachedPlan)
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

// computeKeyWithOptions is the structural hash of the parts a plan is made
// under, folded FNV-1a: the query's hash; the rule set's hash, computed once
// at the set's construction, after a word saying whether the request carries
// a set; the bound symbols, each by its interned pointer into one word
// whatever order the caller's map iterates; which invocations are held,
// since that decides which bodies the planner walks; and the options that
// shape a plan.
func (c *PlanCache) computeKeyWithOptions(q *query.Query, bindings query.Bindings, opts PlannerOptions) uint64 {
	const prime = 1099511628211
	key := uint64(14695981039346656037)
	fold := func(w uint64) { key = (key ^ w) * prime }

	fold(q.Hash())
	if bindings.Rules == nil {
		fold(0)
	} else {
		fold(1)
		fold(bindings.Rules.Hash())
	}
	var bound uint64
	for sym := range bindings.Symbols {
		bound ^= datalog.HashValue(sym)
	}
	fold(uint64(len(bindings.Symbols)))
	fold(bound)
	fold(uint64(bindings.Held))
	options := planOptionsOf(opts)
	var bits uint64
	if options.algebraOptimizer {
		bits |= 1
	}
	if options.joinProjectInsertion {
		bits |= 2
	}
	if options.collectRosters {
		bits |= 4
	}
	fold(bits)
	return key
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
	var oldestKey uint64
	var oldestTime time.Time
	found := false

	for key, cached := range c.cache {
		if !found || cached.timestamp.Before(oldestTime) {
			oldestKey = key
			oldestTime = cached.timestamp
			found = true
		}
	}

	if found {
		delete(c.cache, oldestKey)
	}
}
