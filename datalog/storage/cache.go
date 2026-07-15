package storage

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// CacheKey identifies a unique (Entity, Attribute) pair for caching.
//
// A CacheKey space belongs to a single snapshot. The latest-state cache (one
// per Database) and each AsOf handle's private cache are separate Cache
// instances, so the same (E, A) never collides across snapshots and no
// per-snapshot dimension is needed in the key.
type CacheKey struct {
	E Entity    // 20-byte entity hash
	A Attribute // 32-byte attribute
}

// CacheEntry holds the resolved CRDT view for an (E, A) pair
type CacheEntry struct {
	// inFlight marks a placeholder entry stored while a commit to this (E, A) is
	// in progress. Readers that see it bypass the cache and resolve directly from
	// storage, and cache writers refuse to overwrite it, so no reader observes the
	// pre-commit value once the storage commit is visible. The remaining fields
	// are unset on a sentinel.
	inFlight    bool
	version     datalog.ElementID // Max ElementID when this entry was computed
	cardinality schema.Cardinality

	// Resolved views (one populated based on cardinality)
	oneValue    any                 // Cardinality-One: single current value
	manySet     map[any]any         // Cardinality-Many: hashable key → original value
	vectorList  []any               // Cardinality-Vector: ordered elements
	vectorIndex []datalog.ElementID // Cardinality-Vector: position → ElementID for O(1) access
}

// Version returns the ElementID when this entry was computed
func (e *CacheEntry) Version() datalog.ElementID {
	return e.version
}

// Cardinality returns the cardinality of this cache entry
func (e *CacheEntry) Cardinality() schema.Cardinality {
	return e.cardinality
}

// OneValue returns the value for cardinality-one attributes
func (e *CacheEntry) OneValue() any {
	return e.oneValue
}

// ManySet returns the set for cardinality-many attributes
func (e *CacheEntry) ManySet() map[any]any {
	return e.manySet
}

// VectorList returns the ordered list for cardinality-vector attributes
func (e *CacheEntry) VectorList() []any {
	return e.vectorList
}

// VectorIndex returns the position index for cardinality-vector attributes
func (e *CacheEntry) VectorIndex() []datalog.ElementID {
	return e.vectorIndex
}

// Cache provides O(1) access to resolved CRDT views
//
// The cache is the primary query resolution mechanism for latest and concrete
// AsOf queries. It stores resolved views (LWW value, add-wins set, RGA vector)
// for each cache key.
//
// Freshness is tracked via maxVersions, updated atomically on every write.
// This provides O(1) freshness checks without storage seeks.
type Cache struct {
	// Per-(E,A) resolved views
	entries sync.Map // map[CacheKey]*CacheEntry

	// Per-(E,A) max ElementID tracking - updated atomically on every write
	// This avoids storage seeks for freshness checks
	maxVersions sync.Map // map[CacheKey]datalog.ElementID

	// Per-attribute version tracking for fast A-bound query freshness checks
	// When querying [?e :name "Bob"], we can check if ANY :name has changed
	// without checking every individual (E, :name) pair
	attrVersions sync.Map // map[Attribute]datalog.ElementID

	// Per-entity attribute tracking - tracks which attributes we've cached per entity
	// This is NOT source of truth - just tracks what's in cache for difference-based
	// decisions on whether to do individual lookups vs full entity scan
	entityAttrs sync.Map // map[Entity]*sync.Map (inner: map[Attribute]struct{})
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{}
}

// inFlightEntry is the shared sentinel stored for an (E, A) while its commit is
// in progress. It carries no resolved value — readers and writers key off the
// inFlight flag alone — so a single shared instance is safe across all keys.
var inFlightEntry = &CacheEntry{inFlight: true}

// BeginInFlight marks the given keys as committing by storing the in-flight
// sentinel for each, overwriting any cached entry. Called by Transaction.Commit
// BEFORE the storage commit; the matching clear is Invalidate (called after the
// commit, or after a failed commit), which deletes the entries. While a key is
// in-flight, GetOrResolve resolves it from storage and storeIfNotInFlight refuses
// to cache it, so the storage commit and the cache become visible atomically from
// a reader's perspective.
func (c *Cache) BeginInFlight(keys []CacheKey) {
	for _, key := range keys {
		c.entries.Store(key, inFlightEntry)
	}
}

// storeIfNotInFlight stores entry for key unless an in-flight sentinel is
// present, returning whether it stored. The compare-and-swap loop closes the
// window where a concurrent rebuild, having resolved the pre-commit value, could
// otherwise clobber a sentinel that BeginInFlight set in the meantime: if the
// slot turned into (or already holds) a sentinel, the store is abandoned.
func (c *Cache) storeIfNotInFlight(key CacheKey, entry *CacheEntry) bool {
	for {
		cur, ok := c.entries.Load(key)
		if ok && cur.(*CacheEntry).inFlight {
			return false // a commit owns this key; do not cache
		}
		if !ok {
			if _, loaded := c.entries.LoadOrStore(key, entry); !loaded {
				return true
			}
			continue // lost the race; re-check (the winner may be a sentinel)
		}
		if c.entries.CompareAndSwap(key, cur, entry) {
			return true
		}
		// The slot changed under us (possibly to a sentinel); re-evaluate.
	}
}

// GetOrResolve returns cached entry if fresh, rebuilds if stale
//
// Freshness is tracked via maxVersions sync.Map, updated atomically on every write.
// This provides O(1) freshness checks without any storage seeks.
// False negatives (returning stale data) are NOT acceptable.
func (c *Cache) GetOrResolve(key CacheKey, resolver CacheResolver) *CacheEntry {
	// Fast path: load existing entry
	if val, ok := c.entries.Load(key); ok {
		entry := val.(*CacheEntry)

		// In-flight: a commit to this (E, A) is in progress. Resolve from storage
		// (the rebuild below) without caching, so the result reflects whichever
		// side of the commit is currently durable — never a stale cache hit.
		if entry.inFlight {
			return c.rebuild(key, resolver)
		}

		// Check freshness: compare stored version with maxVersions (O(1) map lookup)
		if maxVal, ok := c.maxVersions.Load(key); ok {
			currentMax := maxVal.(datalog.ElementID)
			if entry.version == currentMax {
				return entry // Fresh - cache hit
			}
		}
		// Stale or no max tracked - fall through to rebuild
	}

	// Slow path: rebuild and store
	// Note: Two goroutines might both rebuild for the same key.
	// That's fine - CRDT resolution is deterministic, both compute same result.
	entry := c.rebuild(key, resolver)
	if entry != nil {
		// storeIfNotInFlight refuses to overwrite an in-flight sentinel, so a
		// rebuild that raced a commit doesn't re-cache the pre-commit value.
		if c.storeIfNotInFlight(key, entry) {
			// Update maxVersions to reflect what we just resolved
			c.UpdateMaxVersion(key, entry.version)
			// Track that we've cached this attribute for this entity
			c.TrackEntityAttr(key.E, key.A)
		}
	}
	return entry
}

// UpdateMaxVersion updates the max ElementID for a (E,A) pair.
// Called by Transaction.Commit() for every written datom.
// This enables O(1) cache freshness checks without storage seeks.
func (c *Cache) UpdateMaxVersion(key CacheKey, elemID datalog.ElementID) {
	for {
		val, loaded := c.maxVersions.Load(key)
		if !loaded {
			// No existing value - try to store
			if _, swapped := c.maxVersions.LoadOrStore(key, elemID); swapped {
				// We stored successfully
				return
			}
			// LoadOrStore returned existing value, retry
			continue
		}
		current := val.(datalog.ElementID)
		if current.Compare(elemID) >= 0 {
			return // Current is already >= new value
		}
		// Try to update to new max using CompareAndSwap
		if c.maxVersions.CompareAndSwap(key, current, elemID) {
			return
		}
		// CAS failed, retry
	}
}

// Invalidate removes cached entries for the given keys.
// Called by Database on tx.Commit() with all touched (E,A) pairs.
// Note: maxVersions is NOT cleared here - it's updated by UpdateMaxVersion()
// during commit, preserving the max for freshness checks.
func (c *Cache) Invalidate(touched []CacheKey) {
	for _, key := range touched {
		c.entries.Delete(key)
	}
	// Note: attrVersions invalidation is implicit -
	// next IsAttributeFresh() call will fetch current max from store
}

// InvalidateRewind drops cached state for keys whose datoms a rollback physically removed.
// Unlike Invalidate (forward commits, where maxVersions was just advanced and must be
// kept), a rewind RETREATS the version high-water, and UpdateMaxVersion is monotonic — so
// the per-(E,A) max and the per-attribute version must be dropped too, or a rebuilt
// lower-version entry would compare unequal to the stranded max forever and never cache-hit
// again. Pairs with BeginInFlight: that opens the uncached window before the delete, this
// closes it after.
func (c *Cache) InvalidateRewind(touched []CacheKey) {
	seenAttr := make(map[Attribute]struct{}, len(touched))
	for _, key := range touched {
		c.entries.Delete(key)
		c.maxVersions.Delete(key)
		if _, ok := seenAttr[key.A]; !ok {
			seenAttr[key.A] = struct{}{}
			c.attrVersions.Delete(key.A)
		}
	}
}

// InvalidateAttribute removes all cached entries for the given attribute,
// regardless of entity. Used when a write to a unique attribute may have
// silently staled other entities' cached values under the walk-based
// (A, V)-LWW resolution.
//
// Conservative strategy (CRDT_UNIQUE_SEMANTICS.md D3): one unique-attr
// write invalidates every cached entry for that attribute. A future
// optimization could maintain an (A, V) → [E] reverse index to invalidate
// only the entities whose walks could actually change; the current
// approach is simpler and correct.
//
// Also removes the per-(E, A) max-version entries for this attribute so
// the next read recomputes freshness from storage. attrVersions (for
// whole-attribute freshness) is not touched here — subsequent writes
// on any E still advance the per-key max.
func (c *Cache) InvalidateAttribute(a Attribute) {
	c.entries.Range(func(k, _ any) bool {
		if key, ok := k.(CacheKey); ok && key.A == a {
			c.entries.Delete(key)
		}
		return true
	})
	c.maxVersions.Range(func(k, _ any) bool {
		if key, ok := k.(CacheKey); ok && key.A == a {
			c.maxVersions.Delete(key)
		}
		return true
	})
}

// IsAttributeFresh checks if the entire attribute is fresh in cache
// Used for A-bound queries like [?e :name "Bob"] to avoid checking every entity
//
// IMPLEMENTATION NOTE: MaxElementIDForAttribute() performs an O(1) forward seek
// on the ATEV index. ATEV is ordered A → Tx↓ → E → V, so the first entry under
// prefix [A] is the global max-Tx datom for the attribute in a single seek.
//
// EDGE CASE - Initial population after restart:
// After process restart, attrVersions is empty. The first A-bound query will:
// 1. Return false from IsAttributeFresh (no cached version)
// 2. Trigger resolution of all entities for that attribute
// 3. Call UpdateAttributeVersion with the max seen
//
// For attributes with millions of entities, this first query after restart
// may be slow. Subsequent queries use the cached attrVersions and are O(1).
type attributeVersionStore interface {
	MaxElementIDForAttribute(a []byte) (datalog.ElementID, error)
}

func (c *Cache) IsAttributeFresh(a Attribute, store attributeVersionStore) bool {
	val, ok := c.attrVersions.Load(a)
	if !ok {
		return false // No cached version - first query after restart will be slow
	}
	cachedMax := val.(datalog.ElementID)
	storeMax, err := store.MaxElementIDForAttribute(a[:])
	if err != nil {
		return false
	}
	return cachedMax == storeMax
}

// UpdateAttributeVersion updates the cached version for an attribute
// Called after resolving all entities for an attribute
func (c *Cache) UpdateAttributeVersion(a Attribute, version datalog.ElementID) {
	c.attrVersions.Store(a, version)
}

// Clear removes all entries from the cache
// Useful for testing or forced cache invalidation
func (c *Cache) Clear() {
	c.entries = sync.Map{}
	c.maxVersions = sync.Map{}
	c.attrVersions = sync.Map{}
	c.entityAttrs = sync.Map{}
}

// TrackEntityAttr records that we have cached an (E, A) entry
// Called when storing cache entries to track what's cached per entity
func (c *Cache) TrackEntityAttr(e Entity, a Attribute) {
	// Get or create the attribute set for this entity
	val, _ := c.entityAttrs.LoadOrStore(e, &sync.Map{})
	set := val.(*sync.Map)
	set.Store(a, struct{}{})
}

// GetCachedAttrs returns the set of attributes we've cached for an entity
// Returns nil if no attributes are cached for this entity
func (c *Cache) GetCachedAttrs(e Entity) map[Attribute]bool {
	val, ok := c.entityAttrs.Load(e)
	if !ok {
		return nil
	}
	set := val.(*sync.Map)

	result := make(map[Attribute]bool)
	set.Range(func(key, _ any) bool {
		result[key.(Attribute)] = true
		return true
	})
	return result
}

// PopulateFromDatoms resolves a group of datoms for a single (E, A) pair
// and stores the result in the cache. Uses the canonical Resolve*FromDatoms
// functions from crdt_resolve.go — no CRDT logic duplication.
//
// This is the dispatch target for PrefetchEntities: as the EATV iterator
// crosses an attribute boundary, the accumulated datoms are resolved and
// cached here in a single call.
func (c *Cache) PopulateFromDatoms(key CacheKey, card schema.Cardinality, datoms []datalog.Datom) {
	// Skip if a commit to this (E, A) is in flight (the committer owns the cache
	// for this key), or if already cached and fresh.
	if val, ok := c.entries.Load(key); ok {
		entry := val.(*CacheEntry)
		if entry.inFlight {
			return
		}
		if maxVal, ok := c.maxVersions.Load(key); ok {
			if entry.version == maxVal.(datalog.ElementID) {
				return
			}
		}
	}

	var entry *CacheEntry

	switch card {
	case schema.CardinalityOne:
		value, maxID := ResolveLWWFromDatoms(datoms)
		entry = &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityOne,
			oneValue:    value,
		}

	case schema.CardinalityMany:
		members, maxID := ResolveAddWinsFromDatoms(datoms)
		entry = &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityMany,
			manySet:     members,
		}

	case schema.CardinalityVector:
		elements, positions, maxID := ResolveRGAFromDatoms(datoms)
		entry = &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityVector,
			vectorList:  elements,
			vectorIndex: positions,
		}

	default:
		value, maxID := ResolveLWWFromDatoms(datoms)
		entry = &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityOne,
			oneValue:    value,
		}
	}

	if entry != nil {
		// Refuse to cache if a commit grabbed this key after the freshness check.
		if c.storeIfNotInFlight(key, entry) {
			c.UpdateMaxVersion(key, entry.version)
			c.TrackEntityAttr(key.E, key.A)
		}
	}
}

// rebuild resolves the current value for (E, A) based on cardinality
func (c *Cache) rebuild(key CacheKey, resolver CacheResolver) *CacheEntry {
	card := resolver.GetCardinality(key.A)

	switch card {
	case schema.CardinalityOne:
		return c.rebuildOne(key, resolver)

	case schema.CardinalityMany:
		return c.rebuildMany(key, resolver)

	case schema.CardinalityVector:
		return c.rebuildVector(key, resolver)

	default:
		// Default to cardinality-one for schemaless
		return c.rebuildOne(key, resolver)
	}
}

// rebuildOne resolves cardinality-one using LWW semantics
// Scans EATV with descending ElementID, first entry is current
func (c *Cache) rebuildOne(key CacheKey, resolver CacheResolver) *CacheEntry {
	value, maxID, err := resolver.ResolveLWW(key.E, key.A)
	if err != nil {
		return nil
	}

	return &CacheEntry{
		version:     maxID,
		cardinality: schema.CardinalityOne,
		oneValue:    value,
	}
}

// rebuildMany resolves cardinality-many using add-wins semantics
func (c *Cache) rebuildMany(key CacheKey, resolver CacheResolver) *CacheEntry {
	members, maxID, err := resolver.ResolveAddWins(key.E, key.A)
	if err != nil {
		return nil
	}

	return &CacheEntry{
		version:     maxID,
		cardinality: schema.CardinalityMany,
		manySet:     members,
	}
}

// rebuildVector resolves cardinality-vector using RGA reconstruction
func (c *Cache) rebuildVector(key CacheKey, resolver CacheResolver) *CacheEntry {
	elements, positions, maxID, err := resolver.ResolveRGA(key.E, key.A)
	if err != nil {
		return nil
	}

	return &CacheEntry{
		version:     maxID,
		cardinality: schema.CardinalityVector,
		vectorList:  elements,
		vectorIndex: positions,
	}
}

// CacheResolver provides methods to resolve CRDT values from storage
// This interface decouples the cache from specific storage implementation
type CacheResolver interface {
	// GetCardinality returns the cardinality for an attribute
	GetCardinality(a Attribute) schema.Cardinality

	// ResolveLWW returns the current value for cardinality-one (highest ElementID wins)
	// Returns (value, maxElementID, error)
	ResolveLWW(e Entity, a Attribute) (any, datalog.ElementID, error)

	// ResolveAddWins returns the current set members for cardinality-many
	// Returns (members, maxElementID, error)
	ResolveAddWins(e Entity, a Attribute) (map[any]any, datalog.ElementID, error)

	// ResolveRGA returns the ordered vector for cardinality-vector
	// Returns (elements, positionIndex, maxElementID, error)
	ResolveRGA(e Entity, a Attribute) ([]any, []datalog.ElementID, datalog.ElementID, error)
}

// ResolveEntry resolves a CacheEntry directly from storage without caching.
// This is used when cache is disabled but CRDT resolution is still needed.
func ResolveEntry(key CacheKey, resolver CacheResolver) *CacheEntry {
	card := resolver.GetCardinality(key.A)

	switch card {
	case schema.CardinalityOne:
		value, maxID, err := resolver.ResolveLWW(key.E, key.A)
		if err != nil {
			return nil
		}
		return &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityOne,
			oneValue:    value,
		}

	case schema.CardinalityMany:
		members, maxID, err := resolver.ResolveAddWins(key.E, key.A)
		if err != nil {
			return nil
		}
		return &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityMany,
			manySet:     members,
		}

	case schema.CardinalityVector:
		elements, positions, maxID, err := resolver.ResolveRGA(key.E, key.A)
		if err != nil {
			return nil
		}
		return &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityVector,
			vectorList:  elements,
			vectorIndex: positions,
		}

	default:
		// Default to cardinality-one for schemaless
		value, maxID, err := resolver.ResolveLWW(key.E, key.A)
		if err != nil {
			return nil
		}
		return &CacheEntry{
			version:     maxID,
			cardinality: schema.CardinalityOne,
			oneValue:    value,
		}
	}
}
