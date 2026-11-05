package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// CacheKey identifies a unique (Entity, Attribute) pair for caching.
//
// The key names no view. A database's cache holds the latest resolutions, and
// a view as of a target holds its own in a cache of its own (Cache.ForView).
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
	cardinality datalog.Keyword
	// unique is whether the entry was resolved under the unique rule, the
	// resolver's answer (CacheResolver.ResolvesUnique) when it was built, or
	// false for an entry PopulateFromDatoms built from a group's datoms under
	// the plain rule. With cardinality it is the entry's resolution rule: a
	// reader under another rule is not served the entry.
	unique bool
	// fence is the read point the entry's resolution began at, recorded when a
	// view's cache stores the entry. The view serves the entry while no arrival
	// on its key lies above the fence.
	fence datalog.ElementID

	// Resolved views (one populated based on cardinality)
	oneValue    any                 // Cardinality-One: single current value
	manySet     map[string]any      // Cardinality-Many: memberKey → original value
	vectorList  []any               // Cardinality-Vector: ordered elements
	vectorIndex []datalog.ElementID // Cardinality-Vector: position → ElementID for O(1) access

	// scanned is the index intake the resolution that built this entry spent.
	// It belongs on the entry rather than on the call that triggered the build:
	// the entry is what the cost bought, so a later reader of the same entry can
	// say what it cost, and a hit reports zero because a hit reads no index.
	scanned int
}

// Version returns the ElementID when this entry was computed
func (e *CacheEntry) Version() datalog.ElementID {
	return e.version
}

// valueCount is how many values this entry hands a pattern that reads it,
// across whichever view its cardinality populated. It is what a cache-resolved
// pattern reports as values.served, against the tuples it went on to match.
//
// Counted in the unit the pattern binds, which is why the vector arm does not
// return its length: a cardinality-vector entry holds one value — the vector —
// and V binds to the whole of it. Reporting three served against one matched
// for a three-element vector would render as a pattern discarding two values it
// never saw separately.
func (e *CacheEntry) valueCount() int {
	switch e.cardinality {
	case schema.CardinalityMany:
		return len(e.manySet)
	case schema.CardinalityVector:
		// One, whatever the length. A cardinality-vector entry holds a single
		// value — the vector — and V binds to the whole of it. An entry exists
		// only for an (E, A) that has datoms, so an empty vectorList is a
		// vector that was cleared, and it serves that empty vector rather than
		// serving nothing.
		return 1
	default:
		// CardinalityOne and schemaless. An entry whose highest-Tx datom is a
		// tombstone resolves to no value — zero, not one, or a tombstoned
		// attribute would read like a present one.
		if e.oneValue == nil {
			return 0
		}
		return 1
	}
}

// Cardinality returns the cardinality of this cache entry
func (e *CacheEntry) Cardinality() datalog.Keyword {
	return e.cardinality
}

// OneValue returns the value for cardinality-one attributes
func (e *CacheEntry) OneValue() any {
	return e.oneValue
}

// ManySet returns the set for cardinality-many attributes, each member under
// its memberKey.
func (e *CacheEntry) ManySet() map[string]any {
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
// Datoms of a key can arrive below the highest ElementID among those already
// present: a transaction that drew earlier commits later. So a commit's
// arrival is an event of its own: it takes an ElementID from the database's
// clock once its datoms are readable, and raises its keys' versions to that
// ElementID. A reader takes its read point
// from the clock before it reads, and the cache stores a resolution only when
// no arrival on the key lies above that read point.
//
// A view as of a target keeps its resolutions in a cache of its own (ForView),
// freed with the view. That cache records no arrival: it reads the database
// cache's record of each key, and serves an entry while no arrival on the key,
// and no drop of its slot, lies above the read point its resolution began at.
type Cache struct {
	// Per-(E,A) combined state: the resolved view and its max-version
	// high-water mark live in one slot, so the hot path — load, check
	// in-flight, check freshness — is a single trie walk.
	slots *cacheTrie

	// Per-entity attribute tracking - tracks which attributes we've cached per entity
	// This is NOT source of truth - just tracks what's in cache for difference-based
	// decisions on whether to do individual lookups vs full entity scan
	entityAttrs sync.Map // map[Entity]*sync.Map (inner: map[Attribute]struct{})

	// clock is the database's clock: a reader of the latest store takes its
	// read point from it, and dropping slots draws the floor from it.
	clock *LamportClock

	// floor is the version a key with no slot reads. It lies at or above every
	// read point taken before the cache was made, and above every one taken
	// before slots were last dropped.
	floor atomic.Pointer[datalog.ElementID]

	// arrivals is the cache that records the arrivals on each key: the cache
	// itself for a database's cache, and the database's cache for a view's.
	arrivals *Cache
}

// NewCache creates a cache for a database whose clock is clock. Its floor
// starts at the clock's read point: the datoms already in the store arrived
// before the cache was made, so a read that began before then stores nothing
// into a key with no slot.
func NewCache(clock *LamportClock) *Cache {
	c := &Cache{slots: newCacheTrie(), clock: clock}
	c.arrivals = c
	floor := clock.ReadPoint()
	c.floor.Store(&floor)
	return c
}

// ForView creates the cache of a view as of a target over c's database. It
// holds the view's resolutions alone, so they are freed with the view, and it
// reads c's record of the arrivals on each key.
func (c *Cache) ForView() *Cache {
	v := &Cache{slots: newCacheTrie(), clock: c.clock, arrivals: c}
	v.floor.Store(&datalog.ElementID{})
	return v
}

// ReadPoint is the read point of a reader of the latest store that begins
// now.
func (c *Cache) ReadPoint() datalog.ElementID {
	return c.clock.ReadPoint()
}

// versionOf is the version recorded for a key: its slot's when it has one
// (ok), and the floor when it has none.
func (c *Cache) versionOf(slot cacheSlot, ok bool) datalog.ElementID {
	if !ok {
		return *c.floor.Load()
	}
	return slot.version
}

// raiseFloor draws an ElementID above every read point taken so far and raises
// the floor to it. A caller raises it before it drops slots, so a resolution
// that read before the drop is not stored into a slot the drop emptied.
func (c *Cache) raiseFloor() {
	next := c.clock.Next()
	for {
		current := c.floor.Load()
		if !current.Less(next) {
			return
		}
		if c.floor.CompareAndSwap(current, &next) {
			return
		}
	}
}

// annotateRebuild reports one cache rebuild and why it happened. Callers
// check handler != nil before calling; the emission itself never guards.
func annotateRebuild(handler annotations.Handler, key CacheKey, reason string, slot cacheSlot) {
	data := map[string]interface{}{
		// The keyword, not its rendering. Interning already produced the
		// canonical pointer; String() would spend an allocation per rebuild to
		// hand the consumer something to parse back.
		annotations.KeyAttribute: datalog.InternKeywordFromBytes(key.A),
		"reason":                 reason,
		"slot_version":           slot.version.Lamport,
	}
	if slot.entry != nil {
		data["entry_version"] = slot.entry.version.Lamport
	}
	handler(annotations.Event{Name: annotations.CacheRebuild, Data: data})
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
		for {
			slot, ok := c.slots.Load(key)
			if !ok {
				if _, loaded := c.slots.LoadOrStore(key, cacheSlot{entry: inFlightEntry, version: c.versionOf(slot, ok)}); !loaded {
					break
				}
				continue // lost the race; re-read and CAS
			}
			if c.slots.CompareAndSwap(key, slot, cacheSlot{entry: inFlightEntry, version: slot.version}) {
				break
			}
		}
	}
}

// storeForView is storeIfNotInFlight in a view's cache. The database's cache
// records the arrivals on key: the view stores entry, with readPoint as its
// fence, unless a commit owns the key there or an arrival on the key, or a
// drop of its slot, lies above readPoint. An entry of the view's resolved
// under the same rule and fenced at or above readPoint stands.
func (c *Cache) storeForView(key CacheKey, entry *CacheEntry, readPoint datalog.ElementID) bool {
	arrival, ok := c.arrivals.slots.Load(key)
	if ok && arrival.entry != nil && arrival.entry.inFlight {
		return false
	}
	if readPoint.Less(c.arrivals.versionOf(arrival, ok)) {
		return false
	}
	entry.fence = readPoint
	stored := cacheSlot{entry: entry}
	for {
		slot, held := c.slots.Load(key)
		if !held {
			if _, loaded := c.slots.LoadOrStore(key, stored); !loaded {
				return true
			}
			continue // lost the race; re-read and CAS
		}
		if slot.entry != nil && slot.entry.cardinality == entry.cardinality &&
			slot.entry.unique == entry.unique && !slot.entry.fence.Less(readPoint) {
			return false
		}
		if c.slots.CompareAndSwap(key, slot, stored) {
			return true
		}
	}
}

// storeIfNotInFlight stores entry for key unless an in-flight sentinel is
// present, returning whether it stored. The compare-and-swap loop closes the
// window where a concurrent rebuild, having resolved the pre-commit value, could
// otherwise clobber a sentinel that BeginInFlight set in the meantime: if the
// slot turned into (or already holds) a sentinel, the store is abandoned.
//
// readPoint is the read point the resolution took before it read. When the
// version recorded for the key lies above it, a datom arrived on the key after
// the resolution began, so the resolution may lack it and is not stored.
// storeIfNotInFlight also advances the slot's max-version high-water mark to
// the entry's version. In a view's cache it is storeForView.
func (c *Cache) storeIfNotInFlight(key CacheKey, entry *CacheEntry, readPoint datalog.ElementID) bool {
	if c.arrivals != c {
		return c.storeForView(key, entry, readPoint)
	}
	for {
		slot, ok := c.slots.Load(key)
		if ok && slot.entry != nil && slot.entry.inFlight {
			return false // a commit owns this key; do not cache
		}
		recorded := c.versionOf(slot, ok)
		if readPoint.Less(recorded) {
			return false
		}
		if ok && slot.entry != nil && slot.entry.version.Compare(entry.version) > 0 {
			// An existing fresher entry stands. A rebuild from an older
			// snapshot must never regress the cache: its entry would be
			// born stale (never served) and would evict a servable one.
			return false
		}
		version := recorded
		if entry.version.Compare(version) > 0 {
			version = entry.version
		}
		stored := cacheSlot{entry: entry, version: version}
		if !ok {
			if _, loaded := c.slots.LoadOrStore(key, stored); loaded {
				continue // lost the race; re-check (the winner may be a sentinel)
			}
			// A drop that raised the floor past the read point may have run
			// between reading the floor and creating the slot, and found no
			// slot to drop. The entry is withdrawn then; whether this delete
			// or a later change to the slot removes it, it is not served.
			if readPoint.Less(*c.floor.Load()) {
				c.slots.CompareAndDelete(key, stored)
				return false
			}
			return true
		}
		if c.slots.CompareAndSwap(key, slot, stored) {
			return true
		}
		// The slot changed under us (possibly to a sentinel); re-evaluate.
	}
}

// getOrResolveForView is GetOrResolve in a view's cache. Every entry the view
// holds is a resolution as of its target, so an entry under the reader's rule
// serves while the version the database's cache records for its key lies at
// or below the entry's fence. A commit that owns the key there sends the
// reader to storage, uncached.
func (c *Cache) getOrResolveForView(key CacheKey, card datalog.Keyword, resolver CacheResolver, readPoint datalog.ElementID, handler annotations.Handler, report *scanReport) (*CacheEntry, error) {
	arrival, ok := c.arrivals.slots.Load(key)
	if ok && arrival.entry != nil && arrival.entry.inFlight {
		if handler != nil {
			annotateRebuild(handler, key, "in-flight", arrival)
		}
		return ResolveEntry(key, card, resolver, report)
	}
	recorded := c.arrivals.versionOf(arrival, ok)
	slot, held := c.slots.Load(key)
	switch {
	case !held:
		if handler != nil {
			annotateRebuild(handler, key, "absent", slot)
		}
	case slot.entry.cardinality != card:
		if handler != nil {
			annotateRebuild(handler, key, "cardinality", slot)
		}
	case slot.entry.unique != resolver.ResolvesUnique(key.A):
		if handler != nil {
			annotateRebuild(handler, key, "uniqueness", slot)
		}
	case !slot.entry.fence.Less(recorded):
		return slot.entry, nil
	default:
		if handler != nil {
			annotateRebuild(handler, key, "arrival", slot)
		}
	}
	entry, err := ResolveEntry(key, card, resolver, report)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	if c.storeForView(key, entry, readPoint) {
		c.TrackEntityAttr(key.E, key.A)
	}
	return entry, nil
}

// GetOrResolve returns the cached entry if fresh, rebuilding if stale. In a
// view's cache it is getOrResolveForView.
//
// card is the cardinality the caller resolves the attribute under, the
// schema's for a declared attribute and one otherwise. An entry resolved under
// another cardinality is not a hit: a set's view serves no reader under
// cardinality one, so the entry is rebuilt under card.
//
// readPoint is the read point the caller took before it read: before its
// session opened, or now for a reader of the latest store. An entry serves the
// caller when every datom it holds is at or below the read point, which makes
// it the view as of the read point. Otherwise the caller resolves through its
// resolver, and the cache stores that resolution only when no datom arrived on
// the key after the read point.
//
// An arrival clears its key's entry, and a resolution is stored only when it
// read every arrival, so an entry present and not in flight holds every datom
// of its key. False negatives (returning stale data) are NOT acceptable.
// The report accrues the index intake *this call* spends: the entry's build cost
// when this call built it, and nothing when the entry came from cache. It is
// not entry.scanned, which is the entry's build cost forever. The distinction
// is the difference between "what this read cost" and "what this (E, A) cost to
// resolve once, some time ago", and datoms.scanned means the first everywhere
// else in the engine — a trace of a thousand hits must not report a thousand
// index reads that did not happen.
//
// Accrual happens inside the resolvers, so a rebuild that fails keeps what it
// read; the callers that report nothing pass DiscardIntake and say so.
func (c *Cache) GetOrResolve(key CacheKey, card datalog.Keyword, resolver CacheResolver, readPoint datalog.ElementID, handler annotations.Handler, report *scanReport) (*CacheEntry, error) {
	card = resolvedCardinality(card)
	if c.arrivals != c {
		return c.getOrResolveForView(key, card, resolver, readPoint, handler, report)
	}
	// Fast path: one trie walk yields the entry and its freshness bound.
	slot, ok := c.slots.Load(key)
	if ok && slot.entry != nil {
		// In-flight: a commit to this (E, A) is in progress. Resolve from storage
		// (the rebuild below) without caching, so the result reflects whichever
		// side of the commit is currently durable — never a stale cache hit.
		if slot.entry.inFlight {
			if handler != nil {
				annotateRebuild(handler, key, "in-flight", slot)
			}
			entry, err := ResolveEntry(key, card, resolver, report)
			if err != nil {
				return nil, err
			}
			return entry, nil
		}

		// The entry serves iff it was resolved under the rule this reader
		// resolves under, cardinality and uniqueness, and every datom it
		// holds lies at or below the reader's read point.
		switch {
		case slot.entry.cardinality != card:
			// Resolved under another declaration of the attribute: a set's
			// view serves no reader under cardinality one. The rebuild under
			// this reader's cardinality stands at the same version and so
			// replaces the entry.
			if handler != nil {
				annotateRebuild(handler, key, "cardinality", slot)
			}
		case slot.entry.unique != resolver.ResolvesUnique(key.A):
			// Resolved under the other uniqueness rule: a value the unique
			// walk settled serves no reader under the plain rule, and the
			// plain first entry serves none under the walk. The rebuild under
			// this reader's rule replaces the entry.
			if handler != nil {
				annotateRebuild(handler, key, "uniqueness", slot)
			}
		case !readPoint.Less(slot.entry.version):
			// Fresh - cache hit. No index was read to answer this call, so
			// the report accrues nothing.
			return slot.entry, nil
		default:
			if handler != nil {
				annotateRebuild(handler, key, "snapshot-bound", slot)
			}
		}
	} else if handler != nil {
		annotateRebuild(handler, key, "absent", slot)
	}

	// Slow path: rebuild and store
	// Note: Two goroutines might both rebuild for the same key.
	// That's fine - CRDT resolution is deterministic, both compute same result.
	entry, err := ResolveEntry(key, card, resolver, report)
	if err != nil {
		// A failed resolution is not an absent value, and it is emphatically
		// not something to cache: storing it would make one failed scan the
		// answer every later reader of this (E, A) receives. What it read is
		// already in the report, which is the difference between a failed read
		// and one that never happened.
		return nil, err
	}
	if entry == nil {
		// The (E, A) has no datoms. Absence is not cached, deliberately: entry
		// existence is what every reader downstream reads as the attribute's
		// existence. The cost is that the next read of the same absent (E, A) scans again.
		return nil, nil
	}
	// storeIfNotInFlight refuses to overwrite an in-flight sentinel, so a
	// rebuild that raced a commit doesn't re-cache the pre-commit value.
	// It also advances the slot's high-water mark to the entry's version.
	if c.storeIfNotInFlight(key, entry, readPoint) {
		// Track that we've cached this attribute for this entity
		c.TrackEntityAttr(key.E, key.A)
	}
	return entry, nil
}

// UpdateMaxVersion records an arrival of datoms on key. arrival is an
// ElementID drawn from the database's clock once the datoms are readable.
// The slot's version rises to it, and the slot's entry, which may lack the
// datoms, is dropped; an in-flight sentinel stays for its commit to clear.
func (c *Cache) UpdateMaxVersion(key CacheKey, arrival datalog.ElementID) {
	for {
		slot, ok := c.slots.Load(key)
		next := cacheSlot{version: c.versionOf(slot, ok)}
		if ok && slot.entry != nil && slot.entry.inFlight {
			next.entry = slot.entry
		}
		if next.version.Less(arrival) {
			next.version = arrival
		}
		if !ok {
			if _, loaded := c.slots.LoadOrStore(key, next); !loaded {
				return
			}
			continue // lost the race; re-read and CAS
		}
		if next == slot {
			return
		}
		if c.slots.CompareAndSwap(key, slot, next) {
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
		for {
			slot, ok := c.slots.Load(key)
			if !ok || slot.entry == nil {
				break // nothing cached; the version high-water mark stands
			}
			if c.slots.CompareAndSwap(key, slot, cacheSlot{version: slot.version}) {
				break
			}
		}
	}
}

// InvalidateRewind drops the slots of keys whose datoms a rollback physically
// removed, once the removal is readable. It raises the floor first, so a
// resolution that read the removed datoms is not stored for them. Pairs with
// BeginInFlight: that opens the uncached window before the delete, this closes
// it after.
func (c *Cache) InvalidateRewind(touched []CacheKey) {
	c.raiseFloor()
	for _, key := range touched {
		c.slots.Delete(key)
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
// It raises the floor before it drops the attribute's slots, so a resolution
// of another entity's key that read before the write is not stored, whether or
// not the key had a slot.
func (c *Cache) InvalidateAttribute(a Attribute) {
	c.raiseFloor()
	c.slots.Range(func(key CacheKey, _ cacheSlot) bool {
		if key.A == a {
			c.slots.Delete(key)
		}
		return true
	})
}

// Clear removes all entries from the cache. It raises the floor first, so a
// resolution that read before the clear is not stored afterward.
func (c *Cache) Clear() {
	c.raiseFloor()
	c.slots.Clear()
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
//
// readPoint is the read point the caller took before it began reading the
// datoms.
func (c *Cache) PopulateFromDatoms(key CacheKey, card datalog.Keyword, datoms []datalog.Datom, readPoint datalog.ElementID) {
	// Skip if a commit to this (E, A) is in flight (the committer owns the cache
	// for this key), or if already cached under this cardinality. An entry a
	// view holds may be one an arrival has passed, so storeForView decides
	// whether a view's stands.
	if slot, ok := c.slots.Load(key); ok && slot.entry != nil && c.arrivals == c {
		if slot.entry.inFlight {
			return
		}
		if slot.entry.cardinality == resolvedCardinality(card) {
			return
		}
	}

	// No datoms means the (E, A) does not exist, and an absent one is never
	// cached: downstream, entry existence is the attribute's existence.
	if len(datoms) == 0 {
		return
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
		// Refuse to cache if a commit grabbed this key after the check above,
		// or if a datom arrived on it after the read point; the store advances
		// the slot's high-water mark itself.
		if c.storeIfNotInFlight(key, entry, readPoint) {
			c.TrackEntityAttr(key.E, key.A)
		}
	}
}

// resolvedCardinality is the cardinality an (E, A) resolves under: the
// attribute's declared one, and one for an attribute with no declaration.
func resolvedCardinality(card datalog.Keyword) datalog.Keyword {
	switch card {
	case schema.CardinalityMany, schema.CardinalityVector:
		return card
	}
	return schema.CardinalityOne
}

// ResolveEntry resolves the current value for (E, A) under card, the
// cardinality the caller resolves the attribute under.
//
// It returns three outcomes, and they are three: an error, a non-nil entry, or
// a nil entry with a nil error meaning **this (E, A) has no datoms**. Absence
// is a resolved answer, not a failure and not an empty value, and it is never
// cached — so downstream, the existence of an entry *is* the existence of the
// attribute, and nothing has to reconstruct that from a count or a nil slice.
//
// It takes no receiver: resolution reads storage through the resolver and
// touches no cache state. GetOrResolve is this plus the freshness check and the
// store; the cache-disabled read path calls it directly.
func ResolveEntry(key CacheKey, card datalog.Keyword, resolver CacheResolver, report *scanReport) (*CacheEntry, error) {
	switch resolvedCardinality(card) {
	case schema.CardinalityMany:
		return rebuildMany(key, resolver, report)
	case schema.CardinalityVector:
		return rebuildVector(key, resolver, report)
	}
	return rebuildOne(key, resolver, report)
}

// rebuildOne resolves cardinality-one using LWW semantics
// Scans EATV with descending ElementID, first entry is current
func rebuildOne(key CacheKey, resolver CacheResolver, report *scanReport) (*CacheEntry, error) {
	var before int
	if report != nil {
		before = report.scanned
	}
	value, maxID, present, err := resolver.ResolveLWW(key.E, key.A, report)
	if err != nil {
		return nil, fmt.Errorf("resolve LWW value: %w", err)
	}
	if !present {
		return nil, nil
	}

	entry := &CacheEntry{
		version:     maxID,
		cardinality: schema.CardinalityOne,
		unique:      resolver.ResolvesUnique(key.A),
		oneValue:    value,
	}
	if report != nil {
		entry.scanned = report.scanned - before
	}
	return entry, nil
}

// rebuildMany resolves cardinality-many using add-wins semantics
func rebuildMany(key CacheKey, resolver CacheResolver, report *scanReport) (*CacheEntry, error) {
	var before int
	if report != nil {
		before = report.scanned
	}
	members, maxID, present, err := resolver.ResolveAddWins(key.E, key.A, report)
	if err != nil {
		return nil, fmt.Errorf("resolve add-wins set: %w", err)
	}
	if !present {
		return nil, nil
	}

	entry := &CacheEntry{
		version:     maxID,
		cardinality: schema.CardinalityMany,
		unique:      resolver.ResolvesUnique(key.A),
		manySet:     members,
	}
	if report != nil {
		entry.scanned = report.scanned - before
	}
	return entry, nil
}

// rebuildVector resolves cardinality-vector using RGA reconstruction
func rebuildVector(key CacheKey, resolver CacheResolver, report *scanReport) (*CacheEntry, error) {
	var before int
	if report != nil {
		before = report.scanned
	}
	elements, positions, maxID, present, err := resolver.ResolveRGA(key.E, key.A, report)
	if err != nil {
		return nil, fmt.Errorf("resolve RGA vector: %w", err)
	}
	if !present {
		return nil, nil
	}

	entry := &CacheEntry{
		version:     maxID,
		cardinality: schema.CardinalityVector,
		unique:      resolver.ResolvesUnique(key.A),
		vectorList:  elements,
		vectorIndex: positions,
	}
	if report != nil {
		entry.scanned = report.scanned - before
	}
	return entry, nil
}

// CacheResolver provides methods to resolve CRDT values from storage
// This interface decouples the cache from specific storage implementation
//
// Every resolve method accrues the index intake it spends into the report it is
// handed. Resolution reads the index on a pattern's behalf and emits no
// annotation of its own — one event per nested read would bury the query the
// stream exists to describe — so the count reaches its reporter through the
// report, and the cache stores the entry's own share on the CacheEntry, which is
// what the read bought.
type CacheResolver interface {
	// Every resolve method reports `present`: whether the (E, A) carries any
	// datom at all. It is not "did a value survive resolution" — a tombstoned
	// scalar, an all-removed set and an all-tombstoned vector are all present,
	// and each resolves to a state that is a value rather than an absence. Only
	// an (E, A) with no datoms is absent, and an absent one is never cached, so
	// entry existence is the presence fact everywhere downstream.
	//
	// It is reported rather than derived because nothing in the resolved value
	// carries it: zero members, an empty vector and a nil scalar all occur in
	// both states.

	// ResolvesUnique reports whether the resolver resolves a under the unique
	// rule, the entity's first assertion no other entity's later claim
	// supersedes. An entry built through the resolver carries the answer,
	// and the cache serves an entry to a reader whose answer is the same.
	ResolvesUnique(a Attribute) bool

	// ResolveLWW returns the current value for cardinality-one (highest ElementID wins)
	// Returns (value, maxElementID, present, error); intake accrues into the report.
	ResolveLWW(e Entity, a Attribute, report *scanReport) (any, datalog.ElementID, bool, error)

	// ResolveAddWins returns the current set members for cardinality-many,
	// each under its memberKey.
	// Returns (members, maxElementID, present, error); intake accrues into the report.
	ResolveAddWins(e Entity, a Attribute, report *scanReport) (map[string]any, datalog.ElementID, bool, error)

	// ResolveRGA returns the ordered vector for cardinality-vector
	// Returns (elements, positionIndex, maxElementID, present, error); intake
	// accrues into the report.
	ResolveRGA(e Entity, a Attribute, report *scanReport) ([]any, []datalog.ElementID, datalog.ElementID, bool, error)
}
