package storage

import (
	"fmt"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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
	cardinality datalog.Keyword

	// Resolved views (one populated based on cardinality)
	oneValue    any                 // Cardinality-One: single current value
	manySet     map[any]any         // Cardinality-Many: hashable key → original value
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
	// Per-(E,A) combined state: the resolved view and its max-version
	// high-water mark live in one slot, so the hot path — load, check
	// in-flight, check freshness — is a single trie walk.
	slots *cacheTrie

	// Per-entity attribute tracking - tracks which attributes we've cached per entity
	// This is NOT source of truth - just tracks what's in cache for difference-based
	// decisions on whether to do individual lookups vs full entity scan
	entityAttrs sync.Map // map[Entity]*sync.Map (inner: map[Attribute]struct{})
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{slots: newCacheTrie()}
}

// annotateRebuild reports one cache rebuild and why it happened. Callers
// check handler != nil before calling; the emission itself never guards.
//
// The handler arrives per call rather than being stored on the Cache. A stored
// copy would be a second home for a value the Database already owns, and an
// assignment to that one cannot reach this one — keeping them in step needs a
// method where a field would do. Every emission site here is inside
// GetOrResolve, which is handed the handler along with the resolver and the
// snapshot bound: per-call context, not retained state, so there is nothing to
// go stale.
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
				if _, loaded := c.slots.LoadOrStore(key, cacheSlot{entry: inFlightEntry}); !loaded {
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

// storeIfNotInFlight stores entry for key unless an in-flight sentinel is
// present, returning whether it stored. The compare-and-swap loop closes the
// window where a concurrent rebuild, having resolved the pre-commit value, could
// otherwise clobber a sentinel that BeginInFlight set in the meantime: if the
// slot turned into (or already holds) a sentinel, the store is abandoned.
// storeIfNotInFlight also advances the slot's max-version high-water mark to
// the entry's version, folding what the two-map layout did in a separate
// UpdateMaxVersion call into the same atomic slot swap.
func (c *Cache) storeIfNotInFlight(key CacheKey, entry *CacheEntry) bool {
	for {
		slot, ok := c.slots.Load(key)
		if ok && slot.entry != nil && slot.entry.inFlight {
			return false // a commit owns this key; do not cache
		}
		if ok && slot.entry != nil && slot.entry.version.Compare(entry.version) > 0 {
			// An existing fresher entry stands. A rebuild from an older
			// snapshot must never regress the cache: its entry would be
			// born stale (never served) and would evict a servable one.
			return false
		}
		if !ok {
			if _, loaded := c.slots.LoadOrStore(key, cacheSlot{entry: entry, version: entry.version}); !loaded {
				return true
			}
			continue // lost the race; re-check (the winner may be a sentinel)
		}
		version := slot.version
		if entry.version.Compare(version) > 0 {
			version = entry.version
		}
		if c.slots.CompareAndSwap(key, slot, cacheSlot{entry: entry, version: version}) {
			return true
		}
		// The slot changed under us (possibly to a sentinel); re-evaluate.
	}
}

// GetOrResolve returns the cached entry if fresh, rebuilding if stale.
//
// bound is the caller's snapshot high-water mark, following the matcher's
// *ElementID mode convention: nil reads latest (any fresh entry serves);
// non-nil serves a fresh entry only when the slot's version lies within the
// snapshot, so a sessioned query is never handed cache content newer than
// the state its scans observe. A bound-rejected lookup resolves through the
// caller's resolver — the session — instead.
//
// Freshness is the slot's own version bookkeeping, updated atomically on
// every write. This provides O(1) freshness checks without storage seeks.
// False negatives (returning stale data) are NOT acceptable.
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
func (c *Cache) GetOrResolve(key CacheKey, resolver CacheResolver, bound *datalog.ElementID, handler annotations.Handler, report *scanReport) (*CacheEntry, error) {
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
			entry, err := ResolveEntry(key, resolver, report)
			if err != nil {
				return nil, err
			}
			return entry, nil
		}

		// Fresh iff the entry's version matches the slot's high-water mark —
		// and, for a bounded read, the latest write lies within the snapshot,
		// which makes the fresh entry identical to the session's resolution.
		if slot.entry.version == slot.version {
			if bound == nil || slot.version.Compare(*bound) <= 0 {
				// Fresh - cache hit. No index was read to answer this call, so
				// the report accrues nothing.
				return slot.entry, nil
			}
			// Fresh but past the caller's snapshot — resolve through the
			// caller's session instead of serving the future.
			if handler != nil {
				annotateRebuild(handler, key, "snapshot-bound", slot)
			}
		} else {
			// Stale - fall through to rebuild
			if handler != nil {
				annotateRebuild(handler, key, "stale", slot)
			}
		}
	} else if handler != nil {
		annotateRebuild(handler, key, "absent", slot)
	}

	// Slow path: rebuild and store
	// Note: Two goroutines might both rebuild for the same key.
	// That's fine - CRDT resolution is deterministic, both compute same result.
	entry, err := ResolveEntry(key, resolver, report)
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
		// existence, and a placeholder entry here would put back the very
		// conflation the resolvers were changed to remove — a never-written
		// attribute would again be indistinguishable from an emptied one. The
		// cost is that the next read of the same absent (E, A) scans again.
		return nil, nil
	}
	// storeIfNotInFlight refuses to overwrite an in-flight sentinel, so a
	// rebuild that raced a commit doesn't re-cache the pre-commit value.
	// It also advances the slot's high-water mark to the entry's version.
	if c.storeIfNotInFlight(key, entry) {
		// Track that we've cached this attribute for this entity
		c.TrackEntityAttr(key.E, key.A)
	}
	return entry, nil
}

// UpdateMaxVersion updates the max ElementID for a (E,A) pair.
// Called by Transaction.Commit() for every written datom.
// This enables O(1) cache freshness checks without storage seeks.
func (c *Cache) UpdateMaxVersion(key CacheKey, elemID datalog.ElementID) {
	for {
		slot, ok := c.slots.Load(key)
		if !ok {
			if _, loaded := c.slots.LoadOrStore(key, cacheSlot{version: elemID}); !loaded {
				return
			}
			continue // lost the race; re-read and CAS
		}
		if slot.version.Compare(elemID) >= 0 {
			return // Current is already >= new value
		}
		if c.slots.CompareAndSwap(key, slot, cacheSlot{entry: slot.entry, version: elemID}) {
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

// InvalidateRewind drops cached state for keys whose datoms a rollback physically removed.
// Unlike Invalidate (forward commits, where maxVersions was just advanced and must be
// kept), a rewind RETREATS the version high-water, and UpdateMaxVersion is monotonic — so
// the per-(E,A) max must be dropped too, or a rebuilt lower-version entry would compare
// unequal to the stranded max forever and never cache-hit again. Pairs with BeginInFlight:
// that opens the uncached window before the delete, this closes it after.
func (c *Cache) InvalidateRewind(touched []CacheKey) {
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
// Also removes the per-(E, A) max-version entries for this attribute so
// the next read recomputes freshness from storage.
func (c *Cache) InvalidateAttribute(a Attribute) {
	c.slots.Range(func(key CacheKey, _ cacheSlot) bool {
		if key.A == a {
			c.slots.Delete(key)
		}
		return true
	})
}

// Clear removes all entries from the cache
// Useful for testing or forced cache invalidation
func (c *Cache) Clear() {
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
func (c *Cache) PopulateFromDatoms(key CacheKey, card datalog.Keyword, datoms []datalog.Datom) {
	// Skip if a commit to this (E, A) is in flight (the committer owns the cache
	// for this key), or if already cached and fresh.
	if slot, ok := c.slots.Load(key); ok && slot.entry != nil {
		if slot.entry.inFlight {
			return
		}
		if slot.entry.version == slot.version {
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
		// Refuse to cache if a commit grabbed this key after the freshness
		// check; the store advances the slot's high-water mark itself.
		if c.storeIfNotInFlight(key, entry) {
			c.TrackEntityAttr(key.E, key.A)
		}
	}
}

// ResolveEntry resolves the current value for (E, A) based on cardinality.
//
// It returns three outcomes, and they are three: an error, a non-nil entry, or
// a nil entry with a nil error meaning **this (E, A) has no datoms**. Absence
// is a resolved answer, not a failure and not an empty value, and it is never
// cached — so downstream, the existence of an entry *is* the existence of the
// attribute, and nothing has to reconstruct that from a count or a nil slice.
//
// Intake accrues into the report rather than travelling back, because the absent
// case has no entry to carry it and a failure has no return value at all, while
// both of them read the index to establish what they established.
//
// The two numbers are different — the caller's report spans however many entries
// it resolves, and the entry's is its own build cost — so the arms take the
// entry's as a delta across the resolver call rather than measuring into a
// second report. A report per rebuild would be a heap allocation on every cache
// miss to populate a field that exists to be reported; with a discarding report
// the delta is zero, which is the same statement the report itself makes.
//
// It takes no receiver: resolution reads storage through the resolver and
// touches no cache state. GetOrResolve is this plus the freshness check and the
// store; the cache-disabled read path calls it directly.
func ResolveEntry(key CacheKey, resolver CacheResolver, report *scanReport) (*CacheEntry, error) {
	card := resolver.GetCardinality(key.A)

	switch card {
	case schema.CardinalityOne:
		return rebuildOne(key, resolver, report)

	case schema.CardinalityMany:
		return rebuildMany(key, resolver, report)

	case schema.CardinalityVector:
		return rebuildVector(key, resolver, report)

	default:
		// Default to cardinality-one for schemaless
		return rebuildOne(key, resolver, report)
	}
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
//
// Inward rather than outward: a report passed in is accrued at scan time, so a
// resolution that fails keeps what it read, where a count returned alongside a
// result is lost with the result a failure never produces.
type CacheResolver interface {
	// GetCardinality returns the cardinality for an attribute
	GetCardinality(a Attribute) datalog.Keyword

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

	// ResolveLWW returns the current value for cardinality-one (highest ElementID wins)
	// Returns (value, maxElementID, present, error); intake accrues into the report.
	ResolveLWW(e Entity, a Attribute, report *scanReport) (any, datalog.ElementID, bool, error)

	// ResolveAddWins returns the current set members for cardinality-many
	// Returns (members, maxElementID, present, error); intake accrues into the report.
	ResolveAddWins(e Entity, a Attribute, report *scanReport) (map[any]any, datalog.ElementID, bool, error)

	// ResolveRGA returns the ordered vector for cardinality-vector
	// Returns (elements, positionIndex, maxElementID, present, error); intake
	// accrues into the report.
	ResolveRGA(e Entity, a Attribute, report *scanReport) ([]any, []datalog.ElementID, datalog.ElementID, bool, error)
}
