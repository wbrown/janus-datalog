# PullInto Returns Raw Historical Values Instead of CRDT-Resolved Values

## Summary

When using `PullInto` to load an entity struct, CardinalityOne attributes return `[]interface{}` (all historical values) instead of the resolved current value. The CRDT resolution layer is being bypassed.

## Reproduction

```go
// Schema defines :task/status as CardinalityOne (no .Many())
Attribute(":task/status").Type(schema.TypeKeyword).Add()

// Entity struct expects single value
type TaskEntity struct {
    ID     datalog.Identity `datalog:"db/id"`
    Status datalog.Keyword  `datalog:"task/status"`  // Single value
}

// Write status twice (simulating status transitions)
tx.Set(taskID, ":task/status", StatusPending)
tx.Commit()

tx2.Set(taskID, ":task/status", StatusComplete)
tx2.Commit()

// PullInto fails
var task TaskEntity
err := db.PullInto(taskID, &task)
// Error: field Status (attr task/status): expected Keyword, got []interface {}
```

## Expected Behavior

PullInto should use CRDT resolution based on cardinality:

| Cardinality | Storage | PullInto Returns |
|-------------|---------|------------------|
| One | All historical values | Single current value (LWW - highest ElementID) |
| Many | All add/remove ops | Resolved set (add-wins) |
| Vector | All RGA ops | Ordered slice (RGA reconstruction) |

## Actual Behavior

PullInto returns raw storage data (all historical values as `[]interface{}`) without applying CRDT resolution.

## Test Case

From `narrative-generators/pkg/db`:

```
=== RUN   TestIsTaskComplete_AfterInvalidation
    database_test.go:278: Failed to invalidate task: field Status (attr task/status): expected Keyword, got []interface {}
--- FAIL: TestIsTaskComplete_AfterInvalidation (0.04s)
```

The test writes multiple status values to the same task (pending → running → complete → invalidated), then tries to load the entity. PullInto returns the slice of all historical statuses instead of the current one.

---

## Detailed Analysis

### Code Path Overview

The PullInto call chain:

1. `Database.PullInto` (database.go:2364)
2. Creates matcher via `d.Matcher()` which sets schema and cache
3. Creates `PullExecutor` with that matcher
4. Calls `PullResolved` → `processResolvedSpec`
5. For `IsMany=false`, calls `lookupAttribute` (pull.go:385)
6. `lookupAttribute` checks for `EntityLookupMatcher` interface
7. `BadgerMatcher.LookupAttribute` is called (matcher.go:767)

### What Works: Cache Path

When the cache path is taken (`matchFromCache` in matcher_relations.go:527), CRDT resolution is applied correctly:

```go
case schema.CardinalityOne:
    val := entry.OneValue()
    if val == nil {
        return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
    }
    // Returns single-tuple relation with resolved value
    tuple := buildTuple(val, entry.Version())
    return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true
```

- CardinalityOne → returns `entry.OneValue()` (single value)
- CardinalityMany → returns resolved add-wins set
- CardinalityVector → returns RGA-reconstructed list

### What Breaks: Multiple Failure Modes

#### 1. Wildcard Pulls (`getAllAttributes`)

Location: `pull.go:237-301`

When E is bound but A is unbound (wildcard pull `[*]`), the code:
- Creates pattern `[entity, ?a, ?v]`
- Scans EAVT and returns ALL datoms including historical values
- No CRDT resolution is applied

Then in `processResolvedSpec` for `*query.ResolvedPullWildcard` (pull.go:390-409):

```go
for _, datom := range datoms {
    key := query.KeyName(datom.A)
    if existing, ok := result[key]; ok {
        // Attribute already seen - accumulate into slice (cardinality-many)
        switch v := existing.(type) {
        case []interface{}:
            result[key] = append(v, datom.V)
        default:
            result[key] = []interface{}{v, datom.V}
        }
    } else {
        result[key] = datom.V
    }
}
```

**Bug**: Blindly accumulates duplicate attributes into slices without checking cardinality or applying CRDT resolution.

#### 2. Cache Miss Fallback

When `matchFromCache` returns `handled=false` (cache miss), the code falls through to storage. The storage path in `matchUnboundAsRelation` (lines 229-312) sets CRDT flags like `returnOnlyFirst`, but these may not be consistently applied in all code paths.

#### 3. Potential Keyword Interning Issues

Schema lookup uses pointer equality for `datalog.Keyword` map keys:

```go
func (s *Schema) GetAttribute(attr datalog.Keyword) *AttributeDefinition {
    if def, ok := s.attrs[attr]; ok {
        return def
    }
    return nil
}
```

Since `datalog.Keyword` is `*keyword` (a pointer type), map lookup compares by pointer address. If the keyword passed to `GetAttribute` isn't the exact same interned pointer used when building the schema, the lookup fails silently and:

- `card` defaults to `CardinalityOne`
- But storage may still return multiple historical values if the fallback path doesn't apply CRDT resolution

### Why `LookupAttribute` May Return `[]interface{}`

In `BadgerMatcher.LookupAttribute` (matcher.go:767-910):

```go
// Determine cardinality for correct resolution
card := schema.CardinalityOne // default
if m.schema != nil {
    if def := m.schema.GetAttribute(attr); def != nil {
        card = def.Cardinality
    }
}

// Try cache first...
if m.cache != nil && m.txID == 0 {
    entry := m.cache.GetOrResolve(key, m)
    if entry != nil {
        switch card {
        case schema.CardinalityOne:
            return entry.OneValue(), true  // Single value ✓
        case schema.CardinalityMany:
            return result, true  // []interface{} - multiple values
        case schema.CardinalityVector:
            return list, true    // []any - multiple values
        }
    }
}
```

If the schema lookup fails (returns nil), `card` stays as `CardinalityOne`. But if the cache entry was built with a different cardinality (due to `GetCardinality` succeeding via `decodeAttribute`), there's a mismatch.

### The Fundamental Problem

**CRDT resolution is inconsistent across code paths:**

| Code Path | CRDT Resolution | Notes |
|-----------|-----------------|-------|
| `matchFromCache` | ✓ Correct | Uses cache entry's resolved values |
| `LookupAttribute` cache hit | ✓ Correct | Returns `entry.OneValue()` etc. |
| `LookupAttribute` storage fallback | ⚠ Partial | Uses `returnOnlyFirst` but may miss edge cases |
| `getAllAttributes` (wildcard) | ✗ Broken | Returns all historical datoms |
| `lookupAllValues` | ⚠ Depends | Goes through `Match` which may or may not resolve |

---

## Suggested Fix

### Option 1: Direct Cache Access in PullExecutor (Recommended)

PullExecutor should directly use the EA cache rather than going through the Match interface:

```go
// New interface for cache-aware attribute lookup
type CacheAwareLookup interface {
    LookupResolved(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool)
}

// In PullExecutor.lookupAttribute:
if cacheLookup, ok := pe.matcher.(CacheAwareLookup); ok {
    return cacheLookup.LookupResolved(entity, attr)
}
```

Implementation in BadgerMatcher:

```go
func (m *BadgerMatcher) LookupResolved(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
    if m.cache == nil {
        return m.LookupAttribute(entity, attr) // Fallback
    }

    key := buildCacheKey(entity, attr)
    entry := m.cache.GetOrResolve(key, m)
    if entry == nil {
        return nil, false
    }

    switch entry.Cardinality() {
    case schema.CardinalityOne:
        return entry.OneValue(), entry.OneValue() != nil
    case schema.CardinalityMany:
        return sliceFromSet(entry.ManySet()), len(entry.ManySet()) > 0
    case schema.CardinalityVector:
        return entry.VectorList(), len(entry.VectorList()) > 0
    }
    return nil, false
}
```

### Option 2: Fix Wildcard Pull Path

Update `getAllAttributes` to apply CRDT resolution:

```go
func (pe *PullExecutor) getAllAttributes(entity datalog.Identity) ([]datalog.Datom, error) {
    // Get unique attributes for entity
    attrs := pe.getEntityAttributes(entity)

    var datoms []datalog.Datom
    for _, attr := range attrs {
        // Use cache-aware lookup for each attribute
        if val, ok := pe.lookupAttribute(entity, attr); ok {
            datoms = append(datoms, datalog.Datom{E: entity, A: attr, V: val})
        }
    }
    return datoms, nil
}
```

### Required Changes

1. **PullExecutor** needs access to cache or a cache-aware lookup interface
2. **BadgerMatcher** needs a new method that guarantees CRDT resolution
3. **Wildcard pull** needs to iterate attributes and resolve each one
4. **Add tests** for PullInto with multiple writes to same attribute

---

## Option 3: Entity-Level Cache Integration (Recommended)

The above options fix the immediate bug but don't address the fundamental design tension between scan efficiency and CRDT resolution.

### The Design Tension

PullInto's efficiency comes from its single EAVT scan to get all attributes for an entity. But this bypasses the EA cache which is keyed by (Entity, Attribute) pairs.

**Current state:**

| Operation | Index | Warms Cache | CRDT Resolution |
|-----------|-------|-------------|-----------------|
| `WarmCache(attrs)` | AEVT | ✓ All entities for given attrs | ✓ via GetOrResolve |
| `getAllAttributes(entity)` | EAVT | ✗ | ✗ Returns raw datoms |
| `LookupAttribute(e, a)` | Per (E,A) | ✓ On demand | ✓ via cache |

**The problem:** PullInto for an entity with 10 attributes has two bad options:

1. **Scan path** (`getAllAttributes`): 1 seek, no CRDT resolution → **broken**
2. **Per-attribute path**: 10 cache lookups, each potentially hitting storage → **N+1 problem**

### The Solution: `ResolveEntityAttributes`

Add entity-level cache warming that preserves scan efficiency:

```
Current WarmCache:  AEVT scan → warms all entities for specific attributes
New method needed:  EAVT scan → warms all attributes for specific entity
```

### Current Flow (Broken)

```
PullInto(entity)
  → getAllAttributes(entity)
    → EAVT scan → raw datoms (includes historical values)
    → processResolvedSpec accumulates duplicates into []interface{}
    → ReadStructWithID fails: "expected Keyword, got []interface{}"
```

### Proposed Flow

```
PullInto(entity)
  → ResolveEntityAttributes(entity)  // NEW
    → EAVT scan → group by attribute
    → For each unique (E, A): cache.GetOrResolve(key, resolver)
    → Return map[Keyword]interface{} with CRDT-resolved values
  → ReadStructWithID succeeds
```

### Implementation

**Extend the cache** to track which attributes we've cached per entity:

```go
// In storage/cache.go - add to Cache struct

type Cache struct {
    entries      sync.Map // map[CacheKey]*CacheEntry      (existing)
    maxVersions  sync.Map // map[CacheKey]datalog.ElementID (existing)
    attrVersions sync.Map // map[Attribute]datalog.ElementID (existing)

    // NEW: track which attributes we've cached per entity
    // This is NOT source of truth - just tracks what's in cache
    entityAttrs  sync.Map // map[Entity]map[Attribute]bool
}

// Update entityAttrs whenever we cache an (E, A) entry
func (c *Cache) trackEntityAttr(key CacheKey) {
    attrs, _ := c.entityAttrs.LoadOrStore(key.E, make(map[Attribute]bool))
    attrs.(map[Attribute]bool)[key.A] = true
}

func (c *Cache) GetCachedAttrs(e Entity) map[Attribute]bool {
    if attrs, ok := c.entityAttrs.Load(e); ok {
        return attrs.(map[Attribute]bool)
    }
    return nil
}
```

**PullInto flow - smart decision between individual lookups vs scan:**

```go
const scanThreshold = 5 // if more than N attrs missing, scan is cheaper

func (d *Database) ResolveEntityAttributes(entity datalog.Identity, needed []datalog.Keyword) (map[datalog.Keyword]interface{}, error) {
    eBytes := Entity(entity.Hash())
    matcher := d.Matcher().(*BadgerMatcher)
    result := make(map[datalog.Keyword]interface{})

    // What do we have cached for this entity?
    cachedAttrs := d.cache.GetCachedAttrs(eBytes)

    // Partition needed attrs into cached vs missing
    var missing []datalog.Keyword
    for _, kw := range needed {
        aBytes := toAttribute(kw)
        if cachedAttrs != nil && cachedAttrs[aBytes] {
            // Already cached - GetOrResolve (freshness check, usually cache hit)
            key := CacheKey{E: eBytes, A: aBytes}
            if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
                result[kw] = entryToValue(entry)
            }
        } else {
            missing = append(missing, kw)
        }
    }

    if len(missing) == 0 {
        return result, nil // All cached
    }

    // Decision: individual lookups vs full scan
    if len(missing) <= scanThreshold {
        // Few missing - individual GetOrResolve calls
        for _, kw := range missing {
            aBytes := toAttribute(kw)
            key := CacheKey{E: eBytes, A: aBytes}
            if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
                result[kw] = entryToValue(entry)
            }
        }
    } else {
        // Many missing - full EAVT scan is cheaper
        scanned, err := d.scanAndResolveEntity(entity)
        if err != nil {
            return nil, err
        }
        // Merge scanned results (only take what we need)
        for _, kw := range missing {
            if val, ok := scanned[kw]; ok {
                result[kw] = val
            }
        }
    }

    return result, nil
}

func (d *Database) scanAndResolveEntity(entity datalog.Identity) (map[datalog.Keyword]interface{}, error) {
    eBytes := Entity(entity.Hash())
    matcher := d.Matcher().(*BadgerMatcher)

    // ONE EAVT scan for this entity
    prefix := buildEAVTPrefix(eBytes)
    iter, err := d.store.Scan(EAVT, prefix, prefixEnd(prefix))
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    // Group datoms by attribute
    attrDatoms := make(map[Attribute][]datalog.Datom)
    for iter.Next() {
        datom, _ := iter.Datom()
        attr := toAttribute(datom.A)
        attrDatoms[attr] = append(attrDatoms[attr], *datom)
    }

    // Resolve each attribute using factored resolution logic
    result := make(map[datalog.Keyword]interface{})
    for attr, datoms := range attrDatoms {
        kw := datalog.NewKeyword(decodeAttribute(attr))
        card := d.getCardinality(kw)

        entry := resolveFromDatoms(datoms, card)

        // Populate cache
        key := CacheKey{E: eBytes, A: attr}
        d.cache.Store(key, entry)
        d.cache.UpdateMaxVersion(key, entry.version)
        d.cache.trackEntityAttr(key)

        result[kw] = entryToValue(entry)
    }

    return result, nil
}
```

**Factor the resolution logic** so it can be used by both `GetOrResolve` (single E,A scan) and `scanAndResolveEntity` (batch from EAVT scan):

```go
// In storage/cache_resolver.go

// resolveFromDatoms applies CRDT resolution to a slice of datoms
// Used by both single-attribute resolution and batch entity resolution
func resolveFromDatoms(datoms []datalog.Datom, card schema.Cardinality) *CacheEntry {
    switch card {
    case schema.CardinalityOne:
        return resolveLWWFromDatoms(datoms)
    case schema.CardinalityMany:
        return resolveAddWinsFromDatoms(datoms)
    case schema.CardinalityVector:
        return resolveRGAFromDatoms(datoms)
    }
    return nil
}

// Existing ResolveLWW can call this after scanning
func (m *BadgerMatcher) ResolveLWW(e Entity, a Attribute) (any, datalog.ElementID, error) {
    datoms, err := m.scanEATVForDatoms(e, a)
    if err != nil {
        return nil, datalog.ElementID{}, err
    }
    entry := resolveLWWFromDatoms(datoms)
    return entry.oneValue, entry.version, nil
}
```

### Integration with PullInto

```go
// In storage/database.go - updated PullInto

func (d *Database) PullInto(entityID datalog.Identity, v interface{}) error {
    // For simple struct pulls, use the optimized path
    if d.cache != nil {
        resolved, err := d.ResolveEntityAttributes(entityID)
        if err != nil {
            return err
        }
        return dlreflect.ReadStructWithID(resolved, v, d.schema, entityID)
    }

    // Fallback to existing pull executor path for complex patterns
    // ... existing implementation ...
}
```

### Benefits

| Scenario | Current (`getAllAttributes`) | Proposed (`ResolveEntityAttributes`) |
|----------|------------------------------|--------------------------------------|
| All attrs cached | 1 scan, no resolution | 0 scans, N cache hits |
| Few attrs missing | 1 scan, no resolution | K individual `GetOrResolve` calls |
| Many attrs missing | 1 scan, no resolution | 1 scan + resolve + cache |
| CRDT resolution | ✗ | ✓ |
| Handles history | ✗ (returns all values) | ✓ (resolves to current) |

The heuristic (`scanThreshold`) balances individual lookup cost vs scan overhead.

### Implementation Notes

1. **Storage is source of truth**: `entityAttrs` only tracks what's *in the cache*, not what exists in storage. New attributes can appear at any time; the cache doesn't need to know about them until requested.

2. **Factor resolution logic**: Extract `resolveFromDatoms(datoms, cardinality)` for use by both `GetOrResolve` (single E,A scan) and `scanAndResolveEntity` (batch EAVT scan).

3. **Threshold tuning**: `scanThreshold` should be tuned based on typical entity size and storage latency. Start with ~5, measure, adjust.

4. **entityAttrs concurrency**: The inner `map[Attribute]bool` needs synchronization. Consider `sync.Map` or per-entity mutex.

5. **Wildcard pulls**: Always require a scan since we don't know which attributes exist. Can't optimize with `entityAttrs` alone.

### Why This Is Better Than Options 1 & 2

**Option 1** (per-attribute cache lookup) always does individual lookups - misses opportunity to batch when many attrs are missing.

**Option 2** (fix wildcard processing) still requires full scan every time, doesn't leverage cache.

**Option 3** (smart cache with difference-based decision):
- Tracks what's cached per entity (`entityAttrs`)
- Computes difference: needed - cached = missing
- Small difference → individual `GetOrResolve` (avoids scan overhead)
- Large difference → one scan cheaper than N lookups
- Storage remains source of truth
- Cache is pure optimization layer

### Additional Integration Points

1. **PullExecutor.getAllAttributes** → delegate to `ResolveEntityAttributes` when available
2. **Wildcard pull** → use resolved map instead of raw datoms
3. **PullIntoMany** → batch entity resolution with shared scan patterns

---

## Impact

Any code using PullInto/SaveStruct with entities that have been updated (multiple writes to same attribute) will fail after upgrading to CRDT storage.

### Affected APIs

- `Database.PullInto`
- `Database.PullIntoMany`
- Any code using wildcard pulls `[*]`
- Any code relying on `LookupAttribute` for cardinality-one attributes with history

### Workaround

Until fixed, users can query attributes directly using Datalog queries which properly use the cache resolution path:

```go
// Instead of:
db.PullInto(taskID, &task)

// Use:
result, _ := db.Query(`[:find ?status :where [?e :task/status ?status]]`,
    datalog.Inputs{"?e": taskID})
task.Status = result[0][0].(datalog.Keyword)
```

---

## Additional Finding: Pull Expressions in Queries

**Standalone `PullInto` works correctly. Pull expressions `(pull ?e [*])` in queries do not.**

### Test

See: `datalog/storage/pull_expression_crdt_test.go`

```go
// Write status 3 times via SaveStruct
task.Status = ":status/pending"
tx.SaveStruct(task)

task.Status = ":status/running"
tx2.SaveStruct(task)

task.Status = ":status/complete"
tx3.SaveStruct(task)

// Standalone PullInto - WORKS
var loaded TaskEntity
db.PullInto(task.ID, &loaded)  // loaded.Status == :status/complete ✓

// Pull expression in query - BROKEN
db.QueryOneInto(&loaded,
    `[:find (pull ?e [*]) :where [?e :task/name "test-task"]]`)
// Error: field Status (attr task/status): expected Keyword, got []interface {}
```

### Test Output

```
=== RUN   TestPullInto_CardinalityOne_MultipleWrites
--- PASS: TestPullInto_CardinalityOne_MultipleWrites (0.04s)
=== RUN   TestPullExpression_CardinalityOne_MultipleWrites
    pull_expression_crdt_test.go:160: QueryOneInto with pull expression failed: field Status (attr task/status): expected Keyword, got []interface {}
--- FAIL: TestPullExpression_CardinalityOne_MultipleWrites (0.03s)
```

### Implication

The CRDT resolution path differs between:
- `db.PullInto(id, &struct)` → uses cache, resolves correctly
- `[:find (pull ?e [*]) ...]` → bypasses cache, returns raw historical values

---

## Resolution

**Fixed in commit: [pending]**

### Root Causes

Two separate bugs were identified and fixed:

#### Bug 1: `ResolveAllAttributes` always scanned storage

`ResolveAllAttributes` (used by wildcard pulls) always performed a full EAVT scan, ignoring the difference-based logic in `ResolveEntityAttributes` that checks the cache first.

**Fix:** When schema exists, `ResolveAllAttributes` now delegates to `ResolveEntityAttributes` with all schema attributes. This uses the cache when data is already cached, only scanning when necessary.

```go
// datalog/storage/database.go
func (d *Database) ResolveAllAttributes(entity datalog.Identity) (map[datalog.Keyword]interface{}, error) {
    // If schema exists, delegate to ResolveEntityAttributes with all schema attrs.
    // This uses difference-based logic: check cache first, scan only if needed.
    if d.schema != nil {
        if s, ok := d.schema.(*schema.Schema); ok && s.HasSchema() {
            attrs := s.Attributes()
            keywords := make([]datalog.Keyword, len(attrs))
            for i, def := range attrs {
                keywords[i] = def.Ident
            }
            return d.ResolveEntityAttributes(entity, keywords)
        }
    }
    // No schema: must scan EAVT to discover all attributes
    // ... existing scan logic ...
}
```

#### Bug 2: `entityResolver` not propagated to temporary executor

In `Executor.ExecuteWithRelations`, a temporary executor was created for annotation wrapping, but it did not copy the `entityResolver` field. This caused pull expressions in queries to have a nil `entityResolver`, bypassing CRDT resolution.

**Fix:** Added `entityResolver` to the temporary executor struct initialization.

```go
// datalog/executor/executor.go - ExecuteWithRelations
executor := &Executor{
    matcher:                  matcher,
    entityResolver:           e.entityResolver,  // WAS MISSING
    planner:                  e.planner,
    options:                  e.options,
    enableParallelSubqueries: e.enableParallelSubqueries,
    maxSubqueryWorkers:       e.maxSubqueryWorkers,
}
```

#### Bug 3: `entryToValue` returned raw map for CardinalityMany

`entryToValue` returned `map[any]bool` for CardinalityMany attributes instead of converting to `[]interface{}`, breaking API consistency with Pull.

**Fix:** Convert set to slice in `entryToValue`:

```go
case schema.CardinalityMany:
    set := entry.ManySet()
    if len(set) == 0 {
        return nil
    }
    values := make([]interface{}, 0, len(set))
    for v := range set {
        values = append(values, v)
    }
    return values
```

### Tests

All tests now pass:
- `TestBugVerification_PullInto_CardinalityOne` - Original bug repro
- `TestPullExpression_CardinalityOne_MultipleWrites` - Pull expressions in queries
- `TestPull_Wildcard_CRDTResolution` - Wildcard with CardinalityOne and CardinalityMany
- `TestPull_Wildcard_CardinalityVector` - Wildcard with ordered vector (new test)
- `TestPullInto_CardinalityVector_RGA` - Vector ordering preserved
