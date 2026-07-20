# BUG: `AsOf()` wildcard pull can return latest-state values via the shared EA cache

**Date**: 2026-04-17 **Severity**: High - temporal reads can return newer data than the requested snapshot **Status**: Resolved (2026-05-22) **Affected**: `storage.Database.AsOf()`, `storage.Database.ResolveAllAttributes()`, `storage.Database.ResolveEntityAttributes()`, wildcard `Pull("[*]")`

## Summary

`db.AsOf(tx).Pull(entity, "[*]")` does not reliably read the database "as of" `tx`.

Wildcard pull goes through:

```
Pull("[*]")
  -> executor.PullExecutor.processSpec(PullWildcard)
  -> EntityResolver.ResolveAllAttributes()
  -> ResolveEntityAttributes()
  -> EA cache GetOrResolve(...)
```

That path is different from explicit attribute lookup (`Pull` with explicit attrs, `PullInto`, `get-else`, `missing?`, `get-some`), which goes through `BadgerMatcher.LookupAttribute()` and explicitly bypasses the cache for temporal matchers.

This creates two related bugs:

1. **Warm-cache leak**: `AsOf()` shares the parent's EA cache, and cache keys are only `(E, A)`. If the latest-state cache entry is already warm, `AsOf()` wildcard pull can reuse it unchanged.
2. **Cold-cache rebuild still ignores `AsOf`**: even on a cache miss, `ResolveLWW()` and `resolveAddWinsSet()` do not apply `shouldFilterTx()`, so the rebuilt cache entry is still resolved against the latest state, not the as-of state.

The result is that the same `AsOf()` handle can disagree with itself:

- `db.AsOf(tx).Pull(entity, "[*]")` can return latest-state values
- `db.AsOf(tx).Pull(entity, "[:user/name]")` can return the correct historical value

This is separate from `BUG_TEMPORAL_DATABASE_HANDLES_ARE_SHALLOW.md`. That bug is about handle lifecycle and API footguns. This one is about wildcard temporal reads returning the wrong snapshot.

## Reproduction

Minimal cardinality-one repro:

```go
// T1
tx := db.NewTransaction()
tx.Set(alice, datalog.NewKeyword(":user/name"), "Alice")
tx1, _ := tx.Commit()

// T2
tx = db.NewTransaction()
tx.Set(alice, datalog.NewKeyword(":user/name"), "Bob")
_, _ = tx.Commit()

// Warm the latest-state cache
latest, _ := db.Pull(alice, `[*]`)
// latest["user/name"] == "Bob"

// Read the old snapshot
asOf := db.AsOf(tx1)

wildcard, _ := asOf.Pull(alice, `[*]`)
explicit, _ := asOf.Pull(alice, `[:user/name]`)
```

Expected:

- `wildcard["user/name"] == "Alice"`
- `explicit["user/name"] == "Alice"`

Actual from the current code path:

- `explicit` goes through `LookupAttribute()` and respects `shouldFilterTx()`
- `wildcard` can reuse the warm latest-state cache entry and return `"Bob"`

The same shape exists for cardinality-many attributes:

```go
// T1: Add "red"
// T2: Add "blue"
// Warm cache from latest -> {"red", "blue"}
// db.AsOf(T1).Pull(e, "[*]") should show only {"red"}
// but wildcard pull can still return {"red", "blue"}
```

## Code Evidence

### 1. `AsOf()` shares the parent's cache

From `datalog/storage/database.go`:

```go
func (d *Database) AsOf(txID datalog.ElementID) *Database {
    return &Database{
        store:        d.store,
        schema:       d.schema,
        planCache:    d.planCache,
        cache:        d.cache,   // <- shared pointer
        temporalTxID: &txID,
    }
}
```

The temporal handle is not getting an as-of-aware cache. It is reusing the latest-state cache directly.

### 2. Wildcard pull goes through `ResolveAllAttributes()`

From `datalog/executor/pull.go`:

```go
case *query.PullWildcard:
    if pe.entityResolver != nil {
        resolved, err := pe.entityResolver.ResolveAllAttributes(entity)
        // ...
    }
```

Explicit pull attributes do **not** use this path. They call `lookupAttribute()` instead, which delegates to `LookupAttribute()`.

### 3. `ResolveEntityAttributes()` still uses cache on temporal handles

From `datalog/storage/database.go`:

```go
matcher := d.Matcher().(*BadgerMatcher)

// ...
if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
    if val := entryToValue(entry, getValueType(kw)); val != nil {
        result[kw] = val
    }
}
```

There is no `temporalTxID` guard here. `AsOf()` handles still call `GetOrResolve(...)`.

### 4. Cache freshness has no temporal dimension

From `datalog/storage/cache.go`:

```go
type CacheKey struct {
    E Entity
    A Attribute
}

func (c *Cache) GetOrResolve(key CacheKey, resolver CacheResolver) *CacheEntry {
    if val, ok := c.entries.Load(key); ok {
        entry := val.(*CacheEntry)
        if maxVal, ok := c.maxVersions.Load(key); ok {
            currentMax := maxVal.(datalog.ElementID)
            if entry.version == currentMax {
                return entry // <- "fresh" means "latest", not "correct for this tx"
            }
        }
    }
    entry := c.rebuild(key, resolver)
    // ...
}
```

If the latest `(E, A)` cache entry is fresh relative to the latest max version, the temporal read never invokes the resolver at all.

### 5. Explicit attribute lookup already knows temporal reads must bypass cache

From `datalog/storage/matcher.go`:

```go
// Try cache first for O(1) access (only for latest state, not as-of queries)
if m.cache != nil && m.txID == nil {
    // cache path
}

// Fallback to storage scan (for as-of queries or when cache is not set)
if card == schema.CardinalityOne {
    // ...
    if m.shouldFilterTx(datom.Tx) {
        continue
    }
}
```

This is the key inconsistency. The direct lookup path already treats temporal reads as a special semantic case. The wildcard/entity-resolution path does not.

### 6. Cold-cache rebuild is also latest-state only

From `datalog/storage/cache_resolver.go`:

```go
func (m *BadgerMatcher) ResolveLWW(e Entity, a Attribute) (any, datalog.ElementID, error) {
    iter, err := m.store.Scan(EATV, prefix, prefixEnd(prefix))
    // ...
    if iter.Next() {
        datom, err := iter.Datom()
        // ...
        if datom.Op == datalog.OpCRDTRemove {
            return nil, datom.Tx, nil
        }
        return datom.V, datom.Tx, nil
    }
}
```

And from `datalog/storage/set_resolution.go`:

```go
for iter.Next() {
    datom, err := iter.Datom()
    // ...
    // no shouldFilterTx(datom.Tx) check here
}
```

Neither cache rebuild path applies temporal filtering. Even with a cold cache, the wildcard path still resolves against latest state.

## Why This Is A Correctness Bug

`AsOf()` is not just a performance hint. It is a semantic promise:

- values newer than the target `ElementID` must be invisible
- different read APIs on the same `AsOf()` handle must agree on the same snapshot

Right now the API can become **order-dependent**:

1. read latest state and warm cache
2. call `AsOf(tx).Pull("[*]")`
3. get a different answer than if the `AsOf()` read had happened first

That is a snapshot-isolation bug, not just an optimization bug.

## Impact

### 1. Wildcard pull can disagree with explicit pull

On the same `AsOf()` handle:

- `Pull("[*]")` can return latest-state data
- `Pull("[:attr]")` can return correct historical data

That makes the API internally inconsistent.

### 2. `AsOf()` behavior becomes cache-order dependent

Whether a temporal read is correct depends on whether the latest-state cache was already populated.

### 3. Cardinality-many and unique attributes are especially risky

For cardinality-many, the cache rebuild path uses `resolveAddWinsSet()` with no tx filtering. For unique cardinality-one, the wildcard path can disagree with the direct lookup path's history/as-of semantics even more sharply.

### 4. This is easy to miss in testing

Existing temporal tests tend to exercise:

- explicit attribute pull paths
- `PullInto`
- cold read paths without a warmed latest-state cache

Those are not the failing path.

## Test Gap

There are already tests around temporal pull behavior, but the critical gap is:

1. warm the cache from the latest handle
2. read via `AsOf()`
3. compare wildcard pull vs explicit pull

Without that sequence, the shared-cache bug does not surface.

## Possible Fix Directions

### Option 1: Disable EA cache usage for temporal `ResolveEntityAttributes()`

If `temporalTxID != nil`, skip `GetOrResolve(...)` entirely and resolve via the matcher's temporal-aware lookup path for each attribute.

This is the simplest correctness fix.

### Option 2: Add temporal dimension to cache keys

Make cache entries snapshot-aware instead of keying only by `(E, A)`.

This is more complex and may not be worth it unless temporal wildcard pulls are a hot path.

### Option 3: Make cache rebuild functions tx-aware

`ResolveLWW()` and `resolveAddWinsSet()` would need to apply temporal filtering using the matcher's `txID`.

Even with that change, warm-cache reuse would still be wrong unless the cache key or freshness model also changes.

### Option 4: Route wildcard pull through the same temporal semantics as explicit pull

Instead of resolving through `ResolveAllAttributes()` + EA cache, wildcard pull on temporal handles could enumerate attrs and call the same temporal-aware attribute lookup used by explicit pull.

## Test Plan

1. `TestAsOfWildcardPull_DoesNotReuseLatestCache`
   - warm latest cache with `db.Pull(e, "[*]")`
   - assert `db.AsOf(tx1).Pull(e, "[*]")` returns `tx1` view, not latest

2. `TestAsOfWildcardPull_AgreesWithExplicitPull`
   - compare `db.AsOf(tx).Pull(e, "[*]")` vs `db.AsOf(tx).Pull(e, "[:attr]")`

3. `TestAsOfResolveEntityAttributes_ColdCacheFiltersTx`
   - disable warm-cache leak and prove cold rebuild still respects `AsOf`

4. `TestAsOfWildcardPull_CardinalityMany`
   - add value at `T1`, add second value at `T2`
   - as-of `T1` wildcard pull must only return the first set member

5. `TestHistoryWildcardPull_DoesNotReuseLatestCache`
   - if wildcard history semantics remain supported, verify they do not reuse a
     latest-state cache entry from the parent handle

## Resolution (2026-05-22)

Both sub-bugs are fixed. The two problems were independent and each needed its own change.

### Sub-bug 2 (cold-cache rebuild ignored AsOf): make rebuild tx-aware

The cache-rebuild resolvers now apply the matcher's `shouldFilterTx()` so a cold rebuild on a temporal matcher resolves against the requested snapshot, not the latest state:

- `BadgerMatcher.ResolveLWW()` (`cache_resolver.go`) — the EATV first-entry scan is now a loop that skips datoms with `Tx` after the as-of target, so the first *visible* entry wins (cardinality-one).
- `BadgerMatcher.resolveAddWinsSet()` (`set_resolution.go`) — skips filtered datoms before folding the add-wins set (cardinality-many).

The other two resolution paths already filtered correctly and were left untouched: `loadRGAElements()` (cardinality-vector, `vector_resolution.go:87`) and the unique-attribute walk `walkUniqueEntityValue()` / `resolveMaxOtherTxForValue()` (`unique_resolve.go`).

`PrefetchEntities()` (`prefetch.go`) now also applies `shouldFilterTx()` and returns early in history mode, so warming never seeds latest-state datoms into a temporal cache.

### Sub-bug 1 (warm-cache leak): temporal handles do not share the global cache

Rather than add a temporal dimension to the shared cache key (the report's Option 2), we bound the as-of cache to the handle lifetime:

- `Database.AsOf()` (`database.go`) now allocates its **own** `NewCache()` instead of sharing the parent's `cache` pointer. The snapshot is immutable, so this private cache fills lazily, never needs invalidation, and is freed when the handle is garbage-collected. A warm latest-state entry in the parent cache can no longer leak into an as-of read, because they are different `Cache` instances.
- `CacheKey` stays `{E, A}` — no per-snapshot `Tx` dimension is needed, because each snapshot already has its own `Cache` instance. (An interim fix that added `AsOf bool` + `Tx` fields to the key was reverted in favor of this approach, because per-snapshot keys accumulated in the shared map forever with no eviction.)
- `BadgerMatcher.cacheKey()` (`matcher.go`) centralizes the policy: history mode returns `ok=false` (raw datoms are never cached); latest and concrete as-of modes both return an `{E, A}` key, kept apart by living in separate `Cache` instances.

### Routing changes so as-of/explicit/wildcard agree

- The cache guards in `matcher.go` and `matcher_relations.go` changed from `m.txID == nil` (latest only) to `!m.isHistoryMode()`. Concrete as-of reads now use their handle's private cache (with the tx-aware rebuild above); history reads bypass the cache entirely.
- `Database.ResolveEntityAttributes()` (`database.go`) routes history mode (and `DisableCache`) through the direct, cache-less matcher path; concrete as-of goes through the handle's private cache. Wildcard `Pull("[*]")` and explicit `Pull("[:attr]")` now resolve through the same temporal-aware logic and agree.

### Why this fixes the order-dependence

The bug made `AsOf()` results depend on whether the latest-state cache was warm. With a private per-handle cache plus tx-aware rebuild, an `AsOf(tx)` handle's answer is independent of the parent's cache state: it never reads the parent cache, and its own cache is always resolved against `tx`.

### Tests

`datalog/storage/asof_wildcard_pull_test.go`:

- `TestAsOfWildcardPullUsesSnapshot` — warms the latest cache (`"Bob"`), then on an `AsOf(tx1)` handle asserts both explicit and wildcard pull return `"Alice"`.
- `TestAsOfWildcardPullUsesSnapshotForCardinalityMany` — `T1` adds `"red"`, `T2` adds `"blue"`; latest wildcard pull returns `{red, blue}`, `AsOf(T1)` wildcard pull returns only `{red}` (exercises the `resolveAddWinsSet` filter).

Full suite green (15 packages, 0 failures).

### Files changed

`cache.go`, `cache_resolver.go`, `set_resolution.go`, `prefetch.go`, `matcher.go`, `matcher_relations.go`, `database.go`, plus the new test file.
