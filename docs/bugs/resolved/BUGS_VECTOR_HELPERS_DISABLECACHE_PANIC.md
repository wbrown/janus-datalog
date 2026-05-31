# BUG: Vector Cache Helper APIs Panic When `DisableCache` Is Set

**Date**: 2026-05-31
**Severity**: Correctness / Stability (Medium)
**Status**: ✅ RESOLVED (2026-05-31)
**Affected**: `Database.GetVectorNth`, `Database.GetVectorLength`; possibly other cache-specific helper paths

## Resolution

Both methods now branch on `d.cache`: when it is non-nil they keep the cache
path (`d.cache.GetOrResolve`); when it is nil (`DisableCache: true`) they resolve
through `ResolveEntry(key, matcher)` — the existing cache-less resolver, which
returns the same `*CacheEntry` shape and determines cardinality the same way
(`GetCardinality` over `d.schema`). All downstream logic — the
`Cardinality() != Vector` check, `VectorList()` indexing/length — is unchanged,
so cache-disabled results are identical to cache-enabled, just resolved directly
from storage.

This works because cardinality is available without the cache: a supplied schema
provides it, and a schemaless open reconstructs it into `d.schema` at open
(`inferSchemaFromStore`, independent of the cache setting). The helper's matcher
carries `d.schema`, so `GetCardinality` returns the correct cardinality in both
modes.

Regression coverage in `datalog/storage/get_vector_cache_modes_test.go` runs
every case under both cache modes (the cache-disabled subtests panicked against
the unfixed code): `GetVectorNth` and `GetVectorLength` happy paths,
out-of-bounds, missing vector (nil/0), and a non-vector attribute (returns the
"not a vector" error rather than panicking).

## Summary

The database supports `DisableCache: true`, and the architecture explicitly
treats the EA cache as an optimization rather than a correctness requirement.
Most query and pull paths now work in both cache-enabled and cache-disabled
modes.

The vector-specific helper APIs still dereference `d.cache` unconditionally.
When a database is opened with `DisableCache: true`, `d.cache` is nil and these
methods can panic.

This is the same bug class previously fixed for `ResolveEntityAttributes`, but
the vector helper APIs were not covered by that fix.

## Root Cause

`NewDatabaseWithOptions` sets `cache` only when cache is enabled:

```go
var cache *Cache
if !opts.DisableCache {
    cache = NewCache()
}
```

`GetVectorNth` assumes the cache exists:

```go
entry := d.cache.GetOrResolve(key, matcher)
if entry == nil {
    return nil, nil
}
```

`GetVectorLength` has the same pattern:

```go
entry := d.cache.GetOrResolve(key, matcher)
if entry == nil {
    return 0, nil
}
```

With `DisableCache: true`, both call through a nil pointer.

## Reproduction Sketch

```go
db, _ := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:         dir,
    Schema:       vectorSchema,
    DisableCache: true,
})

e := datalog.NewIdentity("character:alice")
skills := datalog.NewKeyword(":character/skills")

tx := db.NewTransaction()
_ = tx.Add(e, skills, "stealth")
_, _ = tx.Commit()

_, _ = db.GetVectorNth(e, skills, 0)    // panic
_, _ = db.GetVectorLength(e, skills)    // panic
```

## Expected Behavior

Cache-disabled mode should behave the same as cache-enabled mode, possibly with
slower direct storage resolution.

Expected results:

- `GetVectorNth(e, attr, 0)` returns the first vector element.
- `GetVectorLength(e, attr)` returns the resolved vector length.
- Missing vectors return nil / zero as they do today.
- Non-vector attributes still return the existing "not a vector" error.
- No cache setting should cause a process crash.

## Why This Matters

`DisableCache` is not just a testing knob. It is used to verify that query
correctness does not depend on cache state. The codebase already documents this
principle in prior CRDT cache work:

> The code MUST work correctly with `DisableCache: true`.

Leaving public helper APIs outside that contract creates a split-brain API:
queries and pull paths behave correctly, while direct vector helpers can crash.

## Relation To Prior Bug

This is closely related to:

`docs/bugs/BUG_DISABLECACHE_RESOLVE_ENTITY_ATTRIBUTES_PANIC.md`

That bug was resolved by adding a cache-less path that queries the matcher
directly. The same approach should apply here.

## Fix Direction

Add a cache-less branch to both vector helpers.

For `GetVectorNth`:

1. If `d.cache != nil`, keep the current cache path.
2. If `d.cache == nil`, resolve the full vector through the matcher/storage
   path, then return index `n`.

For `GetVectorLength`:

1. If `d.cache != nil`, keep the current cache path.
2. If `d.cache == nil`, resolve the full vector through the matcher/storage
   path, then return `len(vector)`.

The existing `resolveAttributeViaMatcher` path used by
`ResolveEntityAttributes` may already provide most of the needed behavior:

```go
val, err := d.resolveAttributeViaMatcher(entity, attr, matcher, schema.CardinalityVector, valueType)
```

The implementation should preserve typed vector behavior where schema value
type is known.

## Verification Plan

Add regression tests that run every case under both cache modes:

1. `GetVectorNth` returns expected element with cache enabled.
2. `GetVectorNth` returns expected element with cache disabled.
3. `GetVectorLength` returns expected length with cache enabled.
4. `GetVectorLength` returns expected length with cache disabled.
5. Missing vector returns nil / zero in both modes.
6. Non-vector attribute returns "not a vector" error in both modes.
7. AsOf / History handles either bypass or document these helpers explicitly if
   they are latest-state only.

Test names should make the contract obvious, e.g.:

```go
TestVectorHelpers_BothCacheModes_GetNth
TestVectorHelpers_BothCacheModes_GetLength
TestVectorHelpers_DisableCache_NoPanic
```
