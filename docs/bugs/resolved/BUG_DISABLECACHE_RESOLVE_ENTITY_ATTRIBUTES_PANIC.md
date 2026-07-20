# BUG: `DisableCache` Can Panic in `ResolveEntityAttributes`

**Date**: 2026-04-16 **Severity**: Correctness / Stability (Medium) **Status**: Resolved 2026-04-16 **Affected**: Direct calls to `Database.ResolveEntityAttributes(...)` when DB is opened with `DisableCache: true`

## Summary

`Database.ResolveEntityAttributes(...)` unconditionally dereferences `d.cache`. When the database is opened with `DisableCache: true`, `d.cache` is nil by design, and this method can panic with a nil-pointer dereference.

## Root Cause

`NewDatabaseWithOptions` intentionally sets `cache` only when cache is enabled:

```go
var cache *Cache
if !opts.DisableCache {
    cache = NewCache()
}
```

But `ResolveEntityAttributes` uses `d.cache` without a nil guard:

```go
cachedAttrs := d.cache.GetCachedAttrs(eBytes)
```

Later it also calls:

```go
entry := d.cache.GetOrResolve(key, matcher)
```

So the method assumes cache availability even though cache is optional.

## Expected Behavior

When cache is disabled, the method should:

- either return a clear error (like `ResolveAllAttributes` does), or
- transparently fall back to storage-based resolution without cache.

It should never panic.

## Actual Behavior

Direct callers can trigger a runtime panic when `DisableCache: true`.

## Why This Is Subtle

- Most query paths do not call this method directly.
- `ResolveAllAttributes` *does* have a nil-cache guard, so nearby code looks safe.
- Full integration tests can pass if they never invoke `ResolveEntityAttributes` under disabled cache mode.

## Reproduction Sketch

```go
db, _ := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:         dir,
    DisableCache: true,
})

e := datalog.NewIdentity("e1")
attrs := []datalog.Keyword{datalog.NewKeyword(":person/name")}

_, _ = db.ResolveEntityAttributes(e, attrs) // panic (nil d.cache dereference)
```

## Impact

- Unexpected process crash in applications that use this API directly.
- Inconsistent API contract: cache-disabled mode works in many paths, but this path panics.

## Fix Direction

Align behavior with the existing `ResolveAllAttributes` contract:

Option A (strict):
- return error when `d.cache == nil` (explicitly unsupported without cache)

Option B (more flexible):
- add no-cache fallback resolution path using matcher/storage scans

Either approach is acceptable, but behavior should be explicit and consistent.

## Verification Plan

Add regression tests:

- `TestResolveEntityAttributes_DisableCache_NoPanic`
- `TestResolveEntityAttributes_DisableCache_ReturnsErrorOrFallback`

And include a contract test that behavior matches documented semantics for cache-disabled mode.

## Resolution

**Resolved**: 2026-04-16

### The original framing was wrong

This report listed two acceptable fixes:

- **Option A**: return error when `d.cache == nil` (consistent with sibling `ResolveAllAttributes`)
- **Option B**: no-cache fallback path

Both were called "acceptable." That framing was wrong. The codebase has an explicit principle that the cache is an optimization, not a correctness requirement — captured in `datalog/storage/crdt_cache_matrix_test.go:22`:

> The code MUST work correctly with `DisableCache: true`

Option A would have made the cache a hard dependency for these methods, violating the principle. Only Option B is consistent with the architecture. Furthermore, the existing `ResolveAllAttributes` `if d.cache == nil { return nil, fmt.Errorf("...") }` was the legacy version of the same architectural mistake — error rather than panic, but still treating the cache as required. Both methods needed the fix.

### Fix

- `ResolveEntityAttributes` got a cache-less branch at the top of the function: when `d.cache == nil`, query the matcher directly for each attribute via a new `resolveAttributeViaMatcher` helper. The matcher applies CRDT resolution (LWW for one, add-wins for many, RGA for vector) through `CRDTResolvingIterator` when no cache is set. The cache-aware branch is unchanged.
- `ResolveAllAttributes` had its `cache == nil` error removed. Both its schema-driven and EAVT-discovery paths now end by delegating to `ResolveEntityAttributes`, which handles cache-aware and cache-less modes uniformly. The previous duplicated cache-aware resolution logic at the bottom of `ResolveAllAttributes` is gone.

### Test coverage

`datalog/storage/resolve_disablecache_test.go` exercises both methods under both cache modes, with subtests for each cardinality:

| Test | Cardinality / scope |
|---|---|
| `TestResolveEntityAttributes_BothCacheModes_CardinalityOne` | one (with LWW overwrite) |
| `TestResolveEntityAttributes_BothCacheModes_CardinalityMany` | many (add-wins set) |
| `TestResolveEntityAttributes_BothCacheModes_CardinalityVector` | vector (RGA order) |
| `TestResolveEntityAttributes_BothCacheModes_MissingAttribute` | absent attribute is omitted, no error |
| `TestResolveAllAttributes_BothCacheModes_WithSchema` | schema-driven path |
| `TestResolveAllAttributes_BothCacheModes_NoSchema` | EAVT-discovery path |

Each runs against `cache_enabled` and `cache_disabled`. Pre-fix: the `cache_disabled` subtest of `CardinalityOne` panicked, halting the test binary; the `ResolveAllAttributes` cache_disabled subtests would have errored. Post-fix: 12/12 subtests pass.

### Lesson

When a bug report frames a fix as "Option A or Option B, either works," check whether one option violates a documented architectural principle. Here the "either is acceptable" framing came from the report itself, and I initially defaulted to the simpler error-out option (A) without checking the principle — the user had to redirect me. The right reflex when a principle exists is to honor it, not pick the smaller fix.
