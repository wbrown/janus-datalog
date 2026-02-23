# EA Cache Bypass When Attribute Is a Variable Bound from Input

**Date**: 2026-02-19
**Severity**: Performance (Critical in write-heavy workloads)
**Status**: Open
**Affected**: All queries where A comes from bindings rather than being a pattern constant

## Summary

The EA cache is bypassed when the attribute position in a data pattern is a Variable — even when that variable's value is known from input bindings. The cache check in `MatchWithConstraints` requires A to be a Constant in the pattern itself; it does not consider values provided through binding relations. This forces every such query through a full BadgerDB storage scan with a new transaction and iterator per call.

The most common query affected is the `HasAttribute` / `EntityExists` pattern:

```clojure
[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]
```

Here `?attr` is a Variable. `extractValue(pattern.GetA())` returns nil for Variables. The cache check fails. Every call creates a new BadgerDB read transaction, seeks an index, and closes the transaction.

## Discovery

Identified via performance profiling of a downstream application during a heavy import workload. Out of 527 existence checks, 491 (93%) took 1ms–890ms each, totaling **100.5 seconds** of pure existence-check overhead. The slow checks correlated with production writes — every `HasAttribute` call after a write to any entity took hundreds of milliseconds due to memtable/SSTable pressure in BadgerDB.

126 of those checks (40%) hit the same (E, A) pair — the same entity and attribute, over and over. With the cache, these would be O(1) after the first resolution. Without it, each one creates a new BadgerDB iterator.

## Root Cause

### Two cache check sites, both require A as Constant

The cache is checked in two places in `MatchWithConstraints` (`matcher_relations.go`):

**Site 1: With bindings** (lines 97-108)
```go
// CACHE OPTIMIZATION: When A is a constant and E comes from bindings,
// use the cache for each E value instead of storage scans.
if m.cache != nil && m.txID == 0 {
    if a := m.extractValue(pattern.GetA()); a != nil {   // ← nil for Variables
        if aKw, ok := a.(datalog.Keyword); ok {
            cacheResult, handled := m.matchWithBindingsFromCache(pattern, bindingRel, symbols, aKw)
            if handled {
                return cacheResult, nil
            }
        }
    }
}
```

**Site 2: Without bindings** (lines 245-256, inside `matchUnboundAsRelation`)
```go
// CACHE OPTIMIZATION: When E and A are bound and we're querying latest state,
// use the cache for O(1) access instead of storage scans.
if m.cache != nil && m.txID == 0 && e != nil && a != nil {
    if eIdent, ok := e.(datalog.Identity); ok {
        if aKw, ok := a.(datalog.Keyword); ok {
            cacheResult, handled := m.matchFromCache(pattern, symbols, eIdent, aKw, v, card)
            if handled {
                return cacheResult, nil
            }
        }
    }
}
```

Both depend on `extractValue`:

```go
// matcher.go:473-487
func (m *BadgerMatcher) extractValue(elem query.PatternElement) interface{} {
    switch e := elem.(type) {
    case query.Variable:
        return nil          // ← Variables always return nil
    case query.Blank:
        return nil
    case query.Constant:
        return e.Value      // ← Only Constants return a value
    default:
        return nil
    }
}
```

When the query is `[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`, `?attr` is a Variable in the pattern. `extractValue` returns nil. The cache check is skipped. Code falls through to join strategies (`matchWithHashJoin`, `matchWithMergeJoin`, etc.) which go directly to BadgerDB storage scans.

### The gap

`extractValue` only looks at the **pattern**. It does not consider whether the variable has a known value in the **binding relation**. The binding relation contains concrete values for `?e` and `?attr` — but the cache check never looks at it.

| A binding type | A value known? | Cache usable? | Current behavior |
|----------------|---------------|---------------|------------------|
| Constant in pattern | Yes, at parse time | Yes | Cache hit |
| Scalar input `?a` | Yes, from input | Yes | **Cache bypassed** |
| Collection input `[?a ...]` | Yes, set from input | Yes | **Cache bypassed** |
| Tuple input `[?e ?a]` | Yes, from input | Yes | **Cache bypassed** |
| Relation input `[[?e ?a]]` | Yes, set of pairs | Yes | **Cache bypassed** |
| Join-bound | Yes, after join | Yes | **Cache bypassed** |
| Truly unbound | No | No | Correctly bypassed |

## Dead Code Red Herring

Note: `matchBoundPattern` (matcher.go:188-264) is **dead code**. Its only caller, `matchWithoutRelation`, has zero callers. This was the old query path before `Match` → `MatchWithConstraints` was built. Any analysis pointing to `matchBoundPattern` as the bypass location is incorrect — the actual bypass is in `MatchWithConstraints` as described above.

## Impact

### Performance characteristics

Every bypassed query creates:
1. A new BadgerDB read transaction
2. An iterator seek on the chosen index (typically AEVT or EATV)
3. Key decoding + value deserialization per matching datom
4. Transaction close

Under write pressure (concurrent writes to the same database), BadgerDB's memtable/SSTable compaction makes these seeks progressively slower. The cache would serve these as O(1) in-memory lookups with zero BadgerDB interaction.

### Affected query patterns

Any query where E and/or A come from `:in` parameters or bindings:

```clojure
;; HasAttribute pattern — most common
[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]

;; Entity existence check
[:find ?e :in $ ?e ?attr :where [?e ?attr _]]

;; Multiple attributes for same entity
[:find ?v :in $ ?e [?attr ...] :where [?e ?attr ?v]]

;; Batch entity lookup
[:find ?e ?v :in $ [[?e ?attr]] :where [?e ?attr ?v]]
```

### Quantified impact (from downstream profiling)

| Metric | Value |
|--------|-------|
| Total existence checks | 527 |
| Checks in millisecond range | 491 (93%) |
| Top 30 checks | 650–890ms each |
| Total Exists overhead | **100.5 seconds** |
| Average per check | 204.6ms |
| Repeated (E,A) pairs | 126 (40%) hitting same pair |

The 126 repeated (E,A) pairs would be a single cache miss + 125 cache hits = sub-microsecond. Instead, each one is a full BadgerDB round-trip.

## Correctness Note

This is a **performance bug**, not a correctness bug. The storage scan path applies CRDT resolution correctly via `CRDTResolvingIterator` (see `BUG_CRDT_QUERY_RESOLUTION_NOT_APPLIED.md` for the history of that fix). The results are correct — they're just obtained via an unnecessarily expensive path.

The cache path (`matchWithBindingsFromCache`, `matchFromCache`) and the storage scan path return identical results. The difference is purely performance:
- Cache path: O(1) per (E,A) pair from in-memory `sync.Map`
- Storage scan path: O(log N) BadgerDB seek + I/O per (E,A) pair

## Fix

### Approach: Extract A from bindings before cache check

Before falling through to join strategies, check whether A is a Variable whose value is available in the binding relation. If so, extract it and use the cache path.

The existing `matchWithBindingsFromCache` already handles E-from-bindings correctly. The only missing piece is extracting A's value from bindings when it's not a pattern constant.

### Where to fix

`matcher_relations.go`, `MatchWithConstraints`, between lines 96 and 110 (after vector cardinality check, before join strategy selection).

### Sketch

```go
// Current: only checks pattern constants
if a := m.extractValue(pattern.GetA()); a != nil { ... }

// Fixed: also check binding relation for A's value
aValue := m.extractValue(pattern.GetA())
if aValue == nil && bindingRel != nil {
    // A is a Variable — check if bindings provide its value
    if aVar, ok := pattern.GetA().(query.Variable); ok {
        aValue = extractSingleValueFromBindings(bindingRel, aVar.Name)
    }
}
if aValue != nil {
    if aKw, ok := aValue.(datalog.Keyword); ok {
        cacheResult, handled := m.matchWithBindingsFromCache(pattern, bindingRel, symbols, aKw)
        if handled {
            return cacheResult, nil
        }
    }
}
```

For scalar inputs (single-tuple binding), `extractSingleValueFromBindings` returns the value directly. For collection/relation inputs (multi-tuple bindings), this gets more complex — each tuple may have a different A value. The simplest initial fix handles the single-value case (which covers the `HasAttribute` pattern) and falls through to storage scans for multi-value A bindings.

### What NOT to do

- Do not duplicate cache logic into the join strategies. One resolution path, not two.
- Do not add a separate cache check inside `matchWithHashJoin` or `matchWithMergeJoin`.
- Do not change `extractValue` to look at bindings — it's a pattern-level utility used elsewhere.

### Optimization phases

**Phase 1** (high impact, simple): Handle single-value A from bindings. This covers `HasAttribute` (`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`) which is the dominant pattern in downstream applications. Extract A from a single-tuple binding, use the existing `matchWithBindingsFromCache`.

**Phase 2** (medium impact): Handle collection/relation A inputs. For each tuple in the binding relation, extract the (E, A) pair and do a cache lookup. This is a loop over `cache.GetOrResolve` calls with tuple building per hit.

**Phase 3** (low impact, complex): Handle join-bound A. When A gets its value from a prior join (not from `:in`), the binding relation passed to the matcher already contains A's value. The same Phase 1/2 logic applies — the binding relation is the binding relation regardless of where it came from.

## Test Plan

### Tests needed before fix

1. **Benchmark: cache hit vs cache bypass for HasAttribute pattern**
   - Setup: database with N entities, each with a CardinalityOne attribute
   - Measure time for N `ExecuteQueryWithInputs` calls with bound E and A
   - Compare: A as pattern constant vs A as input parameter
   - Expected: pattern constant is fast (cache hit), input parameter is slow (bypass)
   - After fix: both should be equally fast

2. **Functional equivalence: cache path vs bypass path**
   - For each cardinality (one, many, vector):
     - Query with A as pattern constant (cache path)
     - Query with A as input parameter (currently bypass path)
     - Assert identical results
   - This verifies the fix doesn't change semantics

3. **Cache invalidation through the new path**
   - Write value → query via input A (cache miss → populate) → write new value → query again
   - Assert second query returns new value (cache was invalidated and rebuilt)

4. **Multi-value A inputs**
   - Collection input: `[:find ?e ?v :in $ ?e [?attr ...] :where [?e ?attr ?v]]`
   - Verify all attributes are resolved correctly
   - Verify cache is consulted for each A value

### Existing test coverage

The `matchWithBindingsFromCache` method is well-tested for the A-as-constant case:
- `matcher_cache_test.go`: `BenchmarkCachePathWithBindings` (100 entities)
- `crdt_cache_matrix_test.go`: 17-pattern test matrix (cache enabled/disabled)

These tests verify the cache path works correctly. The gap is that they never exercise the path where A is a Variable from bindings.

## Files Involved

| File | Role |
|------|------|
| `datalog/storage/matcher_relations.go:97-108` | Cache check that requires A as Constant (fix location) |
| `datalog/storage/matcher.go:473-487` | `extractValue` — returns nil for Variables |
| `datalog/storage/matcher_relations.go:1089-1223` | `matchWithBindingsFromCache` — existing cache path (reuse target) |
| `datalog/storage/cache.go` | EA cache implementation (`GetOrResolve`) |
| `datalog/storage/matcher_cache_test.go` | Existing cache tests (extend) |

## Relationship to Other Bugs

- **BUG_CRDT_QUERY_RESOLUTION_NOT_APPLIED.md**: That bug was about CRDT *correctness* — storage scans returning all historical values instead of resolved current values. Fixed by `CRDTResolvingIterator`. This bug is about *performance* — the storage scan path is correct but unnecessarily slow because the cache is bypassed.

- **BUG_CACHE_CARDINALIY_ONE_TOMBSTONE.md**: That bug was about `ResolveLWW` not checking `datom.Op` for tombstones. Fixed. The cache path itself is correct; this bug is about the cache path not being *reached*.

Both prior bugs were about the cache and storage paths producing different results. This bug is different: both paths produce the same results, but the fast path (cache) is not reached when it should be.
