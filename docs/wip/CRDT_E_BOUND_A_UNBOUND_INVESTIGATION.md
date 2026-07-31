# Investigation: CRDT Resolution for E Bound, A Unbound Patterns

**Date**: 2026-02-06
**Status**: FIXED
**Severity**: Critical
**Pattern**: `[:find ?a ?v :in $ ?e :where [?e ?a ?v]]` (entity enumeration)

## Resolution Summary

The investigation identified THREE bugs:

### Bug 1: Wrong Index for Full Scans in matchUnboundAsRelation (FIXED)
`matchUnboundAsRelation` used EAVT for full scans, but `CRDTResolvingIterator` needs the newest write for an (E, A) to arrive first. EAVT encodes `Tx↓` like every index — its problem is Tx's *position*: `[E][A][V][Tx↓]` groups by value, so the first entry under `[E][A]` is the lowest value, not the newest write. Fixed by switching to EATV when schema exists in `matcher_relations.go`.

### Bug 2: Wrong Default for Schemaless Attributes (FIXED)
`CRDTResolvingIterator` defaulted to `CardinalityOne` for attributes without schema definitions. This silently hid data. Fixed by introducing `CardinalityUnknown` which returns ALL values (Datascript-style default).

### Bug 3: chooseIndex Not CRDT-Aware (FIXED)
The root cause: `chooseIndex` in `matcher.go` was NOT CRDT-aware. It picked V-before-Tx indices (EAVT) for:
- E-only scans (only E bound, A/V unbound)
- Full scans (nothing bound)

Both cases scan multiple (E, A) groups and require Tx-first ordering for `CRDTResolvingIterator`'s "first entry wins" logic.

**Fix**: Changed `chooseIndex` to always use EATV (Tx-first) for these cases. The principle: **chooseIndex should ALWAYS think in CRDT terms**.

### Files Changed
- `datalog/schema/types.go` - Added `CardinalityUnknown`
- `datalog/storage/crdt_resolving_iterator.go` - Use `CardinalityUnknown` for unschemaed attributes
- `datalog/storage/matcher_relations.go` - CRDT wrapping unconditional when schema exists
- `datalog/storage/matcher.go` - **`chooseIndex` now CRDT-aware**: EAVT→EATV for E-only and full scans
- `datalog/storage/crdt_cache_matrix_test.go` - Added test for all-unbound pattern

---

## Problem Statement

Entity browser queries that enumerate all attributes of a specific entity are returning **duplicate values** for cardinality-one attributes. This suggests CRDT resolution is not being applied.

Example output showing duplicates:
```
entity/name          Imported Module
entity/name          The Sunken Sanctum of Valdris
dungeon/room-count   0
dungeon/room-count   12
```

Both `:entity/name` and `:dungeon/room-count` are cardinality-one attributes but show multiple values.

## Query Pattern Analysis

```datalog
[:find ?a ?v
 :in $ ?e
 :where [?e ?a ?v]]
```

- **E**: Bound via `:in` clause (scalar input)
- **A**: Unbound variable
- **V**: Unbound variable

This is the fundamental entity enumeration pattern used by entity browsers, debuggers, and admin tools.

## Code Path Trace

### 1. Entry Point: `MatchWithConstraints` (matcher_relations.go)

When called with this pattern:
- `bindings` contains the entity ID for `?e`
- `bindingRel := bindings.FindBestForPattern(pattern)` finds the binding
- Does NOT go to `matchUnboundAsRelation` because bindings exist

### 2. Strategy Analysis: `analyzeReuseStrategy` (matcher_strategy.go)

For `[?e ?a ?v]` with E bound from bindings:
- `boundPositions` = `[0]` (only E is bound)
- Goes to `case 0: // E is bound`
- A is a Variable (not Constant), so goes to else branch:

```go
// matcher_strategy.go:114-117
} else {
    // E is bound, A varies → use EATV (E-primary with Tx descending)
    indexType = EATV // E is primary sort key, Tx descending for CRDT
    canReuse = true
}
```

Returns `SinglePositionReuse` strategy with EATV index.

### 3. Join Strategy Selection

Based on binding size, goes to one of:
- `matchWithHashJoin` (most common for small bindings)
- `matchWithMergeJoin` (large bindings, high selectivity)
- `matchWithIteratorReuse` (when IndexNestedLoopThreshold is set)

### 4. CRDT Resolution Wrapping

All three join paths wrap with `CRDTResolvingIterator`:

**matchWithHashJoin** (hash_join_matcher.go:196-202):
```go
var resolvedIter Iterator = storageIter
if m.schema != nil {
    resolvedIter = NewCRDTResolvingIterator(storageIter, m.schema, m.txID)
}
```

**matchWithMergeJoin** (hash_join_matcher.go:569-573):
```go
var resolvedIterMerge Iterator = storageIter
if m.schema != nil {
    resolvedIterMerge = NewCRDTResolvingIterator(storageIter, m.schema, m.txID)
}
```

**matchWithIteratorReuse** (matcher_iterator_reusing.go:72-77):
```go
if it.matcher.schema != nil {
    it.storageIter = NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher.txID)
} else {
    it.storageIter = rawIter
}
```

**matchWithoutIteratorReuse** (matcher_iterator_nonreusing.go:79-84):
```go
if it.matcher.schema != nil {
    it.currentScan = NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher.txID)
} else {
    it.currentScan = rawIter
}
```

## Index Order Analysis

The `CRDTResolvingIterator` assumes Tx descending order (line 11):
> "EATV index stores Tx in descending order (highest Tx first). This means resolution is just filtering - no buffering needed."

### EATV Index Order

EATV: E → A → Tx↓ → V (Tx is descending)

For a single entity scan:
```
(E1, A1, Tx100, V1)  // first entry for (E1, A1) = LWW winner
(E1, A1, Tx50, V2)   // older entry for (E1, A1) = skip
(E1, A2, Tx200, V3)  // first entry for (E1, A2) = LWW winner
(E1, A2, Tx150, V4)  // older entry for (E1, A2) = skip
```

**This should work correctly.** The (E, A) groups are contiguous, and within each group Tx is descending.

## Missing Test Coverage

The existing test matrix (from `BUG_CRDT_QUERY_RESOLUTION_NOT_APPLIED.md`) shows:

| Test | Description |
|------|-------------|
| AConstant | E bound, A constant - `[?e :person/name ?v]` |
| AFromScalarInput | E bound, A from input - `[:in $ ?e ?a :where [?e ?a ?v]]` |
| **AUnbound** | E **unbound**, A unbound - `[?e ?a ?v]` |

Note: `AUnbound` test has E **unbound**. There is NO test for:
- **E bound via input, A unbound** - `[:in $ ?e :where [?e ?a ?v]]`

This is exactly the entity browser pattern!

## Hypotheses

### Hypothesis 1: Test Gap Hiding Real Bug

The code paths appear correct, but there's no test for this specific pattern. The bug may exist somewhere not yet traced.

### Hypothesis 2: Index Selection Bug

The `matchWithHashJoin` path may be using the wrong index despite `strategy.Index` being EATV. Need to verify the actual index used in the scan.

### Hypothesis 3: Schema Not Being Applied

If `m.schema` is nil when the query runs, no CRDT resolution will be applied. The scry database should have schema set via `db.SetSchema(schema.Schema())`.

### Hypothesis 4: Data Corruption

The data was written without schema (before schema existed), so the writes used raw `tx.Add()` which doesn't enforce CRDT semantics. But this shouldn't matter - the schema should still resolve at read time.

### Hypothesis 5: Cache Path Skipping Resolution

When the cache is enabled and handles the query, resolution happens. When cache misses or is disabled, the non-cache path may have issues.

## Next Steps

1. **Add Test Case**: Create explicit test for `[:find ?a ?v :in $ ?e :where [?e ?a ?v]]` pattern with multiple writes to cardinality-one attributes

2. **Verify Index Usage**: Add logging/tracing to confirm which index is actually used in the scan

3. **Verify Schema Presence**: Confirm `m.schema != nil` during the matcher call

4. **Test With Cache Disabled**: Run with `DisableCache: true` to isolate cache from resolution

5. **Trace matchUnboundAsRelation**: There's a potential gap where it only wraps when `e == nil && a != nil`:
   ```go
   // matcher_relations.go:382-388
   if m.schema != nil && e == nil && a != nil {
       regularIter.storageIter = NewCRDTResolvingIterator(...)
   } else {
       regularIter.storageIter = rawStorageIter  // NO RESOLUTION!
   }
   ```
   This path shouldn't be hit for our pattern (we have bindings), but worth verifying.

## Related Files

- `datalog/storage/matcher_relations.go` - Main Match() dispatch
- `datalog/storage/matcher_strategy.go` - Index/strategy selection
- `datalog/storage/hash_join_matcher.go` - Hash/merge join implementations
- `datalog/storage/matcher_iterator_reusing.go` - Iterator reuse implementation
- `datalog/storage/matcher_iterator_nonreusing.go` - Non-reusing iterator
- `datalog/storage/crdt_resolving_iterator.go` - CRDT resolution wrapper
- `BUG_CRDT_QUERY_RESOLUTION_NOT_APPLIED` - Parent bug document

## Resolution Criteria

The fix is complete when:
1. Test case for `[:find ?a ?v :in $ ?e :where [?e ?a ?v]]` passes
2. Entity browser shows single value per cardinality-one attribute
3. Works with cache enabled AND disabled
