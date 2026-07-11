# TupleKeyMap Optimization for Set Membership

**Status**: `PutIfAbsent` adoption complete (2026-07-11); specialized set
representation remains a proposal
**Created**: 2025-12-24
**Context**: Performance regression from set semantics fix

## Problem Statement

The set semantics fix for `StreamingRelation.Project()` added `DedupIterator` which uses `TupleKeyMap` for deduplication. This caused:

| Metric | Before | After | Regression |
|--------|--------|-------|------------|
| Time | 35,326 ns/op | 44,331 ns/op | +25% |
| Memory | 2,520 B/op | 16,744 B/op | +6.6× |
| Allocs | 111 allocs/op | 224 allocs/op | +2× |

The original implementation also performed separate `Exists` and `Put` calls.
That redundant lookup is now removed across all production add-if-absent paths:
deduplication is **7.3% faster geomean** with unchanged memory and allocations.

The remaining overhead comes from `TupleKeyMap` being designed for **key-value
storage** when deduplication only needs **set membership**.

## Current Implementation

```go
// TupleKeyMap stores key-value pairs
type TupleKeyMap struct {
    m map[uint64][]mapEntry
}

type mapEntry struct {
    values []interface{}  // For collision detection
    value  interface{}    // The stored value - NOT NEEDED FOR DEDUP
}

// DedupIterator now uses the single-walk insertion API
if !it.seen.PutIfAbsent(key, struct{}{}) {
    return true
}
```

For every unique tuple:
1. Compute FNV-1a hash over all values
2. Look up hash in map
3. If collision bucket exists, compare values using `datalog.ValuesEqual()`
4. Store `mapEntry{values, struct{}{}}` — the `value` field remains unnecessary
   for set membership

## Proposed Optimizations

### Option 1: TupleSet (Minimal Change)

Create a specialized `TupleSet` that doesn't store values:

```go
type TupleSet struct {
    m map[uint64][]Tuple  // Just tuples, no mapEntry wrapper
}

func (s *TupleSet) Add(tuple Tuple) bool {
    hash := hashValues(tuple)
    for _, existing := range s.m[hash] {
        if tuplesEqual(tuple, existing) {
            return false  // Already exists
        }
    }
    s.m[hash] = append(s.m[hash], tuple)
    return true  // Was added
}

func (s *TupleSet) Contains(tuple Tuple) bool {
    hash := hashValues(tuple)
    for _, existing := range s.m[hash] {
        if tuplesEqual(tuple, existing) {
            return true
        }
    }
    return false
}
```

**Expected savings**: Eliminates `mapEntry` wrapper and `value` field per unique tuple.

### Option 2: Interned Value Fast Path

For single-symbol projections on interned types (`*Identity`, `*Keyword`), use pointer comparison:

```go
type SingleColumnSet struct {
    m map[interface{}]struct{}
}

func (s *SingleColumnSet) Add(val interface{}) bool {
    if _, exists := s.m[val]; exists {
        return false
    }
    s.m[val] = struct{}{}
    return true
}
```

**When applicable**: Projection to single symbol of interned type.
**Expected speedup**: Significant - avoids hashing entirely, uses Go's native map.

### Option 3: Adaptive Strategy

Choose strategy based on projection characteristics:

```go
func NewDedupIterator(source Iterator, symbols []Symbol) Iterator {
    if len(symbols) == 1 && isInternedType(symbols[0]) {
        return &SingleColumnDedupIterator{...}
    }
    if expectedCardinality < 100 {
        return &LinearScanDedupIterator{...}  // O(n²) but cache-friendly
    }
    return &HashDedupIterator{...}  // Current approach with TupleSet
}
```

### Option 4: Probabilistic (Not Recommended)

Use Bloom filter for approximate membership. **Rejected** because false positives would drop valid tuples, violating correctness.

## Implementation Plan

1. ✅ **Phase 0**: Replace `Exists` + `Put` with `PutIfAbsent`
   - 5.4–9.0% faster across unique/duplicate-heavy materialized/streaming paths
   - 7.3% geomean; no memory or allocation change
2. **Phase 1**: Create `TupleSet` (Option 1)
   - Simple change, no behavior difference
   - Measure improvement
3. **Phase 2**: Add single-symbol fast path (Option 2)
   - Requires detecting projection symbol types
   - Only if Phase 1 shows insufficient improvement
4. **Phase 3**: Adaptive strategy (Option 3)
   - Only if real-world profiling shows benefit

## Acceptance Criteria

- Memory overhead reduced by at least 50%
- Time overhead reduced by at least 15%
- No correctness regressions (set semantics tests pass)
- No additional allocations per tuple

## Related Files

- `datalog/executor/tuple_key.go` - Current `TupleKeyMap` implementation
- `datalog/executor/iterator_composition.go` - `DedupIterator` implementation
- `datalog/executor/relation.go` - `StreamingRelation.Project()` fix
- `datalog/executor/set_semantics_test.go` - Correctness tests

## Notes

The 6.6× memory increase is significant but the fix is necessary for correctness. Datalog relations are **sets**, not **bags**. Without deduplication, queries can return incorrect results.

The optimization should be measured against the fixed implementation, not the broken one. The goal is to reduce the correctness tax, not eliminate it entirely.
