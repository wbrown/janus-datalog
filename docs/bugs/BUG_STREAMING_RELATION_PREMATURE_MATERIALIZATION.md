# BUG: StreamingRelation Premature Materialization Without Tuple Copying

**Date Discovered**: 2025-10-24
**Status**: ACTIVE - Root cause identified
**Severity**: CRITICAL - Causes incorrect query results
**Affects**: All queries with `EnableTrueStreaming=true` (default since October 2025)

## Summary

When `EnableTrueStreaming=true`, StreamingRelation's fallback materialization path at `datalog/executor/relation.go:864` performs `append(tuples, it.Tuple())` without copying tuples. Since iterators reuse buffers for performance, all tuples end up pointing to the same memory location, causing catastrophic data corruption.

**The bug manifests when**:
1. StreamingRelation is created with `EnableTrueStreaming=true`
2. `Iterator()` is called BEFORE `Materialize()` is called
3. The relation goes to the fallback materialization path (lines 843-869)
4. Tuples are appended without copying: `tuples = append(tuples, it.Tuple())`
5. All tuples reference the same reused buffer

## Bug Location

**Primary Bug**: `datalog/executor/relation.go:864`

```go
func (r *StreamingRelation) Iterator() Iterator {
    // ... [locking and cache checking omitted] ...

    // Fallback: materialize on first access for backward compatibility
    r.materializeOnce.Do(func() {
        var tuples []Tuple

        // Safe path: iterator hasn't been consumed yet
        it := r.iterator
        count := 0
        for it.Next() {
            count++
            tuples = append(tuples, it.Tuple())  // ❌ BUG: Line 864 - No copy!
        }
        it.Close()
        r.materialized = NewMaterializedRelationWithOptions(r.columns, tuples, r.options)
    })
    return r.materialized.Iterator()
}
```

**Secondary Bug**: `datalog/executor/relation.go:207` (CachingIterator)

```go
func (ci *CachingIterator) Next() bool {
    // ... [done check omitted] ...

    if ci.inner.Next() {
        tuple := ci.inner.Tuple()

        // Thread-safe append to cache
        ci.mu.Lock()
        *ci.cache = append(*ci.cache, tuple)  // ❌ BUG: No copy!
        ci.mu.Unlock()

        return true
    }
    // ...
}
```

## Manifestation Sequence

From gopher-street CLI debug output:

```
DEBUG StreamingRelation.Iterator: Called for [?s], shouldCache=false, cacheReady=false, EnableTrueStreaming=true
DEBUG StreamingRelation.Iterator: Fallback materialization path (line 843-869)
DEBUG materializeRelationsForPattern: Materializing relation 0 (type=*executor.MaterializedRelation, symbols=[?s])
DEBUG materializeRelationsForPattern: After Materialize, type=*executor.MaterializedRelation
```

**Timeline**:
1. First pattern creates StreamingRelation with `[?s]` columns
2. **Iterator() is called** with `shouldCache=false` (before Materialize())
3. StreamingRelation enters fallback materialization path (lines 843-869)
4. Line 864: `tuples = append(tuples, it.Tuple())` - NO COPY
5. Iterator reuses single buffer for all tuples (EnableTrueStreaming=true)
6. All tuples point to same memory → all have identical values (last tuple's data)
7. materializeRelationsForPattern() is called AFTER iteration
8. Relation is already MaterializedRelation with corrupted data
9. Subsequent joins/operations use corrupted tuples

## Root Cause Analysis

### Why Iterator() is Called Before Materialize()

The query execution flow is:

```go
// In executePhaseSequential (executor_sequential.go):

// 1. Match pattern returns StreamingRelation
rel, err = e.matchPatternWithRelations(ctx, pattern, availableRelations)

// 2. Collapse is called, which calls Size()
independentGroups = append(independentGroups, rel)
independentGroups = independentGroups.Collapse(ctx)  // Calls Size()!

// 3. Size() triggers Iterator() to count tuples
func (r *StreamingRelation) Size() int {
    // ...
    it := r.Iterator()  // ❌ Called here, before Materialize()!
    // ...
}

// 4. Later, materializeRelationsForPattern() is called
availableRelations = materializeRelationsForPattern(pattern, availableRelations)
```

**The Problem**:
- `Size()` is called during collapse to compute join costs
- `Size()` calls `Iterator()` to count tuples
- This happens BEFORE `materializeRelationsForPattern()` is called
- StreamingRelation has `shouldCache=false` at this point
- Goes to fallback path instead of caching path
- Tuples are not copied

### Why Tuples Are Not Copied

When `EnableTrueStreaming=true`:
- Iterators use a single reusable buffer: `var reusableTuple Tuple`
- Each `it.Next()` overwrites the same buffer
- `it.Tuple()` returns a pointer to this shared buffer
- `append(tuples, it.Tuple())` appends the SAME POINTER multiple times
- Result: All tuple entries point to the same memory

**Analogy**: It's like taking multiple photos of a whiteboard, but between each photo you erase and rewrite the board. All your photos show the same final content.

## Impact

### Queries Affected

**All queries with multiple tuples in intermediate results**, including:
- Entity joins: `[?s :symbol/ticker ?ticker] [?b :price/symbol ?s]`
- Attribute scans: `[?e :person/name ?name]` (when multiple people exist)
- Value lookups: `[?e :price/open ?o]` (when multiple prices exist)

### Observable Symptoms

1. **Duplicate Results**: All tuples have identical values
   ```
   Expected: [NVDA] [CRWV]
   Actual:   [CRWV] [CRWV]  // All tuples = last tuple
   ```

2. **Missing Results**: First N-1 tuples are lost
   ```
   Expected: 5 results
   Actual:   1 result (last tuple duplicated)
   ```

3. **Incorrect Joins**: Corrupted binding relations cause wrong join results
   ```
   Expected: 390 matched tuples
   Actual:   78 matched tuples (only last binding value used)
   ```

## Reproduction

### Minimal Test Case

```go
func TestStreamingRelationTupleCopyingBug(t *testing.T) {
    // Create streaming relation with EnableTrueStreaming=true
    opts := executor.ExecutorOptions{EnableTrueStreaming: true}

    // Create iterator that reuses buffer
    tuples := []executor.Tuple{
        {uint64(1), "NVDA"},
        {uint64(2), "CRWV"},
    }
    iter := &reusingIterator{tuples: tuples}

    // Create StreamingRelation
    rel := executor.NewStreamingRelationWithOptions(
        []query.Symbol{"?id", "?ticker"},
        iter,
        opts,
    )

    // BUG: Call Iterator() before Materialize()
    // This triggers Size() → Iterator() → fallback materialization
    size := rel.Size()  // Internally calls Iterator()

    // Collect tuples
    it := rel.Iterator()
    var results []executor.Tuple
    for it.Next() {
        results = append(results, it.Tuple())
    }
    it.Close()

    // Expected: [{1, "NVDA"}, {2, "CRWV"}]
    // Actual:   [{2, "CRWV"}, {2, "CRWV"}]  ❌ All identical!

    require.Equal(t, 2, len(results))
    require.NotEqual(t, results[0], results[1])  // FAILS!
}
```

### Real-World Reproduction

**Using gopher-street database**:

```bash
cd /Users/wbrown/go/src/github.com/wbrown/janus-datalog
go build -o /tmp/datalog-cli ./cmd/datalog-cli

/tmp/datalog-cli -db ../gopher-street/datalog-db \
  -query '[:find ?ticker :where [?s :symbol/ticker ?ticker] [?b :price/symbol ?s]]'
```

**Expected**: 2 results (NVDA, CRWV)
**Actual**: 0-2 results with duplicates or missing data

## Proposed Fixes

### Fix 1: Copy Tuples in Fallback Path (IMMEDIATE)

```go
// In StreamingRelation.Iterator() fallback path:
for it.Next() {
    count++
    tuple := it.Tuple()
    tupleCopy := make(Tuple, len(tuple))
    copy(tupleCopy, tuple)
    tuples = append(tuples, tupleCopy)  // ✅ Use copy
}
```

### Fix 2: Copy Tuples in CachingIterator (IMMEDIATE)

```go
// In CachingIterator.Next():
if ci.inner.Next() {
    tuple := ci.inner.Tuple()
    tupleCopy := make(Tuple, len(tuple))
    copy(tupleCopy, tuple)

    ci.mu.Lock()
    *ci.cache = append(*ci.cache, tupleCopy)  // ✅ Use copy
    ci.mu.Unlock()

    return true
}
```

### Fix 3: Prevent Premature Iterator() Calls (ARCHITECTURAL)

**Option A**: Lazy Size() that doesn't materialize
```go
func (r *StreamingRelation) Size() int {
    r.mu.Lock()
    defer r.mu.Unlock()

    if r.cacheReady {
        return len(r.cache)
    }

    // Return -1 to indicate "unknown size"
    // Caller must handle unknown size gracefully
    return -1
}
```

**Option B**: Call Materialize() before Size() in collapse
```go
// In Relations.Collapse():
for _, rel := range relations {
    // Materialize before calling Size() to avoid premature Iterator()
    rel = rel.Materialize()
    size := rel.Size()
    // ...
}
```

**Option C**: Make shouldCache=true by default
```go
func NewStreamingRelationWithOptions(...) *StreamingRelation {
    return &StreamingRelation{
        // ...
        shouldCache: true,  // Default to caching
    }
}
```

### Fix 4: Remove Fallback Path Entirely (ARCHITECTURAL)

The fallback path (lines 843-869) exists for "backward compatibility" but:
- It's never supposed to be hit in normal operation
- When it IS hit, it causes bugs
- The caching path is the correct implementation

**Proposal**: Remove fallback entirely and require explicit materialization:

```go
func (r *StreamingRelation) Iterator() Iterator {
    r.mu.Lock()

    // Fast path: If we have a complete cache, return reusable iterator
    if r.cacheReady {
        r.mu.Unlock()
        return &sliceIterator{
            tuples: r.cache,
            pos:    -1,
        }
    }

    // Caching path: Build cache on first iteration
    if r.shouldCache && r.cacheBuilder == nil {
        // ... [existing caching logic] ...
    }

    r.mu.Unlock()

    // ❌ REMOVE THIS FALLBACK PATH ENTIRELY
    // If we get here, it's a programming error - the relation should have
    // been materialized before Iterator() was called
    panic("StreamingRelation.Iterator() called without materialization")
}
```

## Recommended Solution

**Immediate (Hot Fix)**:
- Apply Fix 1 and Fix 2 to copy tuples in both locations
- Add tests verifying tuple independence

**Short Term (Within Sprint)**:
- Apply Fix 3 Option A: Make Size() return -1 for unmaterialized relations
- Update Collapse() to handle unknown sizes gracefully
- Ensure materializeRelationsForPattern() is called before Size()

**Long Term (Next Quarter)**:
- Apply Fix 4: Remove fallback path entirely
- Make materialization explicit in query execution flow
- Add debug assertions to catch premature Iterator() calls

## Testing Requirements

### Unit Tests

1. **TestStreamingRelationTupleCopying**: Verify tuples are independent
2. **TestCachingIteratorTupleCopying**: Verify cached tuples are independent
3. **TestStreamingRelationSizeWithoutMaterialization**: Verify Size() behavior

### Integration Tests

1. **TestGopherStreetExactBug**: Use actual gopher-street database
2. **TestMultiTupleJoins**: Verify joins with >1 binding tuple
3. **TestEntityJoins**: Verify entity reference joins

### Regression Tests

All existing tests must pass after fixes are applied.

## Related Issues

- **BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md**: Related to premature IsEmpty() calls
- **STREAMING_ARCHITECTURE_COMPLETE.md**: EnableTrueStreaming feature introduction
- **CLAUDE_BUGS.md**: General patterns to avoid

## References

- Code: `datalog/executor/relation.go:864` (primary bug)
- Code: `datalog/executor/relation.go:207` (secondary bug)
- Code: `datalog/executor/executor_sequential.go:127` (collapse triggers Size())
- Code: `datalog/executor/executor_sequential.go:266-305` (materializeRelationsForPattern)
- Test: `datalog/storage/gopher_street_exact_bug_test.go`

## Timeline

- **2025-10-24**: Bug discovered during gopher-street OHLC performance testing
- **2025-10-24**: Root cause identified (premature Iterator() call triggers fallback path)
- **2025-10-24**: This document created
- **Next**: Apply immediate fixes and create tests
