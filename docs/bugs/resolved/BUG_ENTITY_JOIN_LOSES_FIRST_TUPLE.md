# BUG: Entity Join Loses First Tuple

**Status:** RESOLVED
**Date Discovered:** 2025-10-16
**Date Fixed:** 2025-10-16
**Severity:** CRITICAL - Data loss during query execution

## Symptom

Joining two patterns on the same entity variable loses one result tuple.

**Example Query:**
```datalog
[:find ?bar :where
  [?bar :price/high ?h]
  [?bar :price/low ?l]]
```

**Expected:** 5 results
**Actual:** 4 results (first tuple missing)

**Consistently Missing:** The first tuple in iteration order (`bar:0` / `4GCe+%zbX2@b5`BBECl`;!.Zk`)

## Root Cause

StreamingRelation has a materialization bug when its iterator is partially consumed before `Size()` is called.

### The Bug Sequence

1. Pattern matching returns a `StreamingRelation` with a fresh storage iterator
2. First `Iterator()` call creates a `CountingIterator` wrapper (`relation.go:820-824`)
3. **Something consumes 1 tuple from this iterator** ← Still investigating what
4. Later, `Size()` is called (likely from join size comparison)
5. `Size()` triggers materialization via fallback path (`relation.go:900 → 838-867`)
6. Materialization iterates `r.iterator`, but this has already been wrapped and partially consumed
7. **Only the remaining tuples get materialized** (4 instead of 5)

### Evidence

Debug output from `relation.go:853-865`:
```
[StreamingRelation.materializeOnce] Starting materialization, columns=[?bar], counter=&{0x1400027d5e0 1 false}
[StreamingRelation.materializeOnce] Materialized 4 tuples, columns=[?bar]
```

The counter exists with `count=1` (one tuple already read), then materialization only captures 4 remaining tuples.

## Key Code Locations

### Where Counter is Created
**File:** `datalog/executor/relation.go:820-824`
```go
// Wrap with counting iterator for lightweight size tracking
if r.counter == nil {
    r.counter = NewCountingIterator(baseIter)
    baseIter = r.counter
}
```

### Where Materialization Happens (Buggy)
**File:** `datalog/executor/relation.go:838-867`
```go
r.materializeOnce.Do(func() {
    // BUG: Iterates r.iterator which may have been wrapped by counter
    // and partially consumed
    it := r.iterator
    for it.Next() {
        tuples = append(tuples, it.Tuple())
    }
    // Result: Only captures remaining tuples!
})
```

### Where Size() Triggers Materialization
**File:** `datalog/executor/relation.go:900`
```go
_ = r.Iterator() // Triggers materialization
if r.materialized != nil {
    return r.materialized.Size()
}
```

## Reproduction

**Minimal Test:** `entity_join_bug_test.go`

```go
// Create 5 entities with both :price/high and :price/low
for i := 0; i < 5; i++ {
    barID := datalog.NewIdentity(fmt.Sprintf("bar:%d", i))
    tx.Add(barID, datalog.NewKeyword(":price/high"), float64(100+i*10))
    tx.Add(barID, datalog.NewKeyword(":price/low"), float64(90+i*10))
}

// Query joining both patterns
query := `[:find ?bar :where [?bar :price/high ?h] [?bar :price/low ?l]]`

// Expected: 5 results
// Actual: 4 results (bar:0 missing)
```

## Resolution

### Root Cause Identified

**The culprit:** `StreamingRelation.IsEmpty()` at `relation.go:973`

`IsEmpty()` calls `r.counter.Next()` to peek at the first tuple. This consumes it from the underlying iterator. Later, when `Size()` is called, it triggers materialization which only captures the **remaining** tuples.

**Call chain:**
1. `matcher_strategy.go:52` - `bindingRel.IsEmpty()` checks if bindings are empty
2. `relation.go:973` - `IsEmpty()` calls `r.counter.Next()` to peek
3. First tuple consumed from iterator
4. `hash_join_matcher.go:46` - `Size()` called to determine join strategy
5. `relation.go:917-919` - `Size()` triggers materialization via fallback
6. `relation.go:857-863` - Materialization iterates already-consumed iterator
7. **Only remaining 4 tuples captured**

### The Fix

**Three files modified:**

1. **`datalog/storage/matcher_strategy.go:51-64`**
   - Skip `IsEmpty()` check on StreamingRelations
   - Only call `IsEmpty()` on MaterializedRelations where it's safe

2. **`datalog/storage/matcher_relations.go:64-76`**
   - Skip `IsEmpty()` check on StreamingRelations
   - Only call `IsEmpty()` on MaterializedRelations where it's safe

3. **`datalog/executor/relation.go:218-238`**
   - Fixed race condition in `CachingIterator.signalComplete()`
   - Move `signaled` check inside lock to prevent double-close
   - Keep channel close outside lock (don't hold lock while unblocking waiters)

### Verification

- ✅ `TestEntityJoinBug` - Returns 5/5 results (was 4/5)
- ✅ `TestMultipleAggregateSubqueriesNilBug` - Returns correct aggregates (was 0 rows)

## Impact (When Bug Was Active)

**Affected Queries:**
- Any query joining 2+ patterns on the same entity variable
- Particularly common in OHLC aggregations and entity attribute queries

**Data Loss:**
- Consistently lost the first tuple in iteration order
- Silent data loss - no error, just wrong results

**Lesson Learned:**
- `IsEmpty()` on streaming iterators is dangerous - it must consume data to check
- Always prefer lazy evaluation over eager peeking for streaming data sources
- Type-based dispatch (MaterializedRelation vs StreamingRelation) can prevent such bugs
