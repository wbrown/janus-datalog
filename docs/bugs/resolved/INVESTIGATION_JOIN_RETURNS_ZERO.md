# Investigation: Join Returning Zero Results After Lazy Materialization

**Date**: 2025-10-15
**Status**: IN PROGRESS
**Severity**: HIGH - Breaks basic join queries

## Summary

After implementing lazy materialization (commit 6dc74bf), basic join queries started returning 0 results even though:
- Individual patterns work correctly
- Identity comparison works correctly
- Identities are properly interned

## Symptoms

TestDebugBasicQuery demonstrates the issue:

```datalog
[:find ?e ?name ?age
 :where [?e :person/name ?name]
        [?e :person/age ?age]]
```

**Expected**: 1 result (person1 with name="Alice" and age=30)
**Actual**: 0 results

## What We Ruled Out

### ✅ Identity Comparison Bug (Fixed)
- **Issue**: `ValuesEqual()` was comparing ALL Identity struct fields instead of just the hash
- **Fix**: Commit 8d8c0e3 - Moved Identity comparison BEFORE general `==` check
- **Verification**: Test `TestIdentityStorageRoundTrip` confirms ValuesEqual works correctly

### ✅ Identity L85 Encoding Bug (Fixed)
- **Issue**: `InternIdentityFromHash()` wasn't eagerly computing L85, leading to empty `str` and `l85` fields
- **Fix**: Commit ab98da1 - Made `NewIdentityFromHash()` and `InternIdentityFromHash()` eagerly compute L85
- **Verification**: Identities from storage now have properly computed L85 encodings

### ✅ Identity Interning Works
```
Pattern 1 ?e pointer: 0x140000db840
Pattern 2 ?e pointer: 0x140000db840
Are they the same pointer? true
ValuesEqual(e1, e2): true
Pattern 1 hash: 64abaade7c05afb815c615d3c43509fbd6ebd2ab
Pattern 2 hash: 64abaade7c05afb815c615d3c43509fbd6ebd2ab
```

Both patterns produce the **same interned pointer** with **identical hashes**.

### ✅ Individual Patterns Work
```
Pattern 1 ([?e :person/name ?name]): 1 results
Pattern 2 ([?e :person/age ?age]): 1 results
```

Each pattern successfully retrieves data from storage.

## What's Failing

### ❌ The Join Operation
```
Join result size: 0
```

The join of two 1-tuple relations with a shared column `?e` produces **0 results** instead of 1.

## Root Cause Hypothesis

The bug is in the **join/collapse logic**, likely related to the lazy materialization changes in commit 6dc74bf.

### Suspicious Areas

1. **StreamingRelation Iterator Consumption**
   - Pattern results from storage are `StreamingRelation` instances
   - The first pattern's iterator may be consumed before the join
   - `Materialize()` sets `shouldCache=true` but doesn't materialize immediately

2. **CachingIterator Implementation**
   - Lazy materialization uses `CachingIterator` as a side effect during iteration
   - There may be a bug in how cached data is replayed for subsequent iterations

3. **Collapse/Join Logic**
   - `executor_sequential.go:123-131` - Pattern results are added to `independentGroups` and collapsed
   - `relations.go:135-187` - `Collapse()` method joins relations with shared columns
   - `join.go:136-536` - `HashJoin` builds a hash table and probes

4. **materializeRelationsForPattern()**
   - `executor_sequential.go:249-288` - Materializes relations sharing symbols with pattern
   - May be called **after** iterator has been consumed

## Investigation Timeline

### Phase 1: Identity Comparison (COMPLETE)
1. Discovered `ValuesEqual()` compared all Identity fields → **Fixed in 8d8c0e3**
2. Discovered `InternIdentityFromHash()` didn't compute L85 → **Fixed in ab98da1**
3. Added `TestIdentityStorageRoundTrip` to verify fixes → **Passing**

### Phase 2: Join Logic (IN PROGRESS)
1. Confirmed identities are identical (same pointer, same hash)
2. Confirmed individual patterns work
3. Confirmed join produces 0 results
4. Hypothesis: Iterator consumption or caching bug in lazy materialization

## Code Locations

### Test Case
- **File**: `datalog/storage/debug_test.go`
- **Function**: `TestDebugBasicQuery`
- **Line**: Creates entity, queries with join, expects 1 result

### Join Execution Path
1. `executor_sequential.go:12` - `executePhaseSequential()`
2. `executor_sequential.go:123-131` - Add pattern results to independentGroups, collapse
3. `relations.go:135` - `Collapse()` joins relations with shared columns
4. `relations.go:166` - `ctx.JoinRelations()` wrapper for annotation
5. `relation.go:570` - `MaterializedRelation.Join()` delegates to `HashJoin()`
6. `join.go:136` - `HashJoinWithOptions()` builds hash table and probes

### Lazy Materialization
- `relation.go:1051` - `StreamingRelation.Materialize()` sets `shouldCache=true`
- `relation.go:899` - `StreamingRelation.Iterator()` checks `shouldCache`, creates `CachingIterator`
- `relation.go:748` - `CachingIterator.Next()` builds cache during iteration

## Debugging Steps Taken

1. ✅ Added debug output to TestDebugBasicQuery showing:
   - Entity creation
   - Individual pattern results
   - Identity pointer addresses
   - Identity hashes
   - Join result size

2. ✅ Created `TestIdentityStorageRoundTrip` to verify:
   - Value equality without pointers
   - Pointer equality after interning
   - ValuesEqual with pointers
   - ValuesEqual with dereferenced pointers
   - Direct struct comparison fails (as expected)

3. ⏳ Next: Add debug output to join logic to see:
   - Hash table build phase (how many entries?)
   - Probe phase (are keys matching?)
   - TupleKey hashing (are keys identical?)

## Other Failing Tests

Many other tests are also failing, likely due to the same root cause:

### Storage Package
- `TestBatchScanTrace`
- `TestDatabasePlanCache`
- `TestDebugBasicQuery` ← This one
- `TestParallelDecorrelationColumnOrderBadger`
- `TestPredicatePushdownIntegration`
- `TestEarlyPredicateFiltering`
- `TestPredicatePushdownCorrectness`
- `TestPlannerPredicatePushdownIntegration`
- `TestPureAggregationWithBadgerDB` (some subtests)

### Tests Package
- `TestConditionalAggregatePlanningDebug`
- `TestConditionalAggregateRewritingE2E`
- `TestOptimizationComposition`
- `TestParameterizedQueryCartesianProduct`
- `TestPhaseReorderingEffectiveness`
- `TestSubqueryPlanningDebug`

### Root Package
- `TestMultipleAggregateSubqueriesNilBug`

All of these may share the same underlying join/iterator issue.

## Investigation Progress (2025-10-15 Continued)

### Phase 3: HashJoin Debug Logging (COMPLETE)

**Fixed**: `Database.NewExecutorWithOptions()` was not passing debug options to matcher
- Bug in `database.go:149` - called `d.Matcher()` which always uses DEFAULT options
- Fixed by creating matcher with custom options inline

**Added debug logging to HashJoin**:
```
[HashJoin] Called with left/right types, sizes, columns
[HashJoin] Build phase: X tuples, first key/tuple
[HashJoin] Probe phase: probed X tuples, found X matches, produced X results
```

### Phase 4: Root Cause Discovery (IN PROGRESS)

**Key Finding**: Probe iterator returns **0 tuples**!

Debug output shows:
```
[HashJoin] Built hash table with 1 tuples, first key: ..., first tuple: [FD6glMq1eg+}o!@g)2r}m+Hii 30]
[HashJoin] Probe phase complete: probed 0 tuples, found 0 matches, produced 0 results
```

The left StreamingRelation (from pattern `[?e :person/name ?name]`) has an empty iterator when the join tries to probe it.

**Caching Debug Output**:
```
First Iterator() call: shouldCache=false  ← NOT MATERIALIZED!
Second Iterator() call: shouldCache=true, cache=false, cachingInProgress=false
[StreamingRelation.Iterator] Starting caching (first call)
```

**Root Cause Identified**:
1. First pattern result is NOT materialized before the join (shouldCache=false)
2. Second pattern IS materialized (shouldCache=true)
3. When HashJoin probes the first relation, it gets the base storage iterator
4. This iterator was already consumed during binding-based filtering
5. Result: Probe phase gets 0 tuples

**The Bug**: `materializeRelationsForPattern()` materializes relations in `availableRelations`, but when patterns are added to `independentGroups` and collapsed, the ORIGINAL unmaterialized versions are used, not the materialized copies.

### Attempted Fixes

**Fix 1: Cache Completion Checks in Iterator()** (CORRECT, but incomplete)
- Added checks to `StreamingRelation.Iterator()` to return cached iterator on subsequent calls
- This is the CORRECT approach for lazy materialization
- Lines 758-796 in `relation.go`

**Fix 2: Update independentGroups after materializeRelationsForPattern** (DIDN'T WORK)
- Tried replacing relations in `independentGroups` with materialized versions from `availableRelations`
- Problem: `Materialize()` returns `self` with `shouldCache=true`, not a different object
- So the replacement did nothing

**Fix 3: Call materializeRelationsForPattern after collapse** (DIDN'T WORK)
- Tried calling materialization after the collapse to ensure cache is built
- Problem: By then it's too late - the iterator is already consumed

### Root Cause Discovery (COMPLETE)

Debug output reveals the actual sequence:

```
[HashJoin] Left is StreamingRelation: shouldCache=true, iteratorCalled=false, cache len=0
[StreamingRelation.Iterator] Called: shouldCache=true, cache=false (len=0), cachingInProgress=false, iteratorCalled=false, symbols=[?e ?name]
[StreamingRelation.Iterator] First call with caching enabled
[HashJoin] Probe phase complete: probed 0 tuples
```

**The Problem**:
1. Pattern matching consumes the base storage iterator for binding-based filtering
2. `Materialize()` is called, setting `shouldCache=true` on the relation
3. HashJoin calls `Iterator()` - this is marked as the FIRST call
4. `Iterator()` creates `CachingIterator` wrapping the base iterator
5. But the base iterator is ALREADY EXHAUSTED from step 1
6. CachingIterator caches 0 tuples
7. Probe phase gets 0 tuples

**The Real Issue**: `shouldCache` is set, but `Iterator()` hasn't been called yet to build the cache. The base iterator gets consumed BEFORE the cache is built. When HashJoin calls `Iterator()`, it's too late - the storage iterator is already at EOF.

## The Correct Fix

**Root Cause**: `ProjectIterator` was accessing `r.iterator` (the raw storage iterator) directly instead of calling `r.Iterator()` which respects caching.

The problem sequence:
1. Pattern returns `StreamingRelation` with `r.iterator = unboundIterator`
2. Something calls `Project([?e])` on that relation
3. `Project()` creates `ProjectIterator` wrapping `r.iterator` **directly**
4. Returns a **new** `StreamingRelation` with `ProjectIterator`
5. That new relation is iterated, calling `ProjectIterator.Next()`, which calls `unboundIterator.Next()`
6. This **consumes the original relation's iterator**!
7. Later, HashJoin tries to iterate the original relation
8. But its `unboundIterator` is already exhausted → 0 tuples

**The Fix**: Make `ProjectIterator` wrap the `Relation` itself, not the raw iterator. Call `relation.Iterator()` when needed.

### Changes Made:

**1. Updated `ProjectIterator` to store and call the Relation** (`iterator_composition.go:74-136`):
```go
type ProjectIterator struct {
    relation   Relation // Source relation (may be cached/materialized)
    source     Iterator // Lazily obtained from relation.Iterator()
    ...
}

func (it *ProjectIterator) Next() bool {
    // Lazily get iterator from relation on first call
    if it.source == nil {
        it.source = it.relation.Iterator()
    }
    ...
}
```

**2. Updated `StreamingRelation.Project()` to pass relation** (`relation.go:1087-1093`):
```go
projIter := NewProjectIterator(r, r.columns, columns)  // Pass r, not r.iterator
```

**3. Fixed `CachingIterator.signalComplete()` race condition** (`relation.go:218-236`):

Multiple `CachingIterator` instances can share the same `cacheComplete` channel. Without proper synchronization, both could try to close it, causing "close of closed channel" panic.

```go
func (ci *CachingIterator) signalComplete() {
    if ci.signaled {
        return
    }

    ci.mu.Lock()
    // Check if we need to close the channel (first iterator to finish)
    shouldClose := *ci.cachingInProgress
    if shouldClose {
        *ci.cachingInProgress = false
    }
    ci.mu.Unlock()

    // Only close if we were the first to mark caching complete
    if shouldClose {
        close(ci.cacheComplete)
    }
    ci.signaled = true
}
```

## Test Results

**TestDebugBasicQuery**: ✅ PASS

Debug output confirms correct behavior:
```
[StreamingRelation.Iterator] Called: shouldCache=true, cache=false (len=0)
[StreamingRelation.Iterator] First call with caching enabled
[unboundIterator.Next] First call for pattern: [?e :person/name ?name]
[unboundIterator.Next] Matched datom #1
[HashJoin] Left is StreamingRelation: shouldCache=true, iteratorCalled=true, cache len=1
[HashJoin] Probe phase complete: probed 1 tuples, found 1 matches, produced 1 results
Join result size: 1
```

The sequence is now correct:
1. ProjectIterator calls `relation.Iterator()` (first call, builds cache with 1 tuple)
2. HashJoin calls `relation.Iterator()` (cache already built, returns cached data)
3. Probe gets 1 tuple → 1 match → Correct result!

### Potential Fixes

If the issue is iterator consumption:
- Ensure `Materialize()` is called **before** any `Iterator()` calls
- Check `materializeRelationsForPattern()` is called at the right time
- Verify `shouldCache=true` actually triggers caching in `Iterator()`

If the issue is in `CachingIterator`:
- Check `cache` slice is correctly populated
- Verify `cacheIdx` correctly iterates over cache
- Ensure `cacheComplete` channel is properly signaled

If the issue is in `HashJoin`:
- Verify build phase is populating hash table
- Check probe phase is looking up correct keys
- Ensure `TupleKey.Equal()` correctly handles Identity pointers

## Related Documents

- `LAZY_MATERIALIZATION_PLAN.md` - Architecture and design of lazy materialization
- `STREAMING_ITERATOR_INVESTIGATION.md` - Root cause analysis of previous iterator issues
- `CRITICAL_PERFORMANCE_INSIGHT.md` - Iterator reuse issues
- `docs/bugs/resolved/BUG_EXPRESSION_ONLY_PHASES.md` - Similar phase execution bug

## Key Insights

**This is NOT an Identity comparison bug**. Identity comparison works perfectly.

**This IS a lazy materialization iterator consumption bug**:
1. Pattern results from storage are `StreamingRelation` with single-use iterators
2. `materializeRelationsForPattern()` is called to prepare relations for reuse
3. **BUG**: Materialized relations are stored in `availableRelations`, but pattern results are added to `independentGroups` BEFORE this materialization
4. When `Collapse()` joins relations, it uses the unmaterialized versions
5. The first iterator access (binding-based filtering) consumes the storage iterator
6. The second iterator access (join probe) gets an empty iterator

**The Fix**: Ensure pattern results are materialized BEFORE adding to `independentGroups`, or replace unmaterialized relations with materialized versions before collapse.

**Code Locations**:
- `executor_sequential.go:57` - `materializeRelationsForPattern()` called
- `executor_sequential.go:122-131` - Pattern results added to `independentGroups` and collapsed
- Problem: Materialization happens on `availableRelations`, but `independentGroups` uses original relations

## Architectural Observation: Unnecessary Materialization in Legacy Executor

**Discovered**: 2025-10-16 during ExecutorOptions preservation fix

### The Issue

The legacy executor (`executor.go:344-355`) materializes phase results between every phase:

```go
// Materialize phase result to avoid iterator consumption issues
var tuples []Tuple
it := phaseResult.Iterator()
for it.Next() {
    tuples = append(tuples, it.Tuple())
}
it.Close()
currentResult = NewMaterializedRelationWithOptions(phaseResult.Columns(), tuples, opts)
```

**This materialization is unnecessary** given the ProjectIterator fix and lazy materialization architecture.

### Why It's Unnecessary

With the ProjectIterator fix (commit 5e64b1e):
1. `applyExpressionsAndPredicates()` returns a StreamingRelation with `shouldCache=true`
2. The first `Iterator()` call builds the cache via `CachingIterator`
3. Subsequent `Iterator()` calls replay from the cache
4. The iterator can be safely reused without manual materialization

The comment "avoid iterator consumption issues" refers to the **exact bug that ProjectIterator fixed**.

### Performance Impact

This unnecessary materialization:
- Forces full materialization of every phase result
- Defeats the purpose of lazy materialization
- Allocates memory eagerly instead of on-demand
- Creates intermediate copies that could be avoided

### The Fix

The materialization block could be replaced with:
```go
currentResult = phaseResult  // Keep as StreamingRelation with lazy cache
```

The StreamingRelation would:
- Build its cache on first iteration (lazy)
- Replay from cache on subsequent iterations
- Only materialize when actually consumed

### Why Not Fixed Yet

This change requires careful testing because:
1. The legacy executor is still the default path
2. Many tests depend on current behavior
3. Need to verify all phase-to-phase handoffs work with StreamingRelations
4. Should probably be done as part of executor consolidation

### Related Code

- `executor.go:344-355` - The unnecessary materialization
- `executor_sequential.go` - Already uses lazy materialization correctly
- `relation.go:748-796` - CachingIterator implementation that makes this safe
