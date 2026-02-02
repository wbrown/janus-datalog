# RequiresCopy() Analysis and Fix Plan

## Problem Summary

VectorQuery has a 30.84% time regression (p=0.000, n=100) with NO change in allocations. Root cause: we added `copyTuple()` calls throughout the executor to protect against workspace reuse, but this copies ALL tuples including those that don't need it.

## Key Insight

`RequiresCopy()` should be based on the relation type's **own iterator behavior**, not what it wraps.

## Iterator Analysis

### Iterators that CREATE fresh tuples (RequiresCopy = false):

1. **sliceIterator** (MaterializedRelation): Returns `it.tuples[it.pos]` - stable reference into pre-built slice.

2. **ProductIterator** (ProductRelation):
   ```go
   func (pi *ProductIterator) Tuple() Tuple {
       var result Tuple  // NEW allocation each call
       for _, tuple := range l
           result = append(result, tuple...)
       }
       return result
   }
   ```

3. **hashJoinIterator**: Already copies internally at lines 66-67 and 89-91:
   ```go
   joinedCopy := make(Tuple, len(joined))
   copy(joinedCopy, joined)
   it.currentJoined = joinedCopy
   ```

4. **bufferedSliceIterator**: Returns from tuples slice - stable reference.

5. **CachingIterator**: Returns from cache slice - stable reference.

### Iterators that PASS THROUGH from source (inherit safety):

1. **UnionIterator**: `it.currentTuple = tuple` from underlying iterator - no copy
2. **PrependedIterator**: `it.currentTuple = it.restIter.Tuple()` - no copy
3. **OrFallbackIterator**: `it.currentTuple = branchIter.Tuple()` - no copy (sometimes projects which creates new)
4. **CountingIterator**: `return i.inner.Tuple()` - direct passthrough
5. **projectedIterator**: Sometimes projects (new tuple), sometimes passes through

### Iterators that USE WORKSPACE (RequiresCopy = true):

These are in the **storage layer**, not executor:
- `matcherIteratorReusing`
- `matcherIteratorNonReusing`
- `matcherIteratorUnbound` (two types)
- `hashJoinIterator` in storage (different from executor's)

## The StreamingRelation Problem

`StreamingRelation` is used for TWO different purposes:
1. Wrapping storage iterators that USE workspace → needs copy
2. Wrapping join/other iterators that DON'T use workspace → no copy needed

Current code marks ALL StreamingRelation as `RequiresCopy() = true`, causing unnecessary copies.

## Proposed Fix

Add `requiresCopy bool` field to `StreamingRelation`:

```go
type StreamingRelation struct {
    columns      []query.Symbol
    iterator     Iterator
    options      ExecutorOptions
    requiresCopy bool  // NEW: set at construction
    // ...
}

func NewStreamingRelationWithOptions(columns []query.Symbol, iterator Iterator, opts ExecutorOptions, requiresCopy bool) *StreamingRelation {
    return &StreamingRelation{
        columns:      columns,
        iterator:     iterator,
        options:      opts,
        requiresCopy: requiresCopy,
    }
}

func (r *StreamingRelation) RequiresCopy() bool {
    return r.requiresCopy
}
```

Then update call sites:
- Storage layer creates with `requiresCopy: true`
- Executor joins create with `requiresCopy: false` (they already copy internally)

## Relation RequiresCopy Summary

| Relation Type | RequiresCopy | Reason |
|--------------|--------------|--------|
| MaterializedRelation | false | Tuples stored in stable slice |
| StreamingRelation | **depends** | Based on underlying iterator |
| ProductRelation | false | Creates new tuple each call |
| UnionRelation | false | Copies internally if source.RequiresCopy() |
| PrependedRelation | false | firstTuple safe; copies from rest if needed |
| OrFallbackRelation | false | Copies internally if branch.RequiresCopy() |
| StreamingAggregateRelation | false | Materializes internally |

## Files Modified So Far

1. `relation.go`: Added `RequiresCopy() bool` to interface, implemented for MaterializedRelation (false), StreamingRelation (true), ProductRelation (true - needs fix to false)

2. `union_relation.go`: Added RequiresCopy() = true

3. `or_fallback_relation.go`: Added RequiresCopy() = true

4. `aggregation.go`: Added RequiresCopy() = true (needs fix - should be false after materialization)

5. `prepended_relation.go`: NOT YET DONE

## Key Principle: Wrapper Relations Delegate

Wrapper relations that pass through tuples from sources should:
1. Check `source.RequiresCopy()` when storing a tuple
2. Copy if the source requires it
3. Return `RequiresCopy() = false` because they guarantee safe output

Example for UnionIterator:
```go
func (it *UnionIterator) Next() bool {
    // ...
    tuple := it.currentIter.Tuple()
    if it.currentRelation.RequiresCopy() {
        tuple = copyTuple(tuple)
    }
    it.currentTuple = tuple
    // ...
}

func (ur *UnionRelation) RequiresCopy() bool {
    return false  // We guarantee safety by copying at the source boundary
}
```

This pushes the copy decision to the **earliest point** where we know whether it's needed, avoiding redundant copies downstream.

## Completed Work (Feb 2025)

### Phase 1: RequiresCopy() Implementation
1. ✅ Added `RequiresCopy() bool` to Relation interface
2. ✅ MaterializedRelation returns `false` (tuples in stable slice)
3. ✅ StreamingRelation returns `true` (wraps storage iterators)
4. ✅ ProductRelation returns `false` (creates fresh tuple each call via append)
5. ✅ StreamingAggregateRelation returns `false` (materializes internally)
6. ✅ PrependedRelation returns `true` (wraps raw Iterator, can't delegate)
7. ✅ UnionRelation, OrFallbackRelation return `true` (conservative)

### Phase 2: Join Hot Path Fix
Added `maybeCopy` closure in `HashJoinWithOptions` (join.go):
```go
needsCopy := buildRel.RequiresCopy()
maybeCopy := func(t Tuple) Tuple {
    if needsCopy { return copyTuple(t) }
    return t
}
```
All 8 copyTuple calls in hash table building now use maybeCopy.

### Phase 3: collectTuplesInto Helper
Created `collectTuplesInto(dest *[]Tuple, rel Relation)` in relation.go:
- Appends directly to destination slice (no intermediate allocation)
- Checks `rel.RequiresCopy()` once before loop
- No defer (would prevent inlining, though function still too complex to inline)

Updated call sites in executor.go to use collectTuplesInto.

### Benchmark Results

**VectorQuery Regression Recovery:**
- Original regression: +30.84%
- After RequiresCopy: +14.64%
- After collectTuplesInto: +5.07%

**Overall Impact (vs baseline):**
| Benchmark | Time | Memory | Allocs |
|-----------|------|--------|--------|
| SimpleQuery | -8.9% | **-15.9%** | -19% |
| JoinQuery | -3.6% | -6.2% | -7.8% |
| CardinalityManyQuery | +3% | ~ | ~ |
| VectorQuery | +1.7% | ~ | ~ |
| **geomean** | **-2.1%** | **-5.8%** | **-7.0%** |

### Remaining copyTuple Sites
Still using unconditional copyTuple (potential future optimization):
- query_executor.go:790, 1338
- executor_utils.go:79, 116, 320
- relation.go Materialize methods
- helpers.go, subquery.go, streaming_union.go, etc.

These are lower priority as the main hot path (joins) is now optimized.

## Copy Tracking Annotations

Added annotation infrastructure to track tuple copy decisions during joins:

### Event Type
`annotations.JoinBuildCopy` - emitted after hash join build phase completes

### Event Data
```go
map[string]interface{}{
    "copied":        int,  // Number of tuples that were copied
    "passthru":       int,  // Number of tuples that didn't need copying
    "requires_copy": bool, // Whether the build relation required copying
}
```

### Usage
Set `ExecutorOptions.Collector` to receive events:
```go
collector := annotations.NewCollector(handler)
opts := ExecutorOptions{Collector: collector}
joined := HashJoinWithOptions(left, right, joinCols, opts)
```

### Current Wiring (Complete)
- ✅ ExecutorOptions has `Collector *annotations.Collector` field
- ✅ join.go emits `JoinBuildCopy` event when collector is present
- ✅ BadgerMatcher.SetHandler() creates collector in options
- ✅ SourceRouter.SetHandler() propagates to underlying matchers
- ✅ ExecuteRealized() copies collector from context to options
- ✅ Full E2E test verifies annotation flows through queries

## Future Work (Lower Priority)

1. Implement delegation pattern for wrapper relations (UnionRelation, OrFallbackRelation, PrependedRelation)
   - Copy at boundary when source.RequiresCopy()
   - Return false since output is then guaranteed safe
2. Add requiresCopy parameter to StreamingRelation constructor for non-storage sources
3. Update remaining copyTuple sites in query_executor.go, executor_utils.go

## VectorQuery Path Analysis

VectorQuery uses:
1. `matchVectorWithBindings` → returns `MaterializedRelation` (RequiresCopy=false)
2. Pattern matching → may return `StreamingRelation` wrapping storage iterator
3. Hash join combines them

The join was copying BOTH sides unnecessarily. With this fix, MaterializedRelation tuples won't be copied.
