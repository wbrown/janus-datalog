# Iterator Seek Optimization Plan

## Problem Summary

The current implementation performs poorly when:
1. We have many binding tuples (e.g., 3,900 bars for a symbol)
2. Each binding requires either:
   - Opening/closing a new iterator (without reuse): ~280x slower
   - Seeking to a new position (with reuse): still ~100x slower
3. Predicate pushdown makes this WORSE because it adds evaluation overhead without reducing seeks

The root cause: **3,900 BadgerDB Seek operations are extremely expensive**.

## Solution Strategy

### Option 1: Batch Scanning (Recommended - Short Term)
**Idea**: Instead of seeking to each binding value individually, scan ranges and collect matches in memory.

#### Implementation:
1. **Group consecutive binding values**
   - When we have `[?b :price/time ?t]` with 3,900 ?b values
   - These entity IDs might be clustered (e.g., "bar-1-0" through "bar-30-389")
   - Group into ranges that are close together in the index

2. **Scan ranges instead of seeking to points**
   ```go
   // Instead of:
   for each binding in 3900 bindings {
       iterator.Seek(binding)
       scan_for_matches()
   }
   
   // Do:
   ranges = group_into_ranges(bindings)  // e.g., 30 ranges of ~130 consecutive IDs
   for each range {
       iterator.Scan(range.start, range.end)
       filter_for_bindings_in_memory()
   }
   ```

3. **Benefits**:
   - Reduces seeks from 3,900 to ~30-100 (depending on clustering)
   - Leverages BadgerDB's sequential read performance
   - Works with existing storage layer

### Option 2: Query Planner Optimization (Recommended - Medium Term)
**Idea**: Push predicates into the query planner to reduce initial binding sets.

#### Implementation:
1. **Analyze predicates during planning**
   - Identify predicates that could reduce pattern matches
   - Example: `[(day ?t) 20]` could be pushed to the `:price/time` pattern

2. **Reorder pattern execution**
   ```datalog
   ;; Current execution:
   ;; 1. [?b :price/symbol CRWV]     -> 3,900 results
   ;; 2. [?b :price/time ?t]          -> 3,900 results  
   ;; 3. Filter: [(day ?t) 20]        -> 390 results
   
   ;; Optimized execution:
   ;; 1. [?b :price/time ?t] with day=20 constraint -> 390 results
   ;; 2. [?b :price/symbol CRWV]                    -> 390 results
   ```

3. **Pattern-level predicate integration**
   - Modify planner to attach constraints to patterns
   - Execute constrained patterns first when beneficial

### Option 3: Storage Layout Optimization (Long Term)
**Idea**: Add time-partitioned indices for time-series queries.

#### Implementation:
1. **Add composite indices**
   - Symbol+Date index: groups all bars for a symbol on a date
   - Time-bucketed index: partition by day/hour buckets

2. **Smart index selection**
   - Detect time-series query patterns
   - Route to optimized indices

### Option 4: Streaming Join Optimization (Alternative)
**Idea**: For patterns with many bindings, use merge-join instead of nested-loop join.

#### Implementation:
1. **Sort both relations by join key**
2. **Single parallel scan through both**
3. **Emit matches as we go**

## Recommended Implementation Order

### Phase 1: Batch Scanning (1-2 days)
```go
// In matcher_v2.go, modify reusingIterator

type rangeGroup struct {
    startKey []byte
    endKey   []byte
    bindings []executor.Tuple
}

func (it *reusingIterator) Next() bool {
    // Group consecutive bindings into ranges
    ranges := it.groupBindingsIntoRanges()
    
    for _, range := range ranges {
        // One scan per range instead of per binding
        iter := it.matcher.store.Scan(range.startKey, range.endKey)
        
        // Collect all datoms in range
        datoms := collectDatoms(iter)
        
        // Match against bindings in memory
        for _, binding := range range.bindings {
            for _, datom := range datoms {
                if matches(datom, binding) && satisfiesConstraints(datom) {
                    yield(datom)
                }
            }
        }
    }
}
```

**Expected improvement**: 10-50x faster for large binding sets

### Phase 2: Query Planner Integration (2-3 days)
```go
// In planner package

type PatternPlan struct {
    Pattern     Pattern
    Constraints []Constraint  // Add this
    Selectivity float64       // Add this
}

func (p *Planner) optimizePhase(phase *Phase) {
    // Estimate selectivity with constraints
    for _, pattern := range phase.Patterns {
        pattern.Selectivity = estimateWithConstraints(pattern)
    }
    
    // Sort by selectivity (most selective first)
    sort.Slice(phase.Patterns, func(i, j int) bool {
        return phase.Patterns[i].Selectivity < phase.Patterns[j].Selectivity
    })
}
```

**Expected improvement**: 10-100x for queries with selective predicates

### Phase 3: Benchmark and Tune (1 day)
1. Create benchmarks for different query patterns:
   - Few bindings with selective predicates
   - Many bindings with selective predicates  
   - Time-series queries
   - Cross-product queries

2. Tune thresholds:
   - When to use batch scanning vs. individual seeks
   - Range grouping distance threshold
   - When to materialize vs. stream

## Success Metrics

1. **Production query** (OHLC with date filter):
   - Current: ~11 seconds for day filter
   - Target: < 500ms
   - Stretch: < 100ms

2. **Memory usage**:
   - Should not increase by more than 2x
   - Streaming where possible

3. **General queries**:
   - No regression for queries with few bindings
   - Significant improvement for queries with many bindings + predicates

## Risk Mitigation

1. **Compatibility**: All changes internal to storage/executor layers
2. **Fallback**: Keep existing code paths, select based on binding count
3. **Testing**: Extensive testing with different data distributions
4. **Gradual rollout**: Feature flag for new optimizations

## Decision Point

**Recommend starting with Phase 1 (Batch Scanning)** because:
1. Addresses the immediate problem (3,900 seeks)
2. Localized change to storage layer
3. Benefits all queries with large binding sets
4. Can be implemented and tested quickly

Phase 2 (Query Planner) is important but requires more design work to get right.

## Next Steps

1. ✅ Approve this plan
2. ⬜ Implement batch scanning in `matcher_v2.go`
3. ⬜ Add benchmarks to measure improvement
4. ⬜ Test with production OHLC query
5. ⬜ Document tuning parameters