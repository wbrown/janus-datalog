# Smart Predicate Pushdown Plan

## The Problem

Currently, when querying for "CRWV bars on day 20":
1. We fetch ALL 7,800 bars for CRWV using AVET index
2. Filter to 390 bars for day 20 in memory
3. That's 20x overfetch!

The issue: **We can only use ONE index, but queries often have multiple selective predicates.**

## Current State

### What We Have
1. **Storage constraints** - Can evaluate predicates during scan (equality, range, time extraction)
2. **Predicate classifier** - Can identify which predicates to push
3. **Multiple indices** - EAVT, AEVT, AVET, VAET, TAEV
4. **Batch scanning** - Efficient range scanning

### What's Missing
1. **Cross-pattern predicate analysis** - Predicates that span multiple patterns
2. **Predicate combination** - Using predicates from different patterns together
3. **Smart index selection** - Choosing index based on predicate selectivity
4. **Secondary filtering** - Applying remaining predicates during scan

## Solution: Multi-Stage Predicate Pushdown

### Stage 1: Query Analysis (Planner Phase)

```go
type PatternPredicates struct {
    Pattern     *query.DataPattern
    
    // Predicates that can be pushed to this pattern
    Direct      []Predicate  // e.g., [(> ?price 100)] where ?price from this pattern
    Indirect    []Predicate  // e.g., [(= ?time X)] where ?time from another pattern on same ?entity
    
    // Selectivity estimates
    IndexSelectivity  float64  // How selective is the index choice
    PredicateSelectivity float64  // How much do predicates reduce results
}
```

### Stage 2: Cross-Pattern Predicate Propagation

When we have:
```clojure
[?b :price/symbol ?s]
[?b :price/time ?t]
[(day ?t) ?d]
[(= ?d 20)]
```

The planner should recognize:
1. Both patterns share `?b` (same entity)
2. The time predicate `[(= (day ?t) 20)]` can be pushed to the first pattern
3. Even though `?t` comes from pattern 2, we can add a constraint on `?b`

### Stage 3: Smart Index Selection

```go
func (m *BadgerMatcher) chooseIndexWithPredicates(
    pattern *query.DataPattern,
    constraints []StorageConstraint,
) (IndexType, []byte, []byte, []StorageConstraint) {
    
    // Analyze constraints to estimate selectivity
    symbolSelectivity := estimateSelectivity(pattern, constraints, AVET)
    timeSelectivity := estimateSelectivity(pattern, constraints, AEVT) 
    
    // Choose index that reduces the most data
    if symbolSelectivity < timeSelectivity {
        // Use AVET for symbol, apply time constraint during scan
        return AVET, symbolKey, symbolEnd, timeConstraints
    } else {
        // Use AEVT for time, apply symbol constraint during scan
        return AEVT, timeKey, timeEnd, symbolConstraints
    }
}
```

### Stage 4: Hybrid Filtering

During the scan, apply ALL constraints regardless of index choice:

```go
func (it *scanIterator) Next() bool {
    for it.iter.Valid() {
        datom := it.decodeDatom()
        
        // Apply ALL constraints, not just the ones for this index
        passesAll := true
        for _, constraint := range it.constraints {
            if !constraint.Evaluate(datom) {
                passesAll = false
                break
            }
        }
        
        if passesAll {
            it.current = datom
            return true
        }
        
        it.iter.Next()
    }
    return false
}
```

## Implementation Steps

### Step 1: Enhanced Pattern Analysis
```go
// In planner/planner.go
func analyzePatternPredicates(patterns []PatternPlan, predicates []Predicate) []PatternPredicates {
    result := make([]PatternPredicates, len(patterns))
    
    for i, pattern := range patterns {
        // Find predicates that directly reference this pattern's variables
        result[i].Direct = findDirectPredicates(pattern, predicates)
        
        // Find predicates that can be propagated via shared entity
        result[i].Indirect = findIndirectPredicates(pattern, patterns, predicates)
        
        // Estimate selectivity
        result[i].IndexSelectivity = estimateIndexSelectivity(pattern)
        result[i].PredicateSelectivity = estimatePredicateSelectivity(result[i])
    }
    
    return result
}
```

### Step 2: Entity-Based Predicate Propagation
```go
// Propagate predicates across patterns that share entities
func propagatePredicates(phase *Phase) {
    // Group patterns by shared entity variable
    entityGroups := groupByEntityVariable(phase.Patterns)
    
    for _, group := range entityGroups {
        // Collect all predicates affecting this entity
        allPredicates := collectEntityPredicates(group)
        
        // Distribute to all patterns in the group
        for _, pattern := range group {
            pattern.AvailableConstraints = allPredicates
        }
    }
}
```

### Step 3: Selectivity Estimation
```go
func estimateSelectivity(pattern *DataPattern, constraints []StorageConstraint) float64 {
    // Start with index selectivity
    selectivity := 1.0
    
    if pattern.A != nil {
        // Attribute is selective
        selectivity *= 0.1  // Assume 10 different attributes
    }
    
    if pattern.V != nil {
        // Value is selective
        selectivity *= 0.01  // Assume 100 different values per attribute
    }
    
    // Apply constraint selectivity
    for _, constraint := range constraints {
        switch c := constraint.(type) {
        case *TimeExtractionConstraint:
            if c.Op == "day" {
                selectivity *= 1.0/30  // ~30 days per month
            }
        case *RangeConstraint:
            selectivity *= 0.2  // Assume range covers 20% of values
        case *EqualityConstraint:
            selectivity *= 0.01  // Exact match is very selective
        }
    }
    
    return selectivity
}
```

### Step 4: Query Rewriting
```go
// Rewrite query to push predicates earlier
func rewriteWithPredicates(query *Query) *Query {
    for _, phase := range query.Phases {
        // Move predicates as close to their data source as possible
        for i, pattern := range phase.Patterns {
            // Find predicates that can be evaluated after this pattern
            availableVars := getAvailableVars(phase.Patterns[:i+1])
            pushable := findPushablePredicates(phase.Predicates, availableVars)
            
            // Attach to pattern for storage-level evaluation
            pattern.StorageConstraints = convertToStorageConstraints(pushable)
            
            // Remove from phase-level predicates
            phase.Predicates = removePredicates(phase.Predicates, pushable)
        }
    }
    return query
}
```

## Example: OHLC Query Optimization

### Before (Current Behavior)
```clojure
; Query
[?b :price/symbol ?s]
[?b :price/time ?t]
[(day ?t) ?d]
[(= ?d 20)]

; Execution
1. Fetch ALL 7,800 bars for symbol (AVET index)
2. Join with time pattern
3. Filter by day = 20
4. Result: 390 bars (20x overfetch!)
```

### After (With Smart Predicates)
```clojure
; Query (same)
[?b :price/symbol ?s]
[?b :price/time ?t] 
[(day ?t) ?d]
[(= ?d 20)]

; Execution
1. Analyze: Both patterns share ?b, day predicate is selective
2. Push day=20 constraint to BOTH patterns
3. Pattern 1: Fetch symbol bars WITH day=20 filter during scan
4. Result: 390 bars directly (no overfetch!)
```

## Performance Impact

### Expected Improvements
- **OHLC queries**: 20x reduction in fetched datoms (7,800 → 390)
- **Time-range queries**: 10-100x reduction depending on range
- **Multi-attribute queries**: 5-50x reduction

### Trade-offs
- **Planning overhead**: ~1-2ms additional planning time
- **Complexity**: More complex planner logic
- **Memory**: Need to track constraint propagation

## Success Metrics

1. **Production query test**: Should fetch 390 bars, not 7,800
2. **OHLC aggregation**: Sub-100ms for daily rollups
3. **Memory usage**: No increase despite smarter planning
4. **Correctness**: All existing tests still pass

## Implementation Priority

1. **Phase 1**: Entity-based predicate propagation (biggest win)
2. **Phase 2**: Selectivity estimation (optimize index choice)
3. **Phase 3**: Cross-pattern constraint generation
4. **Phase 4**: Statistics-based selectivity (requires data collection)

## Key Insight

**The index selection is fine. The problem is we're not using ALL available information when scanning.** By propagating predicates across patterns that share entities, we can dramatically reduce the data fetched from storage without needing compound indices or index intersection.