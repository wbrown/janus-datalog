# Predicate Pushdown Design for Janus Datalog

## Problem Statement

Currently, the query executor fetches ALL datoms matching a pattern, then applies predicates to filter results in memory. This causes massive over-fetching:

```
Pattern: [?b :price/symbol ?s]  → Fetches 7,088 bars
Pattern: [?b :price/time ?t]    → Fetches times
Predicate: [(day ?t) ?d] [(= ?d 20)]  → Filters to 200 bars in memory

Result: 35x overhead (7,088 fetched / 200 needed)
```

## Solution: Push Predicates to Storage Layer

Transform predicates into storage-level constraints that filter DURING the scan, not after.

## Architecture Changes

### 1. Enhanced Pattern Matcher Interface

```go
// datalog/executor/pattern_match.go

// Current interface (unchanged for backward compatibility)
type PatternMatcher interface {
    Match(pattern *query.DataPattern, bindings Relations) (Relation, error)
}

// New extended interface
type PredicateAwareMatcher interface {
    PatternMatcher
    // New method that accepts predicates to push down
    MatchWithPredicates(
        pattern *query.DataPattern, 
        bindings Relations,
        predicates []Predicate,
    ) (Relation, error)
}
```

### 2. Predicate Classification

```go
// datalog/executor/predicate_pushdown.go

type PredicateClass int

const (
    PushableEquality PredicateClass = iota  // [(= ?x value)]
    PushableRange                            // [(> ?x min)] [(< ?x max)]
    PushableFunction                         // [(day ?t) ?d] where ?d is bound
    NotPushable                              // Complex predicates that need full context
)

// Analyze which predicates can be pushed to storage
func ClassifyPredicates(predicates []Predicate, pattern *query.DataPattern) map[Predicate]PredicateClass {
    result := make(map[Predicate]PredicateClass)
    
    for _, pred := range predicates {
        // Check if predicate references only variables from this pattern
        vars := pred.GetVariables()
        patternVars := pattern.GetVariables()
        
        if !varsSubsetOf(vars, patternVars) {
            result[pred] = NotPushable
            continue
        }
        
        // Classify based on predicate type
        switch p := pred.(type) {
        case *EqualityPredicate:
            if p.HasBoundValue() {
                result[pred] = PushableEquality
            }
        case *ComparisonPredicate:
            if p.IsSingleVarComparison() {
                result[pred] = PushableRange
            }
        case *FunctionPredicate:
            if p.CanPartiallyEvaluate() {
                result[pred] = PushableFunction
            }
        default:
            result[pred] = NotPushable
        }
    }
    
    return result
}
```

### 3. Storage Constraints

```go
// datalog/storage/constraints.go

// Constraints that can be evaluated at storage level
type StorageConstraint interface {
    // Check if a datom satisfies this constraint
    Evaluate(datom *datalog.Datom) bool
    
    // Get bounds for index scans (if applicable)
    GetScanBounds() (start, end []byte, isRange bool)
}

type EqualityConstraint struct {
    Position int         // 0=E, 1=A, 2=V, 3=T
    Value    interface{}
}

func (c *EqualityConstraint) Evaluate(datom *datalog.Datom) bool {
    switch c.Position {
    case 2: // Value position
        return valuesEqual(datom.V, c.Value)
    // ... other positions
    }
}

type RangeConstraint struct {
    Position int
    Min, Max interface{}
    IncludeMin, IncludeMax bool
}

func (c *RangeConstraint) Evaluate(datom *datalog.Datom) bool {
    switch c.Position {
    case 2: // Value position
        return compareValue(datom.V, c.Min) >= 0 && 
               compareValue(datom.V, c.Max) <= 0
    }
}

// Time extraction constraint for things like [(day ?t) ?d]
type TimeExtractionConstraint struct {
    Position  int
    ExtractFn string // "day", "month", "year", "hour", etc.
    Expected  interface{}
}

func (c *TimeExtractionConstraint) Evaluate(datom *datalog.Datom) bool {
    if c.Position != 2 {
        return false
    }
    
    t, ok := datom.V.(time.Time)
    if !ok {
        return false
    }
    
    switch c.ExtractFn {
    case "day":
        return t.Day() == c.Expected.(int)
    case "month":
        return int(t.Month()) == c.Expected.(int)
    case "year":
        return t.Year() == c.Expected.(int)
    // ... other functions
    }
}
```

### 4. Modified Storage Matcher

```go
// datalog/storage/matcher.go

func (m *BadgerMatcher) MatchWithPredicates(
    pattern *query.DataPattern,
    bindings executor.Relations,
    predicates []executor.Predicate,
) (executor.Relation, error) {
    
    // Convert predicates to storage constraints
    constraints := m.convertToStorageConstraints(pattern, predicates)
    
    // If we have range constraints, adjust scan bounds
    index, start, end := m.chooseIndex(/* ... */)
    for _, c := range constraints {
        if scanStart, scanEnd, isRange := c.GetScanBounds(); isRange {
            // Narrow the scan range
            if bytes.Compare(scanStart, start) > 0 {
                start = scanStart
            }
            if bytes.Compare(scanEnd, end) < 0 {
                end = scanEnd
            }
        }
    }
    
    // Scan with constraints
    iter, err := m.store.Scan(index, start, end)
    if err != nil {
        return nil, err
    }
    defer iter.Close()
    
    var results []datalog.Datom
    for iter.Next() {
        datom, err := iter.Datom()
        if err != nil {
            continue
        }
        
        // Apply storage constraints during scan
        satisfiesAll := true
        for _, constraint := range constraints {
            if !constraint.Evaluate(datom) {
                satisfiesAll = false
                break
            }
        }
        
        if satisfiesAll {
            results = append(results, *datom)
        }
    }
    
    return executor.NewDatomRelation(results, pattern), nil
}
```

### 5. Query Executor Integration

```go
// datalog/executor/executor_sequential_v2.go

func executePhaseWithPushdown(
    ctx context.Context,
    phase *Phase,
    bindings executor.Relations,
    matcher executor.PatternMatcher,
) (executor.Relations, error) {
    
    var results executor.Relations
    
    for _, pattern := range phase.Patterns {
        // Identify pushable predicates for this pattern
        pushable, remaining := splitPredicates(phase.Predicates, pattern)
        
        // Check if matcher supports predicate pushdown
        if pam, ok := matcher.(PredicateAwareMatcher); ok && len(pushable) > 0 {
            // Use enhanced matching with pushed predicates
            rel, err := pam.MatchWithPredicates(pattern, bindings, pushable)
            if err != nil {
                return nil, err
            }
            results = append(results, rel)
            
            // Remove pushed predicates from phase
            phase.Predicates = remaining
        } else {
            // Fall back to standard matching
            rel, err := matcher.Match(pattern, bindings)
            if err != nil {
                return nil, err
            }
            results = append(results, rel)
        }
    }
    
    // Apply remaining predicates (those that couldn't be pushed)
    for _, pred := range phase.Predicates {
        results = applyPredicate(results, pred)
    }
    
    return results, nil
}
```

## Example: Production Query Optimization

### Before Predicate Pushdown:
```
Query:
[:find ?open ?high ?low ?close
 :where [?b :price/symbol ?s]
        [?b :price/time ?t]
        [(day ?t) ?d] [(= ?d 20)]
        [(month ?t) ?m] [(= ?m 6)]
        [?b :price/open ?open]
        [?b :price/high ?high]
        [?b :price/low ?low]
        [?b :price/close ?close]]

Execution:
1. Fetch ALL bars for symbol (7,088 datoms)
2. Fetch times for all bars (7,088 datoms)
3. Filter to day=20, month=6 in memory (→ 390 datoms)
4. Fetch OHLC values (1,560 datoms)

Total fetched: 15,736 datoms
Actually needed: 1,560 datoms
Overhead: 10x
```

### After Predicate Pushdown:
```
Execution:
1. Fetch bars for symbol WITH time constraints pushed down
   - Storage evaluates day=20, month=6 during scan
   - Returns only 390 matching bars
2. Fetch OHLC values for those 390 bars (1,560 datoms)

Total fetched: 1,950 datoms
Actually needed: 1,560 datoms
Overhead: 1.25x

Improvement: 8x reduction in datoms fetched!
```

## Implementation Phases

### Phase 1: Equality Constraints (1 week)
- Implement `EqualityConstraint`
- Support `[(= ?var value)]` predicates
- Modify `BadgerMatcher` to accept constraints
- Test with production query

### Phase 2: Range Constraints (1 week)
- Implement `RangeConstraint`
- Support `[(> ?var min)]`, `[(< ?var max)]` predicates
- Optimize scan bounds based on ranges
- Benchmark improvements

### Phase 3: Function Constraints (2 weeks)
- Implement `TimeExtractionConstraint`
- Support `[(day ?t) ?d]`, `[(month ?t) ?m]`, etc.
- Handle partial evaluation of functions
- Production deployment

### Phase 4: Statistics & Cost Model (2 weeks)
- Track selectivity statistics
- Choose optimal predicate push order
- Cost-based optimization

## Performance Impact

### Expected Improvements:
- **Time-filtered queries**: 10-50x reduction in fetched datoms
- **Range queries**: 5-20x improvement
- **Equality filters**: 2-10x improvement
- **Complex predicates**: 3-15x improvement

### Queries That Benefit Most:
1. Time-series with date/time filters
2. Range scans (price > X, age < Y)
3. Status/type filters (status = "active")
4. Prefix matches on strings
5. Geo-spatial queries with bounding boxes

## Risks and Mitigations

### Risk 1: Incorrect Predicate Evaluation
**Mitigation**: Extensive testing comparing pushed vs non-pushed results

### Risk 2: Performance Regression for Some Queries
**Mitigation**: Cost model to decide when to push predicates

### Risk 3: Complex Predicates Can't Be Pushed
**Mitigation**: Hybrid approach - push what we can, evaluate rest in memory

## Success Metrics

1. **Correctness**: 100% identical results with/without pushdown
2. **Performance**: >5x reduction in datoms fetched for target queries
3. **Coverage**: >60% of predicates pushable in typical workload
4. **Latency**: <10% overhead for queries that don't benefit

## Alternative Approaches Considered

1. **Materialized Views**: Too specific, high storage overhead
2. **Custom Indices**: Not general purpose enough
3. **Query Result Caching**: Doesn't help with parameter changes
4. **Storage Partitioning**: Major architectural change

Predicate pushdown provides the best balance of:
- General applicability
- Implementation complexity
- Performance improvement
- Backward compatibility