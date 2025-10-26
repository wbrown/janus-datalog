# Query Planner Improvements from Clojure Analysis

## Key Insights from Clojure Planner

The Clojure planner treats query optimization as an **information flow problem** rather than a **pattern selectivity problem**. This fundamental difference leads to better query plans.

## Current Go Planner Limitations

1. **Static Scoring**: We score patterns based on constants vs variables in isolation
2. **Fixed Phases**: Once patterns are assigned to phases, they don't move
3. **Local Optimization**: Each pattern is optimized independently
4. **Complex Weights**: Magic numbers (-800, +1000, etc.) that are hard to tune

## Proposed Improvements

### 1. Symbol Connectivity Scoring

Replace the current scoring with Clojure's approach:

```go
// Instead of:
// Entity: -800 for constant, +1000 for unbound
// Value: -500 for constant, +500 for unbound

// Use:
type SymbolScore struct {
    IntersectionCount     int  // How many symbols overlap with current phase
    BoundIntersections    int  // How many overlapping symbols are already bound
    IsAssertion          bool // Penalty for assertion patterns
}

func (s SymbolScore) Score() int {
    score := s.IntersectionCount + s.BoundIntersections
    if !s.IsAssertion {
        score++
    }
    return score
}
```

### 2. Dynamic Phase Reordering

Implement `reorder-plan-by-relations` algorithm:

```go
func reorderPlanByRelations(phases []Phase, providedSymbols map[Symbol]bool) []Phase {
    var resolved []Phase
    remaining := phases
    resolvedSymbols := providedSymbols
    
    for len(remaining) > 0 {
        // Score each remaining phase by symbol connectivity
        best := findMostConnectedPhase(remaining, resolvedSymbols)
        
        // Move best phase to resolved
        resolved = append(resolved, best)
        remaining = removePhase(remaining, best)
        
        // Update resolved symbols
        for _, sym := range best.Provides {
            resolvedSymbols[sym] = true
        }
    }
    
    return resolved
}
```

### 3. Information Flow Tracking

Track how information flows between phases:

```go
type PhaseFlow struct {
    Available map[Symbol]bool  // Symbols available from previous phases
    Provides  map[Symbol]bool  // Symbols this phase provides
    Keeps     map[Symbol]bool  // Symbols needed by later phases
    Carries   map[Symbol]bool  // Symbols to carry forward
}
```

### 4. Graph-Based Query Model

Model the query as a dependency graph:

```go
type QueryGraph struct {
    Nodes map[Symbol]*PatternGroup
    Edges []SymbolFlow  // Tracks which symbols flow between groups
}

type SymbolFlow struct {
    From     Symbol
    To       Symbol
    Symbols  []Symbol  // Which symbols flow along this edge
}
```

## Implementation Strategy

### Phase 1: Add Symbol Connectivity Scoring
1. Add `IntersectionCount` calculation to pattern scoring
2. Track bound vs unbound intersections
3. Compare performance with current scoring

### Phase 2: Dynamic Reordering
1. Implement `reorderPlanByRelations` after initial phase assignment
2. Allow phases to be reordered based on actual symbol availability
3. Measure impact on complex queries

### Phase 3: Full Information Flow
1. Build complete dependency graph
2. Use graph algorithms for optimal ordering
3. Consider implementing Clojure's elegant functional approach

## Expected Benefits

1. **Better Join Order**: Phases that share many symbols will be adjacent
2. **Reduced Intermediate Results**: Connected patterns process together
3. **Simpler Mental Model**: "Follow the data" instead of complex weights
4. **Easier Tuning**: One simple formula instead of many magic numbers

## Example Impact

Current approach might order patterns as:
```
Phase 1: [?e :price/symbol "AAPL"]     // High selectivity
Phase 2: [?e :price/time ?t]           // Less selective
Phase 3: [?e2 :price/symbol "GOOGL"]   // High selectivity
Phase 4: [?e2 :price/time ?t]          // Shares ?t with phase 2!
```

Symbol connectivity approach would recognize the shared `?t` and order as:
```
Phase 1: [?e :price/symbol "AAPL"]
Phase 2: [?e :price/time ?t]
Phase 3: [?e2 :price/time ?t]          // Same ?t, process together!
Phase 4: [?e2 :price/symbol "GOOGL"]   // Filter after join
```

This could reduce a 10,000 × 10,000 cross product to a much smaller join.

## Testing Approach

1. **Benchmark Current Planner**: Establish baseline for complex queries
2. **Implement Scoring Change**: Measure impact on plan quality
3. **A/B Testing**: Run both planners and compare:
   - Execution time
   - Memory usage
   - Intermediate result sizes
4. **Edge Cases**: Test with pathological queries that break assumptions