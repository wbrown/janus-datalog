# Phase Reordering Design

**Date**: 2025-10-12
**Status**: Design Phase
**Feature**: Complete implementation of `EnableDynamicReordering`

---

## Problem Statement

The current Go planner creates phases based on symbol dependencies but doesn't reorder them after creation. Some implementations have a `reorder-plan-by-relations` function that optimizes phase ordering based on **symbol connectivity** (information flow).

**Current behavior:**
```go
phases := p.createPhases(...)  // Creates phases in dependency order
for i := range phases {
    p.optimizePhase(&phases[i])  // Optimizes patterns within each phase
}
// NO REORDERING - phases stay in creation order
```

**Desired behavior:**
```go
phases := p.createPhases(...)
if p.options.EnableDynamicReordering {
    phases = p.reorderPhasesByRelations(phases, inputSymbols)  // ← NEW
}
for i := range phases {
    p.optimizePhase(&phases[i])
}
```

---

## Algorithm (from Clojure Reference)

### Scoring Function

```
score = intersection_count + bound_intersections + (1 if not assertion else 0)
```

Where:
- **intersection_count**: Number of symbols this phase shares with resolved symbols
- **bound_intersections**: Number of those symbols that are actually bound (available)
- **assertion_penalty**: Assertion patterns (provenance joins) score lower

### Reordering Algorithm

```
Given: phases, initial_symbols
Output: reordered phases

resolved_symbols = initial_symbols
result = []

while phases remain:
    // Separate related from unrelated phases
    related = phases that have ANY symbol intersection with resolved_symbols
    unrelated = phases with NO symbol intersection

    if related is not empty:
        // Score and sort related phases
        scored = sort related by score (high to low)
        best = first(scored)

        // Add best phase to result
        result.append(best)
        resolved_symbols.add(best.provides)

        // Continue with remaining phases
        phases = rest(scored) + unrelated
    else:
        // No related phases - just take the first one
        result.append(first(phases))
        resolved_symbols.add(first(phases).provides)
        phases = rest(phases)

return result
```

**Key insight**: This is a **greedy information flow optimizer** - it tries to execute phases in an order that maximizes shared symbols between consecutive phases.

---

## Go Implementation Plan

### 1. Data Structures

```go
// PhaseScore tracks scoring information for phase ordering
type PhaseScore struct {
    Phase              *Phase
    IntersectionCount  int           // How many symbols overlap
    BoundIntersections int           // How many are bound
    Intersections      []query.Symbol // Which symbols intersect
    Score              int           // Total score
}
```

### 2. Helper Functions

```go
// scorePhase scores a phase against resolved symbols
func scorePhase(phase Phase, resolvedSymbols map[query.Symbol]bool) PhaseScore

// hasSymbolIntersection checks if phase shares any symbols with resolved
func hasSymbolIntersection(phase Phase, resolvedSymbols map[query.Symbol]bool) bool

// orderPhasesByScore sorts phases by their scores (descending)
func orderPhasesByScore(phases []Phase, resolvedSymbols map[query.Symbol]bool) []PhaseScore
```

### 3. Main Reordering Function

```go
// reorderPhasesByRelations reorders phases to maximize symbol connectivity
func (p *Planner) reorderPhasesByRelations(phases []Phase, initialSymbols map[query.Symbol]bool) []Phase {
    if len(phases) <= 1 {
        return phases
    }

    resolvedSymbols := make(map[query.Symbol]bool)
    for sym := range initialSymbols {
        resolvedSymbols[sym] = true
    }

    // If no initial symbols, bootstrap with first phase's symbols
    if len(resolvedSymbols) == 0 && len(phases) > 0 {
        for _, sym := range phases[0].Provides {
            resolvedSymbols[sym] = true
        }
    }

    var result []Phase
    remaining := phases

    for len(remaining) > 0 {
        // Separate related from unrelated
        var related, unrelated []Phase
        for _, phase := range remaining {
            if hasSymbolIntersection(phase, resolvedSymbols) {
                related = append(related, phase)
            } else {
                unrelated = append(unrelated, phase)
            }
        }

        var selectedPhase Phase
        var newRemaining []Phase

        if len(related) > 0 {
            // Score and select best related phase
            scored := orderPhasesByScore(related, resolvedSymbols)
            selectedPhase = *scored[0].Phase

            // Remaining = rest of related + unrelated
            for i := 1; i < len(scored); i++ {
                newRemaining = append(newRemaining, *scored[i].Phase)
            }
            newRemaining = append(newRemaining, unrelated...)
        } else {
            // No related phases - just take first
            selectedPhase = remaining[0]
            newRemaining = remaining[1:]
        }

        // Add selected phase to result
        result = append(result, selectedPhase)

        // Update resolved symbols
        for _, sym := range selectedPhase.Provides {
            resolvedSymbols[sym] = true
        }

        remaining = newRemaining
    }

    return result
}
```

---

## Test Strategy

### Test Case 1: Cross-Product Scenario

**Bad ordering** (creates cross-product):
```
Phase 1: [?person :person/name ?name]    // 1000 people
Phase 2: [?product :product/price ?price]  // 1000 products
Phase 3: [?person :bought ?product]       // 10 purchases
```
Result: 1000 × 1000 = 1,000,000 intermediate tuples

**Good ordering** (with reordering):
```
Phase 1: [?person :person/name ?name]    // 1000 people
Phase 2: [?person :bought ?product]       // 10 purchases (joins on ?person)
Phase 3: [?product :product/price ?price]  // 10 products (from phase 2)
```
Result: 1000 → 10 → 10 tuples

### Test Case 2: Multi-Branch Join

**Bad ordering**:
```
Phase 1: [?a :attr1 ?x]
Phase 2: [?b :attr2 ?y]
Phase 3: [?c :attr3 ?z]
Phase 4: [?a :connects ?b]  // Should be earlier!
Phase 5: [?b :connects ?c]  // Should be earlier!
```

**Good ordering**:
```
Phase 1: [?a :attr1 ?x]
Phase 2: [?a :connects ?b]  // Immediately joins with ?a
Phase 3: [?b :attr2 ?y]     // Now ?b is bound
Phase 4: [?b :connects ?c]  // Immediately joins with ?b
Phase 5: [?c :attr3 ?z]     // Now ?c is bound
```

### Test Case 3: Already Optimal

Query where phases are already in optimal order - reordering should not hurt.

```
Phase 1: [?e :type :person]
Phase 2: [?e :person/name ?name]  // Uses ?e
Phase 3: [?e :person/age ?age]    // Uses ?e
```

Should remain unchanged.

### Test Case 4: Disjoint Query Groups

Query with two independent subgraphs that should be kept separate.

```
Group A: person queries
Group B: product queries
```

Reordering should not interleave them.

---

## Benchmarks

Create benchmarks for each test case measuring:
- Execution time
- Peak memory usage
- Intermediate relation sizes
- Number of tuples processed

Compare:
- `EnableDynamicReordering: false` (current behavior)
- `EnableDynamicReordering: true` (with phase reordering)

---

## Success Criteria

1. **Correctness**: All test queries return identical results with/without reordering
2. **Performance**: At least one test case shows measurable improvement (>2× faster)
3. **No regressions**: No test case is >10% slower with reordering enabled
4. **Code quality**: Implementation follows Go idioms, no Java patterns
5. **Documentation**: PLANNER_OPTIONS_REFERENCE.md updated with results

---

## Implementation Order

1. ✅ Study Clojure algorithm (DONE)
2. ⏳ Design Go implementation (THIS DOC)
3. Implement helper functions (scorePhase, hasSymbolIntersection, etc.)
4. Implement reorderPhasesByRelations
5. Create test queries (bad vs good ordering)
6. Write unit tests
7. Create benchmarks
8. Integrate into planner.go
9. Update documentation

---

## Open Questions

1. **Should we special-case assertion patterns?** The Clojure planner gives them lower scores. Do we have similar patterns to penalize?

2. **What about expression outputs?** Phases that produce expression outputs might need special handling.

3. **Subquery phases?** Do decorrelated subquery phases need different scoring?

4. **Performance threshold?** What's the minimum improvement to justify enabling by default?

---

## References

- **Current Go planner**: `datalog/planner/planner.go`
- **Pattern grouping**: `datalog/planner/planner_patterns.go`
- **Design inspiration**: `docs/ideas/planner-improvements.md`
