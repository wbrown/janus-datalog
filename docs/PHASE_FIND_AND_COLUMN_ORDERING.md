# Phase Find and Column Ordering

## Discovery

While implementing phase reordering, we discovered a subtle but important architectural consideration regarding how phases handle column ordering.

## Order-Preserving Phases: An Alternative Approach

In some distributed Datalog planners, each phase has a `:find` field that specifies:
1. **Which columns** the phase should output
2. **In what order** those columns should appear

```clojure
find (if-let [find? (not-empty (intersection keep referred))]
       find?
       referred)
```

The `:find` is calculated as:
- Intersection of `:keep` (symbols needed later) and `:referred` (symbols this phase provides)
- If empty, then just `:referred`
- **Crucially**: Ordered according to the query's final find clause via `order-by-desired`

### Why Order Matters

Some implementations maintain **consistent column ordering** throughout query execution:
- All phases output columns in an order aligned with the final query's find clause
- Additional columns (needed for joins but not in final result) appear after the find columns
- This means **no reordering is needed at the end** - the final result is already in the correct order

## Go's Current Approach: Project Without Order

In our Go implementation, phases use `Keep` to specify which columns to retain, but don't specify order:

```go
phases[i].Find = findVars  // Same for all phases (global find clause)
```

We project to `phase.Keep` at phase boundaries (line 236-244 in `expressions_and_predicates.go`):

```go
projected, err := result.Project(phase.Keep)
```

But `Project()` doesn't guarantee any particular column order - columns end up in whatever order the relation operations naturally produce.

## The Positional Nature of Tuples

While we use **named columns** (`[]query.Symbol`), the underlying data is **positional**:

```go
type MaterializedRelation struct {
    columns []query.Symbol    // Named columns
    tuples  []Tuple           // [][]interface{} - positional arrays!
}
```

This means:
- Looking up a column by name finds its **index position**
- Accessing values requires `tuple[columnIndex]`
- **Reordering columns means creating new tuple arrays** with values copied to new positions

## Performance Implications

### With Arbitrary Column Order (Current Go):
1. Join two relations → create new tuples with merged column order
2. Project to `Keep` → create new tuples with subset of columns (arbitrary order)
3. Pass to next phase → repeat
4. Final projection to `query.Find` → **reorder tuples again** to match find clause

**Cost**: Multiple tuple array allocations and memory copies per phase.

### With Consistent Column Order (Clojure):
1. All phases maintain same column order (find clause order + extras)
2. Joins naturally align (same columns in same positions)
3. Projecting to `Keep` = dropping trailing columns (cheap)
4. Final projection to `query.Find` = **free** (already in right order)

**Cost**: Minimal - mostly just slice operations, no tuple copying.

## Current Status in Go

**What we set**:
- `phase.Find` = query's find variables (same for all phases)
- `phase.Keep` = symbols to carry forward (phase-specific)

**What we use**:
- `phase.Keep` for projection
- `phase.Find` is **never read** by the executor

**Impact**:
- Code works correctly (named columns ensure we get right values)
- But potentially less efficient due to tuple reordering overhead
- May not matter much with named column lookups and modern allocators

## Decision: Do We Need Phase-Specific Find?

### Arguments FOR implementing it:
- Reduces tuple reordering overhead
- Better cache locality (fewer intermediate allocations)
- Matches proven Clojure architecture
- Could be significant for large result sets (1M+ tuples)

### Arguments AGAINST:
- Current approach works and is simpler
- Named columns already incur lookup overhead
- Modern allocators are fast
- Optimization may be premature
- Would require changes throughout projection/join code

### Recommendation

**For now: Leave as-is** with this understanding documented.

**Reasoning**:
1. Current code is correct and passes all tests
2. Profile first before optimizing - we don't have data showing this is a bottleneck
3. If we see performance issues with large result sets, we know what to optimize
4. The reordering logic would add complexity to implement correctly

**If we implement it later**:
- Calculate `phase.Find` as ordered subset of `phase.Keep` matching query find clause order
- Modify `Project()` to accept target column order
- Modify join operations to maintain consistent column ordering
- Benchmark to verify the optimization actually helps

## Key Insight for Phase Reordering

When implementing phase reordering, we need to recalculate:
- ✅ **`Available`**: What symbols flow into each phase (from inputs + previous phases)
- ✅ **`Keep`**: What symbols to carry forward (used for projection)
- ⚠️ **`Find`**: Currently just set to query find vars (not used by executor)

The Clojure code calculates these **after reordering** in this sequence:
```clojure
(reorder-plan-by-relations provided)
(determine-phase-rel-carries pq)      ; Calculates :keep and :find
(determine-phases-provides)            ; Calculates :provides
```

Our Go equivalent:
```go
phases = p.reorderPhasesByRelations(phases, inputSymbols)
phases = updatePhaseSymbols(phases, findSymbols, inputSymbols)  // Recalc Available/Keep
```

We don't need to recalculate `Provides` because it's intrinsic to the phase's patterns (doesn't change with reordering).

## References

- Go planner: `datalog/planner/phase_reordering.go`
  - `updatePhaseSymbols()`: Recalculates Available/Keep after reordering
- Go executor: `datalog/executor/expressions_and_predicates.go`
  - Line 236-244: Projects to `phase.Keep` at phase boundaries
