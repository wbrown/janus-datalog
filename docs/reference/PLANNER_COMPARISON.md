# Planner Comparison: Old vs New

This document explains the architectural differences between the old (phase-based) and new (clause-based) query planners, their performance characteristics, and how to benchmark them.

**Last Updated**: October 2025

---

## Executive Summary

Both planners produce **phases** as their output format, but they differ fundamentally in **how** those phases are created:

- **Old Planner (Phase-Based)**: Groups clauses by type, then optimizes within phases
- **New Planner (Clause-Based)**: Optimizes clauses first, then phases them greedily

**Performance Results**:
- New planner produces **13% better plans** (measured using same executor)
- Planning overhead is **negligible** (1-15 microseconds vs milliseconds of execution)
- Combined with new executor: **2× faster on complex queries**

---

## Architectural Comparison

### Old Planner (Phase-Based)

**Location**: `datalog/planner/planner.go`

**Approach**:
1. Group clauses by **type** (patterns, expressions, predicates)
2. Create phases based on **pre-defined type groupings**
3. Optimize **within** each phase (predicate pushdown, join ordering)
4. Convert to `RealizedPlan` via `PlannerAdapter`

**Characteristics**:
- Rigid, predictable phase structure
- Optimization happens after phasing
- Compatible with old executor only (unless using `SetUseQueryExecutor(true)`)

**Code Path**:
```go
oldPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
    UseClauseBasedPlanner: false,
})
```

---

### New Planner (Clause-Based)

**Location**: `datalog/planner/planner_clause_based.go`, `datalog/planner/clause_phasing.go`

**Approach**:
1. Flatten query into **optimized clause list** (all types mixed)
2. Apply optimizations as **pure clause transformations** (future: semantic rewriting, decorrelation)
3. Phase the optimized clauses **greedily** using `createPhasesGreedy()`
4. Convert to `RealizedPlan` directly

**Greedy Phasing Algorithm** (`clause_phasing.go`):
```go
type ClausePhase struct {
    Clauses   []query.Clause    // What executes in this phase
    Available []query.Symbol    // Symbols available at phase start
    Provides  []query.Symbol    // New symbols this phase produces
}

// For each phase:
// 1. Find all clauses that CAN execute (have required symbols available)
// 2. Score each clause (prefer patterns, then bound expressions, etc.)
// 3. Select best clause, add its output symbols to available set
// 4. Repeat until no more clauses can execute
// 5. Start new phase with remaining clauses
```

**Characteristics**:
- Flexible, adaptive phase structure
- Optimization happens **before** phasing
- Better clause ordering within phases
- Produces `RealizedPlan` natively
- Compatible with both executors (via `SetUseQueryExecutor(true)`)

**Code Path**:
```go
newPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
    UseClauseBasedPlanner: true,
})
```

---

## Output Format: RealizedPlan

**Both planners produce the same output format**:

```go
type RealizedPlan struct {
    Query  *query.Query     // Original user query
    Phases []RealizedPhase  // Phases as Datalog query fragments
}

type RealizedPhase struct {
    Query     *query.Query           // Datalog query fragment for this phase
    Available []query.Symbol         // Symbols from previous phases
    Provides  []query.Symbol         // Symbols this phase produces
    Keep      []query.Symbol         // Symbols to pass to next phase
    Metadata  map[string]interface{} // Phase metadata (decorrelation hints, etc.)
}
```

The executor receives `RealizedPlan` and executes each phase sequentially, threading symbols between phases.

---

## Executor Compatibility

### Old Executor (Legacy Phase Executor)

**Location**: `datalog/executor/executor.go` (legacy path, line 170+)

**Compatible With**:
- Old planner ONLY (requires `PlannerAdapter`)

**Code Path**:
```go
exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: false,
})
// Uses old executor automatically
```

**Characteristics**:
- Multi-phase execution with phase-specific logic
- Separate handling for patterns, expressions, predicates
- Uses old `QueryPlan` format internally
- Cannot use new planner directly

---

### New Executor (QueryExecutor)

**Location**: `datalog/executor/query_executor.go`

**Compatible With**:
- Old planner (via `SetUseQueryExecutor(true)`)
- New planner (via `SetUseQueryExecutor(true)`)

**Code Path**:
```go
// Old planner → QueryExecutor
exec1 := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: false,
})
exec1.SetUseQueryExecutor(true)

// New planner → QueryExecutor
exec2 := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: true,
})
exec2.SetUseQueryExecutor(true)
```

**Characteristics**:
- Clause-by-clause streaming execution
- Universal "Query + Relations → Relations" interface
- Works with `RealizedPlan` from either planner
- More efficient on complex queries

---

## Benchmark Suite

**Location**: `datalog/executor/planner_executor_comparison_bench_test.go`

### What Each Benchmark Measures

#### 1. `BenchmarkPlannerOnly*` (planner_only_bench_test.go)
**Measures**: Planning overhead ONLY (no execution)
**Purpose**: Isolate planning time from execution time
**Result**: New planner is 37-88% faster at planning (but planning is negligible anyway)

```go
// Just plan, don't execute
oldPlanner.PlanQuery(q)  // 3-12 microseconds
newPlanner.PlanQuery(q)  // 1-7 microseconds
```

#### 2. `BenchmarkFullQueryOldVsNewPlanner`
**Measures**: Full query execution (planning + execution)
**Purpose**: Compare complete old vs new architecture
**Configuration**:
- Old: `UseClauseBasedPlanner: false` → old planner + old executor
- New: `UseClauseBasedPlanner: true` + `SetUseQueryExecutor(true)` → new planner + new executor

**Result**: 2× faster on OHLC queries (combined effect)

#### 3. `BenchmarkPlanQuality*` (NEW!)
**Measures**: Both planners through SAME executor (QueryExecutor)
**Purpose**: Isolate plan quality improvements from executor improvements
**Configuration**:
- Old planner: `UseClauseBasedPlanner: false` + `SetUseQueryExecutor(true)`
- New planner: `UseClauseBasedPlanner: true` + `SetUseQueryExecutor(true)`

**Result**: New planner produces 13% better plans

**Example**:
```go
// Simple join query
Old planner → QueryExecutor: 2.25 ms
New planner → QueryExecutor: 1.95 ms
// 13% improvement from better plan structure alone
```

#### 4. `BenchmarkFullQueryOldVsNewExecutor`
**Measures**: Old executor vs QueryExecutor
**Purpose**: Compare executor implementations
**Configuration**:
- Old: `UseClauseBasedPlanner: false` → old planner + old executor
- New: `UseClauseBasedPlanner: true` + `SetUseQueryExecutor(true)` → new planner + QueryExecutor

#### 5. `BenchmarkDecorrelationComparison`
**Measures**: QueryExecutor with/without decorrelation
**Purpose**: Measure decorrelation optimization impact
**Configuration**: Uses `EnableSubqueryDecorrelation` flag

#### 6. `BenchmarkOHLCFullComparison`
**Measures**: Full comparison on OHLC-style queries
**Purpose**: Realistic workload performance comparison

---

## Performance Results

### Planning Overhead (Negligible)

From `BenchmarkPlannerOnly`:

| Query Type | Old Planner | New Planner | Speedup |
|-----------|-------------|-------------|---------|
| Simple pattern | 3,940 ns | 2,048 ns | 52% faster |
| Single subquery | 4,382 ns | 758 ns | 83% faster |
| OHLC query | 11,546 ns | 1,673 ns | 86% faster |

**Conclusion**: Planning is 1-15 microseconds. Even though new planner is much faster at planning, this has **negligible impact** on total query time (milliseconds to seconds).

---

### Plan Quality (Significant)

From `BenchmarkPlanQuality` (both using QueryExecutor):

| Query Type | Old Planner | New Planner | Improvement |
|-----------|-------------|-------------|-------------|
| Simple join | 2.25 ms | 1.95 ms | **13% faster** |

**Conclusion**: The new planner produces **better-quality plans** that execute more efficiently. The greedy phasing algorithm creates phases with better clause ordering.

---

### Combined Performance (Most Impactful)

From `BenchmarkFullQueryOldVsNewPlanner`:

| Query Type | Old Architecture | New Architecture | Improvement |
|-----------|------------------|------------------|-------------|
| OHLC queries | ~4-8 seconds | ~2-4 seconds | **~2× faster** |

**Conclusion**: The 2× improvement comes from **both** better planning (13%) and better execution (remaining improvement from clause-by-clause streaming).

---

## Why The New Planner Is Better

### 1. Greedy Clause Selection
The new planner scores each clause and selects the best one that can execute with available symbols. This produces better join orders than the type-based grouping approach.

**Scoring Function** (`clause_phasing.go`):
```go
func scoreClause(clause query.Clause, available map[query.Symbol]bool) int {
    score := 0

    // Prefer patterns (data access)
    if _, ok := clause.(*query.PatternClause); ok {
        score += 100
    }

    // Prefer expressions with more bound inputs
    if expr, ok := clause.(*query.ExpressionClause); ok {
        boundInputs := countBoundSymbols(expr, available)
        score += boundInputs * 10
    }

    // Prefer predicates (filtering)
    if _, ok := clause.(*query.PredicateClause); ok {
        score += 50
    }

    return score
}
```

### 2. Optimize-First Approach
By optimizing clauses before phasing, the planner can make better global decisions. The old planner optimized within already-created phases, missing opportunities.

### 3. Adaptive Phase Structure
Phases adapt to the actual query structure rather than being constrained by pre-defined type groupings. This is especially beneficial for complex queries with mixed clause types.

---

## Running Benchmarks

### Compare Planning Overhead Only
```bash
go test -bench=BenchmarkPlannerOnly -benchmem ./datalog/executor/
```

### Compare Plan Quality (Same Executor)
```bash
go test -bench=BenchmarkPlanQuality -benchmem ./datalog/executor/
```

### Compare Full Architecture (Old vs New)
```bash
go test -bench=BenchmarkFullQueryOldVsNewPlanner -benchmem ./datalog/executor/
```

### Compare All Dimensions
```bash
go test -bench=Benchmark -benchmem ./datalog/executor/ > benchmark_results.txt
```

---

## Migration Path

### Current Status (October 2025)
- New planner is **production-ready** and **default**
- New executor (QueryExecutor) is **production-ready** and **default**
- Old executor still available for compatibility

### Recommended Configuration
```go
// Production: Use new architecture
exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: true,  // Use new planner
})
exec.SetUseQueryExecutor(true)    // Use new executor

// Enable decorrelation for subquery-heavy workloads
executor := executor.NewQueryExecutor(matcher, executor.ExecutorOptions{
    EnableSubqueryDecorrelation: true,
})
```

### Backward Compatibility
```go
// Legacy mode: Use old architecture
exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: false,  // Use old planner
})
// Automatically uses old executor
```

### Testing New Planner with Old Executor
**Not recommended**, but possible:
```go
exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: false,  // Old planner
})
exec.SetUseQueryExecutor(true)     // Force new executor
// Old planner → QueryExecutor (still works, slightly slower than new planner)
```

---

## Future Work

### Clause-Based Optimizations (TODOs in planner_clause_based.go)
1. **Semantic Rewriting**: Transform time predicates into index scans (as pure clause transformation)
2. **Decorrelation**: Batch identical subqueries (as pure clause transformation)
3. **CSE (Common Subexpression Elimination)**: Deduplicate expressions (as pure clause transformation)

All of these can be implemented as **clause transformations** in the optimize-first phase, before greedy phasing.

### Advanced Phasing
- Cost-based clause selection (use statistics)
- Look-ahead scoring (consider downstream impact)
- Parallel phase execution (when phases are independent)

---

## References

- **Implementation**: `datalog/planner/planner_clause_based.go`
- **Greedy Algorithm**: `datalog/planner/clause_phasing.go`
- **Benchmarks**: `datalog/executor/planner_executor_comparison_bench_test.go`
- **Architecture**: See [ARCHITECTURE.md](../../ARCHITECTURE.md)
- **Decorrelation**: See [docs/wip/DECORRELATION_NEW_EXECUTOR_PLAN.md](../wip/DECORRELATION_NEW_EXECUTOR_PLAN.md)
