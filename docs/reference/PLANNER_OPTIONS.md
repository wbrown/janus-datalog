# Planner Options Reference

**Last Updated**: October 13, 2025
**Version**: Post-streaming unification

## Table of Contents

1. [Overview](#overview)
2. [Unified Configuration](#unified-configuration)
3. [Complete Options Reference](#complete-options-reference)
4. [Performance Guidance](#performance-guidance)
5. [Configuration Recipes](#configuration-recipes)
6. [Migration Guide](#migration-guide)

---

## Overview

Janus Datalog uses a single `PlannerOptions` struct to configure both query planning and execution behavior. This unified approach eliminates the need for separate ExecutorOptions and provides clean, centralized configuration.

### Key Principles

1. **Single Configuration Point**: All options flow through `PlannerOptions`
2. **Sensible Defaults**: Optimizations enabled based on production validation
3. **No Global State**: Configuration belongs to instances, not globals
4. **Options Propagation**: Settings flow through entire execution pipeline

---

## Unified Configuration

### The Combined Structure

```go
type PlannerOptions struct {
    // Query Planning Options
    EnableDynamicReordering     bool
    EnablePredicatePushdown     bool
    EnableSubqueryDecorrelation bool
    EnableParallelDecorrelation bool
    EnableCSE                   bool
    EnableSemanticRewriting     bool
    MaxPhases                   int
    EnableFineGrainedPhases     bool
    Cache                       *PlanCache

    // Executor Streaming Options
    EnableIteratorComposition   bool  // Lazy evaluation
    EnableTrueStreaming        bool  // No auto-materialization
    EnableSymmetricHashJoin    bool  // Stream-to-stream joins

    // Executor Parallel Options
    EnableParallelSubqueries bool  // Parallel subquery execution
    MaxSubqueryWorkers      int   // Worker pool size (0 = NumCPU)

    // Executor Join/Aggregation Options
    EnableStreamingJoins            bool
    EnableStreamingAggregation      bool
    EnableStreamingAggregationDebug bool
    EnableDebugLogging              bool
}
```

### Default Configuration

```go
func DefaultPlannerOptions() planner.PlannerOptions {
    return planner.PlannerOptions{
        // Query planning
        EnableDynamicReordering:     true,
        EnablePredicatePushdown:     true,
        EnableSubqueryDecorrelation: true,
        EnableParallelDecorrelation: true,
        MaxPhases:                   10,
        EnableFineGrainedPhases:     true,

        // Streaming (enabled by default)
        EnableIteratorComposition: true,
        EnableTrueStreaming:      true,
        EnableSymmetricHashJoin:  false,  // Conservative

        // Parallel execution
        EnableParallelSubqueries: true,
        MaxSubqueryWorkers:      0,  // Use all cores

        // Other optimizations
        EnableCSE:               false,  // Minimal benefit with parallel
        EnableSemanticRewriting: true,   // 2-6× on time queries
        EnableDebugLogging:      false,
    }
}
```

### How It Works

```go
// 1. Database creates executor with default options
func (d *Database) NewExecutor() *executor.Executor {
    opts := DefaultPlannerOptions()
    opts.Cache = d.planCache
    return executor.NewExecutorWithOptions(d.Matcher(), opts)
}

// 2. Executor configures itself from options
func NewExecutorWithOptions(matcher PatternMatcher, opts planner.PlannerOptions) *Executor {
    return &Executor{
        matcher: matcher,
        planner: planner.NewPlanner(nil, opts),
        options: convertToExecutorOptions(opts),
        // ... streaming and parallel settings from opts
    }
}
```

**Benefits**:
- Single configuration to manage
- Streaming enabled by default
- Clean architecture with no breaking changes

---

## Complete Options Reference

### Planning Options

#### EnableDynamicReordering
**Default**: `true`
**Performance**: 1-3µs overhead, prevents 10-1000× slowdowns
**When to Enable**: Complex queries with many joins
**When to Disable**: Simple queries (< 10 patterns)

**What it does**: Reorders query phases to maximize symbol connectivity using an "information flow" algorithm.

**Why it's enabled**: Prevents catastrophic cross-products in complex queries. The microsecond overhead is negligible compared to preventing disasters.

**Trade-offs**:
- ✅ Prevents accidental Cartesian products
- ✅ Optimizes complex join orders
- ⚠️ 30% overhead on very simple queries (see Performance Guidance)

**Related Code**:
- `datalog/planner/phase_reordering.go`
- Algorithm: Information flow with symbol connectivity scoring

#### EnablePredicatePushdown
**Default**: `true`
**Performance**: Small improvement, negligible overhead
**When to Enable**: Always
**When to Disable**: Never

**What it does**: Applies predicates as soon as their required symbols are available, reducing intermediate result sizes.

**Why it's enabled**: Universally beneficial optimization with minimal cost.

**Example**:
```datalog
[?e :event/value ?v]
[(> ?v 100)]  ; Applied immediately after ?v available
```

**Related Code**:
- `datalog/executor/predicate_classifier.go`
- `datalog/executor/join_conditions.go`

#### EnableSubqueryDecorrelation
**Default**: `true`
**Performance**: 10-100× speedup, no overhead
**When to Enable**: Always
**When to Disable**: Never

**What it does**: Merges correlated subqueries with identical signatures using Selinger's decorrelation algorithm (1979).

**Why it's enabled**: Massive performance improvement with zero downside.

**Example**:
```datalog
; User writes 4 separate subqueries
[(q [:find (max ?o) ...] $ ?s) [[?open]]]
[(q [:find (max ?h) ...] $ ?s) [[?high]]]
[(q [:find (max ?l) ...] $ ?s) [[?low]]]
[(q [:find (max ?c) ...] $ ?s) [[?close]]]

; Janus executes as single merged query
SELECT ?s, MAX(?o), MAX(?h), MAX(?l), MAX(?c)
FROM ... GROUP BY ?s
```

**Real-world impact**: Gopher-street queries improved 8.3× (see SUBQUERY_PERFORMANCE_ANALYSIS.md).

**Related Code**:
- `datalog/planner/decorrelation.go`
- Based on: Selinger et al. "Access Path Selection in a Relational Database" (1979)

#### EnableParallelDecorrelation
**Default**: `true`
**Performance**: 1.2-1.8× additional speedup on multi-core
**When to Enable**: Multi-core systems, complex queries
**When to Disable**: Single-core systems

**What it does**: Executes decorrelated merged queries in parallel using worker pools.

**Why it's enabled**: Significant benefit on modern multi-core systems with negligible overhead.

**How it works**:
- Uses `runtime.NumCPU()` workers by default
- Round-robin work distribution
- Channel-based coordination
- Deterministic ordering preserved

**Concurrency safety**: All race conditions fixed as of October 2025.

**Related Code**:
- `datalog/executor/parallel_decorrelation.go`
- `datalog/executor/worker_pool.go`

#### EnableCSE
**Default**: `false`
**Performance**: 1-3% improvement sequential, -1% with parallel
**When to Enable**: Single-threaded environments, expensive predicates
**When to Disable**: Parallel execution (default)

**What it does**: Common Subexpression Elimination - merges filter groups with identical structure.

**Why it's disabled**: Parallel execution removes parallelism opportunity. The 1% improvement isn't worth losing concurrency.

**When useful**:
- Expensive predicates evaluated multiple times
- Single-threaded execution
- Memory-constrained environments

**Related Code**:
- `datalog/planner/cse.go`
- See: `docs/archive/2025-10/CSE_FINDINGS.md`

#### EnableSemanticRewriting
**Default**: `true`
**Performance**: 2.6-5.8× speedup on time-filtered queries
**When to Enable**: Temporal queries with time extraction
**When to Disable**: Non-temporal queries (neutral impact)

**What it does**: Transforms expensive time extraction expressions into cheaper range constraints.

**Example transformation**:
```datalog
; Before: Extract year for every tuple, then filter
[(year ?time) ?y]
[(= ?y 2025)]

; After: Range constraint (much faster)
[(>= ?time #inst "2025-01-01")]
[(< ?time #inst "2026-01-01")]
```

**Benchmarks**:
- Year filter (33% selective): 2.6× faster
- Day filter (12.5% selective): 4.1× faster
- Hour filter (1.4% selective): 5.8× faster

**Note**: With decorrelation enabled, shows no additional speedup (both optimize same bottleneck). Still valuable for standalone queries.

**Related Code**:
- `datalog/planner/semantic_rewriting.go`
- See: `docs/archive/2025-10/SEMANTIC_REWRITING_FINDINGS.md`

#### MaxPhases
**Default**: `10`
**Performance**: Balances planning vs execution
**When to Increase**: Very complex queries with many patterns
**When to Decrease**: Simpler queries

**What it does**: Limits maximum number of phases in query plan.

**Why 10**: Empirically determined balance point between flexibility and overhead.

#### EnableFineGrainedPhases
**Default**: `true`
**Performance**: Prevents OOM, 5-10% overhead on simple queries
**When to Enable**: Complex queries, large intermediate results
**When to Disable**: Simple queries with few patterns

**What it does**: Creates selectivity-based phases to prevent memory exhaustion.

**Why it's enabled**: Insurance against OOM failures outweighs overhead.

**Trade-offs**:
- ✅ Prevents out-of-memory failures
- ⚠️ 5-10% overhead on simple queries

### Streaming Options

#### EnableIteratorComposition
**Default**: `true`
**Performance**: 1.5-2.5× faster, 50-99% less memory
**When to Enable**: Always
**When to Disable**: Debugging materialization issues

**What it does**: Enables lazy evaluation through composed iterators (FilterIterator, ProjectIterator, etc.).

**Why it's enabled**: Massive performance and memory improvements with full correctness.

**Architecture**:
```
Source → FilterIterator → ProjectIterator → Result
         ↑                ↑
         No materialization, pure iteration
```

**Related Code**:
- `datalog/executor/iterator_composition.go`
- See: `docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md`

#### EnableTrueStreaming
**Default**: `true`
**Performance**: 50-99% memory reduction for selective queries
**When to Enable**: Always
**When to Disable**: Debugging size calculations

**What it does**: Prevents auto-materialization in StreamingRelation, returns `-1` for unknown sizes.

**Why it's enabled**: True streaming semantics with dramatic memory savings.

**How it works**:
- `Size()` returns `-1` without consuming iterator
- `IsEmpty()` uses BufferedIterator for efficient check
- Only materializes when explicitly requested

#### EnableSymmetricHashJoin
**Default**: `false`
**Performance**: Enables stream-to-stream joins, slightly slower than standard
**When to Enable**: Full pipeline streaming, memory-constrained
**When to Disable**: Performance-critical joins (default)

**What it does**: Dual hash table architecture for incremental join processing without full materialization.

**Why it's disabled**: Conservative default. Standard hash join is faster when one side can be materialized.

**When useful**:
- Both join inputs are large streams
- Memory constraints prevent materialization
- True end-to-end streaming required

### Parallel Execution Options

#### EnableParallelSubqueries
**Default**: `true`
**Performance**: 6.9× speedup with in-memory storage
**When to Enable**: Multi-core systems
**When to Disable**: Single-core or debugging

**What it does**: Worker pool with bounded parallelism for subquery execution.

**Implementation**:
- Uses `runtime.NumCPU()` workers by default
- Query plan reuse across iterations
- Thread-safe result aggregation

#### MaxSubqueryWorkers
**Default**: `0` (unlimited, uses `runtime.NumCPU()`)
**When to Change**: Resource-constrained environments

**What it does**: Limits concurrent worker goroutines for subquery execution.

---

## Performance Guidance

### Understanding the Trade-offs

The October 2025 defaults are optimized for **complex queries** and **safety**. For simple queries, some optimizations add overhead:

#### Overhead Analysis (from Gopher-Street Benchmarks)

| Optimization | Simple Query Overhead | Complex Query Benefit |
|--------------|----------------------|----------------------|
| EnableDynamicReordering | 25-30% | Prevents 10-1000× slowdowns |
| EnableFineGrainedPhases | 5-10% | Prevents OOM failures |
| **Combined** | **30-40%** | **Safety against disasters** |

**Dataset**: 79-19,750 price bars, simple OHLC extraction queries

### When Defaults Are Perfect

✅ **Enable all defaults when**:
- Queries have > 10 patterns
- Risk of cross-products
- Complex join graphs
- Unknown query patterns (exploratory)
- Production safety is priority

### When to Optimize for Speed

⚠️ **Consider disabling reordering/fine-grained when**:
- Queries are simple (< 10 patterns)
- Hand-optimized queries
- Performance-critical path
- Queries are well-constrained

**Custom configuration for simple queries**:
```go
opts := storage.DefaultPlannerOptions()
opts.EnableDynamicReordering = false
opts.EnableFineGrainedPhases = false
exec := db.NewExecutorWithOptions(opts)
```

**Measured improvement**: 20-40% faster on simple queries (see DEFAULT_PLANNER_OPTIONS_FEEDBACK.md)

### Adaptive Optimization (Future)

**Proposed**: Automatically enable optimizations based on query complexity.

**Heuristics for "needs reordering"**:
1. Pattern count > 10
2. Disconnected components detected
3. High-degree join nodes
4. Nested aggregates or complex subqueries

**Implementation status**: Under consideration (see DEFAULT_PLANNER_OPTIONS_FEEDBACK.md)

---

## Configuration Recipes

### Maximum Performance (Simple Queries)

```go
opts := planner.PlannerOptions{
    // Disable overhead-inducing optimizations
    EnableDynamicReordering: false,
    EnableFineGrainedPhases: false,

    // Keep critical optimizations
    EnablePredicatePushdown:     true,
    EnableSubqueryDecorrelation: true,
    EnableParallelDecorrelation: true,
    EnableSemanticRewriting:     true,

    // Streaming enabled
    EnableIteratorComposition: true,
    EnableTrueStreaming:      true,

    // Parallel execution
    EnableParallelSubqueries: true,
    MaxSubqueryWorkers:      0,
}
```

**Use when**: Simple, hand-optimized queries with < 10 patterns.

### Maximum Safety (Complex Queries)

```go
opts := storage.DefaultPlannerOptions()
// Already optimized for safety - use as-is
```

**Use when**: Complex queries, exploratory analytics, unknown patterns.

### Memory-Constrained Environment

```go
opts := planner.PlannerOptions{
    // Enable all streaming
    EnableIteratorComposition: true,
    EnableTrueStreaming:      true,
    EnableSymmetricHashJoin:  true,  // Full pipeline streaming

    // Disable parallelism to save memory
    EnableParallelDecorrelation: false,
    EnableParallelSubqueries:   false,

    // Other defaults
    EnablePredicatePushdown:     true,
    EnableSubqueryDecorrelation: true,
}
```

**Use when**: Limited memory, single-core systems.

### Debug Configuration

```go
opts := planner.PlannerOptions{
    // Disable optimizations for clarity
    EnableDynamicReordering:     false,
    EnableSubqueryDecorrelation: false,
    EnableSemanticRewriting:     false,

    // Disable streaming for visibility
    EnableIteratorComposition: false,
    EnableTrueStreaming:      false,

    // Enable debug output
    EnableDebugLogging:              true,
    EnableStreamingAggregationDebug: true,
}
```

**Use when**: Debugging query behavior, understanding execution.

### Temporal Query Optimization

```go
opts := storage.DefaultPlannerOptions()
opts.EnableSemanticRewriting = true  // Already true, but emphasis
opts.EnableSubqueryDecorrelation = true
opts.EnableParallelDecorrelation = true
```

**Use when**: Time-based queries with year/month/day extraction.

**Expected**: 2-6× speedup on time-filtered queries.

---

## Migration Guide

### From Global Variables (Old)

**❌ OLD CODE** (no longer works):
```go
executor.EnableIteratorComposition = true
executor.EnableTrueStreaming = true
exec := executor.NewExecutor(matcher)
```

**✅ NEW CODE** (required):
```go
opts := storage.DefaultPlannerOptions()
opts.EnableIteratorComposition = true
opts.EnableTrueStreaming = true
exec := db.NewExecutorWithOptions(opts)
```

### From Separate ExecutorOptions (Old)

**❌ OLD CODE**:
```go
execOpts := executor.ExecutorOptions{...}
plannerOpts := planner.PlannerOptions{...}
// Duplicated configuration
```

**✅ NEW CODE**:
```go
opts := storage.DefaultPlannerOptions()
// Single configuration point
```

### Migration Checklist

1. ✅ Remove global variable assignments (`executor.EnableXYZ = true`)
2. ✅ Use `DefaultPlannerOptions()` as starting point
3. ✅ Customize as needed for workload
4. ✅ Use `NewExecutorWithOptions()` with combined options
5. ✅ Verify tests pass with new pattern

### Why This Change

1. **Concurrency safety**: Global variables break concurrent executor usage
2. **Proper architecture**: Configuration belongs to instances
3. **Simpler API**: Single options struct vs multiple
4. **Options propagation**: Matcher and executor settings stay in sync

---

## Performance Monitoring

### Key Metrics to Track

1. **Query Latency**: Monitor p50, p95, p99
2. **Memory Usage**: Peak and average per query
3. **Planning Time**: Time spent in planner vs executor
4. **Worker Utilization**: Parallel execution efficiency

### Annotation System

Enable detailed performance metrics:

```go
handler := func(event annotations.Event) {
    log.Printf("%s: %v", event.Name, event.Data)
}

ctx := executor.NewContext(handler)
result, err := exec.ExecuteWithContext(ctx, query)
```

**Event types**:
- `phase/complete` - Phase timing and tuple counts
- `join/hash` - Join sizes and reduction ratios
- `aggregation/executed` - Grouping and result counts
- `decorrelation/merged` - Subquery merging decisions

### Profiling

For deep performance analysis:

```bash
go test -bench=. -cpuprofile=cpu.prof -memprofile=mem.prof
go tool pprof cpu.prof
go tool pprof mem.prof
```

See `docs/archive/optimization-attempts/PROFILING_GUIDE.md` for details.

---

## Related Documentation

- **Architecture**: `ARCHITECTURE.md` - System architecture overview
- **Performance Status**: `PERFORMANCE_STATUS.md` - Current performance state
- **Streaming**: `docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md`
- **Subquery Performance**: `docs/archive/2025-10/SUBQUERY_PERFORMANCE_ANALYSIS.md`
- **Gopher-Street Feedback**: Archive of real-world performance analysis

---

## Source Documents

This reference consolidates:
- `PLANNER_OPTIONS_REFERENCE.md` - Original comprehensive guide
- `PLANNER_OPTIONS_UNIFIED.md` - Unification documentation
- `DEFAULT_PLANNER_OPTIONS_FEEDBACK.md` - Performance analysis from gopher-street

**All source documents can be removed from repository root.**