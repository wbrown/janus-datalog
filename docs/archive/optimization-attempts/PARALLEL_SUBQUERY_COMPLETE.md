# Parallel Subquery Execution - Complete Implementation

**Date**: October 2025
**Status**: ✅ Production-ready, 5.2× speedup achieved
**Implementation**: `datalog/executor/subquery.go`

## Problem Statement

### The OHLC Query Bottleneck

OHLC queries executed subqueries sequentially, causing massive overhead:

**Example workload** (from gopher-street):
- 147 hourly bars × 4-5 subqueries per bar = 588-735 subquery executions
- Sequential execution: 18.23 seconds
- Most time spent in overhead, not actual work

**Why sequential was slow**:
- Query planning overhead per execution
- Context setup/teardown
- No CPU parallelization opportunity

## Solution: Parallel Worker Pool

### Architecture

Transform RelationInput iteration to use worker pool:

```go
// Sequential (old)
for each binding tuple:
    execute subquery with tuple
    collect result

// Parallel (new)
create worker pool (runtime.NumCPU() workers)
for each binding tuple:
    submit work to pool
wait for all workers to complete
collect results
```

### Key Design Decisions

**1. Configuration**
```go
type SubqueryExecutionOptions struct {
    EnableParallelDecorrelation bool  // Default: true
    MaxWorkers int                    // Default: runtime.NumCPU()
}
```

**2. Work Distribution**
- Each tuple becomes independent work item
- Workers execute subqueries concurrently
- Results collected in thread-safe manner

**3. Query Plan Reuse**
- Plan query once, reuse across all iterations
- Massive savings vs re-planning for each tuple

**4. Thread Safety**
- Mutex-protected result collection
- Each worker gets isolated execution context
- No shared mutable state during execution

## Performance Results

### OHLC Queries (147 hourly bars)

| Metric | Sequential | Parallel | Improvement |
|--------|-----------|----------|-------------|
| Execution Time | 18.23s | 3.49s | **5.22× faster** |
| Time per Hour | 124.03 ms | 23.69 ms | **5.24× faster** |
| Memory | ~19.6 GB | ~19.3 GB | Comparable |
| Correctness | 147 rows | 147 rows | ✅ Identical |

**Why 5.2× on 10-core CPU?**
- Workload is CPU-bound (joins, aggregations)
- Minimal synchronization overhead
- Near-perfect parallelization (147 independent tasks)
- Query plan reuse eliminates planning overhead

### Benchmarks

```
BenchmarkOHLC_Sequential:     18.23s
BenchmarkOHLC_Parallel:        3.49s
Speedup: 5.22×
```

## Implementation Details

### Core Components

**1. Worker Pool** (`subquery.go:140-180`)
```go
func executeRelationInputParallel(
    ctx Context,
    subqueryPlan *planner.QueryPlan,
    inputs Relation,
    options SubqueryExecutionOptions,
) (Relation, error)
```

- Creates worker goroutines
- Distributes work via channels
- Collects results thread-safely

**2. Single Worker** (`subquery.go:182-220`)
```go
func executeSingleSubqueryIteration(
    ctx Context,
    subqueryPlan *planner.QueryPlan,
    inputBindings map[query.Symbol]interface{},
) (*MaterializedRelation, error)
```

- Executes one subquery with given bindings
- Isolated context per execution
- Returns materialized results

**3. Query Plan Reuse** (`subquery.go:90-100`)
- Plan generated once before parallel execution
- Shared read-only across all workers
- Eliminates per-iteration planning cost

### Thread Safety Guarantees

1. **Input iteration**: Main thread only
2. **Work submission**: Main thread via channel
3. **Execution**: Independent workers, no shared state
4. **Result collection**: Mutex-protected append
5. **Error handling**: First error wins, cancels remaining work

## Configuration

### Enable/Disable

**Via ExecutorOptions**:
```go
executor := NewExecutorWithOptions(matcher, ExecutorOptions{
    Subquery: SubqueryExecutionOptions{
        EnableParallelDecorrelation: true,  // Enable parallel
        MaxWorkers: 8,                      // Limit workers
    },
})
```

**Disable for debugging**:
```go
options.Subquery.EnableParallelDecorrelation = false
```

### When to Disable

Parallel execution should be disabled when:
- Debugging subquery issues (deterministic execution helpful)
- Running on single-core systems
- Memory-constrained environments
- Non-thread-safe matchers (though ours are safe)

## Testing

### Test Coverage

1. **Correctness tests** (`executor/subquery_test.go`)
   - Verifies identical results vs sequential
   - Tests with various binding counts
   - Validates tuple ordering

2. **Integration tests** (`executor/decorrelation_integration_test.go`)
   - Full OHLC query execution
   - Comparison with sequential results
   - Performance benchmarks

3. **Concurrency tests**
   - Race detector enabled
   - Stress testing with many concurrent executions

### Validation

All tests pass with parallel execution enabled:
```bash
go test -race ./datalog/executor
ok      github.com/wbrown/janus-datalog/datalog/executor
```

## Limitations and Future Work

### Current Limitations

1. **Worker pool per RelationInput**: Could share pool across query
2. **No work stealing**: Static work distribution
3. **Memory overhead**: Each worker needs execution context

### Future Optimizations

1. **Global worker pool**: Share across entire query execution
2. **Work stealing**: Balance load dynamically
3. **Streaming results**: Don't materialize all results upfront
4. **Batch size tuning**: Optimize work item granularity

## Compatibility

**Backward compatible**: Sequential execution still available via config flag

**Storage layer**: Works with all PatternMatcher implementations:
- MemoryPatternMatcher ✅
- BadgerMatcher ✅
- Custom implementations ✅

**Query features**: All Datalog features supported:
- Aggregations ✅
- Expressions ✅
- Predicates ✅
- Nested subqueries ✅

## Monitoring

### Annotations

Parallel execution adds metadata to execution context:
```
Subquery execution (parallel): 147 iterations
  Workers: 10
  Time: 3.49s
  Results: 147 tuples
```

### Debugging

Enable sequential mode for debugging:
```go
options.Subquery.EnableParallelDecorrelation = false
```

This provides deterministic execution order for issue reproduction.

## Conclusion

Parallel subquery execution delivers **5× performance improvement** for OHLC workloads with minimal code complexity and full backward compatibility. This optimization was critical for making Janus Datalog production-viable for time-series aggregation workloads in gopher-street.

**Key Achievement**: Transformed 18-second queries into 3-second queries through intelligent parallelization.
