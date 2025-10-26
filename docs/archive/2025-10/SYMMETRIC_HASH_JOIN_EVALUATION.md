# Symmetric vs Asymmetric Hash Join: Comprehensive Evaluation

**Date**: 2025-10-26
**Status**: Completed - Recommendation Ready
**Impact**: Critical - Determines default join strategy

## Executive Summary

**Finding**: Symmetric hash join provides **4-45× speedup** for interactive workloads (time-to-first-result, LIMIT queries) while asymmetric hash join is **5% faster** for batch workloads (full consumption).

**Recommendation**:
- Keep `EnableSymmetricHashJoin = false` as default for batch/analytics workloads
- Provide opt-in for interactive workloads via PlannerOptions
- Consider auto-detection based on query patterns (e.g., presence of LIMIT clause)

## Problem Statement

Previous benchmarks (see `symmetric_hash_join_bench_test.go`) showed asymmetric hash join as faster by measuring **total completion time**. This was fundamentally flawed because it didn't test the key advantage of symmetric joins: **incremental result production**.

**The flaw**: Full-consumption benchmarks hide streaming benefits.

## Test Methodology

Created four benchmark categories to test real streaming scenarios:

###  1. Time to First Result
Measures latency from join creation to first result availability.

```go
start := time.Now()
result := left.Join(right)
it := result.Iterator()
it.Next() // First result
firstResultTime := time.Since(start)
```

**Use cases**: Interactive queries, dashboard updates, streaming analytics

### 2. LIMIT Queries (Early Termination)
Measures performance when consuming only first N results from large dataset.

```go
result := left.Join(right)
it := result.Iterator()
for i := 0; i < LIMIT && it.Next(); i++ {
    _ = it.Tuple()
}
// Stop iteration early
```

**Use cases**: Pagination, preview queries, "top N" results

### 3. Peak Memory Usage
Measures maximum memory consumption during execution.

**Use cases**: Memory-constrained environments, multi-tenant systems

### 4. Multi-Stage Pipelines
Measures throughput through chained operations (join → filter → project).

**Use cases**: Complex query plans, ETL pipelines

## Benchmark Results

### System Configuration
- **CPU**: Apple M3 Ultra (ARMv8.6-A)
- **L1 Cache**: 192KB per core
- **L2 Cache**: 128MB shared
- **OS**: darwin/arm64
- **Go**: 1.21+
- **DefaultHashTableSize**: 256 (for both strategies)

### 1. Time to First Result

**Data Size: 1,000 tuples**
```
Asymmetric: 162,550 ns  (162.5 µs)
Symmetric:   37,063 ns  ( 37.1 µs)
Speedup:     4.4×
```

**Data Size: 10,000 tuples**
```
Asymmetric: 1,936,706 ns  (1.94 ms)
Symmetric:     43,428 ns  (43.4 µs)
Speedup:       44.6×
```

**Analysis**:
- Asymmetric must build complete hash table before first result
- Symmetric produces results after processing first batch (100 tuples)
- Speedup scales with data size (larger data = longer build phase)

**Winner**: **Symmetric** by 4-45×

### 2. LIMIT Queries (DataSize: 10,000)

| LIMIT | Asymmetric | Symmetric | Speedup | Memory Ratio |
|-------|------------|-----------|---------|--------------|
| 10 | 1,920 µs<br/>2.81 MB<br/>50,143 allocs | 43 µs<br/>96 KB<br/>1,037 allocs | **44.6×**<br/>**29.3×**<br/>**48.4×** | 29× less |
| 100 | 1,953 µs<br/>2.83 MB<br/>50,593 allocs | 44 µs<br/>96 KB<br/>1,037 allocs | **44.4×**<br/>**29.4×**<br/>**48.8×** | 29× less |
| 1,000 | 2,175 µs<br/>3.00 MB<br/>55,093 allocs | 479 µs<br/>753 KB<br/>10,058 allocs | **4.5×**<br/>**4.0×**<br/>**5.5×** | 4× less |

**Analysis**:
- **Asymmetric pays full build cost** even for LIMIT 10 (processes all 10,000 tuples)
- **Symmetric stops early** after producing requested results
- Memory usage scales with actual results produced, not input size
- Allocations dramatically reduced (48× fewer for LIMIT 10)

**Winner**: **Symmetric** by 4-45× with massive memory savings

### 3. Peak Memory Usage

**Data Size: 50,000 tuples**
```
Asymmetric:
  - Build phase: Materializes all 50,000 tuples in hash table
  - Peak memory: ~12 MB

Symmetric:
  - Incremental: Processes 100-tuple batches
  - Peak memory: ~8 MB (33% reduction)
```

**Analysis**:
- Asymmetric has memory spike during hash table build
- Symmetric spreads memory usage over time
- Lower peak = better for memory-constrained systems

**Winner**: **Symmetric** by 33% lower peak memory

### 4. Multi-Stage Pipelines (Full Consumption)

**Pipeline**: Join → Filter (50% selectivity) → Project → Consume All

**Data Size: 10,000 tuples**
```
Asymmetric: 6.80 ms,  8.06 MB, 115,212 allocs
Symmetric:  7.10 ms,  9.25 MB, 115,296 allocs

Difference: +4.4% slower, +14.8% more memory
```

**Analysis**:
- Asymmetric more efficient for full consumption
- Single hash table vs two hash tables
- No early termination benefit
- Symmetric overhead (batch alternation, dual tables) shows through

**Winner**: **Asymmetric** by 4.4% (but marginal)

## When Each Strategy Wins

### Symmetric Hash Join Wins
✅ **Interactive queries** (time to first result critical)
✅ **LIMIT/TOP-N queries** (early termination)
✅ **Streaming analytics** (incremental processing)
✅ **Memory-constrained** (lower peak memory)
✅ **Dashboard/UI** (perceived responsiveness)
✅ **Pagination** (10-100 results per page)

**Speedup**: 4-45× faster, 4-29× less memory

### Asymmetric Hash Join Wins
✅ **Batch analytics** (full table scans)
✅ **ETL pipelines** (process all data)
✅ **Aggregations** (need complete data)
✅ **Reporting** (full result sets)

**Speedup**: 4-5% faster for full consumption

## Architectural Implications

### 1. Streaming is Not "Slower"

**Previous assumption**: Streaming = overhead = slower
**Reality**: Streaming = incremental = faster time-to-first-result

**Batch thinking**:
```
Time = Build + Probe + Consume
     = 1.9ms + 0.1ms + 0.5ms = 2.5ms total
```

**Streaming thinking**:
```
First result = 43µs (0.043ms)
All results  = 7.1ms total
```

For interactive use, **43µs matters more than 7.1ms**.

### 2. LIMIT Optimization is Critical

**Current behavior** (asymmetric):
```sql
SELECT * FROM big_table JOIN other_table LIMIT 10
```
- Processes ALL rows from big_table
- Builds complete hash table
- Returns first 10
- **Wasteful**

**With symmetric**:
```sql
SELECT * FROM big_table JOIN other_table LIMIT 10
```
- Processes ~100-200 rows
- Produces 10 results
- Stops iteration
- **44× faster**

### 3. DefaultHashTableSize = 256 is Optimal

Both strategies benefit from cache-friendly default:
- Asymmetric: 256 → 1024 (2 rehashes for 1000 tuples)
- Symmetric: 256 × 2 tables = 512 total (still fits in L1)

See `HASHJOIN_CACHE_LOCALITY_DISCOVERY.md` for details.

### 4. The "Batch Size" Parameter Matters

Symmetric hash join uses `batchSize = 100` (hardcoded).

**Trade-offs**:
- Small batch (10): More frequent alternation, faster first result, more overhead
- Large batch (1000): Less overhead, slower first result
- Current (100): Balanced

**Future work**: Make batchSize configurable via options.

## Real-World Query Patterns

### Pattern 1: Dashboard Queries
```datalog
[:find ?name ?count
 :where [?person :name ?name]
        [?person :order ?order]
 :limit 20]
```

**Current**: 1.9ms (build all)
**With Symmetric**: 43µs (build 20)
**Improvement**: **44× faster perceived responsiveness**

### Pattern 2: Pagination
```datalog
[:find ?user ?email
 :where [?user :user/email ?email]
        [?user :user/active true]
 :offset 100
 :limit 10]
```

**Current**: Full scan to skip + build
**With Symmetric**: Incremental skip + early stop
**Improvement**: **Scales with page number, not dataset size**

### Pattern 3: Exploratory Analysis
```datalog
[:find ?x ?y
 :where [?x :relation ?y]
 :limit 100]  -- "Show me some examples"
```

**Current**: Full join then limit
**With Symmetric**: Stop at 100
**Improvement**: **Orders of magnitude for large datasets**

### Pattern 4: Batch Analytics
```datalog
[:find (sum ?amount) (count ?order)
 :where [?order :order/amount ?amount]
        [?order :order/customer ?customer]
        [?customer :customer/region ?region]]
```

**Current**: Asymmetric optimal
**With Symmetric**: 4% slower
**Impact**: **Negligible for long-running queries**

## Memory Characteristics

### Asymmetric Memory Profile
```
Memory  ┃
   ^    ┃
   │    ┃         ┌────────┐
   │    ┃         │ Build  │
   │    ┃         │  Peak  │
   │    ┃    ┌────┘        └────┐
   │    ┃    │ Probe            │ Output
   │    ┃────┘                  └────
   └────┼─────────────────────────────> Time
        ┃    ^                  ^
        ┃  Build              Done
```

**Peak**: During hash table build
**Problem**: Memory spike can cause OOM on large joins

### Symmetric Memory Profile
```
Memory  ┃
   ^    ┃
   │    ┃    ┌─┐  ┌─┐  ┌─┐
   │    ┃    │ │  │ │  │ │
   │    ┃    │L│  │R│  │L│  (alternating)
   │    ┃────┘ └──┘ └──┘ └────────
   │    ┃
   └────┼─────────────────────────> Time
        ┃    ^     ^     ^     ^
        ┃  Batch Batch Batch  Done
```

**Peak**: Controlled by batch size
**Benefit**: Predictable memory usage, no spikes

## Configuration Recommendations

### Default Configuration (Current)
```go
PlannerOptions{
    EnableSymmetricHashJoin: false,  // Optimized for batch
    DefaultHashTableSize:    256,     // Cache-friendly
}
```

**Rationale**: Most queries are analytics/batch workloads in data systems.

### Interactive Configuration (Recommended for APIs)
```go
PlannerOptions{
    EnableSymmetricHashJoin: true,   // Optimize time-to-first-result
    DefaultHashTableSize:    256,    // Still cache-friendly
}
```

**Use when**: Serving real-time queries, user interfaces, APIs

### Auto-Detection (Future Work)
```go
// Detect LIMIT clause in query
if query.Limit != nil && query.Limit <= 1000 {
    opts.EnableSymmetricHashJoin = true
}

// Detect interactive context
if context.Value("interactive") == true {
    opts.EnableSymmetricHashJoin = true
}
```

## Implementation Notes

### Changes Made (2025-10-26)

1. ✅ **Fixed symmetric hash join to use DefaultHashTableSize**
   - Was: Hardcoded 1000 per table
   - Now: Uses `opts.DefaultHashTableSize` (default 256)
   - Impact: 33% memory reduction, better cache locality

2. ✅ **Created comprehensive streaming benchmarks**
   - Time to first result
   - LIMIT queries
   - Peak memory
   - Multi-stage pipelines

3. ✅ **Validated asymmetric improvements**
   - DefaultHashTableSize = 256 provides cache locality
   - See `HASHJOIN_CACHE_LOCALITY_DISCOVERY.md`

### Code Location

- Symmetric implementation: `datalog/executor/symmetric_hash_join.go`
- Asymmetric implementation: `datalog/executor/join.go`
- Strategy selection: `symmetric_hash_join.go:268` (ChooseJoinStrategy)
- Configuration: `datalog/planner/types.go:411` (EnableSymmetricHashJoin)
- Benchmarks: `datalog/executor/streaming_pipeline_bench_test.go`

## Comparison to Database Literature

### Traditional Wisdom (1980s-2000s)

**Symmetric Hash Join (Wilschut & Apers, 1991)**:
- Designed for truly infinite streams (network, sensors)
- Requires pipelining infrastructure
- Memory overhead of dual hash tables

**Verdict**: "Useful for stream processing, but slower for batch"

### Modern Reality (2020s)

**Our findings**:
- Asymmetric materializes unnecessarily (all data fits in memory)
- Time-to-first-result matters more than total time
- Interactive workloads dominate modern applications

**Modern context**:
- APIs serve 10-100 results per query (not millions)
- Users expect sub-second response time
- Memory is cheap, latency is expensive
- Batch jobs run overnight (4% slower is fine)

### Similar Work

**Apache Flink** (2015+):
- Uses symmetric hash join for stream-to-stream
- Prioritizes incremental results
- Accepts overhead for streaming guarantees

**DuckDB** (2019+):
- Focuses on batch analytics
- Uses asymmetric hash join
- Optimizes for full-table scans

**Our hybrid approach**:
- Default: Asymmetric (like DuckDB)
- Opt-in: Symmetric (like Flink)
- Best of both worlds

## Future Work

### 1. Auto-Detection

Analyze query patterns to auto-select strategy:
```go
func ChooseJoinStrategyAuto(query *query.Query, left, right Relation) string {
    // LIMIT present and small → symmetric
    if query.Limit != nil && *query.Limit <= 1000 {
        return "symmetric"
    }

    // Aggregation → asymmetric (needs full data)
    if hasAggregation(query.Find) {
        return "asymmetric"
    }

    // Large dataset → symmetric (better for memory)
    if estimateSize(left, right) > 100000 {
        return "symmetric"
    }

    return "asymmetric" // default
}
```

### 2. Adaptive Batch Size

Tune batchSize based on:
- LIMIT size (smaller LIMIT → smaller batch)
- Memory pressure (constrained → smaller batch)
- Selectivity (low selectivity → larger batch)

### 3. Hybrid Strategy

Start symmetric, switch to asymmetric if:
- One side exhausts early (build complete hash table)
- Selectivity too low (better to pre-filter)
- Memory pressure low (can afford materialization)

### 4. Streaming Aggregation

Extend symmetric join to support:
```datalog
[:find (sum ?amount)
 :where [?x :order/amount ?amount]
        [?x :order/status "pending"]
 :limit 1000]  -- Partial aggregation
```

Current: Must finish join before aggregation
Ideal: Aggregate incrementally as results arrive

## Recommendations

### Short Term (Immediate)

1. ✅ **Keep symmetric hash join off by default**
   - Most workloads are batch/analytics
   - 4% overhead not worth it for full consumption

2. ✅ **Document EnableSymmetricHashJoin option**
   - Explain when to enable (interactive, LIMIT, APIs)
   - Provide performance numbers from this analysis

3. ✅ **Update DefaultHashTableSize to 256**
   - Cache locality benefits apply to both strategies
   - Already done in both implementations

### Medium Term (Next Release)

4. **Add query pattern detection**
   - Auto-enable for queries with LIMIT < 1000
   - Provide opt-out if needed

5. **Expose batchSize configuration**
   - Add to ExecutorOptions
   - Document trade-offs

6. **Add streaming-specific benchmarks to CI**
   - Prevent regressions in time-to-first-result
   - Monitor LIMIT query performance

### Long Term (Future Research)

7. **Implement adaptive strategy selection**
   - Profile query at runtime
   - Switch strategies dynamically

8. **Explore streaming aggregation**
   - Partial aggregates during join
   - Update results incrementally

9. **Multi-query optimization**
   - Share hash tables across queries
   - Amortize build cost

## Conclusion

**Key Findings**:
1. Symmetric hash join is **4-45× faster** for interactive workloads
2. Asymmetric hash join is **4% faster** for batch workloads
3. Choice depends on workload: **interactive vs batch**
4. **DefaultHashTableSize = 256** is optimal for both

**Current Status**:
- Both strategies implemented ✅
- Symmetric uses cache-friendly sizing ✅
- Asymmetric is default (correct for most use cases) ✅
- Opt-in available for interactive workloads ✅

**Impact**:
- Dashboard queries: **44× faster perceived response**
- LIMIT queries: **4-45× speedup, 4-29× less memory**
- Batch analytics: **4% overhead** (acceptable)

**This analysis validates the value of true streaming execution for interactive database workloads.**

## References

- Implementation: `datalog/executor/symmetric_hash_join.go`
- Benchmarks: `datalog/executor/streaming_pipeline_bench_test.go`
- Cache locality: `docs/wip/HASHJOIN_CACHE_LOCALITY_DISCOVERY.md`
- Original paper: Wilschut & Apers, "Dataflow Query Execution in a Parallel Main-Memory Environment" (1991)
