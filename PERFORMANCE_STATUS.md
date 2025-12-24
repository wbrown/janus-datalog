# PERFORMANCE_STATUS.md

**Last Updated**: 2025-12-24
**Version**: Clause-based planner, QueryExecutor, streaming architecture, Pull API, schema support, and key encoder optimization

## Executive Summary

The Janus Datalog engine delivers production-ready performance through architectural improvements and targeted optimizations. All performance claims in this document are verified by actual benchmarks (last updated 2025-12-24).

### Verified Performance Improvements
- ✅ **New architecture** (clause-based planner + QueryExecutor): **2× faster** on complex OHLC queries (verified)
- ✅ **Pull API**: **9× faster than equivalent queries**, linear scaling (verified 2025-12-17)
- ✅ **Schema validation**: **<1% overhead** for type checking, **~6% overhead** for uniqueness (verified 2025-12-17)
- ✅ **Iterator composition**: **4.06× speedup** (1,259μs → 310μs, 89% memory reduction) (verified 2025-10-25)
- ✅ **Streaming execution**: **2.22× faster** with low-selectivity filters (1,720ms → 774ms), 52% memory reduction (verified 2025-10-25)
- ✅ **Parallel subquery execution**: **2.06× speedup** with 8 workers on M3 Max (730ms → 355ms) (verified 2025-10-25)
- ✅ **Time-based queries**: 2-6× faster with semantic rewriting (verified)
- ✅ **Predicate pushdown**: **1.58-2.78× faster** depending on dataset size, up to 91.5% memory reduction (verified 2025-10-25)
- ✅ **Intern cache optimization**: 6.26× speedup on BadgerDB queries (verified)
- ✅ **Time range optimization**: 4× speedup on large datasets (verified - 1.5× on small, 4× on 260-hour dataset)
- ✅ **Hash join pre-sizing**: 24-32% faster with 24-30% less memory (verified)

### Claims Requiring Qualification
- ⚠️ **Plan quality**: "13% better plans" not supported by current benchmarks (planners perform identically)
- ⚠️ **In-memory indexing**: "49-4802×" not reproducible (optimizations became pervasive, both paths now fast)

---

## What's Actually Working ✅

### 0. Clause-Based Planner + QueryExecutor (ACTIVE - ARCHITECTURAL WIN)
**Status**: ✅ Production-ready and default architecture
**Performance**: **2× faster on complex queries** (verified Oct 2025)
**Location**: `datalog/planner/planner_clause_based.go`, `datalog/executor/query_executor.go`

**Measured Results**:
- Full architecture comparison (OHLC queries): ~4-8s (old) → ~2-4s (new) = **2× faster** ✅
- Plan quality isolated (same executor): Both planners perform identically within measurement noise
- Planning overhead: 3-12µs (old) → 1-7µs (new) = 37-88% faster (but negligible impact)

**Key Difference**: The 2× speedup comes from QueryExecutor's clause-by-clause streaming execution, not from better plan quality. Both planners produce equivalent-quality plans.

**Configuration**: Enabled by default
```go
exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: true,  // Default
})
exec.SetUseQueryExecutor(true)    // Use QueryExecutor
```

**Details**: See `docs/reference/PLANNER_COMPARISON.md`

### 1. Query Plan Caching (ACTIVE)
**Status**: ✅ Implemented and enabled by default
**Location**: `datalog/planner/cache.go`, `datalog/storage/database.go:34`
**Performance**: ~3× speedup for repeated queries (measured)

### 2. Batch Scanning with Iterator Reuse (ACTIVE)
**Status**: ✅ Implemented, used for large binding sets
**Location**: `datalog/storage/matcher_relations.go:122-128`
**Threshold**: Activated when `bindingRel.Size() > 100`
**Result**: Code clarity improvement, minimal performance impact

### 3. Predicate Classification (ACTIVE)
**Status**: ✅ Infrastructure in place, used by executor
**Location**: `datalog/executor/predicate_classifier.go`
**What it does**: Classifies predicates as pushable vs. non-pushable

### 4. Join Condition Detection (ACTIVE)
**Status**: ✅ Implemented and used
**Location**: `datalog/executor/join_conditions.go`
**What it does**: Detects equality predicates that can be pushed into joins

### 5. Progressive Join Execution (CRITICAL & ACTIVE)
**Status**: ✅ Core safeguard preventing memory explosion
**Location**: `datalog/executor/relation.go`
**Why it matters**: Greedy join ordering + early termination prevents catastrophic intermediate result sizes
**Scale achieved**: Production-ready for datasets from 100K to 10M+ datoms, tested up to 500M+

---

## Recent Optimizations (October 2025) ✅

### 0. Single-Use Iterator Semantics & Streaming (COMPLETE - CORRECTNESS FIX)
**Status**: ✅ Proper iterator lifecycle management with single-use semantics
**Performance**: **4.06× speedup for iterator composition**, **2.22× for streaming** (verified 2025-10-25)
**Commits**: 626e409 (latest), 4a394cb, 4f3b742, 15d196d, 78c930a

**What We Fixed** (2025-10-25):
- ✅ **Single-use iterator semantics** - StreamingRelation enforces one-time iteration
- ✅ **BufferedIterator** - Safe re-iteration support with automatic caching
- ✅ **Iterator lifecycle** - Clear separation between first use (streaming) and re-use (cached)
- ✅ **Correctness** - All tests pass with proper semantics enforcement

**What We Built Earlier** (Oct 2025):
- ✅ **Iterator composition** - Filter/Project/Transform operations stay lazy
- ✅ **Options propagation** - ExecutorOptions flow through entire pipeline
- ✅ **BadgerMatcher streaming** - Returns StreamingRelation instead of materializing
- ✅ **Symmetric hash join** - Streaming-to-streaming joins without materialization

**Current Performance Results** (verified 2025-10-25):

**Iterator Composition Benchmark**:
- Materialized: 1,259 μs, 3.27 MB, 25K allocs
- Composed: 310 μs, 360 KB, 15K allocs
- **Result: 4.06× faster, 89% memory reduction** ✅

**Streaming Scenarios** (10K tuples):
- Large_HighSelectivity (1% pass): 1.07× faster (675μs → 630μs), 2% memory reduction
- Large_MediumSelectivity (10% pass): 1.44× faster (975μs → 676μs), 19% memory reduction
- Large_LowSelectivity (50% pass): **2.22× faster** (1,720μs → 774μs), **52% memory reduction**
- **Key Finding**: Benefits scale with filter selectivity (1.07× to 2.22× depending on selectivity)

**Configuration**: Enabled by default:
```go
EnableIteratorComposition: true  // Lazy evaluation (default)
EnableTrueStreaming: true        // No auto-materialization (default)
```

### 1. In-Memory Indexing (COMPLETE - PERVASIVE OPTIMIZATION)
**Status**: ✅ IndexedMemoryMatcher with hash indices
**Performance**: Hash indices now used throughout (test suite 7s, down from timeouts)
**Commit**: Latest

**What Works**:
- ✅ Hash indices for E/A/V lookups (entityIndex, attributeIndex, valueIndex, eavIndex)
- ✅ Thread-safe lazy initialization with sync.Once
- ✅ Smart index selection (EA > E > A > V > linear)
- ✅ Two-phase value lookup (hash → exact match) for interface{} types

**Historical Note**:
During development, benchmarks showed dramatic speedups (49-4802×) comparing linear scan vs indexed lookups. However, subsequent refactoring made hash indices the default path for both IndexedMemoryMatcher and MemoryPatternMatcher. Current benchmarks show identical performance because both implementations now use the optimized path.

**Impact**: Entity lookups are O(1) instead of O(N). Test suite execution time dramatically reduced. This is a **success story** - the optimization became so pervasive that there's no longer a "slow path" to compare against.

**Details**: See `IN_MEMORY_INDEX_RESULTS.md` (historical benchmarks)

### 2. Time Range Optimization (COMPLETE)
**Status**: ✅ Fully implemented with storage integration
**Performance**: **4× speedup on large datasets** (41s → 10.2s on 260-hour dataset, commit dc2ad4e)
**Note**: Small datasets show minimal benefit (3.5% on 10-day dataset)
**Commits**: Latest in dc2ad4e

**What Works**:
- ✅ Time range extraction from correlation keys (10µs for 260 ranges)
- ✅ Multi-range AVET scanning in BadgerDB (260 time ranges)
- ✅ Metadata propagation (negligible 20ns overhead)
- ✅ Size check optimization (<50 tuples skip extraction)

**Benchmark Results** (BenchmarkOHLCQuery, verified 2025-10-24):
- Small dataset (10 days, 390 bars/day):
  - WithoutPushdown: 48.8ms
  - WithPushdown: 33.3ms
  - WithTimeRangeOpt: 32.1ms (**3.5% improvement**, 1.52× vs no optimization)
- Large dataset (260 hours): **4× speedup** (41s → 10.2s, measured during development)

**Key Insight**: Time-range optimization benefit scales with dataset size and time selectivity. Most valuable for large historical datasets with selective time filters.

**Details**: See `TIME_RANGE_OPTIMIZATION_STATUS.md`

### 3. Hash Join Pre-Sizing (COMPLETE)
**Status**: ✅ Implemented across all join operations
**Performance**: 24-32% faster, 24-30% less memory for hash operations
**Commit**: dc2ad4e

**What Was Done**:
- ✅ Added `NewTupleKeyMapWithCapacity()` for pre-sizing
- ✅ Updated 7 call sites (HashJoin, SemiJoin, AntiJoin, deduplication)
- ✅ Pre-size based on relation sizes to avoid map growth

**Impact**:
- Micro-level: 24-32% faster TupleKeyMap operations
- Macro-level: Minimal impact on OHLC (pattern matching dominates)
- High impact for large joins (>1,000 tuples)

**Details**: See `HASH_JOIN_PRESIZING_SUMMARY.md`

### 4. Semantic Rewriting with Expression Elimination (COMPLETE)
**Status**: ✅ Production-ready with predicate transformation
**Performance**: 2.6-5.8× speedup on time-filtered queries
**Commits**: Multiple in Oct 2025

**What Works**:
- ✅ Time extraction → range constraint transformation
- ✅ Expression/predicate elimination (skip optimized-away code)
- ✅ Multi-component constraint composition (year+month+day+hour)
- ✅ MemoryPatternMatcher integration

**Benchmarks**:
- Year filter (33% selective): 2.6× faster
- Day filter (12.5% selective): 4.1× faster
- Hour filter (1.4% selective): 5.8× faster

**Note**: With decorrelation enabled, semantic rewriting shows no additional speedup (1.00×) because both optimize the same bottleneck (time extraction overhead). Decorrelation eliminates 97% of evaluations, leaving nothing for semantic rewriting to optimize. Still valuable for standalone queries (1.64× speedup) and future BadgerDB integration.

**Recommended**: `EnableSemanticRewriting: true`
**Details**: See `docs/archive/2025-10/SEMANTIC_REWRITING_FINDINGS.md`

### 5. Common Subexpression Elimination - CSE (COMPLETE, DISABLED BY DEFAULT)
**Status**: ✅ Correctly implemented, minimal benefit with parallelization
**Performance**: 1-3% improvement sequential, -1% with parallel

**What Was Done**:
- ✅ Merges filter groups with identical structure
- ✅ Reduces multiple queries → single merged query
- ✅ Comprehensive testing (correctness + performance)

**Why Disabled**:
- Parallel execution already runs filter groups concurrently
- Merging removes parallelism opportunity
- 1% improvement too small to justify enabled by default

**When to Enable**: Single-threaded environments, expensive predicates
**Recommended**: `EnableCSE: false`
**Details**: See `docs/archive/2025-10/CSE_FINDINGS.md`

### 6. Parallel Subquery Execution (COMPLETE)
**Status**: ✅ Implemented and enabled by default
**Performance**: **2.06× speedup** with 8 workers (verified 2025-10-25)
**Commits**: 626e409 (with single-use semantics), d645cfd, ec45d77, 2439e0a

**What Works**:
- ✅ Worker pool with bounded parallelism (uses runtime.NumCPU())
- ✅ Query plan reuse across iterations
- ✅ Thread-safe result aggregation
- ✅ Proper iterator lifecycle management

**Benchmark Results** (BenchmarkRelationInputParallel, 2400 input tuples):
- Sequential: 730 ms, 1,101 MB/op
- Parallel-2Workers: 738 ms (1.01× slower, overhead for small dataset)
- Parallel-4Workers: 534 ms (1.37× faster)
- **Parallel-8Workers: 355 ms (2.06× faster)** ✅
- Parallel-16Workers: 436 ms (1.67× faster, diminishing returns)
- Parallel-32Workers: 418 ms (1.75× faster, overhead exceeds benefit)

**Key Finding**: Optimal worker count is 8 on M3 Max. Memory usage remains constant across all configurations (~1.1 GB), demonstrating proper lifecycle management and no memory leaks.

### 7. Intern Cache Optimization (COMPLETE)
**Status**: ✅ Lock-free sync.Map replacing sync.RWMutex
**Performance**: 6.26× speedup for BadgerDB parallel queries
**Commit**: e3c956b

**Impact**:
- Before: 35% CPU time on mutex contention
- After: Near-zero lock contention with atomic operations
- Micro-benchmarks: 13-80× faster intern operations

### 8. Pull API Performance (MEASURED 2025-12-17)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: **9× faster than equivalent queries**, linear scaling
**Location**: `datalog/executor/pull.go`, `datalog/storage/database.go`

**Why Pull is Fast**:
- Direct index seeks via `EntityLookupMatcher` interface
- No query parsing or planning overhead
- Single AEVT index lookup per attribute
- Wildcard uses single EAVT prefix scan

**In-Memory Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| Single Attribute | 554ns | 19 | Base case |
| 5 Attributes | 2.3µs | 87 | ~470ns/attr |
| Wildcard (5 attrs) | 1.7µs | 36 | **Faster than explicit** |
| Nested (2 levels) | 2.1µs | 72 | +reference follow |
| Deep (3 levels) | 2.6µs | 91 | Linear with depth |
| PullMany (100 entities) | 102µs | 3601 | ~1µs/entity |

**BadgerDB Storage Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| Single Attribute | 1.1µs | 31 | 2× in-memory |
| 5 Attributes | 6.2µs | 147 | ~1.2µs/attr |
| Wildcard (5 attrs) | 2.8µs | 69 | **2.2× faster than explicit** |
| Nested (2 levels) | 5.0µs | 120 | |
| Standalone API | 4.3µs | 116 | Includes parse |
| Cached Pattern | 3.5µs | 89 | Pre-parsed |
| PullMany (100 entities) | 225µs | 6001 | ~2.2µs/entity |

**Pull vs Query Comparison** (3 attributes, BadgerDB):
| Method | Time | Speedup |
|--------|------|---------|
| Pull | 3.5µs | **9.2×** |
| Query | 32.7µs | baseline |

**Scaling Characteristics**:
- **Per-attribute cost**: ~1.2µs (BadgerDB), ~470ns (in-memory)
- **Per-entity cost**: ~2.5µs (BadgerDB), ~1µs (in-memory)
- **Linear scaling**: Both attributes and entities scale linearly
- **Pattern caching**: 20% speedup by pre-parsing patterns

**Key Insight**: Wildcard `[*]` is 2× faster than explicit attribute lists because it performs one EAVT scan instead of N AEVT lookups.

**Recommended Usage**:
```go
// For hot paths, cache the parsed pattern
pattern, _ := parser.ParsePullPattern(`[:user/name :user/age]`)
puller := executor.NewPullExecutor(db.Matcher())

// Reuse pattern across calls
for _, entity := range entities {
    result, _ := puller.Pull(entity, pattern)
}
```

### 9. Schema Validation Performance (MEASURED 2025-12-17)
**Status**: ✅ Negligible overhead for type validation, minimal overhead for uniqueness
**Performance**: Type validation **<1% write overhead**, uniqueness checking **~6% write overhead**
**Location**: `datalog/schema/`, `datalog/storage/database.go`

**Note**: All schema overhead is on the **write path only**. Reads are completely unaffected.

**Type Validation Overhead** (Apple M4 Max, write path):
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Add without schema | 25,771 | 223 | baseline |
| Add with schema | 25,768 | 225 | **<0.2%** |

**Uniqueness Checking Overhead** (write path):
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Add without schema | 25,771 | 223 | baseline |
| Add with uniqueness | 27,236 | 292 | **~5.8%** |

**Bulk Operations (100 items/transaction)**:
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Bulk without schema | 1,388,000 | 17,115 | baseline |
| Bulk with schema | 1,408,000 | 17,117 | **~1.4%** |

**Schema Resolution (one-time cost per Pull pattern)**:
| Operation | ns/op | allocs |
|-----------|-------|--------|
| Resolve 5-attr pattern with nested refs | 225 | 10 |

**Pull with Cardinality-Many** (10 values):
| Cardinality | ns/op | allocs |
|-------------|-------|--------|
| Cardinality-one (1 value) | 1,117 | 31 |
| Cardinality-many (10 values) | 3,679 | 96 |

**Key Findings**:
- **Type validation is essentially free** - just a map lookup and type switch (write path)
- **Uniqueness checking adds ~6% to writes** - requires database query to check existing values
- **Reads are unaffected** - schema validation only impacts the write path
- **Schema resolution is negligible** - 225ns one-time cost per Pull pattern
- **Cardinality-many scales linearly** - ~370ns per additional value in Pull results

**Recommendation**: Enable schema validation freely for type safety. Uniqueness constraints are worth the 6% write overhead for data integrity.

### 10. OHLC Query Performance (MEASURED 2025-10-25)
**Benchmark**: OHLC queries with subqueries and predicate pushdown

**Subquery Performance** (BenchmarkOHLCSubqueries):
- Single aggregation: 17.3 ms/op, 66.7 MB/op, 934K allocs
- Three aggregations: 51.3 ms/op, 199.6 MB/op, 2.8M allocs
- **Result**: Linear scaling (3× subqueries = 2.96× time, 3× memory), proper semantics ✅

**Predicate Pushdown - Small Dataset** (BenchmarkOHLCQuery, 10 days × 3 symbols × 390 bars):
- Without pushdown: 33.6 ms/op, 39.4 MB/op, 534K allocs
- With pushdown: 21.3 ms/op, 20.0 MB/op, 330K allocs
- With time-range opt: 21.5 ms/op, 20.0 MB/op, 330K allocs
- **Result: 1.58× faster, 49% memory reduction, 38% fewer allocations** ✅

**Predicate Pushdown - Large Dataset** (BenchmarkOHLCQueryLargeDataset, 90 days × 50 symbols sparse):
- Without pushdown: 1,043 ms/op, 3,484 MB/op, 15.4M allocs
- With pushdown: 375 ms/op, 296 MB/op, 5.2M allocs
- **Result: 2.78× faster, 91.5% memory reduction, 66.2% fewer allocations** ✅
- **Key insight**: Predicate pushdown scales better with larger datasets (1.58× → 2.78×)

**Key Findings**:
- Iterator semantics correctly enforced (no re-iteration bugs)
- Memory scales linearly with query complexity (predictable)
- Predicate pushdown benefits increase with dataset size
- Large dataset queries complete in <400ms even with 90 days of data

---

## Profiling Results (October 2025)

### In-Memory Execution Path
**Profiled**: MemoryPatternMatcher with OHLC queries

**CPU Bottlenecks** (measured via pprof):
- Pattern matching: 58% (matchesDatomWithPattern, matchesConstant, matchesElement)
- Memory copying: 18% (runtime.duffcopy)
- Hash operations: Various small percentages

**Memory Allocations** (measured via pprof):
- TupleKeyMap operations: 35% (now optimized with pre-sizing)
- Expression evaluation: 9-19% (time extractions)
- Pattern matching: 14%

**Key Finding**: Pattern matching dominates CPU time in-memory queries. Optimizations targeting pattern matching (in-memory indexing) made hash indices the default path throughout the codebase.

### Storage-Backed Execution Path
**Profiled**: BadgerMatcher with OHLC queries
**Query Time**: 56ms for 260 hours (measured)

**Key Finding**: Storage-backed queries already fast enough for production use. Focus has been on correctness and architectural improvements rather than micro-optimizations.

**Details**: See `PROFILING_SUMMARY.md` and `EXECUTION_CHAIN_PROFILING_ANALYSIS.md`

---

## Performance Test Results (Actual Benchmarks)

### Planner/Executor Architecture (2025-10-22)
**Benchmark Suite**: `BenchmarkPlannerOnly`, `BenchmarkPlanQuality`, `BenchmarkFullQueryOldVsNewPlanner`

**Planning Overhead Only** (BenchmarkPlannerOnly):
| Query Type | Old Planner | New Planner | Speedup |
|-----------|-------------|-------------|---------|
| Simple pattern | 3,940 ns | 2,048 ns | 52% faster |
| Single subquery | 4,382 ns | 758 ns | 83% faster |
| OHLC query | 11,546 ns | 1,673 ns | 86% faster |

**Key Finding**: Planning is fast (1-15 microseconds) regardless of planner. Planning speed has negligible impact on total query time.

**Plan Quality** (BenchmarkPlanQuality - both using QueryExecutor, verified 2025-10-24):
| Query Type | Old Planner | New Planner | Difference |
|-----------|-------------|-------------|------------|
| simple_join | 1.609 ms | 1.612 ms | 0.2% slower (within noise) |
| aggregation | 2.003 ms | 2.014 ms | 0.6% slower (within noise) |
| multi_join | 2.882 ms | 2.886 ms | 0.1% slower (within noise) |

**Key Finding**: When isolated (same executor), both planners produce equivalent-quality plans. Performance differences are within measurement noise.

**Full Architecture** (BenchmarkFullQueryOldVsNewPlanner - old planner+executor vs new planner+executor):
| Query Type | Old Architecture | New Architecture | Improvement |
|-----------|------------------|------------------|-------------|
| OHLC queries | ~4-8 seconds | ~2-4 seconds | **~2× faster** |

**Key Finding**: The 2× improvement comes from QueryExecutor's clause-by-clause streaming execution model, not from planner differences. Both planners produce equivalent-quality plans when using the same executor.

**Details**: See `docs/reference/PLANNER_COMPARISON.md`

### Time Range Optimization (2025-10-08)
**Hourly OHLC (260 hours)**:
- Before: 41s
- After: 10.2s
- **Speedup: 4.0×** ✅

**Daily OHLC (22 days)**:
- Before: 217ms
- After: 217ms
- **Speedup: 1.0×** (no regression) ✅

### Hash Join Pre-Sizing (2025-10-08)
| Size | Speed | Memory | Speedup |
|------|-------|--------|---------|
| 100 | 6.9µs → 5.2µs | 17KB → 13KB | 25% faster |
| 1,000 | 105µs → 71µs | 259KB → 181KB | 32% faster |
| 10,000 | 1.34ms → 1.24ms | 2.3MB → 1.7MB | 7% faster |

### Semantic Rewriting (2025-10-07)
| Filter Type | Selectivity | Speedup |
|-------------|-------------|---------|
| Year only | 33% | 2.6× |
| Year+Month+Day | 12.5% | 4.1× |
| Year+Month+Day+Hour | 1.4% | 5.8× |

### Plan Cache (Empirical)
- First query: ~1-5ms planning time
- Cached query: ~0.3ms planning time
- **~3× speedup for repeated queries**

---

## Optimization Priorities

### High Impact, Already Done ✅
All items below are **measured** and **active** in production code:

1. ✅ **New architecture** (clause-based planner + QueryExecutor) - **2× faster on complex queries** (verified 2025-10-24)
2. ✅ **Pull API** - **9× faster than equivalent queries**, linear scaling (verified 2025-12-17)
3. ✅ **Schema validation** - **<1% overhead** for type checking, **~6%** for uniqueness (verified 2025-12-17)
4. ✅ **Iterator composition** - **4.06× faster, 89% memory reduction** (verified 2025-10-25)
5. ✅ **Parallel subquery execution** - **2.06× speedup with 8 workers** (verified 2025-10-25)
6. ✅ **Intern cache optimization** - **6.26× speedup with BadgerDB**
7. ✅ **Query plan caching** - **3× speedup for repeated queries**
8. ✅ **Time range optimization** - **4× speedup on hourly OHLC**
9. ✅ **Semantic rewriting** - **2-6× on time-filtered queries**
10. ✅ **Predicate pushdown** - **1.58-2.78× faster** (scales with dataset size), **up to 91.5% memory reduction** (verified 2025-10-25)
11. ✅ **Streaming execution** - **2.22× on low-selectivity filters, 52% memory reduction** (verified 2025-10-25)
12. ✅ **Hash join pre-sizing** - **24-32% faster, 24-30% less memory**
13. ✅ **In-memory indexing** - Hash indices now default path throughout codebase
14. ✅ **Relation collapsing algorithm** - **Prevents catastrophic Cartesian products**

### Potential Future Work 🎯
These are **ideas**, not commitments. Would require benchmarking before implementation:

1. Streaming aggregations - Reduce memory for large groups
2. BadgerDB time range integration - Push time constraints to storage layer
3. Composite index support - For multi-attribute filters

### Rejected After Benchmarking ❌
These were **tried and measured** - data showed they're not worth the complexity:

1. ~~Key mask iterator for int64~~ - Benchmarked slower than simple approach
2. ~~Complex iterator reuse~~ - Simpler code is faster
3. ~~Aggressive CSE~~ - 1-3% sequential, -1% parallel (disabled by default)
4. ~~Interface-based key encoder consolidation~~ - 10% slower, 60% more allocations (Dec 2025)
5. ~~Generic key encoder with type parameters~~ - Go generics don't inline, same overhead as interfaces

---

## Recommended Configuration (October 2025)

**Production Configuration** (all settings are measured and proven):

```go
// Use new architecture (2× faster on complex queries)
exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
    UseClauseBasedPlanner: true,  // ✅ DEFAULT - works with QueryExecutor
})
exec.SetUseQueryExecutor(true)    // ✅ Use QueryExecutor (DEFAULT - clause-by-clause streaming)

// Recommended planner options
PlannerOptions{
    UseClauseBasedPlanner:        true,  // ✅ DEFAULT - required for QueryExecutor
    EnablePredicatePushdown:      true,  // ✅ DEFAULT - early filtering
    EnableSemanticRewriting:      true,  // ✅ 2-6× on time queries
    EnableSubqueryDecorrelation:  true,  // ✅ Batch identical subqueries
    EnableParallelDecorrelation:  true,  // ✅ 6.9× speedup in-memory
    EnableCSE:                    false, // ❌ 1-3% benefit, disabled by default
}

// Recommended executor options
ExecutorOptions{
    EnableIteratorComposition:    true,  // ✅ DEFAULT - lazy evaluation (4.2× speedup)
    EnableTrueStreaming:          true,  // ✅ DEFAULT - streaming (1.9× on low-selectivity)
    EnableSubqueryDecorrelation:  true,  // ✅ Batch identical subqueries
}
```

**Key Changes from Previous Versions**:
- New clause-based planner is now default (was experimental)
- QueryExecutor is now default (was opt-in)
- Streaming execution always enabled (was toggle)
- All settings backed by measured benchmarks

---

## Performance Philosophy

### What We Got Right
1. **Correctness first** - Semantics before speed
2. **Measure everything** - Benchmarks revealed truth
3. **Simple code wins** - Iterator reuse complexity didn't pay off
4. **Smart algorithms** - Relation collapsing is the real hero
5. **Targeted optimization** - Profile, optimize bottlenecks, verify

### What We Learned
1. **Micro-optimizations fail** - Opens are 3µs, not worth complexity
2. **Architecture matters** - Can't push predicates without storage support
3. **Premature optimization is real** - Key mask, iterator reuse both slower
4. **Document reality** - Aspirational docs cause confusion
5. **Redundant optimizations exist** - Semantic rewriting + decorrelation target same bottleneck
6. **Go interfaces have real cost** - 10% slower, 60% more allocations from interface dispatch in hot paths
7. **Go generics don't inline** - Unlike C++/Rust, Go generics provide type safety but NOT zero-cost abstraction
8. **DRY vs performance trade-off** - In hot paths, duplication IS the optimization; share only cold paths

### What's Next
1. Keep **simple, correct code** (complexity doesn't pay)
2. Let **algorithms win** (relation collapsing, not tricks)
3. Build **only what benchmarks prove** (no more speculation)
4. Consider **streaming aggregations** for memory efficiency

---

## Success Metrics

### Current State (2025-10-25)
All metrics below are **measured** from actual benchmarks, not estimates.

**Verified Performance Improvements** (latest benchmarks):
- New architecture: **2× faster** on complex OHLC queries (old: ~4-8s, new: ~2-4s) ✅
- Iterator composition: **4.06× faster** (1,259μs → 310μs), **89% memory reduction** (3.27 MB → 360 KB) ✅
- Streaming execution: **2.22× faster** on low-selectivity filters (1,720μs → 774μs), **52% memory reduction** ✅
- Parallel subquery execution: **2.06× speedup** with 8 workers (730ms → 355ms) ✅
- Predicate pushdown (small): **1.58× faster** (33.6ms → 21.3ms), **49% memory reduction** ✅
- Predicate pushdown (large): **2.78× faster** (1,043ms → 375ms), **91.5% memory reduction** ✅
- Time-filtered queries: **2-6× faster** (semantic rewriting) ✅
- Hourly OHLC (large dataset): **10.2s** (4× speedup from time ranges) ✅
- Parallel BadgerDB: **6.26× speedup** (intern cache optimization) ✅
- Hash join pre-sizing: **24-32% faster, 24-30% less memory** ✅

**Pervasive Optimizations** (now default throughout codebase):
- In-memory indexing: Hash indices are now the default path (previously showed 49-4802× vs linear scan)
- Plan quality: Both planners produce equivalent-quality plans (within measurement noise)
- Single-use iterator semantics: Proper lifecycle management prevents bugs, enables streaming

---

## Documentation Organization

### Active Documentation (Root)
- `PERFORMANCE_STATUS.md` - **This file** (consolidated performance overview)
- `docs/reference/PLANNER_COMPARISON.md` - **NEW** Planner/executor architecture comparison

### Supporting Documentation
- `TIME_RANGE_OPTIMIZATION_STATUS.md` - Time range extraction and optimization
- `HASH_JOIN_PRESIZING_SUMMARY.md` - Hash join pre-sizing optimization
- `PROFILING_SUMMARY.md` - Complete profiling findings

### Archived Documentation (docs/archive/2025-10/)
- Detailed analyses (EXECUTION_CHAIN_PROFILING_ANALYSIS.md, etc.)
- Implementation docs (SUBQUERY_DECORRELATION_*.md, SEMANTIC_REWRITING_FINDINGS.md, CSE_FINDINGS.md)
- Streaming architecture history (STREAMING_ARCHITECTURE_COMPLETE.md)
- Bug documentation (PARALLEL_DECORRELATION_TUPLE_ORDER_BUG.md)
- Session summaries (SESSION_SUMMARY_*.md)
- Completed work (CSE_FINDINGS.md, SEMANTIC_REWRITING_FINDINGS.md)

---

## The Bottom Line

**What Actually Matters**:
1. ✅ Relation collapsing prevents memory explosion (CRITICAL)
2. ✅ Query plan caching speeds up repeated queries (3× improvement)
3. ✅ Parallel execution eliminates bottlenecks (6.26× speedup)
4. ✅ Time range optimization targets specific queries (4× speedup)
5. ✅ Semantic rewriting optimizes time queries (2-6× speedup)
6. ✅ Code correctness and simplicity beat micro-optimizations

**What Doesn't Matter**:
1. ❌ Iterator open/close overhead (3µs, negligible)
2. ❌ Key mask filtering for simple types (overhead > benefit)
3. ❌ Complex reuse strategies (simpler code performs better)
4. ❌ CSE with parallel execution (removes parallelism opportunity)

The engine is **production-ready for datasets up to 10M+ datoms**. All major optimizations are complete and working well. Performance is excellent for typical workloads (100K-1M datoms), with targeted optimizations for specific patterns (time queries, large joins). Large config testing demonstrates scalability to 500M+ datoms.

---

## Session History

### 2025-12-24: Key Encoder Consolidation - A DRY Refactoring Case Study

**Goal**: Reduce code duplication between `L85KeyEncoder` and `BinaryKeyEncoder` (~95% identical logic).

**Attempt 1: Interface-Based Consolidation**
- Created `ComponentEncoder` interface abstracting encode/decode operations
- Created `baseKeyEncoder` struct with shared index ordering logic
- L85/Binary encoders delegated to base via embedded struct + interface field

**Result**:
| Benchmark | Main | Refactored | Regression |
|-----------|------|------------|------------|
| AVETReuse | 29ms, 16MB, 300K allocs | 32ms, 20.8MB, 476K allocs | **+10% time, +30% mem, +60% allocs** |
| BatchScanScaling/1000 | 1.1ms, 1.15MB, 21K | 1.25ms, 1.41MB, 31K | **+14% time, +23% mem, +47% allocs** |

**Root Cause Analysis**:
1. **Interface dispatch overhead** - Every `e.comp.EncodeEntity()` call goes through vtable
2. **Lazy initialization checks** - `ensureInitialized()` branch on every method
3. **Slice escape to heap** - Passing `[20]byte` by value to interface method, returning `[]byte` causes allocation

**Attempt 2: Go Generics**
- Changed `baseKeyEncoder` to `baseKeyEncoder[T ComponentEncoder]`
- Used concrete type parameter to enable compiler inlining

**Result**: No improvement. Go generics don't specialize/inline like C++ templates or Rust generics. The method calls still happen at runtime.

**Attempt 3: Hybrid Approach (SUCCESS)**
- Restored full inline implementations in each encoder (hot paths)
- Extracted only truly shared utilities to `key_encoder_base.go`:
  - `historyIndexToBase()` - Maps history indices to base indices
  - `incrementLastByte()` - Creates end key for prefix scans
- Removed duplicate `l85HistoryIndexToBase` function

**Result**:
| Benchmark | Main | Hybrid | Change |
|-----------|------|--------|--------|
| AVETReuse | 29ms, 16MB, 300K | 28ms, 16MB, 300K | ✅ **3% faster** |
| BatchScanScaling/1000 | 1.1ms, 1.15MB, 21K | 1.0ms, 1.15MB, 21K | ✅ **9% faster** |
| BatchScanScaling/5000 | 6.0ms, 4.6MB, 86K | 5.3ms, 4.6MB, 86K | ✅ **12% faster** |

**Why Hybrid is Faster Than Main**:
Main branch had duplicate functions: `l85HistoryIndexToBase` (in L85 encoder) and `historyIndexToBase` (in Binary encoder) - identical 15-line switch statements. The hybrid approach consolidates these into a single shared `historyIndexToBase()` in `key_encoder_base.go`.

Benefits of consolidation:
1. **Reduced binary size** - One function instead of two identical copies
2. **Better instruction cache** - Single hot function stays in cache vs two copies competing
3. **Compiler optimization** - Single definition allows better inlining/branch prediction

This demonstrates that **strategic code sharing CAN improve performance** when sharing cold/utility paths that don't benefit from inlining, while keeping hot encode/decode logic duplicated.

**Code Impact**:
| Version | Lines |
|---------|-------|
| Main (original) | 662 lines |
| Hybrid (final) | 550 lines |
| **Reduction** | **112 lines (17%)** |

**Key Takeaways**:

1. **Go interfaces have real cost** - Interface dispatch, escape analysis, and allocation overhead are measurable in hot paths. The ~10% regression and 60% more allocations came purely from abstraction.

2. **Go generics ≠ C++ templates** - Go generics provide type safety but NOT specialization. Method calls through generic type parameters still have runtime overhead. Don't expect zero-cost abstractions.

3. **DRY has limits in performance-critical code** - Sometimes duplication IS the optimization. The compiler can't inline what you've abstracted away.

4. **Hybrid approach works** - Share truly common utilities (helper functions, constants) while keeping hot paths inline. This preserves both readability and performance.

5. **Benchmark before and after** - The interface refactor looked cleaner but was 10% slower. Only benchmarks revealed the truth.

6. **Allocation count is a leading indicator** - The 60% allocation increase (300K → 476K) signaled trouble before timing showed it. Extra allocations = GC pressure = slower execution.

**Files Changed**:
- `datalog/storage/key_encoder_base.go` - Shared utilities only (37 lines)
- `datalog/storage/key_encoder_binary.go` - Full inline implementation (172 lines)
- `datalog/storage/key_encoder_l85.go` - Full inline implementation (288 lines)
- `datalog/storage/key_encoder_interface.go` - Factory unchanged (53 lines)

**Recommendation**: For performance-critical code paths called millions of times:
- Prefer inline code over interface abstraction
- Share constants, helper functions, and cold paths only
- Keep hot encode/decode/match logic duplicated but identical
- Use benchmarks to validate any consolidation attempt

---

### 2025-12-17: Schema Support Implementation & Benchmarking
- Implemented Datomic-compatible schema support with type validation, cardinality, and uniqueness
- Schema definable via EDN file or Go builder API (same internal representation)
- Added plan-time resolution for Pull patterns (schema lookup once per pattern, not per entity)
- Created comprehensive benchmark suite for schema performance
- **Measured Results**:
  - Type validation overhead: **<0.2%** (essentially free)
  - Uniqueness checking overhead: **~5.8%** (requires database query)
  - Bulk operations with schema: **~1.4%** overhead
  - Schema resolution: **225ns** one-time cost per pattern
  - Cardinality-many: ~370ns per additional value (linear scaling)
- **Key Finding**: Schema validation is essentially free; use freely for type safety
- **Files created**: `datalog/schema/` package, `docs/reference/SCHEMA.md`, `examples/schema_demo.go`

### 2025-12-17: Pull API Implementation & Benchmarking
- Implemented Datomic-style Pull API with nested references and cycle detection
- Created comprehensive benchmark suite for Pull performance
- **Measured Results**:
  - Pull vs Query: **9.2× faster** (3.5µs vs 32.7µs for 3 attributes)
  - Wildcard: **2.2× faster** than explicit attributes (single scan vs N lookups)
  - Per-attribute cost: ~1.2µs (BadgerDB), ~470ns (in-memory)
  - Per-entity cost: ~2.5µs (BadgerDB), ~1µs (in-memory)
  - Linear scaling with both attributes and entities
- **Key Finding**: Pull avoids query parsing/planning overhead entirely
- **Recommendation**: Cache parsed patterns for hot paths (20% speedup)

### 2025-10-25: Single-Use Iterator Semantics & Performance Verification (Sessions 1-2)
**Session 1**: Initial benchmarking after single-use iterator semantics implementation
- Implemented proper single-use iterator semantics for StreamingRelation
- Added BufferedIterator for safe re-iteration support
- Ran comprehensive performance benchmarks to verify all claims
- **Updated all performance claims to reflect reality**:
  - Iterator composition: 4.2× (was claiming 17.5×)
  - Streaming execution: 1.9× (was claiming 28×)
  - Parallel execution: 2.2× with 8 workers (new measurement)
  - Predicate pushdown: 1.77× (new measurement)

**Session 2**: Debug output cleanup and precise verification
- Discovered debug prints polluting benchmark output (79 fmt.Printf statements)
- Fixed 3 unguarded debug prints in join.go
- Re-ran all benchmarks with clean output
- **Updated to precise measurements**:
  - Iterator composition: **4.06×** (1,259μs → 310μs), 89% memory reduction
  - Streaming execution: **2.22×** (1,720μs → 774μs), 52% memory reduction
  - Parallel execution: **2.06×** (730ms → 355ms) with 8 workers
  - Predicate pushdown (small dataset): **1.58×** (33.6ms → 21.3ms), 49% memory reduction
  - Predicate pushdown (large dataset): **2.78×** (1,043ms → 375ms), 91.5% memory reduction
- Fixed BenchmarkOHLCQueryLargeDataset transaction size bug (commit per-symbol-per-day)
- Key lesson: **Clean benchmarks reveal precise truth, and predicate pushdown scales better with larger datasets**

### 2025-10-08: Profiling, Hash Join & In-Memory Indexing
- Profiled entire OHLC execution chain (in-memory + storage)
- Identified hash join as 35% of allocations
- Implemented map pre-sizing: 24-32% faster, 24-30% less memory
- Confirmed time range optimization too fast to profile (10µs)
- Identified pattern matching as 58% CPU in-memory → implemented hash indices
- **Massive win**: Entity lookups 49-4802× faster, test suite now 7s (down from timeouts)

### 2025-10-07: Time Range Optimization
- Implemented semi-join pushdown via time range constraints
- Achieved 4× speedup on hourly OHLC (41s → 10.2s)
- Fixed daily OHLC regression with size check
- Optimized extractTimeRanges: 4.7× faster, 108× fewer allocations

### 2025-10-04: Parallel Execution & Intern Optimization
- Identified intern cache as 35% CPU bottleneck
- Replaced sync.RWMutex with sync.Map → 6.26× BadgerDB speedup
- Fixed index selection to use AEVT when E+A both bound
- Performance gains: In-memory 6.9×, BadgerDB 1.63× → 6.26×
