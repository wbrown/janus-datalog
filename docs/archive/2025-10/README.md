# October 2025 Optimization Sprint

Documentation from the October 2025 intensive optimization work.

## Summary

This sprint achieved major performance improvements through:
- **6.26× speedup** via parallel execution and lock-free intern caches
- **4× speedup** on time-range queries via extraction optimization
- **49-4802× speedup** on entity lookups via in-memory indexing
- **24-32% memory reduction** via hash join pre-sizing

## Key Achievements

### 1. Parallel Execution (6.26× speedup)
- **PARALLEL_EXECUTION_STATUS.md** - Implementation status
- **PARALLEL_DECORRELATION_TUPLE_ORDER_BUG.md** - Tuple ordering bug fix
- **SUBQUERY_DECORRELATION_IMPLEMENTATION.md** - Complete decorrelation implementation

### 2. Query Optimization (2-6× on time queries)
- **QUERY_OPTIMIZATION_FINDINGS.md** - Consolidated CSE and semantic rewriting analysis
  - Common Subexpression Elimination (CSE): 1-3% benefit sequential, -1% parallel
  - Semantic Rewriting (time ranges): 2-6× speedup on time queries

### 3. OHLC Query Analysis and Optimization (4× speedup)
- **OHLC_QUERY_ANALYSIS.md** - Comprehensive analysis of hourly OHLC aggregation
  - Query structure and complexity analysis
  - Redundancies identified (1,040 subquery executions)
  - Optimizations: decorrelation, parallel execution, time range pushdown
  - Combined results: 41s → 10.2s (4× speedup)
- **TIME_RANGE_EXTRACTION_OPTIMIZATION.md** - Time range extraction details

### 4. Memory Optimizations
- **HASH_JOIN_PRESIZING_ANALYSIS.md** - Hash join pre-sizing (24-30% reduction)
- **LARGE_BINDING_SET_DIAGNOSIS.md** - Large binding set issue resolution

### 5. Profiling and Analysis
- **EXECUTION_CHAIN_PROFILING_ANALYSIS.md** - Complete execution chain profiling

### 6. Failed/Rejected Optimizations
- **SEMI_JOIN_PUSHDOWN_FAILED_ATTEMPT.md** - Semi-join pushdown (didn't improve performance)

## Bug Investigations

### Decorrelation Bug (October 9-10, 2025)
Critical bug where pure aggregation subqueries returned nil values. Investigation documents:
- **AGGREGATE_SUBQUERY_BUG_ANALYSIS.md** - Initial root cause analysis (Oct 9)
- **PURE_AGGREGATION_NIL_BUG.md** - Pure aggregation investigation (Oct 10)
- **SUBQUERY_NIL_AGGREGATION_BUG.md** - Subquery angle investigation (Oct 10)
- **STREAMING_AGGREGATION_NIL_BUG.md** - Streaming aggregation investigation (Oct 10)
- **Final resolution**: See root `/DECORRELATION_BUG_FIX.md` for authoritative summary

Root cause: Decorrelation optimization incorrectly added input parameters as FindVariable elements to pure aggregations, changing them from single aggregations to grouped aggregations.

### Other Bugs Fixed
- **CONCURRENT_MAP_BUG_REPORT.md** - Tuple builder cache race condition (Oct 10)
  - Fatal: `concurrent map read and map write`
  - Fixed with `sync.Map` for thread-safe cache
- **PARALLEL_DECORRELATION_TUPLE_ORDER_BUG.md** - Tuple ordering bug in parallel execution

### Session Summaries
- **FIXES_2025_10_10.md** - Complete summary of concurrent map + nil aggregation fixes
- **SESSION_SUMMARY_2025_10_04.md** - October 4th optimization session
- **SESSION_SUMMARY_2025_10_05.md** - October 5th hash join implementation

## Streaming Architecture Refactoring (October 14-17, 2025)

Major architectural refactoring to achieve true streaming execution:

### Branch: fix-buffered-iterator-architecture
- **BRANCH_REPORT_FIX_BUFFERED_ITERATOR_ARCHITECTURE.md** - Complete 29-commit journey from WIP to production
  - What started as "remove BufferedIterator" became comprehensive architectural refactoring
  - Fixed 10 critical bugs: 3 data correctness, 2 concurrency, 5 architectural issues
  - All 25 validation tests passing - both legacy and new executors produce identical results

- **BUFFERED_ITERATOR_ARCHITECTURAL_PROBLEM.md** - Root cause analysis
  - BufferedIterator was band-aid hiding fundamental architectural problem
  - Codebase assumed iterators could be consumed multiple times, defeating streaming
  - Stage B (QueryExecutor) required true streaming semantics

- **PHASE_REORDERING_RESULTS.md** - Phase reordering implementation complete
  - Greedy information flow optimization based on Clojure's `reorder-plan-by-relations`
  - Maximizes symbol connectivity between consecutive phases
  - Enabled by default with `EnableDynamicReordering: true`

- **AGGREGATION_BEHAVIOR_CHANGES.md** - Decorrelation bug fix impact
  - Pure aggregation subqueries now return correct values (was returning nil)
  - Planner correctly distinguishes pure vs grouped aggregations

- **FAILING_TESTS.md** - Test status snapshot at branch completion
  - All executor bugs resolved (was tracking 6 failing tests)
  - Both legacy and new executors now handle all test queries correctly

### Key Achievements
- **True streaming architecture**: 1.5-2.5× faster, 50-99% memory reduction
- **Bug discoveries**: Architecture revealed 10 hidden bugs through validation testing
- **Differential testing**: Both executors validated against each other, found bugs in legacy executor
- **Zero regressions**: All features working, no performance degradation

## Current Status

All major optimizations from this sprint are **active and enabled** in production. See root-level `PERFORMANCE_STATUS.md` for current performance profile.

**Benchmarked results**:
- Simple queries: 10-50ms (100K-1M datoms)
- Complex queries: 50-200ms
- Test suite: 7 seconds (was timing out before optimizations)
- Memory: <100MB for typical queries
