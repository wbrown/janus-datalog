# Archived Optimization Documentation

This directory contains historical optimization attempts, profiling results, and implementation plans that have been completed or superseded. Documents are preserved for historical reference and to document lessons learned.

## What's Here

### Completed Optimizations (October 2025)

**Comprehensive Summary**:
- **OCTOBER_2025_OPTIMIZATION_SUMMARY.md** - Complete overview of October work
  - 6.26× parallel speedup, 61% memory reduction, 49-4802× indexing speedup
  - Consolidates: profiling results, hash join pre-sizing, parallel execution, memory optimizations

**Individual Optimizations**:
- **PARALLEL_SUBQUERY_COMPLETE.md** - Worker pool implementation (5.2× speedup)
- **BADGERDB_OPTIMIZATION_COMPLETE.md** - Tuple builder caching (15% memory reduction)
- **IN_MEMORY_INDEX_*.md** - In-memory indexing (49-4802× speedup)
- **INTERN_OPTIMIZATION_PLAN.md** - Lock-free intern cache (6.26× BadgerDB speedup)
- **STREAMING_AGGREGATION_IMPLEMENTATION.md** - Streaming aggregation
- **TIME_RANGE_OPTIMIZATION_STATUS.md** - Time range optimization (4× speedup)

### Profiling and Analysis
- **PROFILING_*.md** - Various profiling sessions and results
- **PREBUILT_DATABASE_PROFILING.md** - Performance testing with prebuilt databases
- **BENCHMARK_MIGRATION_GUIDE.md** - Benchmark migration documentation

### Failed/Abandoned Experiments (August 2025)
- **ITERATOR_REUSE_ANALYSIS.md** - Analysis showing simpler iteration performs better than complex reuse
- **ITERATOR_REUSE_BUG.md** - Debugging notes from reuse attempts
- **KEY_MASK_ITERATOR_PLAN.md** - Plan for byte-level key filtering optimization
- **CONSTRAINT_OPTIMIZATION_RESULTS.md** - Fast-path optimization for constraint evaluation
- **PERFORMANCE_IMPROVEMENTS_AUGUST_2025.md** - Summary of August optimization sprint

### Other Documentation
- **TEST_DATABASE_INTEGRATION.md** - Test database setup documentation
- **DATABASE_SIZES.md** - Database size analysis
- **DOCUMENTATION_CONSOLIDATION_PLAN.md** - Documentation cleanup plan

## Key Lessons Learned

1. **Measure, Don't Assume**: Multiple "obvious" optimizations proved slower in benchmarks
2. **Architecture Matters**: Can't push predicates to storage without index-level support
3. **Simple Code Wins**: Complex iterator reuse had worse performance than straightforward approach
4. **Micro-optimizations Fail**: Saving 3µs per operation doesn't justify code complexity

## Current Status

See `/PERFORMANCE_STATUS.md` for the consolidated view of what's actually active and working.

**What's Active**:
- ✅ Query plan caching (proven 3x speedup)
- ✅ Batch scanning with threshold (code clarity, modest performance)
- ✅ Relation collapsing algorithm (critical for memory management)

**What's Not Used**:
- ❌ Key mask iterator (benchmarked slower)
- ❌ Complex iterator reuse (simpler is faster)
- ❌ Aggressive predicate pushdown (architectural limits)

## Why Archive These?

These documents represent good engineering practice:
- Documenting what was tried
- Showing benchmark evidence
- Explaining why approaches didn't work
- Preventing future attempts at the same optimizations

The lessons here are valuable even though the optimizations weren't successful.
