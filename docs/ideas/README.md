# Future Ideas and Proposals

Potential optimizations and features under consideration.

## Status Key
- ✅ **Implemented** - Moved to `docs/archive/completed/`
- 🎯 **Active consideration** - Under evaluation or planning
- ⏸️ **Deferred** - Good idea but not priority
- ❌ **Rejected** - Evaluated and decided against

## Current Ideas

### Performance Optimizations

- **HASH_JOIN_HOT_PATH_OPTIMIZATIONS.md** - 🎯 Five inner-loop inefficiencies in `combineTuples`, `ValuesEqual`, identity hashing, defensive copies, and dedup lookups
- **REMOVE_HASHJOIN_MATERIALIZATION.md** - 🎯 Remove forced materialization in HashJoin streaming path
- **iterator-seek-optimization-plan.md** - ✅ **MOVED TO ARCHIVE** - SimpleBatchScanner implemented (Phase 1 complete)
- **LAZY_MATERIALIZATION_IDEAS.md** - ✅ **MOVED TO ARCHIVE** - Realized as streaming architecture (October 2025)
- **STORAGE_OPTIMIZATION_PLAN.md** - ✅ **MOVED TO ARCHIVE** - Phase 1 implemented, performance goals achieved

### Query Optimization

- **SMART_PREDICATE_PUSHDOWN_PLAN.md** - 🎯 Advanced predicate pushdown (partially implemented, more possible)
- **planner-improvements.md** - 🎯 Query planner enhancements from Clojure's information flow approach

### Infrastructure

- **TEST_COVERAGE_PLAN.md** - 🎯 Test coverage improvements (always relevant)

## Guidelines

When evaluating ideas:
1. **Benchmark first** - Profile to confirm bottleneck exists
2. **Measure impact** - Compare before/after performance
3. **Consider complexity** - Simple code that's "fast enough" beats complex code that's faster
4. **Document outcome** - Move to appropriate archive location

See `PERFORMANCE_STATUS.md` for lessons learned about optimization priorities.
