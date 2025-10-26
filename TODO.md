# TODO - Realistic Roadmap

## Current System Status

### What's Working Well ✅
- Complete Datalog query engine with EAVT storage
- BadgerDB persistent storage with 5 indices
- Pattern matching, joins, and predicates
- Expression clauses with arithmetic and string operations
- Aggregations (sum, count, avg, min, max)
- Subqueries with proper scoping (semantically correct)
- Order-by clause with multi-column sorting
- Time extraction functions
- **True streaming execution (2.22× speedup, 52% memory reduction, up to 91.5% on large datasets)**
- **Iterator composition with lazy evaluation (4.06× speedup)**
- **Parallel subquery execution (2.06× speedup with 8 workers)**
- **Predicate pushdown (1.58-2.78× speedup, scales with dataset size)**
- **Lock-free intern caches (6.26× BadgerDB speedup)**
- Query plan caching (3× speedup, active)
- Relation collapsing algorithm (prevents memory explosion)

### Production Readiness: ~85%
The engine is **functionally complete, semantically correct, and memory-efficient**. Streaming execution is now enabled by default, providing significant performance and memory improvements (2.22× faster, 52-91.5% memory reduction). See `PERFORMANCE_STATUS.md` for detailed analysis.

## Immediate Priorities (Critical)

### 1. ✅ Fix AEVT Index Performance Bug - COMPLETED
**Problem**: AEVT scans 12.8M datoms instead of 65 for E+A bound patterns
**Root Cause**: Lack of visibility into iterator reuse scan counts
**Fix**: Added datom tracking to reusingIterator; verified Seek() works correctly
**Status**: ✅ RESOLVED (commit 0565b85)
**Result**: 5 scans for 3 entities (3 matches + 2 "moved past"), no database-wide scans

### 2. ✅ True Streaming Execution - COMPLETED
**Problem**: StreamingRelation forced materialization at every operation
**Solution**: Iterator composition with BufferedIterator for re-iteration
**Status**: ✅ IMPLEMENTED (October 13, 2025)
**Result**: 1.5-2.5× speedup, 50-99% memory reduction
**Docs**: STREAMING_PERFORMANCE_REPORT.md, STREAMING_FINAL_SUMMARY.md

### 3. ✅ Fix Conditional Aggregate Rewriting - PLANNING COMPLETE
**Problem**: Cross-phase expression dependencies cause projection failures
**Root Cause**: Rewriter adds expressions to phases without checking if input dependencies are satisfied
**Status**:
- ✅ Fixed non-deterministic pattern grouping (sorting by entity symbol)
- ✅ Fixed missing expression outputs (ExpressionPlan.Output now set)
- ✅ Wired up rewriteCorrelatedAggregates() call in planner
- ✅ Fixed expression phase placement (moves expressions to satisfy dependencies)
- ✅ Fixed aggregate metadata movement (follows expressions to correct phase)
- ✅ Fixed phase reordering to respect subquery dependencies
- ✅ Expression outputs now included in Provides field
**Result**: Query planning is now correct and 100% deterministic (no more projection failures)
**Remaining**: Execution returns 0 results - need to debug executor conditional aggregate logic
**See**: commits 51f0842, c0f5bce, ef78db9

### 4. Documentation Cleanup
**Problem**: Old performance docs with outdated/conflicting info
**Status**: ✅ PERFORMANCE_STATUS.md updated with streaming results
**Action**: Move old docs to `docs/archive/optimization-attempts/`

## Medium Term (1-2 Months)

### Query Engine Enhancements
1. **Collection Binding**: `[?x ...]` for set inputs
2. **NOT Clauses**: `(not [?e :attr _])` for negation
3. **OR Clauses**: `(or [...] [...])` for alternatives
4. **Distinct Aggregation**: `(count-distinct ?x)`

### Performance Optimizations
1. ✅ **Streaming Execution**: COMPLETE - Iterator composition with lazy evaluation
2. **Parallel Pattern Execution**: For independent patterns (complex dependency analysis)
3. **Statistics Collection**: For better query planning (requires architecture changes)
4. **Adaptive Streaming Strategy**: Automatically choose streaming vs materialized based on data

**Note**: See `PERFORMANCE_STATUS.md` for realistic assessment of optimization priorities. Focus on proven bottlenecks (parallel execution) over speculative optimizations.

## Long Term (3-6 Months)

### Major Features
1. **Rules System**: Named, reusable query fragments
2. **Pull Syntax**: Entity graph traversal
3. **Recursive Queries**: Graph algorithms
4. **Transaction Functions**: Custom transaction logic

### Infrastructure
1. **WASM Build**: Browser deployment
2. **Distributed Execution**: Multi-node queries
3. **Incremental View Maintenance**: Real-time aggregations
4. **Query Timeout/Cancellation**: Resource limits

## Won't Do (Out of Scope)

These require fundamental architecture changes:
- Full Datomic compatibility (different philosophy)
- Schema enforcement (designed to be schema-less)
- Time-travel queries (would need different storage)
- Lazy evaluation throughout (Go doesn't support well)

## Success Metrics

### Performance Targets
- Simple queries: <10ms
- Complex queries (10+ patterns): <100ms  
- OHLC aggregations: <5s for month of data
- Memory usage: <100MB for typical queries

### Code Quality
- Test coverage: >80%
- All packages have examples
- Core APIs documented
- No V2 functions

## How to Contribute

### Best First Issues
1. Add missing aggregation functions (median, mode)
2. Implement string functions (upper, lower, trim)
3. Add more time extraction functions
4. Write benchmark tests

### Before Starting Work
1. Check this TODO to ensure it's still needed
2. Discuss approach in an issue
3. Write tests first
4. Keep changes focused

## Technical Debt to Address

### High Priority
- [x] Consolidate performance documentation → `PERFORMANCE_STATUS.md` created
- [ ] Archive historical optimization docs → Move to `docs/archive/`
- [ ] Fix examples/ package conflicts → 37 programs can't run via `go test`

### Medium Priority
- [ ] Remove experimental key_mask_iterator_v2.go → Not integrated, benchmarked slower
- [ ] Improve error messages → Add context to query failures
- [ ] Add query explain plan → Show execution strategy
- [ ] Profile memory allocations → Identify hot paths

### Low Priority
- [ ] Add integration test suite → End-to-end query scenarios
- [ ] Add query metrics/telemetry → Production observability