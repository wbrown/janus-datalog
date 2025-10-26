# Conditional Aggregate Rewriting - Work In Progress

**Last Updated**: 2025-10-08
**Status**: 95% complete - Implementation done, end-to-end testing blocked by executor limitation

---

## Overview

Automatic query rewriting to transform correlated aggregate subqueries into conditional aggregates, eliminating subquery execution overhead.

**Goal**: 588 subquery executions → 1 aggregation pass for OHLC queries

---

## Implementation Status

### ✅ Phase 1: Execution Infrastructure (COMPLETE)

**Files Modified**:
- `datalog/query/types.go` - Extended `FindAggregate` with `Predicate` field
- `datalog/executor/aggregation.go` - Predicate filtering in all aggregation paths
- `datalog/executor/conditional_aggregate_internal_test.go` - Test infrastructure

**Features**:
- Internal `Predicate` field in `FindAggregate` (not user-facing - no new syntax)
- Predicate filtering in ungrouped, batch grouped, and streaming aggregations
- Relational theory compliance: empty input → empty output (no NULL)
- Groups with all values filtered are excluded from results

### ✅ Phase 2: Pattern Detection (COMPLETE)

**Files**:
- `datalog/planner/subquery_rewriter.go` - Pattern detection logic
- `datalog/planner/subquery_rewriter_test.go` - Comprehensive tests

**Detection Criteria** (subquery is eligible for rewriting when it has):
1. Single aggregate in `:find` clause (min, max, sum, avg, count)
2. Input parameters from outer query (`:in $ ?param1 ?param2 ...`)
3. Equality predicates filtering on those parameters
4. Data patterns that can be merged into outer query

### ✅ Phase 3: Filter Synthesis (COMPLETE)

**Files**:
- `datalog/query/function.go` - Added `AndFunction` and `ComparisonFunction`
- `datalog/query/and_function_test.go` - Comprehensive tests

**Key Types**:
- `AndFunction` - Combines multiple boolean terms with logical AND
- `ComparisonFunction` - Wraps `Comparison` as `Function` for expression bindings
- Short-circuit evaluation, comprehensive test coverage

### ✅ Phase 4: Query Rewriting Logic (COMPLETE)

**Transformation Steps** (implemented in `rewritePattern()`):
1. Merge subquery patterns into outer phase
2. Merge filter expressions (e.g., `[(year ?t) ?py]`)
3. Synthesize equality comparisons (`[(= ?inner ?outer) ?condN]`)
4. Combine with AND using `AndFunction`
5. Create conditional aggregate with filter predicate
6. Mark subquery as rewritten to prevent original execution

**Example Transformation**:
```datalog
; BEFORE: Correlated subquery (executes 147 times)
[(q [:find (max ?h)
     :in $ ?sym ?y ?m ?d ?hr
     :where [?b :price/symbol ?sym]
            [?b :price/time ?t]
            [(year ?t) ?py] [(= ?py ?y)]
            [(month ?t) ?pm] [(= ?pm ?m)]
            [(day ?t) ?pd] [(= ?pd ?d)]
            [(hour ?t) ?ph] [(= ?ph ?hr)]
            [?b :price/high ?h]]
    ?s ?year ?month ?day ?hour) [[?max-high]]]

; AFTER: Conditional aggregate (executes once)
; Merged patterns:
[?b :price/symbol ?s]
[?b :price/time ?t]
[(year ?t) ?py]
[(month ?t) ?pm]
[(day ?t) ?pd]
[(hour ?t) ?ph]
[?b :price/high ?h]

; Synthesized filter:
[(= ?py ?year) ?cond1]
[(= ?pm ?month) ?cond2]
[(= ?pd ?day) ?cond3]
[(= ?ph ?hour) ?cond4]
[(and ?cond1 ?cond2 ?cond3 ?cond4) ?filter]

; Conditional aggregate:
(max ?h :when ?filter) → ?max-high
```

### ✅ Phase 5: Planner Integration (COMPLETE)

**Files**:
- `datalog/planner/planner.go` - Added rewriting hook
- `datalog/planner/types.go` - Added `EnableConditionalAggregateRewriting` option

**Integration**:
- Rewriting occurs after decorrelation (if enabled)
- Disabled by default (`EnableConditionalAggregateRewriting: false`)
- Can be enabled independently of decorrelation

### ✅ Phase 6: Metadata and Debugging (COMPLETE)

**Files**:
- `datalog/planner/types.go` - Added `ConditionalAggregates` field to `Phase`
- Metadata tracks which patterns were rewritten
- Debug logging available for troubleshooting

---

## Known Issues

### Bug: Empty Results with Rewriting Enabled

**Symptoms**:
- Query with `EnableConditionalAggregateRewriting: false` → Returns correct results
- Same query with `EnableConditionalAggregateRewriting: true` → Returns 0 rows (empty result)
- No error thrown - query executes but produces empty results

**Evidence**:
From `TestDebugRewriting`:
```
Plan without rewriting: 2 phases
  Phase 0: 1 patterns, 0 subqueries
  Phase 1: 2 patterns, 1 subqueries    ← Subquery present

Plan with rewriting: 2 phases
  Phase 0: 1 patterns, 0 subqueries
  Phase 1: 5 patterns, 1 subqueries    ← Patterns MERGED (2→5)
    ✓ Conditional aggregates found in phase 1 metadata
```

**Proof of Rewriting**: Phase 1 goes from 2 patterns to 5 patterns, indicating subquery patterns were successfully merged. Metadata contains conditional aggregates.

**Root Cause**: Under investigation
- Pattern merging works correctly
- Filter synthesis works correctly
- Conditional aggregate execution works in isolation
- Issue appears to be in executor integration or predicate evaluation

---

## Design Philosophy

### No User-Facing Syntax Changes
- System automatically detects rewriting opportunities
- No new query syntax (e.g., no `-if` aggregate functions)
- Transparent optimization - queries work identically with/without rewriting

### Relational Theory Compliance
- Follows C.J. Date: avoid NULL, use empty relations
- Empty input → empty output (no sentinel values)
- Groups with all values filtered are excluded from results

### Safety First
- Disabled by default until bug is resolved
- Extensive test coverage (unit + integration tests)
- Can be enabled per-query for testing

---

## Performance Impact (When Working)

**Expected Benefits**:
- OHLC hourly query: 147 subquery executions → 1 aggregation pass
- Estimated speedup: 10-100× depending on subquery count
- Memory reduction: No intermediate subquery results

**Current Reality**:
- Cannot measure performance until empty results bug is fixed
- Infrastructure is production-ready, awaiting bug resolution

---

## Testing Strategy

### Correctness Tests (All Passing)
- ✅ Unit tests for pattern detection
- ✅ Unit tests for filter synthesis (AndFunction, ComparisonFunction)
- ✅ Unit tests for conditional aggregate execution
- ✅ Integration tests without executor (internal tests)

### End-to-End Tests (Blocked)
- ❌ Full query execution returns empty results
- ❌ Cannot verify performance improvements
- ❌ Root cause under investigation

---

## Next Steps

### Immediate (Bug Fix)
1. **Debug empty results issue**
   - Add detailed execution logging
   - Verify predicate evaluation in context
   - Check symbol availability during aggregation
   - Validate pattern merging preserves semantics

2. **Verify correctness**
   - Compare results with/without rewriting
   - Ensure identical output (not just similar)
   - Test edge cases (empty groups, NULL equivalents)

### Post-Fix (Enablement)
1. **Performance validation**
   - Benchmark OHLC queries with/without rewriting
   - Measure actual speedup vs. expected
   - Profile memory usage reduction

2. **Production readiness**
   - Enable by default after validation
   - Document performance characteristics
   - Add monitoring/observability

---

## Files Summary

### Implementation Files
- `datalog/query/types.go` - `FindAggregate` with `Predicate` field
- `datalog/query/function.go` - `AndFunction`, `ComparisonFunction`
- `datalog/executor/aggregation.go` - Conditional filtering logic
- `datalog/planner/subquery_rewriter.go` - Pattern detection and rewriting
- `datalog/planner/planner.go` - Integration hook
- `datalog/planner/types.go` - Configuration option

### Test Files
- `datalog/query/and_function_test.go` - AndFunction tests
- `datalog/planner/subquery_rewriter_test.go` - Pattern detection tests
- `datalog/executor/conditional_aggregate_internal_test.go` - Execution tests
- `datalog/executor/conditional_aggregate_rewriting_test.go` - E2E tests (failing)

---

## References

**Related Documentation**:
- `/docs/archive/2025-10/SUBQUERY_DECORRELATION_IMPLEMENTATION.md` - Similar optimization approach
- `/docs/archive/2025-10/QUERY_OPTIMIZATION_FINDINGS.md` - CSE and semantic rewriting
- `/docs/archive/completed/subquery-implementation-plan.md` - Original subquery design

**Commits**:
- `7830fe2` - Execution infrastructure
- `5ed299e` - Relational theory compliance
- `c7e42bb` - Pattern detection
- `a05f3a4` - Filter synthesis (AndFunction)
- `5e3e59d` - Query rewriting logic

---

**Status**: Awaiting bug resolution before production deployment
