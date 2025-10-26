# Failing Tests - As of 2025-10-16

**Branch**: `fix-buffered-iterator-architecture`
**Commit**: After expression phase assignment fix

## Summary

- **Tests failing due to our bugs**: 0 ✅
- **Tests failing due to legacy executor bugs**: 0 ✅
- **All validation tests passing!** ✅

**All executor bugs have been resolved!** Both legacy and new executors now handle all test queries correctly.

---

## Root Package: `github.com/wbrown/janus-datalog`

### ✅ TestMultipleAggregateSubqueriesNilBug - FIXED
- **File**: Root package test
- **Issue**: Was returning 0 rows instead of 1 due to entity join bug
- **Fix**: Fixed StreamingRelation.IsEmpty() consuming first tuple (see BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md)

---

## Executor Package: `github.com/wbrown/janus-datalog/datalog/executor`

### ✅ TestComprehensiveExecutorValidation - ALL PASSING

**Status**: All 25 subtests passing ✅

**Passing subtests** (25/25):
- ✅ All basic pattern, predicate, expression, aggregation tests pass
- ✅ `pattern_with_no_results` - Fixed by cacheReady flag
- ✅ `multi-phase_with_expression` - **FIXED** by expression phase assignment after reordering
- ✅ `all_features_combined` - **FIXED** by expression phase assignment after reordering

**Root Cause (Now Fixed)**: Planner was assigning expressions to phases BEFORE phase reordering, causing expressions to end up in phases where their inputs weren't available after reordering.

**The Fix**:
1. Clear expression assignments when re-assigning after reordering
2. Clear expression outputs from Provides before re-assignment
3. Call `updatePhaseSymbols()` BEFORE `assignExpressionsToPhases()` after reordering to ensure correct Available lists
4. Make expression/predicate evaluation lenient (skip if inputs unavailable in specific relation group)
5. Make projection lenient (only project symbols that exist in relation)

**Documentation**: See `docs/bugs/resolved/BUG_LEGACY_EXECUTOR_EXPRESSION_SYMBOL_AVAILABILITY.md` (to be moved to resolved)

---

## Storage Package: `github.com/wbrown/janus-datalog/datalog/storage`

### ✅ TestPredicatePushdownIntegration - NOW PASSING

**All subtests passing**:
- ✅ `WithoutPredicatePushdown`
- ✅ `WithPredicatePushdown`
- ✅ `ExecutorIntegration`

**Fix**: Resolved by combination of entity join fix and cacheReady fix.

### ✅ TestPureAggregationWithBadgerDB - FIXED

**All subtests passing**:
- ✅ `NonAggregated`
- ✅ `PureMaxAggregation`
- ✅ `PureMinAggregation`
- ✅ `PureCountAggregation`

**Fix**: ExecutorOptions weren't being preserved when creating MaterializedRelations in the legacy executor. Changed to use NewMaterializedRelationWithOptions() to preserve EnableStreamingAggregation flag.

---

## Tests Package: `github.com/wbrown/janus-datalog/tests`

### ✅ TestPhaseReorderingEffectiveness - NOW PASSING

**Fix**: Resolved by combination of entity join fix and cacheReady fix.

---

## ✅ ALL NEW EXECUTOR BUGS RESOLVED

**Completed Fixes (New Executor)**:
1. ✅ TestPureAggregationWithBadgerDB - ExecutorOptions preservation
2. ✅ TestMultipleAggregateSubqueriesNilBug - Entity join bug fix
3. ✅ TestEntityJoinBug - Entity join bug fix
4. ✅ TestComprehensiveExecutorValidation/pattern_with_no_results - cacheReady flag (empty stream bug)
5. ✅ TestPredicatePushdownIntegration - All subtests passing
6. ✅ TestPhaseReorderingEffectiveness - All subtests passing

**Outstanding Issues (Legacy Executor)**:
- ❌ 2 subtests in TestComprehensiveExecutorValidation fail due to legacy executor bugs
- **Not new executor bugs** - these are pre-existing issues in legacy code
- See `docs/bugs/active/BUG_LEGACY_EXECUTOR_EXPRESSION_SYMBOL_AVAILABILITY.md`

---

## ✅ BUG FIXED: Entity Join Loses First Tuple

**Status**: RESOLVED (commit a2da7aa)
**Severity**: CRITICAL - Silent data loss
**Doc**: `docs/bugs/BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md`

**Root Cause**: `StreamingRelation.IsEmpty()` called `Next()` to peek, consuming first tuple
- `matcher_strategy.go:52` and `matcher_relations.go:64` called `IsEmpty()` on bindings
- `IsEmpty()` peeked by calling `counter.Next()`, consuming first tuple
- Later `Size()` call triggered materialization of only remaining tuples

**The Fix**:
- Skip `IsEmpty()` check on StreamingRelations (only safe on MaterializedRelations)
- Fixed in `matcher_strategy.go` and `matcher_relations.go`
- Bonus: Fixed CachingIterator race condition in `relation.go`

**Tests Fixed**:
- ✅ TestEntityJoinBug - Returns 5/5 results (was 4/5)
- ✅ TestMultipleAggregateSubqueriesNilBug - Returns correct aggregates (was 0 rows)

---

## ✅ BUG FIXED: Empty Stream Double-Iterator Creation

**Status**: RESOLVED (current commit)
**Severity**: HIGH - Panic on empty query results
**File**: `datalog/executor/relation.go`

**Root Cause**: When a stream had zero tuples:
1. `Materialize()` set `shouldCache = true`, `cache = nil`
2. First `Iterator()` created CachingIterator, iterated zero tuples, signaled complete
3. Cache stayed `nil` (no tuples appended)
4. Second `Iterator()` checked `cache != nil` → false, bypassed "cache ready" path
5. Created second CachingIterator, tried to close already-closed channel
6. **PANIC: close of closed channel**

**The Fix**:
- Added `cacheReady bool` flag to StreamingRelation
- Set by CachingIterator when signaling completion
- Iterator() checks `cacheReady` instead of `cache != nil`
- Works correctly for both empty and non-empty streams

**Tests Fixed**:
- ✅ TestComprehensiveExecutorValidation/pattern_with_no_results - No panic on empty results
- ✅ TestComprehensiveExecutorValidation/filter_eliminates_all_results - No panic
- ✅ TestPredicatePushdownIntegration - All subtests passing
- ✅ TestPhaseReorderingEffectiveness - All subtests passing
