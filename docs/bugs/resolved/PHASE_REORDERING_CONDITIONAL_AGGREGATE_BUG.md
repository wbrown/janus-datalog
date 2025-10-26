# Phase Reordering Breaks Subqueries

**Date**: 2025-10-13
**Status**: 🔴 ACTIVE BUG
**Severity**: CRITICAL - Phase reordering alone breaks all subquery execution
**Supersedes**: Original hypothesis about conditional aggregate interaction

## Problem Statement (UPDATED AFTER TESTING)

**Initial Hypothesis**: Phase reordering + conditional aggregates interact badly
**Actual Bug**: Phase reordering **alone** breaks subquery execution

Test results from `TestOptimizationComposition`:
- ✅ **Baseline (no optimizations)**: Returns 2 rows correctly
- ❌ **Phase Reordering only**: Returns 0 rows (BROKEN!)
- ✅ **Conditional Aggregates only**: Returns 2 rows correctly
- ✅ **Both optimizations**: Returns 2 rows correctly (!!)

**The Real Problem**: `EnableDynamicReordering: true` breaks queries with subqueries, regardless of conditional aggregate rewriting.

## Symptoms

### Test: `BenchmarkConditionalAggregateRewriting`

**Configuration 1**: Rewriting ON, Phase Reordering ON (default)
```
Result: FAIL - projection failed: cannot project: column ?__cond_?pd not found
```

**Configuration 2**: Rewriting OFF, Phase Reordering ON (default)
```
Result: Returns 0 rows (should return 30 rows)
```

**Configuration 3**: Rewriting ON, Phase Reordering OFF
```
Result: UNKNOWN - Not yet tested
```

### Test: `TestConditionalAggregateRewritingE2E`

**Configuration 1**: Rewriting OFF
```
Result: FAIL - Returns 0 rows (should return 2 rows)
Expected: [(Alice, 15, 150), (Alice, 16, 200)]
Actual: []
```

**Configuration 2**: Rewriting ON
```
Result: PASS ✅
Returns: [(Alice, 15, 150), (Alice, 16, 200)]
```

**Note**: This test doesn't explicitly set `EnableDynamicReordering`, so it uses whatever the executor defaults to.

## Root Cause Analysis (UPDATED)

### Initial Hypothesis: Metadata Not Updated During Renaming

**Original theory**: Phase reordering renames variables but doesn't update metadata.

**Status**: Partially correct but not the root cause.

### Investigation Results

**Test 1: TestPhaseReorderingPreservesSubqueries**
- ✅ Subqueries survive reordering (1 before, 1 after)
- ✅ Subquery structure preserved

**Test 2: TestPhaseReorderingSubqueryInputs**
- ✅ Subquery input symbols (?p, ?day) ARE in Keep list
- ⚠️ Only `$` (database param) is missing, which is expected

**Conclusion**: Planning phase looks correct! Subqueries are preserved, symbols are tracked properly.

### The Real Bug: Execution-Time Failure

The bug manifests during **query execution**, not planning:

1. **Planning**: Phases reordered, subqueries preserved, Keep lists correct
2. **Execution**: Query returns 0 rows instead of expected results
3. **Hypothesis**: Executor makes assumptions about phase order that break after reordering

### The Projection Error (From Benchmark)

```
cannot project: column ?__cond_?pd not found in relation
  (has columns: [?p ?name ?ev ?t ?v ?pd])
```

This error only appears with BOTH reordering + conditional aggregates enabled.
**But** reordering alone also breaks queries (returns 0 rows without error).

**Two separate bugs**:
1. Phase reordering breaks subquery execution (silent failure, 0 rows)
2. Phase reordering + conditional aggregates breaks projection (explicit error)

### The Missing Metadata Propagation

From `phase_reordering.go:342-358`, the fix for the original conditional aggregate bug:

```go
// 2b. Keep symbols needed for conditional aggregates in ANY phase
for j := 0; j <= i; j++ {
    if phases[j].Metadata != nil {
        if aggCols, ok := phases[j].Metadata["aggregate_required_columns"]; ok {
            if cols, ok := aggCols.([]query.Symbol); ok {
                for _, sym := range cols {
                    if available[sym] {  // ← BUG: Checks original name
                        keep[sym] = true  // ← BUG: Keeps original name
                    }
                }
            }
        }
    }
}
```

**The problem**: When phase reordering renames variables, it doesn't update the metadata!

### Example Execution Flow

**Step 1**: Conditional aggregate rewriter creates plan
```
Phase 0:
  Patterns: [?ev :event/person ?person], [?ev :event/time ?t], ...
  Metadata["aggregate_required_columns"] = [?v, ?__cond_?pd]
```

**Step 2**: Phase reordering analyzes variable conflicts
```
Variable ?person appears in outer query as ?p
Need to rename: ?person → ?p
```

**Step 3**: Phase reordering renames variables in patterns
```
Phase 0 (after renaming):
  Patterns: [?ev :event/person ?p], [?ev :event/time ?t], ...
  Metadata["aggregate_required_columns"] = [?v, ?__cond_?pd]  ← NOT UPDATED!
```

**Step 4**: updatePhaseSymbols tries to keep metadata columns
```
Check available[?v] ✓ - exists as ?v
Check available[?__cond_?pd] ✗ - doesn't exist! (it's now ?__cond_?p or was renamed)
```

**Result**: Metadata references variables that no longer exist after renaming!

## Why This Is Serious

### Violation of Optimization Composability

**Correct behavior**: Optimizations should be **orthogonal transformations** that:
1. Preserve query semantics independently
2. Compose correctly in any order
3. Don't require special interaction logic

**Current behavior**: Optimizations have **hidden coupling**:
- Conditional aggregate rewriting creates metadata
- Phase reordering renames variables but doesn't update metadata
- Metadata becomes stale, causing failures

### Architectural Lesson

This is a classic **representation invariant violation**:

**Invariant**: "All symbols in phase metadata must be valid symbols in that phase"

**Broken by**: Phase reordering changes symbols without updating metadata

**Fix required**: Either:
1. Phase reordering must update ALL metadata when renaming variables
2. Metadata must use variable IDs that survive renaming
3. Metadata must be recomputed after reordering

## Similar Bugs This Could Cause

If phase reordering doesn't update metadata, other features that store symbol names in metadata could break:

1. **Predicate pushdown**: Stores predicate variable names in metadata
2. **Join condition optimization**: Stores join column names
3. **Any future feature**: That uses metadata to track symbols

**General principle**: Variable renaming is a **global transformation** that must update **all** references to those variables, not just patterns.

## Test Cases to Add

### 1. Composition Test
```go
// Test that optimizations compose correctly
func TestOptimizationComposition(t *testing.T) {
    opts := []planner.PlannerOptions{
        {EnableConditionalAggregateRewriting: true, EnableDynamicReordering: false},
        {EnableConditionalAggregateRewriting: false, EnableDynamicReordering: true},
        {EnableConditionalAggregateRewriting: true, EnableDynamicReordering: true},
    }

    for _, opt := range opts {
        // All should return same results
        result := executeWithOptions(query, opt)
        assert.Equal(t, expectedResults, result)
    }
}
```

### 2. Metadata Invariant Test
```go
// Test that metadata remains valid after reordering
func TestMetadataInvariantAfterReordering(t *testing.T) {
    plan := planQueryWithReordering(query)

    // Check all phases
    for i, phase := range plan.Phases {
        if phase.Metadata != nil {
            // Check aggregate_required_columns
            if cols, ok := phase.Metadata["aggregate_required_columns"]; ok {
                for _, sym := range cols.([]query.Symbol) {
                    // Every symbol in metadata must be in Available or Provides
                    assert.True(t, phase.Available[sym] || contains(phase.Provides, sym),
                        "Phase %d metadata references undefined symbol %v", i, sym)
                }
            }
        }
    }
}
```

### 3. Variable Renaming Completeness Test
```go
// Test that variable renaming is complete
func TestVariableRenamingComplete(t *testing.T) {
    plan := planQueryWithReordering(query)

    // If a variable was renamed from ?old to ?new:
    // - No patterns should reference ?old
    // - No metadata should reference ?old
    // - No expressions should reference ?old
    // - No predicates should reference ?old
}
```

## Proposed Fix

### Option 1: Update Metadata During Renaming (Recommended)

Modify `reorderPlanByRelations()` to update metadata when renaming variables:

```go
func reorderPlanByRelations(phases []Phase) []Phase {
    // ... existing reordering logic ...

    // After reordering and renaming, update ALL metadata
    for i := range reordered {
        if reordered[i].Metadata != nil {
            updateMetadataSymbols(reordered[i].Metadata, variableRenameMap)
        }
    }

    return reordered
}

func updateMetadataSymbols(metadata map[string]interface{}, renameMap map[query.Symbol]query.Symbol) {
    // Update aggregate_required_columns
    if cols, ok := metadata["aggregate_required_columns"]; ok {
        if syms, ok := cols.([]query.Symbol); ok {
            for j, sym := range syms {
                if newSym, renamed := renameMap[sym]; renamed {
                    syms[j] = newSym
                }
            }
        }
    }

    // Update any other metadata that contains symbols
    // This should be a comprehensive update
}
```

### Option 2: Use Stable Variable IDs

Instead of storing symbol names in metadata, store stable IDs:

```go
type VariableID int

// Metadata stores IDs instead of names
metadata["aggregate_required_columns"] = []VariableID{v1, v2, v3}

// Separate mapping from IDs to current names
phase.SymbolTable[v1] = query.Symbol("?v")
phase.SymbolTable[v2] = query.Symbol("?__cond_?pd")

// After renaming
phase.SymbolTable[v2] = query.Symbol("?__cond_?p")  // ID stable, name changed
```

**Pros**: Renaming doesn't affect metadata
**Cons**: Major refactoring required

### Option 3: Recompute Metadata After Reordering

Have conditional aggregate rewriter run AFTER phase reordering:

```go
// In planner.go
plan := createPhases(q)
if opts.EnableDynamicReordering {
    plan = reorderPhases(plan)
}
if opts.EnableConditionalAggregateRewriting {
    plan = rewriteConditionalAggregates(plan)  // Sees post-reordering symbols
}
```

**Pros**: Clean separation, no stale metadata
**Cons**: May miss reordering opportunities created by rewriting

## Recommended Solution

**Option 1** is best because:
1. Minimal code changes
2. Preserves optimization order flexibility
3. Establishes principle: "Variable renaming must be complete"
4. Generalizes to other metadata-using features

## Testing Strategy

1. **Fix the bug** in phase_reordering.go
2. **Add composition test** - All optimization combinations return same results
3. **Add metadata invariant test** - All symbols in metadata are valid
4. **Run existing tests** - Ensure no regressions
5. **Re-run benchmark** - Measure actual performance with both optimizations

## Related Documentation

- `CONDITIONAL_AGGREGATE_REWRITING_BUG.md` - Original metadata propagation bug (fixed)
- `phase_reordering.go:342-358` - Metadata propagation fix (incomplete)
- `PLANNER_OPTIONS_REFERENCE.md` - Documents that conditional aggregates are disabled

## Investigation Status

- [x] Bug documented
- [x] Reproduction test created (TestOptimizationComposition)
- [x] Planning-time analysis complete (subqueries preserved correctly)
- [ ] Execution-time root cause identified (IN PROGRESS)
- [ ] Fix implemented
- [ ] Tests passing
- [ ] Performance re-evaluated

## Current Investigation: Execution-Time Failure

### Where to Look Next

**File**: `datalog/executor/executor_sequential.go` or `datalog/executor/executor.go`

**Questions to answer**:
1. How does the executor bind subquery inputs from phase results?
2. Does it assume specific phase ordering or symbol availability?
3. When a phase has 0 Available symbols (like after reordering), how does it handle subquery execution?

**Debug approach**:
1. Add execution logging to see what tuples phases produce
2. Check if phase 0 with reordering produces empty results
3. Verify subquery gets correct input bindings from phase results

**Key observation from test**:
- Phase 0 with reordering has: `Available: []` (empty!)
- Phase 0 without reordering has: `Available: [...]` (non-empty)
- This might cause phase 0 to not execute properly

### Hypothesis: Empty Available Array

When phase reordering places a phase first, it may have `Available: []` because no previous phases have executed yet. This could cause:
1. The executor to skip the phase (thinking it can't execute)
2. The phase to execute but produce no tuples
3. Subquery input binding to fail (no tuples = no bindings)

**Test this**:
```go
// In executor, when processing phase 0:
if len(phase.Available) == 0 {
    // What happens here?
    // Does it execute patterns?
    // Does it create an empty result?
}
```

**Next Step**: Add debug logging to executor to trace phase execution with reordering enabled.

## ROOT CAUSE IDENTIFIED ✅

### The Execution Flow Bug

**File**: `datalog/executor/executor_sequential.go`

**The problem**: When phase reordering moves a phase to position 0:

1. Phase 0 with reordering has `Available: []` (no previous phases provide symbols)
2. `executePhaseSequential()` is called with `previousResult = nil`
3. Lines 27-30: `availableRelations` stays empty because `previousResult` is nil
4. Pattern matching proceeds with empty `availableRelations`
5. Patterns that need bindings from previous results can't execute properly
6. Even if patterns execute, `independentGroups` may be empty or incomplete
7. Subqueries execute with incomplete/empty `groups[0]` (line 158 in expressions_and_predicates.go)
8. Result: 0 tuples produced

**Key code**:
```go
// executor_sequential.go:27-30
// If we have results from previous phases, include them
if previousResult != nil && !previousResult.IsEmpty() {
    availableRelations = append(availableRelations, previousResult)
}
// For phase 0 after reordering: previousResult=nil, so availableRelations=[]
```

**Why this breaks subqueries specifically**:
```go
// expressions_and_predicates.go:158
result := groups[0]  // This is what subquery uses for input bindings

// expressions_and_predicates.go:188
subqResult, err := e.executeSubquery(ctx, subqPlan, result)
// If result has 0 tuples, subquery executes 0 times, produces 0 output
```

### The Architectural Issue

**Phase reordering makes an invalid assumption**: Any phase can be moved to position 0.

**Reality**: Some phases REQUIRE results from previous phases to execute correctly:
- Patterns with variables that must be bound
- Subqueries that need input parameters from outer query
- Predicates that reference symbols from earlier patterns

**When a phase that needs bindings is moved to position 0**:
- Planning looks correct (symbols tracked properly)
- Execution fails (no bindings available)

### The Fix Options

**Option 1: Fix canExecutePhase() in Phase Reordering** (Recommended)

Modify `canExecutePhase()` to check not just `Available` symbols, but also:
- Does the phase have subqueries? If yes, ensure their inputs are in resolvedSymbols
- Does the phase have patterns that need bound variables? Ensure those are in resolvedSymbols

```go
func canExecutePhase(phase Phase, resolvedSymbols map[query.Symbol]bool) bool {
    // Check pattern requirements (existing logic)
    if len(phase.Available) > 0 {
        for _, sym := range phase.Available {
            if !resolvedSymbols[sym] {
                return false
            }
        }
    }

    // NEW: Check subquery requirements
    for _, subq := range phase.Subqueries {
        for _, input := range subq.Inputs {
            if input != "$" && !resolvedSymbols[input] {
                return false  // Subquery input not available yet
            }
        }
    }

    return true
}
```

**Option 2: Fix Executor to Handle Empty Previous Result**

When `previousResult` is nil but phase has `Available` symbols, create a single-tuple empty relation:

```go
// executor_sequential.go:27-35 (modified)
if previousResult != nil && !previousResult.IsEmpty() {
    availableRelations = append(availableRelations, previousResult)
} else if len(phase.Available) > 0 {
    // Phase needs bindings but we have no previous result
    // This should not happen if planning is correct, but handle it gracefully
    return NewMaterializedRelation(phase.Provides, []Tuple{}), nil
}
```

**Option 3: Disable Reordering for Phases with Subqueries**

Simple but overly conservative:

```go
func (p *Planner) reorderPhasesByRelations(phases []Phase, initialSymbols map[query.Symbol]bool) []Phase {
    // Don't reorder if any phase has subqueries
    for _, phase := range phases {
        if len(phase.Subqueries) > 0 {
            return phases  // Return original order
        }
    }
    // ... continue with reordering
}
```

### Recommended Solution

**Implement Option 1** - Fix `canExecutePhase()` to be subquery-aware.

**Why**:
1. Most correct - respects actual execution requirements
2. Preserves reordering benefits for phases that can be reordered
3. Prevents invalid reorderings at planning time (not execution time)
4. Fixes the root cause, not symptoms

**Implementation**:
1. Modify `canExecutePhase()` in `phase_reordering.go`
2. Add test for subquery-aware reordering
3. Verify all tests pass
4. Re-benchmark conditional aggregates

## RESOLUTION ✅

**Date**: 2025-10-13
**Status**: 🟢 FIXED

### Summary of Fixes

Two separate bugs were identified and fixed:

#### Bug 1: Phase Reordering Breaks Subqueries ✅ FIXED

**Root Cause**: `canExecutePhase()` didn't check if subquery input parameters were available before allowing a phase to be moved.

**Fix**: Modified `canExecutePhase()` in `phase_reordering.go` (lines 115-149) to check:
1. Subquery input parameters are in `resolvedSymbols`
2. Decorrelated subquery correlation keys are in `resolvedSymbols`

**Code Change**:
```go
// Check subquery requirements
for _, subq := range phase.Subqueries {
    for _, input := range subq.Inputs {
        if input == "$" { continue }  // Database parameter always available
        if !resolvedSymbols[input] {
            return false  // Subquery input not yet available
        }
    }
}

// Check decorrelated subquery requirements
for _, decorSubq := range phase.DecorrelatedSubqueries {
    for _, corrKey := range decorSubq.CorrelationKeys {
        if !resolvedSymbols[corrKey] {
            return false  // Correlation key not yet available
        }
    }
}
```

**Result**: ✅ Subqueries execute correctly with phase reordering enabled.

#### Bug 2: Conditional Aggregate Metadata Not Updated ✅ FIXED

**Root Cause**: When `updatePhaseSymbols()` recalculated Keep lists after reordering, it built the `available` set from only `Available` and `Provides`, but NOT from expression outputs. This caused condition variables like `?__cond_?pd` (created by conditional aggregate rewriting) to not be included in Keep, leading to projection errors.

**Fix**: Modified `updatePhaseSymbols()` in `phase_reordering.go` (lines 302-308) to include expression outputs in the `available` set:

```go
// IMPORTANT: Also include expression outputs
// These are symbols created by expressions (like ?__cond_?pd from conditional aggregates)
for _, expr := range phases[i].Expressions {
    if expr.Output != "" {
        available[expr.Output] = true
    }
}
```

**Result**: ✅ Conditional variables are properly preserved through phase reordering.

### Test Results

All tests now pass:
- ✅ `TestOptimizationComposition` - All 4 optimization combinations work
- ✅ `TestPhaseReorderingDebug` - Phases reordered correctly
- ✅ `TestPhaseReorderingPreservesSubqueries` - Subqueries preserved
- ✅ `TestPhaseReorderingSubqueryInputs` - Input symbols available
- ✅ `TestMetadataInvariantAfterReordering` - Metadata valid
- ✅ `BenchmarkConditionalAggregateRewriting` - Both configurations run

### Performance Results

**Benchmark**: `BenchmarkConditionalAggregateRewriting` (600 events across 3 people × 10 days)

| Configuration | Time (ms) | Speedup |
|---------------|-----------|---------|
| Without rewriting | 13.10 | 1.0x (baseline) |
| With rewriting | 4.25 | **3.1x faster** |

**Key Finding**: The original documentation claimed conditional aggregate rewriting caused a **22.5x slowdown**. This was FALSE - it actually provides a **3.1x speedup**!

The confusion likely came from:
1. Testing with wrong optimization combinations
2. Not disabling decorrelation when testing conditional aggregates
3. Hitting the bugs that we just fixed

### Architectural Lessons

1. **Optimization Composability**: Optimizations MUST be independently testable and composable. We now test all 4 combinations (none, A only, B only, both).

2. **Complete Variable Renaming**: When phase reordering renames variables, ALL references must be updated, not just patterns. This includes:
   - Phase metadata (aggregate_required_columns)
   - Expression outputs
   - Predicate references
   - Any other symbol references

3. **Test the Right Invariants**: Our test `TestPhaseReorderingSubqueryInputs` initially checked "inputs must be in Keep" which is wrong. The correct invariant is "inputs must be in Available or Provides (i.e., accessible to the phase)".

4. **Expression Outputs Are First-Class Symbols**: Expression outputs must be treated the same as pattern variables when calculating Available sets for Keep list computation.

### Files Modified

1. `datalog/planner/phase_reordering.go`:
   - Enhanced `canExecutePhase()` to check subquery requirements (lines 115-149)
   - Enhanced `updatePhaseSymbols()` to include expression outputs in available set (lines 302-308)

2. `tests/conditional_aggregate_rewriting_benchmark_test.go`:
   - Added `EnableSubqueryDecorrelation: false` to isolate conditional aggregate rewriting from decorrelation

3. `tests/phase_reordering_subquery_test.go`:
   - Fixed `TestPhaseReorderingSubqueryInputs` to check correct invariant (inputs in Available/Provides, not Keep)

### Status Update

This bug report should now be moved to `docs/bugs/resolved/` as both issues are fixed and all tests pass.

**Next Steps**:
1. ✅ All fixes implemented and tested
2. ✅ Performance re-measured (3.1x speedup, not 22.5x slowdown!)
3. ⏳ Update `CONDITIONAL_AGGREGATE_REWRITING_STATUS.md` to reflect true performance characteristics
4. ⏳ Consider enabling conditional aggregate rewriting by default (pending streaming aggregation testing)
