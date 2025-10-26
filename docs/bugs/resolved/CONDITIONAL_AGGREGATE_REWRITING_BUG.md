# Conditional Aggregate Rewriting Bug: Missing Aggregate Input Variable Mapping

## Status: ACTIVE BUG

## Summary

The conditional aggregate rewriting optimization produces incorrect results because it fails to map the aggregate input variable (`?v` in `(max ?v)`) from the subquery to the outer query. This causes the aggregation to operate on the wrong variable, producing nonsensical results (e.g., "Alice" instead of 150).

## Symptom

**Test:** `TestConditionalAggregateRewritingE2E`

**With rewriting disabled:** ✅ PASS
- Returns correct results: `[(Alice, 15, 150), (Alice, 16, 200)]`

**With rewriting enabled:** ❌ FAIL
- Returns wrong results: `[(Alice, 15, Alice), (Alice, 16, Alice)]`
- The third column should be the max value (150, 200) but returns the person name instead

## Root Cause Analysis

### The Query Structure

**Original query with subquery:**
```datalog
[:find ?name ?day ?max-value
 :where
 [?p :person/name ?name]
 [?e :event/person ?p]
 [?e :event/time ?time]
 [(day ?time) ?day]

 ; Subquery computes max value for each (person, day) pair
 [(q [:find (max ?v)
      :in $ ?person ?d
      :where
      [?ev :event/person ?person]
      [?ev :event/time ?t]
      [(day ?t) ?pd]
      [(= ?pd ?d)]
      [?ev :event/value ?v]]   ; ← ?v is the aggregate input
    $ ?p ?day) [[?max-value]]]]
```

**The subquery:**
- Has input parameters: `?person`, `?d`
- Retrieves event values via: `[?ev :event/value ?v]`
- Aggregates over: `(max ?v)` where `?v` comes from `:event/value`

**The outer query:**
- Has patterns for `:person/name`, `:event/person`, `:event/time`
- Does NOT retrieve `:event/value` (because that's what the subquery does!)

### What the Rewriter Does

The rewriter attempts to eliminate the subquery by:

1. **Map subquery parameters to outer variables:**
   ```
   ?person → ?p    ✅ Correct
   ?d → ?day       ✅ Correct
   ```

2. **Map subquery entity variables to outer entity variables:**
   ```
   ?ev → ?e        ✅ Correct (both access events via :event/person)
   ?t → ?time      ✅ Correct (both access :event/time)
   ```

3. **Map subquery value variables to outer value variables:**
   ```
   ?v → ???        ❌ FAILS - No match found!
   ```

### Why ?v Mapping Fails

The variable unification logic (lines 371-428 in `subquery_rewriter.go`) tries to find matching patterns between the subquery and outer query. For `?v`:

- **Subquery has:** `[?ev :event/value ?v]`
- **Outer query has:** Nothing with `:event/value`
- **Result:** No match found, `?v` remains unmapped

The rewriter DOES merge the pattern `[?e :event/value ?v]` into the outer query (after renaming `?ev` → `?e`), but this happens AFTER the variable mapping phase completes. So `?v` in the merged pattern has no connection to the aggregate.

### What Happens During Execution

When the conditional aggregate executes, it needs to know which variable contains the values to aggregate. Debug output shows:

```
Phase.Provides: [?e ?p ?time ?day ?v ?__cond_?pd]
Phase.Keep: [?day ?p]
```

The aggregate metadata says to aggregate over `?v`, but:
1. `?v` is in `Provides` (it gets computed)
2. `?v` is NOT in `Keep` (it doesn't get projected forward!)
3. The aggregation logic receives tuples with columns `[?name ?day ...]` but no `?v`
4. It falls back to aggregating over the wrong column (probably column 0 = ?name)
5. Result: "Alice" instead of 150

## The Architectural Problem

The conditional aggregate rewriting has a fundamental design flaw:

**Assumption:** All variables in the subquery can be mapped to existing variables in the outer query.

**Reality:** Subqueries often compute NEW values that don't exist in the outer query. That's the whole point of having a subquery!

The rewriter tries to:
1. Map all subquery variables to outer variables
2. Merge subquery patterns into outer query
3. Convert the subquery into a conditional aggregate

But step 1 fails for variables that are subquery-specific (like `?v` from `:event/value`), breaking steps 2 and 3.

## Plan to Fix

### Option 1: Fix Variable Mapping (Recommended)

**Strategy:** Allow unmapped variables to remain as subquery-local variables, but ensure they're properly tracked through to aggregation.

**Implementation:**

1. **Extend variable mapping to handle unmapped variables:**
   ```go
   // After attempting to map variables via pattern matching,
   // identify which variables are still unmapped
   unmappedVars := []query.Symbol{}
   for _, pattern := range subqueryPatterns {
       for _, var := range extractVariables(pattern) {
           if _, mapped := varMap[var]; !mapped {
               unmappedVars = append(unmappedVars, var)
           }
       }
   }
   ```

2. **Ensure unmapped variables are included in Phase.Keep:**
   ```go
   // When updating Phase.Keep, add all aggregate input variables
   // even if they're unmapped
   aggregateInputVar := pattern.Aggregate.Arg // e.g., ?v
   if aggregateInputVar != "" {
       // Don't rename if unmapped
       if _, ok := varMap[aggregateInputVar]; !ok {
           phase.Keep = append(phase.Keep, aggregateInputVar)
       } else {
           renamedVar := varMap[aggregateInputVar]
           phase.Keep = append(phase.Keep, renamedVar)
       }
   }
   ```

3. **Update conditional aggregate metadata to use correct variable name:**
   ```go
   // Store the actual variable name (mapped or unmapped)
   actualAggVar := pattern.Aggregate.Arg
   if mapped, ok := varMap[actualAggVar]; ok {
       actualAggVar = mapped
   }

   conditionalAgg.Arg = actualAggVar
   ```

4. **Ensure the executor projection includes aggregate variables:**
   - Current bug: `Phase.Keep = [?day ?p]` but doesn't include `?v`
   - Fix: Always include aggregate input variables in Keep
   - This ensures they're available when the aggregation executes

### Option 2: Disable Rewriting for Queries with Unmappable Variables

**Strategy:** Detect when a subquery has variables that can't be mapped and skip rewriting.

**Implementation:**
```go
func analyzeSubqueryForRewriting(subqIdx int, subqPlan *SubqueryPlan) (CorrelatedAggregatePattern, bool) {
    // ... existing detection logic ...

    // NEW: Check if aggregate input variable can be mapped
    aggVar := agg.Arg
    if aggVar != "" {
        canMap := false
        // Check if aggVar appears in any pattern that also appears in outer query
        for _, pattern := range subqueryPatterns {
            if patternContainsVar(pattern, aggVar) {
                if outerQueryHasMatchingPattern(pattern, outerQuery) {
                    canMap = true
                    break
                }
            }
        }

        if !canMap {
            // Cannot safely rewrite - aggregate input has no outer mapping
            return pattern, false
        }
    }

    return pattern, true
}
```

**Pros:** Safe, won't break queries
**Cons:** Disables optimization for many valid cases

### Option 3: Two-Phase Variable Mapping

**Strategy:** Map variables in two phases:
1. First phase: Map input parameters and entity variables (as currently done)
2. Second phase: After merging patterns, map value variables using the new patterns

**Implementation:**
```go
// Phase 1: Map input parameters and entity variables (existing code)
varMap := createInitialMapping(subqPlan, outerQuery)

// Merge patterns (existing code)
mergeSubqueryPatterns(phase, subqueryPatterns, varMap)

// Phase 2: Map value variables using newly merged patterns
for _, sqPattern := range subqueryPatterns {
    renamedPattern := renamePatternVariables(sqPattern, varMap)
    // Now find this pattern in the merged phase patterns
    for _, mergedPattern := range phase.Patterns {
        if patternsMatch(renamedPattern, mergedPattern) {
            // Extract additional variable mappings
            extractValueMappings(sqPattern, mergedPattern, varMap)
        }
    }
}
```

**Pros:** More thorough variable mapping
**Cons:** Complex, might introduce new bugs

## Recommended Fix: Option 1

Option 1 is the best approach because:
1. **Semantically correct:** Variables that don't exist in outer query should be preserved as-is
2. **Minimal changes:** Small modifications to existing logic
3. **Preserves optimization:** Doesn't disable rewriting unnecessarily
4. **Clear ownership:** Each variable clearly belongs to either outer query or subquery

The key insight is: **Not all subquery variables need to map to outer variables.** Some variables are legitimately subquery-local (like `?v` from `:event/value`), and the rewriter should handle them gracefully.

## Testing Strategy

After implementing the fix:

1. **Verify TestConditionalAggregateRewritingE2E passes:**
   - Both with and without rewriting should return: `[(Alice, 15, 150), (Alice, 16, 200)]`

2. **Add test for unmapped aggregate variables:**
   ```datalog
   ; Outer query doesn't access :event/value
   ; Subquery aggregates over ?value from :event/value
   [:find ?person ?max
    :where
    [?p :person/name ?person]
    [(q [:find (max ?value)
         :in $ ?person
         :where
         [?e :event/person ?person]
         [?e :event/value ?value]]
       $ ?p) [[?max]]]]
   ```

3. **Verify phase metadata is correct:**
   - `Phase.Keep` includes aggregate input variable
   - `Phase.Provides` includes aggregate input variable
   - Conditional aggregate metadata has correct variable name

4. **Check projection logic:**
   - Executor properly projects aggregate variables before aggregation
   - Final result has correct columns in correct order

## Related Bugs Fixed

### Bug 1: Encoder Mismatch (FIXED)

**Before this investigation, we discovered and fixed:**
- `database.go` comment said "Uses L85 encoding by default"
- `badger_store.go` actually defaulted to BinaryStrategy
- Decoder tried to read Binary keys as L85, producing garbage
- **Fix:** Explicitly use BinaryStrategy in NewDatabase()

This encoder bug caused Identity values to be all zeros, which masked the conditional aggregate bug. After fixing the encoder bug, the conditional aggregate bug became visible.

## The Actual Fix (Implemented)

### Root Cause Discovery

The bug manifested in multi-phase queries where:
1. Phase 0 executed the conditional aggregate rewriting and stored metadata
2. Phase 1+ added additional patterns (like looking up `?name`)
3. Phase 1+ didn't know about the aggregate requirements and projected away critical columns

**Concrete example from failing test:**
```
Phase 0: [?e :event/person ?p], [?e :event/time ?time], [(day ?time) ?day],
         [?e :event/value ?v], [(= ?pd ?day) ?__cond_?pd]
         → Provides: [?e ?p ?time ?day ?v ?__cond_?pd]
         → Keep: [?day ?p ?v ?__cond_?pd] ✓

Phase 1: [?p :person/name ?name]
         → Provides: [?name ?day ?p ?v ?__cond_?pd]
         → Keep: [?name ?day] ✗ WRONG! Dropped ?v and ?__cond_?pd

Result: Aggregation receives [?name ?day] instead of [?name ?day ?v ?__cond_?pd]
        → max() operates on wrong column → returns "Alice" instead of 150
```

### The Fix: Phase Symbol Metadata Propagation

**File:** `datalog/planner/phase_reordering.go`
**Function:** `updatePhaseSymbols`
**Lines:** 342-358

The `updatePhaseSymbols` function recalculates `Keep` columns after phase reordering. It was only checking the current phase for aggregate metadata:

```go
// BEFORE (BROKEN):
// 2b. Keep symbols needed for conditional aggregates in THIS phase
if phases[i].Metadata != nil {
    if aggCols, ok := phases[i].Metadata["aggregate_required_columns"]; ok {
        if cols, ok := aggCols.([]query.Symbol); ok {
            for _, sym := range cols {
                if available[sym] {
                    keep[sym] = true
                }
            }
        }
    }
}
```

This failed because only Phase 0 had the metadata. Phase 1+ would drop the aggregate columns.

**Fix:** Check ALL previous phases (0 through current) for aggregate metadata:

```go
// AFTER (FIXED):
// 2b. Keep symbols needed for conditional aggregates in ANY phase
// These are stored in phase metadata by the conditional aggregate rewriter
// IMPORTANT: Aggregate required columns must be carried through ALL later phases,
// not just the phase that has the metadata!
for j := 0; j <= i; j++ {
    if phases[j].Metadata != nil {
        if aggCols, ok := phases[j].Metadata["aggregate_required_columns"]; ok {
            if cols, ok := aggCols.([]query.Symbol); ok {
                for _, sym := range cols {
                    if available[sym] {
                        keep[sym] = true
                    }
                }
            }
        }
    }
}
```

**Why this works:** Aggregate metadata from Phase 0 now propagates to ALL subsequent phases. Each phase preserves the aggregate input columns (`?v`, `?__cond_?pd`) in its Keep list, ensuring they're available when the final aggregation executes.

### Verification: Before vs After

**Before fix (test output):**
```
Phase 0 Keep: [?day ?p ?v ?__cond_?pd] ✓
Phase 1 Keep: [?name ?day] ✗
currentResult columns: [?name ?day]
Result: [Alice, 15, Alice] ✗ WRONG!
```

**After fix (test output):**
```
Phase 0 Keep: [?day ?p ?v ?__cond_?pd] ✓
Phase 1 Keep: [?name ?day ?v ?__cond_?pd] ✓
currentResult columns: [?name ?day ?v ?__cond_?pd]
Result: [Alice, 15, 150] ✓ CORRECT!
```

### Test Results

Both test cases now pass with identical results:
- **Without rewriting:** `[(Alice, 15, 150), (Alice, 16, 200)]` ✓
- **With rewriting:** `[(Alice, 15, 150), (Alice, 16, 200)]` ✓

The optimization produces identical results to the non-optimized path, confirming semantic correctness.

### Why This Bug Was Subtle

1. **Single-phase queries worked fine** - The bug only manifested with 2+ phases
2. **Required multiple conditions:**
   - Conditional aggregate rewriting enabled
   - Multi-phase query plan (e.g., event patterns + person name lookup)
   - Unmapped subquery variables (like `?v` from `[?ev :event/value ?v]`)
3. **Masked by earlier bug** - The encoder mismatch bug (Binary vs L85) caused all Identity values to be empty, making it impossible to see the aggregate bug until that was fixed first

### Architectural Lesson

**Phase metadata must be treated as transitive:** When a phase creates metadata that affects execution (like aggregate requirements), that metadata must propagate to ALL subsequent phases. The planner can't assume metadata is phase-local.

This is similar to liveness analysis in compilers: if a variable is "live" (needed later), all intermediate phases must preserve it, even if they don't directly use it.

## Status: RESOLVED ✅

- [x] Bug identified and documented
- [x] Root cause understood
- [x] Fix plan created
- [x] Fix implemented
- [x] Tests passing
- [x] Debug output removed
