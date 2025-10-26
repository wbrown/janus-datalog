# Input Parameter in Keep Bug: Phase Symbol Projection Error

## Status: RESOLVED ✅

## Summary

The phase symbol update logic was incorrectly adding input parameters to the `Keep` list even when those parameters weren't present in the relation produced by the phase patterns. This caused projection errors like "cannot project: column ?symbol not found in relation".

## Symptom

**Test:** `TestExecuteQueryWithTimeInput` (datalog/storage/query_inputs_test.go)

**Error:**
```
Query failed: query execution failed: phase 2 failed: projection failed:
cannot project: column ?symbol not found in relation (has columns: [?s ?p ?time ?close])
```

**Query:**
```datalog
[:find ?time ?close
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]
        [?p :price/close ?close]]
```

**What happened:**
- Phase 0 pattern `[?s :symbol/ticker ?symbol]` uses the input parameter `?symbol`
- Phase 1 patterns `[?p :price/symbol ?s]`, `[?p :price/time ?time]`, `[?p :price/close ?close]`
  - Available: `[?symbol ?s]` (inputs from phase 0)
  - Provides: `[?close ?p ?s ?time]` (outputs from patterns)
  - Keep: `[?time ?close ?symbol]` ✗ WRONG! `?symbol` isn't in the relation!
- Projection tries to keep `?symbol` but it's not in the relation columns, causing the error

## Root Cause Analysis

### The Bug Location

**File:** `datalog/planner/phase_reordering.go`
**Function:** `updatePhaseSymbols`
**Section:** "3. For non-first phases, ensure we keep at least one join symbol"

### What Went Wrong

The logic ensures that non-first phases keep at least one symbol for joining with previous phases. If no symbols are already in `Keep`, it adds the first symbol from `Available`:

```go
// BEFORE (BROKEN):
// 3. For non-first phases, ensure we keep at least one join symbol
if i > 0 && len(phases[i].Available) > 0 {
    hasJoinSymbol := false
    for _, sym := range phases[i].Available {
        if keep[sym] {
            hasJoinSymbol = true
            break
        }
    }

    // If no join symbol is kept, keep the first available symbol
    if !hasJoinSymbol {
        keep[phases[i].Available[0]] = true  // ✗ BUG HERE!
    }
}
```

**The problem:** `Available` contains ALL symbols accessible to the phase, including:
- Input parameters (like `?symbol`)
- Symbols from previous phases
- Symbols produced by this phase's patterns

But `Keep` should ONLY contain symbols that are actually IN THE RELATION produced by the phase patterns (i.e., in `Provides`).

### Why This Happens

1. **Phase 1 receives:** Available = `[?symbol ?s]`
   - `?symbol` is an input parameter (from `:in` clause)
   - `?s` is produced by phase 0

2. **Phase 1 executes patterns:**
   - `[?p :price/symbol ?s]` - uses `?s`, produces `?p`
   - `[?p :price/time ?time]` - produces `?time`
   - `[?p :price/close ?close]` - produces `?close`
   - Result: Provides = `[?close ?p ?s ?time]` (no `?symbol`!)

3. **Keep calculation:**
   - Checks if any symbol is in `Keep` from previous logic (find clause, future phases, etc.)
   - Finds: `?time` and `?close` are in `Keep` (from find clause)
   - BUT: Neither of these is in `Available`, so `hasJoinSymbol = false`
   - Adds `Available[0]` = `?symbol` to `Keep`

4. **Result:** Keep tries to include `?symbol` which isn't in the relation!

### The Key Insight

**Input parameters are in `Available` but NOT in the relation.** They're used to filter or correlate data during pattern matching, but they don't appear as columns in the output relation.

**Example:**
```datalog
:in $ ?symbol
:where [?s :symbol/ticker ?symbol]  ← Uses ?symbol to filter
       [?p :price/symbol ?s]        ← Produces ?p, ?s
```

The relation produced has columns `[?s ?p ...]`, NOT `[?symbol ?s ?p ...]`.

## The Actual Fix

### The Solution: Check Both Available AND Provides

Modified section 3 to only keep symbols that exist in BOTH `Available` AND `Provides`:

```go
// AFTER (FIXED):
// 3. For non-first phases, ensure we keep at least one join symbol
if i > 0 && len(phases[i].Available) > 0 {
    hasJoinSymbol := false
    for _, sym := range phases[i].Available {
        if keep[sym] {
            hasJoinSymbol = true
            break
        }
    }

    // If no join symbol is kept, keep the first available symbol that's also in Provides
    // IMPORTANT: Don't keep input parameters that aren't in the relation!
    if !hasJoinSymbol {
        // Find first symbol that's in both Available and Provides
        providesSet := make(map[query.Symbol]bool)
        for _, sym := range phases[i].Provides {
            providesSet[sym] = true
        }

        for _, sym := range phases[i].Available {
            if providesSet[sym] {
                keep[sym] = true
                break
            }
        }
    }
}
```

**Why this works:** We now check that the join symbol actually exists in the relation (`Provides`) before adding it to `Keep`. This ensures we never try to project a column that doesn't exist.

### Verification: Before vs After

**Before fix:**
```
Phase 1:
  Available: [?symbol ?s]
  Provides: [?close ?p ?s ?time]
  Keep: [?time ?close ?symbol]  ✗ ?symbol not in Provides!

Error: cannot project: column ?symbol not found
```

**After fix:**
```
Phase 1:
  Available: [?symbol ?s]
  Provides: [?close ?p ?s ?time]
  Keep: [?time ?close ?s]  ✓ All symbols in Provides!

Success: All tests pass
```

## Test Results

The failing test now passes:
```
ok  	github.com/wbrown/janus-datalog/datalog/storage	6.342s
```

All other tests continue to pass, confirming the fix doesn't break existing functionality.

## Why This Bug Was Subtle

1. **Only affected specific query patterns:**
   - Multi-phase queries with input parameters
   - Where the input parameter is used in an early phase but not kept
   - Where later phases have no natural join symbols

2. **The logic seemed sound:** "Keep at least one join symbol" is correct in principle, but the implementation was wrong

3. **Conflation of Available and Provides:**
   - Available = "symbols I can USE during pattern matching"
   - Provides = "symbols that APPEAR IN the relation I produce"
   - These are NOT the same when input parameters are involved!

4. **Recent feature interaction:** This bug only manifested after adding:
   - Parameterized query support (`:in` clause)
   - Multi-phase query plans
   - Phase symbol tracking

## Architectural Lesson

**Input parameters are metadata, not data.**

In Datalog query execution:
- **Input parameters** (from `:in` clause) are used to FILTER or CORRELATE data
- They exist in the "environment" but NOT in the "relation"
- They're similar to SQL prepared statement parameters

**Correct mental model:**
```
Available = Environment symbols (inputs + previous phase outputs)
Provides = Relation columns (what THIS phase's patterns produce)
Keep ⊆ Provides (you can only keep what's actually in the relation!)
```

**The fix enforces this invariant:**
```go
Keep ⊆ Provides ∩ Available
```

This is analogous to SQL's `SELECT` clause - you can only select columns that exist in the result set, even if you used parameters in the `WHERE` clause.

## Related Issues

This bug was discovered while fixing the conditional aggregate rewriting bug. Both bugs share a common theme:

**Metadata propagation through multi-phase execution must respect relation boundaries.**

See also: [CONDITIONAL_AGGREGATE_REWRITING_BUG.md](./CONDITIONAL_AGGREGATE_REWRITING_BUG.md)
