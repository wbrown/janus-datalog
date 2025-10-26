# Subquery Parameter Bug Investigation

## Symptom

After adding `extractSubqueryParameters()` fix, subqueries return wrong results when `EnableDynamicReordering: true`:
- Without reordering: max values = 150, 200 ✓
- With reordering: max values = 15, 16 ✗ (DAY numbers instead of event values)

## The Change We Made

**Before:**
```go
subqueryBindings := make(map[query.Symbol]bool)
for _, input := range inputs {  // inputs = outer query arguments: [$ ?p ?day]
    subqueryBindings[input] = true
}
```

**After:**
```go
subqueryBindings := make(map[query.Symbol]bool)
for _, param := range p.extractSubqueryParameters(subq) {  // params = subquery's :in clause: [$ ?person ?d]
    subqueryBindings[param] = true
}
```

## The Query Structure

**Subquery declaration:**
```
[:find (max ?v)
 :in $ ?person ?d
 :where [?ev :event/person ?person]
        [?ev :event/time ?t]
        [(day ?t) ?pd]
        [(= ?pd ?d)]
        [?ev :event/value ?v]]
```

**Subquery invocation:**
```
$ ?p ?day
```

**Mapping:**
- Argument 0: `$` → Parameter 0: `$`
- Argument 1: `?p` → Parameter 1: `?person`
- Argument 2: `?day` → Parameter 2: `?d`

## Investigation Plan

### Step 1: Understand Planning vs Execution Split

**Question:** When we create `initialBindings` with parameter names, what does `PlanWithBindings` actually do with them?

**Check:**
- `planner.go:70-72` - `PlanWithBindings` creates `initialBindings` map
- `planner.go:93-119` - Converts to `inputSymbols` map
- Does this get used to track "what's available" or "what order things are in"?

### Step 2: Trace Execution Flow

**Question:** At execution time, how are argument values mapped to parameters?

**Check:**
- `subquery.go:69` - `createInputRelationsFromPattern(subq, outerValues)`
- `subquery.go:297-327` - Processes `subq.Inputs` (arguments) in ORDER
- `subquery.go:330-392` - `createInputRelationsFromValues` processes `subq.Query.In` (parameters) in ORDER
- **Key question:** Does it assume position-based mapping? Or name-based?

### Step 3: Find the Order Dependency

**Hypothesis:** The code assumes argument order matches parameter order

**Test:**
```go
// Subquery.Inputs (arguments):     [$ ?p ?day]
// Subquery.Query.In (parameters):  [$ ?person ?d]

// createInputRelationsFromPattern extracts values:
orderedValues[0] = $
orderedValues[1] = outerValues["?p"]    // Entity reference
orderedValues[2] = outerValues["?day"]  // Day number (15 or 16)

// createInputRelationsFromValues creates relations:
relation[0] = DatabaseInput (skipped)
relation[1] = ScalarInput with symbol "?person", value = entity
relation[2] = ScalarInput with symbol "?d", value = day number

// This should be CORRECT - value at position 1 goes to parameter at position 1
```

**But what if reordering changes something?**

### Step 4: Check What Reordering Does

**Question:** Does phase reordering change how `subq.Inputs` is set up?

**Check:**
- `planner.go:135-139` - Reordering happens AFTER `createPhases` which calls `assignSubqueriesToPhases`
- `phase_reordering.go:237-368` - `updatePhaseSymbols` recalculates Available/Keep/Find
- Does it also update `SubqueryPlan.Inputs`? **Probably NOT**

**Hypothesis:** After reordering, `SubqueryPlan.Inputs` still has original outer query symbols, but those symbols may have different VALUES in the reordered execution.

### Step 5: Identify The Actual Bug

**What we need to check:**

1. **Do `SubqueryPlan.Inputs` symbols match what's actually available after reordering?**
   - Before reordering: `Inputs = [$ ?p ?day]`, Available = `[?p ?day]` ✓
   - After reordering: `Inputs = [$ ?p ?day]`, but is `?p` or `?day` even in the relation anymore?

2. **Does `getUniqueInputCombinations` fail to find the symbols?**
   - Line 247: `indices[i] = ColumnIndex(rel, sym)`
   - If symbol not found, returns `nil` → empty input combinations

3. **Why would it return 15, 16 instead of the correct values?**
   - This suggests the subquery IS executing
   - But it's aggregating over the wrong variable
   - Maybe `?d` is getting bound to something other than the day number?

### Step 6: Add Debug Output

Add logging to:
1. `assignSubqueriesToPhases` - Log what `inputs` and `params` are
2. `createInputRelationsFromPattern` - Log `outerValues` map
3. `createInputRelationsFromValues` - Log what relations are created
4. Nested plan execution - Log what symbols are available

Compare with reordering enabled vs disabled.

### Step 7: Root Cause Hypothesis

**Most likely:** `initialBindings` using parameter names is correct, but somewhere else assumes the PLANNER and EXECUTOR use the same symbol names. When we changed to use parameter names for planning, execution still uses argument names, creating a mismatch.

**Alternative:** Phase reordering changes the Available symbols list, and the subquery's nested plan becomes invalid because it was planned with symbols that are no longer available in that order.

## Next Steps

1. Add debug output to trace exact symbol flow
2. Run test with and without reordering
3. Compare the actual values being passed to subquery
4. Identify where the mismatch occurs
5. Fix the actual mapping issue (not by changing tests or disabling reordering)
