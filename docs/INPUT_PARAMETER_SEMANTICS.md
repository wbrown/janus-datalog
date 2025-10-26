# Input Parameter Semantics in Query Planning

**Status**: Reference Documentation
**Date**: October 13, 2025
**Related**: Multiple bug fixes consolidated understanding of input parameter behavior

## Overview

Input parameters (from `:in` clause) have specific semantics in query planning and execution that differ from both pattern-derived variables and relation columns. Understanding these semantics is critical for correct query planning.

## The Three-Level Type System

Janus Datalog maintains a clear separation between three different "levels" of symbols:

### 1. Input Parameters (Environment Level)
- **Declared**: `:in $ ?symbol ?month` clause
- **Scope**: Available throughout ALL phases of query execution
- **Purpose**: Filtering and correlation (like SQL prepared statement parameters)
- **Location**: In `Phase.Available` but NOT in `Phase.Provides`
- **Analogy**: Function parameters in scope throughout function body

### 2. Pattern Variables (Computation Level)
- **Declared**: Implicitly through pattern usage `[?e :attr ?v]`
- **Scope**: Available from the phase where they're first bound
- **Purpose**: Join keys and data retrieval
- **Location**: Both `Phase.Provides` (where produced) and subsequent `Phase.Available`
- **Analogy**: Local variables that persist through computation

### 3. Relation Columns (Data Level)
- **Actual data**: Columns in the physical relations produced by phases
- **Scope**: Only exist in specific phase outputs
- **Purpose**: Carry actual query results
- **Location**: `Phase.Provides` only (what the relation actually contains)
- **Analogy**: Struct fields in the result

## Key Invariants

### Invariant 1: Available ≠ Provides
```
Available = Environment symbols (inputs + previous outputs)
Provides = Relation columns (what THIS phase produces)
```

Input parameters are in `Available` (can be used) but NOT in `Provides` (not in result).

### Invariant 2: Keep ⊆ Provides
```
Keep ⊆ Provides ∩ Available
```

You can only keep symbols that are actually IN the relation (`Provides`), even if they're available in the environment (`Available`).

### Invariant 3: Input Parameters Are Global
```
∀ phase_i: inputSymbols ⊆ phase_i.Available
```

Input parameters are available in ALL phases, not just phase 0.

## Concrete Example

```datalog
[:find ?time ?close
 :in $ ?symbol ?month
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]
        [(str ?time) ?timeStr]
        [(str/starts-with? ?timeStr ?month)]]
```

### Phase Breakdown

**Phase 0:**
```
Available: [?symbol ?month]           ← Input parameters
Provides:  [?s ?symbol]               ← Pattern produced ?s, used ?symbol
Keep:      [?s]                        ← Only ?s (not ?symbol, not ?month!)
```

**Phase 1:**
```
Available: [?symbol ?month ?s]        ← Inputs + previous provides
Provides:  [?p ?s ?time ?timeStr]     ← Pattern and expression outputs
Keep:      [?time ?s]                  ← What's needed for find clause
```

**Key Points:**
1. `?symbol` and `?month` are in ALL phases' `Available`
2. `?symbol` appears in Phase 0's `Provides` ONLY because a pattern produces it as output
3. `?month` is NEVER in any `Provides` (used only for filtering in predicate)
4. Phase 0 cannot keep `?symbol` or `?month` in its relation (not in `Provides`)

## Common Pitfalls

### Pitfall 1: Treating Input Parameters as Relation Columns
```go
// WRONG: Try to keep input parameter
Keep: [?time ?close ?symbol]  // ?symbol not in Provides!
```

**Error**: "cannot project: column ?symbol not found in relation"

**Why**: Input parameters are environment symbols, not data columns.

**Fix**: Only keep symbols that are in `Provides`:
```go
// CORRECT: Only keep symbols that exist in relation
Keep: [?time ?close ?s]  // ?s is in Provides
```

See: [INPUT_PARAMETER_KEEP_BUG.md](bugs/resolved/INPUT_PARAMETER_KEEP_BUG.md)

### Pitfall 2: Scoring Input Parameters as Unbound
```go
// WRONG: Treat ?symbol as unbound variable
scorePattern([?s :symbol/ticker ?symbol], {})
// Score: +500 (unbound variable - very unselective!)
```

**Error**: Wrong phase ordering, Cartesian products

**Why**: Input parameters have specific values, should be scored like constants.

**Fix**: Pass available symbols including input parameters:
```go
// CORRECT: Treat ?symbol as bound (it has a value)
scorePattern([?s :symbol/ticker ?symbol], {?symbol: true})
// Score: -500 (bound value - highly selective!)
```

See: [BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT.md](bugs/resolved/BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT.md)

### Pitfall 3: Only Making Input Parameters Available in Phase 0
```go
// WRONG: Only check phase 0 for input parameters
if i == 0 {
    for _, sym := range phases[0].Available {
        available[sym] = true
    }
}
// For i > 0, input parameters are missing!
```

**Error**: "CRITICAL: predicates could not be assigned to any phase"

**Why**: Function predicates in later phases can't find input parameters.

**Fix**: Use each phase's `Available` (which includes inputs):
```go
// CORRECT: Input parameters available in all phases
for _, sym := range phases[i].Available {
    available[sym] = true
}
```

See: [BUG_STRING_PREDICATES_CANT_USE_PARAMETERS.md](bugs/resolved/BUG_STRING_PREDICATES_CANT_USE_PARAMETERS.md)

## Implementation Guidelines

### Phase Construction
```go
// Start with input symbols as available
availableSymbols := make(map[query.Symbol]bool)
for sym := range inputSymbols {
    availableSymbols[sym] = true
}

// Each phase gets current available symbols
phase := Phase{
    Available: getResolvedSymbols(availableSymbols),
}

// After phase execution, update available with what was provided
for sym := range phase.Provides {
    availableSymbols[sym] = true
}
```

### Phase Reordering
```go
// After reordering, recalculate Available for each phase
providedSoFar := make(map[query.Symbol]bool)
// Start with input symbols
for sym := range inputSymbols {
    providedSoFar[sym] = true
}

for i := range phases {
    // Available = inputs + union of previous provides
    phases[i].Available = getResolvedSymbols(providedSoFar)

    // Update provided for next phase
    for _, sym := range phases[i].Provides {
        providedSoFar[sym] = true
    }
}
```

### Selectivity Scoring
```go
// Pass available symbols (including input parameters) to scoring
func estimatePatternSelectivity(pattern *DataPattern, available map[Symbol]bool) int {
    return scorePattern(pattern, available)
}

// In scorePattern, treat bound variables as selective as constants
if available[varName] {
    score -= 500  // Bound value (including input param) is highly selective
}
```

### Predicate Assignment
```go
// Use phase's Available which includes input parameters
for i := range phases {
    available := make(map[Symbol]bool)
    for _, sym := range phases[i].Available {
        available[sym] = true
    }

    // Add symbols provided by this phase
    for _, sym := range phases[i].Provides {
        available[sym] = true
    }

    // Check if predicate can be evaluated
    if canEvaluatePredicate(pred, available) {
        phases[i].Predicates = append(phases[i].Predicates, pred)
    }
}
```

## Architectural Philosophy

### Input Parameters Are Metadata, Not Data

This is the core insight that resolves confusion:

- **Metadata**: Information ABOUT the query execution (input parameters)
- **Data**: Information IN the query result (relation columns)

Just as SQL prepared statements have parameters that don't appear in result columns:
```sql
-- ?symbol is a parameter, not a column
SELECT time, close
FROM prices
WHERE symbol = ?  -- ?symbol filters but doesn't appear in output
```

In Datalog:
```datalog
-- ?symbol is a parameter, not a variable in the result
[:find ?time ?close
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]]
```

### The Mental Model

Think of query execution as:
```
Environment (Available) = { input parameters, previous results }
    ↓
Execute phase patterns with environment
    ↓
Produce relation (Provides) = { pattern variables }
```

The environment is "air" that patterns breathe - it's there for filtering and correlation, but it's not the solid data being passed between phases.

## Testing Patterns

When testing input parameter behavior:

1. **Test structure, not just outcomes**: Verify `Available`/`Provides`/`Keep` are correct
2. **Test all phases**: Input parameters should work in phase 0, 1, 2, etc.
3. **Test realistic data sizes**: Small test data can mask selectivity bugs
4. **Use annotations**: Capture internal transformations to verify correctness

Example:
```go
// Verify phase structure
assert.Contains(t, phase.Available, "?symbol")   // Input param available
assert.NotContains(t, phase.Provides, "?symbol")  // But not in relation
assert.NotContains(t, phase.Keep, "?symbol")      // And not kept
```

## Related Documentation

- [INPUT_PARAMETER_KEEP_BUG.md](bugs/resolved/INPUT_PARAMETER_KEEP_BUG.md) - Keep ⊆ Provides invariant
- [BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT.md](bugs/resolved/BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT.md) - Selectivity scoring
- [BUG_STRING_PREDICATES_CANT_USE_PARAMETERS.md](bugs/resolved/BUG_STRING_PREDICATES_CANT_USE_PARAMETERS.md) - Global availability
- [ARCHITECTURE.md](../ARCHITECTURE.md) - Phase-based execution model
- [phase_reordering.go](../datalog/planner/phase_reordering.go#L237) - `updatePhaseSymbols()` implementation

## Historical Context

These semantics were clarified through fixing three related bugs in October 2025:

1. **INPUT_PARAMETER_KEEP_BUG** (Oct 12): Discovered that input parameters shouldn't be in Keep
2. **BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT** (Oct 13): Fixed selectivity scoring for input parameters
3. **BUG_STRING_PREDICATES_CANT_USE_PARAMETERS** (Oct 13): Ensured global availability across phases

Each bug revealed a different aspect of the three-level type system, leading to this consolidated understanding.
