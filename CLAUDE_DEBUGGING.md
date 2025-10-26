# Debugging Query Execution Issues - REQUIRED READING

**This file is PART OF CLAUDE.md and must be read before debugging any query issues.**

When query execution produces unexpected results, use this systematic approach.

---

## 1. Check Annotations First

Annotations reveal execution flow better than printf debugging:

```bash
# Run test and capture all annotations
go test -v ./tests -run YourTest 2>&1 | tee test.log

# Look for phase boundaries
grep "phase/" test.log

# Look for missing operation types
grep "pattern/" test.log    # Should see if patterns executed
grep "expression/" test.log  # Should see if expressions executed
grep "join/" test.log       # Should see if joins happened
```

**Key insight**: Missing annotations are bug symptoms. If a phase should evaluate expressions but you see no `expression/` events, the expressions aren't executing.

---

## 2. Examine Phase Structure

Test output shows phase composition:

```
Phase 2:
  Patterns: 3
  Expressions: 0
  Subqueries: 1
  Available: [?p ?name]
  Provides: [?e ?time ?v]
  Keep: [?name ?time ?v]
```

**Questions to ask**:
- Does any phase have all counts at zero?
- Does a phase with expressions show no `expression/` annotations?
- Does `Provides` match what the phase actually produces?
- Does `Keep` include symbols needed by later phases?
- Is there a gap in symbol flow? (Phase N provides `?x`, Phase N+2 needs `?x`, but Phase N+1 doesn't Keep it)

---

## 3. Check Data Flow Between Phases

Phases are a pipeline. Data must flow correctly:

```
Phase 1: 10 tuples, Provides [?x ?y]    Keep [?x]
Phase 2: 0 tuples  ← BUG! Where did the 10 tuples go?
```

**Common issues**:
- Empty join (no overlapping symbols) creates empty result
- Missing Keep symbols needed by later phases
- Expression-only phase receives empty relations (this bug!)
- Predicate filters out all tuples

**Use annotations to trace**:
```bash
# See tuple counts at phase boundaries
grep "phase/complete" test.log
# Output: phase/complete - map[phase:Phase 2 success:true tuple.count:0]
```

---

## 4. Add Targeted Debug Output

When annotations aren't enough, add debug output at critical junctions:

```go
// At phase boundaries
fmt.Printf("DEBUG Phase %d: patterns=%d, expressions=%d, " +
    "collapsed=%d tuples, available=%d tuples\n",
    phaseIndex, len(phase.Patterns), len(phase.Expressions),
    len(collapsed), len(availableRelations))

// Before/after key operations
fmt.Printf("DEBUG before expression eval: size=%d, cols=%v\n",
    group.Size(), group.Columns())
```

**Strategic locations**:
- Start of `executePhaseSequential()` - what does this phase receive?
- Before `applyExpressionsAndPredicates()` - what relations are passed?
- After each join - did the join reduce or amplify tuples?
- After predicates - how many tuples filtered out?

---

## 5. Test Assumptions About Phase Composition

Don't assume phases always have certain operations. Test edge cases:

```go
// This can create expression-only phases:
// - Conditional aggregate rewriting
// - Phase reordering
// - CSE optimization

// Always test:
if len(phase.Patterns) == 0 {
    // Do we handle expression-only phases correctly?
}
```

---

## 6. Verify Invariants

At each phase boundary, these MUST be true:

```go
// Input invariant
if phaseIndex > 0 {
    assert(previousResult != nil, "Phase receives nil from previous phase")
}

// Output invariant
assert(result != nil, "Phase returns nil result")
assert(result.Columns() matches phase.Provides or phase.Keep)

// Data flow invariant
if phase.Available includes ?x {
    assert(previousResult.Columns() includes ?x OR ?x is input parameter)
}
```

---

## Common Bug Patterns

### 1. Zero tuples from non-empty input
- Check: Failed join (no shared columns)
- Check: Predicate filtered everything
- Check: Expression-only phase got empty relations ← **This bug**

### 2. Missing symbols in later phases
- Check: Previous phase didn't Keep required symbols
- Check: Phase reordering broke symbol flow
- Check: Input parameters added to Keep but not in Provides

### 3. Cartesian product explosion
- Check: Disjoint relations joined without shared symbols
- Check: Missing predicates to connect patterns
- Check: Input parameters treated as unbound

### 4. Wrong aggregation results
- Check: Grouping variables vs input parameters
- Check: Pure vs grouped aggregation distinction
- Check: Conditional aggregate rewriting correctness

---

## Using Annotations for Root Cause Analysis

Annotations show execution flow, not just outcomes:

```bash
# Example: Why did aggregation return nil?

# WRONG approach: Only check outcome
grep "Result:" test.log  # Shows: Result: nil

# RIGHT approach: Trace the transformation
grep "aggregation/executed" test.log
# Shows: groupby_count:0 find_elements:[?person (max ?val)]
# → AHA! Grouped aggregation was changed to pure (0 groupby vars)
# → Root cause: Decorrelation added input params to find clause
```

**Annotation workflow**:
1. Identify unexpected outcome (wrong count, nil value, etc.)
2. Find annotation for that operation (`aggregation/executed`, `join/hash`, etc.)
3. Examine annotation data for unexpected values
4. Trace backwards: what created those values?
5. Use annotations from earlier phases to find transformation point

---

## Testing Strategy for Phase Execution

Every modification to phase execution should test:

```go
func TestPureExpressionPhase(t *testing.T) {
    // Phase with zero patterns, only expressions
}

func TestPurePatternPhase(t *testing.T) {
    // Phase with zero expressions, only patterns
}

func TestEmptyPhase(t *testing.T) {
    // Edge case: should probably error
}
```

**Why**: Optimizations and rewriting can create unexpected phase compositions. If your code assumes "phases always have patterns", it will break.

---

## Debugging Workflow Summary

**When a query fails:**

1. ✅ **Check annotations first** - grep for expected events
2. ✅ **Examine phase structure** - Look at counts and symbol flow
3. ✅ **Trace data flow** - Follow tuples through pipeline
4. ✅ **Add targeted debug** - At critical boundaries
5. ✅ **Test assumptions** - Edge cases and phase composition
6. ✅ **Verify invariants** - Input/output/flow guarantees

**Never:**
- ❌ Skip annotation analysis
- ❌ Assume phase structure
- ❌ Add random debug statements everywhere
- ❌ Change code without understanding root cause

**Always:**
- ✅ Use annotations to understand what's happening
- ✅ Test all phase compositions including edge cases
- ✅ Verify invariants at boundaries
- ✅ Root cause first, fix second