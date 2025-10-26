# A Tale of Correctness Bugs: How Architecture Reveals What Testing Misses

**Branch**: `fix-buffered-iterator-architecture`
**Duration**: 29 commits
**Initial Goal**: Remove BufferedIterator to enable streaming
**Actual Result**: Found and fixed 10 bugs, including 3 critical data corruption issues

---

## The Setup: What We Thought We Were Doing

The task seemed straightforward: remove `BufferedIterator` from `StreamingRelation`. This class was introduced as a "fix" for iterator reuse issues, but it defeated the entire purpose of streaming by buffering everything in memory.

```go
// Before: BufferedIterator defeats streaming
type StreamingRelation struct {
    iterator Iterator
    buffered *BufferedIterator  // Buffers ALL tuples in memory!
}
```

Simple refactoring, right? Remove the buffering, make iterators truly single-use, enable real streaming.

**What we didn't know**: BufferedIterator wasn't just inefficient - it was a **band-aid hiding critical correctness bugs**.

---

## Act I: The Architecture Makes Demands

### Scene 1: Remove the Band-Aid

We removed BufferedIterator and added a simple check:

```go
func (r *StreamingRelation) Iterator() Iterator {
    if r.iteratorCalled && r.options.EnableTrueStreaming {
        panic("VIOLATION: Iterator called twice on StreamingRelation")
    }
    r.iteratorCalled = true
    return r.iterator
}
```

**Immediately, tests started failing.**

Not with wrong results. Not with subtle bugs. With **panics** screaming "VIOLATION: Iterator called twice!"

### Scene 2: The Violations Emerge

The panics pointed to specific violations:

**Violation 1: HashJoin was peeking**
```go
// join.go - BEFORE
func HashJoin(left, right Relation) Relation {
    // Peek at first tuple to check type
    it := left.Iterator()  // First call
    if it.Next() {
        // Check tuple...
    }

    // Build hash table
    it2 := left.Iterator()  // Second call - PANIC!
```

**Violation 2: Sorted() was discarding**
```go
// relation.go - BEFORE
func (r *StreamingRelation) Sorted() []Tuple {
    it := r.Iterator()
    // ... but never actually iterated or materialized!
    return []Tuple{}  // Lost all data
}
```

**Violation 3: IsEmpty() was consuming**
```go
// relation.go - BEFORE
func (r *StreamingRelation) IsEmpty() bool {
    return !r.counter.Next()  // Peeks at first tuple, consumes it!
}
```

**The Key Insight**: These looked like "iterator reuse bugs" but they were actually **logic errors**. BufferedIterator made them work by accident.

---

## Act II: The First Blood

### Scene 3: Fix the Violations

We fixed the obvious violations:
- HashJoin: Don't peek, check type during actual iteration
- Sorted(): Actually materialize the relation
- IsEmpty(): Return false without consuming

Tests stopped panicking. Everything seemed fine.

**Then we ran the integration tests.**

### Scene 4: The Mystery

```
TestDebugBasicQuery: Expected 1 result, got 0
```

A simple query: join two patterns on the same entity variable. Both patterns matched individually (1 result each). The join should return 1 result.

It returned **zero**.

We added debug logging:

```
Pattern 1: [?person :person/name ?name] → 1 result
Pattern 2: [?person :person/age ?age]   → 1 result
Join: ?person                            → 0 results

Available for join: 1 binding
Hash table built: 1 entry
Join executed: 0 results
```

The join logic was executing. The data existed. But **no results came out**.

### Scene 5: The Identity Crisis

After investigation, we found the issue: **Identity equality was broken**.

```go
// ValuesEqual - BEFORE
func ValuesEqual(a, b interface{}) bool {
    // Try direct equality first
    if a == b {  // Compares ALL struct fields
        return true
    }

    // Then check special types
    if aIdent, ok := a.(Identity); ok {
        if bIdent, ok := b.(Identity); ok {
            return bytes.Equal(aIdent.Hash(), bIdent.Hash())
        }
    }
    return false
}
```

**The bug**: When Identities came from storage, they had:
- `hash`: [20 bytes] - correct
- `l85`: "" - empty
- `str`: "" - empty

The pattern results had:
- `hash`: [20 bytes] - same hash!
- `l85`: "base85encoded" - populated
- `str`: "person1" - populated

The `==` check compared **structs with different fields** even though the **hashes were identical**. Joins failed because the same entity looked different!

**The fix**: Check hash FIRST, before struct equality:

```go
// ValuesEqual - AFTER
func ValuesEqual(a, b interface{}) bool {
    // Special handling for Identity (check hash, not struct fields)
    if aIdent, ok := a.(Identity); ok {
        if bIdent, ok := b.(Identity); ok {
            return bytes.Equal(aIdent.Hash(), bIdent.Hash())
        }
    }

    // Then try direct equality
    return a == b
}
```

**Tests passed!**

Or so we thought...

---

## Act III: The Silent Killer

### Scene 6: Five Should Be Five

We were running the comprehensive validation suite - tests that compare legacy executor vs new executor on the same queries.

```
TestEntityJoinBug: Expected 5 results
```

A simple query: find all entities that have both `:price/high` and `:price/low` attributes.

We created 5 entities. Each had both attributes.

The query returned **4 results**.

**Consistently. The same 4 results. The same missing entity: bar:0.**

### Scene 7: The Investigation

We added extensive logging:

```
Created entity: bar:0 (hash: 4GCe+%zbX2@b5`BBECl`;!.Zk)
Created entity: bar:1 (hash: ...)
Created entity: bar:2 (hash: ...)
Created entity: bar:3 (hash: ...)
Created entity: bar:4 (hash: ...)

Pattern [?bar :price/high ?h] → 5 results
Pattern [?bar :price/low ?l]  → 5 results
Join on ?bar                   → 4 results (bar:0 MISSING)
```

Both patterns found all 5 entities. But the join **lost the first one**.

We traced the execution:

```
Match [?bar :price/high ?h]:
  Storage scan: 5 tuples
  Return: StreamingRelation

Match [?bar :price/low ?l]:
  Storage scan: 5 tuples
  Return: StreamingRelation

Join:
  Check if bindingRel.IsEmpty() → Calls Next() to peek
  ^^^^ THIS IS THE BUG
  Size() called on bindingRel    → Triggers materialization
  Materialization iterates counter... which already consumed first tuple!
  Materializes 4 tuples (tuples 2-5)
  Join finds 4 matches
```

### Scene 8: The Smoking Gun

The bug was in `IsEmpty()`:

```go
func (r *StreamingRelation) IsEmpty() bool {
    // Peek at the counter to see if there are any tuples
    return !r.counter.Next()  // <-- CONSUMES FIRST TUPLE
}
```

Called from:
```go
// matcher_strategy.go:52
if !bindingRel.IsEmpty() {
    // Use bindings for filtering
}
```

**What happened**:
1. `IsEmpty()` calls `counter.Next()` to peek
2. Counter consumes **first tuple from storage iterator**
3. Later, `Size()` is called (for join size comparison)
4. `Size()` triggers materialization
5. Materialization iterates the counter... which already advanced
6. **Only tuples 2-5 are materialized. Tuple 1 is lost forever.**

**Impact**: CRITICAL - Silent data loss. No error. No warning. Just wrong results.

**The fix**:
```go
// matcher_strategy.go - AFTER
// CRITICAL: Skip IsEmpty() on StreamingRelations
// IsEmpty() consumes the first tuple via Next() peek
// Only safe on MaterializedRelations
if _, ok := bindingRel.(*MaterializedRelation); ok {
    if bindingRel.IsEmpty() {
        // Empty binding - no results possible
    }
}
```

---

## Act IV: The Crash

### Scene 9: Zero is Not Nil

The validation suite had edge case tests:

```
TestComprehensiveExecutorValidation/pattern_with_no_results
TestComprehensiveExecutorValidation/filter_eliminates_all_results
```

Both **panicked**:

```
panic: close of closed channel
at: relation.go:236 (CachingIterator.signalComplete)
```

### Scene 10: The Empty Stream Problem

We traced the bug:

**When stream has zero tuples:**

1. `Materialize()` sets `shouldCache = true`, `cache = nil`
2. First `Iterator()` call creates `CachingIterator`
3. CachingIterator loops: `for it.Next()` → zero iterations
4. Loop completes, calls `signalComplete()`
5. `signalComplete()` closes the `cacheComplete` channel
6. **Cache is still nil** (no tuples were appended)
7. Second `Iterator()` call checks: `cache != nil` → **false**
8. Creates **second CachingIterator** on same relation
9. Second CachingIterator completes, tries to close `cacheComplete`
10. **PANIC: close of closed channel**

**The bug**: We checked `cache != nil` to determine if caching completed, but empty streams have `cache == nil` after completion!

**The fix**: Add explicit `cacheReady` flag:

```go
type StreamingRelation struct {
    cache       []Tuple
    cacheReady  bool    // NEW: Explicit completion flag
    // ...
}

func (ci *CachingIterator) signalComplete() {
    *ci.cacheReady = true  // Set flag before closing channel
    close(ci.cacheComplete)
}

func (r *StreamingRelation) Iterator() Iterator {
    if r.cacheReady {  // Check flag, not cache != nil
        return &sliceIterator{tuples: r.cache}
    }
    // ...
}
```

---

## Act V: The Phase Reordering Catastrophe

### Scene 11: The Final Two

After all the fixes, we had **two validation tests still failing**:

```
TestComprehensiveExecutorValidation/multi-phase_with_expression
TestComprehensiveExecutorValidation/all_features_combined
```

The errors:
```
Legacy error: projection failed: cannot project: column ?total not found
New error: <nil>
```

One executor worked, one didn't. But the query was **valid** - it should work in both.

### Scene 12: The Query

```clojure
[:find ?name ?total
 :where [?event :event/person ?person]
        [?person :person/name ?name]
        [?person :person/score ?score]
        [?event :event/value ?value]
        [(+ ?score ?value) ?total]]
```

**What should happen**:
- Phase 0: Get ?person, ?name, ?score from person attributes
- Phase 1: Get ?event, ?value from event attributes, join on ?person
- Phase 1: Evaluate `[(+ ?score ?value) ?total]` (all inputs available)
- Return ?name and ?total

**What actually happened**:
- Phase 0: Get ?person, ?name, ?score
- Phase 0: Try to evaluate `[(+ ?score ?value) ?total]` - **?value missing!**
- Expression skipped (inputs unavailable)
- Phase 0: Try to project to Keep=[?name ?total] - **?total missing!**
- **Error: cannot project ?total**

### Scene 13: The Debug Output

We added debug logging to the planner:

```
Creating phases (initial order):
  Phase 0: [?person :person/name], [?person :person/score]
    Provides: [?person ?name ?score]
  Phase 1: [?event :event/person], [?event :event/value]
    Provides: [?event ?person ?value]

Assigning expressions:
  Expression [(+ ?score ?value) ?total]
    Phase 0: Available=[?person ?name ?score]
    Phase 1: Available=[?name ?person ?score ?event ?value]
    ASSIGNED to Phase 1 ✓

Phase reordering:
  Reordering for better connectivity...
  Phase 0: [?person :person/name], [?person :person/score] (was Phase 0)
  Phase 1: [?event :event/person], [?event :event/value]    (was Phase 1)
  (No change in this case)

After reordering - checking phases:
  Phase 0: Provides=[?person ?name ?score ?total]  ← WAIT, HOW?
  Expression: [(+ ?score ?value) ?total]           ← IN PHASE 0!
```

**The bug**: Expression was assigned to Phase 1 correctly, but then **phases were reordered** and the expression **stayed in its original position**, ending up in the wrong phase!

### Scene 14: The Root Cause

The planner workflow:

1. `createPhases()` - Creates phases from patterns
2. `assignExpressionsToPhases()` - Assigns expressions to phases ✓
3. **`reorderPhasesByRelations()` - SHUFFLES PHASES** ❌
4. Expression still in original phase number, but patterns moved!

**Example**:
```
After assignment:
  Phase 0: Patterns A, B
  Phase 1: Patterns C, D, Expression X

After reordering:
  Phase 0: Patterns C, D, Expression X  ← Expression stayed with phase number!
  Phase 1: Patterns A, B

But Expression X needs symbols from Patterns A, B!
```

**The fix**: Re-assign expressions AFTER reordering:

```go
if p.options.EnableDynamicReordering {
    phases = p.reorderPhasesByRelations(phases, inputSymbols)

    // Update Available fields first
    phases = updatePhaseSymbols(phases, q.Find, inputSymbols)

    // Re-assign expressions with correct Available lists
    p.assignExpressionsToPhases(phases, expressions, predicates)

    // Update symbols again to include expression outputs
    phases = updatePhaseSymbols(phases, q.Find, inputSymbols)
}
```

And clear stale data:
```go
func (p *Planner) assignExpressionsToPhases(...) {
    // Clear existing assignments
    for i := range phases {
        phases[i].Expressions = nil

        // Remove expression outputs from Provides
        var newProvides []query.Symbol
        for _, sym := range phases[i].Provides {
            if !isExpressionOutput(sym) {
                newProvides = append(newProvides, sym)
            }
        }
        phases[i].Provides = newProvides
    }
    // ... then assign
}
```

---

## The Denouement: All Tests Pass

After 29 commits, all tests passed:

```
✅ 25/25 validation tests passing
✅ Both legacy and new executors produce identical results
✅ No data loss, no crashes, no corruption
✅ True streaming architecture working
```

---

## The Lessons: A Methodology for Finding Correctness Bugs

### Lesson 1: Band-Aids Hide Bugs

**What we learned**: Defensive code that "makes it work" often masks the real problem.

**The Pattern**:
```
Problem → Quick Fix → Works!
              ↓
        (Real bug still there, hidden)
```

**Examples from this branch**:
- BufferedIterator hid iterator reuse violations
- IsEmpty() "worked" but consumed tuples
- ValuesEqual() "worked" but compared wrong fields

**The Principle**: When you find yourself adding defensive code that "works around" a problem, **dig deeper**. The workaround is hiding a bug.

**How to apply**:
1. Identify defensive code (try/catch, null checks, retries)
2. Ask "What problem is this solving?"
3. Remove it temporarily and see what breaks
4. Fix the underlying issue, not the symptom

---

### Lesson 2: Make Assumptions Explicit and Enforced

**What we learned**: Implicit assumptions fail silently. Explicit assumptions fail loudly.

**The Pattern**:
```
Assumption: "Iterators are single-use"
Reality: Code called Iterator() twice
Result: Silent data corruption (lost first tuple)

After Making Explicit:
Code called Iterator() twice → PANIC immediately
```

**The Principle**: If your architecture assumes X, make violations of X **crash the program**.

**How to apply**:
```go
// Before: Implicit assumption
type StreamingRelation struct {
    iterator Iterator
}

// After: Explicit enforcement
type StreamingRelation struct {
    iterator Iterator
    iteratorCalled bool  // Track assumption
}

func (r *StreamingRelation) Iterator() Iterator {
    if r.iteratorCalled && r.options.EnableTrueStreaming {
        panic("VIOLATION: Iterator called twice")
    }
    r.iteratorCalled = true
    return r.iterator
}
```

---

### Lesson 3: Differential Testing Finds What Unit Tests Miss

**What we learned**: Comparing two independent implementations reveals bugs in both.

**The Pattern**:
```
Implementation A: Bug in evaluation order
Implementation B: Bug in phase assignment
Unit tests: Both pass (wrong for different reasons)
Differential test: FAIL (results don't match)
```

**The Principle**: If two implementations disagree on valid input, **at least one is wrong**.

**How to apply**:
1. Write multiple implementations (legacy vs new, fast vs safe)
2. Run identical inputs through both
3. Assert results match exactly
4. Any discrepancy proves a bug

**From this branch**:
```go
func TestComprehensiveExecutorValidation(t *testing.T) {
    // Execute query with BOTH executors
    legacyResult := legacyExec.Execute(query)
    newResult := newExec.Execute(query)

    // Results MUST match
    if !equalResults(legacyResult, newResult) {
        t.Errorf("Executors disagree - bug in at least one")
    }
}
```

Found:
- Legacy executor: Wrong evaluation order
- New executor: Phase assignment after reordering
- **Both wrong in different ways**

---

### Lesson 4: Edge Cases Are Production Cases

**What we learned**: "Edge cases" expose state bugs that hide in normal cases.

**The Pattern**:
```
Normal case (n > 0): Works fine
Edge case (n = 0): Crashes

Why? Check assumed "cache != nil" meant "cache ready"
But empty results leave cache == nil after completion
```

**The Principle**: Empty, single, and huge cases are **not edge cases** - they're production data.

**How to apply**:

Test matrix:
```
Test Case            | What It Reveals
---------------------|------------------
Empty (0 results)    | State initialization bugs
Single (1 result)    | Off-by-one errors
Large (10000 results)| Performance and memory bugs
Duplicate values     | Deduplication bugs
Null/nil values      | Null handling bugs
```

**From this branch**:
- Empty results: Found cacheReady bug (nil != ready)
- Zero patterns: Found expression-only phase bugs
- Zero inputs: Found input parameter projection bugs

---

### Lesson 5: Peek Operations Are Destructive

**What we learned**: Any operation that "just checks" a stream is **consuming data**.

**The Pattern**:
```go
// Looks innocent
if !stream.IsEmpty() {
    // ... use stream
}

// Actually destructive
func IsEmpty() bool {
    return !stream.Next()  // Consumed first element!
}
```

**The Principle**: On single-use iterators, **all operations are destructive**.

**How to apply**:

**Don't:**
```go
if !rel.IsEmpty() {        // Consumes first tuple
    for it.Next() {        // Iterates remaining tuples
        // Missing first!
    }
}

if rel.Size() > 0 {        // Materializes entire stream
    for it.Next() {        // Already consumed!
        // Empty!
    }
}
```

**Do:**
```go
// Just iterate - empty loop is fine
for it.Next() {
    // Processes all tuples including first
}

// Or check type, not instance
if _, ok := rel.(*MaterializedRelation); ok {
    // Safe to call Size(), IsEmpty() on materialized
}
```

**From this branch**:
- IsEmpty() consumed first tuple → lost data
- Size() materialized stream → iterator exhausted
- Both caused silent corruption

---

### Lesson 6: Nil Is Not Empty

**What we learned**: Uninitialized state (nil) looks like empty state (0 elements) but **means something different**.

**The Pattern**:
```go
var cache []Tuple  // nil

// Process zero elements
for range zeroElements {
    cache = append(cache, ...)  // Loop never executes
}

// cache is still nil!
if cache != nil {  // False - cache was never initialized
    // Won't execute even though processing "completed"
}
```

**The Principle**: Distinguish "not started" from "completed with zero results".

**How to apply**:

**Don't:**
```go
type State struct {
    cache []Tuple  // nil = not started OR empty
}

func (s *State) Ready() bool {
    return s.cache != nil  // Ambiguous!
}
```

**Do:**
```go
type State struct {
    cache []Tuple
    cacheReady bool  // Explicit "processing complete" flag
}

func (s *State) Ready() bool {
    return s.cacheReady  // Unambiguous
}
```

**From this branch**:
- Empty stream completed with `cache == nil`
- Check `cache != nil` failed
- Created second iterator → panic
- **Fix**: `cacheReady` flag separate from `cache` value

---

### Lesson 7: Metadata Goes Stale

**What we learned**: Any derived information becomes **invalid** when source changes.

**The Pattern**:
```
1. Create phases from patterns
2. Assign expressions based on phase symbols  ✓
3. Reorder phases                             ← SOURCE CHANGED
4. Execute with stale expression assignments  ✗
```

**The Principle**: When you transform data, **recalculate all derived metadata**.

**How to apply**:

Identify dependencies:
```
Source Data: Phase order, pattern assignments
Derived: Expression assignments, Available symbols, Keep lists

When source changes: Recalculate ALL derived data
```

**From this branch**:
```go
// BEFORE: Stale metadata
phases = createPhases(...)              // 1. Create
assignExpressionsToPhases(phases, ...)  // 2. Derive
phases = reorderPhases(phases)          // 3. CHANGE SOURCE
// Expressions still in old phase positions! ✗

// AFTER: Recalculate
phases = createPhases(...)              // 1. Create
assignExpressionsToPhases(phases, ...)  // 2. Derive
phases = reorderPhases(phases)          // 3. Change source
updatePhaseSymbols(phases, ...)         // 4. Recalculate
assignExpressionsToPhases(phases, ...)  // 5. Re-derive ✓
```

**Generalize**:
- After sorting: Recalculate indices
- After filtering: Recalculate counts
- After reordering: Recalculate dependencies
- After optimizing: Recalculate metadata

---

## The Meta-Lesson: Architecture Pressure Reveals Bugs

The profound insight from this branch:

**We weren't looking for these bugs. The architecture found them for us.**

### How Architecture Finds Bugs

**1. Architecture makes demands:**
```
Streaming architecture demands: Single-use iterators
Code assumes: Reusable iterators
Conflict → Violations exposed
```

**2. Architecture constrains behavior:**
```
Before: BufferedIterator allowed any access pattern
After: StreamingRelation requires specific access pattern
Violations that "worked" now crash
```

**3. Architecture reveals assumptions:**
```
Assumption: "Iterators are cheap to consume"
Reality: Consuming loses data
Architecture pressure: Forces confrontation with reality
```

### The Process

```
1. Set architectural goal (true streaming)
   ↓
2. Remove accommodations (BufferedIterator)
   ↓
3. Make violations crash (panic on reuse)
   ↓
4. Fix violations one by one
   ↓
5. Find bugs hiding beneath violations
   ↓
6. Fix bugs, validate correctness
```

### Why This Works

**Traditional testing asks**: "Does this code work?"
**Architecture pressure asks**: "Can this code work in principle?"

Traditional testing found: *(nothing - tests passed)*
Architecture pressure found:
- Silent data loss (entity join)
- State corruption (empty stream panic)
- Wrong results (phase assignment)

**Because architecture doesn't ask "does it work now" - it asks "is it correct by design".**

---

## Actionable Principles

### When Writing Code

**1. Make assumptions explicit**
```go
// Bad: Implicit assumption
func process(data) {
    // Assumes data is non-empty
}

// Good: Explicit check
func process(data) {
    if len(data) == 0 {
        panic("VIOLATION: Empty data not supported")
    }
}
```

**2. Fail loudly, not silently**
```go
// Bad: Silent failure
if err != nil {
    return defaultValue  // Hides the error
}

// Good: Loud failure
if err != nil {
    panic(fmt.Sprintf("Invariant violated: %v", err))
}
```

**3. Distinguish nil from empty**
```go
// Bad: Ambiguous
var result []Item  // nil = not ready OR empty?

// Good: Explicit
type Result struct {
    items []Item
    ready bool
}
```

### When Refactoring

**1. Remove accommodations first**
```
Don't: Keep workarounds while changing architecture
Do: Remove workarounds, see what breaks, fix properly
```

**2. Make the architecture demand correctness**
```
Don't: Add more defensive code
Do: Make violations impossible or crashworthy
```

**3. Follow the failure chain**
```
Don't: Stop at first fix
Do: Ask "Why was this necessary?" after each fix
```

### When Testing

**1. Test multiple implementations**
```go
func TestDifferential(t *testing.T) {
    for _, input := range testCases {
        result1 := impl1.Execute(input)
        result2 := impl2.Execute(input)
        assert.Equal(result1, result2)  // Must match!
    }
}
```

**2. Test edge cases specifically**
```go
func TestEdgeCases(t *testing.T) {
    test(emptyInput)      // 0 elements
    test(singleInput)     // 1 element
    test(massiveInput)    // 10000 elements
    test(duplicateInput)  // Repeated values
}
```

**3. Test state transitions**
```go
func TestStateTransitions(t *testing.T) {
    test(beforeStart)     // Not initialized
    test(afterZero)       // Processed zero items
    test(afterN)          // Processed N items
    test(afterComplete)   // Finished
}
```

---

## Conclusion: Trust the Architecture

This branch started with an architectural goal (streaming) and ended with 10 bug fixes, including 3 critical correctness bugs that were **silently corrupting data**.

We didn't find these bugs by:
- Careful code review
- More unit tests
- Better assertions
- Defensive programming

We found them by:
1. **Removing safety nets** (BufferedIterator)
2. **Making violations crash** (panic on reuse)
3. **Comparing implementations** (differential testing)
4. **Testing edge cases** (empty results)
5. **Following failures** (each fix revealed deeper bugs)

**The bugs weren't in "bad" code - they were in code that looked obviously correct.**

The architecture found them by **making implicit assumptions explicit** and **making violations impossible to ignore**.

---

## The Final Lesson

**Good architecture doesn't just organize code - it reveals bugs.**

When architecture and code conflict, the conflict is the bug detector. Don't make the architecture accommodate the code. Make the code meet the architecture's demands.

The bugs we found weren't edge cases. They were:
- **Silent data loss** in production queries (entity join)
- **Application crashes** on valid inputs (empty results)
- **Wrong results** with no error (phase assignment)

All hidden by "code that worked" until **architecture made demands code couldn't meet**.

**Trust the architecture. It knows what code should do, even when code doesn't.**

---

*Written after 29 commits, 10 bugs fixed, and one profound appreciation for how architecture reveals what testing misses.*
