# BufferedIterator: Architectural Problem and Solution

**Date**: October 14, 2025
**Status**: In Progress (Branch: `fix-buffered-iterator-architecture`)
**Severity**: Critical architectural flaw defeating streaming semantics

## Executive Summary

BufferedIterator was introduced as a "fix" for streaming iterator issues, but it was actually a **band-aid hiding a fundamental architectural problem**: the entire codebase assumes iterators can be consumed multiple times. This completely defeats the purpose of streaming, causing everything to materialize in memory "just in case" we need to iterate again.

**The core issue**: We built a streaming architecture but then added BufferedIterator to make it behave like materialized relations, getting the worst of both worlds - memory overhead of buffering everything, complexity of streaming, and none of the benefits.

## Why We're Solving This Now

### 1. Stage B Incompatibility

We're implementing Stage B of the Phase-as-Query architecture (see `PHASE_AS_QUERY_ARCHITECTURE.md`). The new QueryExecutor returns `[]Relation` (potentially multiple disjoint groups) and relies on true streaming semantics.

**The collision**:
- QueryExecutor expects single-use iterators (streaming)
- BufferedIterator forces materialization on first access
- Result: Everything materializes, defeating the architecture

### 2. Discovered During Testing

While implementing executor validation tests (`executor_validation_test.go`), we discovered:

```go
// This pattern appears everywhere:
result := executor.Execute(query)
sorted := result.Sorted()    // Iterates once, should materialize
table := result.Table()      // Tries to iterate AGAIN - panic or empty!
```

When we enabled `EnableTrueStreaming=true`, tests exploded with:
- Panics from double Iterator() calls
- Empty results from consumed iterators
- Hash joins failing mysteriously

### 3. The Band-Aid Made It Worse

Commit 713a721 (Oct 13, 2025) introduced BufferedIterator with this justification:

> "Implement BufferedIterator to solve single-consumption problem"

**The problem**: It didn't solve the single-consumption problem - it **hid** it by buffering everything! Now we have:
- Memory overhead (buffering all tuples)
- Complexity overhead (streaming architecture)
- Concurrency issues (shared buffer state)
- **Zero performance benefit** (everything materializes anyway)

### 4. Timing: Before Stage C

We're between Stage B (QueryExecutor) and Stage C (Planner rewrite). This is the **perfect time** to fix the foundation because:

1. **Stage B is simple**: Just QueryExecutor, easy to verify
2. **Stage C depends on streaming**: AST-based planning assumes efficient streaming
3. **Tests are catching it**: Validation tests expose the violations
4. **No users yet**: Internal refactoring, no external API impact

If we defer this, Stage C will inherit the broken assumptions and we'll have even more code to fix.

## The Problem In Detail

### What BufferedIterator Does

```go
type BufferedIterator struct {
    inner  Iterator
    buffer []Tuple    // Stores ALL tuples in memory
    index  int
}

func (b *BufferedIterator) Clone() Iterator {
    return &BufferedIterator{
        buffer: b.buffer,  // Shared buffer!
        index: 0,
    }
}
```

On **first access**, it consumes the entire iterator and stores all tuples. Then it allows "multiple iterations" by returning clones that read from the shared buffer.

### Why This Defeats Streaming

**True streaming**:
```
Storage → Iterator → Process tuple 1 → Process tuple 2 → ... → Done
Memory: O(1) - just current tuple
```

**BufferedIterator**:
```
Storage → Iterator → Buffer ALL tuples → Clone → Process from buffer
Memory: O(n) - entire relation in memory
```

We get **zero streaming benefit** but pay all the complexity cost.

### Where The Reuse Happens

We found iterator reuse in multiple places:

#### 1. Hash Join Peek Pattern (FIXED)
```go
// OLD CODE (join.go ~276-287)
peekIt := buildRel.Iterator()  // First call
if peekIt.Next() {
    tuple := peekIt.Tuple()
    // Check type...
}
peekIt.Close()

buildIt := buildRel.Iterator()  // Second call - PANIC!
```

**Fix**: Check type on first tuple during actual build iteration (single Iterator() call).

#### 2. Sorted() Discard Pattern (FIXED)
```go
// OLD CODE (relation.go ~819)
func (r *StreamingRelation) Sorted() []Tuple {
    _ = r.Iterator() // Call but discard result!
    if r.materialized != nil {
        return r.materialized.Sorted()
    }
    return []Tuple{} // Always returns empty for streaming!
}
```

**Fix**: Actually use Materialize() before sorting.

#### 3. Test Double Iteration (FIXED)
```go
// OLD CODE (executor_validation_test.go ~116-124)
legacyTuples := legacy.Sorted()  // First iteration
newTuples := new.Sorted()         // First iteration

if len(legacyTuples) != len(newTuples) {
    t.Logf("Legacy:\n%s", legacy.Table())  // Second iteration - PANIC!
    t.Logf("New:\n%s", new.Table())         // Second iteration - PANIC!
}
```

**Fix**: Use formatTuples() on already-materialized arrays.

#### 4. Size() Before Iterator() (STILL PRESENT)
```go
// Appears in many places (join.go ~252-257)
buildSize := buildRel.Size()  // Might consume iterator!
hashTable := NewTupleKeyMapWithCapacity(buildSize)
buildIt := buildRel.Iterator()  // Might panic!
```

This is "safe" now because Size() returns -1 for streaming (doesn't consume), but it's fragile.

#### 5. Multiple Joins On Same Relation (STILL PRESENT)
```go
// Somewhere in Relations.Collapse()
rel1.Join(rel2)  // Consumes rel1 iterator
// Later...
rel1.Join(rel3)  // Tries to consume AGAIN - PANIC!
```

This is why two-pattern tests still fail.

### The Design Flaw

The root cause is **interface design**:

```go
type Relation interface {
    Iterator() Iterator   // Implies "get an iterator" - sounds reusable
    Size() int           // Implies "count tuples" - might consume
    Sorted() []Tuple     // Implies "get sorted tuples" - must consume
    // ... many more methods that might need iteration
}
```

The interface makes relations **look** like collections (reusable, inspectable), but streaming relations are actually **single-use generators**.

## Why This Matters For Stage B/C

### Stage B: QueryExecutor

The QueryExecutor architecture (PHASE_AS_QUERY_ARCHITECTURE.md) assumes:

1. **Phases return []Relation** (disjoint groups)
2. **Groups are collapsed progressively** (join, filter, iterate once)
3. **Single-pass execution** (no re-iteration)

BufferedIterator violated assumption #3, causing:
- Every collapse materializes everything
- Every join buffers both sides
- Zero streaming benefit

### Stage C: AST-Based Planning

The proposed Stage C planner (optimize-first) assumes:

1. **Clauses are executed exactly once** in optimized order
2. **No re-scanning** to check sizes or types
3. **Streaming composition** of iterators

If we keep BufferedIterator, Stage C will:
- Materialize every intermediate result
- Explode memory on complex queries
- Defeat the entire architecture

## The Solution

### Immediate (This Branch)

1. **Remove BufferedIterator** ✅
2. **Add CountingIterator** - lightweight (just tracks count, no buffering) ✅
3. **Panic on reuse** - expose violations ✅
4. **Fix obvious violations** - peek pattern, Sorted(), tests ✅
5. **Find remaining violations** - joins, Size() usage (in progress)

### Short Term (Complete This Branch)

6. **Single-pass hash join** - consume each relation exactly once
7. **Audit Size() calls** - remove or make safe
8. **Audit IsEmpty() calls** - can consume iterator
9. **Relations.Collapse() single-use** - don't reuse relation objects
10. **All tests pass** with streaming enabled

### Long Term (Stage C)

11. **Interface redesign** - separate Streamable from Materialized
12. **Explicit materialization** - force `.Materialize()` where needed
13. **Remove Size() from interface** - pass hints via metadata
14. **Single-pass optimizers** - no peeking, no re-scanning

## Alternative Considered: Keep BufferedIterator

**Why not just keep it?**

1. **Memory**: Defeats streaming, materializes everything
2. **Complexity**: Two code paths (streaming vs materialized) with same behavior
3. **Bugs**: Shared buffer state causes concurrency issues
4. **Performance**: No benefit over explicit materialization
5. **Architecture**: Violates Stage B/C design principles

**When buffering IS appropriate**:
- Explicit: `result.Materialize()` when you KNOW you need multiple passes
- Targeted: Small results (aggregations, LIMIT queries)
- User-controlled: `.ToArray()` in user code

## Evidence Of The Problem

### Performance Claims vs Reality

Commit 713a721 claimed:
> "Performance improvements when enabled:
> - 1.5-2.5x faster query execution
> - 50-99% memory reduction for selective queries"

**Reality**: These benchmarks were comparing:
- Before: Early materialization
- After: Slightly later materialization (via BufferedIterator)

**Not measured**: True streaming without ANY buffering.

### The Smoking Gun

From `relation.go:618-634` (our changes):
```go
// OLD CODE
if r.options.EnableTrueStreaming {
    // Use buffered iterator to support multiple iterations
    if r.buffered != nil {
        return r.buffered.Clone()
    }

    // First access - wrap in buffered iterator
    r.buffered = NewBufferedIterator(r.iterator)
    return r.buffered.Clone()
}
```

**"True streaming" immediately buffers everything.** This is the definition of not streaming.

## Success Criteria

This issue is resolved when:

1. ✅ BufferedIterator removed from codebase
2. ✅ Iterator() panics on second call (with EnableTrueStreaming)
3. ✅ CountingIterator provides size tracking without buffering
4. ✅ All streaming tests pass with EnableTrueStreaming=true
5. ✅ No Iterator() called twice on same StreamingRelation
6. ✅ Memory efficient - O(current tuple) not O(all buffered tuples)

**All criteria met - issue resolved!**

## Current Status (Oct 14, 2025)

**✅ EnableTrueStreaming is NOW ENABLED by default and working!**

### What We Fixed

✅ Removed BufferedIterator completely
✅ Fixed hash join peek pattern (single Iterator() call)
✅ Fixed StreamingRelation.Sorted() to actually materialize
✅ Fixed validation tests to avoid double iteration
✅ Made IsEmpty() safe for streaming (returns false without consuming)
✅ CountingIterator tracks size without full materialization
✅ **Fixed ProjectFromPattern() - THE ROOT CAUSE**

### The Root Cause

After adding stack traces, we found the mystery Iterator() call:

**The Bug**: `StreamingRelation.ProjectFromPattern()` at line 823 was calling:
```go
result, _ := Project(r, neededSymbols)  // Global function - materializes!
```

Instead of calling the method:
```go
result, _ := r.Project(neededSymbols)  // Method - streams!
```

**Why This Mattered**: The pattern matcher calls `ProjectFromPattern()` after matching, which was materializing every relation before returning it. Then when joins tried to use those relations, the iterator was already consumed.

**The Fix**: One line change - use the method instead of the global function. The method creates a `NewProjectIterator` which wraps the original iterator without consuming it.

### How We Found It

Instead of giving up, we:
1. Added `runtime.Stack()` capture on first Iterator() call
2. Ran tests to get full call stack
3. Stack trace showed: `Project() → ProjectFromPattern() → BadgerMatcher.MatchWithConstraints()`
4. Examined ProjectFromPattern() and found the global function call
5. Changed to method call - problem solved!

### Result

EnableTrueStreaming=true now works perfectly:
- ✅ No iterator reuse panics
- ✅ All streaming tests pass
- ✅ True single-pass execution
- ✅ Memory efficient - O(current tuple) not O(all tuples)

## Timeline

- **Oct 13**: BufferedIterator introduced (commit 713a721)
- **Oct 14**: Problem discovered during Stage B validation testing
- **Oct 14**: This document written, branch created, initial fixes committed
- **Target**: Complete by Oct 15 (before starting Stage C)

## References

- `PHASE_AS_QUERY_ARCHITECTURE.md` - Stage B/C design
- `CRITICAL_PERFORMANCE_INSIGHT.md` - Iterator reuse historical issues
- Commit 713a721 - When BufferedIterator was introduced
- Commit decfc58 - Initial fixes (this branch)

## Conclusion

BufferedIterator was a well-intentioned but architecturally wrong solution. It papered over violations of streaming semantics rather than fixing them. We're solving this now because:

1. **Stage B requires true streaming** - no going back
2. **Tests are catching violations** - perfect timing
3. **Stage C depends on this** - foundational for AST-based planning
4. **It's actually broken** - not just theoretical, tests fail

The fix is mechanical but requires discipline: **consume iterators exactly once, materialize explicitly when needed**. This aligns with Go idioms (io.Reader is single-use) and enables the performance benefits streaming promises.
