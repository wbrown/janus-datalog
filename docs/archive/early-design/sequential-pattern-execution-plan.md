# Sequential Pattern Execution Plan

## Goal
Refactor the query executor to execute patterns sequentially within phases, propagating bindings from each pattern to the next. This will enable optimal index usage throughout query execution.

## Current Architecture

```
Phase Execution:
1. Match all patterns independently
2. Collect all relations
3. Join relations at the end
```

## Proposed Architecture

```
Phase Execution:
1. Start with bindings from previous phases
2. For each pattern:
   a. Match with current bindings
   b. Extract new bindings
   c. Merge bindings for next pattern
3. Return accumulated result
```

## Implementation Steps

### Step 1: Modify Pattern Execution Flow
- Change from parallel to sequential execution
- Maintain running bindings throughout the phase
- Each pattern builds on previous patterns' results

### Step 2: Implement Incremental Joining
- Instead of collecting all relations then joining
- Join each new relation with accumulated result immediately
- This naturally propagates bindings

### Step 3: Update Pattern Matcher Interface
- Ensure MatchWithBindings handles multiple bound variables
- Optimize for common cases (single entity binding, etc.)

### Step 4: Handle Edge Cases
- Empty results (short-circuit)
- Patterns with no shared variables
- Expression patterns that need multiple relations

## Expected Performance Improvements

### Example Query
```datalog
[:find ?t
 :where
 [?morning-bar :price/minute-of-day 570]  ; Returns 30 bars
 [?morning-bar :price/symbol ?s]          ; With binding: 30 lookups
 [?morning-bar :price/time ?t]]           ; With binding: 30 lookups
```

**Current**: 12,238 + 12,238 = 24,476 datom scans
**Expected**: 30 + 30 + 30 = 90 datom scans
**Improvement**: 272x fewer scans

## Code Changes Required

1. `executor.go` - `executePhase` method
2. `pattern_match.go` - Enhanced binding extraction
3. `relation.go` - Efficient incremental joining

## Testing Strategy

1. Verify correctness with existing tests
2. Add performance benchmarks
3. Test with gopher-street queries
4. Measure improvement on complex OHLC queries