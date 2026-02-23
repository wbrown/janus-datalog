# Slow Single-Pattern Query with Input Parameter

**Date**: 2025-12-21
**Status**: RESOLVED
**Severity**: High (was 14-16ms for 63-95 results, now ~100µs)

## Resolution

**Root Cause**: Missing AVET case in `chooseIndexForValues()` in `hash_join_matcher.go`.

The `hashJoinIterator` was designed to calculate a precise scan range using the bound values from the input relation. However, the `chooseIndexForValues()` function only handled EAVT (case 0) and AEVT (case 1) indices. When AVET (case 2) was selected, no case matched, so the scan range was just the index byte prefix, causing a **full index scan of 44,548 datoms** instead of the expected 95 matching datoms.

**Fix**: Added proper handling for AVET and VAET indices with correct value encoding:
```go
case AVET: // 2
    if a != nil {
        if kw, ok := a.(datalog.Keyword); ok {
            attr := NewAttribute(kw.String())
            startParts = append(startParts, attr[:])
            endParts = append(endParts, attr[:])

            if v != nil {
                // Values in AVET keys are encoded as: [type byte][value data]
                if entity, ok := v.(datalog.Identity); ok {
                    hash := entity.Hash()
                    vBytes := append([]byte{byte(datalog.TypeReference)}, hash[:]...)
                    startParts = append(startParts, vBytes)
                    endParts = append(endParts, vBytes)
                }
            }
        }
    }
```

**Performance Improvement**:
- **Before**: 14-16ms for 95 results (scanning 44,548 datoms)
- **After**: ~100µs for 95 results (scanning 95 datoms)
- **Speedup**: ~150-160x

## Problem Statement

A simple single-pattern query with an input parameter takes ~14ms:

```datalog
[:find ?e :in $ ?scenario :where [?e :task/scenario ?scenario]]
```

With `?scenario` bound from input, this should be a direct AVET index lookup returning 63 entities in <1ms.

## Analyze Output

```
Query Plan:
  Find: [?e]
  Phases: 1

Phase 1:
  Available: [?scenario]
  Patterns:
    [?e :task/scenario ?scenario] [AVET index, selectivity=490]
      Bound: E=false A=true V=true T=false
      Binds: map[?e:true ?scenario:true]
  Provides: [?scenario ?e]
  Keep: [?e]

Execution:
  Total time: 14.962209ms
  Result tuples: 63

Event Summary:
  Joins: hash=1, nested=0, merge=0
    Hash join time: 14.81ms

Event Trace:
  [  0.00ms] query/invoked
  [  0.00ms] query/plan.created
  [  0.00ms] realized/phase-begin
  [  0.00ms] query/invoked
  [  0.00ms] storage/reuse-strategy (index=AVET)
  [  0.00ms] storage/join-strategy (index=AVET)
  [  0.01ms] matches->relations          <- Storage scan is FAST
  [  0.00ms] pattern/hash-join-complete  <- Storage hash join scan is FAST
  [ 14.81ms] join/hash                   <- Executor collapse is SLOW
  [ 14.81ms] collapse/success
  [ 14.84ms] query/completed
  [  0.00ms] realized/phase-output
```

## Key Finding

The **storage layer is fast** (0.01ms for `matches->relations`). The time is spent in the **executor's relation collapse** (`join/hash` at 14.81ms).

This is the hash join happening in `executor/relations.go:166` during `Collapse()`:
```go
currentGroup = ctx.JoinRelations(currentGroup, remaining[i], func() Relation {
    return currentGroup.Join(remaining[i])
})
```

## What's Being Joined

For a single-pattern query with input:
1. **Input relation**: 1 tuple containing `?scenario`
2. **Storage result**: StreamingRelation yielding 63 tuples with `?e` and `?scenario`

A 1×63 hash join should be sub-millisecond, not 14.81ms.

## Code Path Analysis

1. **Pattern matching** (`matcher_relations.go:80-140`)
   - `analyzeReuseStrategy()` returns `SinglePositionReuse` for position 2 (V)
   - `chooseJoinStrategy()` returns `HashJoinScan` (binding size 1 < 1000)
   - `matchWithHashJoin()` creates a streaming hash join iterator

2. **Storage hash join** (`hash_join_matcher.go:121-212`)
   - Builds hash set from binding relation (1 entry for `?scenario`)
   - Calculates scan range WITH bound value (AVET prefix for attribute + value)
   - Creates `ScanKeysOnly` iterator
   - Returns `StreamingRelation` wrapping `hashJoinIterator`

3. **Executor collapse** (`relations.go:166`)
   - Joins input relation with storage streaming relation
   - Calls `HashJoin()` in `executor/join.go:126`

4. **Executor HashJoin** (`join.go:136-500+`)
   - Determines build/probe based on streaming status
   - Input is materialized (1 tuple), storage is streaming
   - Uses input as build (small), storage as probe
   - **This is where 14.81ms is spent**

## Key Insight: Deferred Materialization

The `join/hash` 14.81ms timing is **misleading**. It's not join computation time - it's when the **streaming relation from storage is finally materialized**.

The storage layer returns a `StreamingRelation` wrapping a `hashJoinIterator`. This iterator doesn't touch BadgerDB until `Next()` is called. When the executor's `HashJoin` probes by iterating through this streaming relation, THAT is when the actual BadgerDB scan happens.

So the 14.81ms is really:
1. Build hash table from input relation (1 tuple) - **trivial**
2. Probe by iterating storage streaming relation - **forces BadgerDB iteration**
3. BadgerDB AVET prefix scan for 63 datoms - **THIS is where 14ms is spent**

## Real Question

Why does iterating 63 datoms from BadgerDB via AVET prefix scan take 14ms?

Possible causes:

1. ~~**BadgerDB cold start**~~ - RULED OUT: Second query after Analyze is still slow
2. **L85 decoding** - Each key requires L85 decode (25 chars → 20 bytes for E, 40 chars → 32 bytes for A)
3. **Value deserialization** - Reading and deserializing the value for each datom
4. **Iterator overhead** - BadgerDB iterator setup/seek cost per scan
5. **Key encoding for hash lookup** - Storage `hashJoinIterator` computes hash keys for each datom

## Diagnostic Test Results (2025-12-21)

Created `datalog/storage/perf_diagnostic_test.go` to isolate the issue. Results show **excellent performance** in isolation:

```
Step1_RawBadgerScan: 100 datoms in 28µs (0.28 µs/datom)
Step2_MatchPatternAsRelation: setup=6µs, iteration=199µs, total=205µs (100 tuples)
Step3_FullQueryExecution:
  Run 1: 409µs (first query warmup)
  Run 2-5: 105-220µs (warmed up)
Step4_AnalyzeQuery: Total=215µs, hash join=0.09ms
Step5_CollapseOverhead: setup=103µs, materialize=4µs
```

**Conclusion**: The issue is NOT reproducible in a simple test case. The 14ms is likely caused by factors specific to the production environment.

### Benchmark Results

```
BenchmarkDatomDecoding:       102ns/op, 3 allocs (208 bytes)
BenchmarkHashJoinIteration:   37µs/op for 100 datoms (370ns/datom)
```

At these speeds, 63 datoms should complete in **~23µs**, not 14ms. The 14ms represents a **~600x slowdown** from expected performance.

## Possible Production-Specific Causes

1. **Database size**: Production database has many more datoms affecting:
   - BadgerDB iterator seek time in larger LSM tree
   - Block cache efficiency with more competing data
   - Memory pressure from other allocations

2. **Data characteristics**: Production data may have:
   - Longer entity ID strings (larger hashes)
   - Different index key distributions
   - More complex value types requiring deserialization

3. **First-access overhead**: Even with "warm" database, specific key prefixes may not be cached:
   - Block cache misses for specific AVET ranges
   - BadgerDB L0/memtable layout affecting seek

4. **Concurrency effects**: Production may have:
   - Concurrent transactions affecting iterator performance
   - GC pressure from other operations

## Investigation TODO

1. ~~**Profile raw BadgerDB scan**~~ - DONE: 0.28µs/datom in test (FAST)
2. ~~**Check cold vs warm**~~ - DONE: First query 4x slower, then stabilizes (NOT the 14ms issue)
3. ~~**Profile production environment**~~ - DONE: Confirmed 16ms issue in production database
4. ~~**Compare database sizes**~~ - DONE: Production has 44,548 total datoms, 1,623 with `:task/scenario`
5. ~~**Root cause found**~~ - Missing AVET case in `chooseIndexForValues()` causing full index scan
6. ~~**Fix applied and verified**~~ - Query now takes ~100µs (150-160x improvement)

## Related Issues

- `docs/bugs/slow-query-multi-position-binding.md` - Similar symptoms but for multi-pattern queries (RESOLVED with 60x improvement)
- That fix addressed `NoReuse` strategy for multi-position bindings, but this is a single-pattern query with single-position binding

## Files Involved

- `datalog/storage/matcher_relations.go` - Strategy selection and matching
- `datalog/storage/hash_join_matcher.go` - Storage-level hash join scan
- `datalog/executor/relations.go` - Relation collapse with ctx.JoinRelations
- `datalog/executor/join.go` - HashJoin and HashJoinWithOptions functions
- `datalog/executor/context.go` - JoinRelations annotation wrapper

## Quick Validation

To check if this is a BadgerDB iteration issue vs executor issue:
```go
// Time just the storage scan
result, _ := matcher.MatchPatternAsRelation(ctx, pattern, bindings, nil)
for it := result.Iterator(); it.Next(); {
    _ = it.Tuple()
}
// If this is fast, the issue is in executor join logic
// If this is slow, the issue is in BadgerDB iteration
```

## New API Added

This investigation used the new `Explain()` and `Analyze()` methods added to the Database API:

```go
// Explain - returns query plan without executing
plan, err := db.Explain(queryStr, inputs...)
fmt.Println(plan.String())

// Analyze - executes with annotation collection
result, err := db.Analyze(queryStr, inputs...)
fmt.Println(result.String())  // Shows plan + execution trace
```

These are in `datalog/storage/database.go` with tests in `datalog/storage/explain_test.go`.
