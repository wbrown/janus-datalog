# Storage-Level Symmetric Hash Join Analysis

**Date**: 2025-10-26
**Question**: Can storage-level joins benefit from symmetric hash join?

## Current Storage Join Strategies

### 1. IndexNestedLoop (Currently Used for Streaming)
```go
for bindingTuple in pattern1Iterator:
    seekKey = buildSeekKey(bindingTuple, pattern2)
    storageResults = storage.Seek(seekKey)
    emit matches
```

**Performance**: ~770ms for 1000 records
- Many expensive seeks (1000 seeks for 1000 bindings)
- No materialization (good!)
- But: Seek overhead dominates

### 2. HashJoinScan (Currently Unused for Streaming)
```go
hashSet = materializeAll(pattern1Iterator)  // ← Materializes everything!
for datom in storage.Scan(pattern2):
    if datom.entity in hashSet:
        emit match
```

**Currently never triggers** because `StreamingRelation.Size() = -1`

### 3. MergeJoin (High Selectivity)
Assumes sorted data, merges in one pass.

## Could Storage Use Symmetric Hash Join?

### Theoretical Approach

```go
// Symmetric hash join at storage level
leftIter = pattern1Iterator  // Streaming from storage
rightIter = storage.Scan(pattern2)  // Full storage scan

leftTable = {}
rightTable = {}

while not done:
    // Process batch from left (pattern1 bindings)
    for i in 0..batchSize:
        if leftIter.Next():
            tuple = leftIter.Tuple()

            // Probe right table for matches
            probe(rightTable, tuple)

            // Add to left table
            leftTable[tuple.entity] = tuple

    // Process batch from right (storage scan)
    for i in 0..batchSize:
        if rightIter.Next():
            datom = rightIter.Tuple()

            // Probe left table for matches
            probe(leftTable, datom)

            // Add to right table
            rightTable[datom.entity] = datom
```

### Benefits

✅ **Early termination**: LIMIT 10 stops after finding 10 matches
✅ **No upfront materialization**: Processes incrementally
✅ **Memory efficient**: Only keeps processed tuples, not full dataset

### Challenges

❌ **Defeats seek optimization**:
   - Normal: Use binding values to seek directly (`?p = person5` → seek to person5's datoms)
   - Symmetric: Must scan ALL storage datoms to find matches incrementally

❌ **Storage scan cost**:
   - Full table scans are expensive in BadgerDB
   - The whole point of storage-level joins is to AVOID full scans

❌ **Iterator overhead**:
   - BadgerDB iterators are expensive to create/maintain
   - Symmetric approach needs long-lived iterator, alternates between sources

❌ **Complexity**:
   - State management across batches
   - Synchronization between sources
   - Error handling

## Why Storage-Level Joins Exist

The key insight: **Storage-level joins exist to reduce storage scans**.

**Without storage-level join** (executor does it):
```
Pattern1: Scan ALL :person/name datoms → 1000 datoms
Pattern2: Scan ALL :person/email datoms → 1000 datoms
Executor: Hash join the 1000 × 1000 results
```

**With storage-level join** (using bindings):
```
Pattern1: Scan ALL :person/name datoms → 1000 datoms
Pattern2: For each ?p value, seek to that entity's :person/email
         → 1000 seeks, but each returns 1 datom
         → Total: 1000 datoms scanned, not full table
```

**Symmetric at storage level loses this**:
```
Pattern1: Batch scan :person/name → emit 100 at a time
Pattern2: Full scan :person/email → 1000 datoms
         → Same cost as executor-level join!
```

## Better Alternatives

### Option A: Batched Seeks (IndexNestedLoop++)

Instead of seeking once per binding:
```go
// Current: 1000 individual seeks
for tuple in bindings:
    storage.Seek(tuple.entity + ":person/email")

// Batched: 10 batch seeks of 100 entities each
for batch in bindings.chunks(100):
    entities = batch.map(t => t.entity)
    storage.SeekMultiple(entities, ":person/email")
```

**Pros**: Amortizes seek overhead
**Cons**: BadgerDB doesn't have efficient multi-seek API

### Option B: Streaming Hash Join (Storage-Level)

Don't check Size(), always use hash join:
```go
// Build hash set incrementally (don't materialize all at once)
hashSet = {}
for tuple in pattern1Iterator:
    hashSet[tuple.entity] = tuple

    // Every N tuples, scan storage for those entities
    if len(hashSet) >= batchSize:
        results = storage.ScanForEntities(hashSet.keys(), pattern2)
        emit results
        hashSet.clear()
```

**Pros**: Batches seeks, can stop early
**Cons**: Still not truly streaming (buffers batchSize)

### Option C: Executor-Level Join Only

Storage layer just returns StreamingRelations:
```go
// Pattern 1 → StreamingRelation (unbounded scan)
// Pattern 2 → StreamingRelation (unbounded scan)
// Executor: Uses symmetric hash join on the two streams
```

**Pros**:
- Simple
- Uses existing symmetric hash join
- No storage-level complexity

**Cons**:
- Both patterns do FULL table scans
- Wasteful when pattern2 only needs specific entities
- Loses the whole point of storage-level join optimization

## Recommendation

**For our use case** (Datalog queries with entity joins):

1. **Keep storage-level joins** - they avoid expensive full table scans
2. **Fix strategy selection** - don't require known Size() for hash join
3. **Implement streaming hash join** - variant that doesn't materialize upfront
4. **Use symmetric hash join** - only at executor level for already-scanned relations

**Symmetric hash join belongs at the executor level**, where both sides are already materialized or streaming from storage scans. Storage-level joins should focus on **reducing the amount of storage accessed**, not on streaming the join algorithm itself.

## Why Benchmarks Are Slow

Current behavior with streaming:
- Pattern 1: Scan storage → StreamingRelation (Size() = -1)
- Pattern 2: `chooseJoinStrategy()` sees Size() = -1 → picks IndexNestedLoop
- Result: 1000 individual seeks to storage (expensive!)

**The problem isn't lack of symmetric join - it's that IndexNestedLoop is slow with many seeks.**

## Proposed Fix

Modify `chooseJoinStrategy()` to use hash join even for unknown sizes:

```go
func (m *BadgerMatcher) chooseJoinStrategy(...) JoinStrategy {
    bindingSize := bindingRel.Size()

    // OLD: Never use hash join for streaming (Size = -1)
    if bindingSize <= 10 {
        return IndexNestedLoop
    }

    // NEW: Use hash join for streaming, estimate size if needed
    if bindingSize < 0 {
        // Unknown size - assume medium/large, use hash join
        // Will materialize during hash set build, but only once
        return HashJoinScan
    }

    if bindingSize <= 10 {
        return IndexNestedLoop
    }

    // ... rest of logic
}
```

This would:
- ✅ Materialize pattern1 once into hash set
- ✅ Scan storage once for pattern2
- ✅ Total: 2 scans instead of 1000+ seeks
- ❌ No early termination for LIMIT (but that's acceptable for correctness)
