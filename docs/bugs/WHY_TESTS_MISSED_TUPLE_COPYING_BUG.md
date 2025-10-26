# Why Tests Didn't Catch the Tuple Copying Bug

**Date**: 2025-10-24
**Related**: BUG_STREAMING_TUPLE_COPYING.md

## The Question

We have comprehensive test coverage with hundreds of tests. How did a **critical data-loss bug** affecting 18 files make it to production?

## Root Causes

### 1. Tests Extract Values Immediately

**Typical test pattern**:
```go
it := result.Iterator()
for it.Next() {
    tuple := it.Tuple()
    name := tuple[0].(string)     // Extract IMMEDIATELY
    count := tuple[1].(int64)      // Extract IMMEDIATELY

    // Use extracted values
    assert.Equal(t, "Alice", name)
}
```

**Why this works even with reusable buffers**:
- Values are extracted from the tuple within the same iteration
- By the time `Next()` is called again (reusing the buffer), the values have already been copied into Go variables
- `name` and `count` are **copies** of the tuple data, not references

**The broken pattern** (what production code does):
```go
var tuples []Tuple
it := result.Iterator()
for it.Next() {
    tuples = append(tuples, it.Tuple())  // Store REFERENCE
}
// Later...
for _, tuple := range tuples {
    // ALL tuples point to same memory - GARBAGE!
}
```

### 2. Small Test Datasets

Most tests use <100 tuples. With small datasets:
- Memory corruption might not be obvious
- Buffer reuse patterns might not trigger
- Random data in buffers might "accidentally" look correct
- Tests might pass due to luck with memory layout

**Example** - A test with 3 tuples might work because:
```go
// Iteration 1: buffer = [1, "Alice", 100]
// Iteration 2: buffer = [2, "Bob", 200]
// Iteration 3: buffer = [3, "Charlie", 300]
// Final: tuples = [ref_to_buffer, ref_to_buffer, ref_to_buffer]
//        All point to buffer = [3, "Charlie", 300]
```

But if the test only checks `len(tuples) == 3`, it passes! The test might not inspect the actual values in detail.

### 3. Tests Don't Verify All Tuple Values

Many tests check:
- Row count (`assert.Equal(t, 3, result.Size())`)
- First row only (`if it.Next() { ... }`)
- Specific values extracted immediately

They **don't** collect all tuples into a slice and verify each one is different.

### 4. Materialized Relations Hide the Bug

When `EnableTrueStreaming: false` (the old default):
- Relations materialize eagerly
- Iterators return pointers to stable storage
- Tuple buffers are NOT reused
- The bug is dormant

Tests written before streaming was enabled by default (commits 4a394cb-78c930a) never exercised reusable buffers.

### 5. The One Place That Should Have Caught It

**StreamingRelation.Iterator() fallback** (relation.go:860-869):
```go
it := r.iterator
count := 0
for it.Next() {
    count++
    tuples = append(tuples, it.Tuple())  // BUG!
}
```

This code path has the same bug! But it's a **fallback path** that only executes when:
- Streaming is disabled OR
- The relation needs materialization for some reason OR
- EnableTrueStreaming is false

Most streaming tests use the fast path (line 838: `return baseIter`) which bypasses this buggy fallback.

## Why gopher-street Caught It

The gopher-street tests succeeded where our tests failed because:

### 1. Large Real-World Dataset
- 28,040 datoms (not 10-100 like our tests)
- 15,552 datoms in first pattern scan
- Multiple patterns requiring tuple collection

### 2. CLI Tool (Not Test Framework)
- CLI calls `result.Table()` which collects ALL tuples
- Verifies displayed output contains actual data
- Human can visually see "0 rows" vs expected output

### 3. Complex Multi-Pattern Queries
```sql
[:find ?year ?month ?day (min ?open) (max ?high) ...
 :where [?s :symbol/ticker "CRWV"]
        [?e :price/symbol ?s]        -- Pattern 1
        [?e :price/time ?time]        -- Pattern 2
        ... 8 more patterns ...]
```

This triggers `matcher_relations.go:241` which collects binding tuples for pattern matching - THE critical bug location.

### 4. EnableTrueStreaming: true by Default
- CLI uses default options (streaming enabled)
- Triggers buffer reuse throughout
- No way to "accidentally" materialize and hide the bug

### 5. Annotations Showed the Smoking Gun
```
Scan([[?e :price/symbol ?s], AEVT, bound: ?) → 15552 datoms
Pattern([?e :price/symbol ?s]) → Relation([?e ?s], 0 Tuples)
```

The verbose output made it obvious: data goes in, nothing comes out.

## What Our Tests Were Missing

### Missing Test Pattern #1: Tuple Collection Verification
```go
func TestTupleCollectionWithStreaming(t *testing.T) {
    rel := NewMaterializedRelationWithOptions(columns, tuples, ExecutorOptions{
        EnableTrueStreaming: true,  // CRITICAL!
    })

    // Collect ALL tuples into slice (the broken pattern!)
    var collected []Tuple
    it := rel.Iterator()
    for it.Next() {
        tuple := it.Tuple()
        tupleCopy := make(Tuple, len(tuple))
        copy(tupleCopy, tuple)
        collected = append(collected, tupleCopy)
    }

    // Verify EACH tuple is different
    for i, tuple := range collected {
        assert.Equal(t, expectedTuples[i], tuple)
    }
}
```

### Missing Test Pattern #2: Large Dataset Testing
```go
func TestLargeDatasetStreaming(t *testing.T) {
    // Generate 10,000+ tuples
    var largeTuples []Tuple
    for i := 0; i < 10000; i++ {
        largeTuples = append(largeTuples, Tuple{
            int64(i),
            fmt.Sprintf("value-%d", i),
        })
    }

    // Test with streaming enabled
    rel := NewMaterializedRelationWithOptions(columns, largeTuples, ExecutorOptions{
        EnableTrueStreaming: true,
    })

    // Verify all tuples are unique and correct
    ...
}
```

### Missing Test Pattern #3: Integration Tests with BadgerDB

Our tests mostly use `MemoryMatcher`. We needed tests with:
- Real BadgerDB storage
- Multi-pattern queries
- Large datasets (thousands of datoms)
- Streaming enabled
- Full tuple collection and verification

**gopher-street** is effectively this integration test!

## Lessons Learned

### 1. Test What You Ship
- We ship CLI with `EnableTrueStreaming: true` by default
- Our tests should use the same defaults
- Don't hide bugs by using different settings in tests vs production

### 2. Test the Full Data Path
- Tests that extract values immediately are incomplete
- Need tests that collect tuples and verify ALL of them
- Need tests that exercise tuple storage, not just immediate consumption

### 3. Test at Multiple Scales
- 10 tuples: finds basic correctness bugs
- 100 tuples: finds off-by-one errors
- 10,000 tuples: finds buffer reuse bugs, memory corruption
- gopher-street has 28,040 datoms - perfect stress test!

### 4. Integration Tests Are Critical
- Unit tests (MemoryMatcher) missed this
- Integration tests (BadgerDB) might have caught it
- Real-world usage (gopher-street CLI) DID catch it

### 5. Annotations/Verbose Logging Saves Lives
The bug was invisible in normal output but obvious with `-verbose`:
```
Scan → 15552 datoms
Relation → 0 Tuples  // ← SMOKING GUN
```

### 6. Code Review Red Flags
This pattern should be flagged in code review:
```go
for it.Next() {
    collection = append(collection, it.Tuple())  // 🚨 RED FLAG
}
```

Should always be:
```go
for it.Next() {
    tuple := it.Tuple()
    tupleCopy := make(Tuple, len(tuple))
    copy(tupleCopy, tuple)
    collection = append(collection, tupleCopy)  // ✅ SAFE
}
```

## Recommendations

### Immediate
1. Add `TestStreamingTupleCopying` to executor package ✅ (already done)
2. Add linter rule to detect `append(x, it.Tuple())` pattern
3. Document `Iterator.Tuple()` lifetime contract

### Short Term
4. Add integration tests with BadgerDB and large datasets
5. Add stress tests with 10K+ tuples
6. Review all 18 files with tuple collection and fix them

### Long Term
7. Consider making Tuples immutable (copy-on-write)
8. Add `Iterator.TupleCopy()` method that always copies
9. Use static analysis to find all tuple collection sites
10. Make `EnableTrueStreaming` explicit in all tests

## Files to Review

Found 18 files with `append(.*it.Tuple())` pattern:
1. ✅ datalog/executor/table_formatter.go - FIXED
2. ❌ datalog/storage/matcher_relations.go:241 - CRITICAL BUG
3. ❌ datalog/executor/relation.go:864 - BUG in fallback path
4. ❌ datalog/executor/subquery.go
5. ❌ datalog/executor/query_executor.go
6. ❌ datalog/executor/executor.go
7. ❌ datalog/executor/streaming_union.go
8. ❌ datalog/executor/union_relation.go
9. ❌ datalog/executor/executor_utils.go
10. ❌ datalog/executor/executor_iteration.go
11. And 8 more...

Each needs manual review to determine if tuple copying is required.

## Conclusion

Our tests didn't catch this because:
1. **They consumed tuples immediately** (safe pattern that works even with reusable buffers)
2. **They used small datasets** (hid buffer reuse bugs)
3. **They didn't collect and verify all tuples** (the broken pattern!)
4. **Many used MemoryMatcher** (not the full storage path)

gopher-street caught it because:
1. **Large real-world dataset** (28K datoms)
2. **CLI tool displaying results** (human verification)
3. **Complex multi-pattern queries** (exercised tuple collection in matcher)
4. **Streaming enabled by default** (no escape hatch to hide the bug)
5. **Verbose annotations** (made the bug obvious)

**The silver lining**: This proves gopher-street is an excellent integration test suite that catches real bugs our unit tests miss!
