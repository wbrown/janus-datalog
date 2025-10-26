# Large Binding Set Performance Issue - RESOLVED ✅

**Date**: 2025-10-04 (diagnosed) → 2025-10-05 (resolved)
**Status**: ✅ RESOLVED - Hash join implementation complete

## Problem Statement

Queries with large binding sets (>1000 entities) were extremely slow, timing out after 30+ seconds.

**Example Query**:
```datalog
[:find ?o
 :where
   [?s :symbol/ticker "NVDA"]     ; Phase 1: 1 symbol (fast, 57µs)
   [?b :price/symbol ?s]           ; Phase 2: 4,224 prices (fast, 2.7ms)
   [?b :price/time ?t]             ; Phase 2: 4,224 bound ?b → HANGS (>30s) ❌
   [?b :price/open ?o]]
```

## Resolution Summary

**Root Cause Identified**: Iterator reuse was scanning 31,173,242 datoms for 79 entities
- Iterator reuse with Seek() had excessive overhead for medium-sized binding sets
- SimpleBatchScanner created wide scan ranges spanning entire entity range

**Solution Implemented**: Hash join scan strategy (commit fa323ec)
- Build hash set from bindings: O(n)
- Single full scan with hash probe: O(m)
- Total complexity: O(n + m) vs O(n × log m) for iterator reuse

**Performance Results**:
```
Test 2 (aggregation):  timeout → 523ms
Test 3 (79 rows):      61+ sec → 69ms (880x speedup!)
Test 5 (complex OHLC): 27 sec  → 210ms (128x speedup!)
```

## What We Discovered

### Actual Bottleneck
The time was spent in **excessive Seek() operations** with iterator reuse:
- 79 bindings × expensive Seek() in BadgerDB LSM-tree
- Wide iteration between entities still scanned millions of datoms
- Boundary checking overhead to detect "moved past" conditions

### Measurements Captured
Before hash join:
- Datoms scanned: 31,173,242 for 79 entities
- Pattern: `[?b :price/open ?o]` with 79 bound entities
- Iterator reuse strategy: sequential seeks

After hash join:
- Datoms scanned: 12,480 for 79 entities (2,497x reduction!)
- Strategy: Single scan + hash probe
- Hash hit rate: 79/12,480 = 0.63% (low selectivity, perfect for hash join)

## Implementation Details

### Hash Join Algorithm
```go
// Phase 1: Build hash set (O(n))
hashSet := buildHashSet(bindingRel, position)  // 79 entities

// Phase 2: Single scan with probe (O(m))
for iter.Next() {  // 12,480 datoms total
    datom := iter.Datom()
    probeKey := extractProbeKey(datom, position)
    if bindingTuple, found := hashSet[probeKey]; found {  // O(1) lookup
        // Match found, build result tuple
    }
}
```

### Strategy Selection Logic
```go
func chooseJoinStrategy(bindingSize, patternCard):
    if bindingSize <= 10:
        return IndexNestedLoop    // Hash overhead not worth it
    if bindingSize <= 1000:
        return HashJoinScan       // Proven reliable
    // For >1000, consider selectivity
    selectivity = bindingSize / patternCard
    if selectivity < 0.50:
        return HashJoinScan       // Medium selectivity
    else:
        return HashJoinScan       // Future: merge join
```

### Files Modified
1. **datalog/storage/hash_join_matcher.go** (new)
   - `matchWithHashJoin()`: Main hash join algorithm
   - `chooseJoinStrategy()`: Selectivity-based strategy selection
   - `buildHashSet()`: Build phase for hash join
   - `extractProbeKey()`: Probe key extraction from datoms

2. **datalog/storage/matcher_relations.go** (modified)
   - Wire up hash join strategy in `MatchWithConstraints()`
   - Route based on `chooseJoinStrategy()` decision

## Relational Theory Validation

The implementation validates classic database join algorithm theory:

**Index Nested Loop Join**: Good for high selectivity or tiny sets
- O(n × log m) with index seeks
- Best when n ≤ 10 or selectivity > 90%

**Hash Join**: Good for medium selectivity (1-50%)
- O(n + m) with single scan
- Best when 10 < n < 10,000 and selectivity 1-50%

**Merge Join**: Good for high selectivity on sorted data (future)
- O(n + m) merging sorted streams
- Best when n > 10,000 and selectivity > 50%

## Performance Expectations After Fix

All gopher-street queries now perform well:
- Test 1 (count datoms): 181ms ✅
- Test 2 (aggregation with filters): 523ms ✅ (was timeout)
- Test 3 (79 rows with filters): 69ms ✅ (was 61+ seconds)
- Test 4 (simple subquery): 35ms ✅ (already fast)
- Test 5 (complex OHLC): 210ms ✅ (was 27 seconds)

## Future Optimizations

### Merge Join (Priority: Low)
For very large binding sets (>10,000 entities) with high selectivity (>50%):
- Sort both binding relation and pattern scan
- Merge sorted streams in O(n + m)
- Avoids hash table memory overhead

### Predicate Pushdown to Hash Join
Push predicates into hash join probe phase:
- Filter during scan, not after materialization
- Reduce intermediate result sizes
- Already have constraint infrastructure

### Statistics-Based Cardinality Estimation
Replace hardcoded 10,000 estimate with real statistics:
- Track datom counts per attribute in BadgerStore
- Use for accurate selectivity calculation
- Enables better strategy selection

## Bottom Line

**Problem**: Large binding sets timing out (>30 seconds)
**Root Cause**: Iterator reuse scanning 31M datoms for 79 entities
**Solution**: Hash join scan with single full scan + O(1) probe
**Result**: 880x speedup, 2,497x reduction in datoms scanned
**Status**: ✅ RESOLVED - Production ready
