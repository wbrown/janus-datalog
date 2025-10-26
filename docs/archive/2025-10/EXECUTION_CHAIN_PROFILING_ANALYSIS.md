# Execution Chain Profiling Analysis

**Date**: 2025-10-08
**Objective**: Profile the entire OHLC query execution chain to find optimization opportunities beyond `extractTimeRanges()`

## Summary

Profiled both daily (22 days) and hourly (260 hours) OHLC queries with decorrelated subqueries. The profiling revealed that **the in-memory execution path dominates**, while time range optimization only applies to BadgerDB storage queries.

## Benchmark Results

| Scenario | Time per Query | Memory | Allocations |
|----------|---------------|---------|-------------|
| **Daily (22 days)** | 1.28s | 54.4 MB | 515k allocs |
| **Hourly (260 hours)** | 1.77s | 74.6 MB | 709k allocs |

## CPU Hotspots

### Top 5 Functions (% of total CPU time)

| Function | Daily | Hourly | Description |
|----------|-------|--------|-------------|
| `matchesDatomWithPattern` | 21% | 20% | Pattern matching logic |
| `matchesConstant` | 20% | 19% | Constant value matching |
| `runtime.duffcopy` | 19% | 17% | Memory copying |
| `matchesElement` | 9% | 10% | Element comparison |
| `runtime.memequal` | 8% | 8% | Memory equality checks |

**Total pattern matching**: ~58% of CPU time

## Memory Allocation Hotspots

### Top 5 Allocators (% of total allocations)

| Function | Daily | Hourly | Description |
|----------|-------|--------|-------------|
| `TupleKeyMap.Put` | 35% | 30% | Hash map operations for joins |
| `evaluateExpressionNew` | 9% | 19% | Expression evaluation (includes time functions) |
| `matchWithBoundPattern` | 8% | 5% | Pattern matching with bindings |
| `matchWithoutBindings` | 6% | 4% | Pattern matching without bindings |
| `HashJoin` | 5% | 3% | Join operations |

## Key Findings

### 1. Pattern Matching Dominates (58% CPU, 20% Memory)

**Observation**: More than half of CPU time is spent in `MemoryPatternMatcher` operations:
- `matchesDatomWithPattern` - 20-21%
- `matchesConstant` - 19-20%
- `matchesElement` - 9-10%

**Why**: These benchmarks use in-memory data (no storage layer), so pattern matching is the primary operation.

**Implication**: For in-memory queries, optimizing pattern matching would have the biggest impact.

### 2. Time Range Optimization NOT Active

**Critical Discovery**: `extractTimeRanges()` and decorrelation functions do **NOT appear in the CPU profiles**.

**Why**:
- Time range optimization (`scanTimeRanges`) only works with **BadgerMatcher** and AVET index
- The benchmark uses **MemoryPatternMatcher** which doesn't support time range scanning
- Memory matcher simply scans all datoms for each pattern

**What This Means**:
- The optimization we implemented and profiled (extractTimeRanges) helps **storage-backed queries only**
- In-memory queries don't benefit from time range extraction
- To profile the time range optimization, we need to use BadgerDB storage

### 3. Hash Join Overhead (35% of allocations)

**Observation**: `TupleKeyMap.Put` allocates 35% of memory (daily), 30% (hourly)

**Why**: Hash joins build hash maps to efficiently match tuples on join keys

**Optimization Opportunities**:
- Pre-size hash maps based on relation sizes
- Reuse hash maps across iterations
- Consider merge joins for sorted relations

### 4. Expression Evaluation (9-19% of allocations)

**Observation**: `evaluateExpressionNew` allocates 9% (daily) to 19% (hourly) of memory

**What's Included**:
- Time extraction functions: `year()`, `month()`, `day()`, `hour()`
- Arithmetic operations
- String formatting

**Why Higher in Hourly**: More expressions per tuple (year + month + day + hour vs just day)

**Optimization Opportunities**:
- Cache time component extractions
- Reuse time.Time allocations
- Pre-compute common expressions

### 5. Memory Copying (17-19% CPU)

**Observation**: `runtime.duffcopy` uses 17-19% of CPU time

**Why**: Go copies structs by value, and Datalog tuples are repeatedly copied:
- Pattern matching copies datoms
- Join operations copy tuples
- Deduplication copies tuples

**Optimization Opportunities**:
- Use pointers where safe
- Reduce intermediate tuple creation
- Streaming iterators to avoid materialization

## What We Learned About the "Whole Chain"

### The Chain for In-Memory Queries

1. **Query Parsing** (not measured - negligible)
2. **Query Planning** (not measured - negligible)
3. **Pattern Matching** (~58% CPU, ~20% allocations)
   - Scan datoms: `matchWithBoundPattern`, `matchWithoutBindings`
   - Filter: `matchesDatomWithPattern`, `matchesConstant`, `matchesElement`
4. **Expression Evaluation** (~9-19% allocations)
   - Time extractions: `year()`, `month()`, `day()`, `hour()`
   - Arithmetic and string operations
5. **Join Operations** (~35% allocations)
   - Build hash maps: `TupleKeyMap.Put`
   - Join tuples: `HashJoin`, `hashJoinWithMapping`
6. **Aggregation** (~2-4% allocations)
   - Group and aggregate: `executeGroupedAggregation`
7. **Deduplication** (~3-18% allocations)
   - Remove duplicates: `deduplicateTuples`
8. **Result Materialization** (included in above)

### The Chain for Storage-Backed Queries (Not Profiled)

What we **didn't** profile but is critical for BadgerDB queries:

1. **Pattern Matching** → `BadgerMatcher.Match()`
   - **Index Selection**: Choose EAVT, AEVT, AVET, VAET, or TAEV
   - **Time Range Scanning**: `scanTimeRanges()` for multi-range AVET scans (THIS IS THE OPTIMIZATION)
   - **Datom Decoding**: Convert storage format to in-memory datoms
2. **Storage I/O**
   - BadgerDB LSM reads
   - Index seeks and scans
   - Value decoding

## Profiling Metadata Operations

Separately profiled metadata propagation overhead (used for time range passing):

| Operation | Time per op | Allocations |
|-----------|-------------|-------------|
| `SetMetadata` | 20.14 ns | 24 B (1 alloc) |
| `GetMetadata` | 6.68 ns | 0 B (0 allocs) |
| `Get + TypeAssert` | 6.07 ns | 0 B (0 allocs) |

**Conclusion**: Metadata operations are **negligible** (<0.01% of execution time)

## What's Missing: BadgerDB + Time Range Profiling

To actually profile the time range optimization, we need:

1. **BadgerDB-backed benchmark**
   - Create database with 260 hours of price data
   - Use `BadgerMatcher` instead of `MemoryPatternMatcher`
   - Run the hourly OHLC query

2. **Profile the storage layer**
   - `scanTimeRanges()` - multi-range AVET scanning
   - `EncodePrefix()` - key encoding for range scans
   - BadgerDB iterator operations

3. **Measure the optimization impact**
   - With time ranges: Multi-range AVET scan (260 ranges)
   - Without time ranges: Full AVET scan or pattern matching
   - Compare execution time and datoms scanned

## Recommended Next Steps

### High Priority: BadgerDB Profiling

Create `ohlc_badger_profile_test.go` to profile:
```go
func BenchmarkOHLCBadgerDBWithTimeRanges(b *testing.B) {
    // 1. Create BadgerDB with realistic data
    db := createBadgerDBWithOHLCData(260 hours, 10 bars/hour)

    // 2. Run hourly OHLC query (triggers time range optimization)
    query := `[:find ?day ?hour ?open ?high ?low ?close
               :where
               [?s :symbol/ticker "TEST"]
               [?b :price/symbol ?s]
               [?b :price/time ?t]
               [(day ?t) ?day]
               [(hour ?t) ?hour]
               ; 4 decorrelated subqueries with time correlation]`

    // 3. Profile with -cpuprofile and -memprofile
    // 4. Look for scanTimeRanges in profile
}
```

### Medium Priority: Hash Join Optimization

**Current State**: 35% of allocations in TupleKeyMap operations

**Options**:
1. Pre-size maps: `make(map[TupleKey][]Tuple, estimatedSize)`
2. Map pooling: Reuse maps across iterations
3. Merge joins: For sorted relations, avoid hash maps entirely

### Lower Priority: Expression Caching

**Current State**: 9-19% of allocations in expression evaluation

**Options**:
1. Cache time component extractions per time.Time value
2. Reuse common subexpressions (CSE already implemented for some cases)
3. Pre-compute expressions that don't depend on datom values

## Lessons Learned

### 1. Profile the Right Thing

We profiled in-memory execution but the optimization targets storage-backed queries. Need to profile BadgerDB queries to see time range optimization impact.

### 2. Optimization Location Matters

- **extractTimeRanges optimization** (4.7× faster, 108× fewer allocations) helps **storage queries**
- **Pattern matching optimization** (58% CPU) would help **in-memory queries**
- Different execution paths have different bottlenecks

### 3. Metadata Overhead is Negligible

Passing time ranges via context metadata adds <0.01% overhead. The size check to skip extraction (<50 tuples) is the right trade-off.

### 4. The Real Bottleneck

For storage-backed OHLC queries:
- **With time range optimization**: Storage I/O and index scanning
- **Without optimization**: Full table scans on :price/time attribute

The 4× speedup we achieved (41s → 10.2s for hourly) shows the optimization works, but we haven't profiled **where** in the storage layer the time is spent.

## Bottom Line

**What We Profiled**: In-memory query execution
**What We Optimized**: Storage-backed time range extraction
**What We Need**: BadgerDB profiling to see storage layer bottlenecks

**The optimization works** (4× speedup on hourly OHLC), but to find more opportunities we need to profile the actual storage execution path where time ranges are used.
