# Key Encoder Consolidation Benchmark Results

**Date**: 2024-12-24
**Branch**: `refactor/consolidate-key-encoders`
**Commit**: f2b047b

## Summary

The key encoder consolidation reduced code by ~50 lines (from ~800 to ~750 lines across 4 files) by extracting shared index ordering logic into a `baseKeyEncoder` with pluggable `ComponentEncoder` implementations for L85 and Binary strategies.

However, this introduced a **~10% performance regression** in storage-heavy operations due to interface indirection.

## Benchmark Comparison

### Key Encoder Heavy Benchmarks (Regression)

| Benchmark | Main | Refactor | Time | Memory | Allocs |
|-----------|------|----------|------|--------|--------|
| BenchmarkAVETReuse | 29ms, 16MB, 300K | 32ms, 20.8MB, 476K | +10% | +30% | +60% |
| BenchmarkBatchScanning/RegularIteratorReuse | 1.9ms, 1.95MB, 30.6K | 2.1ms, 2.35MB, 45.6K | +10% | +20% | +50% |
| BenchmarkBatchScanScaling/Size-1000 | 1.1ms, 1.15MB, 21K | 1.25ms, 1.41MB, 31K | +14% | +23% | +47% |
| BenchmarkBatchScanScaling/Size-5000 | 6.0ms, 4.6MB, 86K | 6.6ms, 5.7MB, 126K | +10% | +23% | +47% |

### Join/Query Benchmarks (Slight Regression)

| Benchmark | Main | Refactor | Time | Memory | Allocs |
|-----------|------|----------|------|--------|--------|
| BenchmarkHashJoinScanRangeComparison/Current | 5.2ms, 4.4MB, 68K | 5.9ms, 5.3MB, 101K | +13% | +20% | +48% |
| BenchmarkIndexNestedLoopVsHashJoin/hash_join_scan | 1.45ms, 1.86MB, 29K | 1.41ms, 2.02MB, 35K | -3% | +9% | +20% |

### Micro-benchmarks (Neutral/Improved)

| Benchmark | Main | Refactor | Change |
|-----------|------|----------|--------|
| BenchmarkIteratorLoop/helper | 3.1ms | 2.8ms | +10% faster |
| BenchmarkConstraintEvaluation/Int64Equality | 1.49ns | 1.37ns | +8% faster |
| BenchmarkConstraintEvaluation/StringEquality | 2.85ns | 3.01ns | -5% slower |

## Root Cause Analysis

The regression is caused by:

1. **Interface indirection**: The `ComponentEncoder` interface adds virtual dispatch overhead on every encode/decode operation
2. **Lazy initialization**: The `ensureInitialized()` check on every method call adds branch overhead
3. **Additional allocations**: Interface boxing and method calls may cause extra allocations

## Code Changes

```
4 files changed, 429 insertions(+), 483 deletions(-)
 datalog/storage/key_encoder_base.go      | 253 ++++++++++++++++++++++++
 datalog/storage/key_encoder_binary.go    | 116 +----------
 datalog/storage/key_encoder_interface.go |   4 +-
 datalog/storage/key_encoder_l85.go       | 186 ++---------------
```

## Recommendations

1. **If performance is critical**: Revert to the original implementation with duplicated but faster code
2. **If maintainability is preferred**: Accept the ~10% regression for cleaner code
3. **If both matter**: Optimize the consolidation by inlining critical paths and removing lazy init

## Raw Benchmark Data

### Refactor Branch (f2b047b)

```
BenchmarkAVETReuse-16                           	      36	  32480233 ns/op	20810866 B/op	  476191 allocs/op
BenchmarkAVETNoReuse-16                         	      36	  32524461 ns/op	20810832 B/op	  476190 allocs/op
BenchmarkBatchScanning/RegularIteratorReuse-16  	     549	   2129889 ns/op	 2349314 B/op	   45620 allocs/op
BenchmarkBatchScanning/BatchScanning-16         	     546	   2122288 ns/op	 2349316 B/op	   45620 allocs/op
BenchmarkBatchScanScaling/Size-1000-16          	     969	   1250260 ns/op	 1412051 B/op	   30979 allocs/op
BenchmarkBatchScanScaling/Size-5000-16          	     180	   6654971 ns/op	 5680673 B/op	  125748 allocs/op
```

### Main Branch (a504729)

```
BenchmarkAVETReuse-16                           	      39	  29125300 ns/op	16084446 B/op	  300459 allocs/op
BenchmarkAVETNoReuse-16                         	      39	  29951049 ns/op	16084474 B/op	  300459 allocs/op
BenchmarkBatchScanning/RegularIteratorReuse-16  	     625	   1972918 ns/op	 1949337 B/op	   30623 allocs/op
BenchmarkBatchScanning/BatchScanning-16         	     586	   1927919 ns/op	 1949337 B/op	   30623 allocs/op
BenchmarkBatchScanScaling/Size-1000-16          	    1099	   1093035 ns/op	 1146490 B/op	   21022 allocs/op
BenchmarkBatchScanScaling/Size-5000-16          	     193	   6008734 ns/op	 4615028 B/op	   85791 allocs/op
```
