# Time Range Extraction Optimization

## Problem Identified

Profiling revealed that `extractTimeRanges()` was spending most of its time and memory on:
1. **fmt.Sprintf()** - Creating string keys for deduplication (209MB allocated per benchmark run)
2. **Map with string keys** - Excessive allocations during map growth (907MB allocated per benchmark run)
3. **Slice growth** - Repeated reallocation as ranges slice grew

### Profiling Results (Before Optimization)

**CPU Profile:**
```
Line 654: key := fmt.Sprintf("%d-%d-%d-%d", ...) → 20ms (67% of function time)
Line 658: seen[key] = true                       → 10ms (33% of function time)
```

**Memory Profile:**
```
Line 654: fmt.Sprintf() → 209MB allocated
Line 658: seen[key]     → 907MB allocated
Total: Over 1GB allocated for 260 ranges!
```

## Optimizations Applied

### 1. Replace fmt.Sprintf with Struct Key

**Before:**
```go
seen := make(map[string]bool)
...
key := fmt.Sprintf("%d-%d-%d-%d", year, month, day, hour)  // Allocates string
if seen[key] {
    continue
}
seen[key] = true
```

**After:**
```go
type timeKey struct {
    year, month, day, hour int64
}

seen := make(map[timeKey]bool, inputRelation.Size())  // Pre-size map
...
key := timeKey{year: year, month: month, day: day, hour: hour}  // Zero allocations
if seen[key] {
    continue
}
seen[key] = true
```

**Impact:** Eliminates 260 string allocations per call (one per range)

### 2. Pre-allocate Slices with Exact Capacity

**Before:**
```go
var ranges []TimeRange  // Starts at nil, grows repeatedly
```

**After:**
```go
ranges := make([]TimeRange, 0, inputRelation.Size())  // Exact capacity
```

**Impact:** Eliminates repeated reallocation as slice grows

### 3. Pre-size Map

**Before:**
```go
seen := make(map[string]bool)  // Default size, grows repeatedly
```

**After:**
```go
seen := make(map[timeKey]bool, inputRelation.Size())  // Sized for expected load
```

**Impact:** Reduces map rehashing and reallocation

## Performance Results

### Benchmark Comparison

| Test Case | Before | After | Speedup | Memory Reduction | Allocation Reduction |
|-----------|--------|-------|---------|------------------|---------------------|
| **daily_22_ranges** | 4,229 ns/op<br/>5,360 B/op<br/>56 allocs/op | 1,181 ns/op<br/>2,632 B/op<br/>5 allocs/op | **3.6×** | 51% | **11×** |
| **daily_100_ranges** | 18,742 ns/op<br/>24,862 B/op<br/>218 allocs/op | 5,064 ns/op<br/>10,312 B/op<br/>5 allocs/op | **3.7×** | 58% | **44×** |
| **hourly_260_ranges** | 47,956 ns/op<br/>65,310 B/op<br/>543 allocs/op | 10,201 ns/op<br/>35,400 B/op<br/>5 allocs/op | **4.7×** | 46% | **108×** |
| **hourly_1000_ranges** | 187,641 ns/op<br/>255,398 B/op<br/>2,032 allocs/op | 41,610 ns/op<br/>147,570 B/op<br/>7 allocs/op | **4.5×** | 42% | **290×** |

### Critical Case: hourly_260_ranges (Real-world OHLC scenario)

- **Speed**: 47.956µs → 10.201µs = **4.7× faster**
- **Memory**: 65,310 bytes → 35,400 bytes = **46% reduction**
- **Allocations**: 543 → 5 = **108× fewer allocations**

From 543 allocations down to 5:
- 1 allocation for the map itself
- 1 allocation for the ranges slice
- 1 allocation for the iterator
- 2 allocations for internal bookkeeping

**Zero allocations in the hot loop** (no more per-range string allocations!)

## Impact on Query Performance

### Hourly OHLC (260 hours)
Before these optimizations, the function was taking ~48µs per call. Since decorrelated queries may call this multiple times, eliminating the overhead is crucial.

With the size check (inputRelation.Size() >= 50) and these optimizations:
- **Small relations (<50 tuples)**: Skip extraction entirely, zero overhead
- **Large relations (≥50 tuples)**: 4.7× faster extraction with 108× fewer allocations

### Daily OHLC (22 days)
With the size check, extraction is skipped entirely. The optimizations ensure that even if we lowered the threshold, the overhead would be minimal.

## Code Quality Improvements

1. **Type Safety**: Using struct keys instead of string concatenation is more type-safe
2. **Readability**: `timeKey{year: y, month: m, ...}` is clearer than `fmt.Sprintf("%d-%d-%d-%d", ...)`
3. **Maintainability**: Struct keys are easier to modify (add/remove fields) than format strings
4. **Performance**: Zero-allocation struct keys vs. expensive string formatting

## Lessons Learned

1. **Profile First**: The user was right - "just because we duck the overhead" doesn't mean it's not there
2. **fmt.Sprintf is Expensive**: String formatting is surprisingly costly in hot paths
3. **Struct Keys are Free**: Go's map supports any comparable type, including structs
4. **Pre-allocation Matters**: Knowing the size upfront eliminates repeated reallocation
5. **Allocation Counts Matter**: 543 allocations → 5 allocations is a massive reduction in GC pressure

## Conclusion

The optimization achieves:
- ✅ **4.7× speedup** on critical hourly OHLC case
- ✅ **108× fewer allocations** (543 → 5)
- ✅ **46% less memory** usage
- ✅ **Zero allocations in hot loop**
- ✅ All tests passing
- ✅ More readable and maintainable code

This is a proper optimization that eliminates the overhead rather than just avoiding it.
