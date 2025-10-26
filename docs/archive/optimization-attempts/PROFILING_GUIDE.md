# Profiling Guide: How to Profile BadgerDB Queries Correctly

**Problem**: Standard benchmarks profile database setup (90%) instead of query execution (10%)

**Solution**: Use pre-built test databases to isolate query performance

---

## Quick Start (3 Steps)

### Step 1: Build Test Database (One-Time Setup)
```bash
# Create a 72,000-bar database (100 symbols × 30 days × 24 hours)
# This takes ~10 seconds and creates testdata/ohlc_benchmark.db
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x
```

**Output**:
```
Writing 504000 datoms to testdata/ohlc_benchmark.db...
  Written 10000/504000 datoms (2.0%)
  Written 20000/504000 datoms (4.0%)
  ...
✅ Database created: testdata/ohlc_benchmark.db
   Total datoms: 504,000
   Symbols: 100, Days: 30, Bars/day: 24
```

### Step 2: Profile Query Execution
```bash
# Run benchmarks WITH profiling
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -cpuprofile=badger_cpu.prof \
    -memprofile=badger_mem.prof \
    -benchtime=3s \
    ./datalog/storage
```

### Step 3: Analyze Profile
```bash
# View CPU profile in browser
go tool pprof -http=:8080 badger_cpu.prof

# Or view in terminal
go tool pprof -top badger_cpu.prof
go tool pprof -list=matchesDatom badger_cpu.prof
```

---

## What This Gives You

### Before (Standard Benchmark)
```
CPU Profile shows:
  runtime.madvise       48%  ← Database memory setup
  BadgerDB.Write        40%  ← LSM tree writes
  matchesDatom           5%  ← Actual query code (buried!)
  chooseIndex            3%
  Iterator.Next          2%
```

**Problem**: 88% of profile is database creation, only 10% is queries!

### After (Pre-built Database)
```
CPU Profile shows:
  matchesDatom          35%  ← Pattern matching logic (clear signal!)
  Iterator.Next         25%  ← Storage scanning
  chooseIndex           15%  ← Index selection
  valuesEqual           10%  ← Value comparison
  buildTuple             8%  ← Tuple construction
  runtime.memequal       7%  ← Memory operations
```

**Success**: 100% of profile is query execution!

---

## Available Test Databases

### 1. Default OHLC (Recommended)
```go
DefaultOHLCConfig()
- Path: testdata/ohlc_benchmark.db
- Symbols: 100 stocks
- Days: 30 days
- Bars/day: 24 (hourly)
- Total bars: 72,000
- Total datoms: ~504,000 (7 attributes/bar)
- Disk size: ~75 MB
- Build time: ~10 seconds
```

### 2. Large OHLC (Stress Testing)
```bash
# Build large database (takes ~5 minutes)
go test -run=^$ -bench=^BenchmarkBuildLargeDatabase$ ./datalog/storage -benchtime=1x
```

```go
LargeOHLCConfig()
- Path: testdata/ohlc_large.db
- Symbols: 500 stocks
- Days: 365 days
- Bars/day: 390 (minute bars)
- Total bars: 71,175,000
- Total datoms: ~498M
- Disk size: ~50 GB
- Build time: ~5 minutes
```

---

## Benchmark Scenarios

### Pattern Matching Benchmarks

```bash
# Run all pattern matching benchmarks
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching -benchtime=3s ./datalog/storage
```

**Available scenarios**:
1. **UnboundAttribute_PriceTime** - Scan all `:price/time` datoms (tests AEVT index)
2. **UnboundAttribute_PriceOpen** - Scan all `:price/open` datoms (tests AEVT index)
3. **BoundEntity_SingleBar** - Lookup single bar entity (tests EAVT index)
4. **BoundSymbol_SpecificStock** - Find all bars for one symbol (tests AVET index)
5. **LargeBindingSet_260Entities** - Match 260 entities with pattern (tests iterator reuse)

### Full Query Benchmarks

```bash
# Run end-to-end query benchmarks
go test -bench=BenchmarkPrebuiltDatabase_FullQuery -benchtime=3s ./datalog/storage
```

**Available scenarios**:
- **DailyOHLC_30Days** - Aggregate 30 days of OHLC for one symbol

---

## Profiling Workflow

### Goal: Optimize `matchesDatom()` Function

#### Step 1: Baseline Profile
```bash
# Profile current implementation
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen \
    -cpuprofile=baseline_cpu.prof \
    -benchtime=5s \
    ./datalog/storage

# View hotspots
go tool pprof -top baseline_cpu.prof
```

**Look for**:
- Time spent in `matchesDatom()`
- Time spent in type switches
- Time spent in `valuesEqual()`
- Time spent in `fmt.Sprintf()`

#### Step 2: Make Changes
```bash
# Edit datalog/storage/matcher.go
# Optimize matchesDatom() implementation
```

#### Step 3: Compare Profiles
```bash
# Profile optimized version
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen \
    -cpuprofile=optimized_cpu.prof \
    -benchtime=5s \
    ./datalog/storage

# Compare side-by-side
go tool pprof -base=baseline_cpu.prof optimized_cpu.prof
```

**Success Criteria**:
```
Showing nodes accounting for -1.20s, 15% of 8.0s total
      flat  flat%   sum%        cum   cum%
    -0.50s  6.25% 6.25%     -0.50s  6.25%  matchesDatom (improved!)
    -0.40s  5.00% 11.25%    -0.40s  5.00%  fmt.Sprintf (eliminated!)
    -0.30s  3.75% 15.00%    -0.30s  3.75%  type switch overhead (reduced!)
```

---

## Advanced: Profile Specific Code Paths

### Profile Index Selection
```bash
# Focus on index selection logic
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -cpuprofile=index_selection.prof \
    -benchtime=10s \
    ./datalog/storage

# Analyze chooseIndex function
go tool pprof -list=chooseIndex index_selection.prof
```

### Profile Iterator Performance
```bash
# Focus on large binding sets (tests iterator reuse)
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching/LargeBindingSet \
    -cpuprofile=iterator.prof \
    -benchtime=10s \
    ./datalog/storage

# Analyze iterator code
go tool pprof -list=reusingIterator iterator.prof
```

### Profile Memory Allocations
```bash
# Profile memory allocations
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -memprofile=mem.prof \
    -benchtime=5s \
    ./datalog/storage

# Find allocation hotspots
go tool pprof -alloc_space -top mem.prof
go tool pprof -alloc_objects -top mem.prof

# View in browser
go tool pprof -http=:8080 -alloc_space mem.prof
```

---

## Benchmark Output Interpretation

### Good Benchmark Run
```
BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen-8
    5000    245678 ns/op    12345 B/op    234 allocs/op
```

**Metrics**:
- `5000 iterations` - Number of times benchmark ran
- `245678 ns/op` - Average time per operation (245µs)
- `12345 B/op` - Bytes allocated per operation
- `234 allocs/op` - Number of allocations per operation

### Compare Before/After
```bash
# Save baseline
go test -bench=. ./datalog/storage > baseline.txt

# Make changes, then compare
go test -bench=. ./datalog/storage > optimized.txt
benchstat baseline.txt optimized.txt
```

**Output**:
```
name                                    old time/op    new time/op    delta
PatternMatching/UnboundAttribute-8       245µs ± 2%     180µs ± 1%  -26.53%
PatternMatching/LargeBindingSet-8       1.5ms ± 3%     0.8ms ± 2%  -46.67%

name                                    old alloc/op   new alloc/op   delta
PatternMatching/UnboundAttribute-8      12.3kB ± 0%    9.1kB ± 0%  -26.02%
PatternMatching/LargeBindingSet-8       89.2kB ± 0%   45.6kB ± 0%  -48.88%
```

---

## Troubleshooting

### "Test database not found"
```
Error: Test database not found: testdata/ohlc_benchmark.db (run BuildTestDatabase first)
```

**Fix**: Build the database first:
```bash
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x
```

### "Profile too short"
```
Error: No samples in profile
```

**Fix**: Run benchmark longer:
```bash
go test -bench=BenchmarkPrebuiltDatabase -benchtime=10s -cpuprofile=cpu.prof ./datalog/storage
```

### "Database is locked"
```
Error: resource temporarily unavailable
```

**Fix**: Close any other processes using the database, or delete and rebuild:
```bash
rm -rf testdata/ohlc_benchmark.db
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x
```

---

## Best Practices

### ✅ DO

1. **Build database once** - Reuse across multiple benchmark runs
2. **Use realistic data** - 72,000 bars matches real hourly OHLC workload
3. **Run benchmarks multiple times** - Use `-benchtime=3s` or `-benchtime=5s`
4. **Profile with longer runs** - At least 3-5 seconds for clean profiles
5. **Compare before/after** - Use `benchstat` to validate improvements
6. **Focus on one thing** - Profile one optimization at a time

### ❌ DON'T

1. **Don't profile database creation** - It's not the query path
2. **Don't run short benchmarks** - Need enough samples for statistical significance
3. **Don't optimize without profiling** - Intuition is often wrong
4. **Don't trust single runs** - Benchmark variance can be high
5. **Don't skip memory profiling** - Allocations often hidden bottleneck

---

## Example Optimization Session

### Goal: Reduce type switch overhead in `matchesDatom()`

#### 1. Baseline
```bash
$ go test -bench=BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen \
    -cpuprofile=baseline.prof -benchtime=5s ./datalog/storage

BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen-8
    5000    245678 ns/op    12345 B/op    234 allocs/op

$ go tool pprof -top baseline.prof | head -15
    flat%   cum%    function
    15%     25%     matchesDatom
    8%      18%     fmt.Sprintf
    7%      15%     type switch (multiple)
    ...
```

#### 2. Optimize
Edit `datalog/storage/matcher.go`:
```go
// Before
if e != nil {
    switch ev := e.(type) {
    case datalog.Identity:
        if !datom.E.Equal(ev) { return false }
    default:
        if datom.E.String() != fmt.Sprintf("%v", e) { return false }
    }
}

// After (inline fast path)
if e != nil {
    if eId, ok := e.(datalog.Identity); ok {
        if !datom.E.Equal(eId) { return false }
    } else {
        // Slow path (avoid string allocation)
        if datom.E.String() != fmt.Sprint(e) { return false }
    }
}
```

#### 3. Verify
```bash
$ go test -bench=BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen \
    -cpuprofile=optimized.prof -benchtime=5s ./datalog/storage

BenchmarkPrebuiltDatabase_PatternMatching/UnboundAttribute_PriceOpen-8
    6800    180456 ns/op    9123 B/op    189 allocs/op

$ benchstat baseline.txt optimized.txt
name                                    old time/op    new time/op    delta
PatternMatching/UnboundAttribute-8       245µs ± 2%     180µs ± 1%  -26.53%  (p=0.000)

name                                    old alloc/op   new alloc/op   delta
PatternMatching/UnboundAttribute-8      12.3kB ± 0%    9.1kB ± 0%  -26.02%  (p=0.000)
```

**Result**: ✅ 26.5% faster, 26% less memory!

---

## Summary

**The Problem**: Standard benchmarks profile 90% database setup, 10% queries

**The Solution**: Pre-built test databases isolate query performance

**The Workflow**:
1. Build test database once: `go test -bench=BenchmarkBuildTestDatabase -benchtime=1x`
2. Profile queries: `go test -bench=BenchmarkPrebuiltDatabase -cpuprofile=cpu.prof -benchtime=5s`
3. Analyze: `go tool pprof -http=:8080 cpu.prof`
4. Optimize and compare: Use `benchstat` to validate improvements

**Now you're profiling the right thing!** 🎯
