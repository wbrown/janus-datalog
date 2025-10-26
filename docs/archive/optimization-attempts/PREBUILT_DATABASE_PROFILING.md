# Pre-built Database Profiling - Quick Reference

**Problem Solved**: Standard benchmarks profile 90% database setup, 10% queries
**Solution**: Pre-build test database once, reuse for all profiling

---

## TL;DR - 3 Commands

```bash
# 1. Build test database (one-time, ~10 seconds)
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x

# 2. Profile queries (run anytime)
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -cpuprofile=cpu.prof -benchtime=3s ./datalog/storage

# 3. View results
go tool pprof -http=:8080 cpu.prof
```

---

## What Gets Created

### Test Database Specs
```
Path: datalog/storage/testdata/ohlc_benchmark.db
Size: ~75 MB
Content:
  - 100 stock symbols (TICK0000 through TICK0099)
  - 30 days of data
  - 24 bars per day (hourly)
  - 72,000 total bars
  - 504,000 datoms (7 attributes per bar)
```

### Attributes Per Bar
```
:price/symbol         - Link to symbol entity
:price/time           - time.Time timestamp
:price/minute-of-day  - int64 (0-1439)
:price/open           - float64
:price/high           - float64
:price/low            - float64
:price/close          - float64
```

---

## Profile Output (Before vs After)

### Before: Standard Benchmark
```
Top 10 CPU consumers:
  48%  runtime.madvise        ← Database memory setup
  40%  BadgerDB.Write         ← LSM tree writes
   5%  matchesDatom           ← Buried in noise!
   3%  chooseIndex
   2%  Iterator.Next
   2%  buildTuple
```

**Problem**: Can't optimize what you can't measure!

### After: Pre-built Database
```
Top 10 CPU consumers:
  35%  matchesDatom           ← Clear target!
  25%  Iterator.Next          ← Storage scanning
  15%  chooseIndex            ← Index selection
  10%  valuesEqual            ← Value comparison
   8%  buildTuple             ← Tuple construction
   7%  runtime.memequal
```

**Success**: 100% query execution, zero setup noise!

---

## Available Benchmarks

### Pattern Matching (5 scenarios)
```bash
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching ./datalog/storage
```

1. **UnboundAttribute_PriceTime** - Tests AEVT index scan performance
2. **UnboundAttribute_PriceOpen** - Tests AEVT index with different attribute
3. **BoundEntity_SingleBar** - Tests EAVT direct entity lookup
4. **BoundSymbol_SpecificStock** - Tests AVET value lookup
5. **LargeBindingSet_260Entities** - Tests iterator reuse with 260 entities

### Full Query (End-to-end)
```bash
go test -bench=BenchmarkPrebuiltDatabase_FullQuery ./datalog/storage
```

- **DailyOHLC_30Days** - Multi-attribute query for single symbol

---

## Common Workflows

### 1. Profile Pattern Matching
```bash
# Profile all patterns
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -cpuprofile=pattern.prof -benchtime=5s ./datalog/storage

# View CPU hotspots
go tool pprof -top pattern.prof

# View specific function
go tool pprof -list=matchesDatom pattern.prof

# Interactive browser view
go tool pprof -http=:8080 pattern.prof
```

### 2. Compare Before/After Optimization
```bash
# Baseline
go test -bench=. ./datalog/storage > baseline.txt

# Make your changes...
# (edit datalog/storage/matcher.go)

# Measure again
go test -bench=. ./datalog/storage > optimized.txt

# Compare
benchstat baseline.txt optimized.txt
```

### 3. Profile Memory Allocations
```bash
# Profile memory
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -memprofile=mem.prof -benchtime=5s ./datalog/storage

# View allocation hotspots
go tool pprof -alloc_space -top mem.prof

# View in browser
go tool pprof -http=:8080 -alloc_space mem.prof
```

---

## Key Files

### Implementation
- `datalog/storage/testdata_builder.go` - Database builder functions
- `datalog/storage/prebuilt_bench_test.go` - Benchmarks using pre-built DB

### Documentation
- `PROFILING_GUIDE.md` - Complete guide with examples
- `BADGERDB_OPTIMIZATION_ANALYSIS.md` - Optimization opportunities
- `PREBUILT_DATABASE_PROFILING.md` - This file (quick reference)

---

## Troubleshooting

### Database Not Found
```
Error: Test database not found: testdata/ohlc_benchmark.db
```
**Fix**: Run the builder:
```bash
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x
```

### Empty Profile
```
Error: No samples in profile
```
**Fix**: Run longer benchmarks:
```bash
go test -bench=BenchmarkPrebuiltDatabase -benchtime=10s -cpuprofile=cpu.prof ./datalog/storage
```

### Benchmark Too Fast
```
Warning: Benchmark ran 10000000 times
```
**Fix**: Either the operation is very fast, or you're measuring wrong thing. Check the benchmark code.

---

## Next Steps

After profiling, see `BADGERDB_OPTIMIZATION_ANALYSIS.md` for:
- Top 3 optimization opportunities
- Implementation guidance
- Expected performance gains

**Recommended starting point**: Type switch reduction in `matchesDatom()` (5-10% gain, 1 day effort)
