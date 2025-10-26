# Test Database Sizes

## Available Configurations

### Default (Recommended for Profiling)
```bash
go run cmd/build-testdb/main.go -config=default
```

**Specs**:
- **Symbols**: 10 stocks (TICK0000-TICK0009)
- **Duration**: 30 days
- **Frequency**: 24 hours/day (hourly bars)
- **Total Bars**: 7,200
- **Total Datoms**: 50,400 (7 attributes per bar)
- **Estimated Size**: ~8 MB
- **Build Time**: ~10 seconds

**Use For**:
- ✅ Daily development profiling
- ✅ Quick iteration on optimizations
- ✅ CI/CD benchmarks
- ✅ Pattern matching benchmarks

### Medium (Production-like)
```bash
go run cmd/build-testdb/main.go -config=medium
```

**Specs**:
- **Symbols**: 50 stocks (TICK0000-TICK0049)
- **Duration**: 30 days
- **Frequency**: 24 hours/day (hourly bars)
- **Total Bars**: 36,000
- **Total Datoms**: 252,000
- **Estimated Size**: ~40 MB
- **Build Time**: ~1 minute

**Use For**:
- ✅ Realistic query performance testing
- ✅ Index selection benchmarks
- ✅ Join performance analysis
- ✅ Memory profiling under load

### Large (Stress Testing)
```bash
go run cmd/build-testdb/main.go -config=large
```

**Specs**:
- **Symbols**: 500 stocks (TICK0000-TICK0499)
- **Duration**: 365 days
- **Frequency**: 390 minutes/day (6.5 hour trading day)
- **Total Bars**: 71,175,000
- **Total Datoms**: ~498 million
- **Estimated Size**: ~50 GB
- **Build Time**: ~5-10 minutes

**Use For**:
- ✅ Stress testing
- ✅ LSM tree performance
- ✅ Large dataset optimizations
- ❌ NOT for regular profiling (too slow)

---

## Database Structure

Each bar has **7 datoms**:

| Attribute | Type | Example |
|-----------|------|---------|
| `:price/symbol` | Identity | `TICK0001` |
| `:price/time` | time.Time | `2025-06-01 09:00:00` |
| `:price/minute-of-day` | int64 | `540` (9:00 AM) |
| `:price/open` | float64 | `102.34` |
| `:price/high` | float64 | `104.56` |
| `:price/low` | float64 | `100.12` |
| `:price/close` | float64 | `103.45` |

**Entity Example**:
```
bar1: {
  :price/symbol #TICK0001,
  :price/time   #inst "2025-06-01T09:00:00Z",
  :price/minute-of-day 540,
  :price/open 102.34,
  :price/high 104.56,
  :price/low 100.12,
  :price/close 103.45
}
```

---

## Storage Breakdown

### Per-Datom Storage Cost

In BadgerDB with L85 encoding:

```
Key size:
  - Index prefix: 1 byte
  - Entity (E): 25 bytes (L85 encoded SHA1)
  - Attribute (A): 40 bytes (L85 encoded keyword hash)
  - Value (V): Variable (2-50 bytes depending on type)
  - Transaction (Tx): 25 bytes
  Total key: ~91-141 bytes

Value size:
  - Type tag: 1 byte
  - Data: Variable (8 bytes for float64, 20+ for time.Time)
  Total value: ~9-30 bytes

Metadata (BadgerDB overhead):
  - LSM tree pointers, timestamps, etc: ~20-30 bytes

Total per datom: ~120-200 bytes (average ~150 bytes)
```

### Database Size Calculation

| Config | Datoms | Size Formula | Estimated Size |
|--------|--------|--------------|----------------|
| **Default** | 50,400 | 50,400 × 150 bytes | **~7.6 MB** |
| **Medium** | 252,000 | 252,000 × 150 bytes | **~38 MB** |
| **Large** | 498M | 498M × 150 bytes | **~75 GB** |

**Note**: Actual size may vary ±20% due to:
- BadgerDB LSM tree compaction
- Index overhead (5 indices: EAVT, AEVT, AVET, VAET, TAEV)
- Value type distribution (time.Time is larger than int64)

---

## Quick Reference

| Need | Config | Size | Build Time |
|------|--------|------|------------|
| Fast profiling | `default` | 8 MB | 10s |
| Realistic queries | `medium` | 40 MB | 1m |
| Stress testing | `large` | 50 GB | 10m |

**Recommendation**: Start with `default`, use `medium` for final validation.

---

## Build Commands

### Quick Start (Default)
```bash
# Option 1: Build command
go run cmd/build-testdb/main.go

# Option 2: Test helper
go test -run=^TestBuildDatabase$ ./datalog/storage

# Verify it was created
ls -lh datalog/storage/testdata/ohlc_benchmark.db
```

### All Configs
```bash
# Default (8 MB, 10s)
go run cmd/build-testdb/main.go -config=default

# Medium (40 MB, 1m)
go run cmd/build-testdb/main.go -config=medium

# Large (50 GB, 10m) - only for stress testing!
go run cmd/build-testdb/main.go -config=large
```

---

## Disk Space Requirements

| Config | Database | Profiles | Total | Recommendation |
|--------|----------|----------|-------|----------------|
| **Default** | 8 MB | 5-10 MB | ~20 MB | ✅ Always safe |
| **Medium** | 40 MB | 10-20 MB | ~60 MB | ✅ Still reasonable |
| **Large** | 50 GB | 100-500 MB | ~51 GB | ⚠️ Ensure free space |

**Profile Storage**: CPU and memory profiles can be 10-100 MB each depending on benchmark duration.

---

## Integration with Benchmarks

### Using Pre-built Database

```go
func BenchmarkMyQuery(b *testing.B) {
    // Open pre-built database
    db, err := storage.OpenTestDatabase("testdata/ohlc_benchmark.db")
    if err != nil {
        b.Skipf("Database not found: %v", err)
        return
    }
    defer db.Close()

    // Your benchmark code...
    matcher := storage.NewBadgerMatcher(db.store)

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        result, _ := matcher.Match(pattern, bindings)
        _ = result.Size()
    }
}
```

### Benchmark Will Skip If Not Built

If database doesn't exist:
```
--- SKIP: BenchmarkMyQuery (0.00s)
    my_bench_test.go:10: Database not found: stat testdata/ohlc_benchmark.db: no such file or directory
```

**Solution**: Build the database first:
```bash
go run cmd/build-testdb/main.go
```

---

## Summary

**For Daily Development**:
- Use **default** (8 MB)
- Builds in 10 seconds
- Good enough for profiling pattern matching

**For Production Validation**:
- Use **medium** (40 MB)
- Builds in 1 minute
- Representative of real workloads

**For Stress Testing**:
- Use **large** (50 GB)
- Builds in 10 minutes
- Only when testing scale limits

**Bottom Line**: Start with `default`, it's fast and sufficient for 95% of profiling needs.
