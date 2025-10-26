# Benchmark Migration Guide

**Problem**: Existing benchmarks create databases on every run, polluting CPU profiles

**Solution**: Migrate key benchmarks to use pre-built test database

---

## Current Benchmark Issues

### Before (Existing Benchmarks)
```go
func BenchmarkAVETReuse(b *testing.B) {
    db, _ := NewDatabase(dbPath)           // ← Creates DB

    // Write 175,500 datoms                // ← 10+ seconds
    for /* ... */ {
        tx.Add(...)
    }

    b.ResetTimer()                         // ← Only excludes from timing!

    for i := 0; i < b.N; i++ {
        matcher.Match(pattern, bindings)   // ← Query we want to profile
    }
}
```

**Profile shows**: 90% database creation, 10% queries

### After (Pre-built Database)
```go
func BenchmarkPrebuilt_AVET(b *testing.B) {
    db, _ := OpenTestDatabase("testdata/ohlc_benchmark.db")  // ← Opens existing DB
    defer db.Close()

    matcher := NewBadgerMatcher(db.store)

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        matcher.Match(pattern, bindings)   // ← Query we want to profile
    }
}
```

**Profile shows**: 100% query execution

---

## Migration Steps

### Step 1: Identify Benchmarks to Migrate

**High Priority** (used for profiling):
- `BenchmarkAVETReuse` → Migrate
- `BenchmarkIteratorReuseClean` → Migrate
- Any benchmark you've profiled with `-cpuprofile`

**Low Priority** (correctness tests):
- `BenchmarkBatchScan*` → Keep as-is
- `BenchmarkKeyMask*` → Keep as-is
- Unit tests → Keep as-is

### Step 2: Create Migrated Benchmark

**Template**:
```go
// OLD: BenchmarkAVETReuse (creates DB)
func BenchmarkAVETReuse(b *testing.B) {
    // ... database creation code ...
    b.ResetTimer()
    // ... benchmark code ...
}

// NEW: BenchmarkPrebuilt_AVETReuse (uses pre-built DB)
func BenchmarkPrebuilt_AVETReuse(b *testing.B) {
    // Open pre-built database
    db, err := OpenTestDatabase("testdata/ohlc_benchmark.db")
    if err != nil {
        b.Skipf("Test database not found: %v", err)
        return
    }
    defer db.Close()

    // Setup pattern and bindings
    pattern := &query.DataPattern{
        Elements: []query.PatternElement{
            query.Variable{Name: "?b"},
            query.Constant{Value: datalog.NewKeyword("price/symbol")},
            query.Variable{Name: "?s"},
        },
    }

    // Create bindings (adjust to match pre-built DB structure)
    var tuples []executor.Tuple
    for i := 0; i < 5; i++ {
        symbolEntity := datalog.NewIdentity(fmt.Sprintf("TICK%04d", i))
        tuples = append(tuples, executor.Tuple{symbolEntity})
    }
    bindings := executor.Relations{
        executor.NewMaterializedRelation([]query.Symbol{"?s"}, tuples),
    }

    matcher := NewBadgerMatcher(db.store)

    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        result, err := matcher.Match(pattern, bindings)
        if err != nil {
            b.Fatalf("Match failed: %v", err)
        }
        if result.Size() == 0 {
            b.Fatal("Expected non-empty result")
        }
    }
}
```

### Step 3: Test Migration

```bash
# First, ensure test database exists
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x

# Run migrated benchmark
go test -bench=BenchmarkPrebuilt_AVETReuse ./datalog/storage

# Profile with clean output
go test -bench=BenchmarkPrebuilt_AVETReuse \
    -cpuprofile=cpu.prof -benchtime=3s ./datalog/storage
```

### Step 4: Compare Results

```bash
# Old benchmark (with DB creation)
go test -bench=BenchmarkAVETReuse -benchtime=1s > old.txt

# New benchmark (pre-built DB)
go test -bench=BenchmarkPrebuilt_AVETReuse -benchtime=1s > new.txt

# Compare timing (should be similar)
benchstat old.txt new.txt
```

---

## Key Differences

### Database Structure Mapping

**Old benchmarks** created custom data:
```go
// Old: Custom symbols
symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"}
for _, ticker := range symbols {
    symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
    // ...
}
```

**Pre-built database** has standardized structure:
```go
// Pre-built: TICK0000 through TICK0099
for i := 0; i < 100; i++ {
    symbol := fmt.Sprintf("TICK%04d", i)
    symbolEntity := datalog.NewIdentity(symbol)
    // ...
}
```

**When migrating**: Update entity references to match pre-built DB:
```go
// OLD
symbolEntity := datalog.NewIdentity("symbol:AAPL")

// NEW
symbolEntity := datalog.NewIdentity("TICK0000")  // First symbol in pre-built DB
```

### Attribute Names

**Pre-built database** uses:
- `:price/symbol` - Reference to symbol
- `:price/time` - time.Time timestamp
- `:price/minute-of-day` - int64 (0-1439)
- `:price/open`, `:price/high`, `:price/low`, `:price/close` - float64

Ensure your migrated benchmarks use these attribute names.

---

## Migration Checklist

### For Each Benchmark to Migrate:

- [ ] Copy benchmark function with `Prebuilt_` prefix
- [ ] Replace `NewDatabase(...)` with `OpenTestDatabase(...)`
- [ ] Update entity references (e.g., `"symbol:AAPL"` → `"TICK0000"`)
- [ ] Verify attribute names match pre-built DB
- [ ] Add skip message if database not found
- [ ] Test benchmark runs successfully
- [ ] Profile to verify clean output (no DB creation)
- [ ] Compare timing with original (should be similar)
- [ ] Document in benchmark comment which DB it uses

---

## Example Migrations

### 1. Iterator Reuse Benchmark

**Before** (`iterator_clean_bench_test.go`):
```go
func BenchmarkIteratorReuseClean(b *testing.B) {
    tempDir := b.TempDir()
    db, _ := NewDatabase(tempDir)
    defer db.Close()

    // Create 10 symbols with 200 bars each
    symbols := make([]string, 10)
    for i := 0; i < 10; i++ {
        symbols[i] = fmt.Sprintf("SYM%02d", i)
    }

    tx := db.NewTransaction()
    for _, ticker := range symbols {
        symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
        tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), ticker)

        for i := 0; i < 200; i++ {
            barEntity := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d", ticker, i))
            tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
            tx.Add(barEntity, datalog.NewKeyword(":price/value"), float64(100.0+float64(i)))
        }
    }
    tx.Commit()

    // ... benchmark code ...
}
```

**After**:
```go
func BenchmarkPrebuilt_IteratorReuse(b *testing.B) {
    db, err := OpenTestDatabase("testdata/ohlc_benchmark.db")
    if err != nil {
        b.Skipf("Test database not found: %v", err)
        return
    }
    defer db.Close()

    // Pre-built DB has 100 symbols (TICK0000-TICK0099)
    // Use first 10 symbols
    symbols := make([]datalog.Identity, 10)
    for i := 0; i < 10; i++ {
        symbols[i] = datalog.NewIdentity(fmt.Sprintf("TICK%04d", i))
    }

    // Query: [?b :price/symbol ?s]
    // Pre-built DB has ~720 bars per symbol (30 days × 24 hours)

    // ... benchmark code ...
}
```

---

## When NOT to Migrate

Keep original benchmarks if:

1. **Testing different data sizes** - Pre-built DB is fixed size
2. **Unit tests** - Should be self-contained
3. **Correctness tests** - Need specific edge cases
4. **CI/CD tests** - Shouldn't depend on external files

Only migrate benchmarks that you actively profile for performance analysis.

---

## Benefits After Migration

### Clean Profiles
```
Top 10 CPU consumers:
  35%  matchesDatom           ← Clear signal!
  25%  Iterator.Next
  15%  chooseIndex
  10%  valuesEqual
   8%  buildTuple
   7%  runtime.memequal
```

### Faster Benchmark Runs
```
Before:
  BenchmarkAVETReuse-8     10    112.5ms/op    (10s setup + 2.5ms query)

After:
  BenchmarkPrebuilt_AVETReuse-8    500    2.5ms/op    (0s setup + 2.5ms query)
```

### Better Development Workflow
```bash
# One-time database build
go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ -benchtime=1x ./datalog/storage

# Rapid iteration on profiling
go test -bench=BenchmarkPrebuilt_* -cpuprofile=cpu.prof ./datalog/storage
go tool pprof cpu.prof

# Make changes...

# Re-profile (no DB rebuild!)
go test -bench=BenchmarkPrebuilt_* -cpuprofile=cpu2.prof ./datalog/storage
go tool pprof -base=cpu.prof cpu2.prof
```

---

## Summary

**Migration Decision Tree**:

```
Do you profile this benchmark?
├─ YES → Migrate to use pre-built DB
│         (Create BenchmarkPrebuilt_* version)
│
└─ NO → Keep original
         (Self-contained correctness test)
```

**Files to Create**:
- Keep: `*_test.go` (original benchmarks)
- Add: `prebuilt_*_bench_test.go` (migrated benchmarks)

**Result**: Clean profiles showing 100% query execution, not database creation.
