# Test Database Integration Guide

**Problem**: Go doesn't have native test dependencies
**Solution**: Multiple integration strategies for different workflows

---

## TL;DR - It Just Works

```bash
# First run - auto-builds database
go test ./datalog/storage

# Output:
# 📦 Test database not found, building it now...
#    (This is a one-time setup, will be cached)
#
# Writing 50400 datoms to testdata/ohlc_benchmark.db...
# ✅ Test database built successfully!
#
# Running tests...

# Subsequent runs - uses cached database
go test ./datalog/storage

# Output:
# Running tests...
```

**The database is automatically built once and cached.**

---

## How It Works

### TestMain Integration

`datalog/storage/testdata_main_test.go`:
```go
func TestMain(m *testing.M) {
    // Check if test database exists
    dbPath := "testdata/ohlc_benchmark.db"
    if _, err := os.Stat(dbPath); os.IsNotExist(err) {
        // Build it automatically
        fmt.Println("📦 Building test database...")
        BuildTestDatabase(DefaultOHLCConfig())
    }

    // Run tests
    os.Exit(m.Run())
}
```

**Behavior**:
- ✅ Runs automatically before any test in the package
- ✅ Checks if database exists
- ✅ Builds it once if missing
- ✅ Cached for all subsequent runs
- ✅ No manual intervention needed

---

## Integration Strategies

### Strategy 1: Pure Go (Automatic) ✅

**Use Case**: Just want tests to work

```bash
# Clone repo
git clone https://github.com/wbrown/janus-datalog

# Run tests - database auto-builds on first run
go test ./datalog/storage
```

**Pros**:
- ✅ Zero manual steps
- ✅ Works with `go test`
- ✅ Works in CI/CD automatically
- ✅ Most Go-idiomatic

**Cons**:
- ⚠️ First test run is slow (10s build time)
- ⚠️ Build happens during test run

### Strategy 2: Makefile (Explicit) 📋

**Use Case**: Want explicit control over build steps

```bash
# Build database first
make build-testdb

# Run tests (uses cached DB)
make test

# Or do both in one command
make test  # auto-builds if missing
```

**Pros**:
- ✅ Explicit control
- ✅ Common pattern in Go projects
- ✅ Easy to integrate in CI/CD
- ✅ Shows build status clearly

**Cons**:
- ⚠️ Requires make installed
- ⚠️ Extra command to learn

### Strategy 3: Manual Build 🔧

**Use Case**: Want full control

```bash
# Build database manually
go run cmd/build-testdb/main.go

# Run tests
go test ./datalog/storage
```

**Pros**:
- ✅ Complete control
- ✅ Can choose database size
- ✅ Can rebuild anytime

**Cons**:
- ⚠️ Must remember to build first
- ⚠️ Not automatic

---

## Recommended Workflows

### For Local Development (Automatic)

```bash
# Just run tests - it auto-builds database once
go test ./datalog/storage

# Run specific benchmark
go test -bench=BenchmarkPrebuilt ./datalog/storage

# Profile with clean output
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -cpuprofile=cpu.prof \
    ./datalog/storage
```

**First run**: 10s (builds DB) + test time
**Subsequent runs**: Just test time
**No manual steps needed!**

### For CI/CD (Makefile)

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      # Option 1: Use Makefile
      - name: Run tests
        run: make test

      # Option 2: Let TestMain handle it
      - name: Run tests
        run: go test ./...

      # Option 3: Cache database between runs
      - name: Cache test database
        uses: actions/cache@v3
        with:
          path: datalog/storage/testdata
          key: testdb-${{ hashFiles('datalog/storage/testdata_builder.go') }}

      - name: Run tests
        run: go test ./...
```

### For Profiling (Makefile)

```bash
# Quick profile
make profile

# Or manually
go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
    -cpuprofile=cpu.prof \
    -benchtime=5s \
    ./datalog/storage

# View results
go tool pprof -http=:8080 cpu.prof
```

---

## Database Management

### Check Database Status

```bash
# Check if database exists
ls -lh datalog/storage/testdata/ohlc_benchmark.db

# See database info
go test -run=^TestBuildDatabase$ -v ./datalog/storage
```

### Rebuild Database

```bash
# Option 1: Remove and let TestMain rebuild
rm -rf datalog/storage/testdata/ohlc_benchmark.db
go test ./datalog/storage

# Option 2: Force rebuild with Makefile
make build-testdb-force

# Option 3: Manual rebuild
go run cmd/build-testdb/main.go -config=default
```

### Clean Database

```bash
# Option 1: Makefile
make clean-testdb

# Option 2: Manual
rm -rf datalog/storage/testdata/*.db

# Option 3: Go clean (clears test cache too)
go clean -testcache
rm -rf datalog/storage/testdata/*.db
```

---

## Different Database Sizes

### Default (8 MB) - Auto-built

```bash
# This is what TestMain builds automatically
go test ./datalog/storage
```

### Medium (40 MB) - Manual

```bash
# Build medium database
go run cmd/build-testdb/main.go -config=medium

# Update benchmarks to use it
db, _ := OpenTestDatabase("testdata/ohlc_medium.db")
```

### Large (50 GB) - Explicit Only

```bash
# Only for stress testing - NEVER auto-built
go run cmd/build-testdb/main.go -config=large
```

---

## Gitignore Integration

Add to `.gitignore`:
```gitignore
# Test databases (generated, don't commit)
datalog/storage/testdata/*.db

# Profiles (generated)
*.prof
cpu.prof
mem.prof
```

**Why not commit the database?**
- ❌ 8 MB binary blob in git
- ❌ Can't review changes
- ✅ Easy to rebuild (10 seconds)
- ✅ Keeps repo small

---

## Troubleshooting

### "Test database not found"

**Symptom**:
```
--- SKIP: BenchmarkPrebuiltDatabase (0.00s)
    prebuilt_bench_test.go:10: Test database not found
```

**Cause**: Database wasn't auto-built (TestMain didn't run)

**Fix**:
```bash
# Let TestMain build it
go test ./datalog/storage

# Or build manually
make build-testdb
```

### "Database is locked"

**Symptom**:
```
Error: resource temporarily unavailable
```

**Cause**: Another process is using the database

**Fix**:
```bash
# Find process
lsof datalog/storage/testdata/ohlc_benchmark.db

# Kill it or wait for it to finish
# Then rebuild
make build-testdb-force
```

### "Stale database"

**Symptom**: Tests fail with unexpected data

**Cause**: Database schema changed but old database still exists

**Fix**:
```bash
# Rebuild database
make build-testdb-force

# Or remove and let auto-rebuild
rm -rf datalog/storage/testdata/ohlc_benchmark.db
go test ./datalog/storage
```

---

## FAQ

### Q: Do I need to run a build command before tests?

**A**: No! `TestMain` automatically builds the database on first run.

### Q: Will every test run rebuild the database?

**A**: No. It's built once and cached. Subsequent runs reuse it.

### Q: How do I know if the database is cached?

**A**:
```bash
# If this exists, it's cached
ls datalog/storage/testdata/ohlc_benchmark.db
```

### Q: Can I use different database sizes?

**A**: Yes, but you need to:
1. Build the size you want: `go run cmd/build-testdb/main.go -config=medium`
2. Update benchmark to use it: `OpenTestDatabase("testdata/ohlc_medium.db")`

### Q: Does this work in CI/CD?

**A**: Yes! TestMain runs automatically in CI. Optionally cache the database to speed up builds.

### Q: How much disk space do I need?

**A**:
- Default: ~20 MB (database + profiles)
- Medium: ~60 MB
- Large: ~51 GB (only for stress testing)

### Q: Can I skip building the database?

**A**: Yes, use `-short` flag:
```bash
go test -short ./datalog/storage
```

Benchmarks that need the database will skip with a message.

---

## Comparison to Other Go Projects

### How Other Projects Handle Test Fixtures

| Project | Approach | Pros | Cons |
|---------|----------|------|------|
| **Kubernetes** | Checked-in testdata | ✅ No build step | ❌ Large repo size |
| **Docker** | Makefile targets | ✅ Explicit | ❌ Requires make |
| **etcd** | TestMain setup | ✅ Automatic | ⚠️ First run slow |
| **Janus** | TestMain + Makefile | ✅ Both options | - |

**We combine the best of both**: Automatic with TestMain, explicit with Makefile.

---

## Summary

### The Magic of TestMain

```go
func TestMain(m *testing.M) {
    // Runs ONCE before all tests in package
    if !databaseExists() {
        buildDatabase()  // ← Auto-builds
    }
    os.Exit(m.Run())
}
```

**Result**: Tests "just work" with no manual steps!

### Available Commands

| Command | What It Does | When To Use |
|---------|--------------|-------------|
| `go test ./datalog/storage` | Run tests (auto-build DB) | ✅ Always |
| `make test` | Explicit build + test | CI/CD |
| `make profile` | Profile with pre-built DB | Optimization |
| `make clean-testdb` | Remove database | Fresh start |

### Bottom Line

**You don't need to do anything special!**

```bash
# This just works:
go test ./datalog/storage
```

The database auto-builds on first run and is cached for all subsequent runs. No manual steps, no build commands, no makefile required (but available if you want it).

**Go-idiomatic and automatic.** ✅
