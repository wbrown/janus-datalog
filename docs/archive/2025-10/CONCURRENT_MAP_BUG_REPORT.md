# Concurrent Map Access Bug in Tuple Builder Cache

**Date**: October 10, 2025
**Severity**: Critical - Race Condition
**Component**: `datalog/storage/matcher.go:68` - `getTupleBuilder()`
**Triggered By**: Parallel decorrelation with concurrent test execution

---

## Bug Summary

Fatal race condition: `concurrent map read and map write` in tuple builder cache when running tests with parallel decorrelation enabled.

## Error Message

```
fatal error: concurrent map read and map write

goroutine 60082 [running]:
internal/runtime/maps.fatal({0x1031cb99c?, 0x140024412d8?})
	/opt/homebrew/Cellar/go/1.24.2/libexec/src/runtime/panic.go:1058 +0x20
github.com/wbrown/janus-datalog/datalog/storage.(*BadgerMatcher).getTupleBuilder(0x1400537f100, 0x14001d388b8, {0x1400413fb00, 0x2, 0x2})
	/Users/wbrown/go/src/github.com/wbrown/janus-datalog/datalog/storage/matcher.go:68 +0xfc
github.com/wbrown/janus-datalog/datalog/storage.(*BadgerMatcher).matchWithHashJoin(0x1400537f100, 0x14001d388b8, {0x10372f160, 0x14004158510}, {0x1400413fb00, 0x2, 0x2}, 0x0, 0x1, {0x0, ...})
	/Users/wbrown/go/src/github.com/wbrown/janus-datalog/datalog/storage/hash_join_matcher.go:132 +0x208
```

## Reproduction

### Environment
- **Janus Commit**: `0f0fda2` (pure aggregation decorrelation fix)
- **Go Version**: 1.24.2
- **Platform**: Darwin 24.6.0 (macOS)
- **Optimizations Enabled**:
  - `EnableSubqueryDecorrelation: true`
  - `EnableParallelDecorrelation: true`
  - `EnableSemanticRewriting: true`

### Reproduction Steps

1. **Setup**: gopher-street project with comprehensive test suite
2. **Configuration**: Enable all three optimizations via `OptimizedPlannerOptions()`
3. **Trigger**: Run full test suite with multiple concurrent tests
   ```bash
   cd /Users/wbrown/go/src/github.com/wbrown/gopher-street
   go test -timeout 1200s -v
   ```
4. **Result**: Fatal error after ~45 seconds when multiple parallel queries access tuple builder cache

### Key Observation

- **Individual tests**: All pass ✅
- **Sequential execution**: No issues
- **Parallel execution**: Race condition triggers after ~60,000 goroutines spawned
- **Timing**: Not a timeout issue - actual concurrent map access violation

## Root Cause Analysis

### Tuple Builder Cache (commit `ff1cecb`)

The tuple builder caching optimization (October 8, 2025) added:

```go
// Hypothetical code structure based on error location
type BadgerMatcher struct {
    tupleBuilderCache map[string]*InternedTupleBuilder  // NOT THREAD-SAFE
    // ...
}

func (m *BadgerMatcher) getTupleBuilder(pattern, columns) *InternedTupleBuilder {
    key := computeKey(pattern, columns)
    if builder, exists := m.tupleBuilderCache[key] {  // Line 68: CONCURRENT READ
        return builder
    }
    builder := createBuilder(pattern, columns)
    m.tupleBuilderCache[key] = builder  // CONCURRENT WRITE
    return builder
}
```

### Why It Fails

1. **Parallel decorrelation** spawns multiple goroutines per subquery
2. Each goroutine calls `matchWithHashJoin` → `getTupleBuilder`
3. Multiple goroutines access same `tupleBuilderCache` map concurrently
4. Go's map type is **not thread-safe** by design
5. Concurrent read/write → fatal error

### Why Individual Tests Pass

- Single test = limited parallelism
- Cache contention low enough that race doesn't trigger
- Full suite = 60,000+ goroutines = guaranteed concurrent access

## Impact Assessment

### What Works
- ✅ Sequential query execution
- ✅ Single parallel query
- ✅ Low concurrency workloads

### What Fails
- ❌ High concurrency test suites
- ❌ Production systems with many concurrent queries
- ❌ Any scenario with multiple goroutines calling `getTupleBuilder` simultaneously

### Severity
**CRITICAL** - This makes parallel decorrelation unsafe for production use with concurrent queries.

## Proposed Fix Options

### Option 1: sync.RWMutex (Recommended)

```go
type BadgerMatcher struct {
    tupleBuilderCache   map[string]*InternedTupleBuilder
    tupleBuilderCacheMu sync.RWMutex
    // ...
}

func (m *BadgerMatcher) getTupleBuilder(pattern, columns) *InternedTupleBuilder {
    key := computeKey(pattern, columns)

    // Try read lock first (fast path)
    m.tupleBuilderCacheMu.RLock()
    if builder, exists := m.tupleBuilderCache[key] {
        m.tupleBuilderCacheMu.RUnlock()
        return builder
    }
    m.tupleBuilderCacheMu.RUnlock()

    // Write lock for cache miss (slow path)
    m.tupleBuilderCacheMu.Lock()
    defer m.tupleBuilderCacheMu.Unlock()

    // Double-check after acquiring write lock
    if builder, exists := m.tupleBuilderCache[key] {
        return builder
    }

    builder := createBuilder(pattern, columns)
    m.tupleBuilderCache[key] = builder
    return builder
}
```

**Pros**: Minimal performance impact, allows concurrent reads
**Cons**: Requires careful double-check locking

### Option 2: sync.Map

```go
type BadgerMatcher struct {
    tupleBuilderCache sync.Map  // map[string]*InternedTupleBuilder
    // ...
}

func (m *BadgerMatcher) getTupleBuilder(pattern, columns) *InternedTupleBuilder {
    key := computeKey(pattern, columns)

    if builder, ok := m.tupleBuilderCache.Load(key); ok {
        return builder.(*InternedTupleBuilder)
    }

    builder := createBuilder(pattern, columns)
    actual, loaded := m.tupleBuilderCache.LoadOrStore(key, builder)
    return actual.(*InternedTupleBuilder)
}
```

**Pros**: Built-in thread safety, atomic LoadOrStore
**Cons**: Type erasure (interface{}), slightly slower than RWMutex for read-heavy workloads

### Option 3: Per-Executor Cache

Move cache from `BadgerMatcher` to `Executor` with goroutine-local access.

**Pros**: Zero contention
**Cons**: Higher memory usage, cache duplication across executors

## Recommended Solution

**Use Option 1 (sync.RWMutex)** because:

1. **Read-heavy workload**: Cache hits vastly outnumber misses after warmup
2. **RWMutex optimizes for this**: Multiple concurrent readers, rare writers
3. **Minimal overhead**: Double-check locking minimizes write lock contention
4. **Type safety**: No interface{} conversions

## Testing Strategy

### Concurrency Safety Test

```go
func TestTupleBuilderCacheConcurrency(t *testing.T) {
    matcher := NewBadgerMatcher(/* ... */)
    pattern := &Pattern{/* ... */}
    columns := []Symbol{/* ... */}

    // Spawn 1000 goroutines accessing cache concurrently
    var wg sync.WaitGroup
    for i := 0; i < 1000; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                builder := matcher.getTupleBuilder(pattern, columns)
                if builder == nil {
                    t.Error("getTupleBuilder returned nil")
                }
            }
        }()
    }

    wg.Wait()
}
```

### Race Detector

```bash
go test -race -run TestTupleBuilderCacheConcurrency
```

Should pass without errors after fix.

## Verification

After implementing fix, verify with:

```bash
# Run gopher-street full test suite with race detector
cd /Users/wbrown/go/src/github.com/wbrown/gopher-street
go test -race -timeout 1200s

# Should complete without concurrent map access errors
```

## Related Issues

- **Memory optimizations (Oct 8)**: Tuple builder caching added in commit `ff1cecb`
- **Concurrency safety (Oct 8)**: Commit `c42e9d5` added iterator concurrency fixes, but missed this cache
- **Parallel decorrelation**: Commit `1dc21a7` enabled parallel subquery execution, increasing concurrency

## Additional Notes

The optimization report (`OPTIMIZATION_REPORT_2025_10_08.md`) mentions:

> **Tuple Builder Caching** (commit `ff1cecb`) - **15% reduction**
> - Problem: Creating new `InternedTupleBuilder` for every pattern match
> - Solution: Cache builders per `(pattern, columns)` combination

The solution is correct, but the implementation missed thread-safety requirements for parallel execution.

## Urgency

**HIGH** - This blocks production deployment of parallel decorrelation optimizations.

---

**Reported by**: gopher-street team
**Reproduction**: Full test suite at `/Users/wbrown/go/src/github.com/wbrown/gopher-street`
**Contact**: File issue at https://github.com/wbrown/janus-datalog/issues
