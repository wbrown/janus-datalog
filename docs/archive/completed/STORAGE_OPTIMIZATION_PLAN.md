# Storage Layer Optimization Plan

## Current Issues

### 1. BadgerDB Default Configuration
- Using `badger.DefaultOptions` with no tuning
- No bloom filter configuration
- No block cache configuration
- No memory table tuning

### 2. Iterator Configuration
- `PrefetchSize = 10` - Way too small for scanning 124k datoms
- Should be 100-1000 for bulk scans
- Currently prefetching only 10 items at a time

### 3. Performance Bottlenecks
- 132ms to scan 124k datoms = ~940k datoms/sec
- Expected: >5M datoms/sec
- 5x slower than expected

## Optimization Opportunities

### 1. BadgerDB Options

```go
opts := badger.DefaultOptions(path)
opts.Logger = nil

// Memory optimizations
opts.MemTableSize = 128 << 20          // 128MB (default is 64MB)
opts.NumMemtables = 5                  // More memtables before stalling
opts.NumLevelZeroTables = 5            
opts.NumLevelZeroTablesStall = 10

// Read optimizations
opts.BloomFalsePositive = 0.01         // Keep default bloom filter
opts.IndexCacheSize = 100 << 20        // 100MB index cache (0 = all in memory)
opts.BlockCacheSize = 256 << 20        // 256MB block cache
opts.MaxLevels = 7                     // Default LSM levels

// Disable unnecessary features for read-heavy workload
opts.DetectConflicts = false           // We handle conflicts at app level

// SSD optimizations
opts.SyncWrites = false                // Async writes for better performance
opts.NumCompactors = 4                 // Parallel compaction

// Value log optimization
opts.ValueThreshold = 1 << 10          // 1KB - store smaller values in LSM tree
```

### 2. Iterator Options

```go
// For bulk scans (like counting all datoms)
opts := badger.DefaultIteratorOptions
opts.PrefetchSize = 1000               // Prefetch 1000 items (was 10)
opts.PrefetchValues = true             // We need values

// For key-only operations (if we add count optimization)
keyOnlyOpts := badger.DefaultIteratorOptions
keyOnlyOpts.PrefetchValues = false     // Much faster for counting
```

### 3. Scan Optimization Strategies

#### A. Parallel Scanning
Split EAVT index into ranges and scan in parallel:
```go
// Split key space into N ranges
// Scan each range in a goroutine
// Combine results
```

#### B. Statistics Cache
Cache common aggregates:
- Total datom count
- Count per attribute
- Selectivity estimates

#### C. Streaming Aggregation
For count queries, we don't need to materialize all datoms:
```go
count := 0
it := store.Scan(EAVT, start, end)
for it.Next() {
    count++
    // Don't call Datom() - avoid deserialization
}
```

## Implementation Plan

### Phase 1: Quick Wins (Immediate)
1. Increase `PrefetchSize` from 10 to 1000
2. Add BadgerDB memory options (MemTableSize, BlockCache)
3. Disable conflict detection

### Phase 2: Configuration (1 day)
1. Create `StorageOptions` struct for tuning
2. Add environment-based configuration
3. Profile different configurations

### Phase 3: Advanced (1 week)
1. Implement parallel scanning
2. Add statistics tracking
3. Optimize specific query patterns

## Expected Impact

### Conservative Estimates
- PrefetchSize 10 → 1000: **2-5x speedup**
- Block cache: **1.5-2x speedup**
- Combined: **3-10x speedup**

### Target Performance
- Current: 132ms for 124k datoms
- Target: <20ms for 124k datoms
- Speedup needed: 6.6x

## Testing Strategy

1. Benchmark current performance
2. Apply each optimization individually
3. Measure combined impact
4. Test with different data sizes

## Risks and Mitigations

### Memory Usage
- Risk: Increased memory from caches
- Mitigation: Make configurable, monitor usage

### Write Performance
- Risk: Larger memtables might slow writes
- Mitigation: This is read-heavy workload

### Compatibility
- Risk: BadgerDB v4 API changes
- Mitigation: Pin version, test thoroughly

## Alternative: Consider Other Storage Engines

If BadgerDB can't meet performance needs:
1. **RocksDB**: Industry standard, great performance
2. **Pebble**: CockroachDB's fork of RocksDB
3. **Custom B+Tree**: For specific access patterns
4. **Memory-mapped files**: For read-only datasets

## Conclusion

The storage layer has significant optimization potential. The combination of:
- Proper BadgerDB configuration
- Larger prefetch sizes
- Block and index caches

Should provide the 6.6x speedup needed to reach <20ms query times.