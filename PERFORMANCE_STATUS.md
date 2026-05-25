# PERFORMANCE_STATUS.md

**Last Updated**: 2026-02-06
**Version**: Clause-based planner, QueryExecutor, streaming architecture, Pull API, schema support, key encoder optimization, conditional aggregate rewriting, CRDT storage, allocation regression fixes, and value elimination

## Executive Summary

The Janus Datalog engine delivers production-ready performance through architectural improvements and targeted optimizations. All performance claims in this document are verified by actual benchmarks (last updated 2025-12-24).

### Verified Performance Improvements
- ✅ **New architecture** (clause-based planner + QueryExecutor): **2× faster** on complex OHLC queries (verified)
- ✅ **Pull API**: **9× faster than equivalent queries**, linear scaling (verified 2025-12-17)
- ✅ **Schema validation**: **<1% overhead** for type checking, **~6% overhead** for uniqueness (verified 2025-12-17)
- ✅ **Iterator composition**: **4.06× speedup** (1,259μs → 310μs, 89% memory reduction) (verified 2025-10-25)
- ✅ **Streaming execution**: **2.22× faster** with low-selectivity filters (1,720ms → 774ms), 52% memory reduction (verified 2025-10-25)
- ✅ **Parallel subquery execution**: **2.06× speedup** with 8 workers on M3 Max (730ms → 355ms) (verified 2025-10-25)
- ✅ **Time-based queries**: 2-6× faster with semantic rewriting (verified)
- ✅ **Predicate pushdown**: **1.58-2.78× faster** depending on dataset size, up to 91.5% memory reduction (verified 2025-10-25)
- ✅ **Intern cache optimization**: 6.26× speedup on BadgerDB queries (verified)
- ✅ **Time range optimization**: 4× speedup on large datasets (verified - 1.5× on small, 4× on 260-hour dataset)
- ✅ **Hash join pre-sizing**: 24-32% faster with 24-30% less memory (verified)
- ✅ **Identity/Keyword interning**: **10-20% faster** on joins and subqueries, **25-44% memory reduction**, pointer equality for all comparisons (verified 2025-12-24)
- ✅ **Conditional aggregate rewriting**: **7.7× faster**, **5.2× less memory**, **8.1× fewer allocations** for correlated aggregate subqueries (verified 2026-01-16)
- ✅ **CRDT storage**: **~25-35µs writes** across all cardinalities, **O(1) LWW resolution** (965ns for 1000 versions), linear vector scaling (verified 2026-01-31)
- ✅ **CRDT allocation optimization**: **90% faster** (1.9×), **2.2× less memory** than pre-CRDT main branch while adding full CRDT semantics (verified 2026-02-02)
- ✅ **AETV index & value elimination**: **5% faster**, **19% less memory**, **17% fewer allocations** (geomean); complex queries see **35% memory reduction** (verified 2026-02-06)
- ✅ **LZ77+FSE compression codec**: **2.1-2.4 GB/s decompression** (7 allocs), **3.6x on prose**, **10-13x on structured/repetitive** data (verified 2026-03-28)

### Claims Requiring Qualification
- ⚠️ **Plan quality**: "13% better plans" not supported by current benchmarks (planners perform identically)
- ⚠️ **In-memory indexing**: "49-4802×" not reproducible (optimizations became pervasive, both paths now fast)

---

## What's Actually Working ✅

### 0. Clause-Based Planner + QueryExecutor (ACTIVE - ARCHITECTURAL WIN)
**Status**: ✅ Production-ready and default architecture
**Performance**: **2× faster on complex queries** (verified Oct 2025)
**Location**: `datalog/planner/planner_clause_based.go`, `datalog/executor/query_executor.go`

**Measured Results**:
- Full architecture comparison (OHLC queries): ~4-8s (old) → ~2-4s (new) = **2× faster** ✅
- Plan quality isolated (same executor): Both planners perform identically within measurement noise
- Planning overhead: 3-12µs (old) → 1-7µs (new) = 37-88% faster (but negligible impact)

**Key Difference**: The 2× speedup comes from QueryExecutor's clause-by-clause streaming execution, not from better plan quality. Both planners produce equivalent-quality plans.

**Configuration**: Enabled by default. The `db.Open` API uses these defaults automatically:
```go
// Public API — uses default planner/executor configuration
d, _ := db.Open("path/to/db")
d.Query(`[:find ?e ?v :where [?e :price/close ?v]]`)

// Advanced: direct executor construction for non-default options
opts := storage.DefaultPlannerOptions()
exec := executor.NewExecutorWithOptions(matcher, opts)
```

**Details**: See `docs/archive/completed/PLANNER_COMPARISON.md` (archived: one planner now)

### 1. Query Plan Caching (ACTIVE)
**Status**: ✅ Implemented and enabled by default
**Location**: `datalog/planner/cache.go`, `datalog/storage/database.go:34`
**Performance**: ~3× speedup for repeated queries (measured)

### 2. Batch Scanning with Iterator Reuse (ACTIVE)
**Status**: ✅ Implemented, used for large binding sets
**Location**: `datalog/storage/matcher_relations.go:122-128`
**Threshold**: Activated when `bindingRel.Size() > 100`
**Result**: Code clarity improvement, minimal performance impact

### 3. Predicate Classification (ACTIVE)
**Status**: ✅ Infrastructure in place, used by executor
**Location**: `datalog/executor/predicate_classifier.go`
**What it does**: Classifies predicates as pushable vs. non-pushable

### 4. Join Condition Detection (ACTIVE)
**Status**: ✅ Implemented and used
**Location**: `datalog/executor/join_conditions.go`
**What it does**: Detects equality predicates that can be pushed into joins

### 5. Progressive Join Execution (CRITICAL & ACTIVE)
**Status**: ✅ Core safeguard preventing memory explosion
**Location**: `datalog/executor/relation.go`
**Why it matters**: Greedy join ordering + early termination prevents catastrophic intermediate result sizes
**Scale achieved**: Production-ready for datasets from 100K to 10M+ datoms, tested up to 500M+

---

## Recent Optimizations (October 2025) ✅

### 0. Single-Use Iterator Semantics & Streaming (COMPLETE - CORRECTNESS FIX)
**Status**: ✅ Proper iterator lifecycle management with single-use semantics
**Performance**: **4.06× speedup for iterator composition**, **2.22× for streaming** (verified 2025-10-25)
**Commits**: 626e409 (latest), 4a394cb, 4f3b742, 15d196d, 78c930a

**What We Fixed** (2025-10-25):
- ✅ **Single-use iterator semantics** - StreamingRelation enforces one-time iteration
- ✅ **BufferedIterator** - Safe re-iteration support with automatic caching
- ✅ **Iterator lifecycle** - Clear separation between first use (streaming) and re-use (cached)
- ✅ **Correctness** - All tests pass with proper semantics enforcement

**What We Built Earlier** (Oct 2025):
- ✅ **Iterator composition** - Filter/Project/Transform operations stay lazy
- ✅ **Options propagation** - ExecutorOptions flow through entire pipeline
- ✅ **BadgerMatcher streaming** - Returns StreamingRelation instead of materializing
- ✅ **Symmetric hash join** - Streaming-to-streaming joins without materialization

**Current Performance Results** (verified 2025-10-25):

**Iterator Composition Benchmark**:
- Materialized: 1,259 μs, 3.27 MB, 25K allocs
- Composed: 310 μs, 360 KB, 15K allocs
- **Result: 4.06× faster, 89% memory reduction** ✅

**Streaming Scenarios** (10K tuples):
- Large_HighSelectivity (1% pass): 1.07× faster (675μs → 630μs), 2% memory reduction
- Large_MediumSelectivity (10% pass): 1.44× faster (975μs → 676μs), 19% memory reduction
- Large_LowSelectivity (50% pass): **2.22× faster** (1,720μs → 774μs), **52% memory reduction**
- **Key Finding**: Benefits scale with filter selectivity (1.07× to 2.22× depending on selectivity)

**Configuration**: Enabled by default:
```go
EnableIteratorComposition: true  // Lazy evaluation (default)
EnableTrueStreaming: true        // No auto-materialization (default)
```

### 1. In-Memory Indexing (COMPLETE - PERVASIVE OPTIMIZATION)
**Status**: ✅ IndexedMemoryMatcher with hash indices
**Performance**: Hash indices now used throughout (test suite 7s, down from timeouts)
**Commit**: Latest

**What Works**:
- ✅ Hash indices for E/A/V lookups (entityIndex, attributeIndex, valueIndex, eavIndex)
- ✅ Thread-safe lazy initialization with sync.Once
- ✅ Smart index selection (EA > E > A > V > linear)
- ✅ Two-phase value lookup (hash → exact match) for interface{} types

**Historical Note**:
During development, benchmarks showed dramatic speedups (49-4802×) comparing linear scan vs indexed lookups. However, subsequent refactoring made hash indices the default path for both IndexedMemoryMatcher and MemoryPatternMatcher. Current benchmarks show identical performance because both implementations now use the optimized path.

**Impact**: Entity lookups are O(1) instead of O(N). Test suite execution time dramatically reduced. This is a **success story** - the optimization became so pervasive that there's no longer a "slow path" to compare against.

**Details**: See `IN_MEMORY_INDEX_RESULTS.md` (historical benchmarks)

### 2. Time Range Optimization (COMPLETE)
**Status**: ✅ Fully implemented with storage integration
**Performance**: **4× speedup on large datasets** (41s → 10.2s on 260-hour dataset, commit dc2ad4e)
**Note**: Small datasets show minimal benefit (3.5% on 10-day dataset)
**Commits**: Latest in dc2ad4e

**What Works**:
- ✅ Time range extraction from correlation keys (10µs for 260 ranges)
- ✅ Multi-range AVET scanning in BadgerDB (260 time ranges)
- ✅ Metadata propagation (negligible 20ns overhead)
- ✅ Size check optimization (<50 tuples skip extraction)

**Benchmark Results** (BenchmarkOHLCQuery, verified 2025-10-24):
- Small dataset (10 days, 390 bars/day):
  - WithoutPushdown: 48.8ms
  - WithPushdown: 33.3ms
  - WithTimeRangeOpt: 32.1ms (**3.5% improvement**, 1.52× vs no optimization)
- Large dataset (260 hours): **4× speedup** (41s → 10.2s, measured during development)

**Key Insight**: Time-range optimization benefit scales with dataset size and time selectivity. Most valuable for large historical datasets with selective time filters.

**Details**: See `TIME_RANGE_OPTIMIZATION_STATUS.md`

### 3. Hash Join Pre-Sizing (COMPLETE)
**Status**: ✅ Implemented across all join operations
**Performance**: 24-32% faster, 24-30% less memory for hash operations
**Commit**: dc2ad4e

**What Was Done**:
- ✅ Added `NewTupleKeyMapWithCapacity()` for pre-sizing
- ✅ Updated 7 call sites (HashJoin, SemiJoin, AntiJoin, deduplication)
- ✅ Pre-size based on relation sizes to avoid map growth

**Impact**:
- Micro-level: 24-32% faster TupleKeyMap operations
- Macro-level: Minimal impact on OHLC (pattern matching dominates)
- High impact for large joins (>1,000 tuples)

**Details**: See `HASH_JOIN_PRESIZING_SUMMARY.md`

### 4. Semantic Rewriting with Expression Elimination (COMPLETE)
**Status**: ✅ Production-ready with predicate transformation
**Performance**: 2.6-5.8× speedup on time-filtered queries
**Commits**: Multiple in Oct 2025

**What Works**:
- ✅ Time extraction → range constraint transformation
- ✅ Expression/predicate elimination (skip optimized-away code)
- ✅ Multi-component constraint composition (year+month+day+hour)
- ✅ MemoryPatternMatcher integration

**Benchmarks**:
- Year filter (33% selective): 2.6× faster
- Day filter (12.5% selective): 4.1× faster
- Hour filter (1.4% selective): 5.8× faster

**Note**: With decorrelation enabled, semantic rewriting shows no additional speedup (1.00×) because both optimize the same bottleneck (time extraction overhead). Decorrelation eliminates 97% of evaluations, leaving nothing for semantic rewriting to optimize. Still valuable for standalone queries (1.64× speedup) and future BadgerDB integration.

**Recommended for time-heavy queries** (opt-in; off by default): `EnableSemanticRewriting: true`
**Details**: See `docs/archive/2025-10/SEMANTIC_REWRITING_FINDINGS.md`

### 5. Common Subexpression Elimination - CSE (REMOVED)
**Status**: ❌ Removed in v0.10.2. The Selinger-style implementation operated on
filter groups from the old decorrelation path, which is superseded by the algebra
bridge. The option, tests, and implementation were dead code.

**Future**: CSE at the algebra IR level (identifying shared subtrees across
decorrelated subqueries) would require extending the IR from tree to DAG.
See `docs/archive/2025-10/CSE_FINDINGS.md` for historical analysis.

### 6. Parallel Subquery Execution (COMPLETE)
**Status**: ✅ Implemented and enabled by default
**Performance**: **2.06× speedup** with 8 workers (verified 2025-10-25)
**Commits**: 626e409 (with single-use semantics), d645cfd, ec45d77, 2439e0a

**What Works**:
- ✅ Worker pool with bounded parallelism (uses runtime.NumCPU())
- ✅ Query plan reuse across iterations
- ✅ Thread-safe result aggregation
- ✅ Proper iterator lifecycle management

**Benchmark Results** (BenchmarkRelationInputParallel, 2400 input tuples):
- Sequential: 730 ms, 1,101 MB/op
- Parallel-2Workers: 738 ms (1.01× slower, overhead for small dataset)
- Parallel-4Workers: 534 ms (1.37× faster)
- **Parallel-8Workers: 355 ms (2.06× faster)** ✅
- Parallel-16Workers: 436 ms (1.67× faster, diminishing returns)
- Parallel-32Workers: 418 ms (1.75× faster, overhead exceeds benefit)

**Key Finding**: Optimal worker count is 8 on M3 Max. Memory usage remains constant across all configurations (~1.1 GB), demonstrating proper lifecycle management and no memory leaks.

### 7. Intern Cache Optimization (COMPLETE)
**Status**: ✅ Lock-free sync.Map replacing sync.RWMutex
**Performance**: 6.26× speedup for BadgerDB parallel queries
**Commit**: e3c956b

**Impact**:
- Before: 35% CPU time on mutex contention
- After: Near-zero lock contention with atomic operations
- Micro-benchmarks: 13-80× faster intern operations

### 8. Pull API Performance (MEASURED 2025-12-17)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: **9× faster than equivalent queries**, linear scaling
**Location**: `datalog/executor/pull.go`, `datalog/storage/database.go`

**Why Pull is Fast**:
- Direct index seeks via `EntityLookupMatcher` interface
- No query parsing or planning overhead
- Single AEVT index lookup per attribute
- Wildcard uses single EAVT prefix scan

**In-Memory Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| Single Attribute | 554ns | 19 | Base case |
| 5 Attributes | 2.3µs | 87 | ~470ns/attr |
| Wildcard (5 attrs) | 1.7µs | 36 | **Faster than explicit** |
| Nested (2 levels) | 2.1µs | 72 | +reference follow |
| Deep (3 levels) | 2.6µs | 91 | Linear with depth |
| PullMany (100 entities) | 102µs | 3601 | ~1µs/entity |

**BadgerDB Storage Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| Single Attribute | 1.1µs | 31 | 2× in-memory |
| 5 Attributes | 6.2µs | 147 | ~1.2µs/attr |
| Wildcard (5 attrs) | 2.8µs | 69 | **2.2× faster than explicit** |
| Nested (2 levels) | 5.0µs | 120 | |
| Standalone API | 4.3µs | 116 | Includes parse |
| Cached Pattern | 3.5µs | 89 | Pre-parsed |
| PullMany (100 entities) | 225µs | 6001 | ~2.2µs/entity |

**Pull vs Query Comparison** (3 attributes, BadgerDB):
| Method | Time | Speedup |
|--------|------|---------|
| Pull | 3.5µs | **9.2×** |
| Query | 32.7µs | baseline |

**Scaling Characteristics**:
- **Per-attribute cost**: ~1.2µs (BadgerDB), ~470ns (in-memory)
- **Per-entity cost**: ~2.5µs (BadgerDB), ~1µs (in-memory)
- **Linear scaling**: Both attributes and entities scale linearly
- **Pattern caching**: 20% speedup by pre-parsing patterns

**Key Insight**: Wildcard `[*]` is 2× faster than explicit attribute lists because it performs one EAVT scan instead of N AEVT lookups.

**Recommended Usage**:
```go
// For hot paths, cache the parsed pattern
pattern, _ := parser.ParsePullPattern(`[:user/name :user/age]`)
puller := executor.NewPullExecutor(d.Unwrap().Matcher())

// Reuse pattern across calls
for _, entity := range entities {
    result, _ := puller.Pull(entity, pattern)
}
```

### 9. Schema Validation Performance (MEASURED 2025-12-17)
**Status**: ✅ Negligible overhead for type validation, minimal overhead for uniqueness
**Performance**: Type validation **<1% write overhead**, uniqueness checking **~6% write overhead**
**Location**: `datalog/schema/`, `datalog/storage/database.go`

**Note**: All schema overhead is on the **write path only**. Reads are completely unaffected.

**Type Validation Overhead** (Apple M4 Max, write path):
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Add without schema | 25,771 | 223 | baseline |
| Add with schema | 25,768 | 225 | **<0.2%** |

**Uniqueness Checking Overhead** (write path):
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Add without schema | 25,771 | 223 | baseline |
| Add with uniqueness | 27,236 | 292 | **~5.8%** |

**Bulk Operations (100 items/transaction)**:
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Bulk without schema | 1,388,000 | 17,115 | baseline |
| Bulk with schema | 1,408,000 | 17,117 | **~1.4%** |

**Schema Resolution (one-time cost per Pull pattern)**:
| Operation | ns/op | allocs |
|-----------|-------|--------|
| Resolve 5-attr pattern with nested refs | 225 | 10 |

**Pull with Cardinality-Many** (10 values):
| Cardinality | ns/op | allocs |
|-------------|-------|--------|
| Cardinality-one (1 value) | 1,117 | 31 |
| Cardinality-many (10 values) | 3,679 | 96 |

**Key Findings**:
- **Type validation is essentially free** - just a map lookup and type switch (write path)
- **Uniqueness checking adds ~6% to writes** - requires database query to check existing values
- **Reads are unaffected** - schema validation only impacts the write path
- **Schema resolution is negligible** - 225ns one-time cost per Pull pattern
- **Cardinality-many scales linearly** - ~370ns per additional value in Pull results

**Recommendation**: Enable schema validation freely for type safety. Uniqueness constraints are worth the 6% write overhead for data integrity.

### 10. CRDT Storage Performance (MEASURED 2026-01-31)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: Write operations ~25-35µs, reads scale linearly with cardinality
**Location**: `datalog/storage/database.go`, `datalog/storage/matcher.go`

**Why CRDT Storage**:
- **LWW (Last-Writer-Wins)** for cardinality-one: Highest ElementID wins
- **Add-wins** for cardinality-many: Concurrent add + remove at same Lamport → add wins
- **RGA** for cardinality-vector: Replicated Growable Array for ordered collections
- All writes preserved with ElementIDs for time-travel queries

**Write Operation Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| CardinalityOne (LWW) | 24,906 ns | 266 | Single value write |
| CardinalityMany/Add | 26,132 ns | 263 | Add to set |
| CardinalityMany/AddRemove | 33,725 ns | 373 | Add + Remove pair |
| CardinalityVector/Append | 27,126 ns | 270 | RGA append |

**Read Operation Benchmarks**:
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| CardinalityOne | ~1.1µs | 29 | Direct index lookup |
| CardinalityMany/10 members | 3,259 ns | 74 | Set resolution |
| CardinalityMany/100 members | 26,522 ns | 443 | Full set resolution |
| CardinalityVector/10 elements | 21,239 ns | 420 | RGA reconstruction |
| CardinalityVector/100 elements | 204,660 ns | 3,687 | RGA reconstruction |

**Comparison with Documented Pull API Performance**:
| Operation | Documented (Pull) | CRDT Benchmark | Status |
|-----------|-------------------|----------------|--------|
| Cardinality-One | 1,117 ns, 31 allocs | 1,100 ns, 29 allocs | ✅ Match |
| Cardinality-Many (10 values) | 3,679 ns, 96 allocs | 3,259 ns, 74 allocs | ✅ Faster |

**CRDT Resolution Benchmarks**:
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| AddWins/NoConflict (50 members) | 13,809 ns | 240 | No tombstones |
| AddWins/WithTombstones (100 adds, 50 removes) | 37,911 ns | 649 | With tombstone filtering |
| LWW/ManyVersions (1000 versions) | 965 ns | 31 | First entry = current |

**Scaling Characteristics**:
- **Per-element cost (vectors)**: ~2µs per element for reconstruction
- **Set resolution**: O(n) where n = total operations (adds + removes)
- **LWW resolution**: O(1) - first entry in descending Tx scan is current

**Key Findings**:
- **Write performance is consistent** across cardinalities (~25-35µs, storage I/O dominates)
- **LWW is extremely fast** - 965ns to resolve current value from 1000 versions (EATV: first entry = current)
- **Vector reconstruction is expensive** - RGA reconstruction (21µs for 10 elements) is **~6× slower** than set resolution (3.3µs for 10 members) due to graph traversal
- **Add-wins resolution** scales with complexity - 50 clean members: 14µs; with 50 tombstones: 38µs (~2.7× more work)

**Comparison with Non-CRDT Writes**:
| Benchmark | CRDT (ns/op) | Schema Validation (ns/op) | Notes |
|-----------|--------------|---------------------------|-------|
| Single Add | 24,906 | 25,768 | CRDT ~3% faster |
| With Uniqueness | 27,236 | 27,236 | Same (uniqueness dominates) |

CRDT semantics add negligible overhead to writes while providing:
- Conflict-free replication capability
- Time-travel queries via `d.History()` and `d.AsOf(elementID)`
- Multi-replica merge support

### 11. CRDT Allocation Optimization (COMPLETE - February 2026)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: **90% faster** (1.9×), **2.2× less memory** than pre-CRDT main branch (verified 2026-02-02)
**Location**: `datalog/storage/`, `datalog/executor/`

**The Story**: CRDT storage added powerful new capabilities (LWW, add-wins sets, RGA vectors, time-travel queries). The initial implementation had allocation overhead. Rather than accept a performance regression, we optimized the entire storage and executor pipeline. The result: **CRDT storage that's faster than the original non-CRDT implementation**.

**What We Optimized** (6 phases targeting hot paths):

**Phase 1: txToDescending optimization**
- Changed return type from `[]byte` to `[16]byte` (stack allocation)
- Result: 16 B/op → 0 B/op, ~29% faster encoding

**Phase 2: Iterator workspace reuse**
- Added `workspace` field to all matcher iterators
- Use `BuildTupleInternedInto()` instead of `BuildTupleInterned()`
- Result: 7-15% memory reduction, 13-29% fewer allocations

**Phase 3: Cache path Datom reuse**
- Pre-allocate `datomBuf` in cache path closures
- Reuse across iterations instead of allocating per tuple
- Result: 5% time improvement, 9% fewer allocations

**Phase 4: DatomFromKey by value**
- Changed return type from `*datalog.Datom` to `datalog.Datom`
- Added `currentDatom` field to iterator structs
- Result: 80 B/op → 0 B/op per datom decode, 25% faster

**Phase 5: RequiresCopy() method**
- Added `RequiresCopy()` to Relation interface
- MaterializedRelation: false (stable slice)
- StreamingRelation: true (iterator reuses workspace)
- Hash join build phase copies only when RequiresCopy()=true

**Phase 6: Conditional copyTuple()**
- Wrapper relations (Union, OrFallback, Prepended) copy once at boundary
- All other copyTuple() calls conditional on RequiresCopy()
- `collectTuplesInto()` helper for consistent conditional copying

**Benchmark Results** (Apple M4 Max, BenchmarkOHLCQuery):

| Metric | Main (pre-CRDT) | With CRDT + Optimizations | Improvement |
|--------|-----------------|---------------------------|-------------|
| Time | 57ms | 30ms | **1.9× faster** |
| Memory | 66MB | 30MB | **2.2× less** |
| Allocations | 897K | 405K | **2.2× fewer** |

This means we added full CRDT semantics (LWW, add-wins sets, RGA vectors, time-travel queries) **and** made the engine nearly 2× faster than before.

**Additional Benchmarks**:

| Benchmark | Improvement | Notes |
|-----------|-------------|-------|
| VectorQuery | 11% faster | Exercises wrapper relation paths |
| CRDT resolution | O(1) LWW | First entry in descending scan |

**Key Insight**: The biggest wins came from eliminating heap allocations in hot paths:
- `DatomFromKey()` called millions of times during scans
- `txToDescending()` called for every key encoding
- Iterator workspace reuse amortizes allocation across all tuples

**Files Changed**:
- `datalog/storage/key_encoder_binary.go` - txToDescending return type
- `datalog/storage/datom_decoder.go` - DatomFromKey by value
- `datalog/storage/matcher_iterator_*.go` - Workspace reuse
- `datalog/storage/matcher_relations.go` - Cache path optimization
- `datalog/executor/relation.go` - RequiresCopy() interface
- `datalog/executor/join.go` - Conditional copy in build phase
- `datalog/executor/*.go` - Conditional copyTuple() throughout

### 12. OHLC Query Performance (MEASURED 2025-10-25)
**Benchmark**: OHLC queries with subqueries and predicate pushdown

**Subquery Performance** (BenchmarkOHLCSubqueries):
- Single aggregation: 17.3 ms/op, 66.7 MB/op, 934K allocs
- Three aggregations: 51.3 ms/op, 199.6 MB/op, 2.8M allocs
- **Result**: Linear scaling (3× subqueries = 2.96× time, 3× memory), proper semantics ✅

**Predicate Pushdown - Small Dataset** (BenchmarkOHLCQuery, 10 days × 3 symbols × 390 bars):
- Without pushdown: 33.6 ms/op, 39.4 MB/op, 534K allocs
- With pushdown: 21.3 ms/op, 20.0 MB/op, 330K allocs
- With time-range opt: 21.5 ms/op, 20.0 MB/op, 330K allocs
- **Result: 1.58× faster, 49% memory reduction, 38% fewer allocations** ✅

**Predicate Pushdown - Large Dataset** (BenchmarkOHLCQueryLargeDataset, 90 days × 50 symbols sparse):
- Without pushdown: 1,043 ms/op, 3,484 MB/op, 15.4M allocs
- With pushdown: 375 ms/op, 296 MB/op, 5.2M allocs
- **Result: 2.78× faster, 91.5% memory reduction, 66.2% fewer allocations** ✅
- **Key insight**: Predicate pushdown scales better with larger datasets (1.58× → 2.78×)

**Key Findings**:
- Iterator semantics correctly enforced (no re-iteration bugs)
- Memory scales linearly with query complexity (predictable)
- Predicate pushdown benefits increase with dataset size
- Large dataset queries complete in <400ms even with 90 days of data

### 13. Identity & Keyword Interning (COMPLETE - December 2025)
**Status**: ✅ Full pointer interning for Identity and Keyword types
**Performance**: **10-20% faster** on join-heavy workloads, **25% memory reduction** (verified 2025-12-24)
**Commits**: a504729

**What We Built**:
- Unexported structs with pointer type aliases: `type Identity = *identity`, `type Keyword = *keyword`
- Storage-aligned cache keys: `[20]byte` for Identity (SHA1), `[32]byte` for Keyword (attribute storage format)
- Zero-allocation lookup from storage via `InternKeywordFromBytes()` and `InternIdentityFromHash()`
- Runtime invariants detect interning failures (same value, different pointer → panic)

**Measured Results** (high-cardinality benchmark, 224 keywords, ~35K datoms):

| Benchmark | Time | Memory | Allocations |
|-----------|------|--------|-------------|
| JoinQuery_CrossNamespace | **-7.6%** | **-44%** | **-38%** |
| WildcardPull_ManyAttributes | **-9.7%** | **-28%** | -1.3% |
| Aggregation_ManyAttributes | -2.3% | -15% | -11% |
| geomean | **-3.8%** | **-25%** | **-14%** |

**Full benchmark suite** (executor package):

| Benchmark | Improvement |
|-----------|-------------|
| SubqueryExecution/Legacy | **-11.8%** |
| SubqueryExecution/Componentized | **-12.1%** |
| SubqueryExecutionLarge | **-10%** |
| SequentialVsParallel | **-19% to -20%** |
| TimeToFirstResult (geomean) | **-17.5%** |
| HashJoin (data_2500+) | **-7% to -11%** |

**Scaling Behavior**:
- Small datasets (< 500 tuples): 3-6% regression (intern lookup cost dominates)
- Large datasets (1000+ tuples): 7-20% improvement (pointer comparison wins amortize)
- Crossover point: ~500-1000 tuples

**Known Regression**: `TimeAggregation` +13% — intern lookup not amortized in this specific pattern.

**Why It Works**: Entities are join keys. Every hash join probe, every equality check in result deduplication, every tuple comparison is now a pointer comparison instead of 20-byte array (Identity) or string (Keyword) comparison. Memory wins come from reusing interned pointers instead of allocating fresh structs per result tuple.

**Key Insight**: The existing `datom_decoder.go` already had `[20]byte` → Identity caching and `[32]byte` → string caching. This optimization completed the picture by making all downstream comparisons pointer-based.

### 14. Conditional Aggregate Rewriting (COMPLETE - January 2026)
**Status**: ✅ Both legacy executor and QueryExecutor now support conditional aggregate rewriting
**Performance**: **7.7× faster**, **5.2× less memory**, **8.1× fewer allocations** (verified 2026-01-16)
**Commits**: Latest

**What It Does**:
Transforms correlated subqueries with aggregates into single-pass conditional aggregation:
```clojure
;; Before: N separate subquery executions
[(q [:find (max ?v) :in $ ?person ?day :where ...] $ ?p ?day) [[?max-value]]]

;; After: Single pass with conditional aggregate
(max ?v :when ?__cond_?pd)  ;; Condition filters which tuples contribute
```

**Benchmark Results** (Apple M4 Max, 3 people × 10 days × 20 events = 600 events):

| Metric | Without Rewriting | With Rewriting | Improvement |
|--------|-------------------|----------------|-------------|
| Time | 16.2 ms/op | 2.1 ms/op | **7.7× faster** |
| Memory | 15.2 MB/op | 2.9 MB/op | **5.2× less** |
| Allocations | 275,359/op | 33,899/op | **8.1× fewer** |

**Executor Comparison** (both with rewriting enabled):

| Executor | Time | Memory | Allocations | vs Legacy |
|----------|------|--------|-------------|-----------|
| Legacy | 1.95 ms | 2.87 MB | 33.4K | baseline |
| QueryExecutor | 1.98 ms | 2.95 MB | 33.9K | **+1.5%** (parity) |

**Scale Test** (with rewriting enabled):

| Scale | Legacy | QueryExecutor | Difference |
|-------|--------|---------------|------------|
| Small (600 events, 30 groups) | 1.96 ms | 1.99 ms | +1.5% |
| Medium (3000 events, 100 groups) | 11.4 ms | 12.1 ms | +6% |

**Why It's Fast**:
- **Without rewriting**: Executes N separate subqueries (one per outer tuple), each scanning and filtering data
- **With rewriting**: Single pass over data with grouped conditional aggregation
- Eliminates repeated index scans, reduces from O(N × M) to O(M)

**Implementation Note** (January 2026 fix):
The planner now emits **two representations** of conditional aggregates:
1. **Metadata**: `phase.Metadata["conditional_aggregates"]` - used by legacy executor
2. **Find clause**: Modified `FindAggregate` with `Predicate` field - used by QueryExecutor

This dual approach maintains backward compatibility while following "Datalog is the IR" principle.

**When It Applies**:
- Correlated subqueries with aggregates (max, min, sum, count, avg)
- Pattern: "For each X, find aggregate of Y where Y relates to X"
- Examples: max value per user per day, latest price per ticker, totals per category

**Configuration**: No separate flag. The rewrite is performed by the default-active
algebra optimizer (`EnableAlgebraOptimizer: true`); there is no
`EnableConditionalAggregateRewriting` option (removed 2026-05).

### 15. Interned-Pointer Index Keys & L85 Cache Removal (COMPLETE - May 2026)
**Status**: ✅ In-memory indices and entity-dedup sets key on interned pointers; the `Identity.l85` cache was removed

**What changed**:
- The in-memory matcher's entity/EA indices and the BadgerMatcher entity-dedup set
  keyed on `Identity.L85()` strings (`E.L85()`, `E.L85()+"|"+A.String()`). Since
  identities and keywords are interned (pointer equality ⟺ value equality), these
  now key on the interned pointers directly — `map[datalog.Identity][]int` and
  `map[eaIndexKey][]int` (an `{Identity, Keyword}` pair). No Base85 encode, no
  per-key string concatenation.
- With no hot `L85()` callers left (only `String()`'s fallback and export), the
  lazily-cached `l85` field on `identity` was removed; `Identity.L85()` now computes
  on demand. This also fixed a data race — the lazy cache write mutated
  globally-interned, shared identities without synchronization
  (`BUG_IDENTITY_L85_LAZY_RACE`).

**Benchmark** (`datalog/executor/index_key_bench_test.go`; (E,A) index build+lookup,
1000 entities × 8 attrs; the string variant uses precomputed L85 to model the prior
cache fairly):

| key | ns/op | B/op | allocs/op |
|-----|-------|------|-----------|
| string (old) | 644,085 | 1,619,084 | 24,033 |
| interned pointer (new) | 240,159 | 851,076 | 8,033 |

~2.7× faster, ~47% less memory, and 1/3 the allocations on the index path — before
counting the removed race and the per-identity L85 string.

**Details**: See `docs/bugs/resolved/BUG_IDENTITY_L85_LAZY_RACE.md`

---

## Profiling Results (October 2025)

### In-Memory Execution Path
**Profiled**: MemoryPatternMatcher with OHLC queries

**CPU Bottlenecks** (measured via pprof):
- Pattern matching: 58% (matchesDatomWithPattern, matchesConstant, matchesElement)
- Memory copying: 18% (runtime.duffcopy)
- Hash operations: Various small percentages

**Memory Allocations** (measured via pprof):
- TupleKeyMap operations: 35% (now optimized with pre-sizing)
- Expression evaluation: 9-19% (time extractions)
- Pattern matching: 14%

**Key Finding**: Pattern matching dominates CPU time in-memory queries. Optimizations targeting pattern matching (in-memory indexing) made hash indices the default path throughout the codebase.

### Storage-Backed Execution Path
**Profiled**: BadgerMatcher with OHLC queries
**Query Time**: 56ms for 260 hours (measured)

**Key Finding**: Storage-backed queries already fast enough for production use. Focus has been on correctness and architectural improvements rather than micro-optimizations.

**Details**: See `PROFILING_SUMMARY.md` and `EXECUTION_CHAIN_PROFILING_ANALYSIS.md`

---

## Performance Test Results (Actual Benchmarks)

### Planner/Executor Architecture (2025-10-22)
**Benchmark Suite**: `BenchmarkPlannerOnly`, `BenchmarkPlanQuality`, `BenchmarkFullQueryOldVsNewPlanner`

**Planning Overhead Only** (BenchmarkPlannerOnly):
| Query Type | Old Planner | New Planner | Speedup |
|-----------|-------------|-------------|---------|
| Simple pattern | 3,940 ns | 2,048 ns | 52% faster |
| Single subquery | 4,382 ns | 758 ns | 83% faster |
| OHLC query | 11,546 ns | 1,673 ns | 86% faster |

**Key Finding**: Planning is fast (1-15 microseconds) regardless of planner. Planning speed has negligible impact on total query time.

**Plan Quality** (BenchmarkPlanQuality - both using QueryExecutor, verified 2025-10-24):
| Query Type | Old Planner | New Planner | Difference |
|-----------|-------------|-------------|------------|
| simple_join | 1.609 ms | 1.612 ms | 0.2% slower (within noise) |
| aggregation | 2.003 ms | 2.014 ms | 0.6% slower (within noise) |
| multi_join | 2.882 ms | 2.886 ms | 0.1% slower (within noise) |

**Key Finding**: When isolated (same executor), both planners produce equivalent-quality plans. Performance differences are within measurement noise.

**Full Architecture** (BenchmarkFullQueryOldVsNewPlanner - old planner+executor vs new planner+executor):
| Query Type | Old Architecture | New Architecture | Improvement |
|-----------|------------------|------------------|-------------|
| OHLC queries | ~4-8 seconds | ~2-4 seconds | **~2× faster** |

**Key Finding**: The 2× improvement comes from QueryExecutor's clause-by-clause streaming execution model, not from planner differences. Both planners produce equivalent-quality plans when using the same executor.

**Details**: See `docs/archive/completed/PLANNER_COMPARISON.md` (archived: one planner now)

### Time Range Optimization (2025-10-08)
**Hourly OHLC (260 hours)**:
- Before: 41s
- After: 10.2s
- **Speedup: 4.0×** ✅

**Daily OHLC (22 days)**:
- Before: 217ms
- After: 217ms
- **Speedup: 1.0×** (no regression) ✅

### Hash Join Pre-Sizing (2025-10-08)
| Size | Speed | Memory | Speedup |
|------|-------|--------|---------|
| 100 | 6.9µs → 5.2µs | 17KB → 13KB | 25% faster |
| 1,000 | 105µs → 71µs | 259KB → 181KB | 32% faster |
| 10,000 | 1.34ms → 1.24ms | 2.3MB → 1.7MB | 7% faster |

### Semantic Rewriting (2025-10-07)
| Filter Type | Selectivity | Speedup |
|-------------|-------------|---------|
| Year only | 33% | 2.6× |
| Year+Month+Day | 12.5% | 4.1× |
| Year+Month+Day+Hour | 1.4% | 5.8× |

### Plan Cache (Empirical)
- First query: ~1-5ms planning time
- Cached query: ~0.3ms planning time
- **~3× speedup for repeated queries**

---

## Optimization Priorities

### High Impact, Already Done ✅
All items below are **measured** and **active** in production code:

1. ✅ **New architecture** (clause-based planner + QueryExecutor) - **2× faster on complex queries** (verified 2025-10-24)
2. ✅ **Pull API** - **9× faster than equivalent queries**, linear scaling (verified 2025-12-17)
3. ✅ **Schema validation** - **<1% overhead** for type checking, **~6%** for uniqueness (verified 2025-12-17)
4. ✅ **Iterator composition** - **4.06× faster, 89% memory reduction** (verified 2025-10-25)
5. ✅ **Parallel subquery execution** - **2.06× speedup with 8 workers** (verified 2025-10-25)
6. ✅ **Intern cache optimization** - **6.26× speedup with BadgerDB**
7. ✅ **Query plan caching** - **3× speedup for repeated queries**
8. ✅ **Time range optimization** - **4× speedup on hourly OHLC**
9. ✅ **Semantic rewriting** - **2-6× on time-filtered queries**
10. ✅ **Predicate pushdown** - **1.58-2.78× faster** (scales with dataset size), **up to 91.5% memory reduction** (verified 2025-10-25)
11. ✅ **Streaming execution** - **2.22× on low-selectivity filters, 52% memory reduction** (verified 2025-10-25)
12. ✅ **Hash join pre-sizing** - **24-32% faster, 24-30% less memory**
13. ✅ **In-memory indexing** - Hash indices now default path throughout codebase
14. ✅ **Relation collapsing algorithm** - **Prevents catastrophic Cartesian products**
15. ✅ **Conditional aggregate rewriting** - **7.7× faster, 5.2× less memory** for correlated aggregate subqueries (verified 2026-01-16)
16. ✅ **CRDT storage** - **~25-35µs writes**, **O(1) LWW resolution**, conflict-free replication (verified 2026-01-31)
17. ✅ **CRDT allocation optimization** - **90% faster** (1.9×), **2.2× less memory** than pre-CRDT main while adding full CRDT semantics (verified 2026-02-02)

### Potential Future Work 🎯
These are **ideas**, not commitments. Would require benchmarking before implementation:

1. Streaming aggregations - Reduce memory for large groups
2. BadgerDB time range integration - Push time constraints to storage layer
3. Composite index support - For multi-attribute filters

### Known Performance Regressions (Correctness Fixes)

**Set Semantics Fix for StreamingRelation.Project() (2025-12-24)**

`StreamingRelation.Project()` was not deduplicating results, violating set semantics. The fix wraps `ProjectIterator` with `DedupIterator`.

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Time | 35,326 ns/op | 44,331 ns/op | +25% |
| Memory | 2,520 B/op | 16,744 B/op | +6.6× |
| Allocs | 111 allocs/op | 224 allocs/op | +2× |

**Why this overhead is acceptable**: Without deduplication, projected relations can contain duplicates, violating Datalog semantics and causing incorrect query results. Correctness trumps performance.

**Future optimization**: See `docs/proposals/TUPLEKEYMAP_OPTIMIZATION.md` for proposal to optimize `TupleKeyMap` for set membership (currently stores values we don't need for dedup).

### Rejected After Benchmarking ❌
These were **tried and measured** - data showed they're not worth the complexity:

1. ~~Key mask iterator for int64~~ - Benchmarked slower than simple approach
2. ~~Complex iterator reuse~~ - Simpler code is faster
3. ~~Aggressive CSE~~ - 1-3% sequential, -1% parallel (disabled by default)
4. ~~Interface-based key encoder consolidation~~ - 10% slower, 60% more allocations (Dec 2025)
5. ~~Generic key encoder with type parameters~~ - Go generics don't inline, same overhead as interfaces

---

## Recommended Configuration (October 2025)

**Production Configuration** (all settings are measured and proven):

```go
// Public API — uses all production defaults automatically
d, _ := db.Open("path/to/db")
d.Query(`[:find ?e ?v :where [?e :price/close ?v]]`)

// With schema:
d, _ := db.Open("path/to/db", db.WithSchema(s))

// For advanced planner tuning, use the internal packages directly:
// Advanced tuning starts from the defaults — these ARE the db.Open defaults:
opts := storage.DefaultPlannerOptions()
// opts.EnableAlgebraOptimizer     == true   // decorrelation + predicate pushdown
// opts.EnableIteratorComposition  == true   // lazy evaluation
// opts.EnableTrueStreaming        == true   // streaming, no auto-materialization
// opts.EnableParallelSubqueries   == true   // parallel subquery execution
// opts.EnableStreamingAggregation == true   // streaming aggregation

// Opt-in (off by default) — enable explicitly if you want them:
opts.EnableSemanticRewriting = true   // fold year(?t)=2025 into a time range

exec := executor.NewExecutorWithOptions(matcher, opts)
```

**Key Changes from Previous Versions**:
- New clause-based planner is now default (was experimental)
- QueryExecutor is now default (was opt-in)
- Streaming execution always enabled (was toggle)
- All settings backed by measured benchmarks

---

## Performance Philosophy

### What We Got Right
1. **Correctness first** - Semantics before speed
2. **Measure everything** - Benchmarks revealed truth
3. **Simple code wins** - Iterator reuse complexity didn't pay off
4. **Smart algorithms** - Relation collapsing is the real hero
5. **Targeted optimization** - Profile, optimize bottlenecks, verify

### What We Learned
1. **Micro-optimizations fail** - Opens are 3µs, not worth complexity
2. **Architecture matters** - Can't push predicates without storage support
3. **Premature optimization is real** - Key mask, iterator reuse both slower
4. **Document reality** - Aspirational docs cause confusion
5. **Redundant optimizations exist** - Semantic rewriting + decorrelation target same bottleneck
6. **Go interfaces have real cost** - 10% slower, 60% more allocations from interface dispatch in hot paths
7. **Go generics don't inline** - Unlike C++/Rust, Go generics provide type safety but NOT zero-cost abstraction
8. **DRY vs performance trade-off** - In hot paths, duplication IS the optimization; share only cold paths

### What's Next
1. Keep **simple, correct code** (complexity doesn't pay)
2. Let **algorithms win** (relation collapsing, not tricks)
3. Build **only what benchmarks prove** (no more speculation)
4. Consider **streaming aggregations** for memory efficiency

---

## Success Metrics

### Current State (2025-10-25)
All metrics below are **measured** from actual benchmarks, not estimates.

**Verified Performance Improvements** (latest benchmarks):
- New architecture: **2× faster** on complex OHLC queries (old: ~4-8s, new: ~2-4s) ✅
- Iterator composition: **4.06× faster** (1,259μs → 310μs), **89% memory reduction** (3.27 MB → 360 KB) ✅
- Streaming execution: **2.22× faster** on low-selectivity filters (1,720μs → 774μs), **52% memory reduction** ✅
- Parallel subquery execution: **2.06× speedup** with 8 workers (730ms → 355ms) ✅
- Predicate pushdown (small): **1.58× faster** (33.6ms → 21.3ms), **49% memory reduction** ✅
- Predicate pushdown (large): **2.78× faster** (1,043ms → 375ms), **91.5% memory reduction** ✅
- Time-filtered queries: **2-6× faster** (semantic rewriting) ✅
- Hourly OHLC (large dataset): **10.2s** (4× speedup from time ranges) ✅
- Parallel BadgerDB: **6.26× speedup** (intern cache optimization) ✅
- Hash join pre-sizing: **24-32% faster, 24-30% less memory** ✅

**Pervasive Optimizations** (now default throughout codebase):
- In-memory indexing: Hash indices are now the default path (previously showed 49-4802× vs linear scan)
- Plan quality: Both planners produce equivalent-quality plans (within measurement noise)
- Single-use iterator semantics: Proper lifecycle management prevents bugs, enables streaming

---

## Documentation Organization

### Active Documentation (Root)
- `PERFORMANCE_STATUS.md` - **This file** (consolidated performance overview)
- `docs/reference/PLANNER_OPTIONS.md` - Planner/executor options reference (defaults + opt-in flags)

### Supporting Documentation
- `TIME_RANGE_OPTIMIZATION_STATUS.md` - Time range extraction and optimization
- `HASH_JOIN_PRESIZING_SUMMARY.md` - Hash join pre-sizing optimization
- `PROFILING_SUMMARY.md` - Complete profiling findings

### Archived Documentation (docs/archive/2025-10/)
- Detailed analyses (EXECUTION_CHAIN_PROFILING_ANALYSIS.md, etc.)
- Implementation docs (SUBQUERY_DECORRELATION_*.md, SEMANTIC_REWRITING_FINDINGS.md, CSE_FINDINGS.md)
- Streaming architecture history (STREAMING_ARCHITECTURE_COMPLETE.md)
- Bug documentation (PARALLEL_DECORRELATION_TUPLE_ORDER_BUG.md)
- Session summaries (SESSION_SUMMARY_*.md)
- Completed work (CSE_FINDINGS.md, SEMANTIC_REWRITING_FINDINGS.md)

---

## The Bottom Line

**What Actually Matters**:
1. ✅ Relation collapsing prevents memory explosion (CRITICAL)
2. ✅ Query plan caching speeds up repeated queries (3× improvement)
3. ✅ Parallel execution eliminates bottlenecks (6.26× speedup)
4. ✅ Time range optimization targets specific queries (4× speedup)
5. ✅ Semantic rewriting optimizes time queries (2-6× speedup)
6. ✅ Code correctness and simplicity beat micro-optimizations

**What Doesn't Matter**:
1. ❌ Iterator open/close overhead (3µs, negligible)
2. ❌ Key mask filtering for simple types (overhead > benefit)
3. ❌ Complex reuse strategies (simpler code performs better)
4. ❌ CSE with parallel execution (removes parallelism opportunity)

The engine is **production-ready for datasets up to 10M+ datoms**. All major optimizations are complete and working well. Performance is excellent for typical workloads (100K-1M datoms), with targeted optimizations for specific patterns (time queries, large joins). Large config testing demonstrates scalability to 500M+ datoms.

---

## Session History

### 2026-04-17: CRDT Unique Resolution Redesign

**Branch**: `feature/crdt-unique-resolution`
**Status**: ✅ Complete — read-time (A, V)-LWW replaces write-time enforcement

**The Story**:
Write-time `validateUniqueness` was incompatible with this codebase's
CRDT-oriented architecture (concurrent writers, `DetectConflicts=false`,
append-only storage with LWW resolution). The bug report framed it as a
TOCTOU race; the real framing was that the whole write-time gate was
the wrong model. Replaced with walk-based `(A, V)`-LWW resolution at
read time: V-view and entity-view share a single rule, all writes
succeed, reads compute the canonical owner via walk.

See `docs/reference/CRDT_UNIQUE_SEMANTICS.md` for the complete design
discussion and decisions (D1–D5).

**What Was Done**:

1. **Walk-based resolution primitive** (`unique_resolve.go`):
   `walkUniqueEntityValue(E, A)` walks an entity's EATV history in
   descending Tx order, handling retractions and supersession by other
   entities. Returns the first non-superseded Set or nothing.

2. **V-view via walk** (`resolveAVLWW`, `LookupByUnique`):
   Finds max-Tx entry for V across all entities, verifies that
   entity's walk emits V. V-view and entity-view are symmetric by
   construction.

3. **Streaming integration** (`CRDTResolvingIterator`):
   CardinalityOne + Unique groups use the walk inline; other paths
   unchanged. Shared `walkApplyEntry` primitive avoids duplicated
   rule-logic between batch and streaming paths.

4. **Cache invalidation** (`Cache.InvalidateAttribute`):
   Conservative: writes to a unique attribute invalidate all cached
   `(E, A)` entries for that attribute, since any write can silently
   stale other entities' walk results.

5. **History-mode bypass**: `ResolveLWW` skips the walk in history
   mode, returning raw first-entry semantics. Fixes a latent issue
   where `d.History().Pull()` via wildcard would return walk-resolved
   fallback values instead of raw assertions.

6. **Error propagation**: `Iterator` interface extended with
   `Error() error`. All storage iterators implement it; wrapping
   iterators propagate deferred errors from inner iterators. Closes
   multiple pre-existing silent-swallow sites (e.g., CRDTResolvingIterator
   `source.Datom() err → continue`).

**Benchmark Results** (Apple M5, 2s duration):

| Benchmark | ns/op | B/op | allocs/op | vs baseline |
|---|---:|---:|---:|---|
| NonUniqueRead_Baseline (cached) | 387 | 801 | 6 | — |
| UniqueRead_Uncontested (cached) | 377 | 802 | 6 | **~0% (noise)** |
| UniqueRead_ContestedLinear (empty result) | 305 | 593 | 5 | -21% (less data) |
| UniqueRead_DeepFallback (5 layers, empty) | 283 | 593 | 5 | -27% (less data) |
| LookupByUnique_Uncontested (V-view warm) | 7319 | 5793 | 93 | n/a |
| LookupByUnique_ColdCache (V-view cold) | 7605 | 5796 | 93 | n/a |

**Key Findings**:

- **Hot path (cached entity-view reads)**: the walk adds effectively
  **zero overhead** vs non-unique CardinalityOne. Both paths land
  around 380 ns/op. The cache stores walk-resolved values, so
  subsequent reads don't repeat the walk.

- **Contested scenarios are faster, not slower**: when the walk finds
  no fallback value (all entries superseded), the result map is empty
  and `ResolveEntityAttributes` has less work. Deep-fallback (5
  superseded entries) comes out at 283 ns/op — faster than both
  uncontested and non-unique — because the empty-result path is
  genuinely cheaper than the populate-map path.

- **LookupByUnique (V-view) is ~7μs per call**. Not cheap, but
  acceptable for realistic use (authentication flows, reconciliation
  lookups). Cold vs warm cache: similar, because V-view doesn't
  benefit from per-(E, A) cache entries. Improving this is a
  follow-up opportunity if profiling warrants it.

- **Allocation profile unchanged**: cached entity-view reads allocate
  the same 6 times as non-unique. No hidden allocations from the
  walk infrastructure.

**The redesign imposes no measurable cost on the read hot path.**

**Files Changed**:

- `datalog/storage/unique_resolve.go` — walk primitive, shared rule
- `datalog/storage/crdt_resolving_iterator.go` — streaming integration
- `datalog/storage/cache_resolver.go` — `ResolveLWW` routes unique
  through walk; history-mode bypass
- `datalog/storage/cache.go` — `InvalidateAttribute`
- `datalog/storage/database.go` — `LookupByUnique` API, removed
  `validateUniqueness`, unique-attr invalidation in `Transaction.Commit`
- `datalog/storage/matcher_relations.go` — V-view single-winner
  selection in `validatingVBoundIterator`
- `datalog/storage/store.go` — `Iterator.Error()` interface extension
- All storage iterator implementations — `Error()` method
- `datalog/storage/unique_benchmark_test.go` — 6 benchmarks
- Multiple test files — 40+ new tests covering V-view, entity-view
  symmetry, retract semantics, history/AsOf, cache invalidation,
  value-encoding edge cases, error propagation
- `docs/reference/CRDT_UNIQUE_SEMANTICS.md` — design doc (promoted
  from `docs/proposals/`)
- `docs/reference/SCHEMA.md`, `docs/reference/CRDT.md` — updated with
  new semantics

---

### 2026-02-06: AETV Index & Value Elimination - Streaming CRDT Fixes

**Branch**: `main`
**Status**: ✅ Complete - CRDT resolution now correct for all query patterns

**The Story**:
The `CRDTResolvingIterator` relies on Tx-descending index order (first entry = LWW winner). When E was bound via input with A constant, the matcher selected AEVT (Tx ascending) instead of a CRDT-aware index, returning stale values. We added AETV as a 7th index and eliminated redundant value storage.

**What Was Done**:

1. **AETV Index** (A → E → Tx↓ → V):
   - New A-primary CRDT index complementing EATV (E-primary CRDT)
   - Index selection updated: A-constant + E-from-input now uses AETV
   - Fixes `CRDTResolvingIterator` returning wrong values for batch entity lookups

2. **Value Elimination**:
   - `assertDatom()` now writes nil values (all datom data is encoded in keys)
   - `BadgerIterator.Datom()` decodes from key via `DatomFromKey()`
   - ~50% storage reduction (no redundant value bytes)

**Benchmark Results** (Apple M4 Max, benchstat n=100):

| Benchmark | Time | Memory | Allocations |
|-----------|------|--------|-------------|
| SimpleQuery | +2.59% | +0.13% | -0.12% |
| JoinQuery | +0.94% | -0.85% | -0.84% |
| CardinalityMany | **-5.16%** | **-32.59%** | **-28.49%** |
| VectorQuery | **-18.60%** | **-35.11%** | **-32.89%** |
| **geomean** | **-5.44%** | **-18.82%** | **-16.97%** |

**Key Findings**:
- Simple queries: minimal change (less datom decoding)
- Complex queries: significant wins from eliminating value reads
- Storage reduced by ~50% (values were 100% redundant with keys)
- All 17 CRDT resolution tests now pass with `DisableCache: true`

**SimpleQuery Regression Explained** (+2.59%):
The regression comes from `KeyCopy(nil)` in `BadgerIterator.Datom()`. Previously, values were read from BadgerDB's value storage. Now we decode from keys, which requires copying the key bytes because BadgerDB reuses its internal buffer.

We investigated key buffer reuse to eliminate this allocation, but it's not possible: `DecodeKey` returns the value component as a slice *into* the key bytes (e.g., `value = key[entitySize+attrSize:vEnd]`). Reusing the key buffer would cause values to be corrupted on subsequent iterations.

This is an acceptable trade-off:
- SimpleQuery: +2.59% time (~1µs absolute on a 44µs query)
- VectorQuery: **-18.60%** time (complex queries dominate real workloads)
- geomean: **-5.44%** time (net positive across all query types)

**Files Changed**:
- `datalog/storage/key_encoder_*.go` - AETV encoding/decoding
- `datalog/storage/matcher_strategy.go` - Index selection for AETV
- `datalog/storage/badger_store.go` - Value elimination in `assertDatom()`
- `datalog/storage/badger_iterator.go` - Decode from key in `Datom()`
- `docs/wip/AETV_INDEX_AND_VALUE_ELIMINATION.md` - Design doc

---

### 2026-02-02: CRDT Allocation Optimization - Faster Than Pre-CRDT

**Branch**: `feature/allocation-regression-fixes`
**Status**: ✅ Complete - CRDT storage now faster than original non-CRDT implementation

**The Story**:
CRDT storage added powerful capabilities (LWW, add-wins sets, RGA vectors, time-travel). Rather than accept performance overhead, we optimized the entire pipeline. Result: **CRDT + 90% faster (1.9×) than pre-CRDT main**.

**What Was Done**:

1. **Storage Layer** (Phases 1-4):
   - `txToDescending()`: `[16]byte` return eliminates heap escape (16 B/op → 0)
   - `DatomFromKey()`: Return by value eliminates pointer allocation (80 B/op → 0)
   - Iterator workspace reuse via `BuildTupleInternedInto()`
   - Cache path `datomBuf` reuse across iterations

2. **Executor Layer** (Phases 5-6):
   - `RequiresCopy()` method on Relation interface
   - Wrapper relations copy once at boundary, return `RequiresCopy()=false`
   - All `copyTuple()` calls conditional on source's `RequiresCopy()`

**Benchmark Results** (Apple M4 Max, BenchmarkOHLCQuery):

| Metric | Main (pre-CRDT) | CRDT + Optimized | Improvement |
|--------|-----------------|------------------|-------------|
| Time | 57ms | 30ms | **1.9× faster** |
| Memory | 66MB | 30MB | **2.2× less** |
| Allocations | 897K | 405K | **2.2× fewer** |

**Key Insight**: Hot path allocations dominated. Eliminating heap escapes in `DatomFromKey()` (called millions of times) and `txToDescending()` (every key encode) yielded massive gains.

**Files Changed**: 40 files, +3293/-284 lines across storage and executor layers

---

### 2026-01-31: CRDT Storage Benchmarks & LookupAttribute Fix

**Branch**: `main`
**Status**: ✅ Complete with comprehensive CRDT benchmarks

**What Was Done**:

1. **Created CRDT benchmark suite** (`datalog/storage/crdt_benchmark_test.go`):
   - Write benchmarks for all three cardinalities (One, Many, Vector)
   - Read benchmarks for set resolution and vector reconstruction
   - CRDT resolution benchmarks (LWW, add-wins with/without tombstones)

2. **Fixed LookupAttribute semantic issue** (`datalog/storage/matcher.go`):
   - **Problem**: `LookupAttribute` for cardinality-many returned single value instead of `[]interface{}`
   - **Impact**: Pull API without schema (unresolved patterns) only got first set member
   - **Fix**: Updated cache path and storage fallback path to return all set members with add-wins resolution

**Benchmark Results** (Apple M4 Max):

| Category | Operation | Time | Notes |
|----------|-----------|------|-------|
| **Writes** | CardinalityOne | 24.9µs | LWW semantics |
| | CardinalityMany/Add | 26.1µs | Add to set |
| | CardinalityVector/Append | 27.1µs | RGA append |
| **Reads** | CardinalityMany/100 members | 26.5µs | Full resolution |
| | CardinalityVector/10 elements | 21.2µs | RGA reconstruction |
| | CardinalityVector/100 elements | 204.7µs | Linear scaling |
| **Resolution** | LWW/1000 versions | 0.97µs | O(1) lookup |
| | AddWins/50 members | 13.8µs | No tombstones |
| | AddWins/150 ops (100 add, 50 remove) | 37.9µs | With tombstones |

**Key Findings**:
- CRDT writes have consistent ~25-35µs performance across cardinalities
- LWW resolution is O(1) - first entry in descending Tx scan is current value
- Vector reconstruction scales linearly at ~2µs per element
- Add-wins tombstone handling adds ~2.7× overhead vs clean sets

**Files Changed**:
- `datalog/storage/crdt_benchmark_test.go` - New comprehensive benchmark suite
- `datalog/storage/matcher.go` - Fixed LookupAttribute to return `[]interface{}` for cardinality-many
- `datalog/storage/crdt_vector_test.go` - Updated test assertions for new semantics

---

### 2026-01-16: Conditional Aggregate Rewriting - QueryExecutor Parity Fix

**Branch**: `main`
**Status**: ✅ Complete with executor comparison benchmarks

**Problem**:
QueryExecutor couldn't handle conditional aggregate rewriting. Tests used `UseLegacyExecutor: true` as workaround. The rewriter stored aggregates in `phase.Metadata` but QueryExecutor didn't know how to interpret this metadata.

**Root Cause**:
1. `rewriteCorrelatedAggregates` stored conditional aggregates in `phase.Metadata["conditional_aggregates"]`
2. Legacy executor had special code to read metadata and apply aggregation
3. QueryExecutor treated metadata as inert data - didn't create the aggregate output symbol
4. Find clause injection was attempted but `updatePhaseSymbols` was overwriting it

**Solution**:
Moved Find clause injection to AFTER `updatePhaseSymbols` in `planner.go`. The planner now emits two representations:
1. **Metadata** (for legacy executor backward compatibility)
2. **Modified Find clause** with `FindAggregate` containing `Predicate` field (for QueryExecutor)

**Key Changes**:
- `datalog/planner/planner.go`: Added conditional aggregate injection after `updatePhaseSymbols`
- `datalog/planner/subquery_rewriter.go`: Added `collectConditionalAggregates()` and `injectConditionalAggregatesIntoFind()` helper functions
- `datalog/executor/executor.go`: Updated comments documenting dual representation

**Benchmark Results** (Apple M4 Max):

| Configuration | Time | Memory | Allocations |
|---------------|------|--------|-------------|
| Without rewriting | 16.2 ms | 15.2 MB | 275K |
| With rewriting | 2.1 ms | 2.9 MB | 33.9K |
| **Improvement** | **7.7×** | **5.2×** | **8.1×** |

**Executor Comparison** (with rewriting):

| Executor | Time | Difference |
|----------|------|------------|
| Legacy | 1.95 ms | baseline |
| QueryExecutor | 1.98 ms | +1.5% (parity achieved) |

**New Benchmarks Added**:
- `BenchmarkConditionalAggregateExecutorComparison`: Compares legacy vs QueryExecutor with/without rewriting
- `BenchmarkConditionalAggregateScale`: Tests scaling behavior at different data sizes

**Files Changed**:
- `datalog/planner/planner.go` - Find clause injection
- `datalog/planner/subquery_rewriter.go` - Helper functions
- `tests/conditional_aggregate_rewriting_benchmark_test.go` - New executor comparison benchmarks

---

### 2025-12-24: Identity/Keyword Pointer Type Alias Optimization

**Branch**: `main` (merged from `feature/intern-all-keywords`)
**Status**: ✅ Complete with comprehensive reflection audit

**What Was Done**:

Changed `Identity` and `Keyword` from value types to pointer type aliases with mandatory interning:

```go
// Before (value types)
type Identity struct { value [20]byte; l85 string; ... }
type Keyword struct { value string }

// After (pointer type aliases)
type identity struct { value [20]byte; l85 string; ... }  // unexported
type Identity = *identity                                  // exported alias

type keyword struct { value string }                       // unexported
type Keyword = *keyword                                    // exported alias
```

**Key Changes**:
1. **Mandatory interning** - All constructors (`NewIdentity`, `NewKeyword`) automatically intern
2. **Pointer equality** - `kw1 == kw2` is O(1) pointer comparison, not O(n) string comparison
3. **Storage-aligned cache keys** - Keyword intern uses `[32]byte` key (matches storage format), Identity uses `[20]byte`
4. **Direct storage reads** - `InternKeywordFromBytes([32]byte)` and `InternIdentityFromHash([20]byte)` avoid string conversion on cache hit
5. **DecodeKey returns arrays** - Changed `DecodeKey` interface to return fixed-size arrays `([20]byte, [32]byte, ...)` instead of slices, avoiding heap escape

**Benchmark Results** (Apple M3 Ultra, n=100):

| Benchmark | Time | Memory | Allocs |
|-----------|------|--------|--------|
| JoinQuery_CrossNamespace | **-7.6%** | **-44.2%** | **-38.5%** |
| WildcardPull_ManyAttributes | **-9.7%** | **-28.3%** | -1.3% |
| Aggregation_ManyAttributes | -2.3% | -14.9% | -10.5% |
| SimpleQuery_HighKeywordVariety | +4.8% | -5.6% | ~ |
| **Geomean** | **-3.8%** | **-24.7%** | **-14.2%** |

**Why JoinQuery Benefits Most** (-44% memory, -38% allocs):
- Hash joins build `TupleKeyMap` using keyword/identity as keys
- Before: Each key comparison called `.String()`, allocating
- After: Pointer comparison is O(1), no allocation
- Join build phase: Fewer allocations for hash table keys
- Join probe phase: Faster lookups with pointer keys

**SimpleQuery Regression** (+4.8% time):
- Small queries have overhead from pointer indirection
- Still uses less memory (-5.6%), acceptable tradeoff
- Query is already fast (47µs), regression is ~2µs absolute

**Implementation Notes**:
- Downstream consumers unaffected - still use `datalog.Identity` and `datalog.Keyword`
- Comparison semantics unchanged - equal values compare equal
- Thread-safe via `sync.Map` for concurrent interning
- Invariant check: `Compare()` panics if two different pointers have same hash (indicates interning bug)

**Files Changed**: 46 files, +923/-686 lines
- `datalog/types.go` - Pointer type aliases, new methods
- `datalog/intern.go` - Storage-aligned cache keys, `InternKeywordFromBytes`
- `datalog/storage/key_encoder_*.go` - `DecodeKey` returns arrays
- `datalog/storage/datom_decoder.go` - Direct byte interning

**Reflection Audit** (completed same day):
- Fixed 4 functions in `datalog/reflect/types.go` that incorrectly dereferenced Identity/Keyword pointer types
- Pattern: Check `t == identityType || t == keywordType` BEFORE `t.Kind() == reflect.Ptr` → `t.Elem()`
- Functions fixed: `InferCardinality`, `IsRefType` (inner loop), `ElementType`, `IsSliceType`
- Added nil-safety to all Identity/Keyword methods (Hash, L85, String, Equal, Compare, etc.)
- Added comprehensive test coverage: `TestNilIdentityHandling`, `TestNilKeywordHandling`

---

### 2025-12-24: Key Encoder Consolidation - A DRY Refactoring Case Study

**Goal**: Reduce code duplication between `L85KeyEncoder` and `BinaryKeyEncoder` (~95% identical logic).

**Attempt 1: Interface-Based Consolidation**
- Created `ComponentEncoder` interface abstracting encode/decode operations
- Created `baseKeyEncoder` struct with shared index ordering logic
- L85/Binary encoders delegated to base via embedded struct + interface field

**Result**:
| Benchmark | Main | Refactored | Regression |
|-----------|------|------------|------------|
| AVETReuse | 29ms, 16MB, 300K allocs | 32ms, 20.8MB, 476K allocs | **+10% time, +30% mem, +60% allocs** |
| BatchScanScaling/1000 | 1.1ms, 1.15MB, 21K | 1.25ms, 1.41MB, 31K | **+14% time, +23% mem, +47% allocs** |

**Root Cause Analysis**:
1. **Interface dispatch overhead** - Every `e.comp.EncodeEntity()` call goes through vtable
2. **Lazy initialization checks** - `ensureInitialized()` branch on every method
3. **Slice escape to heap** - Passing `[20]byte` by value to interface method, returning `[]byte` causes allocation

**Attempt 2: Go Generics**
- Changed `baseKeyEncoder` to `baseKeyEncoder[T ComponentEncoder]`
- Used concrete type parameter to enable compiler inlining

**Result**: No improvement. Go generics don't specialize/inline like C++ templates or Rust generics. The method calls still happen at runtime.

**Attempt 3: Hybrid Approach (SUCCESS)**
- Restored full inline implementations in each encoder (hot paths)
- Extracted only truly shared utilities to `key_encoder_base.go`:
  - `historyIndexToBase()` - Maps history indices to base indices
  - `incrementLastByte()` - Creates end key for prefix scans
- Removed duplicate `l85HistoryIndexToBase` function

**Result**:
| Benchmark | Main | Hybrid | Change |
|-----------|------|--------|--------|
| AVETReuse | 29ms, 16MB, 300K | 28ms, 16MB, 300K | ✅ **3% faster** |
| BatchScanScaling/1000 | 1.1ms, 1.15MB, 21K | 1.0ms, 1.15MB, 21K | ✅ **9% faster** |
| BatchScanScaling/5000 | 6.0ms, 4.6MB, 86K | 5.3ms, 4.6MB, 86K | ✅ **12% faster** |

**Why Hybrid is Faster Than Main**:
Main branch had duplicate functions: `l85HistoryIndexToBase` (in L85 encoder) and `historyIndexToBase` (in Binary encoder) - identical 15-line switch statements. The hybrid approach consolidates these into a single shared `historyIndexToBase()` in `key_encoder_base.go`.

Benefits of consolidation:
1. **Reduced binary size** - One function instead of two identical copies
2. **Better instruction cache** - Single hot function stays in cache vs two copies competing
3. **Compiler optimization** - Single definition allows better inlining/branch prediction

This demonstrates that **strategic code sharing CAN improve performance** when sharing cold/utility paths that don't benefit from inlining, while keeping hot encode/decode logic duplicated.

**Code Impact**:
| Version | Lines |
|---------|-------|
| Main (original) | 662 lines |
| Hybrid (final) | 550 lines |
| **Reduction** | **112 lines (17%)** |

**Key Takeaways**:

1. **Go interfaces have real cost** - Interface dispatch, escape analysis, and allocation overhead are measurable in hot paths. The ~10% regression and 60% more allocations came purely from abstraction.

2. **Go generics ≠ C++ templates** - Go generics provide type safety but NOT specialization. Method calls through generic type parameters still have runtime overhead. Don't expect zero-cost abstractions.

3. **DRY has limits in performance-critical code** - Sometimes duplication IS the optimization. The compiler can't inline what you've abstracted away.

4. **Hybrid approach works** - Share truly common utilities (helper functions, constants) while keeping hot paths inline. This preserves both readability and performance.

5. **Benchmark before and after** - The interface refactor looked cleaner but was 10% slower. Only benchmarks revealed the truth.

6. **Allocation count is a leading indicator** - The 60% allocation increase (300K → 476K) signaled trouble before timing showed it. Extra allocations = GC pressure = slower execution.

**Files Changed**:
- `datalog/storage/key_encoder_base.go` - Shared utilities only (37 lines)
- `datalog/storage/key_encoder_binary.go` - Full inline implementation (172 lines)
- `datalog/storage/key_encoder_l85.go` - Full inline implementation (288 lines)
- `datalog/storage/key_encoder_interface.go` - Factory unchanged (53 lines)

**Recommendation**: For performance-critical code paths called millions of times:
- Prefer inline code over interface abstraction
- Share constants, helper functions, and cold paths only
- Keep hot encode/decode/match logic duplicated but identical
- Use benchmarks to validate any consolidation attempt

### 2025-12-24: DRY Refactoring Analysis - What NOT to Consolidate

**Context: Interface Overhead Constrains Our Options**

The key encoder experiment (above) proved that Go interfaces have real cost in hot paths: 10% slower, 60% more allocations. This fundamentally constrains consolidation options:

- **With interfaces**: Could share logic via polymorphism, but pays performance penalty
- **Without interfaces**: Can only share via extracted helper functions (like `historyIndexToBase()`)

This means any consolidation must either:
1. Accept the interface overhead (unacceptable for hot paths), OR
2. Find substantive duplicated logic that can be extracted to shared functions

With this constraint, we evaluated the remaining consolidation candidates:

**Relation Interface Consolidation (REJECTED)**

Problem statement: `relation.go` has 5-6 types (MaterializedRelation, StreamingRelation, EmptyRelation, etc.) each implementing ~26 methods. Initial estimate: 300-500 lines savings.

Analysis revealed this is **NOT real duplication**:
- `Symbols()` → one-liner returning a field
- `Symbols()` → same as `Symbols()`
- `Options()` → one-liner returning a field
- `Join()` → one-liner calling `HashJoin()`

Each relation type has fundamentally different storage and iteration logic. The "26 methods" are mostly trivial accessors that are **clearer explicit than abstracted**. Consolidating would add interface overhead (as we learned from key encoders) for no readability benefit.

**Storage Iterator Consolidation (REJECTED)**

Problem statement: Four iterator implementations (`matcher_iterator_reusing.go`, `matcher_iterator_nonreusing.go`, `matcher_iterator_unbound.go`) with ~573 lines total. Initial estimate: 200-300 lines savings.

Analysis revealed these are **different iteration strategies**, not duplicated code:
- `unboundIterator`: Simple scan, no bindings
- `nonReusingIterator`: New scan per binding tuple
- `reusingIterator`: Complex range calculation, single scan across all bindings

Shared elements are minimal:
- Field declarations (`tupleBuilder`, `constraints`, `symbols`)
- One-liner validation calls
- Statistics tracking

The core `Next()` logic is completely different for each strategy. Unifying them would create a confusing abstraction with conditionals, not cleaner code.

**Key Insight: Real Duplication vs Structural Similarity**

| Pattern | Example | Should Consolidate? |
|---------|---------|---------------------|
| **Real duplication** | Two identical 15-line switch statements | ✅ YES |
| **Structural similarity** | Multiple types with same field names | ❌ NO |
| **Trivial accessors** | One-liner `Symbols()` methods | ❌ NO |
| **Different algorithms** | Multiple iterator strategies | ❌ NO |

**Decision Criteria for Future Consolidation**:
1. Is the duplicated code **substantive** (>10 lines of real logic)?
2. Would changes to one copy **always** require identical changes to others?
3. Does consolidation **improve readability** or just reduce line count?
4. Is the code on a **hot path** where interface overhead matters?

The key encoder consolidation succeeded because it met criteria 1-3: identical 15-line switch statements that would always change together, and consolidation improved readability. The hybrid approach addressed criterion 4 by keeping hot paths inline.

The relation and iterator consolidations failed criteria 1-3: the "duplication" was structural similarity (same field names, same interface) not identical logic.

**Why Interface-Based Consolidation Was Not An Option**

In languages like Java or C#, the typical solution would be a base class or interface with default implementations. In Go, this would mean:

```go
// Hypothetical base relation (NOT IMPLEMENTED)
type baseRelation struct {
    symbols []query.Symbol
    options ExecutorOptions
}
func (b *baseRelation) Symbols() []query.Symbol { return b.symbols }
// ... 20+ more delegating methods
```

But the key encoder experiment proved this approach costs 10% performance and 60% more allocations due to:
1. Interface dispatch (vtable lookup on every call)
2. Escape analysis failures (values escape to heap through interfaces)
3. Inlining prevention (compiler can't inline through interface calls)

For relations and iterators—called millions of times during query execution—this overhead is unacceptable.

**The Realistic Choices Were:**
1. ❌ Interface-based consolidation → Rejected due to proven performance cost
2. ❌ Extract shared logic to helper functions → Nothing substantive to extract (only trivial accessors)
3. ✅ Keep explicit implementations → Maintains performance, code is already clear

**Conclusion**: The interface overhead lesson from key encoders eliminated our primary consolidation tool. What remained wasn't real duplication—just structural similarity that's clearer left explicit. Line count reduction is not a goal; code clarity and performance are.

---

### 2025-12-17: Schema Support Implementation & Benchmarking
- Implemented Datomic-compatible schema support with type validation, cardinality, and uniqueness
- Schema definable via EDN file or Go builder API (same internal representation)
- Added plan-time resolution for Pull patterns (schema lookup once per pattern, not per entity)
- Created comprehensive benchmark suite for schema performance
- **Measured Results**:
  - Type validation overhead: **<0.2%** (essentially free)
  - Uniqueness checking overhead: **~5.8%** (requires database query)
  - Bulk operations with schema: **~1.4%** overhead
  - Schema resolution: **225ns** one-time cost per pattern
  - Cardinality-many: ~370ns per additional value (linear scaling)
- **Key Finding**: Schema validation is essentially free; use freely for type safety
- **Files created**: `datalog/schema/` package, `docs/reference/SCHEMA.md`, `examples/schema.go`

### 2025-12-17: Pull API Implementation & Benchmarking
- Implemented Datomic-style Pull API with nested references and cycle detection
- Created comprehensive benchmark suite for Pull performance
- **Measured Results**:
  - Pull vs Query: **9.2× faster** (3.5µs vs 32.7µs for 3 attributes)
  - Wildcard: **2.2× faster** than explicit attributes (single scan vs N lookups)
  - Per-attribute cost: ~1.2µs (BadgerDB), ~470ns (in-memory)
  - Per-entity cost: ~2.5µs (BadgerDB), ~1µs (in-memory)
  - Linear scaling with both attributes and entities
- **Key Finding**: Pull avoids query parsing/planning overhead entirely
- **Recommendation**: Cache parsed patterns for hot paths (20% speedup)

### 2025-10-25: Single-Use Iterator Semantics & Performance Verification (Sessions 1-2)
**Session 1**: Initial benchmarking after single-use iterator semantics implementation
- Implemented proper single-use iterator semantics for StreamingRelation
- Added BufferedIterator for safe re-iteration support
- Ran comprehensive performance benchmarks to verify all claims
- **Updated all performance claims to reflect reality**:
  - Iterator composition: 4.2× (was claiming 17.5×)
  - Streaming execution: 1.9× (was claiming 28×)
  - Parallel execution: 2.2× with 8 workers (new measurement)
  - Predicate pushdown: 1.77× (new measurement)

**Session 2**: Debug output cleanup and precise verification
- Discovered debug prints polluting benchmark output (79 fmt.Printf statements)
- Fixed 3 unguarded debug prints in join.go
- Re-ran all benchmarks with clean output
- **Updated to precise measurements**:
  - Iterator composition: **4.06×** (1,259μs → 310μs), 89% memory reduction
  - Streaming execution: **2.22×** (1,720μs → 774μs), 52% memory reduction
  - Parallel execution: **2.06×** (730ms → 355ms) with 8 workers
  - Predicate pushdown (small dataset): **1.58×** (33.6ms → 21.3ms), 49% memory reduction
  - Predicate pushdown (large dataset): **2.78×** (1,043ms → 375ms), 91.5% memory reduction
- Fixed BenchmarkOHLCQueryLargeDataset transaction size bug (commit per-symbol-per-day)
- Key lesson: **Clean benchmarks reveal precise truth, and predicate pushdown scales better with larger datasets**

### 2025-10-08: Profiling, Hash Join & In-Memory Indexing
- Profiled entire OHLC execution chain (in-memory + storage)
- Identified hash join as 35% of allocations
- Implemented map pre-sizing: 24-32% faster, 24-30% less memory
- Confirmed time range optimization too fast to profile (10µs)
- Identified pattern matching as 58% CPU in-memory → implemented hash indices
- **Massive win**: Entity lookups 49-4802× faster, test suite now 7s (down from timeouts)

### 2025-10-07: Time Range Optimization
- Implemented semi-join pushdown via time range constraints
- Achieved 4× speedup on hourly OHLC (41s → 10.2s)
- Fixed daily OHLC regression with size check
- Optimized extractTimeRanges: 4.7× faster, 108× fewer allocations

### 2026-03-28: LZ77+FSE Compression Codec (Phase 1-3)

Custom-owned LZ77+FSE compression codec for transparent value compression in storage keys.
Implementation: `datalog/codec/compress.go`, `fse.go`, `lz77.go`, `sequences.go`.
Design: `docs/proposals/COMPRESSED_STRING_VALUES.md`, `docs/proposals/COMPRESSION_RESEARCH.md`.

**Compression Ratios (LZ77 + FSE, verified):**

| Data Type | Ratio |
|-----------|-------|
| English prose 1KB | 3.6× |
| EDN structured data | 12.9× |
| Source code | 10.6× |
| Repetitive binary | 12.7× |
| All-same bytes | 13.3× |
| Repeated text 50KB | 110× |

**Full Pipeline Throughput (Apple M5 Max, verified):**

| Operation | 256B | 1KB | 4KB | 16KB |
|-----------|------|-----|-----|------|
| Compress | 14 MB/s | 43 MB/s | 112 MB/s | 284 MB/s |
| Decompress | 1.7 GB/s | 2.1 GB/s | 2.3 GB/s | 2.4 GB/s |

**Per-Value Latency:**

| Operation | 256B | 1KB | 4KB |
|-----------|------|-----|-----|
| Compress | 18μs | 24μs | 37μs |
| Decompress | 153ns | 477ns | 1.8μs |

**Allocations:**

| Operation | 256B | 1KB | 4KB | 16KB |
|-----------|------|-----|-----|------|
| Compress | 38 | 56 | 74 | 103 |
| Decompress | 6 | 7 | 7 | 9 |

**Key optimizations:**
- FSE decode table cached via `sync.Map` — eliminates table reconstruction on repeated decompressions
- Encode work buffers pooled via `sync.Pool` — reduces GC pressure on write path
- LZ77 reconstruct: 2.6 GB/s, 1 alloc (just output buffer)
- Determinism verified: 1000× repeated compression + concurrent goroutine compression produce identical output

**Test coverage:** 185 tests including round-trip fuzz (500 random + 200 text + 100 structured), determinism stress, golden byte-level output assertions, safety net validation, and compression ratio assertions.

### 2025-10-04: Parallel Execution & Intern Optimization
- Identified intern cache as 35% CPU bottleneck
- Replaced sync.RWMutex with sync.Map → 6.26× BadgerDB speedup
- Fixed index selection to use AEVT when E+A both bound
- Performance gains: In-memory 6.9×, BadgerDB 1.63× → 6.26×
