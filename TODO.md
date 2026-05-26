# TODO - Realistic Roadmap

## Current System Status

### What's Working Well ✅
- Complete Datalog query engine with EAVT storage
- BadgerDB persistent storage with **8 indices** (EAVT, EATV, AEVT, AETV, ATEV, AVET, VAET, TAEV)
- Pattern matching, joins, and predicates
- Expression clauses with arithmetic and string operations
- Aggregations (sum, count, avg, min, max)
- Subqueries with proper scoping (semantically correct)
- Order-by clause with multi-symbol sorting
- Time extraction functions
- **Pull API with nested references and cycle detection (9× faster than queries)**
- **Schema support: type validation, cardinality, uniqueness (<1% write overhead)**
- **True streaming execution (2.22× speedup, 52% memory reduction, up to 91.5% on large datasets)**
- **Iterator composition with lazy evaluation (4.06× speedup)**
- **Parallel subquery execution (2.06× speedup with 8 workers)**
- **Predicate pushdown (1.58-2.78× speedup, scales with dataset size)**
- **Lock-free intern caches (6.26× BadgerDB speedup)**
- Query plan caching (3× speedup, active)
- Relation collapsing algorithm (prevents memory explosion)
- **CRDT storage: LWW for cardinality-one, add-wins for cardinality-many, RGA for vectors**
- **NOT/OR clauses: `(not ...)`, `(not-join ...)`, `(or ...)`, `(or-join ...)` with Datomic-compatible semantics**
- **History/time-travel queries: `db.History()` and `db.AsOf(elementID)` API**
- **Multi-source queries: named sources, SourceRouter, cross-source joins**
- **QueryInto API: typed query results via struct tag mapping**
- **Database export/import: EDN format for backup and migration**
- **Conditional aggregate rewriting** (folded into the default algebra optimizer; no separate flag — 7.7× in the original standalone benchmark)
- **AETV index for A-primary CRDT resolution**
- **ATEV index for O(1) attribute high-water mark** (cache freshness gate; 555× faster than the prior AEVT scan at 10K datoms-per-attribute)
- **Value elimination: ~50% storage reduction (keys-only storage)**
- **LZ77+FSE compression codec** (3.6× on prose, 10-13× on structured/repetitive; 2.1-2.4 GB/s decompression; deterministic; Tier-3 blob store for large values)
- **Iterator-error contract enforced across executor + storage**: deferred storage errors (Tier-3 blob decode, etc.) ride through every materialization, join, sort, projection, union, and subquery boundary instead of being laundered into clean partial results
- **Collection binding `[?x ...]`** for set inputs (parser + executor wired through subqueries, OR fallback, and input handling)

### Production Readiness: ✅ Ready

The engine is **functionally complete, semantically correct, and blazingly fast**. All core Datalog features are implemented, CRDT storage provides conflict-free replication, and performance exceeds targets by 2-227×.

**The only caveat:** This is a research-oriented project where the author makes breaking API changes between versions. If you need API stability, pin your version.

## Completed (Historical Reference)

These items have been completed and are preserved for historical context:

### ✅ Fix AEVT Index Performance Bug
**Problem**: AEVT scans 12.8M datoms instead of 65 for E+A bound patterns
**Status**: ✅ RESOLVED (commit 0565b85)

### ✅ True Streaming Execution
**Problem**: StreamingRelation forced materialization at every operation
**Status**: ✅ IMPLEMENTED (October 2025)
**Result**: 2.22× speedup, 52-91.5% memory reduction

### ✅ Conditional Aggregate Rewriting
**Problem**: Cross-phase expression dependencies cause projection failures
**Status**: ✅ COMPLETE (February 2026). Folded into the default algebra optimizer in 2026-05 — the standalone `EnableConditionalAggregateRewriting` flag was removed; the rewrite now runs unconditionally inside `EnableAlgebraOptimizer`.
**Result**: 7.7× faster correlated aggregates (original standalone benchmark)

### ✅ NOT/OR Clauses
**Status**: ✅ COMPLETE with Datomic-compatible semantics

### ✅ CRDT Storage Layer
**Status**: ✅ COMPLETE - LWW, add-wins sets, RGA vectors with history queries

### ✅ AETV Index and Value Elimination
**Status**: ✅ COMPLETE (February 2026)
**Result**: Proper A-primary CRDT resolution, ~50% storage reduction

### ✅ LZ77+FSE Compression Codec
**Status**: ✅ COMPLETE (March 2026, v0.11.0)
**Result**: 3.6× on prose, 10-13× on structured data; 2.1-2.4 GB/s decompression; Tier-3 blob store with content-hash deduplication

### ✅ Iterator-Error Propagation Contract
**Status**: ✅ COMPLETE (v0.11.x + v0.12.0)
**Result**: Deferred iterator errors (e.g., Tier-3 blob decode failures surfacing via Error() after Next() returns false) propagate through every materialization, join, sort, projection, union, and subquery path. Five PRs (#68, #77, #78, #79, #83) brought every observed path under the contract. Relation.Sorted() signature changed to `([]Tuple, error)`. Static guard test now fails the build if any collectTuplesInto call drops its error.

### ✅ ATEV Index — O(1) Attribute High-Water Mark
**Status**: ✅ COMPLETE (May 2026, v0.12.0)
**Result**: 8th index added; Cache.IsAttributeFresh and MaxElementIDForAttribute become constant-time. 2.2× → 555× faster than the prior AEVT scan at 10–10,000 datoms-per-attribute. Costs ~14% more write work per commit.

### ✅ Concurrency Hardening (v0.12.0)
**Status**: ✅ COMPLETE
**Result**: Four races fixed — UnionRelation iterator/cache (PR #70), planner handler shared state (PR #74), TupleKey time.Time hash nondeterminism (PR #76, was silently flaking CI), Identity L85 lazy cache (PR #80, cache removed in favour of interned-pointer keys: ~2.7× faster index path).

### ✅ Collection Binding `[?x ...]`
**Status**: ✅ COMPLETE
**Result**: Parser and executor wired through subqueries, OR fallback, and `:in` input handling.

## Medium Term (1-2 Months)

### Query Engine Enhancements
1. **Distinct Aggregation**: `(count-distinct ?x)` and a `distinct` modifier on existing aggregates

### Performance Optimizations
1. **Parallel Pattern Execution**: For independent patterns (complex dependency analysis)
2. **Statistics Collection**: For better query planning (requires architecture changes)
3. **Adaptive Streaming Strategy**: Automatically choose streaming vs materialized based on data

## Long Term (3-6 Months)

### Major Features
1. **Rules System**: Named, reusable query fragments
2. **Recursive Queries**: Graph algorithms
3. **Transaction Functions**: Custom transaction logic

### Infrastructure
1. **WASM Build**: Browser deployment
2. **Distributed Execution**: Multi-node queries
3. **Incremental View Maintenance**: Real-time aggregations
4. **Query Timeout/Cancellation**: Resource limits

## ~~Won't Do~~ (I Lied)

These were originally listed as "out of scope" but got implemented anyway:

- ~~Full history/time-travel queries~~ → ✅ **Done** via CRDT storage with `db.History()` and `db.AsOf(elementID)`
- ~~Would need different storage model~~ → The CRDT refactor made it natural

### Actually Won't Do
- **Full Datomic compatibility** - Different philosophy (simpler, Go-idiomatic)
- **Lazy evaluation throughout** - Go doesn't support well; we use streaming instead

## Success Metrics

### Performance Targets (Crushed)

| Target | Goal | Achieved | How Much Better |
|--------|------|----------|-----------------|
| Simple queries | <10ms | **44µs** | **227× faster** |
| Complex queries | <100ms | **21ms** | **5× faster** |
| OHLC aggregations | <5s/month | **2-4s** | **~2× faster** |
| Memory usage | <100MB | **30MB** | **3× less** |

*Benchmarked on Apple M5 / M5 Max. See PERFORMANCE_STATUS.md for methodology and per-optimization measurements (16 entries through v0.12.0).*

### Code Quality
- Test coverage: >80%
- All packages have examples
- Core APIs documented
- No V2 functions

## How to Contribute

### Best First Issues
1. Add missing aggregation functions (median, mode)
2. Implement string functions (upper, lower, trim)
3. Add more time extraction functions
4. Write benchmark tests

### Before Starting Work
1. Check this TODO to ensure it's still needed
2. Discuss approach in an issue
3. Write tests first
4. Keep changes focused

## Technical Debt to Address

### High Priority
- [ ] Archive historical optimization docs → Move to `docs/archive/`

### Medium Priority
- [ ] Improve error messages → Add context to query failures (the iterator-error contract surfaces the right error now, but the wording at user-facing boundaries could carry more context)
- [ ] Add query explain plan → Show execution strategy (annotations system already exposes per-phase events; a packaged `Explain()` API on top would be the user-facing surface)

### Low Priority
- [ ] Add query metrics/telemetry → Production observability beyond what `annotations.Handler` already provides

### Resolved (historical reference)
- [x] Consolidate performance documentation → `PERFORMANCE_STATUS.md` created
- [x] Fix examples/ package conflicts → v0.11.0 overhauled examples; 14 working programs, no internal-package imports except the escape-hatch example
- [x] Remove experimental key_mask_iterator_v2.go → File removed
- [x] Profile memory allocations → PERFORMANCE_STATUS.md entries through #16 include per-allocation breakdowns
- [x] Add integration test suite → `tests/` directory holds end-to-end scenarios
