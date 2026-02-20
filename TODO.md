# TODO - Realistic Roadmap

## Current System Status

### What's Working Well ✅
- Complete Datalog query engine with EAVT storage
- BadgerDB persistent storage with **7 indices** (EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV)
- Pattern matching, joins, and predicates
- Expression clauses with arithmetic and string operations
- Aggregations (sum, count, avg, min, max)
- Subqueries with proper scoping (semantically correct)
- Order-by clause with multi-column sorting
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
- **Conditional aggregate rewriting (7.7× faster correlated aggregates)**
- **AETV index for A-primary CRDT resolution**
- **Value elimination: ~50% storage reduction (keys-only storage)**

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
**Status**: ✅ COMPLETE (February 2026)
**Result**: 7.7× faster correlated aggregates

### ✅ NOT/OR Clauses
**Status**: ✅ COMPLETE with Datomic-compatible semantics

### ✅ CRDT Storage Layer
**Status**: ✅ COMPLETE - LWW, add-wins sets, RGA vectors with history queries

### ✅ AETV Index and Value Elimination
**Status**: ✅ COMPLETE (February 2026)
**Result**: Proper A-primary CRDT resolution, ~50% storage reduction

## Medium Term (1-2 Months)

### Query Engine Enhancements
1. **Collection Binding**: `[?x ...]` for set inputs
2. **Distinct Aggregation**: `(count-distinct ?x)`

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

*Benchmarked on Apple M4 Max. See PERFORMANCE_STATUS.md for methodology.*

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
- [x] Consolidate performance documentation → `PERFORMANCE_STATUS.md` created
- [ ] Archive historical optimization docs → Move to `docs/archive/`
- [ ] Fix examples/ package conflicts → 37 programs can't run via `go test`

### Medium Priority
- [ ] Remove experimental key_mask_iterator_v2.go → Not integrated, benchmarked slower
- [ ] Improve error messages → Add context to query failures
- [ ] Add query explain plan → Show execution strategy
- [ ] Profile memory allocations → Identify hot paths

### Low Priority
- [ ] Add integration test suite → End-to-end query scenarios
- [ ] Add query metrics/telemetry → Production observability
