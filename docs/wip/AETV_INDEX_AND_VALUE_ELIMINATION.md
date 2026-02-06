# AETV Index and Value Elimination

**Date**: 2026-02-05
**Status**: Design
**Author**: Discussion between user and Claude

## Overview

This document proposes two related storage layer changes:

1. **Replace EAVT with AETV** - EAVT is redundant; EATV covers all E-primary use cases. AETV completes the symmetry for A-primary CRDT-aware queries.
2. **Eliminate redundant values** - Store only keys since all datom information is already encoded there.

## Motivation

### Problem 1: CRDT Resolution Fails for A-Primary Queries

The `CRDTResolvingIterator` assumes Tx descending order (highest Tx first = LWW winner). This works with EATV but fails with AEVT.

**Current index pairs:**
| E-primary | A-primary |
|-----------|-----------|
| EAVT (E → A → V → Tx) | AEVT (A → E → V → Tx) |
| EATV (E → A → Tx↓ → V) | **missing** |

EATV is the "CRDT-aware" version of EAVT. But there's no CRDT-aware A-primary index.

**The bug:** When E is bound via input and A is constant, the selector picks AEVT. CRDTResolvingIterator wraps it but gets wrong Tx order → returns wrong value.

### Problem 2: EAVT is Redundant

Every use case for EAVT is handled equally well (or better) by EATV:

| Use Case | EAVT | EATV |
|----------|------|------|
| All attributes for entity E | E prefix scan | E prefix scan (same) |
| Value of E.A | (E, A) prefix | (E, A) prefix + CRDT resolution |
| History of E.A | (E, A) scan, Tx ascending | (E, A) scan, Tx descending (newest first) |
| Full scan | Works | Groups by (E, A) for CRDT |
| Does E.A have value V? | O(1) seek | O(n) scan, but n is small |

**Conclusion:** EAVT provides no unique value. Replace it with AETV.

### Problem 3: Redundant Value Storage

Current key format contains ALL datom information:
```
[prefix:1][...component order...][V:variable][Tx:16][Op:1][AfterRef?:16]
```

Current value format duplicates this:
```
[E:20][A:32][Tx:16][VSize:2][VType:1][V:variable]
```

The codebase already uses `ScanKeysOnly()` for nearly all reads. Values are 100% redundant.

## Proposed Solution

### Part 1: Replace EAVT with AETV

**Before (6 indices):**
- EAVT (E → A → V → Tx)
- EATV (E → A → Tx↓ → V)
- AEVT (A → E → V → Tx)
- AVET (A → V → E → Tx)
- VAET (V → A → E → Tx)
- TAEV (T → A → E → V)

**After (6 indices):**
- ~~EAVT~~ removed
- EATV (E → A → Tx↓ → V)
- **AETV (A → E → Tx↓ → V)** - NEW
- AEVT (A → E → V → Tx)
- AVET (A → V → E → Tx)
- VAET (V → A → E → Tx)
- TAEV (T → A → E → V)

**Symmetry achieved:**
| E-primary | A-primary |
|-----------|-----------|
| EATV (E → A → Tx↓ → V) | AETV (A → E → Tx↓ → V) |

### Part 2: Index Selection Refactoring

**Current selector is broken.** It doesn't consistently choose CRDT-aware indices.

**Optimal Index Selection Matrix:**

| E | A | Schema | Cardinality | Optimal Index | Reason |
|---|---|--------|-------------|---------------|--------|
| constant | constant | any | One/Vector | EATV | Single (E,A), Tx↓ for CRDT |
| constant | constant | any | Many | EATV | CRDTResolvingIterator handles add-wins |
| from input | constant | yes | One/Vector | **AETV** | Many Es, single A, Tx↓ for CRDT |
| from input | constant | yes | Many | AEVT | Need V grouping for add-wins |
| unbound | constant | yes | One/Vector | **AETV** | All Es for A, Tx↓ for CRDT |
| unbound | constant | yes | Many | AEVT | Need V grouping for add-wins |
| constant | unbound | any | any | EATV | Single E, all As |
| unbound | unbound | any | any | EATV or TAEV | Full scan |

**Key insight:** EAVT never appears as optimal. EATV handles all E-primary cases.

### Part 3: Eliminate Values

Change `assertDatom()` to write nil values:
```go
// Before
value := sd.Bytes()
txn.Set(key, value)

// After
txn.Set(key, nil)
```

## Use Cases for AETV

### 1. Batch Entity Attribute Lookup (the bug case)
```datalog
[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]
```
- 1000 Es from input, single A
- AETV: Single scan, stream CRDT resolution
- Current (AEVT): Wrong Tx order, broken CRDT

### 2. Attribute Enumeration with CRDT
```datalog
[:find ?e ?v :where [?e :status ?v]]
```
- All entities with `:status`, need current value
- AETV: Single scan, first entry per (A, E) is LWW winner

### 3. Pull API Optimization
```go
// Batch pull - same attributes for many entities
db.PullMany(entities, `[:person/name :person/age :person/email]`)
```
- For each attribute, AETV enables single scan
- Gets all entities' current values in one pass
- O(attributes) scans instead of O(entities × attributes) seeks

### 4. Time-Travel Per Attribute
```datalog
[:find ?e ?v :in $ ?as-of :where [?e :price ?v] [(as-of ?as-of)]]
```
- "What was :price for all entities at time T?"
- AETV: Scan, find first Tx ≤ target per (A, E)

### 5. Attribute History/Audit
```datalog
[:find ?e ?v ?tx :where [?e :user/password ?v ?tx] [(history)]]
```
- All changes to `:user/password` across all users
- AETV: Natural grouping by (A, E), Tx descending (newest first)

### 6. Schema Migration
```datalog
[:find ?e :where [?e :old/attribute _]]
```
- Find all entities with deprecated attribute
- AETV: Single scan of `:old/attribute`

### 7. Cross-Entity Joins
```datalog
[:find ?e1 ?e2
 :where [?e1 :person/name ?name]
        [?e2 :person/name ?name]
        [(not= ?e1 ?e2)]]
```
- First clause scans `:person/name` via AETV
- Gets all (entity, current-name) pairs efficiently

## Index Purpose Summary

| Index | Order | Primary Use Case |
|-------|-------|------------------|
| EATV | E → A → Tx↓ → V | E-primary with CRDT (single entity queries) |
| **AETV** | A → E → Tx↓ → V | A-primary with CRDT (batch entity, Pull API) |
| AEVT | A → E → V → Tx | CardinalityMany add-wins (need V grouping) |
| AVET | A → V → E → Tx | Unique lookups ("entity where A=V") |
| VAET | V → A → E → Tx | Reverse refs ("what references V?") |
| TAEV | T → A → E → V | Transaction log ("what changed in Tx?") |

**Note:** EAVT is intentionally absent. EATV covers all its use cases with better CRDT support.

## Cost Analysis

### Storage Cost

| Metric | Before | After |
|--------|--------|-------|
| Number of indices | 6 | 6 (same) |
| Keys per datom | 6 | 6 (same) |
| Values per datom | 6 × ~70 bytes | 0 |
| **Net change** | - | **~50% reduction** |

Replacing EAVT with AETV: zero additional storage.
Eliminating values: ~50% storage reduction.

### Write Cost

| Metric | Before | After |
|--------|--------|-------|
| BadgerDB Set calls | 6 | 6 (same) |
| Bytes per write | key + value | key only |
| **Net change** | - | **~50% reduction** |

## Implementation Plan

### Phase 1: Add AETV Index (keep EAVT temporarily)

1. Add `AETV IndexType` constant
2. Add AETV to key encoders (L85 and Binary)
3. Add AETV to key decoders
4. Update `assertDatom()` to write to AETV
5. Update index selector to use AETV for A-primary CRDT cases
6. Add comprehensive tests
7. Run full test suite

### Phase 2: Remove EAVT Index

1. Remove EAVT from `Indices` slice
2. Update index selector to use EATV for former EAVT cases
3. Remove EAVT from key encoders/decoders
4. Update tests that explicitly reference EAVT
5. Add migration: ignore EAVT keys in old databases
6. Run full test suite

### Phase 3: Eliminate Values

1. Change `assertDatom()` to write nil values
2. Update `Get()` to decode from key
3. Remove any remaining value readers
4. Remove `StorageDatom.Bytes()` method
5. Add migration note (old values ignored)
6. Run full test suite

### Phase 4: Cleanup

1. Update CLAUDE.md with new index structure
2. Archive this design doc
3. Update ARCHITECTURE.md

**Architectural TODO (future):** Evaluate removing AEVT. With AETV + CRDTResolvingIterator handling CardinalityMany, AEVT's V-grouping may also be redundant.

## Testing Strategy

### 1. Key Encoding/Decoding Tests

```go
func TestAETVKeyEncoding(t *testing.T) {
    // Test AETV key format: [prefix][A][E][Tx↓][V][Op][AfterRef?]
    // Verify encode/decode round-trip
    // Verify Tx is encoded with bitwise NOT (descending order)
}

func TestAETVSortOrder(t *testing.T) {
    // Same (A, E) with different Tx should sort by Tx descending
    // Higher Tx should sort FIRST (lower byte value)
}
```

### 2. Index Selection Tests

```go
func TestIndexSelectionMatrix(t *testing.T) {
    // Test all rows of the selection matrix
    // Verify correct index is chosen for each scenario
}

func TestAETVSelectedForInputBoundE(t *testing.T) {
    // Pattern: [?e :attr ?v] with ?e from input bindings
    // Schema: CardinalityOne
    // Verify: AETV is selected (not AEVT)
}
```

### 3. CRDT Resolution Tests

```go
func TestAETVCRDTResolutionMultipleEntities(t *testing.T) {
    // 100 entities, same attribute, multiple writes per entity
    // Query with all Es from input, DisableCache: true
    // Verify: Each E returns only its LWW winner
}

func TestAETVCRDTResolutionCacheDisabled(t *testing.T) {
    // THE critical test that currently fails
    // Must pass after AETV implementation
}
```

### 4. Pull API Tests

```go
func TestPullManyWithAETVOptimization(t *testing.T) {
    // Pull same attributes for 1000 entities
    // Verify: Uses AETV scans, not per-entity seeks
}
```

### 5. EAVT Removal Tests

```go
func TestEATVHandlesFormerEAVTCases(t *testing.T) {
    // All EAVT use cases work with EATV
    // - All attributes for entity
    // - Value of E.A
    // - History of E.A
}

func TestMigrationFromEAVT(t *testing.T) {
    // Old database with EAVT keys
    // Verify: Reads still work (EAVT keys ignored or handled)
}
```

### 6. Value Elimination Tests

```go
func TestNilValueStorage(t *testing.T) {
    // Write datoms with nil values
    // Read back via ScanKeysOnly
    // Verify: All fields decoded from key
}
```

### 7. Benchmarks

```go
func BenchmarkAETVVsAEVTBatchLookup(b *testing.B) {
    // Compare AETV single scan vs AEVT with broken CRDT
    // For various batch sizes
}

func BenchmarkStorageSizeReduction(b *testing.B) {
    // Measure actual storage with/without values
}
```

## Success Criteria

1. All CRDT tests pass with `DisableCache: true`
2. Index selection matches the optimal matrix
3. Storage reduced by ~50% (value elimination)
4. No performance regression for existing queries
5. Pull API benefits from AETV optimization

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| EAVT removal breaks something | Phase 1 adds AETV while keeping EAVT; Phase 2 removes after validation |
| Migration from old databases | EAVT keys ignored; values ignored; backward compatible |
| Index selection regressions | Comprehensive matrix-based testing |
| Performance regression | Benchmark before/after each phase |
