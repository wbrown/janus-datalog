# TRIAGE: CardinalityVector Test Coverage Gaps

**Date**: 2026-02-06
**Priority**: Medium (no known bugs, but significant blind spots)
**Related**: FIX_SCHEMALESS_CRDT_RESOLUTION.md, BUG_SCHEMALESS_ATTR_BOUND_QUERY.md

## Context

During the schemaless CRDT resolution fix, we identified that CardinalityVector
is unique to Janus Datalog (not in Datomic) and easy to overlook. An audit of
existing test coverage revealed good happy-path coverage but critical gaps in
edge cases and integration scenarios.

## Current Coverage (~59 tests)

| Category | Tests | Status |
|----------|-------|--------|
| Basic CRUD (Add/Set/Remove) | 10 | Passing |
| Set() backwards-diff optimization | 9 | Passing |
| RGA encoding/decoding | 4 | Passing |
| RGA reconstruction | 3 | Passing |
| Schema/validation | 2 | Passing |
| Query integration (E-bound, joins) | 4 | Passing |
| Pull API | 1 | Passing |
| Vector caching | 11 | Passing |
| CRDT resolution (resolveRGAGroup) | 7 | Passing |
| Cache matrix | 2 | Passing |
| Bug #5 AVET/VAET index | 5 | Some failing |
| Reflect API | 9 | Expected to fail (unimplemented) |
| Vector functions (nth, count) | 5+ | Passing |

## Gap 1: CRDTResolvingIterator with nil schema (HIGH)

**Risk**: Correctness

Vectors require schema to be recognized as CardinalityVector. With the
"always apply CRDTResolvingIterator" change, a nil-schema matcher would treat
vector datoms as CardinalityUnknown (add-wins). This would:

- Treat `OpRGAInsert` as unknown Op (dropped by processAddWins)
- Treat `OpRGATombstone` as unknown Op (dropped by processAddWins)
- Return zero results instead of ordered vector

**Mitigation**: Vectors are always schema-defined, so this only occurs if a
matcher is created without schema for a schema-aware database. No known
production path does this, but `NewBadgerMatcher(store)` in tests does.

**Test needed**: Query vector attribute through nil-schema matcher, verify
behavior is defined (even if degraded).

## Gap 2: Time-travel queries (MEDIUM)

**Risk**: Correctness

No tests for `[(as-of ?tx N)]` with vectors. RGA reconstruction depends on
seeing all inserts/tombstones up to a point in time. The `txID` filter in
CRDTResolvingIterator (line 97-99) filters datoms before they reach
`accumulateRGA()`. This should work but is untested.

**Tests needed**:
- Write vector across multiple transactions
- Query at each transaction point, verify vector state matches

## Gap 3: V-bound queries with vectors (MEDIUM)

**Risk**: Correctness

Only Bug #5 tests cover V-bound vector queries, and some are failing. The
interaction between `validatingVBoundIterator` and CardinalityVector is unclear.

**Questions**:
- What does `[?e :skills "Go"]` mean for a vector? Membership check?
- Should it use candidate+validate like CardinalityOne?
- Or should it use add-wins-style Op check?

**Tests needed**:
- V-bound membership query on vector attribute
- V-bound query after element tombstoned

## Gap 4: Concurrent/distributed RGA operations (MEDIUM)

**Risk**: CRDT convergence guarantees

No tests verify that concurrent inserts at the same position converge to the
same order across replicas. The RGA algorithm sorts by ElementID for
tiebreaking, which should be deterministic, but this isn't tested with
realistic concurrent scenarios.

**Tests needed**:
- Two "replicas" insert at same AfterRef, verify deterministic order
- Insert + tombstone of same element, verify convergence
- Out-of-order delivery of operations, verify same final state

## Gap 5: Reflect layer — stale comments (LOW)

**Risk**: Documentation accuracy

9 reflect tests have "EXPECTED TO FAIL" comments but ALL PASS. The reflect
layer for CardinalityVector works correctly. The comments are stale and
misleading — they should be removed.

**Action**: Remove stale "EXPECTED TO FAIL" comments from
`datalog/reflect/vector_test.go`.

## Gap 6: tx.Set() semantics change impact (HIGH - if we proceed)

**Risk**: Regression

If `tx.Set()` is changed to error on CardinalityOne (per current discussion),
we must verify:

- `tx.Set()` CardinalityVector path still works
- `tx.Set()` CardinalityMany path still works
- All existing tests that use `tx.Set()` for CardinalityOne are migrated to `tx.Add()`
- No test creates CardinalityOne datoms exclusively via `tx.Set()`

**Audit needed**: grep all `tx.Set(` and `\.Set(` calls in test files,
categorize by cardinality.

## Gap 7: Large-scale vectors (LOW)

**Risk**: Performance

No benchmarks or stress tests for vectors with 100+ elements. RGA
reconstruction is O(n) with a hash map, but `accumulateRGA` uses linear scan
to find tombstone targets (line 253-261 of crdt_resolving_iterator.go).

**Not urgent** — performance issue, not correctness.

## Recommended Priority

1. **Gap 6** — Must address if proceeding with `tx.Set()` semantics change
2. **Gap 1** — Needed for "always apply CRDTResolvingIterator" correctness
3. **Gap 2** — Time-travel is a core feature, vectors should work with it
4. **Gap 3** — V-bound queries are a common pattern
5. **Gap 4** — CRDT convergence is a theoretical guarantee we should verify
6. **Gap 7** — Performance, defer
7. **Gap 5** — Known limitation, defer
