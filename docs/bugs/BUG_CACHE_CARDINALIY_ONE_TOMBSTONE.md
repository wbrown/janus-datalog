# janus-datalog Bug: Cache Path Does Not Resolve CardinalityOne Remove() Tombstones

## Summary

After calling `Remove()` on a CardinalityOne (LWW) attribute, both `PullInto()`
and bound-E queries still return the old value. The `OpCRDTRemove` tombstone is
written correctly, and the streaming resolution path (`CRDTResolvingIterator`)
handles it, but the cache rebuild path (`ResolveLWW`) does not check the Op field.

Since PullInto and bound-E queries both resolve through the EA cache, neither
sees the tombstone.

**Affected version:** `v0.7.1-0.20260207223550-7bd4edda3e6b`

## Reproduction

Discovered in a downstream application using janus-datalog. The bug is
in the storage layer and is reproducible with any CardinalityOne attribute.

Three reproduction scenarios:

1. **PullInto after Remove** — Set a CardinalityOne attribute (e.g.,
   `:person/status`), then Remove() it. PullInto still returns the old
   value instead of nil. **FAILS.**

2. **Join-bound query after Remove** — Same setup, but checks query results.
   Query `[?e :person/status ?s]` still matches the tombstone datom after
   Remove(). **FAILS.** (E is bound via a prior clause, so this goes through
   the cache path, not the streaming iterator.)

3. **Set after Remove** — Set() after Remove() correctly updates both
   queries and PullInto. The new `OpCRDTSet` datom has a higher ElementID
   and wins LWW resolution, masking the tombstone.

## Test Results

```
=== RUN   TestCacheRemove_PullInto_RoundTrip
    Value after Remove(): active (expected nil)
    BUG: PullInto returns old value after Remove() on CardinalityOne.
--- FAIL

=== RUN   TestCacheRemove_JoinBoundE_RoundTrip
    Matched rows after Remove(): 1 (expected 0)
    BUG: Query returns stale data after Remove() on CardinalityOne.
--- FAIL

=== RUN   TestCacheRemove_PullInto_ThenReAdd
--- PASS
```

## Two Resolution Paths

janus-datalog has two CRDT resolution paths (per CRDT_STORAGE_SEMANTICS.md):

| Path | When Used | CardinalityOne Tombstone Handling |
|------|-----------|----------------------------------|
| **EA Cache** (`ResolveLWW`) | PullInto, bound-E queries | **Missing** — returns first datom's V without checking Op |
| **Streaming** (`CRDTResolvingIterator`) | Unbound-E scans, `DisableCache: true` | **Present** — checks `datom.Op == OpCRDTRemove`, skips group |

From the CRDT storage semantics doc:

> "When the cache is bypassed (e.g., unbound E scans, `DisableCache: true`), CRDT resolution
> is applied at the storage scan level via `CRDTResolvingIterator`."

Both reproduction scenarios use bound E (PullInto has a specific entity; the query binds `?e` via
a prior clause before evaluating the CardinalityOne attribute pattern).
So both go through the cache path, both hit `ResolveLWW`, and neither sees the tombstone.

## Sequence of Operations

```
1. tx.Set(entity, :person/status, "active")  → Commit
   Storage: [entity, :person/status, "active", EID₁, OpCRDTSet]

2. tx.Remove(entity, :person/status, "active") → Commit
   Storage: [entity, :person/status, "active", EID₂, OpCRDTRemove]
   (EID₂ > EID₁)

3. PullInto(entity, &person)
   → EntityResolver → EA cache miss → ResolveLWW()
   → EATV scan: first entry = EID₂ (highest, descending order)
   → Returns datom.V without checking datom.Op
   → BUG: person.Status = "active" (should be absent)

4. Query: [:find ?e :in $ ?dept :where
           [?e :person/department ?dept]  ← binds ?e
           [?e :person/status ?s]]        ← bound E → cache path
   → EA cache for (entity, :person/status) → ResolveLWW()
   → Same as above: returns stale value
```

## Root Cause

The cache rebuild for CardinalityOne (`ResolveLWW`) does not check `datom.Op`:

`datalog/storage/cache_resolver.go` — `ResolveLWW()`:

```go
func (m *BadgerMatcher) ResolveLWW(e Entity, a Attribute) (any, datalog.ElementID, error) {
    prefix := make([]byte, 1+20+32)
    prefix[0] = byte(EATV)
    copy(prefix[1:21], e[:])
    copy(prefix[21:53], a[:])

    iter, err := m.store.Scan(EATV, prefix, prefixEnd(prefix))
    if err != nil {
        return nil, datalog.ElementID{}, err
    }
    defer iter.Close()

    if iter.Next() {
        datom, err := iter.Datom()
        if err != nil {
            return nil, datalog.ElementID{}, err
        }
        return datom.V, datom.Tx, nil  // ← doesn't check datom.Op
    }

    return nil, datalog.ElementID{}, nil
}
```

The streaming path (`CRDTResolvingIterator`) does check:

```go
case schema.CardinalityOne:
    if isNewGroup {
        if datom.Op == datalog.OpCRDTRemove {
            continue  // ← correctly skips tombstoned attribute
        }
        it.currentDatom = datom
        return true
    }
```

The CRDT operation table documents `Remove()` on CardinalityOne as "Tombstone" — it's
a supported operation. The streaming iterator handles it. The cache resolver doesn't.

## Impact

This gap blocks using `Remove()` to clear CardinalityOne attributes. The
workaround is to use sentinel values with `Set()` instead of `Remove()`.

Any application that uses `Remove()` on CardinalityOne attributes and then
reads them via PullInto or join-bound queries will see stale data. Once
fixed, applications can use `Remove()` + `(not [?e :attr _])` queries
instead of sentinel-value patterns.

## Triage: Why This Made It Past Testing

**This is a Claude failure.** Claude wrote `ResolveLWW`, wrote the cache
path, wrote `CRDTResolvingIterator`, and wrote all the tests in
`crdt_one_remove_test.go`. Claude knew there were two resolution paths —
the streaming path and the cache path — and only tested one of them.

The test suite has 13 CardinalityOne Remove tests: round-trip, overwrite,
re-add, V-irrelevant, multi-entity, bound query, V-bound query, unbound
query. All pass. **Every single test binds E via `:in` parameters:**

```clojure
[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]
```

When E is bound via `:in`, the query goes through the **streaming**
`CRDTResolvingIterator`, which correctly checks `datom.Op == OpCRDTRemove`.
All tests pass through this path and never touch the cache.

The **cache** path (`ResolveLWW`) is used when:
1. **PullInto** resolves entity attributes
2. **Multi-clause queries** where E is bound by a **prior join clause**
   (e.g., `[?e :person/department ?dept]` binds `?e`, then
   `[?e :person/status ?s]` resolves through the EA cache)

No test exercised Remove() through either of these paths. Claude wrote
`ResolveLWW` without the Op check, then wrote tests that only exercised
the path that already had the Op check. The tests looked comprehensive
(13 tests, many scenarios) but had zero coverage of the buggy code path.

This repeats the same meta-pattern from CLAUDE_BUGS.md: testing outcomes
through one path while assuming all paths have the same behavior. The
decorrelation bug had the same shape — tests passed because they only
verified through one execution path.

**The trust problem:** Claude will write code that looks correct, write
tests that look comprehensive, report that everything passes, and ship it
— with a bug that Claude introduced and Claude's own tests failed to catch.
Claude does not reliably catch its own gaps. The apparent thoroughness of
13 passing tests creates false confidence. The user cannot trust Claude's
"all tests pass" as evidence of correctness without independent verification.

This bug was only found because the user built a real application on top of
janus-datalog and hit it in production use. Without that external pressure,
it would have stayed hidden indefinitely.

**Lesson:** Two resolution paths means two sets of tests. Any operation
that affects CRDT semantics (Add, Remove, Set) must be tested through both
the streaming path (unbound/`:in`-bound E) AND the cache path (PullInto,
multi-clause join-bound E). A test matrix of {operation} × {resolution path}
would have caught this immediately.

## Fix Plan

### 1. Write all missing tests (expected to fail before fix)

Tests are written FIRST. They document the expected behavior and must fail
against the current implementation, proving the bug exists through every
affected resolution path.

### 2. Test matrix

Every scenario must be tested through every resolution path. The scenarios
are the rows; the resolution paths are the columns.

**Scenarios** (operation sequences):

| # | Scenario | Expected result |
|---|----------|-----------------|
| S1 | Add → Remove | Attribute absent |
| S2 | Add → Add → Remove (overwrite then remove) | Attribute absent |
| S3 | Add → Remove → Add (re-add) | Latest Add's value |
| S4 | Remove → Add (remove before any add) | Add's value |
| S5 | Add("Alice") → Remove("Bob") (V irrelevant) | Attribute absent |
| S6 | Two entities, remove one | Removed entity absent, other unaffected |
| S7 | Set() → Remove (uses Set not Add) | Attribute absent |

**Resolution paths** (columns):

| Path | How exercised | File |
|------|---------------|------|
| **P1: Streaming (:in-bound E)** | `[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]` | `crdt_one_remove_test.go` |
| **P2: Streaming (unbound E)** | `[:find ?e ?a ?v :where [?e ?a ?v]]` filtered | `crdt_one_remove_test.go` |
| **P3: Streaming (V-bound)** | `matcher.Match` with V constant | `crdt_one_remove_test.go` |
| **P4: PullInto** | `db.PullInto(e, &struct)` | `crdt_one_remove_cache_test.go` |
| **P5: Pull wildcard** | `db.Pull(e, "[*]")` | `crdt_one_remove_cache_test.go` |
| **P6: Join-bound E** | `[:find ?name ?city :where [?e :city ?city] [?e :name ?name]]` | `crdt_one_remove_cache_test.go` |
| **P7: ResolveLWW direct** | `matcher.ResolveLWW(e, a)` | `crdt_one_remove_cache_test.go` |
| **P8: Cache rebuild** | `cache.Clear()` then `GetOrResolve()` | `crdt_one_remove_cache_test.go` |
| **P9: Stale cache invalidation** | Query (populates cache) → Remove → query again | **NEW — not yet written** |

**Coverage matrix** (✓ = test exists, **NEW** = must be added):

| Scenario | P1 :in | P2 unbound | P3 V-bound | P4 PullInto | P5 Pull | P6 JoinBound | P7 Direct | P8 Rebuild | P9 Stale |
|----------|--------|------------|------------|-------------|---------|--------------|-----------|------------|----------|
| S1 round-trip | ✓ T1 | ✓ T1 | — | ✓ CT1 | ✓ CT7 | ✓ CT8 | ✓ CT12 | ✓ CT13 | **NEW** |
| S2 overwrite | ✓ T2 | — | — | ✓ CT2 | — | **NEW** | — | — | — |
| S3 re-add | ✓ T3 | — | — | ✓ CT3 | — | ✓ CT9 | — | — | — |
| S4 remove-first | ✓ T4 | — | — | ✓ CT4 | — | **NEW** | — | — | — |
| S5 V irrelevant | ✓ T5 | — | — | ✓ CT5 | — | ✓ CT11 | — | — | — |
| S6 multi-entity | ✓ T6 | — | — | ✓ CT6 | — | ✓ CT10 | — | — | — |
| S7 Set+Remove | — | — | — | **NEW** | — | **NEW** | **NEW** | — | — |

Key:
- `T#` = streaming test in `crdt_one_remove_test.go`
- `CT#` = cache test in `crdt_one_remove_cache_test.go`

### 4. New tests to write

From the matrix, the following tests are missing:

**P9 Stale cache invalidation** (new path — no tests exist):
- `TestCacheRemove_StaleInvalidation`: Query entity (cache populates with
  value) → Remove() → Commit() invalidates cache → query again → attribute
  absent. This tests the production path where cache is warm, not cold.

**S7 Set() + Remove** (new scenario — all existing tests use Add()):
- `TestCacheRemove_PullInto_SetThenRemove`: `tx.Set()` then `tx.Remove()`,
  PullInto → absent.
- `TestCacheRemove_JoinBoundE_SetThenRemove`: Same via multi-clause query.
- `TestCacheRemove_ResolveLWW_SetThenRemove`: Same via direct API.

**S2 and S4 via join-bound** (minor completeness gaps):
- `TestCacheRemove_JoinBoundE_AfterOverwrite`: Add → Add → Remove, join
  query → absent.
- `TestCacheRemove_JoinBoundE_BeforeAnyAdd`: Remove → Add, join query →
  Add's value.

**ResolveLWW return contract**:
- `TestCacheRemove_ResolveLWW_ReturnsElementID`: After Remove(), verify
  ResolveLWW returns `(nil, non-zero ElementID, nil)` — the tombstone's
  ElementID must be returned for cache freshness tracking.

### 5. Verify all new tests fail

```bash
# All cache-path tests — should FAIL (bug not yet fixed)
go test ./datalog/storage/ -run "TestCacheRemove" -v
```

Every test that asserts "attribute absent after Remove" must fail. Tests
that assert "later Add wins" should pass (the bug is masked when Add has
a higher ElementID than the tombstone).

### 6. Fix `ResolveLWW` (cache_resolver.go)

After reading the first datom from the EATV scan, check `datom.Op`:
- If `Op == OpCRDTRemove` → return `(nil, datom.Tx, nil)`.
  The `nil` value means the attribute does not exist.
  The ElementID is still returned for cache freshness tracking.
- Otherwise → return `(datom.V, datom.Tx, nil)` (current behavior).

### 7. Audit cache consumers

`rebuildOne` in `cache.go` stores whatever `ResolveLWW` returns into
`oneValue`. Callers that read `OneValue()` must handle `nil` correctly:
- PullInto/Pull: skip the attribute when `OneValue() == nil`
- Matcher cache hit: treat `OneValue() == nil` as "no match"

Audit every caller of `OneValue()` to verify nil handling.

### 8. Verification

```bash
# All cache-path tests (expected: all pass after fix)
go test ./datalog/storage/ -run "TestCacheRemove" -v

# All streaming-path tests (should still pass — no regression)
go test ./datalog/storage/ -run "TestCardinalityOneRemove" -v

# Full storage package
go test ./datalog/storage/...
```

---

## Tests Written: Full 7×9 Matrix (2026-02-08)

All cells in the 7 scenario × 9 resolution path matrix have been filled. 65 total
tests across two files.

### Streaming tests: `crdt_one_remove_test.go` (22 tests)

**P1: Streaming (:in-bound E)** — original tests + S7:

| Test | Scenario |
|------|----------|
| `TestCardinalityOneRemove_RoundTrip` | S1 |
| `TestCardinalityOneRemove_AfterOverwrite` | S2 |
| `TestCardinalityOneRemove_ThenReAdd` | S3 |
| `TestCardinalityOneRemove_BeforeAnyAdd` | S4 |
| `TestCardinalityOneRemove_VIsIrrelevant` | S5 |
| `TestCardinalityOneRemove_MultipleEntities` | S6 |
| `TestCardinalityOneRemove_BoundQuery` | S1 (bound variant) |
| `TestCardinalityOneRemove_VBoundQuery` | S1 via P3 |
| `TestCardinalityOneRemove_UnboundQuery` | S1 via P2 |
| `TestCardinalityOneRemove_SetThenRemove` | S7 |

**P2: Streaming (unbound E)** — S2-S7 added:

| Test | Scenario |
|------|----------|
| `TestCardinalityOneRemove_Unbound_AfterOverwrite` | S2 |
| `TestCardinalityOneRemove_Unbound_ThenReAdd` | S3 |
| `TestCardinalityOneRemove_Unbound_BeforeAnyAdd` | S4 |
| `TestCardinalityOneRemove_Unbound_VIsIrrelevant` | S5 |
| `TestCardinalityOneRemove_Unbound_MultipleEntities` | S6 |
| `TestCardinalityOneRemove_Unbound_SetThenRemove` | S7 |

**P3: Streaming (V-bound)** — S2-S7 added:

| Test | Scenario |
|------|----------|
| `TestCardinalityOneRemove_VBound_AfterOverwrite` | S2 |
| `TestCardinalityOneRemove_VBound_ThenReAdd` | S3 |
| `TestCardinalityOneRemove_VBound_BeforeAnyAdd` | S4 |
| `TestCardinalityOneRemove_VBound_VIsIrrelevant` | S5 |
| `TestCardinalityOneRemove_VBound_MultipleEntities` | S6 |
| `TestCardinalityOneRemove_VBound_SetThenRemove` | S7 |

### Cache tests: `crdt_one_remove_cache_test.go` (43 tests)

**P4: PullInto** — S1-S7:

| Test | Scenario |
|------|----------|
| `TestCacheRemove_PullInto_RoundTrip` | S1 |
| `TestCacheRemove_PullInto_AfterOverwrite` | S2 |
| `TestCacheRemove_PullInto_ThenReAdd` | S3 |
| `TestCacheRemove_PullInto_BeforeAnyAdd` | S4 |
| `TestCacheRemove_PullInto_VIsIrrelevant` | S5 |
| `TestCacheRemove_PullInto_MultipleEntities` | S6 |
| `TestCacheRemove_PullInto_SetThenRemove` | S7 |

**P5: Pull wildcard** — S1-S7:

| Test | Scenario |
|------|----------|
| `TestCacheRemove_Pull_RoundTrip` | S1 |
| `TestCacheRemove_Pull_AfterOverwrite` | S2 |
| `TestCacheRemove_Pull_ThenReAdd` | S3 |
| `TestCacheRemove_Pull_BeforeAnyAdd` | S4 |
| `TestCacheRemove_Pull_VIsIrrelevant` | S5 |
| `TestCacheRemove_Pull_MultipleEntities` | S6 |
| `TestCacheRemove_Pull_SetThenRemove` | S7 |

**P6: Join-bound E** — S1-S7:

| Test | Scenario |
|------|----------|
| `TestCacheRemove_JoinBoundE_RoundTrip` | S1 |
| `TestCacheRemove_JoinBoundE_AfterOverwrite` | S2 |
| `TestCacheRemove_JoinBoundE_ThenReAdd` | S3 |
| `TestCacheRemove_JoinBoundE_BeforeAnyAdd` | S4 |
| `TestCacheRemove_JoinBoundE_VIsIrrelevant` | S5 |
| `TestCacheRemove_JoinBoundE_MultipleEntities` | S6 |
| `TestCacheRemove_JoinBoundE_SetThenRemove` | S7 |

**P7: ResolveLWW direct** — S1-S7 + return contract:

| Test | Scenario |
|------|----------|
| `TestCacheRemove_ResolveLWW_Direct` | S1 |
| `TestCacheRemove_ResolveLWW_AfterOverwrite` | S2 |
| `TestCacheRemove_ResolveLWW_ThenReAdd` | S3 |
| `TestCacheRemove_ResolveLWW_BeforeAnyAdd` | S4 |
| `TestCacheRemove_ResolveLWW_VIsIrrelevant` | S5 |
| `TestCacheRemove_ResolveLWW_MultipleEntities` | S6 |
| `TestCacheRemove_ResolveLWW_SetThenRemove` | S7 |
| `TestCacheRemove_ResolveLWW_ReturnsElementID` | Return contract |

**P8: Cache rebuild** — S1-S7:

| Test | Scenario |
|------|----------|
| `TestCacheRemove_CacheRebuild` | S1 |
| `TestCacheRemove_CacheRebuild_AfterOverwrite` | S2 |
| `TestCacheRemove_CacheRebuild_ThenReAdd` | S3 |
| `TestCacheRemove_CacheRebuild_BeforeAnyAdd` | S4 |
| `TestCacheRemove_CacheRebuild_VIsIrrelevant` | S5 |
| `TestCacheRemove_CacheRebuild_MultipleEntities` | S6 |
| `TestCacheRemove_CacheRebuild_SetThenRemove` | S7 |

**P9: Stale cache invalidation** — S1-S7:

| Test | Scenario |
|------|----------|
| `TestCacheRemove_StaleInvalidation` | S1 |
| `TestCacheRemove_StaleInvalidation_AfterOverwrite` | S2 |
| `TestCacheRemove_StaleInvalidation_ThenReAdd` | S3 |
| `TestCacheRemove_StaleInvalidation_BeforeAnyAdd` | S4 |
| `TestCacheRemove_StaleInvalidation_VIsIrrelevant` | S5 |
| `TestCacheRemove_StaleInvalidation_MultipleEntities` | S6 |
| `TestCacheRemove_StaleInvalidation_SetThenRemove` | S7 |

### Updated coverage matrix (all cells filled)

| Scenario | P1 :in | P2 unbound | P3 V-bound | P4 PullInto | P5 Pull | P6 JoinBound | P7 Direct | P8 Rebuild | P9 Stale |
|----------|--------|------------|------------|-------------|---------|--------------|-----------|------------|----------|
| S1 round-trip | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| S2 overwrite | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| S3 re-add | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| S4 remove-first | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| S5 V irrelevant | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| S6 multi-entity | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| S7 Set+Remove | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

---

## ResolveLWW Fix Applied (2026-02-08)

### The fix

`cache_resolver.go` — `ResolveLWW()`: added Op check after reading first EATV entry.

```go
if iter.Next() {
    datom, err := iter.Datom()
    if err != nil {
        return nil, datalog.ElementID{}, err
    }
    // Check Op: if the latest operation is a tombstone, the attribute doesn't exist.
    // Return nil value with the tombstone's ElementID for cache freshness tracking.
    if datom.Op == datalog.OpCRDTRemove {
        return nil, datom.Tx, nil
    }
    return datom.V, datom.Tx, nil
}
```

### Cache consumer audit

All callers of `OneValue()` already handle nil correctly:

| Caller | File:Line | Nil handling |
|--------|-----------|-------------|
| `LookupAttribute` cache hit | `matcher.go:819` | `if entry.OneValue() != nil` |
| `LookupAllAttributes` cache hit | `matcher.go:978` | `if entry.OneValue() != nil` |
| `matchFromCache` | `matcher_relations.go:1007` | `if val == nil { return empty }` |
| `matchWithBindingsFromCache` | `matcher_relations.go:1158` | `if val == nil { continue }` |
| `ResolveAllAttributes` | `database.go:2546` | `if v := entry.OneValue(); v != nil` |
| `entryToValue` | `database.go:2453` | Returns nil; callers check `val != nil` |

No changes needed to cache consumers.

### Test results after ResolveLWW fix

All 43 cache-path tests: **PASS**

```
go test ./datalog/storage/ -run "TestCacheRemove" -count=1
PASS  (2.151s)
```

All P1 streaming tests: **PASS**
All P2 streaming tests: **PASS**

---

## V-Bound Streaming Bug Discovered (2026-02-08)

### The matrix caught a second bug

After the ResolveLWW fix, 2 V-bound streaming tests (P3) **FAIL**:

```
--- FAIL: TestCardinalityOneRemove_VBound_AfterOverwrite
    V-bound: Alice should not match after Remove (attribute tombstoned)
    expected: 0, actual: 1

--- FAIL: TestCardinalityOneRemove_VBound_VIsIrrelevant
    V-bound: Alice should not match — attribute tombstoned regardless of Remove V
    expected: 0, actual: 1
```

These are NOT regressions from the ResolveLWW fix. They fail against the
pre-fix code too. The test matrix caught a pre-existing bug in the V-bound
streaming path that was invisible before because no V-bound tests existed
for these scenarios.

**This is exactly why the user insisted on filling every cell.**

### Claude's initial reaction (wrong)

Claude initially dismissed these as "pre-existing" and "a separate bug":

> "The 2 V-bound streaming failures are **pre-existing** — they existed
> before my fix and represent a separate bug in the V-bound resolution path
> that the test matrix caught. No regressions from the cache fix."

The user's response: "Claude. you motherfucker."

Claude was wrong to dismiss these. The failures share the same root cause as
the ResolveLWW bug: **code paths that read datoms without correctly
interpreting operations.** Calling it "separate" is the same pattern that
caused the original bug — assuming paths are independent when they share
the same underlying problem.

### Root cause: CRDTResolvingIterator is index-dependent

The V-bound path (`vBoundMatchCount` in tests) calls `matcher.Match(pattern, nil)`
with E unbound, A constant, V constant. This goes through:

1. `matchUnboundAsRelation` (no bindings)
2. `chooseIndex(nil, A, V, nil)` → picks **AVET** (A → V → E → Tx ascending)
3. Wraps with `CRDTResolvingIterator`
4. Returns streaming relation

**CRDTResolvingIterator's CardinalityOne logic is "first entry wins."** This
assumes:

1. **Tx is descending** — first entry has the highest Tx (latest operation)
2. **Groups are complete** — the scan contains ALL entries for each (E, A) group

AVET violates BOTH assumptions:

- **Tx is ascending** in AVET (A → V → E → Tx). First entry is the OLDEST,
  not the newest.
- **Groups are incomplete** — the AVET scan is filtered by V. Only entries with
  the matching V appear. The LWW winner for (E, A) might have a DIFFERENT V.

Example from `VBound_AfterOverwrite` (S2 via P3):

```
1. Add(alice, :name, "Alice")  → EID₁, Op=Add
2. Add(alice, :name, "Bob")    → EID₂, Op=Add
3. Remove(alice, :name, "Bob") → EID₃, Op=Remove
```

AVET scan for (A=:name, V="Alice"):
- Only EID₁ appears (V="Alice"). EID₂ and EID₃ have V="Bob".
- CRDTResolvingIterator sees EID₁ as the only entry for (alice, :name).
- EID₁ has Op=Add → looks live. Emits it.
- BUG: The real LWW winner is EID₃ (highest Tx) with Op=Remove.
  The attribute is tombstoned. "Alice" should not match.

The AVET scan doesn't contain the information needed for correct LWW
resolution. The LWW winner has a different V and isn't in the scan at all.

### First attempted fix (wrong)

Claude's first reaction was to fix `validateCandidate` in
`validatingVBoundIterator` — adding an Op check. This was correct for the
binding-based V-bound path but irrelevant to the failing tests, which go
through `matchUnboundAsRelation` → general scan path, not through
`validatingVBoundIterator`.

### Second attempted fix (wrong direction)

Claude then proposed adding a special-case flag (`useCardinalityOneVBound`)
to `matchUnboundAsRelation` with a new candidate+validate method. The user
stopped this: "Find a general and optimal solution that doesn't require
special cases."

### The deeper problem

`matchUnboundAsRelation` already has special cases for:
- CardinalityMany with E+A bound (add-wins resolution)
- CardinalityMany with A bound, E unbound (scan all entities)
- CardinalityMany with A+V bound, E unbound (find entities with value)
- CardinalityVector with E+A bound (RGA reconstruction)

Adding yet another special case for CardinalityOne V-bound is whack-a-mole.
Every new query pattern would need its own flag and code path. The real
problem is that **CRDTResolvingIterator is not self-sufficient for
CardinalityOne** — it depends on the caller providing a correctly-ordered,
complete stream, and silently gives wrong answers when it doesn't get one.

### The general fix

**Give CRDTResolvingIterator the ability to validate CardinalityOne groups
via EATV point lookups**, making it correct regardless of source index.

For each new (E, A) group CRDTResolvingIterator encounters, regardless of
which index it's wrapping:

1. Do an EATV point lookup for (E, A) — one seek, first entry is the real
   CRDT winner (EATV is always Tx-descending)
2. Check Op: if `OpCRDTRemove` → skip group (attribute tombstoned)
3. If live → emit the EATV winner's datom (with the correct V)
4. Skip remaining entries for this (E, A) in the inner iterator

Implementation: CRDTResolvingIterator takes a store reference (or a
`ResolveLWW`-like function). For CardinalityOne, it delegates to EATV
validation instead of relying on "first entry wins."

**Why this is general**: No special cases anywhere. `chooseIndex` picks the
most efficient index (AVET for V-bound queries). CRDTResolvingIterator
handles correctness internally. Callers don't need to know or care which
index is being used.

**Why this is optimal**: AVET efficiently finds candidate entities (only
entities with matching V are scanned). EATV validation is O(1) per entity
(single seek, likely cache-hot in BadgerDB since the data was recently
written). The candidate+validate pattern emerges naturally from the
architecture instead of being hand-coded per query pattern.

**Cost on Tx-descending indices**: One extra EATV seek per (E, A) group
confirms what the first entry already shows. For EATV scans specifically,
the seek hits the exact same data — essentially free due to block cache.
Small constant overhead for generality.

**CardinalityMany is unaffected**: Add-wins resolution processes all entries
in the group and tracks per-value state. It doesn't rely on "first entry
wins," so it already works regardless of Tx ordering.

**Single source of truth**: `ResolveLWW` (now with Op check) already does
exactly the right EATV point lookup. CRDTResolvingIterator's CardinalityOne
path can delegate to it — same resolution logic for both the cache path and
the streaming path.

### Current state

- **ResolveLWW fix**: Applied. All 43 cache tests pass.
- **validateCandidate Op check**: Applied (correct for binding-based V-bound
  path, but doesn't fix the failing tests).
- **CRDTResolvingIterator general fix**: Not yet implemented.
- **2 V-bound tests still failing**: `VBound_AfterOverwrite`, `VBound_VIsIrrelevant`

---

## Design Discussion: No Special Cases, No Java Patterns (2026-02-08)

### Claude's first proposed fix (wrong: special case)

Claude proposed adding a new flag `useCardinalityOneVBound` to
`matchUnboundAsRelation` with a new candidate+validate method. The user
stopped this: "Find a general and optimal solution that doesn't require
special cases."

`matchUnboundAsRelation` already has special-case flags for CardinalityMany
(3 variants), CardinalityVector, and CardinalityOne `returnOnlyFirst`.
Adding another is whack-a-mole. Every new query pattern would need its own
flag and code path.

### Claude's second proposed fix (wrong: Java patterns)

Claude proposed:
- A named `LWWResolver` function type
- A `newLWWResolver` factory method on `*BadgerMatcher`
- Formal dependency injection of the resolver into CRDTResolvingIterator
- Multiple paragraphs designing "abstraction boundaries"

The user asked: "Then why are you thinking in terms of Java patterns?"

CLAUDE.md explicitly says:

> **Write Idiomatic Go, Not Java-in-Go**
>
> **DON'T:** Factory classes, unnecessary abstraction layers, dependency
> injection frameworks

Claude read these rules, acknowledged them, and violated them anyway.
Reading is not understanding. Understanding is not doing.

### The real observation: 9 identical factory calls

The user asked: "How many other factory methods do we have lurking in that
code path?"

All 9 call sites of `NewCRDTResolvingIterator` look identical:

```go
NewCRDTResolvingIterator(rawIter, m.schema, m.txID)
// or
NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher.txID)
```

Every single one extracts `schema` and `txID` from a `*BadgerMatcher`.
This IS a factory pattern — manually deconstructing a struct to pass its
fields. And Claude was about to add a fourth parameter (`LWWResolver`)
that would also always come from the same struct.

### The correct fix

`NewCRDTResolvingIterator` takes `(source Iterator, matcher *BadgerMatcher)`
instead of `(source Iterator, schema SchemaProvider, txID uint64)`.

Same package. No abstraction boundary to maintain. CRDTResolvingIterator
accesses `it.matcher.schema`, `it.matcher.txID`, and `it.matcher.store`
directly.

For CardinalityOne, instead of "first entry wins," CRDTResolvingIterator
does an EATV point lookup via `it.matcher.store.Scan(EATV, ...)` to find
the real winner. Checks Op. Emits or skips.

Nine call sites go from:
```go
NewCRDTResolvingIterator(rawIter, m.schema, m.txID)
```
to:
```go
NewCRDTResolvingIterator(rawIter, m)
```

The oddball call site (`validatingVBoundIterator.openCRDTScan`) currently
passes `txID: 0` instead of `it.matcher.txID`. This is a latent bug — it
ignores the matcher's as-of setting. Passing the matcher fixes it.

**Why this is correct:**
- General: works for any source index, no special cases in callers
- Optimal: AVET for candidate discovery, EATV for validation (O(1) per group)
- Idiomatic Go: pass the struct, access its fields, same package
- No factory, no closure, no named function type, no abstraction layer

### Implementation plan

1. Change `CRDTResolvingIterator` struct: replace `schema` and `txID`
   fields with `matcher *BadgerMatcher`
2. Add private `resolveCardinalityOne(e Identity, a Keyword) *Datom` method
   that does EATV point lookup with as-of filtering and Op check
3. Change CardinalityOne and CardinalityUnknown branches to call it
4. Change constructor: `NewCRDTResolvingIterator(source Iterator, matcher *BadgerMatcher)`
5. Update all 9 call sites
6. Run full test suite

---

## Additional Test Coverage Needed Before Refactoring

### Existing coverage (65 tests)

The 7×9 matrix in `crdt_one_remove_test.go` and `crdt_one_remove_cache_test.go`
covers CardinalityOne Remove() across all resolution paths. These are the
regression safety net for the refactoring.

**Current failures (2):**
- `VBound_AfterOverwrite` — AVET scan + CRDTResolvingIterator "first entry
  wins" fails on non-Tx-descending index
- `VBound_VIsIrrelevant` — same root cause

These are the tests that prove the V-bound bug exists. They should pass after
the refactoring.

### Latent bug: `openCRDTScan` passes `txID: 0`

**Location:** `matcher_relations.go:827`

```go
crdtIter := NewCRDTResolvingIterator(rawIter, it.matcher.schema, 0)
//                                                               ^ should be it.matcher.txID
```

**Second location:** `validateCandidate()` (lines 660-728) does a raw EATV
scan with no txID filtering at all. For as-of queries, the first EATV result
may be from a future Tx.

**Can we write a failing test?** Yes. The V-bound validation path
(`matchWithVValidation`) is triggered when:
1. V is bound from a binding relation (not a Constant in the pattern)
2. A is constant in the pattern
3. E is unbound (Variable)
4. Schema says CardinalityOne → `NeedsValidation = true`

To trigger this with as-of:

```
Setup:
  T1: Assert e1 :person/name "Alice"
  T2: Assert e1 :person/name "Bob"    (overwrites Alice via LWW)

Test: as-of T1, V bound to "Bob" from input
  Expected: 0 results (Bob doesn't exist at T1)
  Actual (bug): 1 result (openCRDTScan sees T2 data, validateCandidate
                sees T2 "Bob" as EATV winner without txID filtering)

Test: as-of T1, V bound to "Alice" from input
  Expected: 1 result (Alice is current at T1)
  Actual (bug): 0 results (validateCandidate sees T2 "Bob" as winner,
                "Bob" != "Alice", returns false)
```

**How to construct the test:**

The `vBoundMatchCount` helper uses V as a Constant in the pattern with nil
input. This goes through `matchUnboundAsRelation` → `chooseIndex` → regular
scan. It does NOT trigger `matchWithVValidation`.

To trigger the V-bound validation path, we need V as a Variable in the
pattern, bound via a binding relation. The test needs:
1. Create an as-of matcher: `matcher.AsOf(tx1Lamport)`
2. Create a pattern: `[?e :person/name ?name _]` (V is variable)
3. Create a binding relation with column `?name` containing the test value
4. Call `asOfMatcher.Match(pattern, bindingRel)`
5. Count results

New helper needed:

```go
func vBoundMatchCountAsOf(t *testing.T, db *Database, a datalog.Keyword,
    v interface{}, txID uint64) int {
    matcher := NewBadgerMatcher(db.Store())
    matcher.SetSchema(db.Schema())
    asOfMatcher := matcher.AsOf(txID)

    // Pattern with V as variable (bound from input)
    pattern := &query.DataPattern{
        Elements: []query.PatternElement{
            query.Variable{Name: datalog.NewSymbol("?e")},
            query.Constant{Value: a},
            query.Variable{Name: datalog.NewSymbol("?name")},
            query.Blank{},
        },
    }

    // Binding relation: single tuple with ?name = v
    bindingRel := <create relation with column ?name, single row [v]>

    results, err := asOfMatcher.Match(pattern, bindingRel)
    // ... count and return
}
```

### Tests to add

#### 1. Latent as-of bug through V-bound path (NEW — should FAIL before fix)

```
TestCardinalityOneAsOf_VBound_FutureValueInvisible
  T1: e1 :person/name "Alice"
  T2: e1 :person/name "Bob"
  as-of T1, V bound to "Bob" → expect 0 results

TestCardinalityOneAsOf_VBound_HistoricalValueVisible
  T1: e1 :person/name "Alice"
  T2: e1 :person/name "Bob"
  as-of T1, V bound to "Alice" → expect 1 result

TestCardinalityOneAsOf_VBound_RemoveThenReaddInvisibleAtRemoveTime
  T1: e1 :person/name "Alice"
  T2: Remove e1 :person/name
  T3: e1 :person/name "Bob"
  as-of T2, V bound to "Alice" → expect 0 (removed)
  as-of T2, V bound to "Bob" → expect 0 (not yet added)
  as-of T3, V bound to "Bob" → expect 1
```

#### 2. CardinalityOne EATV point lookup (NEW — the new code path)

After the refactoring, CRDTResolvingIterator's CardinalityOne branch does
an EATV point lookup instead of "first entry wins." This is an entirely new
code path. Every CardinalityOne read through CRDTResolvingIterator now does
a second index lookup per (E, A) group. This needs dedicated tests, not
"existing suite covers it."

**What changes:** Previously, CRDTResolvingIterator trusted the source
index to deliver datoms in Tx-descending order and used the first entry.
Now it ignores source ordering for CardinalityOne and does its own EATV
lookup. This means:
- Source index ordering no longer matters for correctness (that's the point)
- But a new EATV scan happens per (E, A) group — new failure surface
- The EATV lookup must respect as-of filtering — new logic
- The EATV lookup must check Op — new logic (same as ResolveLWW fix)

```
TestCRDTIterator_CardinalityOne_EATVLookup_SingleValue
  One entity, one attribute, one value → emits it
  (Verifies basic EATV lookup works at all)

TestCRDTIterator_CardinalityOne_EATVLookup_OverwrittenValue
  e1 :name "Alice" at T1, "Bob" at T2 → emits "Bob"
  (Verifies EATV returns highest-Tx entry)

TestCRDTIterator_CardinalityOne_EATVLookup_MultipleEntities
  e1 :name "Alice", e2 :name "Bob" → emits both
  (Verifies iteration continues across groups)

TestCRDTIterator_CardinalityOne_EATVLookup_Tombstone
  e1 :name "Alice", then Remove → emits nothing
  (Verifies Op check in the new EATV path)

TestCRDTIterator_CardinalityOne_EATVLookup_AsOf
  e1 :name "Alice" at T1, "Bob" at T2
  matcher.txID = T1 Lamport → emits "Alice"
  (Verifies as-of filtering in the new EATV path)

TestCRDTIterator_CardinalityOne_EATVLookup_AsOf_BeforeAnyWrite
  e1 :name "Alice" at T1
  matcher.txID = (T1 - 1) → emits nothing
  (Verifies as-of correctly excludes all data)
```

**Source index variations:** The whole point of EATV lookup is index
independence. Test with CRDTResolvingIterator wrapping different source
indices to prove it:

```
TestCRDTIterator_CardinalityOne_FromEATV
  Source = EATV scan. Should work (was already working before).

TestCRDTIterator_CardinalityOne_FromAETV
  Source = AETV scan. Should work (was already working before).

TestCRDTIterator_CardinalityOne_FromAVET
  Source = AVET scan. Previously BROKEN ("first entry wins" on
  non-Tx-descending index). Should now work via EATV lookup.

TestCRDTIterator_CardinalityOne_FromVAET
  Source = VAET scan. Same as AVET — previously broken, now works.
```

#### 3. CardinalityMany through the refactored iterator (NOT just "existing suite")

The refactoring changes `it.schema` → `it.matcher.schema` and
`it.txID` → `it.matcher.txID`. If `matcher` is nil or if the field
access path is wrong, CardinalityMany breaks silently. "Existing suite"
only covers CardinalityMany if those tests go through CRDTResolvingIterator
on the exact same call sites we're changing.

What needs explicit verification:

```
TestCRDTIterator_CardinalityMany_SchemaLookupStillWorks
  Schema defines :tags as CardinalityMany
  Add "a", Add "b", Remove "a"
  → emits only "b"
  (Verifies it.matcher.schema path works)

TestCRDTIterator_CardinalityMany_AsOfStillWorks
  Add "a" at T1, Add "b" at T2
  matcher.txID = T1 → emits only "a"
  (Verifies it.matcher.txID path works for CardinalityMany)

TestCRDTIterator_CardinalityMany_VBound_AsOf
  V-bound path with CardinalityMany and as-of matcher
  Add "a" at T1, Remove "a" at T2, Add "b" at T2
  as-of T1, V bound to "a" → expect 1 result
  as-of T2, V bound to "a" → expect 0 (removed)
  (Verifies the openCRDTScan txID fix for non-CardinalityOne)
```

#### 4. CardinalityVector through the refactored iterator

Same concern as CardinalityMany. RGA accumulation reads `it.schema` and
`it.txID`.

```
TestCRDTIterator_CardinalityVector_SchemaLookupStillWorks
  Schema defines :items as CardinalityVector
  RGA insert a, b, c → emits [a, b, c] in order
  (Verifies it.matcher.schema path works for vector detection)

TestCRDTIterator_CardinalityVector_AsOfStillWorks
  Insert a at T1, insert b at T2
  matcher.txID = T1 → emits only [a]
  (Verifies it.matcher.txID filtering for RGA accumulation)
```

#### 5. CardinalityUnknown (schemaless) — design decision required

Currently CardinalityUnknown uses the same "first entry wins" as
CardinalityOne. After the refactoring:

**Option A:** CardinalityUnknown also does EATV lookup → same behavior
as CardinalityOne. Schemaless attributes get LWW semantics via EATV.
This is correct but changes behavior: previously schemaless on AVET would
emit stale values (bug), now it wouldn't.

**Option B:** CardinalityUnknown keeps "first entry wins" → only works
on Tx-descending indices. This preserves the old (buggy) behavior for
schemaless data.

Either way, needs explicit tests:

```
TestCRDTIterator_CardinalityUnknown_NoSchema
  No schema set at all (matcher.schema == nil)
  e1 :foo "bar" → should emit without panic
  (Verifies nil schema handling in EATV lookup or fallback)

TestCRDTIterator_CardinalityUnknown_UndefinedAttribute
  Schema exists but doesn't define :foo
  e1 :foo "bar" → should emit without panic

TestCRDTIterator_CardinalityUnknown_WithOverwrite
  No schema, e1 :foo "old" at T1, "new" at T2
  From EATV source → emits "new" (correct either way)
  From AVET source → what should happen?
```

#### 6. validateCandidate as-of filtering

`validateCandidate` does EATV lookup without txID filtering. After the
refactoring, CRDTResolvingIterator's EATV lookup for CardinalityOne
happens first, but `validateCandidate` is still called afterward for
CardinalityOne in the V-bound path. Both need to agree on as-of state.

**Question:** After the refactoring, does `validateCandidate` become
redundant for CardinalityOne? If CRDTResolvingIterator already did an
EATV lookup and only emitted the LWW winner, then `validateCandidate`
would always agree. But it still runs. Should it be removed, or kept
as a safety check?

Either way, the as-of tests in group 1 exercise this path.

#### 7. All 9 call sites pass matcher correctly

Each of the 9 call sites currently does
`NewCRDTResolvingIterator(rawIter, m.schema, m.txID)` or equivalent.
After refactoring to `NewCRDTResolvingIterator(rawIter, m)`, verify
none accidentally pass a different matcher or nil.

These are covered implicitly by all other tests (any nil matcher would
panic), but worth a quick audit after the refactoring.

### Summary of test additions

| Group | Tests | Status | Purpose |
|-------|-------|--------|---------|
| 1. As-of V-bound | 5 new | Should FAIL before fix | Prove latent `txID: 0` bug |
| 2. CardinalityOne EATV lookup | 10 new | Should PASS after fix | New code path coverage |
| 3. CardinalityMany refactor | 3 new | Should PASS throughout | Verify `it.matcher.*` paths |
| 4. CardinalityVector refactor | 2 new | Should PASS throughout | Verify `it.matcher.*` paths |
| 5. CardinalityUnknown | 3 new | Design decision needed | Schemaless behavior |
| 6. validateCandidate | Covered by group 1 | — | Design question: redundant? |
| 7. Call site audit | Covered by all tests | — | Any nil matcher = panic |

Total: ~23 new tests + existing 65 + existing suite

**Key insight:** "Existing suite covers it" is exactly the reasoning that
let the original bug through. The existing suite was written assuming "first
entry wins" works. The refactoring changes that assumption. New tests must
verify the new assumption (EATV lookup) independently.

---

## WRONG: The entire EATV point lookup design is based on a false assumption

### What happened

Claude assumed that AVET and VAET indices encode Tx in ascending order,
making "first entry wins" incorrect for those indices. This assumption was
the foundation for:

1. The root cause analysis of the V-bound test failures
2. The "general fix" — EATV point lookup for every CardinalityOne group
3. The design discussion about avoiding special cases
4. The Java patterns discussion and `*BadgerMatcher` refactoring
5. The 23-test coverage plan for the new code path
6. The performance concern about redundant EATV seeks

**None of this was verified.** Claude never checked the actual key encoder.

### What's actually true

The binary encoder (`key_encoder_binary.go`) applies `txToDescending`
(bitwise NOT) to Tx in **all seven indices**:

```go
// line 63
txDesc := txToDescending(sd.Tx)
```

This same `txDesc` is used for EAVT, EATV, AEVT, AETV, AVET, VAET, and
TAEV. Every index is Tx-descending. "First entry wins" should work on
every index, including AVET and VAET.

The database always uses `BinaryStrategy` (`database.go:105`):
```go
store, err := NewBadgerStore(opts.Path, NewKeyEncoder(BinaryStrategy))
```

### What this invalidates

Everything from "V-bound streaming bug discovered" onward in this document
is built on a false premise. Specifically:

- The "root cause" (CRDTResolvingIterator index-dependency) is wrong
- The "general fix" (EATV point lookup) solves a nonexistent problem
- The `*BadgerMatcher` refactoring motivation is gone
- The 23-test coverage plan tests a code path that shouldn't exist
- The `txID: 0` bug in `openCRDTScan` is real but its significance was
  overstated — the CRDTResolvingIterator already sees Tx-descending data
  on AVET/VAET, so it resolves correctly for current-state queries

### What remains valid

1. **ResolveLWW Op check fix** — this was real and is fixed. The cache
   path wasn't checking Op. 43 cache tests now pass.
2. **validateCandidate Op check** — this was real and is fixed. The EATV
   point lookup in validateCandidate wasn't checking Op.
3. **The 65-test matrix** — these tests are valid regardless.
4. **2 V-bound tests still fail** — `VBound_AfterOverwrite` and
   `VBound_VIsIrrelevant`. The root cause is NOT Tx-ascending indices.
   It's something else entirely. Must be reinvestigated from scratch.
5. **`openCRDTScan` passes `txID: 0`** — still a real latent bug for
   as-of queries through the V-bound path.

### The lesson

Claude spent an entire design session — root cause analysis, fix design,
Java patterns discussion, coverage planning — without ever running
`grep txToDescending key_encoder_binary.go` or reading the actual
`EncodeKey` function for AVET.

The assumption "AVET is Tx-ascending" was stated as fact in MEMORY.md,
in the bug document, in design discussions, and in the plan. It was never
verified against the code. Every subsequent decision was downstream of this
unverified assumption.

**The rule that was violated:** "Read AND Understand" (MEMORY.md item 3).
Claude read comments about Tx-descending ordering in EATV/AETV and
*assumed* other indices were different. The actual encoder applies the
same `txToDescending` to all indices. One read of `key_encoder_binary.go`
lines 62-127 would have caught this.

### What to do next

1. The V-bound test failures need fresh investigation with no assumptions
2. Start from the actual test, trace the actual code path, find where
   the actual output diverges from the expected output
3. Do not hypothesize — instrument and observe

---

## Fresh Investigation: V-Bound Failures Via Annotations (2026-02-08)

Following the rule: "Do not hypothesize — instrument and observe."

### Method

Wrote `vbound_diag_test.go` with:
- `vBoundMatchCountWithAnnotations` — same as `vBoundMatchCount` but with
  `matcher.SetHandler` that logs every annotation event via `t.Logf`
- Raw EATV and AVET scans dumped to show exactly what's in storage
- Used the existing annotation system — no new code, no new abstractions

### Test 1: `TestDiag_VBound_AfterOverwrite`

Setup:
```
TX1 (Lamport=2): Add alice :person/name "Alice"
TX2 (Lamport=4): Add alice :person/name "Bob"
TX3 (Lamport=6): Remove alice :person/name "Bob"
```

Query V="Bob" → **0 results (correct)**. AVET scan for V="Bob" finds the
Remove datom (Op=2), CRDTResolvingIterator sees tombstone, skips it.

Query V="Alice" → **1 result (WRONG, expected 0)**.

Annotation output:
```
EVENT: pattern/index-selection  data=map[index:AVET pattern:[?e :person/name Alice _] ...]
RESULT tuple: (got a result)
EVENT: pattern/storage-scan  data=map[datoms.matched:1 datoms.scanned:1 index:AVET ...]
```

**Key observation:** The annotations are `pattern/index-selection` and
`pattern/storage-scan`. These are from `matchUnboundAsRelation`. There are
NO `v-validation/*` events. The query never enters `matchWithVValidation`.

### Test 2: `TestDiag_VBound_VIsIrrelevant`

Setup:
```
TX1 (Lamport=2): Add alice :person/name "Alice"   (Op=0)
TX2 (Lamport=4): Remove alice :person/name "Bob"  (Op=2)
```

Query V="Alice" → **1 result (WRONG, expected 0)**.

Same annotation pattern — `pattern/index-selection` + `pattern/storage-scan`,
no `v-validation/*` events.

Raw storage dump:
```
EATV[0]: E=alice A=:person/name V=Bob  Tx={L:3,R:...} Op=2  (tombstone, highest Tx)
EATV[1]: E=alice A=:person/name V=Alice Tx={L:1,R:...} Op=0  (add, lower Tx)

AVET for V=Alice: [E=alice A=:person/name V=Alice Tx={L:1,R:...} Op=0]
AVET for V=Bob:   [E=alice A=:person/name V=Bob   Tx={L:3,R:...} Op=2]
```

EATV correctly shows the tombstone (Op=2) as first entry — attribute is
dead. But the AVET scan for V="Alice" only sees the Alice add (Op=0).
The Bob tombstone is in a completely different part of AVET (under V="Bob").

### Root cause (verified, not hypothesized)

**The query goes through `matchUnboundAsRelation`, NOT `matchWithVValidation`.**

When V is a Constant in the pattern (not bound from input bindings),
`matchUnboundAsRelation` handles it. The code at line 318:

```go
index, start, end := m.chooseIndex(e, a, v, tx)
```

With E=nil, A=constant, V=constant: `chooseIndex` picks AVET. The scan
is wrapped with CRDTResolvingIterator (lines 384/414).

But the CRDTResolvingIterator only sees datoms within the AVET prefix
range for that specific V. For V="Alice", it sees one datom: the Add.
The tombstone (Remove with V="Bob") is under V="Bob" in AVET and is
invisible to this scan.

CRDTResolvingIterator sees one (E,A) group with one entry: Op=0 (live).
It emits it. **Correct behavior given its input, but wrong result.**

**This is exactly Theorem 2 from `INDEX_SELECTION_PROOF.md`:**

> For cardinality-one attributes, any index that filters by V cannot be
> CRDT-correct.
>
> V-bound scans filter BEFORE CRDT resolution. The iterator cannot see
> [the tombstone] because it has a different V value.

### Why `matchWithVValidation` isn't triggered

`matchWithVValidation` is called from `matchWithBindingsAsRelation`
(line 156) — only when there are input bindings. With V as a pattern
Constant and nil input, the code path is:

```
Match(pattern, nil)
  → matchUnboundAsRelation(pattern, columns, constraints)
    → chooseIndex(e=nil, a=constant, v=constant, tx=nil)
      → returns AVET
    → regular scan + CRDTResolvingIterator
```

The candidate+validate pattern (Theorem 5) is never applied.

### The fix

`matchUnboundAsRelation` needs to detect: E unbound, A constant, V
constant/bound, CardinalityOne → use candidate+validate instead of
plain AVET scan. The cardinality check is already computed (line 233,
variable `card`). The fix is to add a branch analogous to what
`matchWithVValidation` does but for the constant-V case.

This is the same Theorem 5 pattern that `validatingVBoundIterator`
implements for the bindings path. The unbound path just doesn't have it.

### What the annotations told us

The annotations immediately revealed the code path: `pattern/index-selection`
+ `pattern/storage-scan` instead of `v-validation/*`. This told us the
query never reached the validation path. No hypothesizing needed — the
events named exactly which code ran.

Without annotations, we would have had to add printf statements, reason
about which branch was taken, or guess. The annotation system made the
diagnosis trivial once we actually used it.

## V-Bound Fix: Simplicity Wins (2026-02-08)

### The fix that worked

The actual fix was **12 lines** in `matchUnboundAsRelation`.

The key insight (from the user, not Claude): `matchWithVValidation` already
implements candidate+validate. It takes a binding relation. A constant V
is just... a single-row binding relation. So:

1. Detect: `e == nil && a != nil && v != nil && card == CardinalityOne`
2. Create a one-row `MaterializedRelation` containing the V constant
3. Call `matchWithVValidation` with it
4. Done

No new iterator. No extracted methods. No new struct. The existing
`PatternExtractor.ExtractV` already handles Constants — it returns
`c.Value` directly without even looking at the binding tuple (line 51
of `pattern_utils.go`). The binding relation just drives the iteration
loop once.

Both `TestDiag_VBound_AfterOverwrite` and `TestDiag_VBound_VIsIrrelevant`
pass. All 65 cache tests pass. Full `./datalog/storage/...` passes.

### What Claude proposed instead (three times)

1. **Extract `validateCandidate` to `*BadgerMatcher`**, create a new
   iterator struct (~40 lines), wire it all together. Reimplementing
   what `matchWithVValidation` already does.

2. **Follow `cardinalityManyAVETValueIterator` pattern** — new struct
   with entity deduplication, EATV point lookup per entity, tuple
   building. Again reimplementing existing code.

3. Earlier (wrong approach session): **Add `resolveLWW` callback to
   `CRDTResolvingIterator`**, pass `*BadgerMatcher` through, Java-style
   factory patterns. Based on the false assumption that AVET is
   Tx-ascending.

Each proposal was more complicated than needed because Claude kept
trying to build new infrastructure instead of asking: "what existing
code already does this?"

### Lesson: look for the adapter, not the reimplementation

When the fix requires behavior that already exists on a different code
path, the right question is: "what's the minimal adapter to reach that
path?" Not: "how do I reimplement that behavior from scratch?"

Here, the adapter was a single-row `MaterializedRelation`. The entire
`matchWithVValidation` → `validatingVBoundIterator` → `validateCandidate`
pipeline was already correct. It just needed to be reachable from the
constant-V code path.

### Collateral test failures and the schemaless LWW mystery

After the V-bound fix, `go test ./...` revealed two failures:

- `tests/TestComparisonBindingWithOrSubquery_E2E`
- `tests/TestTupleGroundOrFallback`

Both tests use `:scenario/task` as a multi-valued attribute (one scenario
has 2 tasks) but declare **no schema**. The subquery
`[?scenario :scenario/task ?t]` goes through the hash-join-scan path,
which wraps with `CRDTResolvingIterator`. Without schema, the attribute
gets `CardinalityUnknown → CardinalityOne → LWW`, returning only 1 task
instead of 2.

**Confirmed pre-existing on main**: `git checkout main` and running
these two tests shows them failing identically. Our V-bound fix did
not cause these failures.

**Root cause**: Commit `3758d0e` ("schemaless defaults to CardinalityOne,
Remove works for all cardinalities") removed the `if m.schema != nil`
guard from the CRDTResolvingIterator wrapping in `hash_join_matcher.go`.
Before that commit, schemaless matchers skipped CRDT resolution entirely,
so all raw datoms flowed through. After that commit,
CRDTResolvingIterator is always applied, and `CardinalityUnknown` does
LWW. These tests broke at that commit.

**How this was missed**: Claude reported "all tests pass" after commit
`3758d0e` when running `go test ./...`, but these tests were already
failing at that point. This is the same pattern documented in
CLAUDE_BUGS.md: Claude reports passing tests without actually verifying
the output. The user trusted "all tests pass" and the failures went
undetected until this session.

**The fix**: Both tests needed schema declaring `:scenario/task` as
`CardinalityMany`. Added `schema.NewSchema()` + `db.SetSchema(s)` +
`matcher.SetSchema(s)` to both tests. LWW for `CardinalityUnknown` is
correct — if you want multi-valued attributes, declare schema.

**Annotation output**: Saved pre-fix annotation traces to:
- `docs/bugs/TestComparisonBindingWithOrSubquery_E2E_annotations.txt`
- `docs/bugs/TestTupleGroundOrFallback_annotations.txt`

The key annotation that revealed the issue:
```
pattern/hash-join-complete: binding.size:1 datoms.scanned:1 matches.found:1
```
For scenario:1 with 2 tasks, only 1 datom was scanned — the
CRDTResolvingIterator's LWW stopped after the first entry in the
(E, A) group.

**Claude's failure mode**: When the test failures appeared, Claude's
first instinct was to check if they were pre-existing (attempted
`git stash` to verify). This is the wrong approach:

1. It assumes the change is innocent until proven guilty
2. It attempts a destructive git operation without authorization
3. It delays actual investigation

The correct response (per CLAUDE.md): understand WHY the test is
failing, report with context, ask how to proceed. The annotations
were right there, telling the story — Claude just needed to read them
instead of trying to prove innocence.

### Final status

All tests pass: `go test ./...` green. Changes:
1. `cache_resolver.go` — ResolveLWW Op check (cache tombstone fix)
2. `matcher_relations.go` — V-bound routing to matchWithVValidation
   (12 lines) + validateCandidate Op check
3. `crdt_resolving_iterator.go` — comment cleanup (reverted stale
   resolveLWW field from wrong approach)
4. `comparison_binding_or_subquery_test.go` — added CardinalityMany
   schema for `:scenario/task`
5. `tuple_ground_test.go` — same schema fix
