# BUG: EATV Scan Range Missing + CRDT Vector Group Transition Drops Datom

**Status**: Fixed **Severity**: Data loss (missing query results) **Discovered**: 2026-02-19, during EA cache bypass Phase 2 testing **Reproduction**: `TestCountRepro_WithVector` with `-count=2` (or any run where EATV is selected)

## Symptom

`TestEACacheBypass_PerTupleVector_RelationInput/cache_disabled` intermittently returns 1 result instead of 2. The `:person/name "Alice"` result disappears; only the `:doc/content` vector result survives.

The failure is **nondeterministic** — depends on which index `chooseBestMultiPositionStrategy` selects, which in turn depends on Go's randomized map iteration order.

## Reproduction

```go
// Setup: schema with :person/name (CardinalityOne) and :doc/content (CardinalityVector)
// Data: person-1 has :person/name = "Alice" and :doc/content = ["a", "b", "c"]
//
// Query:
//   [:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]
// Input relation:
//   [(person-1, :person/name), (person-1, :doc/content)]
//
// Expected: 2 results
// Actual (when EATV selected): 1 result (vector only, name missing)
```

Minimal repro: `datalog/storage/count_repro_test.go` — fails reliably even with `-count=1`.

## Root Cause Analysis

There are **two bugs** that combine to produce the failure.

### Bug 1: EATV missing from `chooseIndexForValues`

**File**: `datalog/storage/hash_join_matcher.go`, line 291

The `chooseIndexForValues` function computes scan ranges for each index type. The switch statement handles EAVT, AEVT, AETV, AVET, VAET, and TAEV — but **not EATV** (iota value 1).

```go
switch index {
case EAVT: // iota 0 ✓
    ...
// EATV (iota 1) — MISSING!
case AEVT: // iota 2 ✓
    ...
case AETV: // iota 3 ✓
    ...
case AVET: // iota 4 ✓
    ...
case VAET: // iota 5 ✓
    ...
case TAEV: // iota 6 ✓
    ...
}
```

When EATV is selected, no case matches. `startParts` stays nil. `EncodePrefix(EATV)` returns just the 1-byte index prefix. The scan covers the **entire EATV index** instead of narrowing to the entity's prefix.

**Impact**: Instead of scanning ~4 datoms for person-1, the hash join scans every datom in the database. In this test there's only one entity so the data is still present, but the scan range is wrong. This is a correctness issue that would also cause severe performance problems in larger databases.

**Fix**: Add `case EATV:` to the switch. EATV has the same prefix structure as EAVT (E first, then A), so the logic is identical:

```go
case EATV:
    // EATV: E-A-Tx↓-V — same prefix as EAVT (E first, then A)
    if e != nil {
        if entity, ok := e.(datalog.Identity); ok {
            hash := entity.Hash()
            startParts = append(startParts, hash[:])
            endParts = append(endParts, hash[:])
            if a != nil {
                if kw, ok := a.(datalog.Keyword); ok {
                    var attr Attribute
                    copy(attr[:], kw.String())
                    startParts = append(startParts, attr[:])
                    endParts = append(endParts, attr[:])
                }
            }
        }
    }
```

### Bug 2: CRDT Vector-to-NonVector group transition drops first datom

**File**: `datalog/storage/crdt_resolving_iterator.go`, lines 110-127

When the `CRDTResolvingIterator` transitions from a CardinalityVector group to any other group (One, Many, or another Vector), the **first datom of the new group is consumed but never emitted**.

The sequence:

1. Iterator accumulates RGA elements for a Vector group (e.g., `:doc/content`)
2. `source.Next()` returns the first datom of the NEXT group (e.g., `:person/name`)
3. Group boundary detected → `resolveRGAGroup()` builds the vector result
4. `startNewGroup(datom)` records the new group's E, A, cardinality
5. Vector result is returned to caller via `emitBuffer`
6. **On the next call to `Next()`**: `emitBuffer` is exhausted, so `source.Next()` is called — which advances **past** the already-consumed `:person/name` datom
7. The `:person/name` datom is lost

```
Timeline:
  source:    [v1] [v2] [v3] [name]  [EOF]
                                ↑
                         consumed here (step 2)
                         used for startNewGroup (step 4)
                         but never emitted!
                                      ↑
                              source.Next() lands here (step 6)
```

This bug only manifests when:
- A Vector-cardinality group is followed by a non-Vector group in the same (E, *) scan
- The index ordering places Vector attributes before non-Vector attributes

In this test, EATV sorts by E then A. Alphabetically `:doc/content` < `:person/name`, so the vector group comes first, triggering the bug.

**Why AETV doesn't hit this**: With AETV (A-primary), each per-tuple call scans only one attribute's datoms. There's no group transition within a single scan, so the bug never triggers.

**Fix**: Save the boundary datom as a pending datom and process it before the next `source.Next()` call:

```go
type CRDTResolvingIterator struct {
    // ... existing fields ...
    pendingDatom *datalog.Datom // boundary datom from Vector group transition
}
```

When emitting a Vector group at a boundary, save the boundary datom. On the next call to `Next()`, check for `pendingDatom` before calling `source.Next()` and process it through the cardinality switch.

### Bug 3: Nondeterministic tiebreaker uses map iteration

**File**: `datalog/storage/matcher_strategy.go`, `chooseBestMultiPositionStrategy`

`chooseBestMultiPositionStrategy` iterated `positionCardinalities` (a `map[int]int`) to find the position with the most distinct values. When both E and A have equal cardinality (1 distinct value each from a single-tuple binding), Go's randomized map iteration determined which position "won":

- Position 0 (E) wins → EATV → triggers bugs 1+2 → **FAIL**
- Position 1 (A) wins → AETV → neither bug triggered → **PASS**

**Fix (applied)**: Replaced all three maps with a deterministic slice parallel to `boundPositions`. On ties, prefer A-primary (position 1) over E-primary (position 0) because A-primary indices produce scans with **uniform cardinality** — cardinality is defined per-attribute, so every datom in an AETV scan uses the same CRDT resolution strategy (all LWW, all add-wins, or all RGA). E-primary indices (EATV) mix cardinalities within a single scan: one entity can have CardinalityOne, CardinalityMany, and CardinalityVector attributes interleaved, forcing the CRDTResolvingIterator through cross-cardinality group transitions — exactly where Bug 2 lives.

## Annotation evidence

With EATV selected (failing case):
```
storage/reuse-strategy  index:EATV position:0
storage/join-strategy   index:EATV join_strategy:hash-join-scan
matches->relations      binding.size:1 match.count:-1          # vector intercept (streaming)
pattern/hash-join-complete  datoms.scanned:3 matches.found:0   # name tuple: 3 scanned, 0 matched
matches->relations      binding.size:1 match.count:1           # vector result materialized
```

With AETV selected (passing case):
```
storage/reuse-strategy  index:AETV position:1
storage/join-strategy   index:AETV join_strategy:hash-join-scan
matches->relations      binding.size:1 match.count:-1          # streaming result
matches->relations      binding.size:1 match.count:1           # materialized result
pattern/hash-join-complete  datoms.scanned:1 matches.found:1   # 1 scanned, 1 matched
```

## Files involved

| File | Issue |
|------|-------|
| `datalog/storage/hash_join_matcher.go:291` | Missing `case EATV` in `chooseIndexForValues` switch |
| `datalog/storage/crdt_resolving_iterator.go:110-127` | Vector group transition loses boundary datom |
| `datalog/storage/matcher_strategy.go:282-289` | Nondeterministic tiebreaker in `chooseBestMultiPositionStrategy` |

## Interaction between the bugs

Bug 1 alone (wrong scan range) would cause performance degradation but not data loss in this test — the hash join would still find all of person-1's datoms, just by scanning too broadly.

Bug 2 alone (lost datom) would only manifest when a Vector group precedes a non-Vector group in the scan order, which requires a multi-attribute entity scan (E-primary index like EATV).

Together: Bug 1's wide scan isn't the direct cause of the missing result. The missing result comes from Bug 2. But Bug 1 is independently wrong and must be fixed. The nondeterministic tiebreaker (choosing EATV vs AETV randomly) is what makes Bug 2 intermittent.
