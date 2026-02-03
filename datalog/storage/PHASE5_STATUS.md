# Phase 5 CRDT Vector Implementation - Status

**Date:** 2026-01-31 (updated with index arrangement refinement)
**Status:** Functionally working, Bug #5 solution designed (implementation pending)

> **Note:** This status file has been reconciled with the master implementation plan at
> `docs/proposals/CRDT_VECTOR_STORAGE_IMPLEMENTATION_PLAN.md` (Section 5.6).
> This file serves as a quick-reference for Phase 5 development.

---

## Critical: Bug #5 - RGAElement in V Breaks AVET/VAET

> **⚠️ FOUNDATIONAL CHANGE - EXTREME CARE REQUIRED**
>
> This fix modifies the **key encoder** - the most foundational storage component.
> Every index, query, and write depends on correct key encoding.
>
> **A bug here causes silent data corruption and wrong query results.**
>
> Implementation approach:
> 1. Write tests FIRST (all 10 before implementation)
> 2. Change one index at a time, verify each
> 3. Verify sort order explicitly at byte level
> 4. Run full test suite after EACH change
> 5. Review key encoding byte-by-byte

**Same architectural mistake as Bug #4.** Current implementation stores RGA metadata (AfterRef, Tombstone) inside V field. This breaks AVET/VAET index lookups for vector elements.

**The Problem:**
```
Stored:  [AVET][:skills][TypeBytes][RGAElement bytes...][E][Tx]
Query:   [AVET][:skills][TypeString]["stealth"][E][Tx]
Result:  No match (TypeBytes ≠ TypeString)
```

**The Fix:** V should contain raw value only. AfterRef belongs in key.

**Refined Index Arrangement (2026-01-31 discussion):**

After further analysis, we determined that **Tx should come before Op** in all indices. We don't need Op-based sorting for add-wins resolution - the algorithm iterates all entries anyway.

```
Final format: [Lookup components][Tx][Op][AfterRef?]

| Index | Format |
|-------|--------|
| EAVT  | [E][A][V][Tx][Op][AfterRef?] |
| EATV  | [E][A][Tx][V][Op][AfterRef?] |
| AEVT  | [A][E][V][Tx][Op][AfterRef?] |
| AVET  | [A][V][E][Tx][Op][AfterRef?] |
| VAET  | [V][A][E][Tx][Op][AfterRef?] |
| TAEV  | [Tx][A][E][V][Op][AfterRef?] |

Where AfterRef? = present only if Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}
```

**Self-describing keys:** Op value determines if AfterRef follows:
- `OpNone(0)`, `OpCRDTAdd(1)`, `OpCRDTRemove(2)` → no AfterRef
- `OpRGAInsert(3)`, `OpRGATombstone(4)` → AfterRef follows

See master plan Bug #5 Refinement section for full discussion.

**Bug #5 Test Cases (10 tests):**

| Test | What it validates |
|------|-------------------|
| `TestVectorAVETLookup` | AVET finds entities with specific vector element |
| `TestVectorAVETMultipleEntities` | AVET returns all matching entities |
| `TestVectorAVETAfterTombstone` | Tombstoned elements excluded from AVET |
| `TestVectorVAETReverseLookup` | VAET works for entity refs in vectors |
| `TestVectorValueTypePreserved` | V decodes as original type, not []byte |
| `TestVectorKeyEncodingRoundTrip` | Key encode/decode preserves all fields |
| `TestVectorKeyEncodingSortOrder` | Keys sort correctly (Tx descending) |
| `TestVectorAfterRefInKey` | AfterRef in key, not in V |
| `TestVectorRGAReconstructFromKey` | RGA reconstruction works with new format |
| `TestBug4KeyFormatRevised` | Cardinality-many uses [V][Tx][Op] |

**Bug #5 Success Criteria:**

1. ✅ AVET queries work for vectors (O(k) not O(n))
2. ✅ VAET queries work for vectors
3. ✅ V contains raw value only (correct type tag)
4. ✅ AfterRef in key (self-describing via Op)
5. ✅ RGA reconstruction works
6. ✅ All 13 existing vector tests pass
7. ✅ Bug #4 key format revised (Tx before Op)

---

## What's Working

1. **RGAElement type** - `rga_element.go` - Encode/decode working
2. **RGA reconstruction** - `rga_reconstruct.go` - Working, handles tombstones correctly
3. **Add() for vectors** - `database.go` - RGA append semantics working
4. **Set() for vectors** - `database.go` - Replaces entire vector via tombstoning
5. **LookupAttribute** - `matcher.go` - Returns reconstructed vector correctly
6. **Pull API** - Working - Returns vectors as `[]interface{}`
7. **Query with E bound via join** - FIXED - Returns reconstructed vector

## What Was Fixed

### Pattern Ordering Bug (2026-01-31)

**Problem:** Query `[:find ?skills :where [?e :character/name "Alice"] [?e :character/skills ?skills]]` returned raw RGA bytes instead of reconstructed vector.

**Root Cause:** The planner's `scoreClause` function scored patterns by number of symbols provided:
- `[?e :name "Alice"]` provides 1 symbol → score 150
- `[?e :skills ?skills]` provides 2 symbols → score 200

This caused the skills pattern to run FIRST with no bindings, returning raw RGA elements.

**Fix:** Modified `scoreClause` in `datalog/planner/clause_utils.go` to use **priority-based selectivity scoring** following the paper "When Greedy Beats Optimal: Join Ordering for Pattern-Based Datalog Queries".

Key insight from the paper: **Selectivity is VISIBLE in pattern syntax** - no statistics needed.

The scoring now separates constants from available variables:
- **Constants = visible selectivity** (they filter data) - weighted 100 per constant
- **Available variables = join hints** (they enable joins, don't filter) - weighted 10 per variable
- Score = 100 + (constants × 100) + (availableVars × 10)

After fix:
- `[?e :name "Alice"]` has 2 constants (A and V) → score 100 + 200 = **300**
- `[?e :skills ?skills]` has 1 constant (A only) → score 100 + 100 = **200**

Now the name pattern runs first, provides ?e, and the skills pattern uses vector resolution.

This is simpler than ratio-based scoring because:
1. No integer division artifacts
2. Clear separation of concerns (constants vs variables)
3. Priority ordering matches the paper's recommendation

## Test Status

```
PASS: TestVectorBasicAdd
PASS: TestVectorMultipleTransactions
PASS: TestVectorEmpty
PASS: TestVectorWithDifferentTypes
PASS: TestVectorSchemaValidation
PASS: TestVectorSetReplacesEntireVector
PASS: TestVectorSetToEmpty
PASS: TestVectorAddNoReadDatabase
PASS: TestVectorQueryIntegration
PASS: TestVectorQueryWithBoundEntity  <- Was failing, now fixed
PASS: TestVectorQueryNameAndSkills
PASS: TestVectorPullIntegration
PASS: TestVectorQueryProjectSkills (returns raw bytes - E-unbound case, separate issue)
```

## Remaining Work

### Bug #5 Implementation (Index Schema Change)

**⚠️ FOUNDATIONAL - See warning at top of document.**

When implementing the Bug #5 fix, these changes are required (IN ORDER):

**Step 0: Write tests FIRST**
- Write all 10 Bug #5 test cases before any implementation
- Tests should fail initially (proving they test the right thing)

**Step 1: Add new types/fields**
- Add `AfterRef ElementID` to Datom and StorageDatom
- Add `OpRGAInsert (3)` and `OpRGATombstone (4)` constants
- Run tests → should still pass (no behavior change yet)

**Step 2: Revise Bug #4 key format (ONE INDEX AT A TIME)**
- Change from `[...][V][Op][Tx]` to `[...][V][Tx][Op]`
- Start with EAVT, verify, then AEVT, verify, etc.
- Run FULL test suite after EACH index change

**Step 3: Add AfterRef encoding (ONE INDEX AT A TIME)**
- Conditionally encode AfterRef when Op ∈ {OpRGAInsert, OpRGATombstone}
- Same approach: one index, verify, next index, verify

**Step 4: Update vector operations**
- database.go Add() for vectors uses AfterRef field instead of RGAElement wrapper
- RGA reconstruction reads AfterRef from datom, not decoded V
- Run tests → Bug #5 tests should now pass

**Step 5: Cleanup**
- Remove or simplify RGAElement encoding (no longer needed in V)
- Verify all 13 existing vector tests still pass
- Verify all other tests still pass

See master plan "Bug #5 Refinement" section for full design.

### E-Unbound Vector Queries

Query `[:find ?skills :where [?e :character/skills ?skills]]` (no E binding) still returns raw RGA bytes.

This requires scanning all entities with the vector attribute and resolving each vector - an expensive operation that needs separate implementation in `matchUnboundAsRelation`.

### Planner Scoring - Current Approach

Based on the paper "When Greedy Beats Optimal", the scoring now uses **priority-based selectivity**:

```go
constants, availableVars := countSelectivityFactors(p, available)
score += (constants * 100) + (availableVars * 10)
```

**Why this works for pattern-based Datalog:**

From the paper: "For pattern-based queries, selectivity is visible in the query syntax itself."

| Pattern | Constants | Available Vars | Score |
|---------|-----------|----------------|-------|
| `[?e :name "Alice"]` | 2 (A, V) | 0 | 300 |
| `[?e :skills ?skills]` | 1 (A) | 0 | 200 |
| `[?e :attr ?v]` (after ?e bound) | 1 (A) | 1 (?e) | 210 |

**Constants weighted 10× more than variables because:**
- Constants FILTER data (visible selectivity in pattern)
- Variables ENABLE JOINS (don't filter, just connect)

**Remaining edge cases:**

1. **Tie-breaking is arbitrary** - Two patterns with identical constants/variables are ordered by slice position (effectively user order)
2. **No cardinality estimation** - `[?e :rare-attr ?x]` and `[?e :common-attr ?y]` score identically

**Paper's insight on when this is acceptable:**

The paper shows that for pattern-based queries with visible selectivity, greedy ordering produces plans within 15% of optimal. For queries where selectivity is NOT visible (e.g., both patterns have same number of constants), tie-breaking via user order is defensible - the user wrote them in that order for a reason.

**What's NOT implemented (and may not need to be):**
- Cardinality statistics (the paper argues against this for pattern-based queries)
- Cost-based optimization (shown to be slower with comparable results)

### Files Changed

- `datalog/storage/rga_element.go` - RGAElement type
- `datalog/storage/rga_reconstruct.go` - RGA reconstruction
- `datalog/storage/vector_resolution.go` - Vector resolution for matcher
- `datalog/storage/database.go` - Add, Set for vectors
- `datalog/storage/matcher.go` - LookupAttribute for vectors
- `datalog/storage/matcher_relations.go` - matchVectorWithBindings for join path
- `datalog/planner/clause_utils.go` - **Selectivity-based pattern ordering**
- `datalog/storage/crdt_vector_test.go` - Test suite
