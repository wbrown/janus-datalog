# Phase 5 CRDT Vector Implementation - Status

**Date:** 2026-01-31
**Status:** Query integration FIXED

> **Note:** This status file has been reconciled with the master implementation plan at
> `docs/proposals/CRDT_VECTOR_STORAGE_IMPLEMENTATION_PLAN.md` (Section 5.6).
> This file serves as a quick-reference for Phase 5 development.

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
