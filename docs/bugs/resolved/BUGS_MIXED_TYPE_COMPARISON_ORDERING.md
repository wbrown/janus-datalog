# BUG: Mixed-Type `CompareValues` Ordering Is Not Antisymmetric

**Date**: 2026-05-31 **Severity**: Correctness (Medium) **Status**: ✅ RESOLVED (2026-05-31) **Affected**: Comparison predicates (`<`, `<=`, `>`, `>=`), chained comparisons, ordering, min/max over mixed-type values

## Resolution

Fixed via **Option A (type-ranked total ordering)**. `CompareValues` is now antisymmetric and total across types: when two values are not the same comparable type, it orders them by a stable type rank (`typeRank`) instead of returning `-1` in both directions.

The rank is a **custom** order, deliberately NOT the on-disk `ValueType` tag order. Investigation found the tag order is not a valid rank for `CompareValues`: it separates `TypeInt` (1) and `TypeFloat` (2), but the comparator correctly compares int and float *numerically* across that boundary. So int and float share a single numeric rank here:

```
nil(0) < numeric:int/uint/float(1) < bool(2) < time(3) < string(4) <
bytes(5) < keyword(6) < symbol(7) < identity(8) < elementID(9) < unknown(10)
```

Same-rank values compare by value (numerics by magnitude, including int↔float); different ranks compare by rank. Every former `return -1` type-mismatch site in `compare.go` now returns `compareByRank(left, right)`. The numeric helpers (`compareNumeric`/`compareFloat`/`compareUint64`) keep their direct `return -1` for the numeric-vs-non-numeric case — that is now *correct and antisymmetric* by construction (numeric is rank 1, so it sorts before every higher rank; the reverse direction reaches `compareByRank` and yields +1). A missing `[]byte` same-type branch was added (compares via `bytes.Compare`).

`compareByRank`/`typeRank` are on the type-mismatch cold path only; the hot path (same-type Identity/int64/string compares) returns before any rank logic, so performance is unchanged.

This fixes all caller categories with no call-site changes: the `sort.Slice` order-by comparators (which a non-antisymmetric comparator left in undefined behavior), the `<,<=,>,>=` predicates (which previously passed in both directions for mixed types), and `min`/`max` aggregation.

`ValuesEqual` is untouched — equality semantics for mixed types were already correct (different types are unequal).

### Tests

- `datalog/compare_ordering_laws_test.go` (new) — pins the comparator laws: antisymmetry across a full mixed-type matrix, transitivity, `cmp==0 ⇒ equal`, sort-stability of a heterogeneous slice, the both-directions predicate reproducer, and a guard that int↔float still compare numerically. The antisymmetry/transitivity/sort/predicate tests were verified to FAIL against the unfixed comparator.
- Three existing assertions that pinned the broken `-1` behavior were updated to the documented rank direction (and strengthened with the reverse-direction check): `compare_test.go` symbol-vs-string and elementID-vs-int64, and `executor/filter_test.go` string-vs-int.

Full `go test ./...` green.

## Summary

`datalog.CompareValues` uses `-1` as the fallback result for many type mismatches. That makes the comparator non-antisymmetric: for some mixed-type pairs, both `CompareValues(a, b) < 0` and `CompareValues(b, a) < 0` are true.

The immediate user-visible consequence is that relational predicates can accept both directions of an invalid comparison. For example, string-vs-int and int-vs-string both compare as "left is less than right" through different branches.

This is fine for `=`/`!=` as a rough "different types are unequal" signal, but it is not valid ordering semantics.

## Root Cause

`CompareValues` treats type mismatch as an ordering result instead of either:

1. applying a total type-rank ordering consistently in both directions, or
2. reporting that values are not order-comparable.

Examples from `datalog/compare.go`:

```go
if id1, ok := left.(Identity); ok {
    if id2, ok := right.(Identity); ok {
        return id1.Compare(id2)
    }
    // Identity vs non-Identity: type mismatch
    return -1
}

if kw1, ok := left.(Keyword); ok {
    if kw2, ok := right.(Keyword); ok {
        return kw1.Compare(kw2)
    }
    // Keyword vs non-Keyword: type mismatch
    return -1
}
```

The same shape exists for strings, bools, times, numeric-vs-nonnumeric, and other branches.

Predicates then interpret the comparator result directly:

```go
cmp := datalog.CompareValues(leftVal, rightVal)

switch c.Op {
case OpLT:
    return cmp < 0, nil
case OpLTE:
    return cmp <= 0, nil
case OpGT:
    return cmp > 0, nil
case OpGTE:
    return cmp >= 0, nil
}
```

## Reproduction Sketch

Any query path that evaluates a comparison over mixed types can exhibit the problem:

```clojure
[:find ?e
 :where [?e :thing/value ?v]
        [(< ?v "zzz")]]
```

If `?v` is an integer, `CompareValues(int64(...), "zzz")` returns `-1`, so the predicate passes.

The reverse direction can also pass:

```clojure
[:find ?e
 :where [?e :thing/value ?v]
        [(< "zzz" ?v)]]
```

If `?v` is an integer, `CompareValues("zzz", int64(...))` also returns `-1`, so this predicate can pass too.

## Impact

1. **Incorrect predicate results**: Range predicates can include rows whose value type is not actually order-comparable with the constant.

2. **Invalid chained comparisons**: A chained comparison can succeed due to arbitrary type-mismatch ordering rather than meaningful value order.

3. **Unstable sorting assumptions**: `sort.Slice` expects a strict weak ordering. A comparator where both `a < b` and `b < a` can be true violates that contract for mixed-type result sets.

4. **Potential min/max surprises**: Aggregations over heterogeneous values can select winners based on type-branch accident rather than a defined ordering.

## Why This Is Subtle

The current behavior is partially tested as "type mismatch returns -1" in some unit tests. That pins a behavior, but not a valid ordering invariant. The tests check individual direction outcomes; they do not check comparator laws:

- antisymmetry: `cmp(a,b) == -cmp(b,a)`
- equality consistency: `cmp(a,b)==0` iff values are equal under the comparator
- transitivity: if `a < b` and `b < c`, then `a < c`

The bug is not that mismatched types are unequal. The bug is that mismatched types are treated as ordered in a direction that is not consistent.

## Fix Direction

There are two plausible fixes; choose the semantics explicitly:

### Option A: Type-Ranked Total Ordering

Define a stable type rank:

```go
nil < bool < int/float < time < string < keyword < symbol < identity < elementID < bytes < vector
```

Then mixed-type comparisons use rank order before same-type value comparison. This keeps `CompareValues` total and usable for sorting/min/max.

### Option B: Separate Equality From Ordering

Keep `ValuesEqual` for equality, and add an ordered comparison API that can return an error for non-orderable type pairs:

```go
CompareOrdered(left, right any) (int, error)
```

Then `<`, `<=`, `>`, `>=`, `min`, `max`, and order-by can decide whether to error or apply a documented type rank.

This is semantically cleaner but touches more call sites.

## Verification Plan

Add comparator-law tests:

1. Mixed-type pair matrix verifies antisymmetry.
2. Sorting a heterogeneous slice does not violate strict weak ordering.
3. Relational predicates over mismatched types either error or follow documented type rank consistently.
4. `=` and `!=` remain correct for mixed types.
5. Existing int-width normalization behavior remains unchanged.

Example regression cases:

```go
require.Equal(t, -CompareValues("x", int64(1)), CompareValues(int64(1), "x"))
require.False(t, less("x", int64(1)) && less(int64(1), "x"))
```
