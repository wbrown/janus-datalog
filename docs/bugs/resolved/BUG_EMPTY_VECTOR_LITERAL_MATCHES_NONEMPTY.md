# Empty Vector Literal in Data Pattern Matches Non-Empty Vectors

**Date**: 2026-02-24
**Severity**: Correctness (High)
**Status**: Fixed (2026-02-25)
**Affected**: Any query using `[?e :attr []]` to match entities with empty vector attributes

## Summary

When a data pattern uses an empty vector literal `[]` in the value position (e.g., `[?e :entity/lore []]`), it matches entities that have **non-empty** vectors — the opposite of what it should do. An entity with `:entity/lore ["Deep in the mountains..."]` matches `[?e :entity/lore []]`.

Additionally, the combination with `(or ...)` is broken: `(or [(missing? $ ?e :attr)] [?e :attr []])` returns only the `[]` branch results (the wrong entities), not a union of both branches.

## Discovery

Found during PR review of a `string` → `cardinality/vector` migration in a downstream application. A query was changed from:

```clojure
(or [(missing? $ ?e :entity/lore)]
    [?e :entity/lore ""])
```

to:

```clojure
(or [(missing? $ ?e :entity/lore)]
    [?e :entity/lore []])
```

The expectation was that `[?e :entity/lore []]` would match entities with an empty vector, complementing `missing?` which matches entities without the attribute at all. Instead, the query returned entities that **had** lore and missed entities that **didn't**.

## Reproduction

Three entities created:
- POI A: no `:entity/lore` attribute
- POI B: `:entity/lore` = `["Deep in the mountains..."]`
- POI C: no `:entity/lore` attribute

### Query 1: `missing?` alone (correct)

```clojure
[:find ?code :in $ ?map ?type :where
  [?e :entity/map ?map]
  [?e :entity/type ?type]
  [?e :entity/code ?code]
  [(missing? $ ?e :entity/lore)]]
```

**Result**: `[POI A, POI C]` — correct.

### Query 2: `[?e :entity/lore []]` alone (wrong)

```clojure
[:find ?code :in $ ?map ?type :where
  [?e :entity/map ?map]
  [?e :entity/type ?type]
  [?e :entity/code ?code]
  [?e :entity/lore []]]
```

**Result**: `[POI B]` — **wrong**. POI B has non-empty lore. POI A and POI C (which have no lore at all) are not returned.

### Query 3: `(or ...)` combining both (wrong)

```clojure
[:find ?code :in $ ?map ?type :where
  [?e :entity/map ?map]
  [?e :entity/type ?type]
  [?e :entity/code ?code]
  (or [(missing? $ ?e :entity/lore)]
      [?e :entity/lore []])]
```

**Result**: `[POI B]` — **wrong**. Returns only the `[]` branch results, not the union of both branches.

## Root Causes (Confirmed)

Five issues, not two:

1. **`extractValue()` didn't handle `VectorConstant`.** It returned `nil` for vector literals, making `[]` and `["a" "b"]` both act as wildcards. Fixed in `matcher.go` — now returns `e.Values`.

2. **Vector matching paths never compared against bound V.** Even with `extractValue` fixed, three code paths (`matchCardinalityVectorAsRelation`, `matchVectorWithBindings`, `matchFromCache`) resolved the RGA vector but never compared it against the bound value. All three now call `datalog.ValuesEqual(resolved, v)` when V is non-nil.

3. **E-unbound vector patterns panicked.** Patterns like `[?e :attr []]` without E bindings reached `chooseIndex` which called `datalog.Type()` on `[]interface{}` and panicked. New `matchVectorScanAllEntities` with streaming iterator handles this path, following the `cardinalityManyScanAllEntitiesIterator` pattern.

4. **"Never set" confused with "cleared to empty".** `resolveVector` returns an empty typed slice for both "attribute never written" (`TotalElements=0`) and "attribute cleared to empty" (`TotalElements>0, LiveElements=0`). The empty literal `[]` must match only "cleared". All vector paths now check `Stats.TotalElements==0` to distinguish.

5. **`BranchHasExpressions` missed predicates.** `[(missing? $ ?e :attr)]` parses as `*DatabaseFunctionPredicate` (a `Predicate`, not `*Expression`). `BranchHasExpressions` didn't detect it, routing the OR clause to union mode which passes `nil` binding — predicate-only branches can't produce results without input bindings. Added `Predicate` detection gated on `RequiredSymbols()>0` in `query/clause.go`.

Also consolidated `ValuesEqual`: added reflect-based slice comparison for typed (`[]string`) vs untyped (`[]interface{}`) vectors, removed unsafe `fmt.Sprintf` fallback, eliminated duplicate wrapper functions.

## Files Changed

- `datalog/storage/matcher.go` — `extractValue` handles `VectorConstant`
- `datalog/storage/matcher_relations.go` — bound-V comparison in all vector paths, `matchVectorScanAllEntities` streaming iterator, cache path comparison, "never set" detection
- `datalog/query/clause.go` — `BranchHasExpressions` detects binding-dependent predicates
- `datalog/compare.go` — `ValuesEqual` slice comparison, removed `fmt.Sprintf` fallback
- `datalog/compare_test.go` — 17 `TestValuesEqualSlices` subtests
- `datalog/executor/testing.go` — eliminated duplicate `valuesEqual` wrapper
- `datalog/storage/crdt_vector_test.go` — 19 `TestVectorLiteralMatch` subtests (string, int64, keyword) + `TestVectorLiteralWithOr`

## Regression Tests

- `datalog/storage/crdt_vector_test.go:TestVectorLiteralMatch` — 19 subtests covering empty/populated literals across string, int64, keyword vector types; exact match, partial match rejection, unbound V behavior
- `datalog/storage/crdt_vector_test.go:TestVectorLiteralWithOr` — the original `(or [(missing? ...)] [?e :attr []])` pattern
- Downstream application has a corresponding regression test