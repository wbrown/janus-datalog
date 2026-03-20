# Variable-attribute patterns don't narrow scan range from bindings

## Trigger

A pattern with a variable attribute position where both E and A are bound from a binding relation:

```clojure
[?self ?fwd ?target]
;; ?self bound to a specific entity, ?fwd bound to a specific keyword
```

## Symptoms

- `datoms.matched:2303 datoms.scanned:2303 index:EATV` — full index scan
- Expected: ~0-1 datoms scanned (one entity, one attribute)
- Multi-second query for 0 results on databases with hundreds of entities

## Root cause

When a pattern has a variable in the attribute position (`?fwd`), the matcher selects the EATV index and scans with a prefix based on the bound positions. However, the variable-attribute binding from the input relation is not used to narrow the scan range.

With `?self` bound and `?fwd` bound from a binding relation, the scan should use both as prefix constraints (E + A prefix on EATV). Instead, it scans the entire EATV index.

### Annotation evidence

```
[pattern/storage-scan] datoms.matched:2303 datoms.scanned:2303 index:EATV pattern:[?self ?fwd ?target]
[pattern/storage-scan] datoms.matched:602  datoms.scanned:602  index:AETV pattern:[?self :item/label ?label]
[pattern/storage-scan] datoms.matched:550  datoms.scanned:550  index:AETV pattern:[?self :item/sku ?sku]
```

Constant-attribute patterns correctly narrow via AETV prefix. Variable-attribute patterns scan the full index.

## Context

This bug was exposed by the correlated union fix for `findOuterRelation` (which now correctly joins all input groups into the outer relation). Previously, `?fwd` was unbound inside the OR, causing wrong results. Now `?fwd` is correctly bound, but the matcher doesn't use the binding to narrow the scan.

## Impact

Queries with variable attributes and collection inputs (`[?self ?fwd ?target]` where `?fwd` comes from `[?fwd ...]`) do full index scans instead of point lookups. On databases with hundreds of entities, this turns microsecond queries into multi-second scans.

## Key files

| File | Role |
|------|------|
| `storage/matcher_relations.go` | Pattern matching with binding relations |
| `storage/matcher_strategy.go` | Index selection and scan range computation |
| `storage/matcher.go` | Core matcher — scan range construction from bound values |

## Reproduction

`TestOrCorrelatedUnionPartialOuterRelation` in `storage/or_correlated_perf_test.go` — 500 items, event entity with no `:agent/container`, collection input `[?fwd ...]` = `[:agent/container]`. Asserts query completes in under 1 second.
