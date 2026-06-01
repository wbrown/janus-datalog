# Correlated union only injects one input group as outer relation

## Trigger

A query with multiple disjoint input groups (e.g., `?self` from scalar input and `?fwd` from collection input) and a correlated union OR:

```clojure
[:find ?related :in $ ?self [?fwd ...]
 :where
 (or (and [?self ?fwd ?target]
          (or [?related ?fwd ?target]
              [(identity ?target) ?related]))
     (and [?self :item/label ?label]
          [?related :index/mentions ?label])
     (and [?self :item/sku ?sku]
          [?related :index/mentions ?sku]))]
```

## Symptoms

- **Performance**: Multi-second query for 0 results on a database with hundreds of entities
- **Correctness**: `?fwd` is unbound inside the OR, matching ALL attributes instead of just the collection input keywords

## Root cause (two bugs)

### Bug 1: `findOuterRelation` picks only the first matching group

```go
func (e *DefaultQueryExecutor) findOuterRelation(...) Relation {
    for i, rel := range groups {
        if containsAny(rel.Symbols(), neededSymbols) {
            return groups[i]  // Returns FIRST match, ignores others
        }
    }
}
```

When `groups = [selfRel{?self}, fwdRel{?fwd}]`, it returns `selfRel` and leaves `fwdRel` behind. The OR iterator receives an outer relation with only `?self` — `?fwd` is never injected.

### Bug 2: `nextCorrelatedUnion` evaluates branches before advancing to first outer tuple

On the first call, `unionInputRel` is nil and `unionBranchIdx` is 0. The branch loop executes all branches with nil input (full unbound scan) before ever advancing the outer iterator.

### Consequence chain

1. `findOuterRelation` returns only `selfRel{?self}` (1 tuple)
2. `nextCorrelatedUnion` evaluates branches with nil input on first call
3. `[?self ?fwd ?target]` with `?fwd` free → matches ALL attributes of `?self`
4. For each attribute, the inner OR evaluates `[?related ?fwd ?target]` across the entire DB
5. Each evaluation creates BadgerDB iterators → goroutine scheduler thrashing
6. ~10 attributes × ~700 entities × multiple scans = `pthread_cond_signal` at 61% CPU

### Profile evidence

```
61.71%  runtime.pthread_cond_signal  (goroutine scheduler thrashing)
19.17%  HashJoinWithOptions          (actual work)

Call trace:
Execute → Collapse → HashJoin → OrFallbackIterator.nextCorrelatedUnion
  → executeInnerClauses → Collapse → pthread_cond_signal
```

## Fix

### Fix 1: `findOuterRelation` joins ALL matching groups

```go
var result Relation
for i, rel := range groups {
    if containsAny(rel.Symbols(), neededSymbols) {
        groups[i] = groups[i].Materialize()
        if result == nil {
            result = groups[i]
        } else {
            result = result.Join(groups[i])
        }
    }
}
```

Same fix applied to `findOuterRelationBySymbols`.

### Fix 2: `nextCorrelatedUnion` advances to first outer tuple before evaluating branches

```go
if it.unionInputRel == nil && it.unionBranchIdx == 0 {
    if !it.outerIter.Next() { ... }
    it.unionOuterTuple = it.outerIter.Tuple()
    it.unionInputRel = NewMaterializedRelationWithOptions(...)
}
```

## Key files

| File | Lines | Role |
|------|-------|------|
| `executor/query_executor.go` | `findOuterRelation` | Joins all matching groups |
| `executor/query_executor.go` | `findOuterRelationBySymbols` | Same fix for join-var variant |
| `executor/or_fallback_relation.go` | `nextCorrelatedUnion` | First-iteration initialization |

## Reproduction

`TestOrCorrelatedUnionPartialOuterRelation` in `storage/or_correlated_perf_test.go`
