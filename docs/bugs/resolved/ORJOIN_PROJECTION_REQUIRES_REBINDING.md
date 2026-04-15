# or-join projection variables must be independently bound in each branch

## Trigger

A query uses `or-join` to check a condition on two different entities, with shared bindings established in outer clauses:

```clojure
[:find ?item
 :in $ ?owner ?location
 :where
 [?container :container/owner ?owner]
 [?container :container/location ?location]
 [?container :container/item ?item]
 (or-join [?container ?item]
   [?item :item/flagged true]
   [?container :item/flagged true])
 (not [?container :container/archived true])
 (not [?container :entity/deleted true])]
```

The intent: `?container` and `?item` are bound by the outer clauses; the `or-join` just varies which entity carries the `:item/flagged` attribute. This is the idiomatic Datomic pattern for "check a property on either of two related entities."

## Error

```
NOT-JOIN variable ?container not found in input relation
```

The trailing `(not ...)` clause fails because the planner does not propagate the outer bindings of `?container` and `?item` through the `or-join`.

## Workaround

Repeat all binding clauses inside each `or-join` branch so every projection variable is independently bound:

```clojure
[:find ?item
 :in $ ?owner ?location
 :where
 (or-join [?container ?item]
   (and [?container :container/owner ?owner]
        [?container :container/location ?location]
        [?container :container/item ?item]
        [?item :item/flagged true])
   (and [?container :container/owner ?owner]
        [?container :container/location ?location]
        [?container :container/item ?item]
        [?container :item/flagged true]))
 (not [?container :container/archived true])
 (not [?container :entity/deleted true])]
```

This works but duplicates the binding clauses across every branch, making queries verbose and fragile.

## Root cause

The planner treats `or-join` branches as independent subqueries. Each branch must produce a complete relation for the projection variables listed in the `or-join` head. When a branch contains only `[?item :item/flagged true]`, the planner sees `?container` as unbound within that branch and either drops it from the output relation or errors when a downstream clause (like `not-join`) references it.

In Datomic, `or-join` inherits bindings from the enclosing `where` context. Variables bound before the `or-join` are available inside each branch without re-binding. The janus-datalog planner does not implement this outer-binding inheritance for `or-join` projection variables.

## Impact

Any `or-join` that varies a condition while depending on shared bindings from outer clauses requires duplicating those outer clauses inside every branch. For queries with many shared predicates and multiple `or-join` branches, this can triple or quadruple the query size.

## Expected behavior

Variables named in `or-join`'s projection list that are already bound in the enclosing `where` context should be available inside each branch. Branches should only need to bind variables that are new to the branch. The `or-join` result should be the union of branch results, with outer-bound variables carried through.

## Discovered

2026-04-14. A query needed to check a boolean flag on either of two related entities joined by a reference attribute. The idiomatic single-clause-per-branch form produced the NOT-JOIN error; the workaround required duplicating three binding clauses into each branch.

## Root cause (resolved 2026-04-15)

Three independent bugs, all in the "silent failure" family:

### 1. Planner: `extractOrJoinClauseSymbols` was dead code

`extractClauseSymbols` in `clause_utils.go` had a correct `extractOrJoinClauseSymbols` function (line 414) that properly tracked join vars as Requires when not produced by all branches. But the switch case at line 40 bypassed it, converting OrJoinClause to OrClause and discarding JoinVars with the comment "join vars are planner scheduling metadata." This meant the planner never knew that or-join needed outer-bound variables.

**Fix**: Changed the switch case to call `extractOrJoinClauseSymbols`.

### 2. Executor: `executeOrJoinClause` was dead code

Both call sites (lines 164 and 2087 in `query_executor.go`) converted `OrJoinClause` to `OrClause` before executing, discarding JoinVars. A correct `executeOrJoinClause` function existed at line 1625 with correlated union logic but was never called.

**Fix**: Changed both call sites to call `executeOrJoinClause`. Simplified the function to always use correlated execution, since or-join's purpose is to vary which clause matches while preserving outer-bound join variables — the uncorrelated path is semantically wrong for or-join.

### 3. `findConstantBindableScalars` removed subquery inputs from relations

Scalar inputs that only appeared as subquery arguments (not in data patterns) were classified as "constant-bindable" and projected out of the input relation. The subquery executor then couldn't find them. This was a separate silent failure: `getUniqueInputCombinations` returned `nil` (no error) when an input symbol was missing.

**Fix**: `findConstantBindableScalars` now also checks `collectSubqueryInputSymbols` before classifying a scalar as constant-bindable. `getUniqueInputCombinations` now returns an error instead of silent nil.

### Additional fixes found during investigation

- **Empty subquery results silently skipped**: `executeSubquery` had a `continue` that skipped input combinations when the nested query returned zero groups. Replaced with explicit empty relation creation so the code path is transparent.
- **Time values decoded in local timezone**: `value_encoding.go` used `time.Unix(0, nanos)` which returns local time, not UTC. On machines in non-UTC zones, `day`/`month`/`year` extraction returned wrong values. Fixed to `time.Unix(0, nanos).UTC()`.

## Regression tests

- `datalog/storage/orjoin_projection_regression_test.go` — 4 subtests covering the idiomatic pattern, the workaround, the qb builder variant, and the simple case
- `datalog/storage/orjoin_cardinality_test.go` — verifies that multi-valued attributes require cardinality-many schema
- `datalog/executor/subquery_input_error_test.go` — verifies `getUniqueInputCombinations` returns errors for missing symbols
- `datalog/executor/product_test.go` — verifies `Product()` preserves disjoint symbols
