# NOT clause variables not found in input relation

GitHub: #58

## Trigger

A NOT clause inside a correlated subquery (or any query where clause ordering matters) fails at execution time:

```
query execution failed: phase 1 failed: clause 12 (not) failed: NOT clause variables not found in input relation
```

The variable used in the NOT clause IS bound by prior patterns, but the planner/executor reports it as missing.

## Reproduction

```clojure
;; Correlated subquery with NOT inside
[:find (count ?i)
 :in $ ?p
 :where [?i :item/project ?p]
        [?i :item/status :status/done]
        (not [?i :item/deleted true])]
```

`?i` is bound by the first two patterns. The NOT clause uses `?i` as a join
variable against the outer relation. This should work — `?i` is available when
the NOT executes.

## Root cause (two bugs)

### Bug 1: Planner — `extractNotClauseSymbols()` over-requires

**File**: `datalog/planner/clause_utils.go:192-215`

```go
func extractNotClauseSymbols(n *query.NotClause) ClauseSymbols {
    requires := make(map[query.Symbol]bool)
    for _, innerClause := range n.Clauses {
        innerSymbols := extractClauseSymbols(innerClause)
        for _, sym := range innerSymbols.Requires {
            requires[sym] = true
        }
        for _, sym := range innerSymbols.Provides {
            requires[sym] = true  // BUG: inner-produced vars aren't prerequisites
        }
    }
    // ...
}
```

The function treats ALL variables from inner patterns as prerequisites — including variables that the inner patterns themselves produce. A NOT clause `(not [?t :entity/deleted ?x])` has inner pattern `[?t :entity/deleted ?x]` which **provides** `?x`. The planner incorrectly says `?x` must be bound before the NOT can execute.

This causes `selectPhaseClauses()` to defer the NOT to a later phase (or never schedule it), because the inner-produced variable isn't available in the outer scope.

Compare with `extractNotJoinClauseSymbols()` (line 220) which correctly uses only the explicit `JoinVars`:

```go
func extractNotJoinClauseSymbols(n *query.NotJoinClause) ClauseSymbols {
    return ClauseSymbols{
        Requires: n.JoinVars,  // Only explicit join vars
        Provides: nil,
    }
}
```

### Bug 2: Executor — `collectInnerVars()` returns all vars, not just join vars

**File**: `datalog/executor/helpers.go:346`

`collectInnerVars()` collects every variable from inner patterns. This becomes the `joinVars` passed to `filterWithNotClause()`. When the input relation only has outer-scope variables, the intersection can come up empty.

For `(not [?t :entity/deleted ?x])`:
- `collectInnerVars` returns `[?t, ?x]`
- Input relation only has `?t` (bound by prior patterns)
- `actualJoinVars` filters to `[?t]` — this works for simple cases

The executor bug amplifies Bug 1: when the planner puts the NOT in a wrong phase (due to over-requirement), the input relation may not have ANY of the expected variables.

### Combined failure chain

1. `extractNotClauseSymbols()` says NOT requires `[?t, ?x]` (Bug 1 — `?x` is inner-produced)
2. `selectPhaseClauses()` defers NOT until `?x` is available — which never happens since `?x` is produced inside the NOT itself
3. NOT gets placed in a phase where its actual join variable (`?t`) isn't in the input relation
4. `filterWithNotClause()` computes `actualJoinVars = []` → error

## Correct behavior

`extractNotClauseSymbols()` should only require variables that overlap with the outer scope — i.e., variables that must be pre-bound for the NOT to execute as an anti-join. Variables produced exclusively inside the NOT's inner patterns are local to the NOT's execution.

The fix should make `extractNotClauseSymbols()` behave like `extractNotJoinClauseSymbols()`: distinguish between variables that must come from outside (Requires) and variables produced internally (inner Provides that aren't also inner Requires).

## Workaround

Replace `(not [?t :entity/deleted true])` with `(missing? $ ?t :entity/deleted)`. This is semantically correct for deletion flags but NOT should still work for general anti-join patterns.

## Confirmed reproduction

Two tests in `datalog/storage/not_clause_regression_test.go` reproduce two manifestations:

### 1. Production query (exact reproduction of issue #58)

`TestNotClauseComplexQuery_E2E` builds a complex query exercising the interaction between NOT, multiple get-else, 3 OR-with-correlated-subquery branches, comparison binding, and order-by.

Error: `phase 1 failed: clause 12 (not) failed: NOT clause variables not found in input relation`

The planner places the NOT as clause 12 in phase 1. By execution time, the input relation doesn't contain the NOT's join variable (`?scenario`). The complex query structure (get-else, OR-with-subquery) changes phasing behavior so the NOT ends up in a context where its variable is unavailable.

### 2. Inner-only variable (extractNotClauseSymbols bug)

`TestNotClauseWithUnboundInnerVar_E2E` triggers the planner-side bug directly:

```clojure
[:find ?name
 :where [?e :item/name ?name]
        (not [?e :item/error ?err])]   ;; ?err is inner-only → planner stuck
```

Error: `cannot create phase: 1 clauses remaining but none can execute with available symbols`

The planner treats `?err` (produced inside the NOT) as a prerequisite. Since `?err` is never bound in the outer scope, the NOT can never be scheduled.

## Fix

### The one-line fix

`compileNot` in `algebra/compile.go` already computed the correct join variables:
`joinSyms = sharedSymbols(current.Symbols(), inner.Symbols())`. It set
`ExplicitJoin: false`, causing the decompiler to emit `NotClause` (implicit join
vars). Changed to `ExplicitJoin: true` — the decompiler now emits `NotJoinClause`
with declared join vars.

The executor uses standard "all required" scheduling for `NotJoinClause`. No
special cases, no runtime inference, no planner modifications needed.

### Why this works

The algebra bridge has the context to resolve NOT's join variables statically —
`current.Symbols()` contains the outer relation's symbols at the point the NOT
is compiled. The intersection with inner symbols gives exactly the join keys.
This is the same computation a user performs when writing `not-join` manually.

## Version

Regression between v0.9.1 (working) and v0.10.2.
