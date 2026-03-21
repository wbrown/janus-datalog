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

### Why the naive fix is wrong

The obvious fix — "only include inner Requires, not inner Provides" — doesn't work.
`extractPatternSymbols` returns `Requires: nil` for DataPatterns (patterns don't
require symbols; they provide them). So computing the inner clauses' free variables
gives an empty set, and the NOT would have no Requires at all. It could be scheduled
before any of its join variables are bound, and the anti-join would have no keys.

### Why executor-side fixes are wrong

Adding runtime decision-making to the executor (special-casing NOT in the scheduler,
intersecting inner variables with input symbols, passing through disjoint groups)
is the wrong layer. The executor should be dumb. Cataloguing the runtime decisions
this approach requires:

1. `collectInnerVars` — executor walks the NOT's inner clauses at runtime to compute
   candidate join variables
2. `filterWithNotClause` intersection — executor intersects candidates with input
   symbols to determine actual join variables at runtime
3. `filterWithNotClause` disjoint check — executor decides "does this NOT apply to
   this group?" at runtime
4. `canExecuteClauseWithContext` NOT special case — planner scheduler needs
   NOT-specific "any one available" logic instead of the universal "all required" check

Four runtime decisions for something the planner has full context to resolve statically.

### The real problem

A NOT clause is an anti-semi-join. Its join variables are **context-dependent** —
they're the intersection of inner variables with whatever the outer scope has produced.
This can't be computed statically from the NOT clause alone.

But the **planner** has the context. When the planner schedules a NOT clause, it
knows exactly which symbols are available. It can compute the join variables at
planning time.

### The correct fix: `not` → `not-join` in the planner

Datalog already has the right abstraction: `not-join`. In Datalog syntax:

```clojure
(not [?e :attr ?v])              ;; implicit join vars — context-dependent
(not-join [?e] [?e :attr ?v])    ;; explicit join vars — context-independent
```

`not-join` declares its join variables. The executor handles `NotJoinClause`
with zero runtime decisions:

```go
func extractNotJoinClauseSymbols(n *query.NotJoinClause) ClauseSymbols {
    return ClauseSymbols{
        Requires: n.JoinVars,  // explicit, statically known
        Provides: nil,
    }
}
```

Standard "all required" scheduling works. No special cases.

The fix: **the planner converts `NotClause` to `NotJoinClause`** during planning.
The join variables are the intersection of the NOT's inner variables with the
symbols available at the point the NOT is scheduled. This is exactly what a user
does when they write `not-join` — the planner infers it automatically.

All four runtime decisions disappear:

1. **Gone.** Join variables are declared in `NotJoinClause.JoinVars`.
2. **Gone.** No intersection needed — `JoinVars` ARE the intersection.
3. **Gone.** A `NotJoinClause` with correct join vars is never applied to a
   disjoint group, because the planner scheduled it when those vars are available.
4. **Gone.** `NotJoinClause` uses standard "all required" semantics. No special case.

### Where to do the conversion

In the algebra bridge, not the planner. This is a structural AST rewrite
(converting one clause type to another), which is the bridge's job. The planner
should receive well-formed `NotJoinClause` and schedule it — not convert clause
types.

The bridge doesn't need execution-order context. The join variables are a static
property of the query:

```
joinVars = innerVars(NOT) ∩ varsInRestOfQuery(WHERE)
```

Any variable that appears both inside the NOT and anywhere else in the query's
where clauses is a join variable. This is computable from the query AST alone.
The planner then schedules the resulting `NotJoinClause` with standard "all
required" semantics — it naturally defers until those join vars are bound.

### Why this is algebraically correct

An anti-semi-join R ▷ S requires shared attributes between R and S to define the
join predicate. The shared attributes are `schema(R) ∩ schema(S)` — exactly the
intersection of available symbols with inner variables. This intersection is known
at planning time. Encoding it in the clause type (`NotJoinClause`) makes the
semantics explicit, statically verifiable, and requires no runtime interpretation.

### Required tests

**1. Bridge unit test** — verify the algebra bridge emits `NotJoinClause` with
correct join vars, not `NotClause`. Input: query with `(not [?e :attr ?v])` where
`?e` appears elsewhere. Assert: output clause is `*query.NotJoinClause` with
`JoinVars = [?e]`.

**2. Semantic equivalence** — `(not [?e :attr ?v])` and
`(not-join [?e] [?e :attr ?v])` produce identical results on the same data.
Proves the bridge transformation is semantics-preserving.

**3. All inner variables shared** — `(not [?e :attr ?v])` where both `?e` and
`?v` are bound in outer scope. Bridge should emit `JoinVars = [?e, ?v]`.

**4. Multiple inner clauses** — `(not [?e :a ?v] [?v :b ?x])` where only `?e`
is in outer scope. Bridge should compute inner vars across all clauses
(`{?e, ?v, ?x}`), intersect with outer (`{?e}`), emit `JoinVars = [?e]`.

**5. No shared variables** — `(not [?x :attr ?y])` where neither appears
elsewhere in the query. This is a meaningless NOT (no join key). Should be a
planning error, not a silent cross-product or no-op.

**6. NOT inside subquery** — the bridge must handle NOT clauses in subquery
where-clauses, not just top-level.

## Implementation: three bugs, not two

Annotation tracing of `TestNotClauseComplexQuery_E2E` revealed a third bug
that the NOT error was masking.

### Bug 3: Executor — `filterWithNotClause` errors on disjoint groups

**File**: `datalog/executor/query_executor.go` (filterWithNotClause)

`executeNotClause` iterates over ALL relation groups and applies
`filterWithNotClause` to each. When `actualJoinVars` is empty (no overlap
between the NOT's inner variables and this group's symbols), it errors:

```go
if len(actualJoinVars) == 0 {
    return nil, fmt.Errorf("NOT clause variables not found in input relation")
}
```

A group disjoint from the NOT should pass through unchanged — the NOT simply
doesn't apply to it. This is the direct source of the "NOT clause variables
not found in input relation" error in the production query.

**Fix**: return `input` (unchanged) when `actualJoinVars` is empty.

### Why disjoint groups exist in the complex query

The algebra bridge (`algebra/bridge`) rewrites `get-else` expressions into
`OrDefaultJoinClause` clauses. Annotation tracing shows:

```
algebra/bridge-complete: output_types=[
  *query.DataPattern          -- [?project :project/type ...]
  *query.NotClause            -- (not [?project :project/deleted true])
  *query.OrDefaultJoinClause  -- get-else → or-default-join (×6)
  *query.DataPattern          -- [?project :project/created-at ?createdAt]
  *query.OrClause             -- OR with correlated subqueries (×3)
  *query.Expression           -- [(> ?doneCount 0) ?ready]
]
```

Each `OrDefaultJoinClause` produces a relation with symbols like
`{?project, ?name}` that gets appended to `groups` and collapsed. But
collapses consistently show `relations.after:2` — two disjoint groups remain
after each or-default-join, because the collapse can't merge them with the
growing main relation.

By the time the NOT executes (clause 12, scored last by the greedy scheduler),
`groups` contains multiple disjoint fragments. One has `?project`, others
don't. The NOT is applied to each group; the groups without `?project` trigger
the "not found" error.

### Exposed issue: Cartesian product from disjoint groups

After fixing Bugs 1–3, the production query no longer errors but produces
16 results instead of 2 (a Cartesian product). The disjoint groups created
by the algebra bridge's get-else → or-default-join rewriting are not properly
joined with the main relation. When these groups reach the final projection,
they cross-product.

This is a **separate bug** in the algebra bridge's rewriting, not in the NOT
clause handling. The NOT error was masking it — the query failed at clause 12
before the cross-product could manifest. Fixing the NOT correctly exposes the
pre-existing algebra bridge issue.

### Summary of all fixes applied

| Bug | Location | Fix | Test |
|-----|----------|-----|------|
| 1. Planner scheduling | `canExecuteClauseWithContext` | NOT uses "any one available" instead of "all must be available" | `TestNotClauseWithUnboundInnerVar_E2E` |
| 2. Executor disjoint pass-through | `filterWithNotClause` | Disjoint groups pass through unchanged instead of erroring | `TestNotClauseComplexQuery_E2E` (no longer errors) |
| 3. Algebra bridge Cartesian product | algebra bridge get-else rewriting | **Not yet fixed** — separate issue | Production query returns 16 rows instead of 2 |

## Version

Regression between v0.9.1 (working) and v0.10.2.
