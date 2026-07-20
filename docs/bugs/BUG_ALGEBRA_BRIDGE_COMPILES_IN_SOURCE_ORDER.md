# BUG: The algebra bridge compiles clauses in source order — NOT before its binder fails only with the optimizer on

**Status**: Open (2026-07-19). Confirmed mechanically from the code chain; reported by external review against `a58c0f8`. Loud error, no wrong data — but only on the default path, and only because of clause order, which Datalog semantics say must not matter. Fix direction pending an owner ruling; the red pin rides the optimizer mode matrix (`docs/wip/OPTIMIZER_MODE_MATRIX.md`).

## Symptom

```clojure
[:find ?goal
 :where (not [?x :event/goal ?goal])
        [?goal :entity/type :type/goal]]
```

The NOT is written before the clause that binds its shared `?goal`. With `EnableAlgebraOptimizer` off, the query plans and executes: the planner schedules on canonical clause scopes, so the NOT waits until `?goal` is bound (pinned by `TestNotClauseWithExistentialBodyVariablePlans`, which places a NOT first in the clause list). With the optimizer on — the default — the same query errors: "NOT clause requires prior relation".

## Root cause

`optimizeViaAlgebra` runs `Compile(q)` before phasing, and `compileClausesFrom` (`datalog/algebra/compile.go`) folds clauses in source order: each clause compiles against the tree of the clauses written before it. `compileNot` requires a prior relation (`current == nil` → error), and a NOT whose binder appears later in the text has none. The planner's reordering never gets a chance — the bridge fails the whole query first ("algebra optimization failed: ...").

The reviewer reports base main failed this shape on both paths (pre-scoped-body, every NOT body variable was a hard scheduling requirement), so no released behavior regressed — but the scoped-body work made the baseline path order-independent while the bridge stayed order-dependent, and the divergence violates both the language contract (clause order must not change meaning) and the optimizer-transparency invariant (the optimizer must never change whether a query works).

Related: `compileNot`'s unification check (`NOT clause ... shares no variable with the outer relation`, added with the disjoint-NOT ruling) evaluates against the source-order prefix, not the whole query — for a NOT-before-binder it reports insufficient unification for a query that unifies fine. Same root cause, same fix: the bridge must see clauses in a dependency-honoring order.

## Fix direction (owner ruling pending)

Two candidate shapes:

1. **Pre-order clauses entering `Compile`** using the same `ScopeOf`-driven readiness the planner's `createPhasesGreedy` uses — one canonical ordering authority, applied before the bridge folds.
2. **Teach `compileClausesFrom` to defer**: clauses that cannot compile against the current tree go to a retry queue; error only when a full pass makes no progress.

Whichever is ruled, the pin is a both-modes matrix test of the symptom query: green on `algebra_off` today, red on `algebra_on` until the fix.
