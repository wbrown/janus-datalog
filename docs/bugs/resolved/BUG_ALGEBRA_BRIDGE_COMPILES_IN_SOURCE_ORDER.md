# BUG: The algebra bridge compiles clauses in source order — NOT before its binder fails only with the optimizer on

**Status**: RESOLVED (2026-07-20). Fix direction ruled: pre-order clauses entering `Compile` by the same `ScopeOf`-driven readiness the planner's phasing uses. Originally reported by external review against `a58c0f8`; loud error, no wrong data.

## Resolution (2026-07-20)

Readiness now has one home: `query.ClauseBlockers` / `query.ClauseReady` (`datalog/query/clause_ready.go`, moved verbatim from the planner's `clauseReady` together with `query.CountProviders`). The planner's `clauseSelectable` and the bridge's new compile-order pass both consume it, so the bridge and the planner agree by shared definition on which queries are schedulable.

The bridge orders every clause list before folding: `orderClausesForCompile` (`datalog/algebra/compile_order.go`) repeatedly takes the first source-order clause that is ready under the symbols bound so far — the identity permutation for lists already in dependency order. `compileClausesFrom` applies it at every fold site, so OR branches and NOT/not-join bodies are ordered too (body ordering seeds availability from the enclosing relation's symbols). A stall — some clause's mandatory correlates bindable by no input and no clause — is rejected loudly naming each stuck clause and its blocking symbols, the same queries the planner's phasing rejects with its no-progress error.

Two adjacent defects fell out of the same change:

- `optimizeAlgebra` built `&query.Query{Where: clauses}`, stripping `:in` — the bridge could never know an input-bound correlate was bindable. It now passes the full query, and `Compile` seeds ordering with the `:in` symbol set (`ConstantInputSymbols` ∪ `IteratedInputSymbols`).
- The decorrelation rewrite compiled inner queries via bare `compileClauses(decorrelated.Where)`, also dropping the inner `:in`; both sites now call `Compile(decorrelated)`.

`compileNot`'s prefix-scoped unification check now evaluates against a prefix containing every binder, so the false rejection is gone; its checks remain as backstops. Two invalid-query rejection tests (`TestCompileRejectsInvalidNotJoinHeaders/header_symbol_not_bound_outside`, `TestCompileRejectsPlainNotWithUnboundOuterRequirement`) now reject at the ordering gate — upstream of the old sites — with messages that still name the unbindable symbol.

## Reproducers (red-first, now green)

- `datalog/executor/not_or_test.go` / `TestClauseOrderIndependenceForNot` — both documented shapes (NOT before its binder; NOT after a non-unifying prefix), executed on both optimizer modes with results asserted.

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
