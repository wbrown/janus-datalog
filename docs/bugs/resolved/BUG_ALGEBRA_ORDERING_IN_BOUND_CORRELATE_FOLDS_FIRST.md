# BUG: Compile ordering folds a leading consumer-only clause first when its correlate is :in-bound

**Status**: RESOLVED (2026-07-20, same day as introduction). Found by external re-re-review at `d6d4721`; a hole in the clause-ordering fix (`resolved/BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`). Loud error on the default path only — no wrong data.

## Symptom

A leading NOT, not-join, or `missing?` whose only correlate is bound by `:in` failed to compile on the algebra path while the baseline planner executed it correctly:

```clojure
[:find ?name
 :in $ ?goal
 :where (not [?x :event/goal ?goal])
        [?goal :entity/name ?name]]
```

`algebra compile: NOT clause requires prior relation` (not-join and `missing?` analogues identical, with their own guard messages).

## Root cause

`orderClausesForCompile` seeds its bound set with the query's `:in` symbols, so an input-bound correlate makes a consumer-only clause `ClauseReady` at iteration zero — before anything has been folded — and first-ready source order picks it into a nil-relation fold. The prior fix handled correlates bound by a *later clause* (the provider count keeps the consumer blocked); an `:in`-bound correlate has no pending provider, so nothing forced a generator to precede it.

## Fix

The **first pick** prefers generators (non-empty `Provides`) among ready clauses; once anything is folded, a relation exists and every later pick reverts to plain first-ready source order. The first version of this fix deferred ready consumers behind *all* generators — the full gate caught it pushing `Select` nodes above `Join`s, breaking the join-project selective-child lowering and its planner contract tests — and the rule was narrowed to the first pick only, which is exactly what the requires-prior-relation invariant demands. Clause lists containing no generator at all keep the fold's loud guards (a consumer-only-clause query stays rejected on the bridge).

## Reproducers (red-first, now green)

- `datalog/executor/not_or_test.go` / `TestClauseOrderIndependenceForInBoundCorrelates` — leading NOT and not-join with `:in`-bound correlates, both modes, rows asserted.
- `datalog/storage/database_function_integration_test.go` / `TestLeadingMissingWithInBoundEntity` — the `missing?` analogue on `BadgerMatcher` (the executor package's memory matchers implement no entity lookup), both modes, both polarities.
- The prior ordering pins (`TestClauseOrderIndependenceForNot`) and the join-project contract tests (`TestInsertJoinProjects*`, `TestOptimizeViaAlgebraReturnsNestedDatalogWithoutMutatingInput`, `TestClauseBasedPlannerLowersJoinProjectIntoNestedDatalog`) unchanged and green.

## Side discovery

Pinning the `missing?` shape on the executor package's lookup-less matcher exposed an unrelated live silent-empty — `docs/bugs/BUG_MISSING_ON_LOOKUPLESS_MATCHER_SILENTLY_EMPTY.md` (open, committed-red reproducer).

## Lesson

Readiness is necessary but not sufficient for the fold: consumer-only clauses additionally require a prior relation, and any binding source that makes them ready without one (`:in` today; any future environment source tomorrow) re-opens the hole unless the ordering states the prior-relation requirement itself. And an ordering preference stronger than the invariant it serves is its own regression — defer exactly as much as the invariant requires, nothing more.
