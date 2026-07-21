# BUG: Boundary validation rejects a nested existential NOT inside not-join

**Status**: RESOLVED (2026-07-20, same day as introduction). Found by external review at `cc7827b`; introduced with the boundary-validation arc. Loud rejection at all three doors, both modes, of legal Datalog — no wrong data. The same flaw was latent in `OrDefaultJoinClause.Validate` and is fixed there too.

## Symptom

Standard existential negation nested inside a not-join was rejected at parse, `qb.Build`, and executor entry:

```clojure
(not-join [?goal]
  [?ev :event/goal ?goal]
  (not [?flagger :other/flag ?goal]))
```

rejected with "not-join header [?goal] does not declare ?flagger" — but `?flagger` is the nested NOT's body-local existential variable, not an outer requirement. Bypassing the gate, the query plans and runs correctly.

## Root cause

`NotJoinClause.Validate` (and `OrDefaultJoinClause.Validate`) collected the body's correlates through `branchInterface`, which unioned `ScopeOf(clause).Correlates` per clause and **dropped the `CorrelatesOptional` flag**. A plain NOT's scope reports its body free variables as *optional* correlates — Datomic's unification rule: they unify when the environment binds them and are existential otherwise — but the flattened list made the validators treat them as mandatory outer requirements demanding header declaration.

## Fix

`branchInterface` now returns the correlates split by obligation — `(provided, mandatory, optional)` — preserving what `ScopeOf` declares instead of erasing it. The validators demand header/required-var coverage for **mandatory** correlates only (predicates, expressions, subquery inputs, explicit-join headers — the forms that cannot run unbound); optional correlates never create declaration requirements. `branchInterfaces` (the or/or-default `ScopeOf` arms) merges both classes into externals exactly as before — no behavior change there.

## Reproducers (red-first, now green)

- `datalog/executor/static_clause_validation_test.go` / `TestNotJoinAcceptsNestedExistentialNot` — the exact shape accepted at the parser door and executed on both modes with the existential-negation semantics verified (the goal with an event and no flagger is excluded; the event-less goal survives).
- The mandatory-correlate rejection pins (`TestExecutorEntryRejectsStaticallyInvalidClauses`, the correlated not-join predicate-input differentials) unchanged and green.

## Lesson

The scoping taxonomy carries an obligation bit for a reason; any consumer that flattens `ClauseScope` into a bare symbol list re-derives a weaker taxonomy and inherits a category error. Consumers take the split or take `ScopeOf` directly.
