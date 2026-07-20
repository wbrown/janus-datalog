# BUG: Subqueries With Multiple Inputs Silently Return Empty Results

**Date**: 2026-05-31 **Severity**: Medium — wrong results (silent empty), not a crash; single-input subqueries are unaffected **Status**: RESOLVED (2026-07-20) — misdiagnosis; the defect was in the reproducer's fixture, not the engine **Affected**: subquery execution for `:in` clauses with more than one input beyond `$`

## Resolution (2026-07-20)

The engine was never broken on this path. The reproducer passed its date constant as a bare EDN string — `"2025-01-01T00:00:00Z"` — while the fixture stores `:sale/date` as `time.Time` datoms. Type-strict matching correctly matches no sale against a string-typed date, the `sum` group is empty, the tuple binding drops the outer rows, and the totals read 0. That empty result is the *correct* answer to the question as asked; nothing about input forwarding was involved.

Changing the constant to the typed literal — `#inst "2025-01-01T00:00:00Z"` — makes the test pass on both optimizer modes with no engine change. Both call-site inputs (correlated variable + typed constant) bind into the nested `:in` exactly as declared.

The "Actual Behavior" hypothesis below ("only one input is bound, or the input binding collapses") is therefore refuted. Multi-input forwarding with a call-site constant is pinned green on both modes by two independent tests with two constant types:

- `datalog/executor/executor_subquery_comprehensive_test.go` / `TestSubqueryWithTwoInputs` — variable + `#inst` constant (skip removed, totals asserted: Electronics 300, Books 50).
- `datalog/executor/executor_subquery_datomic_test.go` / `TestSubqueryDatomicCompatible/MultipleInputsWithDatabase` — variable + float constant.

**Lesson**: a reproducer is itself a claim that must be verified against the value domain of its fixture. This entry stood for seven weeks as a "silent-wrong-answer engine bug" — the worst class — on the strength of a test whose constant could never match its own data. Before root-causing a silent-empty, first confirm the query's constants inhabit the same types as the datoms they must match; under type-strict matching, a wrong-typed constant produces exactly this symptom.

The sections below are the original report, retained unedited.

## Summary

A correlated subquery that declares more than one input after the database — e.g. `:in $ ?cat ?date` — returns empty results instead of the correct aggregate. The outer query completes successfully and produces a result set, but every row bound from the subquery is empty/zero. There is no error: the failure is silent and looks like a clean result.

Single-input subqueries (`:in $ ?x`) work correctly; the gap is specifically multiple inputs.

## Reproducer

`datalog/executor/executor_subquery_comprehensive_test.go::TestSubqueryWithTwoInputs` already encodes this case. It is currently `t.Skip`-ed; remove the skip and it fails:

```
executor_subquery_comprehensive_test.go:283: Expected Electronics total 300, got 0
executor_subquery_comprehensive_test.go:288: Expected Books total 50, got 0
```

The query under test (outer pattern + correlated `sum` subquery with two inputs):

```clojure
[:find ?category ?total
 :where
 [?p :product/category ?category]
 [(q [:find (sum ?amount)
      :in $ ?cat ?date
      :where [?prod :product/category ?cat]
             [?s :sale/product ?prod]
             [?s :sale/date ?date]
             [?s :sale/amount ?amount]]
     $ ?category "2025-01-01T00:00:00Z") [[?total]]]]
```

Here the subquery is invoked with two inputs after `$`: the correlated `?category` and the constant date. Expected per-category totals for that date are Electronics = 300 (sale:1 100 + sale:3 200) and Books = 50 (sale:4); the query returns 0 for both.

## Expected Behavior

The subquery should bind all of its declared inputs — the correlated variable(s) from the outer relation and any constants supplied at the call site — and compute the aggregate per outer tuple, exactly as the single-input case does.

## Actual Behavior

Only one input is bound (or the input binding collapses), so the subquery's `:where` matches nothing and the aggregate is empty/zero. No error is raised.

## Where To Investigate

The subquery call site supplies inputs positionally after the nested query EDN (`... ] $ ?category "..."`). The per-tuple input binding for subquery/iteration execution lives in the executor's RelationInput / per-tuple input path (`prepareIteration` / `perTupleInputSession` / `BindQueryInputs` and the subquery dispatch around `executeRealizedWithRelationInputIteration`). The likely fault is that the binding path handles a single correlated input and drops or mis-aligns the second, so the nested `:where` is evaluated with an unbound or wrong value. The related fix `BUG_SCALAR_PLUS_RELATION_INPUT_DROPS_OTHER_INPUTS` (resolved) shows the positional-forwarding shape that multiple subquery inputs must also satisfy.

## Related (2026-07-20)

The optimizer mode matrix migration surfaced the algebra-path side of this family: subqueries whose second `:in` input is a *variable* worked on the baseline path but failed loudly under the algebra optimizer (`cannot resolve right term`). That side is RESOLVED (2026-07-20): the decorrelation rewrite now classifies correlation parameters and preserves the call site's argument list through the compile→decompile round trip — see the resolution in `resolved/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md` (reproducers `TestSubqueryDatomicCompatible/MultipleInputsWithDatabase` and `TestSubqueryMultiValueFindClauseBug`, both green both modes). This entry's silent-empty applies to call-site *constants* on the baseline path and remains open. Distinct defects, same input-forwarding territory; root-cause work here should read that resolution first.

## Status of the Skip

Per the project rule against using `t.Skip` to hide unimplemented features, the skip in `TestSubqueryWithTwoInputs` is retained ONLY as a tracked pointer to this document (its message references this file) until the gap is fixed, at which point the skip is removed and the test asserts the expected totals.
