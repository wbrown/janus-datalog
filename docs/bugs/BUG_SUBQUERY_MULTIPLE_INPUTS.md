# BUG: Subqueries With Multiple Inputs Silently Return Empty Results

**Date**: 2026-05-31 **Severity**: Medium — wrong results (silent empty), not a crash; single-input subqueries are unaffected **Status**: Open **Affected**: subquery execution for `:in` clauses with more than one input beyond `$`

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
