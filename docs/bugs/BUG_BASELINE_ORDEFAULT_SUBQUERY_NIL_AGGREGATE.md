# BUG: or-default aggregate subquery yields nil sums for get-else-defaulted absent attributes — baseline path only

**Status**: Open (2026-07-20). Found by the optimizer mode matrix migration (OR-family batch) — the third divergence the regime has surfaced, and the first in the inverted direction: the **baseline** path fails while the optimizer path executes the same query correctly. This is precisely the "optimizer makes things work" condition the matrix mandate exists to catch. Loud error, no wrong data.

## Symptom

`TestNotClauseComplexQuery_E2E` (`datalog/storage/not_clause_regression_test.go`) — the GitHub #58 reproducer: a query with NOT, six get-else expressions, three `or-default` clauses whose primary branches are correlated aggregate subqueries, a comparison binding, and order-by. With the algebra optimizer on, it passes. With it off:

```
query execution failed: phase 1 failed: clause 13 (not) failed: NOT input combinations failed: binding form application failed: subquery result contains nil value at position 3 - this violates datalog semantics
```

Reproduction is deterministic: `go test ./datalog/storage -run 'TestNotClauseComplexQuery_E2E/algebra_off'`.

The test is migrated onto the optimizer mode matrix and its `algebra_off` leg stands red in-tree pending the fix — the divergence is visible, not curated away.

## Reading of the failure (hypothesis, to be verified at fix time)

The `(not)` in the error is not the fault site. The baseline planner schedules the source `(not [?project :project/deleted true])` late in the phase; consuming that NOT's input relation forces the lazy `OrFallbackRelation` chain, which applies the item-stats subquery binding form — the error originates there and propagates out through the consuming clause's label.

The failing binding form is the first or-default's primary branch:

```clojure
[(q [:find (count ?i) (sum ?cost) (sum ?weight) (sum ?vol) (sum ?units)
     :in $ ?p
     :where [?i :item/project ?p]
            [?i :item/status :status/done]
            (not [?i :item/deleted true])
            [(get-else $ ?i :item/cost 0) ?cost]
            [(get-else $ ?i :item/weight 0) ?weight]
            [(get-else $ ?i :item/volume 0) ?vol]
            [(get-else $ ?i :item/units 0) ?units]]
    $ ?project) [[?itemCount ?totalCost ?totalWeight ?totalVolume ?totalUnits]]]
```

Position 3 is `(sum ?vol)`. The annotation stream (`or-fallback/outer.tuple`) shows the failing combination is the project that *has* matching items, and the nil check fires at position 3, not position 1 — so `(count ?i)`, `(sum ?cost)`, `(sum ?weight)` produced real values while `(sum ?vol)` produced nil. Rows flowed into the aggregate; only the sums whose inputs come from get-else over **universally absent** attributes (`:item/volume` and `:item/units` have no datoms for any entity in the fixture) come back nil. Cost and weight — attributes that exist on the contributing items — aggregate correctly through the identical get-else shape.

Hypothesis: on the baseline path's per-combination subquery execution (`subquery/executor-path: Per-combination QueryExecutor`), get-else over an attribute that is absent fails to bind the default into the rows feeding the aggregate — the aggregate then sums an empty or nil-bound column and emits nil. The optimizer path executes the same subquery through the decorrelated/global route and applies the default correctly. Whether the defect lives in the baseline get-else evaluation, in how per-combination execution phases the get-else expressions relative to aggregation, or in the aggregate's handling of the resulting column, is the root-cause question.

Related family: the or-default fallback machinery and per-combination subquery execution (this is the executor's own baseline route, not an optimizer rewrite), and the matrix's two prior catches (`BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md`, `BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md`) — both of those failed on the algebra path; this one is the baseline path rotting behind the optimizer, which the plan of record (`docs/wip/OPTIMIZER_MODE_MATRIX.md`) names as the highest-value catch class.

## Protocol

Root-cause and fix direction need an owner ruling. When fixed, the red `algebra_off` leg of `TestNotClauseComplexQuery_E2E` is the regression guard; no assertions were weakened and the query is byte-identical to its pre-migration form.
