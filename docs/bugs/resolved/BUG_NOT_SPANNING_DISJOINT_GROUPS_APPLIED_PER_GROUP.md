# BUG: a NOT whose variables span disjoint relation groups anti-joins each group separately

**Status**: Open (2026-07-22). Found while fixing `BUG_ORJOIN_IN_BOUND_CORRELATE_TREATED_AS_BRANCH_LOCAL` — the or-join branch-visibility fix delivered a WHERE-bound external into branch evaluation correctly, and the branch's own NOT execution then mishandled it. The defect is engine-wide, not or-specific.

## Mechanism

`executeNotClause` (`datalog/executor/query_executor.go`) computes the anti-join keys as the body's free variables, then applies `filterWithNotClause` to **each relation group independently**. When the body's variables span two disjoint groups — e.g. `groups = [{?outer}, {?e}]` with body `[?e :entity/flag ?outer]` — both applications are wrong, in different directions:

- The `{?outer}` group anti-joins on `?outer` alone: the body runs with `?outer` bound and `?e` free, matches existentially, and **wipes the outer binding itself** (the probe row is excluded because *some* entity carries the flag).
- The `{?e}` group anti-joins on `?e` alone: the body runs with `?outer` free, so the NOT turns **existential** — "has any flag" instead of "has the outer-bound flag" — over-excluding.

The correct execution joins the spanning groups first (a bounded product when one side is a single-tuple probe — the same product `findOuterRelation` already performs for or-branch outer selection), anti-joins once on the full key set, and replaces the spanning groups with the filtered join. This is the NOT-analog of the theta-join added for bridging predicates in `599266f`, expressed relationally.

## Shapes

Top-level (no or-forms involved):

```clojure
[:find ?e
 :where
 [_ :probe/flag ?outer]
 [?e :entity/kind "thing"]
 (not [?e :entity/flag ?outer])]
```

when the planner leaves `{?outer}` and `{?e}` in separate groups at the NOT's execution point.

Inside an or-join branch (pinned red, in-tree): `TestOrJoinBranchNotConsumesWhereBoundExternal` in `datalog/storage/orjoin_in_bound_correlate_test.go` — the branch input `{?outer}` and the branch's pattern group `{?e}` are the spanning pair; the branch returns empty (the wipe) instead of the correlated complement. In algebra mode the same shape fails loudly instead: the bridge lowers the bare `not` to a `not-join` whose header demands both variables, and `filterWithNotJoinClause` errors `"NOT-JOIN variable ... not found in input relation"`.

## Scope ruling

Ruled split from the or-join environment/visibility fix (2026-07-22): this defect gets its own arc with top-level reproducers; the in-tree or-join reproducer stays red and references this doc. The `clausesNeedCorrelation` taxonomy fix (NOT forms are correlated forms) landed with the or-join work — it routes plain-or NOT branches to correlated execution, which resolves the inference-form leg; the spanning-groups anti-join is what remains.
