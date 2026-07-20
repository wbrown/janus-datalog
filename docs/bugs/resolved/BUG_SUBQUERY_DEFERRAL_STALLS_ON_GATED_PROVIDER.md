# BUG: Subquery deferral gate stalls phasing by waiting on a gated provider

**Status**: RESOLVED (2026-07-20, same day as introduction). Found by external review at `cc7827b`; introduced with the deferral gate in the decorrelation arc. Loud planning failure ("cannot create phase") on a valid query — no wrong data.

## Symptom

A valid query composing an uncorrelated subquery whose outputs feed an expression that feeds a pattern failed to plan on both modes:

```clojure
[:find ?x ?y
 :where
 [(q [:find ?x ?z :in $ :where [?x :bar ?z]] $) [[?x ?z] ...]]
 [(+ ?z 1) ?y]
 [?x :rel ?y]]
```

The trigger composition is the decorrelated-grouped-subquery shape the gate exists to serve, so the regression could break its own target case.

## Root cause

`subqueryDependsOnPendingProvider` deferred the subquery whenever a pending non-subquery clause was `clauseReady` and provided one of its binding variables. But readiness is not selectability: the selection loop additionally applies `patternDependsOnPendingExpression`, so a DataPattern can be ready yet skipped (it depends on a pending expression). In the reproducer the pattern is ready-but-gated (it uses `?y` from the pending expression), the expression is not ready (it needs `?z` from the subquery), and the subquery deferred on the gated pattern — nothing selectable, phase empty, loud failure. The gate's doc comment asserted the progress guarantee its readiness check did not provide.

## Fix

Selectability now has a single definition, `clauseSelectable` (`datalog/planner/clause_utils.go`): every scheduling gate plus readiness. The selection loop and the subquery gate's provider scan both consume it, so the gate cannot wait on a clause the loop itself would skip — the progress guarantee holds by shared definition rather than by a parallel approximation of the loop's behavior. In the reproducer the subquery finds no selectable provider, is selected first, and unblocks the expression and pattern in turn.

## Reproducers (red-first, now green)

- `datalog/planner/subquery_scheduling_test.go` / `TestSubqueryDeferralNeverStallsOnGatedProvider` — the exact composition, asserting planning succeeds and the pattern schedules after the subquery.
- The three prior scheduling pins (correlated deferral, uncorrelated exemption, provider deferral) unchanged and green.

## Lesson

A gate that replicates the selection loop's admission criteria as a *subset* diverges exactly where the omitted criterion fires. The fix is the same single-home principle as every parallel-implementation bug this branch has closed: one predicate, two consumers.
