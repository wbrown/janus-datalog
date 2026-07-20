# BUG: Uncorrelated subquery schedules before the pending providers of its binding variables

**Status**: RESOLVED (2026-07-20). `subqueryDependsOnPendingProvider` wired into `selectPhaseClauses`, in its readiness-restricted form: a subquery defers only on pending non-subquery providers that are **currently ready** (`clauseReady`). Deferral therefore implies a selectable provider exists, so the phase always advances — deadlock is impossible by construction, and the pathological mutual-dependence shape (a provider that needs a symbol only the subquery provides) executes subquery-first with the provider unifying against its output, instead of erroring. With the executor unification fix landed (see sibling below), this entry is a plan-quality defect, not a correctness one: join order cannot change results in a sound engine, only inflate intermediates. Structural pin green; exemption pin unchanged.

## Symptom

A query containing an uncorrelated subquery whose binding variables are also provided by other clauses (typically expressions) returns cross-product rows: the subquery's relation joins the accumulated relation on only the subset of its binding variables that happen to be bound at the moment it is selected.

This is the planner half of the remaining OHLC decorrelation failures. The equality-bound decorrelation translation rewrites a correlated aggregate subquery into an uncorrelated grouped subquery whose relation binding renames the group columns to the outer names (`[[?s ?year ?month ?day ?hour ...] ...]`) precisely so they join. Scheduling that subquery before the `[(year ?t) ?year]`-family expressions joins it on `?s` alone — 7 outer hours × 10 groups instead of 7 rows.

## Root cause

`scoreClause` (`datalog/planner/clause_utils.go`) defers correlated subqueries by -1000 but exempts uncorrelated ones on the reasoning that they "execute exactly once wherever placed, so deferral cannot reduce their cost." That is true of their *cost* and false of their *join*: placement determines which binding variables are bound at selection time, and therefore the join key set. An uncorrelated subquery gets only the provides bonus (+10 per binding variable), but expressions score 10 — a grouped subquery binding N ≥ 1 variables outscores every expression that provides its join keys and is selected first.

This is the same invariant `patternDependsOnPendingExpression` already enforces for DataPatterns (high-scoring provider selected before the low-scoring expressions that provide its join keys → under-keyed join), not applied to subquery bindings.

## Reproducers

Structural pin (red independent of executor semantics — it inspects phase order, never executes):

- `datalog/planner/subquery_scheduling_test.go` / `TestUncorrelatedSubqueryDefersBehindPendingBindingProviders`

Composed with the executor defect (see sibling bug below), outcome-level:

- `datalog/executor` / `TestHourlyOHLCDecorrelation/algebra_on` (9 ≠ 3)
- `datalog/executor` / `TestParallelVsSequentialDecorrelation/algebra_on` (70 ≠ 7)
- `datalog/executor` / `TestTimeRangeOptimizationCorrectness/algebra_on` (25 ≠ 5)

## Fix

`subqueryDependsOnPendingProvider` (`datalog/planner/clause_utils.go`), wired into `selectPhaseClauses`: defer a subquery while a pending, **currently-ready**, **non-subquery** clause provides one of its unavailable binding variables. The two restrictions make deferral unable to block progress by construction — non-subquery-only prevents two grouped subqueries providing the same symbols (e.g. two translated OHLC subqueries both binding `[?s ?year ?month ?day ?hour]`) from deferring on each other, and ready-only guarantees a deferring subquery always leaves a selectable clause on the table. A provider that is not ready cannot be waited on safely (it may need a symbol only the subquery provides), so the subquery runs first and the provider unifies against its output.

The existing exemption pin (`TestUncorrelatedSubqueryKeepsDataSourceScheduling`) still holds under the gate: a binding variable nothing else provides never defers the subquery.

## Composition note

Fixing this alone makes the OHLC tests green only because it removes the under-keyed join; the executor's overwrite-instead-of-unify defect (`BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md`) would remain latent. Fixing the executor alone makes results correct but leaves the plan materializing the cross-product before filtering it back down. The two are independent defects with independent pins.

## Cross-references

- `docs/bugs/resolved/BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md` — the executor half of the composed failure.
- `docs/bugs/resolved/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md` — the decorrelation rewrite whose (correct) output first exposed this scheduling shape.
