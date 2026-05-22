# BUG: legacy subquery decorrelation reorders `:where` clauses

**Date**: 2026-04-17
**Severity**: High - semantic change in query execution; results depend on executor entrypoint
**Status**: Resolved (2026-05-22)
**Affected**: `datalog/executor/query_executor.go`, `datalog/executor/decorrelation_executor.go`, `executor.NewExecutor()`

## Summary

When `EnableSubqueryDecorrelation` is enabled and a query contains two or more
top-level subqueries, the legacy decorrelation executor stops evaluating
`:where` clauses in their original order.

Instead, it does this:

1. remove all `SubqueryPattern` clauses from `q.Where`
2. execute **all non-subquery clauses first**
3. execute **all subqueries afterward**

That is not a performance-only rewrite. It is a semantic change.

Any query where:

- subquery 1 binds a symbol
- a later pattern / expression / predicate depends on that symbol
- another subquery appears later in the query (so decorrelation is enabled)

can execute incorrectly under the decorrelation path.

This is **not** the algebra-bridge decorrelation bug. It is a separate issue in
the executor-level `executeWithDecorrelation()` path.

## Trigger

`shouldDecorrelate()` only checks the number of top-level subqueries:

```go
func shouldDecorrelate(clauses []query.Clause) bool {
    subqueryCount := 0
    for _, clause := range clauses {
        if _, ok := clause.(*query.SubqueryPattern); ok {
            subqueryCount++
        }
    }
    return subqueryCount >= 2
}
```

So any query with 2+ top-level subqueries enters the alternate execution path,
even if the clause ordering is semantically load-bearing.

## Minimal Query Shape

This shape is enough to show the problem:

```clojure
[:find ?dept-name ?count
 :where
 [(q [:find ?dept
      :in $ ?emp
      :where [?emp :employee/dept ?dept]]
    $ ?e) [[?dept]]]

 [?dept :dept/name ?dept-name]

 [(q [:find (count ?p)
      :in $ ?dept
      :where [?p :project/dept ?dept]]
    $ ?dept) [[?count]]]]
```

Expected sequential semantics:

1. first subquery binds `?dept`
2. pattern `[?dept :dept/name ?dept-name]` uses that bound value
3. second subquery also uses `?dept`

Decorrelated execution currently does:

1. execute `[?dept :dept/name ?dept-name]` first
2. run both subqueries later

So the dependent pattern executes before `?dept` exists.

Depending on the data and matcher path, that can produce:

- wrong results
- empty results
- much broader scans than intended
- accidental Cartesian-product-style behavior

## Code Evidence

### 1. The normal executor preserves clause order

From `datalog/executor/query_executor.go`:

```go
func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
    if e.options.EnableSubqueryDecorrelation && shouldDecorrelate(q.Where) {
        return e.executeWithDecorrelation(ctx, q, inputs)
    }

    for i, clause := range q.Where {
        switch c := clause.(type) {
        case *query.DataPattern:
            // ...
        case *query.Expression:
            // ...
        case query.Predicate:
            // ...
        case *query.SubqueryPattern:
            // ...
        }
    }
}
```

The baseline path walks `q.Where` in order.

### 2. `analyzeSubqueries()` splits the clause list by type

From `datalog/executor/decorrelation_executor.go`:

```go
func analyzeSubqueries(clauses []query.Clause) (
    subqueries []*query.SubqueryPattern,
    subqueryIndices []int,
    otherClauses []query.Clause,
    groups map[string]*SubqueryGroup,
) {
    for i, clause := range clauses {
        if subq, ok := clause.(*query.SubqueryPattern); ok {
            subqueryIndices = append(subqueryIndices, i)
            subqueries = append(subqueries, subq)
            // ...
        } else {
            otherClauses = append(otherClauses, clause)
        }
    }
}
```

The original clause order across the two groups is lost at this point.

### 3. `executeWithDecorrelation()` runs all non-subqueries first

From `datalog/executor/decorrelation_executor.go`:

```go
subqueries, _, otherClauses, groupMap := analyzeSubqueries(q.Where)

// Phase 1: Execute all non-subquery clauses
for i, clause := range otherClauses {
    switch c := clause.(type) {
    case *query.DataPattern:
        // ...
    case *query.Expression:
        // ...
    case query.Predicate:
        // ...
    }
}

// Phase 2: Execute subqueries
for idx, subq := range subqueries {
    // ...
}
```

This is the semantic bug. Clause order is no longer preserved.

## Existing Tests Already Hint That Order Matters

From `datalog/executor/executor_subquery_comprehensive_test.go`:

```go
// NOTE: Changed order - subquery must come before its result is used
```

That comment is correct for the normal executor: subquery output symbols have to
exist before later clauses use them.

The legacy decorrelation executor violates that same rule whenever there are
2+ subqueries.

## Why Current Tests Miss This

The existing decorrelation tests are mostly OHLC-style queries where:

- all patterns/expressions/predicates come first
- all subqueries already sit at the end of `:where`

For those shapes, moving "all non-subqueries first" happens to preserve the same
effective order.

What is missing is a test with:

1. subquery
2. dependent non-subquery clause
3. another subquery

Without that interleaving, the reordering bug stays invisible.

## Entry Point Mismatch Makes This Worse

The default differs depending on how the executor is constructed.

`executor.NewExecutor()` enables legacy subquery decorrelation by default:

```go
defaultOpts := planner.PlannerOptions{
    EnableSubqueryDecorrelation: true,
    // ...
}
```

But `storage.DefaultPlannerOptions()` disables it:

```go
func DefaultPlannerOptions() planner.PlannerOptions {
    return planner.PlannerOptions{
        EnableSubqueryDecorrelation: false,
        // ...
    }
}
```

So the same parsed query can have different semantics depending on whether the
caller uses:

- `executor.NewExecutor(...)`
- `db.NewExecutor()`
- `db.Query(...)`

This is especially dangerous because it looks like a planner/performance option,
but it actually changes correctness.

## Impact

### 1. Interleaved subquery queries can execute incorrectly

A later pattern or predicate that depends on the first subquery's output can run
before the output exists.

### 2. Results can depend on executor construction

The same query can behave differently between the storage API and direct executor
API because the default decorrelation flag differs.

### 3. Failures can look like ordinary planner bugs

Because the alternate executor still "sort of works" on OHLC-style queries, the
bug is easy to misclassify as:

- bad join ordering
- missing symbol propagation
- flaky subquery behavior

The real issue is simpler: the executor changed the clause order.

## Why This Is Not A Safe Optimization

Decorrelating or batching subqueries is only semantics-preserving if the
transformation preserves all data dependencies.

This implementation does not.

It assumes:

- all non-subquery clauses can safely run before any subquery

That is false as soon as a non-subquery clause consumes symbols produced by a
subquery.

## Possible Fix Directions

### Option 1: Preserve clause order and batch opportunistically

Keep walking `q.Where` sequentially. When a run of subqueries is reached, batch
only the subqueries that are safe to batch at that position.

This keeps performance work within the existing semantic model.

### Option 2: Only use the legacy decorrelation path on suffix-subquery shapes

Before enabling `executeWithDecorrelation()`, require that:

- every subquery is already after every non-subquery clause, and
- no non-subquery clause after the first subquery consumes subquery outputs

This is more restrictive but safe.

### Option 3: Disable the legacy decorrelation path entirely

`storage.DefaultPlannerOptions()` already treats it as redundant to the algebra
optimizer. Disabling the executor-level decorrelation path everywhere would avoid
semantic drift between entrypoints.

## Test Plan

1. `TestDecorrelationPreservesInterleavedSubqueryOrder`
   - shape: subquery -> dependent pattern -> second subquery
   - compare decorrelation on vs off

2. `TestDecorrelationPreservesSubqueryPredicateDependency`
   - shape: subquery -> predicate using its output -> second subquery

3. `TestExecutorAndDatabaseEntryPointsAgreeOnDecorrelatedQuery`
   - same query executed through `executor.NewExecutor(...)` and `db.Query(...)`
   - assert identical results

4. `TestShouldDecorrelateDoesNotEnableUnsafeInterleavedQuery`
   - prove that the current `subqueryCount >= 2` heuristic is too weak

## Short-Term Safety Recommendation

Until this is fixed, the legacy executor-level decorrelation path should not be
treated as a correctness-preserving optimization.

At minimum, direct users of `executor.NewExecutor()` should assume:

- queries with 2+ top-level subqueries may change semantics when
  `EnableSubqueryDecorrelation` is on
- especially if a non-subquery clause appears between subqueries or depends on a
  subquery-produced symbol

## Resolution (2026-05-22)

Fixed via the report's **Option 3: disable the legacy decorrelation path
entirely.** The executor-level decorrelation was redundant — relational-algebra
decorrelation (`EnableAlgebraOptimizer`, on by default) already handles the
correct, structure-aware transformation — so the buggy clause-reordering path
was removed rather than repaired.

### Changes

- `DefaultQueryExecutor.Execute()` (`query_executor.go`) — removed the
  `if e.options.EnableSubqueryDecorrelation && shouldDecorrelate(q.Where)` branch
  that dispatched to `executeWithDecorrelation()`. All queries now take the
  in-order, clause-by-clause path that walks `q.Where` sequentially.
- `decorrelation_executor.go` and `decorrelation_executor_test.go` — deleted.
  With the dispatch gone, `shouldDecorrelate()`, `analyzeSubqueries()`,
  `executeWithDecorrelation()`, and the batching machinery were unreachable dead
  code. (Note: `datalog/algebra/rewrite_decorrelate.go` has its own,
  unrelated `shouldDecorrelate(*LateralJoin)` — that is the live algebra path and
  was not touched.)
- The flags are now deprecated no-ops everywhere:
  - `executor.NewExecutor()` (`executor.go`) defaults flipped to
    `EnableSubqueryDecorrelation: false`, `EnableParallelDecorrelation: false`.
  - `storage.DefaultPlannerOptions()` (`database.go`) already had them false;
    comments updated to mark them deprecated no-ops.
  - `PlannerOptions`/`ExecutorOptions` field docs (`planner/types.go`,
    `executor/options.go`) marked deprecated.
  - CLI `-decorrelate` (`cmd/datalog/main.go`) is now a deprecated no-op; the
    `opts.EnableSubqueryDecorrelation = …` assignments were removed.

### Why this resolves the entry-point mismatch

The report's most dangerous symptom was that the same query could have different
semantics through `executor.NewExecutor()` (default on) vs. `db.Query()`
(default off). Because the flag now has no runtime effect on any path, all
entrypoints execute clauses in source order and agree.

### Test

`datalog/executor/decorrelation_clause_order_test.go`:
`TestLegacyDecorrelationPreservesInterleavedClauseDependencies` uses the report's
danger shape — subquery binds `?dept` → dependent clause `(identity ?dept)` →
second subquery — and asserts the result with the flag on equals the result with
the flag off. Since the flag is inert, this verifies the paths can no longer
diverge.

Full suite green (15 packages, 0 failures).

### Files changed

`query_executor.go`, `executor.go`, `options.go`, `planner/types.go`,
`database.go`, `cmd/datalog/main.go`; `decorrelation_executor.go` and
`decorrelation_executor_test.go` deleted; new test file added.
