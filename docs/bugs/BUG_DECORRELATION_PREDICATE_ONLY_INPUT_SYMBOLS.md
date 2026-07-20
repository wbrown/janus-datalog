# BUG: Decorrelated subquery errors when input symbols are consumed only by predicates — optimizer path only

**Status**: Open (2026-07-20). Found by the optimizer mode matrix migration (wave 1, executor-path batch) — the second divergence the regime has surfaced. Loud error on the optimizer path, no wrong data; the baseline path executes the same query correctly.

## Symptom

`TestParallelDecorrelationSymbolOrderBadger` (`datalog/storage/parallel_decorrelation_symbol_order_test.go`): an OHLC-shaped query with four correlated `(q ...)` subqueries whose inner queries take `?y ?m ?d` as `:in` inputs and consume them only through predicate operands (`[(= ?py ?y)]` etc.). With the algebra optimizer off, both its subtests pass. With it on, both fail:

```
Query execution failed: phase 1 failed: clause 4 (subquery) failed: nested query execution failed: group-by symbol ?y is not present in source relation
```

The test remains in-tree in its pre-migration single-mode form (it ran only the default path with explicit options predating the matrix); the migration attempt was reverted per protocol pending this fix.

## Reading of the failure (hypothesis, to be verified at fix time)

The error text is the grouped-aggregation executor refusing a group-by key its source relation does not carry. Decorrelation rewrites a correlated subquery by moving correlation variables from `:in` to `:find`, which turns the aggregate grouped on those variables — the same move whose category error produced the pure-aggregation decorrelation bug. Here the correlation variables are consumed **only by predicates** inside the inner query, so they are environment (Available), never data (Provides): no pattern binds `?y` into the inner relation, and the grouped aggregate then demands a group-by column the source relation cannot contain. The input-parameter type system says exactly this: input parameters are not attributes of the result relation. The rewrite appears to assume correlation variables are always joinable data.

Related family: the decorrelation pure-vs-grouped category error (resolved), the input-parameter semantics bugs (resolved), and the two divergences this regime has caught (`BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md`, `BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`) — all shapes where the algebra path's static analysis disagrees with the canonical scoping.

## Protocol

Reproduction is deterministic: run the test's query shape with `EnableAlgebraOptimizer: true`. Fix direction needs an owner ruling once root-caused (candidates: decorrelation must skip subqueries whose correlation variables are predicate-only in the inner body, or the rewrite must materialize the correlation column into the inner relation before grouping). When fixed, the test migrates onto the optimizer mode matrix like its batch siblings and the both-modes pin is the regression guard.
