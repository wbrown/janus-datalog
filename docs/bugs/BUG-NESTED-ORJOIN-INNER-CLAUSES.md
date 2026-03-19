# Nested or-join inside AND branches: unsupported inner clause type

## Trigger

A query uses nested `or-join` inside `and` branches of an outer `or-join` to implement type-dependent relationship traversal:

```clojure
(or-join [?related ?self ?stype]
  (and [(ground :type/alpha) ?stype]
       (or-join [?related ?self]
         [?related :rel/location ?self]
         (and [?self :rel/region ?rgn]
              [?related :rel/region ?rgn])))
  (and [(ground :type/beta) ?stype]
       (or-join [?related ?self]
         (and [?self :rel/location ?loc]
              (or [?related :rel/location ?loc]
                  [(identity ?loc) ?related]))
         [?related :rel/provider ?self]))
  ...)
```

## Error

```
unsupported inner clause type: *query.OrJoinClause
```

The executor's `executeInnerClauses` function — which evaluates the clauses within `and` branches of OR fallback — handles DataPattern, Expression, Predicate, SubqueryPattern, NotClause, and NotJoinClause, but not OrClause or OrJoinClause.

## Root cause

`executeInnerClauses` was built incrementally. It started handling DataPatterns and Expressions, then NOT/NOT-JOIN were added when the algebra compiler needed them. But OR/OR-JOIN inside and branches were never encountered in the test suite until this query pattern.

The OR fallback evaluator (`OrFallbackIterator`) calls `executeInnerClauses` for each branch per outer tuple. When a branch contains `(and [(ground :type/alpha) ?stype] (or-join ...))`, the and-branch has two clauses: an Expression and an OrJoinClause. The Expression is handled; the OrJoinClause hits the default case and errors.

## Error swallowing (secondary bug)

The error from `executeInnerClauses` was being swallowed by `OrFallbackIterator.Next()`:

```go
branchResult, err = it.executor.executeInnerClauses(it.ctx, branch, execInput)
if err != nil {
    it.err = err
    it.done = true
    return false
}
```

The `Iterator` interface had no `Error()` method, so `it.err` was unreachable. Callers saw 0 results with no error — the query silently returned empty instead of failing.

## Fix (two parts)

### Part 1: Add Error() to Iterator interface

Added `Error() error` to the `Iterator` interface so iteration errors can propagate:

- **Storage-level iterators** (leaf): store errors from `Datom()`, `ScanKeysOnly()`, etc. in an `err` field. `Error()` returns the stored error.
- **Wrapper iterators** (executor): delegate `Error()` to their inner iterator. No `err` field needed — errors propagate up the chain.
- **Test mocks**: have an `err` field so tests can inject errors and verify propagation.

### Part 2: Extend executeInnerClauses

Add `*query.OrClause` and `*query.OrJoinClause` cases to `executeInnerClauses`, delegating to the existing `executeOrClause` and `executeOrJoinClause` methods.

## Reproduction test

`TestOrUnionWithExpressionBranches/nested_or_join_in_and_branches` in `datalog/storage/or_union_expressions_test.go`.
