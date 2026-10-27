# A get-else onto a variable the clauses before it bound was rewritten into an outer join, and the optimizer refused the query the executor answers

**Status**: Resolved.

An expression whose binding variable is already bound is a unification: the
tuple survives when the computed value equals the bound value and is dropped
otherwise, under both planner modes. A get-else is such an expression. With
the optimizer on, a get-else whose binding the clauses before it bound was
refused at planning; with it off, the query answered.

## The rewrite

`GetElseScanRewritePass` (`datalog/algebra/rewrite_getelse.go`) rewrites a
`Map` holding a get-else into a left outer join between the child relation
and a scan `[?entity :attr ?binding]`, with the default standing in for the
binding where the scan has no match. It declined a candidate whose entity
the child relation did not provide, whose entity the scope's required run
alone provided, or whose binding was a tuple form, and it did not ask
whether the child relation already provided the binding.

A get-else whose binding is its own entity variable,
`[?f :b ?w] [(get-else $ ?f :b :k) ?f]`, gave the scan the output
`[?f ?f]`, which `RefreshSchemas` refused as a duplicate output symbol, so
the query failed at planning with the optimizer on and answered with it off.
A get-else whose binding another pattern bound,
`[?f :m ?x] [(get-else $ ?f :n :k) ?x]`, was rewritten into a join whose
default stood in for a value the child already held.

## The fix

The pass declines a get-else whose binding the child relation already
provides, recording the failed precondition as it records its other
declines, and the `Map` evaluates the unification per tuple.

## Tests

`TestGetElseScanRewrite_BoundBindingSkipped`
(`datalog/algebra/rewrite_getelse_test.go`): a get-else whose binding is its
entity variable, and one whose binding another pattern binds, each stay a
`Map` through the pass, and the optimized tree analyzes.

`TestAGetElseOntoABoundVariableUnifies`
(`datalog/storage/getelse_unification_test.go`), over every backend and
optimizer mode: with an entity linked to itself, another linked to it and a
third with no link, a get-else onto the entity variable finds the
self-linked entity alone; with the same entities carrying two numbers, a
get-else onto a variable another pattern binds finds the one whose numbers
agree; and a pattern naming one variable at two positions finds the
self-linked entity alone.
