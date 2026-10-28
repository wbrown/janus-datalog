# The aggregate decorrelation rebuilt a subquery call with `$` as its only source, and refused call shapes the engine answers with the optimizer off

**Status**: Resolved.

The algebra optimizer's decorrelation of an aggregating subquery had two
defects. With the optimizer off, every query below answered correctly. With
it on, a call that passed a named source read the wrong source. Also with
it on, two call shapes were refused at planning: a scalar beside a
collection, tuple or relation entry in the inner `:in`, and one outer
variable passed at two scalar positions.

## The wrong source

`DecorrelationPass` (`datalog/algebra`) compiles the decorrelated inner
query into an `Aggregate` node. That node carried no call input.
`decompileAggregate` rebuilt the call through `subqueryInputs`, which
declares and passes `$` alone. So an inner pattern on a named source read a
source the rebuilt call never passed. An inner pattern on `$` read the
outer `$`, whatever the call passed. The non-aggregating route keeps the
call's own sources, and those calls answered correctly.

The cases below ran with the optimizer on, on every backend. The database
`$` holds `p1 :p/n 1` and the database `$other` holds `p1 :p/n 10`.

- Inner `:in $t`, with the call passing `$other` or `$`: refused with
  `phase 1 failed: clause 1 (pattern) failed: unknown source: $t`.
- Inner `:in $`, with the call passing `$other`: answered `(p1 1)`. The
  correct answer is `(p1 10)`. The same happened under an `or-default-join`
  with a zero default, and inside an `or-join` branch.
- Grouped, `[:find ?n (sum ?v)]` over `$other`: answered `(p1 "home" 1)`.
  The correct answer is `(p1 "away" 10)`.
- An aggregating call inside a filtering call, with the inner `:in $u`
  given `$t`: refused with `unknown source: $u`.
- Inside a `not-join` keyed on the sum: `p1` was dropped. The correct answer
  keeps it.

## The refused shapes

The compilation made one correlation variable per variable argument.
`mapCorrelationToInnerParams` mapped scalar inputs only. So the rebuilt
aggregate's binding disagreed with its group keys and functions.

- A scalar beside a collection entry (`:in $ ?n [?n ...]` or
  `:in $ ?n [?m ...]`), beside a tuple entry, or beside a relation entry:
  refused with `algebra optimize: refresh Aggregate: aggregate binding
  arity 3 does not match 1 group keys and 1 functions`. The tuple entry
  reported arity 4.
- One outer variable at two scalar positions (`:in $ ?n ?n`, or
  `:in $ ?n ?m` called with `$ ?p ?p`): refused with `refresh algebra
  schemas: Join child 1: duplicate output symbol ?p`.

## The fix

The `Aggregate` node carries the decorrelated inner query's `:in`, the
sources and the rule set the written inner declared, and the call's source
and rule-set arguments, read by `callSourceArguments`, which the
non-aggregating route reads its own from. `decompileAggregate` rebuilds the
call declaring that `:in` and passing those arguments, so the inner query
reads the sources the written call passed, under its own names.

`compileSubquery` collects each outer variable the call passes once, so an
outer variable passed at two positions is one correlation variable.
`correlationParameters` pairs each inner `:in` symbol the call binds with
the outer variable at its call position, as the executor's frame reads the
call: a scalar entry's symbol and each symbol of a tuple or relation entry
is a correlation parameter. It declines the shapes with no grouped form, a
collection entry, a constant at a binding position, and an inner symbol
bound at two positions by different outer variables, and the subquery keeps
per-combination execution. `decorrelateQuery` keeps the sources and the
rule set alone in `:in`, prepends one group symbol per outer variable, and
equates a later parameter's group symbol with the first's through a
predicate in `:where`, so the rebuilt aggregate's binding agrees with its
keys.

## Tests

`TestAnAggregatingSubqueryReadsTheSourcesItsCallPasses`
(`datalog/storage/aggregate_decorrelation_test.go`), over every backend and
planner mode: each query of the first list answers the tuples named beside
it.

`TestAnAggregatingSubqueryAnswersUnderEveryShapeOfItsIn` (the same file),
over every backend and planner mode: a scalar beside a collection entry
over another symbol and over its own, beside a tuple entry and beside a
relation entry, and one outer variable at two scalar positions the inner
names apart and alike, each answer every entity the sum its arguments
select.
