# The aggregate decorrelation rebuilds a subquery call with `$` as its only source, and refuses call shapes the engine answers with the optimizer off

**Status**: Open.

The algebra optimizer's decorrelation of an aggregating subquery has two
defects. With the optimizer off, every query below answers correctly. With
it on, a call that passes a named source reads the wrong source. Also with
it on, two call shapes are refused at planning: a scalar beside a
collection, tuple or relation entry in the inner `:in`, and one outer
variable passed at two scalar positions.

## The wrong source

`DecorrelationPass` (`datalog/algebra`) compiles the decorrelated inner
query into an `Aggregate` node. That node carries no call input.
`decompileAggregate` rebuilds the call through `subqueryInputs`, which
declares and passes `$` alone. So an inner pattern on a named source reads a
source the rebuilt call never passes. An inner pattern on `$` reads the
outer `$`, whatever the call passed. The non-aggregating route keeps the
call's own sources, and those calls answer correctly.

The cases below run with the optimizer on, on every backend. The database
`$` holds `p1 :p/n 1` and the database `$other` holds `p1 :p/n 10`.

- Inner `:in $t`, with the call passing `$other` or `$`: refused with
  `phase 1 failed: clause 1 (pattern) failed: unknown source: $t`.
- Inner `:in $`, with the call passing `$other`: answers `(p1 1)`. The
  correct answer is `(p1 10)`. The same happens under an `or-default-join`
  with a zero default, and inside an `or-join` branch.
- Grouped, `[:find ?n (sum ?v)]` over `$other`: answers `(p1 "home" 1)`.
  The correct answer is `(p1 "away" 10)`.
- An aggregating call inside a filtering call, with the inner `:in $u`
  given `$t`: refused with `unknown source: $u`.
- Inside a `not-join` keyed on the sum: `p1` is dropped. The correct answer
  keeps it.

## The refused shapes

The compilation makes one correlation variable per variable argument.
`mapCorrelationToInnerParams` maps scalar inputs only. So the rebuilt
aggregate's binding disagrees with its group keys and functions.

- A scalar beside a collection entry (`:in $ ?n [?n ...]` or
  `:in $ ?n [?m ...]`), beside a tuple entry, or beside a relation entry:
  refused with `algebra optimize: refresh Aggregate: aggregate binding
  arity 3 does not match 1 group keys and 1 functions`. The tuple entry
  reports arity 4.
- One outer variable at two scalar positions (`:in $ ?n ?n`, or
  `:in $ ?n ?m` called with `$ ?p ?p`): refused with `refresh algebra
  schemas: Join child 1: duplicate output symbol ?p`.

## The fix

The decorrelated aggregate carries the call's sources. The rebuilt call then
passes what the written call passed, and the inner query reads them under
its own `:in` names, as the non-aggregating route does. The correlation
covers every binding entry of the inner `:in`. A collection, tuple or
relation entry contributes its symbols as a group. An outer variable passed
at two positions is one correlation variable. The rebuilt aggregate's
binding then agrees with its keys.

## Tests

Each query above answers the same tuples with the optimizer on as with it
off, on every backend. The refused shapes answer.
