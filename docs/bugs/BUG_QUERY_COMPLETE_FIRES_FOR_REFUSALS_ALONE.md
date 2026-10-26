# `QueryComplete` fires for a refusal alone, while its doc and its renderer describe the completion of a query that runs

**Status**: Open.

A query's result is unaffected. The defect is a stated contract the engine
does not deliver. A handler is promised one completion per query, with its
success and its counts, and sees one only for the queries that never ran.

## What is stated

`KeySuccess`'s doc (`datalog/annotations/types.go`) says `QueryComplete`
fires either way, carrying whether the query succeeded. The formatter's
success arm (`datalog/annotations/output.go`) renders a completion with its
relation and tuple counts.

## What is emitted

`QueryComplete` is emitted by the refusal path alone. The executor's entry
completes a query it refuses with the error. The database's `Query` path
does the same for a refusal it raises before the executor's entry. A query
that runs emits an invocation and no completion. So a handler counting
completions against invocations finds every running query unfinished, and
`Analyze` has no event to attribute a running query's duration to.

## Why no completion fires today

`Query` returns a streaming relation, so at the moment `Query` returns, the
tuple count the formatter renders is unknown. The tuples are counted as the
result is drained, and the session-bound result releases its read session
when it is exhausted or closed, after `Query` has returned.

## The fix

The completion of a running query fires where the query ends, at the
session-bound result's exhaustion or close, with `success` true and the
tuples drained. A handler then sees one invocation and one completion per
query, and `Analyze` attributes a running query's duration to an event.

The alternative is to restate the doc and the formatter to the refusal
alone. It is ruled out because it deletes a stated contract its renderer
already implements, and leaves `Analyze` with no completion for the queries
it measures.

## Tests

A test for the fix expects a handler over a query that runs to its end to
see one `QueryInvoked` and one `QueryComplete` with `success` true and the
drained count. Over a query closed early, it expects the completion at the
close, with the count drained so far.
