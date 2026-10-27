# `QueryComplete` fires for a refusal alone, while its doc and its renderer describe the completion of a query that runs

**Status**: Resolved.

A query's result was unaffected. The defect was a stated contract the engine
did not deliver. A handler was promised one completion per query, with its
success and its counts, and saw one only for the queries that never ran.

## What is stated

`KeySuccess`'s doc (`datalog/annotations/types.go`) says `QueryComplete`
fires either way, carrying whether the query succeeded. The formatter's
success arm (`datalog/annotations/output.go`) renders a completion with its
relation and tuple counts.

## What was emitted

`QueryComplete` was emitted by the refusal path alone. The executor's entry
completed a query it refused with the error. The database's `Query` path did
the same for a refusal it raised before the executor's entry. A query that
failed in execution, after planning, returned its error with no completion.
A query that ran emitted an invocation and no completion. So a handler
counting completions against invocations found every running query
unfinished, and `Analyze` had no event to attribute a running query's
duration to.

## Why no completion fired

`Query` returns a streaming relation, so at the moment `Query` returns, the
tuple count the formatter renders is unknown. The tuples are counted as the
result is drained, and the session-bound result releases its read session
when it is exhausted or closed, after `Query` has returned.

## The fix

A query completes once, where it ends.

The session-bound result (`datalog/storage/session_bound_relation.go`)
carries the query's completion: the handler, the moment the query began on
the database's path, and whether the completion has fired, one shared by
the result and every rename of it. Its iterator counts the tuples it yields,
and at exhaustion or close, whichever comes first, it reports the query
complete with that count under `KeyTuplesCount`, its one result relation
under `KeyRelationsCount`, and `KeySuccess` true, then releases the session.
A drain that ends in a read fault completes with `KeySuccess` false and the
fault under `KeyError`, with the tuples drained before it. A result
exhausted and then closed completes once. The database's `Query` path
builds the completion when a handler is registered and hands it to the
result; a query with no handler carries none.

The executor's entry (`ExecuteWithRelations`, `datalog/executor/executor.go`)
completes a query that fails in execution as it completes one it refuses:
through `failed`, with `KeySuccess` false and the error under `KeyError`,
before its caller gets a result.

`Analyze` drains the result on `Query`'s own path and collects every event,
so it collects the completion with the query's duration.

The alternative, restating the doc and the formatter to the refusal alone,
was ruled out because it deletes a stated contract its renderer already
implements, and leaves `Analyze` with no completion for the queries it
measures.

## Tests

`TestQuery_ACompletionReachesTheHandlerWhereTheQueryEnds`
(`datalog/storage/query_completion_events_test.go`), over every backend and
planner mode: a handler over a query that runs sees one `QueryInvoked` and
one `QueryComplete` with `KeySuccess` true and the drained count, at the
result's exhaustion and once more not at the close that follows it; over a
result closed early, the completion at the close with the count drained so
far; over a renamed result, one completion; over a closure cancelled from
its first iteration, one completion with `KeySuccess` false carrying the
failure; in what `Analyze` collects, the one completion with the drained
count; and over a drain that ends in an injected read fault, one completion
with `KeySuccess` false, the fault under `KeyError`, and the tuples drained
before it.

`TestQuery_ARefusalBeforeTheExecutorsEntryReachesTheHandler`
(`datalog/storage/query_refusal_events_test.go`) pins the refusal
completions the fix leaves as they were.
