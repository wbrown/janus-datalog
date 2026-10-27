# A refusal before the query was resolved emitted no event, and the realized plan's refusal emitted an invocation and no completion

**Status**: Resolved.

The engine did not deliver a stated contract on two routes. The
`QueryInvoked` doc (`datalog/annotations/types.go`) said that a query the
database refuses before the executor's entry, once it is resolved, is
reported invoked and completed. Two refusals fell outside that sentence. A
handler counting completions against invocations saw neither query end.

## What a handler saw

The cases ran on every backend and in both optimizer modes.

- `Query`, `QueryInto`, `QueryOneInto` and an `AsOf` handle's `Query` given
  text that did not parse, and `Query` given a value that was neither text
  nor a `*query.Query`: no `QueryInvoked` and no `QueryComplete`. The error
  was `failed to resolve query: …`.
- `Query`, `QueryInto` and a `History` handle's `Query` given
  `[:find ?x :where [?e :t/n 1]]`, which the realized plan's validation
  refuses as `invalid realized plan: phase 1 find symbol ?x is unavailable`:
  one `QueryInvoked` and no `QueryComplete`.

## The mechanism

`Database.queryUnderPlannerOptions` (`datalog/storage`) called
`resolveQuery` first. It returned that error before the `refused` closure
that emits the invocation and the completion existed. So a query that never
resolved was never reported. `QueryInto` and `QueryOneInto` resolved the
input themselves before calling `Query`, so their resolve failure returned
before the entry was reached at all.

`Executor.ExecuteWithRelations` (`datalog/executor`) emitted `QueryInvoked`,
prepared, counted the inputs, planned and emitted `QueryPlanCreated`. Each
refusal on that way completed the query through its own closure. The entry
then returned `ExecuteRealized`'s result directly. The realized plan's
validation runs inside `ExecuteRealized`, so its error returned through the
entry with no completion.

## The fix

The database's entry builds its report before it resolves the query, so a
query it cannot resolve is reported as a refusal after resolution is: it
emits `QueryInvoked` carrying the value as received, text as text, and a
completion carrying the error. The entry's body returns the query it
resolved beside the result, and `QueryInto` and `QueryOneInto` take it from
there for their find symbols, so every route into the entry resolves once
and reports there.

The executor's entry completes every query that fails after it was
invoked, the realized plan's validation among them, through the closure the
record `BUG_QUERY_COMPLETE_FIRES_FOR_REFUSALS_ALONE.md` describes. The
validation runs where it ran, inside `ExecuteRealized` after
`QueryPlanCreated`, and its refusal completes the query. The `QueryInvoked`
doc states the unresolved case and no longer says "once it is resolved".

## Tests

`TestQuery_ARefusalReachesTheHandlerAsACompletedQuery`
(`datalog/storage/query_refusal_events_test.go`), over every backend and
optimizer mode: `Query` given text that does not parse, `Query` given an
integer, `QueryInto` and `QueryOneInto` given text that does not parse, and
an `AsOf` handle's `Query` given text that does not parse, each report one
`QueryInvoked` carrying the value as received and one `QueryComplete`
reporting the refusal, with the error the call returned; `Query`,
`QueryInto` and a `History` handle's `Query` given a query the realized
plan's validation refuses each report one invocation and one completion
carrying the refusal.
