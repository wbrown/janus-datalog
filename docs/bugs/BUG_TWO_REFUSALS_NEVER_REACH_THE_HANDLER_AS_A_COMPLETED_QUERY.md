# A refusal before the query is resolved emits no event, and the realized plan's refusal emits an invocation and no completion

**Status**: Open.

The engine does not deliver a stated contract on two routes. The
`QueryInvoked` doc (`datalog/annotations/types.go`) says that a query the
database refuses before the executor's entry, once it is resolved, is
reported invoked and completed. Two refusals fall outside that sentence. A
handler counting completions against invocations sees neither query end.

## What a handler sees

The cases run on every backend and in both optimizer modes.

- `Query`, `QueryInto`, `QueryOneInto` and an `AsOf` handle's `Query` given
  text that does not parse, and `Query` given a value that is neither text
  nor a `*query.Query`: no `QueryInvoked` and no `QueryComplete`. The error
  is `failed to resolve query: …`.
- `Query`, `QueryInto` and a `History` handle's `Query` given
  `[:find ?x :where [?e :t/n 1]]`, which the realized plan's validation
  refuses as `invalid realized plan: phase 1 find symbol ?x is unavailable`:
  one `QueryInvoked` and no `QueryComplete`.

## The mechanism

`Database.queryUnderPlannerOptions` (`datalog/storage`) calls
`resolveQuery` first. It returns that error before the `refused` closure
that emits the invocation and the completion exists. So a query that never
resolves is never reported.

`Executor.ExecuteWithRelations` (`datalog/executor`) emits `QueryInvoked`,
prepares, counts the inputs, plans and emits `QueryPlanCreated`. Each
refusal on that way completes the query through its own `refused`. The
entry then returns `ExecuteRealized`'s result directly. The realized plan's
validation runs inside `ExecuteRealized`, so its error returns through the
entry with no completion.

## The fix

The database's entry reports a query it cannot resolve the way it reports a
refusal after resolution. It emits `QueryInvoked` carrying the value as
received, rendered as text where it is text, and a completion carrying the
error. The realized plan's validation moves to the entry, beside its other
refusals, before the plan is cached and before `QueryPlanCreated`. It
completes the query through `refused`. The `QueryInvoked` doc then drops
"once it is resolved".

## Tests

A test for the fix expects each route above to produce one `QueryInvoked`
and one `QueryComplete` carrying the error, on every backend and in both
modes. A query that runs still produces one invocation.
