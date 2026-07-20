# Bug: Per-Query Annotation Handler Mutates Shared Planner State

## Summary

Annotated query execution mutates the `QueryPlanner` stored on the `Executor` by calling `SetHandler()` before planning and resetting it with `defer SetHandler(nil)` afterward. The planner object is shared by all concurrent queries using that executor.

Two annotated queries running at the same time can overwrite each other's planner handler or clear it while another query is still planning. This creates a data race and can route algebra-bridge annotations to the wrong collector.

## Trigger

Any concurrent use of the same `Executor` with annotation handlers enabled:

```go
exec := db.NewExecutor()

go exec.ExecuteWithContext(ctxA, queryA)
go exec.ExecuteWithContext(ctxB, queryB)
```

If both contexts have collectors, both goroutines call `executor.planner.SetHandler(...)` on the same planner instance.

## Code Evidence

`ExecuteWithRelations` creates a shallow `Executor` copy, but the planner pointer is reused:

```go
executor := &Executor{
	matcher:        matcher,
	entityResolver: e.entityResolver,
	planner:        e.planner,
	options:        e.options,
	// ...
}
```

The handler is then installed on that shared planner:

```go
if collector := ctx.Collector(); collector != nil {
	executor.planner.SetHandler(collector.Handler())
	defer executor.planner.SetHandler(nil)
}
```

`ClauseBasedPlanner.SetHandler` is a plain field write:

```go
func (p *ClauseBasedPlanner) SetHandler(h annotations.Handler) {
	p.handler = h
}
```

Planning reads the same field when invoking the algebra optimizer:

```go
optimized, err := optimizeViaAlgebra(clauses, p.handler)
```

There is no synchronization and no per-query ownership of the handler.

## Impact

- Data race under concurrent annotated queries.
- Annotation events can be delivered to the wrong query collector.
- One query can clear another query's handler before the algebra bridge emits events.
- Debugging and optimization tests that rely on annotation structure can observe nondeterministic results.

This is especially risky because annotation handlers are explicitly documented as safe for parallel workers after wrapping, but the planner handler field itself is not safe.

## Expected Behavior

Per-query annotation state should be passed as an argument through planning, not stored on a shared planner object.

Two concurrent queries should be able to plan with different handlers without any shared mutable state.

## Suggested Fix

Remove `SetHandler` as mutable planner state for request-scoped annotations.

Possible approaches:

1. Add a planning context argument that carries the handler:

   ```go
   PlanQuery(ctx PlanningContext, q *query.Query)
   ```

2. Copy or clone the planner per query before installing handler state.
3. Pass the handler directly into `PlanWithBindings` / `optimizeViaAlgebra` from `ExecuteWithRelations`.

The cleanest design is to make `ClauseBasedPlanner` immutable with respect to per-query execution state.

## Tests Needed

- A race test that runs many annotated queries concurrently through one `Executor`.
- A deterministic test with two distinct handlers that asserts algebra-bridge events from query A never arrive at query B's collector.
- A test that one query's deferred `SetHandler(nil)` cannot suppress another in-flight query's planning annotations.

---

## Resolution (2026-05-25)

**Resolved.** The per-query annotation handler is now threaded through planning instead of stored on the shared planner. The mutable `handler` field and `SetHandler` are gone — from both `ClauseBasedPlanner` and the `QueryPlanner` interface — so there is no shared per-query state to race on.

- `datalog/planner/planner_clause_based.go` — removed the `handler` field; `Plan` and `PlanWithBindings` take a `handler annotations.Handler` parameter and pass it straight to `optimizeViaAlgebra`.
- `datalog/planner/interface.go` — `PlanQuery` / `PlanQueryWithBindings` gained the handler parameter; `SetHandler` was removed from the interface and the implementation.
- `datalog/executor/executor.go` — deleted the `SetHandler(...)` + `defer SetHandler(nil)` mutation; the executor computes the per-query handler from `ctx.Collector()` and passes it into the plan call.
- `datalog/storage/database.go` — the two direct plan-only paths (Explain, Analyze) pass `nil`.
- `datalog/executor/planner_handler_race_test.go` — `TestPlannerHandler_ConcurrentAnnotatedQueriesIsolated` runs 32 annotated queries concurrently through one Executor and asserts each collector sees exactly one `algebra/bridge-begin`. It fails on the pre-fix code both on the standard gate (cross-routed events) and under `-race` (data race), and passes both ways after the fix.

### Notes on this report

The report's analysis was accurate. Among its "possible approaches" we took the strongest form of #3 (pass the handler into planning) and went one step further: rather than keep `SetHandler` and merely clone the planner per query, we deleted the mutable field and `SetHandler` from the interface entirely. That achieves the report's stated goal — "make `ClauseBasedPlanner` immutable with respect to per-query execution state" — and removes the footgun instead of avoiding it on one path. A `PlanningContext` struct (approach #1) was unnecessary for a single field; a plain parameter is simpler.

One clarification: the bug was not limited to concurrency. The direct plan-only paths in `database.go` (Explain, Analyze) called `PlanQuery` without ever setting a handler, so under the old code they read whatever stale handler the last executor query had left on the shared field. Threading makes that explicit (`nil`).

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green; the new regression test fails (standard gate and `-race`) before the fix and passes after.
