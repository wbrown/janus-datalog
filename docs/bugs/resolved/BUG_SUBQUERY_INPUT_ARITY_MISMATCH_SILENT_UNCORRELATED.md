# Bug: Subquery Input Arity Mismatch Silently Produces Nil Relations

## Summary

Subquery input binding treats an arity mismatch as `nil` instead of an error. Callers then proceed with a nil input relation slice, which can turn a correlated subquery into an uncorrelated/global execution or produce an empty result without explaining the real problem.

The comment says "this is an error", but the function has no error return and silently returns `nil`.

## Trigger

Any `SubqueryPattern` whose `Inputs` list does not exactly match the nested query's `:in` clause after accounting for database inputs.

Examples:

- Query-builder or algebra-generated subquery omits `$`.
- A nested query declares `:in $ ?x ?y`, but the subquery call only supplies `$ ?outer-x`.
- A nested query declares no `$`, but the subquery call includes one.
- Future multi-source subqueries declare multiple database inputs and the call site supplies a different shape.

## Code Evidence

The binding code counts required inputs and returns `nil` on mismatch:

```go
func createInputRelationsFromValuesWithOptions(q *query.Query, orderedValues []interface{}, opts ExecutorOptions) []Relation {
	var relations []Relation

	expectedInputs := 0
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			expectedInputs++
		case query.ScalarInput:
			expectedInputs++
		case query.RelationInput:
			expectedInputs += len(inp.Symbols)
		case query.TupleInput:
			expectedInputs += len(inp.Symbols)
		case query.CollectionInput:
			expectedInputs++
		}
	}

	if len(orderedValues) != expectedInputs {
		return nil // Wrong number of inputs - this is an error
	}
```

Callers do not receive the error because the function cannot return one:

```go
func createInputRelationsFromPattern(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}) []Relation {
	return createInputRelationsFromPatternWithOptions(subq, outerValues, ExecutorOptions{})
}
```

The result is passed into nested execution as if it were a valid empty input set:

```go
inputRelations := createInputRelationsFromPattern(subqPlan.Subquery, inputValues)
result, err := executePhasesWithInputs(ctx, parentExec, subqPlan.NestedPlan, inputRelations)
```

## Impact

- Silent wrong results for correlated subqueries.
- A missing `$` or missing variable can produce a global aggregate instead of an error.
- Query-builder and algebra-decompiler bugs become much harder to diagnose because the executor hides the malformed input call.
- Tests can pass accidentally when the uncorrelated result has the same shape as the expected correlated result.

## Expected Behavior

Subquery input binding must fail loudly when the invocation does not match the nested query's `:in` contract.

The error should include:

- The nested query's `:in` clause.
- The supplied subquery inputs.
- Expected input count and actual input count.
- Whether database source inputs were included or missing.

## Suggested Fix

Change the input creation functions to return `([]Relation, error)`:

```go
func createInputRelationsFromPattern(...) ([]Relation, error)
func createInputRelationsFromValuesWithOptions(...) ([]Relation, error)
```

Then propagate the error through all subquery execution paths:

- Sequential materialized
- Sequential streaming
- Parallel materialized
- Parallel streaming
- QueryExecutor subquery path
- OR / OR-default subquery branches
- Componentized subquery path, if still used

Do not interpret nil as a valid relation list unless the nested query truly has no non-database inputs and the invocation shape has been validated.

## Tests Needed

- Subquery with `:in $ ?x ?y` invoked as `$ ?x` must return an error.
- Subquery with `:in $ ?x` invoked as `?x` must return an error, not run globally.
- Query-builder-generated subqueries should include `$` when the inner query declares `qb.DB`.
- Algebra-decompiled subqueries should be validated the same way as parsed EDN subqueries.
- Error messages should identify expected and actual input shapes.

---

## Resolution (2026-05-25)

**Resolved**, with a correction to the contract this report assumed.

`createInputRelationsFromValuesWithOptions`, `createInputRelationsFromPattern`,
and `createInputRelationsFromPatternWithOptions` now return `([]Relation, error)`,
and the error propagates through every subquery execution path (sequential and
parallel, materialized and streaming, OR / OR-default branches, and the
QueryExecutor path). The silent `return nil` on mismatch is gone.

### Correction: arity is validated against Datomic's defaulted :in

The report assumed *any* arity mismatch is an error, and listed "a nested query
declares no `$` but the subquery call includes one" as such a case. That is
wrong, and enforcing it broke the most common subquery form. In Datomic an
**omitted `:in` defaults to `[$]`** — it is not a zero-input query. So
`[(q [:find (max ?a) :where [?p :age ?a]] $) ...]`, whose nested query has no
`:in`, correctly receives the implicit default source; supplying `$` is right,
not an over-supply.

The fix defaults an absent `:in` to `[$]` before checking arity, then validates
exactly. This matches Datomic on both ends:

- nested `:in $ ?x ?y`, call supplies `$ ?x` → expected 3, got 2 → **error**
  (the report's real bug: a missing correlation variable that would otherwise run
  the subquery uncorrelated).
- nested with no `:in`, call supplies `$` → defaulted to expected 1, got 1 →
  **accepted** (the common form; the source binds the default database).

Datomic likewise raises on a genuine wrong-arity call and never silently runs a
correlated subquery as a global one, so behavior now agrees with it.

### Code

- `datalog/executor/subquery.go` — error returns plus the `:in`→`[$]` default;
  deleted the dead `createInputRelationsFromValues`.
- `datalog/executor/query_executor.go` —
  `createInputRelationsForSubqueryWithOptions` returns an error and its four
  callers propagate it; deleted the dead `createInputRelationsForSubquery`.

### Tests

- `datalog/executor/subquery_input_arity_test.go` — too-few inputs and a
  non-source value at the database position each return an error; the correct
  shape succeeds.
- Two OR-fallback tests in `not_or_test.go` hand-built the source as the string
  `"db"`; normalized to a real `$` symbol (the parser's representation — the
  comments in those tests already showed `$`). Their OR-default assertions are
  unchanged.

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
