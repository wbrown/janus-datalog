# Bug: Some Materialization Paths Drop Iterator Errors

## Summary

The iterator contract requires callers to check `Iterator.Error()` after `Next()` returns false. The repository has already fixed several public boundaries by using `collectTuplesInto`, which returns deferred iterator and close errors.

However, several materialization/union paths call `collectTuplesInto` and ignore its returned error, or manually iterate without checking `Error()`. These paths can turn a failed partial stream into a clean materialized relation.

## Trigger

Any storage iterator or composed relation that emits some tuples and then reports an error through `Iterator.Error()`, when consumed through one of the affected materializers.

Examples:

- Subquery result union.
- `MaterializeResult`.
- Streaming union materialized fallback.
- Table formatting/debug output.
- Legacy project/sort/materialize utilities.

## Code Evidence

`combineSubqueryResults` ignores the returned error:

```go
for _, rel := range validResults {
	collectTuplesInto(&allTuples, rel)
}

result := NewMaterializedRelation(symbols, allTuples)
return result, nil
```

`MaterializeResult` ignores the returned error:

```go
func MaterializeResult(rel Relation, symbols []query.Symbol) Relation {
	var tuples []Tuple
	collectTuplesInto(&tuples, rel)

	opts := rel.Options()
	return NewMaterializedRelationWithOptions(symbols, tuples, opts)
}
```

`StreamingUnionBuilder.unionMaterialized` ignores branch errors:

```go
for _, rel := range relations {
	collectTuplesInto(&allTuples, rel)
}

return NewMaterializedRelation(symbols, allTuples)
```

`TableFormatter.FormatRelation` also consumes without surfacing errors:

```go
var tuples []Tuple
collectTuplesInto(&tuples, rel)

symbols := rel.Symbols()
return tf.formatTable(symbols, tuples)
```

Some nearby code already demonstrates the expected pattern:

```go
err := collectTuplesInto(&tuples, r)
mat := NewMaterializedRelationWithOptions(r.symbols, tuples, r.options)
mat.err = err
```

## Impact

- Query results can be incomplete but appear successful.
- Subquery unions can drop an error from one branch and return partial data from other branches.
- Debug/table output can consume a stream and hide the reason rows are missing.
- Error handling behavior depends on which internal materialization path the planner/executor happens to choose.

This undermines the iterator error propagation fixes already documented elsewhere.

## Expected Behavior

Every path that consumes an iterator must do one of:

1. Return the error immediately.
2. Store the error on the derived `MaterializedRelation` so it is replayed at the next public boundary.
3. Explicitly document why the error is irrelevant, if such a case exists.

No path should silently discard the result of `collectTuplesInto`.

## Suggested Fix

Audit all calls to `collectTuplesInto` and all manual `Iterator()` loops.

For functions that return `(Relation, error)`, return the error directly.

For functions that only return `Relation`, attach the error to the materialized relation:

```go
err := collectTuplesInto(&tuples, rel)
mat := NewMaterializedRelationWithOptions(symbols, tuples, rel.Options())
mat.err = err
return mat
```

For formatting/debug functions, include the error in the rendered output or return an error from the formatting API.

## Tests Needed

- A failing iterator that yields one tuple and then reports `Error()`.
- Regression tests for:
  - `combineSubqueryResults`
  - `MaterializeResult`
  - `StreamingUnionBuilder.unionMaterialized`
  - `TableFormatter.FormatRelation`
- End-to-end query test where a subquery branch fails after partial output and the public query returns an error, not partial rows.
- A grep-based or static test that fails when `collectTuplesInto` is called without using its returned error, except for explicitly whitelisted debug paths.

---

## Resolution (2026-05-25)

**Resolved.** All ten paths that consumed an iterator and dropped the error now
propagate it — either by returning it (functions that already return an error) or
by carrying it onto the derived `MaterializedRelation.err` (functions that return
only a `Relation`), which is replayed at the next public boundary via
`Iterator().Error()`. No whitelist was needed: every `collectTuplesInto` call now
captures its result.

### `collectTuplesInto` sites (9)

- Attach to `mat.err`: `MaterializeResult`, `SortRelation`, `BindQueryInputs`
  (input binding), `StreamingUnionBuilder.unionMaterialized`,
  `OrFallbackRelation.Materialize`, `combineSubqueryResultsSimple`.
- Return the error: `combineSubqueryResults`, the parallel-subquery collector in
  `executor.go`.
- Formatter: `TableFormatter.FormatRelation` renders the error inline instead of
  silently showing partial rows.

### `Relation.Sorted()` (the 10th path) — interface change

`Sorted()` returned `[]Tuple` with no error channel, so it could only drop a
failed sort source. Leaving it lossy vs. fixing it is itself an architectural
choice, so it was decided explicitly (not whitelisted): **the interface method
became `Sorted() ([]Tuple, error)`**, updated across all implementations
(`Materialized`, `Streaming`, `Product`, `OrFallback`, `Prepended`, `Union`,
`LazySeq`, `StreamingAggregate`) and all callers (the storage matchers in
`hash_join_matcher.go` / `matcher_relations.go` and `indexed_memory_matcher.go`
now return the error). `MaterializedRelation.Sorted()` surfaces a carried
`r.err`; `StreamingRelation.Sorted()` returns the collect error.

### Tests

- `materialization_error_propagation_test.go` — a failing iterator
  (`newFailingStream`) through `MaterializeResult`, `SortRelation`, and
  `combineSubqueryResultsSimple` must surface the error (fails before, passes
  after).
- `collect_tuples_guard_test.go` — static guard that scans the package source and
  fails if any `collectTuplesInto` call is a bare statement (error dropped). No
  whitelist entries.

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
