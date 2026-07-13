# BUG: Relation Transforms Drop Deferred Iterator Errors

**Date**: 2026-05-25
**Severity**: Correctness (High)
**Status**: Resolved (2026-05-25) — see Resolution at the end
**Affected**: expression evaluation, predicate filtering, projection/materialization-style loops in `executor/relation_ops.go`

## Summary

The executor has a documented iterator contract: after `Next()` returns false,
callers must check `Iterator.Error()` before treating iteration as successful.
Several relation transforms still consume an iterator, build a clean
`MaterializedRelation`, and return it without checking `Error()` or `Close()`.

This can turn a storage decode failure or other deferred iterator failure into a
successful empty or truncated relation.

## Code Evidence

The contract is explicit:

```go
type Iterator interface {
    Next() bool
    Tuple() Tuple
    Close() error

    // Error returns any error encountered during iteration.
    // Callers must check Error() after Next() returns false to
    // distinguish normal exhaustion from execution failure.
    Error() error
}
```

But `evaluateExpressionWithLookup` does not check it:

```go
iter := rel.Iterator()
defer iter.Close()
for iter.Next() {
    tuple := iter.Tuple()
    // evaluate expression and append tuples
}

opts := rel.Options()
return NewMaterializedRelationWithOptions(newSymbols, newTuples, opts)
```

`filterWithPredicateAndLookup` and `projectToSymbols` follow the same pattern:
consume the source iterator, ignore `iter.Error()`, ignore `Close()` errors, and
return a clean materialized relation.

## Why This Matters

Storage iterator failures are often deferred. For example, a Tier-3 blob value
stores a content hash in the key and the compressed bytes in the blob store. If
the blob is missing, the scan may only surface the failure through
`Iterator.Error()` after `Next()` returns false.

The codebase already has regression tests proving this class at public
boundaries. The remaining risk is inside transforms:

1. A scan yields a prefix of tuples.
2. A transform consumes the scan and never checks `Error()`.
3. The transform returns a clean `MaterializedRelation`.
4. The public boundary correctly checks the transformed relation and sees no
   error, because the error was laundered earlier.

This is especially dangerous for predicates and expressions because a decode
failure can look identical to "no tuple passed the filter."

## Expected Behavior

Every internal loop that drains a relation must preserve the iterator outcome:

- If source iteration fails, the derived relation must replay that error via
  its own `Iterator().Error()`.
- If `Close()` fails and no earlier iteration/function error occurred, that
  error should also be surfaced.
- Partial tuples from a failed stream must not be reported as clean success.

## Actual Behavior

Some transforms return `NewMaterializedRelationWithOptions(...)` directly after
iteration, without carrying any deferred source error onto the result relation.

## Suspect Sites

- `filterWithPredicateAndLookup` in `datalog/executor/relation_ops.go`
- `evaluateExpressionWithLookup` in `datalog/executor/relation_ops.go`
- `projectToSymbols` in `datalog/executor/relation_ops.go`
- Any other `for iter.Next() { ... }` loop that constructs a derived relation
  without checking `iter.Error()` and `iter.Close()`

## Fix Direction

Use the existing error-preserving iteration utilities consistently:

- Prefer `ForEach` for tuple-by-tuple transforms.
- Prefer `collectTuplesInto` when materializing whole relations.
- When returning a materialized relation after a transform, attach the first
  source iterator error to the result so public boundaries can observe it.

## Verification Plan

Add regression tests that inject a failing source relation through each transform:

- expression transform over a failing relation
- predicate transform over a failing relation
- projection over a failing relation
- storage-backed integration using a corrupted Tier-3 blob plus an expression or
  predicate in the query

Each test should fail if the transformed relation reports clean success.

---

## Resolution (2026-05-25)

**Resolved** with a wider scope than the original report described, because
the review surfaced four related defects in the same loops that the "iterator
error" framing had let slide. All from the same root pattern: silent
`continue` past errors, with comments either lying ("Log error but continue
processing" — nothing logs) or asserting a contract that the code didn't
enforce.

### What was fixed

1. **Iterator errors propagate** in the three `relation_ops.go` transforms
   (`filterWithPredicateAndLookup`, `evaluateExpressionWithLookup`,
   `projectToSymbols`). Each captures `iter.Error()` and the `iter.Close()`
   error, attaches the first non-nil to `result.err` (replayed via
   `Iterator().Error()` at the next public boundary).

2. **Predicate / expression evaluation errors propagate, not swallowed.**
   The bare `continue` on `pred.Eval` / `expr.Function.Eval` errors silently
   treated any failure as "predicate said no" / "expression produced no
   result." Now the first eval error stops the loop and surfaces via
   `result.err`. Fail-fast, matching the rest of the codebase.

3. **`iter.Close()` runs on panic.** The three transforms use named-return
   + `defer` + closure so `Close()` is called whether the loop completes,
   breaks on an eval error, or unwinds via panic from user-supplied `Eval`
   code. The Close error is still captured and folded into `result.err`.

4. **Two duplicate sites of the same eval-error bug** outside `relation_ops.go`:
   - `MaterializedRelation.EvaluateFunction` (`relation.go:~770`) — the
     materialized counterpart of expression evaluation. Same `result, err :=
     fn.Eval(bindings); if err != nil { continue }` pattern; same fix.
   - `FunctionEvaluatorIterator.Next` (`iterator_composition.go:~327`) — the
     streaming counterpart. Added an `err` field on the iterator; eval errors
     stop iteration and surface via `Error()` rather than being skipped past.

5. **Two `Project()` error-discards** that asserted a contract via comment
   instead of enforcing it (`relation.go:603`, `:1182`):
   ```go
   result, _ := r.Project(neededSymbols)
   // Ignore error as neededSymbols are derived from the pattern elements
   // which must exist if we got this far
   ```
   Both now `panic` on Project error, matching the
   `extractElementIDFromKey` / encoder-switch convention that the project
   adopted alongside the ATEV index work: silent zero/empty returns hid
   programmer-error contract violations; panic surfaces them loudly.

6. **`UnionRelation.Materialize` comment** corrected — the comment claimed
   "Errors from Close() are silently dropped (limitation of Relation
   interface)" but the code already propagates close errors via
   `mat.err = err` (the result-ride pattern). Comment now describes the
   actual behavior.

### `get-some` API change

Fail-fast on eval errors exposed that `query.GetSomeFunction.EvalWithLookup`
was using `error` as a soft signal for "no attribute matched" — the comment at
the no-match return path literally said *"return nil (will cause tuple to be
filtered out)"*, relying on the upstream `continue` that this PR removes.
This was the actual reason the swallowing loops existed.

Aligned `get-some` with the rest of the database-function family
(`get-else`, `missing?`), neither of which uses error-as-signal:

- Added `Found bool` to `query.GetSomeResult`.
- Match returns `&GetSomeResult{Attr, Value, Found: true}, nil`.
- No-match returns `&GetSomeResult{Found: false}, nil` (no error).
- All `GetSomeResult` consumers (`evaluateExpressionWithLookup`,
  `query_executor.go`, `MaterializedRelation.EvaluateFunction`,
  `FunctionEvaluatorIterator`) check `Found` and drop the tuple without
  surfacing an error.

### Why the original transforms slipped past prior sweeps

The earlier iterator-error fixes covered `collectTuplesInto` call sites
(caught by a static guard) and the SemiJoin/AntiJoin helpers. The transforms
in relation_ops, the matching EvaluateFunction sites, and the Project-error
discards are all hand-rolled `for/if` patterns that bypass `collectTuplesInto`
and aren't join helpers. Only direct regression tests surface them.

### Tests added

`datalog/executor/relation_ops_error_propagation_test.go` (9 cases total):

- `TestFilterWithPredicateAndLookup_PropagatesIteratorError` — failAfter=0
- `TestFilterWithPredicateAndLookup_PropagatesAfterPartialResults` — failAfter=1
- `TestFilterWithPredicateAndLookup_PropagatesEvalError` — predicate references
  `?missing`, `Comparison.Eval` returns "cannot resolve" for every tuple
- `TestEvaluateExpressionWithLookup_PropagatesIteratorError`
- `TestEvaluateExpressionWithLookup_PropagatesEvalError`
- `TestProjectToSymbols_PropagatesIteratorError`
- `TestProjectToSymbols_PropagatesAfterPartialResults`
- `TestMaterializedRelation_EvaluateFunction_PropagatesEvalError`
- `TestFunctionEvaluatorIterator_PropagatesEvalError`

Pre-existing tests covering `get-some` no-match semantics
(`TestGetSomeNoMatch`, `TestGetSome_WithScalarInput_NoMatch`) continue to
pass against the new `Found`-based signal.

The shared failing-iterator helpers (`failingIterator`, `newFailingStream`,
`driveErr`, `errInjectedIterator`) live in `iterator_error_boundary_test.go`.

### Files

- `datalog/executor/relation_ops.go` — three transforms rewritten with named
  return + closure, fail-fast on eval errors, deferred Close that captures
  the close error
- `datalog/executor/relation.go` — `MaterializedRelation.EvaluateFunction`
  fail-fast + result.err; two `ProjectFromPattern` Project-error discards
  replaced with panic
- `datalog/executor/iterator_composition.go` — `FunctionEvaluatorIterator`
  gains an `err` field; `Next` fails fast, `Error` surfaces it
- `datalog/executor/union_relation.go` — comment corrected
- `datalog/executor/query_executor.go` — `GetSomeResult.Found` consumer
  updated
- `datalog/query/database_function.go` — `GetSomeResult.Found` added,
  `GetSomeFunction.EvalWithLookup` no-match returns a value (not an error)
- `datalog/executor/relation_ops_error_propagation_test.go` (new) — 9 tests

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
