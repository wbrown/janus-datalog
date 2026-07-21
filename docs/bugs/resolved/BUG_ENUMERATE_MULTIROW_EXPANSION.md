# Bug: Enumerate Multi-Tuple Expansion Not Handled by Expression Evaluator

**Status:** Fixed **Fix:** `datalog/executor/helpers.go` — added `[][]interface{}` handler before existing `[]interface{}` path **Tests:** `TestVectorEnumerateQuery`, `TestVectorEnumerateMultipleEntities`, `TestVectorEnumerateRefWithFilter` in `datalog/storage/crdt_vector_test.go`

## Summary

`evaluateExpressionWithLookup` could not handle the `[][]interface{}` return type from `EnumerateFunction.Eval`. The existing tuple binding code expected `[]interface{}` (a single tuple), but enumerate returns `[][]interface{}` (multiple tuples, one per vector element). The Go type assertion `result.([]interface{})` silently fails for `[][]interface{}`, so binding variables were never populated.

## The Type Mismatch

`EnumerateFunction.Eval` (query/vector_functions.go:330-348):
```go
// Returns [][]interface{} — one [index, value] pair per vector element
result := make([][]interface{}, len(vec))
for i, v := range vec {
    result[i] = []interface{}{int64(i), v}
}
return result, nil
```

`evaluateExpressionWithLookup` (executor/helpers.go), pre-fix path:
```go
// Expects []interface{} — a single tuple
if tb, ok := expr.Binding.(query.TupleBinding); ok {
    values, ok := result.([]interface{})  // FAILS: [][]interface{} is not []interface{}
    if ok && len(values) == len(tb.Variables) {
        // Never reached for enumerate
    }
}
newTuples = append(newTuples, newTuple)  // Tuple appended with nil binding values
```

In Go, `[][]interface{}` does not satisfy a `[]interface{}` type assertion. The assertion returns `ok=false`, the binding block is skipped, and the tuple is appended with zero values in the `?idx` and `?item` symbols.

## Consequence

For a vector `["a", "b", "c"]` with binding `[(enumerate ?vec) [?idx ?item]]`:

**Expected:** 3 output tuples: `[0, "a"]`, `[1, "b"]`, `[2, "c"]` **Actual:** 1 output tuple with nil values in ?idx and ?item symbols

The query would either return wrong results (nil values passing through) or return zero tuples (if subsequent patterns tried to join on the nil values).

## Fix

Added a `[][]interface{}` type check before the existing `[]interface{}` path (helpers.go:214-246). For each sub-tuple in the multi-tuple result:

1. Creates a new output tuple (copy of the input tuple)
2. Populates binding variables from the sub-tuple
3. Appends to result — one output tuple per vector element

Handles both cases:
- **`hasAllBindings`** (binding variables already exist as symbols): overwrites existing symbol positions
- **new symbols** (binding variables are new): fills positions in the extended symbol list

This is the one-input-tuple-to-many-output-tuples expansion that enumerate requires. All other expressions remain 1:1 (one input tuple produces one output tuple).

## Related

This bug (Layer 1) was discovered and fixed before a second bug (Layer 2) in the same enumerate query path. Layer 2 is the planner reordering bug documented in [BUG_PLANNER_REORDERS_PATTERN_BEFORE_ENUMERATE.md](BUG_PLANNER_REORDERS_PATTERN_BEFORE_ENUMERATE.md). Both bugs had to be fixed for enumerate with tuple binding to work correctly in queries.
