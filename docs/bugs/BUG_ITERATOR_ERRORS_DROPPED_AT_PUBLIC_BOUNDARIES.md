# BUG: Iterator Errors Dropped at Public Result Boundaries

**Date**: 2026-05-24
**Severity**: High - storage/decode/subquery failures can become successful partial results
**Status**: Open (code review finding; needs dedicated repro tests)
**Affected**: `executor.CollectTuples`, `storage.Database.QueryInto`, `storage.Database.QueryOneInto`, tuple collection paths that cannot return errors

## Summary

The executor `Iterator` contract says callers must check `Error()` after `Next()`
returns false. Several public result-consumption APIs iterate relations and return
success without checking `Iterator.Error()`.

That means an iterator-level failure can be misreported as:

1. an empty successful result,
2. a truncated successful result, or
3. `found=false, nil` from `QueryOneInto`.

This is especially risky because the storage layer intentionally moved some
failures into deferred iterator errors. `KeyOnlyIterator.Next()` returns false on
decode failure and preserves the error for `Error()`. `CRDTResolvingIterator`
does the same for source decode errors and unique-walk failures.

## Root Cause

The iterator contract is explicit:

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

But result collection paths do not consistently enforce that contract.

### `executor.CollectTuples`

```go
func CollectTuples(rel Relation, err error) ([][]interface{}, error) {
    if err != nil {
        return nil, err
    }
    if rel == nil {
        return [][]interface{}{}, nil
    }
    var tuples [][]interface{}
    it := rel.Iterator()
    defer it.Close()
    for it.Next() {
        src := it.Tuple()
        t := make([]interface{}, len(src))
        copy(t, src)
        tuples = append(tuples, t)
    }
    if tuples == nil {
        tuples = [][]interface{}{}
    }
    return tuples, nil
}
```

There is no `if err := it.Error(); err != nil { return nil, err }` after the
loop. A failing iterator is indistinguishable from a normally exhausted iterator.

### `storage.Database.QueryInto`

```go
iter := rel.Iterator()
defer iter.Close()

// ...
for iter.Next() {
    elem := reflect.New(elemType).Elem()
    if err := mapper.MapTuple(iter.Tuple(), elem); err != nil {
        return err
    }
    newSlice = reflect.Append(newSlice, elem)
}
sliceVal.Set(newSlice)
return nil
```

Both the struct-mapping path and scalar path return nil after the loop without
checking `iter.Error()`.

### `storage.Database.QueryOneInto`

```go
iter := rel.Iterator()
defer iter.Close()

if !iter.Next() {
    return false, nil
}
firstTuple := make([]interface{}, len(iter.Tuple()))
copy(firstTuple, iter.Tuple())

if iter.Next() {
    return false, dlreflect.ErrMultipleResults
}
```

If the first `Next()` returns false due to an iterator failure, the API reports
"not found" with no error. If the second `Next()` fails, the method proceeds as if
there was exactly one result.

## Why This Matters

The storage and executor layers now have several iterators where failures are
only visible through `Error()`:

- `KeyOnlyIterator` decodes datoms in `Next()` and stores decode/blob lookup
  errors in `currentError`
- `CRDTResolvingIterator` records source `Datom()` errors and unique-walk errors
  in `err`
- `UnionIterator` can hold errors from parallel subquery workers
- wrapper iterators generally propagate inner errors through their own `Error()`

The repository already has tests for the storage iterator error contract, but the
public consumption APIs do not consistently participate in that contract.

## Expected Behavior

Any public API that consumes a `Relation` should return iterator errors to the
caller.

Examples:

- `executor.CollectTuples(...)` should return the iterator error
- `QueryInto(...)` should return the iterator error and not silently install a
  partial destination slice
- `QueryOneInto(...)` should return the iterator error instead of `found=false`
  or a partial success

## Actual Behavior

Several public APIs return success after `Next()` stops, regardless of whether
iteration stopped normally or due to failure.

## Failure Modes

### Failure Mode 1: Blob Decode Failure Becomes Empty Result

If a Tier 3 hashed value points at a missing/corrupt blob, `DatomFromKey` returns
an error. Through `KeyOnlyIterator`, that can surface as `Next() == false` plus a
non-nil `Error()`.

`CollectTuples` currently reports `[][]interface{}{}` and `nil`.

### Failure Mode 2: `QueryOneInto` Reports Not Found

If the first iterator step fails, `QueryOneInto` returns:

```go
return false, nil
```

That is indistinguishable from a legitimate empty query result.

### Failure Mode 3: Truncated Results

If an iterator yields some tuples and then fails, `QueryInto` can populate the
destination with the prefix and return nil.

That is worse than an empty result because the caller sees plausible data.

## Impact

### Correctness

Errors in storage decoding, blob lookup, CRDT resolution, or streaming union
execution can be silently converted into valid-looking query results.

### Debuggability

The lower layers may preserve the right error, but the public API drops it at the
last boundary. Users will debug "missing data" instead of seeing the real decode
or iterator failure.

### Trust

This bug class undermines the iterator error propagation work already present in
the storage layer. The contract is only useful if every consumer checks it.

## Fix Direction

Add explicit iterator error checks in all public and internal collection paths
that can return errors.

Immediate targets:

1. `executor.CollectTuples`
2. `storage.Database.QueryInto`
3. `storage.Database.QueryOneInto`

For internal helpers that currently cannot return errors, either:

1. introduce error-returning variants, or
2. ensure callers consume through a boundary that checks `Error()` before
   returning to the user.

Be careful with `defer iter.Close()`: `Close()` and `Error()` are separate
signals. The iterator error should be checked before returning, and close errors
should not mask a more specific iteration error unless that is a deliberate
policy.

## Verification Plan

Add regression tests with a relation whose iterator yields controlled failures:

1. `TestCollectTuples_ReturnsIteratorError`
   - relation iterator returns false with `Error() == sentinel`
   - expect `CollectTuples` returns sentinel

2. `TestQueryInto_ReturnsIteratorError`
   - execute through a fake or injected relation path if possible
   - expect destination is not silently populated as success

3. `TestQueryOneInto_ReturnsIteratorErrorOnFirstNext`
   - first `Next()` returns false due to sentinel error
   - expect `found=false, err=sentinel`

4. `TestQueryOneInto_ReturnsIteratorErrorOnSecondNext`
   - first tuple succeeds, second `Next()` fails
   - expect sentinel error, not single-result success

Also add one storage-backed test if practical:

- create a datom with a Tier 3 hashed value, delete/corrupt the blob key, query
  it, and verify the public API returns a blob/decode error.

## Related

- `datalog/storage/iterator_error_propagation_test.go` documents the storage
  iterator error contract.
- `docs/bugs/BUG_ITERATOR_LEAK_BUILTIN_EVALUATION.md` is another example where
  iterator lifecycle obligations were correct in the interface but not
  consistently observed by consumers.
