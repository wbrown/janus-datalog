# BUG: ProductRelation Reopens Single-Use Streaming Relations

**Date**: 2026-05-25
**Severity**: Correctness / Stability (High)
**Status**: Open
**Affected**: Queries whose `:find` symbols span disjoint streaming relation groups, and any internal path using `Relations.Product()` over streaming inputs

## Summary

`ProductRelation` implements Cartesian product by reopening inner relation
iterators as the nested loop advances. That is incompatible with the default
streaming architecture: `StreamingRelation` is explicitly single-use unless
`Materialize()` was called before first iteration.

When `QueryExecutor` decides that final projection requires a Cartesian product
of disjoint groups, it passes those groups directly to `Relations.Product()`.
If any non-leftmost group is a `StreamingRelation`, `ProductIterator.Next()`
will call `Iterator()` on it more than once and panic.

This is not just a theoretical API mismatch. The executor has a live product
path for disjoint `:find` symbols, while the relation contract says streams are
single-use by default.

## Code Evidence

The executor creates a product when `:find` symbols span multiple groups:

```go
// query_executor.go
// :find symbols span multiple groups - need Cartesian product
needsProduct = true

if needsProduct {
    combined := Relations(groups).Product()
    projected, err := combined.Project(findSymbols)
    ...
}
```

`Relations.Product()` preserves the input relations as-is:

```go
// relations.go
func (rs Relations) Product() Relation {
    if len(rs) == 0 {
        return NewMaterializedRelation(nil, nil)
    }
    if len(rs) == 1 {
        return rs[0]
    }
    return NewProductRelation(rs)
}
```

`ProductIterator` reopens exhausted inner relations:

```go
// relation.go
pi.iterators[i].Close()
pi.iterators[i] = pi.relations[i].Iterator()
if !pi.iterators[i].Next() {
    pi.done = true
    return false
}
```

But `StreamingRelation.Iterator()` panics on the second call unless caching was
enabled via `Materialize()` before iteration started:

```go
// relation.go
if r.iteratorCalled && !r.shouldCache {
    panic("StreamingRelation.Iterator() called multiple times without Materialize(). " +
        "Streaming iterators are single-use only. " +
        "Call Materialize() first if you need multiple iterations.")
}
```

## Failure Mode

Any product of two streaming relations where the right side has to rewind can
panic:

```text
left stream:  L1, L2
right stream: R1, R2

emit L1 x R1
emit L1 x R2
right exhausted
advance left to L2
reopen right iterator
panic: StreamingRelation.Iterator() called multiple times without Materialize()
```

The leftmost relation is consumed once. Every relation to its right must be
re-iterable for the nested loop to work.

## Reproduction Sketch

Use two materialized relations only as sources for streaming wrappers, then
product them:

```go
func TestProductRelation_MaterializesStreamingInputsBeforeReopen(t *testing.T) {
    opts := executor.ExecutorOptions{EnableTrueStreaming: true}

    leftMat := executor.NewMaterializedRelation(
        []query.Symbol{datalog.NewSymbol("?x")},
        []executor.Tuple{{1}, {2}},
    )
    rightMat := executor.NewMaterializedRelation(
        []query.Symbol{datalog.NewSymbol("?y")},
        []executor.Tuple{{"a"}, {"b"}},
    )

    left := executor.NewStreamingRelationWithOptions(leftMat.Symbols(), leftMat.Iterator(), opts)
    right := executor.NewStreamingRelationWithOptions(rightMat.Symbols(), rightMat.Iterator(), opts)

    product := executor.Relations{left, right}.Product()

    // Expected: four tuples, no panic.
    // Current behavior: panics when ProductIterator reopens right.
    _, err := executor.CollectTuples(product, nil)
    require.NoError(t, err)
}
```

An end-to-end query reproducer should use a query where the final `:find`
projects symbols from two disjoint groups that the executor leaves separate
until projection. That exercises the production path in `query_executor.go`,
not just the relation primitive.

## Impact

- Queries can crash the process instead of returning an error.
- The crash depends on execution shape, so it can appear only after planner or
  optimizer changes make groups remain disjoint until projection.
- The codebase documentation says Cartesian products are generally rejected,
  but the executor still has a product path. That makes this bug easy to miss.

## Fix Direction

`ProductRelation` must make every input except possibly the leftmost relation
re-iterable before iteration begins.

Possible approaches:

1. In `NewProductRelation`, call `Materialize()` on all relations after the
   first, before any iteration starts.
2. In `ProductIterator.Next()`, require right-side relations to be materialized
   and return an error if they cannot be made re-iterable safely.
3. Replace nested-loop reopening with a buffering product iterator that caches
   each right-side stream on first pass, then replays from cache.

The first option is the smallest change and matches the nested-loop requirement:
all inner inputs must be re-iterable.

## Verification Plan

Add regression tests:

- `TestProductRelation_StreamingRightSide_NoPanic` - product of two streaming
  relations returns all tuples without reopening panic.
- `TestProductRelation_StreamingErrorReplay` - if a materialized inner stream
  fails during cache build, the product surfaces the iterator error.
- End-to-end query test where `:find` symbols span disjoint streaming groups and
  the executor product path is exercised.

Also verify:

```bash
go test -count=1 ./...
```

---

## Resolution (2026-05-25)

**Resolved** with the report's option 1, but reached deliberately rather than by
reflex.

`NewProductRelation` now makes the non-leftmost operands re-iterable via
`Materialize()`:

```go
reiterable[0] = relations[0]
for i := 1; i < len(relations); i++ {
    reiterable[i] = relations[i].Materialize()
}
```

### Why this shape (the reasoning behind it)

- **Which operands:** the iterator (`ProductIterator.Next`) consumes index 0 once
  and reopens every operand `1..n-1` (for a 2-way product, the rightmost). So
  exactly `relations[1:]` need re-iterability; materializing the leftmost would be
  wasted.
- **Reuse, don't reinvent:** option 3 (a buffering product iterator) would
  duplicate machinery that already exists — `StreamingRelation.Materialize()` +
  `CachingIterator` (and `LazySeqRelation`/`BufferedIterator`) already cache on
  the first pass and replay on rewind. `Materialize()` here is *lazy* (it sets
  `shouldCache`; the cache is built during the product's first pass over the inner
  side), not eager full materialization.
- **No `M×N` guard:** the product output streams. `ProductIterator` generates
  combinations on demand and `ProductRelation.Project()` wraps it in a streaming
  `ProjectIterator`, so the `M×N` result is never buffered — only the rewound
  operands are (bounded by their own size). A guard against the product size would
  be second-guessing a result the query explicitly asked for, and protects no
  memory that the streaming output doesn't already.
- **Ordering left alone:** the executor hands groups to `Product()` in
  accumulation order (`query_executor.go`), with no size awareness; and product
  operands are typically `StreamingRelation`s with `Size() == -1`, so a
  "stream-the-largest" reorder isn't reliably available. Reordering is
  content-safe (Project normalizes columns) but unmeasurable here, so it's left as
  a possible future planner-level optimization for the case where group sizes are
  known.

### Tests

- `product_relation_streaming_test.go` —
  `TestProductRelation_StreamingRightSide_NoPanic` products two `StreamingRelation`s
  and asserts the full 2×2 result (panics before the fix; replays the cached inner
  side after). `TestProductRelation_SurfacesInnerStreamError` asserts a failed
  non-leftmost operand surfaces its error (`ProductIterator.Error()` already checks
  every operand).

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
