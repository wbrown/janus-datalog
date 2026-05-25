# BUG: UnionRelation Concurrent Iteration and Inner Error Drop

**Date**: 2026-05-24
**Severity**: Medium-High - streaming union can race, duplicate consumption, or hide worker/inner iterator failures
**Status**: Resolved (2026-05-25) — see Resolution below
**Affected**: `executor.UnionRelation`, `executor.UnionIterator`, streaming subquery union paths

## Summary

`UnionRelation` is intended to make a channel-backed streaming union reusable:
the first `Iterator()` consumes the channel and builds a tuple cache, and later
`Iterator()` calls replay that cache.

The implementation has two related problems:

1. **Concurrent `Iterator()` calls before the cache is built can create multiple
   consumers over the same one-shot channel and shared cache slice.**
2. **Errors from the current inner relation iterator are dropped when that inner
   iterator exhausts and is closed.**

Both issues sit on a high-risk boundary: parallel subquery execution produces
relations through a channel, while the `Relation` interface promises reusable
iteration.

## Root Cause

### 1. `Iterator()` protects construction, not active cache building

```go
func (ur *UnionRelation) Iterator() Iterator {
    ur.cacheMutex.Lock()
    defer ur.cacheMutex.Unlock()

    // If cache is already built, return a simple slice iterator over cached tuples
    if ur.cacheBuilt {
        return &sliceIterator{
            tuples: ur.cached,
            pos:    -1,
        }
    }

    // First call - need to consume channel and build cache
    return NewUnionIteratorWithCache(ur.source, &ur.cached, &ur.cacheBuilt)
}
```

The mutex only protects the short critical section that creates the iterator. It
does not record "cache build in progress."

If goroutine A calls `Iterator()` and starts consuming the channel, goroutine B
can call `Iterator()` before A finishes. Since `cacheBuilt` is still false, B also
gets a `UnionIterator` over the same channel and the same cache pointer.

That violates the comment at the top of the file:

```go
// IMPORTANT: Channels can only be consumed once, but Relations must be reusable
// (multiple Iterator() calls).
// Solution: First Iterator() call consumes channel and caches results,
// subsequent calls replay from cache.
```

The current code implements "subsequent calls after cache completion replay from
cache," not "subsequent calls while cache is building wait for cache completion."

### 2. Inner iterator errors are not checked before discard

```go
// Current iterator exhausted - close it and get next relation
if it.currentIter != nil {
    it.currentIter.Close()
    it.currentIter = nil
    it.currentRelation = nil
}
```

When an inner iterator's `Next()` returns false, `UnionIterator` closes and drops
it without checking `it.currentIter.Error()`.

`UnionIterator.Error()` can only return `currentIter.Error()` while
`currentIter` is still assigned:

```go
func (it *UnionIterator) Error() error {
    if it.firstError != nil {
        return it.firstError
    }
    if it.currentIter != nil {
        return it.currentIter.Error()
    }
    return nil
}
```

After the iterator is closed and set to nil, that error is lost.

## Expected Behavior

`UnionRelation` should preserve normal `Relation` semantics:

1. Multiple calls to `Iterator()` should be safe.
2. A channel-backed relation should only have one channel consumer.
3. Iterators created while the cache is being built should either:
   - block until the cache is complete and then replay it, or
   - participate in a documented single-use protocol.
4. Any error from worker items or inner relation iterators should surface via
   `Iterator.Error()`.

## Actual Behavior

1. Multiple concurrent `Iterator()` calls can consume the same channel.
2. Multiple iterators can append to the same cache slice without synchronization.
3. Inner iterator errors can be dropped at relation boundaries.

## Failure Mode 1: Split Channel Consumption

### Sketch

```go
union := NewUnionRelation(ch, symbols, opts)

go func() {
    it := union.Iterator()
    defer it.Close()
    for it.Next() {
        // consume some results
    }
}()

go func() {
    it := union.Iterator()
    defer it.Close()
    for it.Next() {
        // consumes from the same channel concurrently
    }
}()
```

Possible outcomes:

- each iterator sees only part of the union
- cache contents are incomplete or nondeterministic
- tuple order and dedup behavior depends on scheduling
- cache append races under the race detector

## Failure Mode 2: Inner Iterator Error Lost

### Sketch

1. Channel yields a relation whose iterator returns several tuples.
2. Inner iterator then stops with `Error() == sentinel`.
3. `UnionIterator.Next()` sees `currentIter.Next() == false`.
4. It calls `currentIter.Close()` and sets `currentIter = nil`.
5. `UnionIterator.Error()` returns nil because `firstError` is nil and
   `currentIter` is gone.

The caller gets a successful prefix of the union.

## Failure Mode 3: Worker Error Can Be Delayed or Masked

`relationItem.err` is captured in `firstError`, but the iterator continues
processing other items:

```go
if item.err != nil {
    if it.firstError == nil {
        it.firstError = item.err
    }
    continue
}
```

Continuing after an error is defensible if the final `Error()` is always checked,
but it compounds the public-boundary bug documented in
`BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md`: callers that collect
tuples without checking `Error()` will see a plausible partial result.

## Impact

### Correctness

Parallel subquery results can be silently truncated or split between iterators if
the relation is iterated concurrently before cache completion.

### Resource Safety

If a second iterator drains the channel unexpectedly, the first iterator's close
and drain behavior can interact badly with producer goroutines. The current code
tries to drain remaining items in `Close()` to unblock producers, which is
reasonable for a single consumer but fragile when there are multiple consumers.

### Error Reporting

Inner relation failures can vanish even if callers correctly check
`UnionIterator.Error()`.

## Fix Direction

### Cache Build Protocol

Make cache construction an explicit state machine:

```text
notStarted -> building -> built
                 |
                 v
              failed
```

Only one iterator should own channel consumption. Other `Iterator()` calls while
state is `building` should wait on a completion channel/condition and then replay
the built cache.

The cache append path must be protected if any design allows more than one
goroutine to touch the cache. The simpler design is single builder, many replay
readers.

### Error Preservation

Before closing an exhausted inner iterator:

```go
if err := it.currentIter.Error(); err != nil && it.firstError == nil {
    it.firstError = err
}
_ = it.currentIter.Close()
```

Also consider preserving close errors if no iteration error has already been
recorded.

### Public Boundary

This bug should be fixed together with public iterator error propagation. A
perfect `UnionIterator.Error()` still does not help if `CollectTuples`,
`QueryInto`, or `QueryOneInto` drop the error after iteration.

## Verification Plan

Add focused unit tests:

1. `TestUnionRelation_ConcurrentIteratorWhileBuilding`
   - create a channel-backed union whose producer blocks between relation items
   - call `Iterator()` from two goroutines before the first finishes
   - expect both iterators to see the same complete tuple set
   - run under `-race`

2. `TestUnionRelation_OnlyOneChannelConsumer`
   - instrument channel item consumption count
   - verify one builder consumes the channel and later iterators replay cache

3. `TestUnionIterator_InnerIteratorErrorPropagates`
   - channel yields a relation with an iterator that fails after one tuple
   - consume union to exhaustion
   - expect `it.Error()` returns sentinel

4. `TestUnionRelation_MaterializeReturnsIteratorError`
   - after the public-boundary fix, materializing or collecting a failing union
     should return the stored error instead of silently returning a prefix

## Related

- `docs/bugs/BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md`
- `docs/bugs/BUG_STREAMING_TUPLE_COPYING.md`
- `docs/bugs/BUG_ITERATOR_LEAK_BUILTIN_EVALUATION.md`

---

## Resolution (2026-05-25)

**Resolved.** All three failure modes are addressed.

### Failure Mode 1 — split channel consumption / cache race (fixed in this change)

`UnionRelation.Iterator()` is now a three-way state machine under `cacheMutex`:

- **built**: replay the final cache via a slice iterator that carries any build error.
- **building**: a concurrent caller gets a `unionReplayWaitIterator` that blocks on a completion channel (`buildDone`) and then replays the complete cache. It never touches the source channel.
- **not started**: the first caller becomes the sole builder. It streams the channel, dedups, and builds the cache locally, then publishes it via `finishBuild` (sets the cache, records the first error, closes `buildDone`). `Close()` on an abandoned builder drains the remainder into the cache so the published cache is always complete.

This guarantees exactly one channel consumer and that every `Iterator()` sees the complete union. Verified by `TestUnionRelation_ConcurrentIteratorWhileBuilding` (previously a 277/500 channel split plus a data race on the cache append; now both iterators see all 500 and `-race` is clean) and `TestUnionRelation_OnlyOneChannelConsumer` (the builder iterates each source relation exactly once; the replay call re-consumes nothing).

The blocking-replay design has one internal-only hazard: a single goroutine that holds both the builder and a replay iterator and drives the replay one *before* completing the builder would self-deadlock. The executor never does this — it drives one iterator to completion before obtaining another (the later call then sees `built` and replays without blocking), and genuine concurrent callers are on separate goroutines — so it cannot arise in current usage.

### Failure Mode 2 — inner iterator error dropped (fixed earlier this session)

`UnionIterator.Next()` now captures an exhausted inner iterator's `Error()` into `firstError` before closing and discarding it. Covered by `TestUnionIterator_InnerIteratorErrorPropagates`.

### Failure Mode 3 — worker error masked at the public boundary

`UnionIterator` still continues past a worker `item.err` (recording the first one), which is correct only if the final `Error()` is checked. The public boundaries now do check it — `CollectTuples`, `QueryInto`, and `QueryOneInto` all surface iterator errors (see `resolved/BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md`) — so a worker error can no longer be silently swallowed into a plausible partial result.
