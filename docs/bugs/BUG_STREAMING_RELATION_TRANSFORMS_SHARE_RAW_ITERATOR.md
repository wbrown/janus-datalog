# NOTE: StreamingRelation Transforms Share Raw Iterator

**Date**: 2026-05-25
**Severity**: Low — Latent code-hygiene inconsistency, no observed failure
**Status**: Open (low priority)
**Affected**: `StreamingRelation.Filter`, `FilterWithPredicate`, `EvaluateFunction`

## Summary

Three of `StreamingRelation`'s transform methods wrap `r.iterator` (the raw
field) directly instead of calling `r.Iterator()`. The result is functionally
correct for the way the executor pipelines actually compose relations today,
but the pattern is inconsistent with `StreamingRelation.Project`, which goes
through `Relation.Iterator()` via `NewProjectIterator(r, ...)`. Worth tidying
for consistency; not a known correctness bug.

**No reproducer exists.** This was caught by reading the code, not by an
observed failure. The transforms whose pattern this report criticises are
each tested for their intended behaviour and pass; the report describes a
hypothetical mis-composition that the surrounding code happens not to exercise.

## Code Evidence

`Filter` constructs a new iterator directly from `r.iterator`:

```go
func (r *StreamingRelation) Filter(filter Filter) Relation {
    if r.options.EnableIteratorComposition {
        filterIter := NewFilterIterator(r.iterator, r.symbols, filter)
        return NewStreamingRelationWithOptions(r.symbols, filterIter, r.options)
    }
    return FilterRelation(r, filter)
}
```

`FilterWithPredicate` and `EvaluateFunction` follow the same pattern:

```go
predIter := NewPredicateFilterIterator(r.iterator, r.symbols, pred)
```

```go
evalIter := NewFunctionEvaluatorIterator(r.iterator, r.symbols, fn, outputSymbol)
```

By contrast, `StreamingRelation.Project` uses the relation-as-source form:

```go
projIter := NewProjectIterator(r, r.symbols, symbols)
```

The `Project` shape is what the rest of the streaming machinery expects:
`r.Iterator()` does the single-use guard, lazy `Materialize` caching, and
deferred-error replay. The three transforms named above skip that step.

## Why It's Not Currently Biting

For a derived relation to observe a difference, you would need *both*:

1. `Materialize()` to have been called on the source `StreamingRelation`
   *before* `Filter` / `FilterWithPredicate` / `EvaluateFunction`, so the
   cache machinery is engaged.
2. Both the source relation *and* the derived relation to be iterated.

In practice the executor consumes the derived relation only and drops the
source. The transforms-on-`r.iterator` path then consumes the raw iterator
exactly once, and the absence of cache participation has no observable effect.
That's why the inconsistency has gone unnoticed.

## Hypothetical (Not Reproduced) Failure Modes

If somebody composed pipelines that did trigger both preconditions above,
they could see:

- the derived relation skipping tuples that the source's `Materialize` cache
  thought it owned
- deferred errors stored on the source's `err` field not appearing on the
  derived relation
- `Iterator()`'s single-use guard not firing in a case it would have under
  the `Project` shape

None of these has been demonstrated against the current executor. They are
the consequences predicted by reading `Iterator()`'s body, not bugs observed
in tests or production.

## Fix Direction

Mechanical: change

```go
NewFilterIterator(r.iterator, ...)
NewPredicateFilterIterator(r.iterator, ...)
NewFunctionEvaluatorIterator(r.iterator, ...)
```

to the relation-source form (analogous to `NewProjectIterator(r, ...)`), so
the iterator delays calling `r.Iterator()` until its own iteration begins.
The `Project` site is the existing template.

## Verification Plan (if anyone picks this up)

1. A test that calls `Materialize()` then `Filter`, drives both the source
   *and* the derived relation, and verifies the cache replay rules apply
   uniformly. (Today this likely doesn't pin a meaningful behaviour because
   nothing exercises it — write the test first to prove which behaviour
   is intended.)
2. Failing-iterator source + derived transform; verify deferred-error replay.
3. Verify `EnableIteratorComposition: true` and `false` paths agree.

If any of those tests fail before the fix and pass after, this stops being
hypothetical and the severity goes up. Until then it's an inconsistency worth
fixing only when someone is already in the area.
