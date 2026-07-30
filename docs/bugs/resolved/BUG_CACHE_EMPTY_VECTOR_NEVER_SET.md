# A never-set vector attribute matches an empty-vector literal through the cache and not without it

**Status**: **Fixed** (2026-07-28, PR #114). Reading (1) below was ruled correct
and the fix went further than this document proposed — see *Resolution*.
`TestCacheMatrix_NeverSetVectorAgainstEmptyVectorLiteral` in
`datalog/storage/crdt_cache_matrix_test.go` is green, as are the
`vector/never set, V bound empty` and `vector/cleared to empty, V unbound` cases
of `TestConstantPatternCacheParity`. `make test` green, native and wasm.

## Resolution

The ruling: **presence is resolved, not counted, and an (E, A) that never existed
has no key in the cache.**

That is stronger than the fix this document sketched. Rather than `CacheEntry`
gaining a field to answer one predicate, all three `CacheResolver` methods report
`present`, `rebuild*` return a nil entry for an absent (E, A), and the cache
stores nothing for it — so entry existence *is* attribute existence and no arm
reconstructs the fact from a count or a nil slice.

The diagnosis below located the defect in the cache arm, which is where it
showed. The cause was one layer up: presence had never been modelled anywhere,
and the streaming arm only appeared correct because it borrowed
`Stats.TotalElements` from a struct whose constructor is documented "for
debugging and monitoring". Four correctness sites read that field; all four now
read a resolved `Present`, and `RGAStats` is off the correctness path entirely.

The internal inconsistency named at the end of *Mechanism* — the unbound-V branch
treating an empty `vectorList` as absence while the bound-V branch treats it as
the empty value — was a second, separate defect. Both arms held it, so the
cache-parity harness could not see it; it is recorded as D8 in
`docs/wip/COLLECTION_VALUE_SEMANTICS.md`.

`valueCount`'s vector arm now reports **1** for an empty `vectorList`, which is
correct precisely because of the change above: an entry exists only for an (E, A)
with datoms, so an empty list is a *cleared* vector and the empty vector is the
one value it serves.

## Summary

For a cardinality-vector attribute an entity never had, a pattern binding V to
an empty vector returns **one row with the cache on and zero rows with it off**.

```
cache_disabled = 0 rows
cache_enabled  = 1 row
```

The two arms disagree about what an absent vector is. One treats "never set" as
absence; the other cannot see the distinction at all.

## Mechanism

The streaming arm reads a fact the cached arm does not have.

`matchCardinalityVectorAsRelation` resolves through `resolveVector`, whose
result carries `Stats.TotalElements` — the number of RGA elements loaded for the
(E, A), tombstoned ones included. Its first exit is:

```go
// No datoms at all → attribute was never set
if result.Stats.TotalElements == 0 {
    return executor.NewMaterializedRelation(symbols, nil), nil
}
```

That return happens **before** V is compared, so a never-set attribute matches
nothing, whatever the pattern binds. A vector written and then wholly tombstoned
has `TotalElements > 0` with no live elements, falls past that exit, and *does*
compare — `ValuesEqual` of two empty vectors is true, so it matches an
empty-vector literal. The arm distinguishes the two states and answers them
differently, which is Pillar 1: a tombstone is an operation record, and "never
written" is not the same state as "written and retracted".

`matchFromCache` cannot make that distinction. `CacheEntry` holds
`vectorList []any` and nothing else about the vector's history —
`rebuildVector` stores `elements` from `ResolveRGA` and discards the stats — so
both states arrive as an empty `vectorList`. Its vector arm compares V against
the empty resolved vector and matches, giving the never-set case the answer the
streaming arm reserves for the tombstoned one.

The arm is also internally inconsistent about which answer it believes: its
unbound-V branch returns no tuple for an empty `vectorList`, treating that state
as absence, while its bound-V branch matches, treating it as the empty value.

## Why the reproducer drives the matcher directly

The V position needs an empty-vector constant. Routing through `db.Query` would
make the case depend on how the parser renders `[]` in the value position, which
is a separate question from whether the two arms agree once they have one. The
divergence is between `matchFromCache` and `matchCardinalityVectorAsRelation`,
so the reproducer calls the dispatch that chooses between them.

~~**Not established**: whether the parser can produce an empty-vector V constant,
and therefore whether the divergence is reachable from query text.~~

**Established, and it was reachable.** `TestConstantPatternCacheParity` drives
`[#id "cache-parity:subject" :person/skill [] ?tx]` through `db.Query` and
reproduces the same divergence the direct-matcher reproducer does. The parser
produces the empty-vector constant, so this was a live wrong answer from query
text, not a latent inconsistency between two internal arms.

## Where it surfaced

Derived while implementing R3 (`docs/reviews/PR114_TYPED_SCAN_BOUND_ROUND4_2026_07.md`),
which moved the cache arm's report off the three-term funnel onto
`values.served`. Deciding what a vector entry serves forced the question of what
an empty `vectorList` means, and the two arms turned out to answer it
differently.

`CacheEntry.valueCount` reports **0** for an empty `vectorList`, matching the
arm's unbound-V branch and the treatment a nil `oneValue` already gets. The
consequence is that the bound-V branch reports one matched against zero served.
That line is the divergence showing through the annotation rather than a
counting error: reporting 1 there would have produced a plausible-looking line
and made every never-set vector lookup claim a value it did not have.

## The fix, if it is in scope

Not "make the cache check `TotalElements`" in isolation — the entry would gain a
field to answer one predicate. The question the owner has to settle first is
which answer is correct, because the two arms currently ship both:

1. **A never-set vector attribute is absent**, and absence matches no V
   including the empty vector. This is the streaming arm's answer, and it is
   consistent with cardinality-one, where a never-set attribute matches nothing.
   `CacheEntry` then needs to record that the (E, A) had no datoms — one bool or
   the element total — and the bound-V branch must consult it.
2. **An empty vector is the value of an unset vector attribute**, so both states
   match an empty literal. This is the cache arm's bound-V answer. The streaming
   arm's `TotalElements` exit would go, and its unbound-V branch would have to be
   reconciled too, since it returns no tuple for a vector that exists and is
   empty.

Reading (1) is the one the rest of the CRDT model supports. Under (2), an
attribute nobody ever wrote has a value, and every cardinality-vector attribute
in the schema is implicitly present on every entity.
