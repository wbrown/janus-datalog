# BUG: Scan relations are not sets when the pattern projects away key components

**Status**: Fixed (2026-07-22, fix/identity-hash-only). Pinned by `TestSetSemantics_StorageQuery` (value-only leg), `TestScanProjectionPreservesSet`, `TestScanSetSemantics_DeclaredManyValueWildcard`, `TestScanSetSemantics_HistoryReassertedValue`, `TestPatternCoversDatomIdentity`, `TestMemoryMatcherScanRestoresSetSemantics`, `TestMemoryMatcherBoundScanIgnoresPassengerMultiplicity`, and the projection pins in `relation_properties_test.go` / `product_test.go`.

## Invariant

A `Relation` is always a set: each complete tuple appears at most once, from birth. Temporary tuple streams may require deduplication *before* becoming a Relation; set-preserving operators must not defensively deduplicate again.

## Symptom

`[:find ?v :where [_ :test/value ?v]]` returned every scanned datom's value, duplicates included (`[shared] [shared] [unique] [shared]`), in both planner modes. Surfaced by `TestSetSemantics_StorageQuery` the moment `Project` stopped deduplicating projections that provably preserve set-ness.

## Root cause

A pattern scan is a projection of the resolved datom stream onto the pattern's variables. When the pattern wildcards a component that belongs to the stream's candidate key, that projection is not injective — two distinct stream rows project to the same tuple — yet every scan constructor wrapped the tuple stream directly as a `StreamingRelation`. The relation was born carrying duplicates.

The candidate key of the emitted stream, verified against the implementation:

- **Current/as-of**: resolution emits one row per (E, A) group for effective cardinality-one — including CardinalityUnknown/schemaless, which the resolver defaults to LWW (`CRDTResolvingIterator.startNewGroup`) — and for cardinality-vector; declared CardinalityMany emits one row per (E, A, V). With A not constant the cardinality varies per attribute, so the conservative key is the superkey {E, A, V}.
- **History**: raw operation records, each carrying its own ElementID (`Transaction.Add` draws `clock.Next()` per datom), so {Tx} alone is a candidate key. Without Tx bound, re-assertions of the same (E, A, V) project to identical tuples.
- **Memory matcher**: raw datoms with no CRDT resolution and no ElementID guarantee — the datom's full identity (E, A, V, Tx) is the only key its stream carries.

A key component is covered when the pattern binds it to a constant (it does not vary) or a variable (it appears in the tuple). A wildcard — or an absent Tx position — drops a varying key component.

## Why it was invisible

Every `Project` implementation deduplicated unconditionally whenever the projected properties retained no candidate key — including identity projections, where dedup is semantically inert on a true set. The find boundary's projection therefore laundered the non-set scan relations into set results. Unkeyed joins retain internal full-tuple deduplication, masking the remaining paths. The violation predates the projection change that exposed it; only the last coat of paint was new.

## Fix

Set-ness is proven or restored at birth, and projections trust the invariant:

1. **Projection set-preservation is decided structurally** (`projectionPreservesSet`, all four Project homes: `MaterializedRelation`, `StreamingRelation`, `ProductRelation`, `LazySeqRelation`): a projection preserves set-ness iff the projected properties retain a candidate key, or the projection is a permutation of the full symbol set (targets pairwise distinct, arity equal, presence validated — pairwise distinctness matters because `[?x ?x]` passes presence validation at equal arity yet reads one position twice). Everything else wraps a streaming dedup.
2. **Storage scans restore set semantics at birth** (`scanProjectionPreservesSet` beside `unboundScanProperties`; `restoreScanSetSemantics` at the two dispatch exits, `MatchWithConstraints` and `matchUnboundAsRelation`): streaming scans whose projection drops a key component wrap a copying `DedupIterator` (scan iterators reuse workspace tuples, so the seen-keys copy). Materialized results are born deduplicated by their constructors. Keyed shapes — `[?e :attr ?v]` and every hot path — retain the key and pay nothing.
3. **The memory matcher applies the raw-bag rule** (`patternCoversDatomIdentity` in `datomsToRelationWithOptions` and the binding-driven path) and now projects its binding relation onto the pattern's symbols before matching, as the storage matcher always did — binding rows differing only in passenger symbols must not rescan the identical bound pattern.
4. **`DedupIterator` gained a copy mode**: its seen-key map retains admitted tuples by reference, which is sound over fresh-tuple sources (`ProjectIterator`) and was unsound over workspace-reusing scan iterators.

## Consequence for history projections

One pinned expectation changed (`TestHistoryModeCardinalityMany`): a history query projecting `?tag` with Tx blank sees two bindings for two adds and one remove of the same value — the add and the remove of "warrior" are distinct operation records that project to the same tuple, and a relation is a set. Observing every raw operation record requires binding `?tx`, which distinguishes the records; the test now pins both projections.

## Lesson

An unconditional dedup at a downstream boundary is a laundering layer: it repairs the observable result while leaving every intermediate consumer exposed to the invariant violation. When an optimization makes a boundary trust an invariant, red tests point at the producers that never honored it — fix the birth, not the boundary.

The full narrative — how this violation was exposed by retiring the last map-shaped exchange channel, and why the compensating dedup existed at all — is documented in `docs/TALE_AND_LESSONS_OF_RELATIONS_AS_EXCHANGE.md`.
