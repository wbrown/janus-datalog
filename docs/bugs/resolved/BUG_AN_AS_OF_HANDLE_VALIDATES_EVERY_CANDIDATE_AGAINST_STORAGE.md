# An as-of handle validates every candidate against storage

**Status**: Resolved. The body states the routes as filed; the Resolution at the end states the fix.

`Database.AsOf` gives its handle a private EA cache, which the handle's
resolvers fill with the view as of its target. Two routes of the matcher
refuse the cache by mode, the latest view alone. On an as-of handle a
bound-entity attribute check, a cardinality-one attribute of an entity a
prior clause bound compared to a constant, which the latest view answers by
one cache lookup per tuple, is left a pattern of its own, served by a
candidate scan over every entity carrying the constant, and each candidate
is validated by a point scan on EATV. The handle's cache is read on neither
route and filled by neither.

## The route

- `validatingVBoundIterator.validateCandidate`
  (`datalog/storage/matcher_relations.go`) takes the cache under
  `it.matcher.cache != nil && it.matcher.mode == latestMode && !it.attrIsUnique(a)`.
  Its comment lists the preconditions: "latest mode. The scan below reads
  the absolute-latest entry with NO shouldFilterTx, so it equals ResolveLWW
  only when no as-of/history filter applies. Concrete-AsOf and history keep
  the scan path untouched." The scan below the guard applies
  `shouldFilterTx` and so reads the as-of view, and the cache's resolver
  applies it too: `ResolveLWW` skips the datoms after the target. The
  precondition names a scan the function does not perform, and the cache it
  excludes answers what the scan answers.
- `PatternMatcher.CanFuseAttributeFetch` (`datalog/storage/matcher.go`)
  answers false under `m.mode != latestMode`. Its doc states the exclusion
  as a fact, "False ... on a history or as-of matcher", with no reason
  beside it. The executor's fetch bundle
  (`DefaultQueryExecutor.tryFuseAttributeFetchBundle`,
  `datalog/executor/query_executor.go`) folds a run of patterns
  `[?e :attr v]` on one bound entity into one point lookup per tuple through
  `LookupAttribute`, a constant `v` as a constraint, and asks the gate for
  each. `LookupAttribute` (`matcher.go`) reads the cache under
  `m.cache != nil && m.mode != historyMode`, so on an as-of handle it
  answers the handle's view through the private cache, and the fusion's own
  contract, that a match of the attribute answers per entity what
  `LookupAttribute` answers, holds there as on the latest view.

A pattern the latest view fuses is left a pattern on an as-of handle and
matched where the planner places it; placed before the clause that binds its
entity, it is the value-bound scan of `matchUnboundScan`
(`matcher_relations.go`): candidates by the constant over the index run of
every entity carrying it, each validated by the point scan above. Every
other cache route admits the as-of handle: `matchFromCache` and
`matchWithBindingsFromCache` (`matcher_relations.go`), and `LookupAttribute`
and `LookupAllAttributes` (`matcher.go`), each under
`m.cache != nil && m.mode != historyMode`, and `PatternMatcher.cacheKey`
(`matcher.go`) answers an (E, A) key in as-of mode.

## Contracts it contradicts

- `Database.AsOf`'s doc (`database.go`): the handle "owns a private EA cache
  scoped to the handle's lifetime. The snapshot is immutable, so this cache
  fills lazily, never needs invalidation". On the two routes nothing fills
  it.
- `validateCandidate`'s comment: "The EA cache resolves the current (E, A)
  value with the same EATV-first-entry + tombstone semantics this scan
  performs, but memoized", and the precondition quoted above. The scan
  filters by the target and `ResolveLWW` filters by it, so the equivalence
  the comment rests on holds on an as-of matcher.
- `CanFuseAttributeFetch`'s doc, which excludes the as-of matcher where
  `LookupAttribute` answers the as-of view through the cache.
- `docs/reference/CRDT.md`, under "Three database modes": "The cache stores
  current value only. History and as-of queries bypass the cache and scan
  storage directly." Every other route reads the as-of handle's cache, and
  these two obey the sentence.

## The fix, as filed

Derived from the sibling routes:

- `validateCandidate`'s guard becomes
  `it.matcher.cache != nil && it.matcher.mode != historyMode && !it.attrIsUnique(a)`,
  the sibling routes' condition. The private cache resolves the as-of value
  through `ResolveLWW`'s filter, and the latest cache is never the as-of
  handle's, so the key never crosses views. The comment's precondition list
  loses "latest mode" and names the filter both paths apply.
- `CanFuseAttributeFetch` answers false under `m.mode == historyMode` alone:
  on an as-of matcher `LookupAttribute` answers the view through the cache,
  which is the equivalence the doc states, and under History nothing
  resolves.
- `CRDT.md`, under "Three database modes", states what the code does: the
  cache holds the view's values, and a history read bypasses it.

## Reproducer

`datalog/storage/asof_candidate_validation_cache_test.go`. Two hundred items
in a chain, every item of one kind, and a query that binds each item from
the chain and checks its kind, run twice on the database and twice on
`db.AsOf(position)`, `position` the one commit. On the second run the latest
view validates 0 candidates against storage and 399 through the cache; the
as-of handle validates 399 against storage and 0 through the cache. At
`79fe33f` the test fails at its last assertion, "Should be zero, but was
399". `BenchmarkBoundAttributeCheckOnAnAsOfHandle` in the same file runs the
query over five thousand items on the persistent backend and logs each
view's account.

## Related

- `BUG_ASOF_WILDCARD_PULL_IGNORES_TEMPORAL_VIEW.md`, resolved: gave
  `Database.AsOf` its private cache and opened the matcher's cache guards
  from latest-only to every mode but History. The two routes here carry the
  latest-only guard that resolution retired.
- `BUG_A_MATCHERS_AS_OF_COPY_SHARES_THE_LATEST_CACHE.md`, resolved:
  `PatternMatcher.AsOf`'s copy held the latest cache, the other side of the
  same seam.
- `BUG_EA_CACHE_BYPASS_VARIABLE_ATTRIBUTE.md`, resolved: a cache route not
  reached under a binding shape, the same kind of defect.

## Resolution

- `validatingVBoundIterator.validateCandidate`
  (`datalog/storage/matcher_relations.go`) reads its view's cache in every
  mode but History, the sibling routes' condition. Its list of preconditions
  names the target filter that the scan and `ResolveLWW` both apply
  (`shouldFilterTx`).
- `PatternMatcher.CanFuseAttributeFetch` (`datalog/storage/matcher.go`)
  answers false in History alone. On an as-of matcher a cardinality-one
  attribute's fetch fuses, and `LookupAttribute` answers it as of the target.
- An as-of handle's reads take the cache rather than the scan path, for
  three reasons:
  - `Database.AsOf`'s doc promises the handle a private cache that fills
    lazily.
  - Every sibling route reads that cache.
  - The scan and the cache resolve the same value under the target filter.
- `docs/reference/CRDT.md`, under "Three database modes", already states
  that an as-of handle keeps its resolutions in a cache of its own and that
  History bypasses the cache.
- `TestVBoundCache_AsOfValidatesThroughItsViewsCache`
  (`vbound_cache_validation_test.go`) holds the as-of route on its view's
  cache, and still compares the answer with the cache enabled and disabled.
- The tests, each with the cache enabled and disabled:
  - `TestAsOfHandleValidatesCandidatesThroughItsOwnCache`
  - `TestAsOfHandleValidatesCandidatesAgainstTheValueAsOfItsTarget`
  - `TestFusionGate_ModeAndCardinality`
  - `TestFusion_CoverageFires`
  - `TestFusionDifferentialAsOfUpdatesAndTombstones`
  - `TestASchemalessDatabaseAnswersWhetherAMatchAnswersItsLookup`
