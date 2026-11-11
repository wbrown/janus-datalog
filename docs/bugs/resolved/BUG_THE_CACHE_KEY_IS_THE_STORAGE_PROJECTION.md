# The EA cache keys on the storage projection and re-interns on every hit

**Status**: Resolved. The body states the defect as filed; the Resolution at the end states the fix.

## Summary

The EA cache names an (entity, attribute) pair by its byte projection, and
every hit turns the bytes back into the interned keyword.

- `CacheKey` is `{E Entity, A Attribute}` (`datalog/storage/cache.go`).
  `Entity` is `[20]byte`, the entity's SHA1 hash, and `Attribute` is
  `[32]byte`, the keyword's text.
- A reader holds the pair as an interned `datalog.Identity` and
  `datalog.Keyword`, and projects both into bytes to build the key.
- On a fresh hit, `Cache.GetOrResolve` checks the uniqueness rule through
  `PatternMatcher.ResolvesUnique(key.A)`. That is
  `m.schema.GetAttribute(datalog.InternKeywordFromBytes(a)).HasUniqueConstraint()`:
  a hash and a 32-byte compare in the keyword byte cache, then a lookup in
  the schema's map, on every hit.

## Where the hit's time goes

A CPU profile of `BenchmarkGetOrResolve_FreshHit`, memory-trees leg,
`-benchtime 3s`, on a loaded machine. At 9f80575 it ran at 46.19 ns/op, and
3.87s were sampled:

| Function | Cumulative (s) |
|---|---|
| `GetOrResolve`'s uniqueness case | 2.33 |
| `InternKeywordFromBytes`, within it | 1.39 |
| `Schema.GetAttribute`, within it | 0.73 |
| `cacheTrie.Load` | 0.86 |

The uniqueness case is over half of the hit. The hit's own work, the trie
walk and the freshness comparison, is the rest.

## The round trips

The pair crosses between its interned values and its bytes at each of these:

- `PatternMatcher.LookupAttribute` (`matcher.go`) builds the key from the
  storage projections `eBytes` and `aStorage`.
- `Transaction.Commit` (`database.go`) builds each touched key from
  `Entity(d.E.Hash())` and `copy(aBytes[:], d.A.String())`.
- `PatternMatcher.ResolvesUnique` re-interns the keyword with
  `InternKeywordFromBytes`, as above.
- `PatternMatcher.resolveAddWinsSet` (`set_resolution.go`) re-interns both
  halves, `NewIdentityFromHash(e)` and `InternKeywordFromBytes(a)`, to bound
  its scan.
- `annotateRebuild` (`cache.go`) re-interns the keyword for the event.
- `CacheResolver`'s methods take the pair as `(e Entity, a Attribute)`.
- `CacheKey.routing` (`hashtriemap.go`) routes the trie from E's first word
  and A's four words.

## The fact

The key names one (entity, attribute) pair. The interned `Identity` and
`Keyword` are that pair's values: interning makes each a canonical pointer,
so `==` compares them and a map keys on them. The byte arrays encode the same
pair a second time, and each crossing between the two encodings pays a copy
or an intern lookup.

## The fix, as filed

Key the cache by the interned `Identity` and `Keyword` the readers already
hold, and hand the resolvers the same values. A hit then compares pointers,
and the uniqueness rule is read from the keyword in hand.

## Resolution

- `CacheKey` is `{E datalog.Identity, A datalog.Keyword}`. Every site that
  built a key from bytes builds it from the values it holds.
- `CacheKey.routing` (`hashtriemap.go`) folds E's first hash word with
  `A.Hash64()`. `keyword.hash`, the FNV-1a hash of the keyword's text, is
  computed once where the keyword is interned.
- The resolvers under the cache take the interned values:
  - `CacheResolver`'s methods
  - `resolveAddWinsSet`
  - `walkUniqueEntityValue`, `walkApplyEntry` and `resolveMaxOtherTxForValue`
  - `loadRGAElements` and `resolveVector`
- The reader passes the cache the attribute's `*schema.AttributeDefinition`,
  nil for an undeclared attribute. `GetOrResolve`, `ResolveEntry` and the
  rebuilds take it, and `ResolveLWW` walks the unique rule when the
  definition declares one and the matcher is not in History. A hit compares
  the entry's cardinality and uniqueness with the definition's and looks
  nothing up. `ResolvesUnique` is gone.
- A History handle shares its database's cache. `WarmCache`, `GetVectorNth`
  and `GetVectorLength` build their keys through `PatternMatcher.cacheKey`,
  which refuses History, so a History handle stores nothing in the cache.
- The tests:
  - `TestAKeywordsHash64IsTheFNV1aHashOfItsText`
  - `TestAHistoryHandleStoresNothingInTheCacheItShares`, every backend,
    with the cache enabled and disabled
  - `TestAUniqueSetScannedOverEveryEntityIsServedFromTheCache`, every
    backend, with the cache enabled and disabled
  - the existing cache suites, with the cache on against off

The measurements, Apple M5, `-count 5`, run back to back. Before is the
janus checkout at `cd029af`. After is the branch with this fix and its four
companions. Each range spans one backend's five runs; the badger-emulator
leg the benchmark also runs is not a measurement target and is left out.

`BenchmarkGetOrResolve_FreshHit`:

| backend | before (ns/op) | after (ns/op) |
|---|---|---|
| memory-trees | 20.05–21.45 | 5.56–5.92 |
| badger | 19.98–22.59 | 5.44–5.79 |

A fresh hit allocates nothing before and after.

`BenchmarkComplexQueryCheckpoint` and its `Prefetch` variant, badger:

| benchmark | before (ms/op) | after (ms/op) |
|---|---|---|
| checkpoint | 15.2–17.7 | 11.9–12.3 |
| checkpoint, prefetch | 15.2–17.2 | 12.0–13.2 |

Both checkpoint benchmarks allocate 13.71 MB/op before and after.
