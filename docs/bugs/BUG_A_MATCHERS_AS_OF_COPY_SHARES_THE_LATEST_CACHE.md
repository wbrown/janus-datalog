# A matcher's as-of copy shares the latest view's cache

**Status**: Open, recorded 2026-10-28. Found in the review of `d1e765c`.

`PatternMatcher.AsOf` returns a copy of the matcher that holds the same EA
cache as the matcher it was copied from. When that matcher is a database's
latest matcher, the copy reads and writes the latest view's cache. A fresh
latest entry answers the copy on every route the cache serves, so the copy
reads the latest view where its contract states the view as of its target.
An entry the copy resolves for an (E, A) the cache holds no slot for is
stored there as the latest view, and the database's own latest queries then
answer the as-of view. Both wrong answers are silent.

`Database.AsOf`, the documented temporal route, gives its handle a private
cache and answers correctly on every route.

## The route

`withMode` (`datalog/storage/matcher.go`, lines 169-191 at `d1e765c`) is the
copy both `PatternMatcher.AsOf` and `PatternMatcher.History` return. It sets
`cache: m.cache`, and its doc states the sharing: "The copy shares the store,
the read session, the schema, the cache and the tuple-builder population".

`cacheKey` (lines 89-99) refuses the cache in history mode alone. In as-of
mode it answers `CacheKey{E: e, A: a}` and ok, with no part of the key naming
the target. Its doc rests that on the cache instance:

> Latest and concrete AsOf modes both return an (E, A) key; they live in
> separate Cache instances (the Database's global latest cache vs. the AsOf
> handle's private cache), so the key never crosses snapshots.

That holds for a matcher minted by an as-of `Database` handle, and not for an
as-of copy of a matcher that carries the latest cache. The copy's key is the
latest view's key, in the latest view's cache.

`Cache.GetOrResolve` (`cache.go`) serves a fresh entry when the caller's
bound is nil or the slot's version lies within it. `cacheBound` answers nil
for a matcher with no read session. With a session it answers the session's
high-water mark, which the latest entry lies within as well. Neither reads
the matcher's as-of target, so the copy is served the latest entry.

The routes that reach the cache through the copy:

- The bound-entity route. `matchUnboundScan` (`matcher_relations.go`) sends
  a pattern with the entity and attribute bound to `matchFromCache`, which
  reads `GetOrResolve` under the copy's `cacheKey`. `MatchWithConstraints`
  reads it through `matchWithBindingsFromCache` for a binding route, and
  `LookupAttribute` and `LookupAllAttributes` (`matcher.go`) read it for
  entity lookups. Each is guarded by `m.cache != nil && m.mode !=
  historyMode`, and so is taken in as-of mode. This route answers the latest
  view at the merge-base `ea54ba8`.
- The unbound-entity cardinality-many scan, since `4062348`.
  `matchCardinalityManyScanAllEntities` scans AEVT for the entities carrying
  the attribute, and `cardinalityManyScanAllEntitiesIterator.Next` reads each
  entity's set through `GetOrResolve` when `cacheKey` answers ok and the
  matcher has a cache. Before `4062348` the scan resolved each set through
  `resolveAddWinsSet` (`set_resolution.go`), whose accumulator skips every
  datom `shouldFilterTx` filters, so it answered the as-of view.

A miss goes the other way. `GetOrResolve` rebuilds through the copy, which
resolves the as-of view, and `storeIfNotInFlight` stores the entry. Where the
cache holds no slot for the (E, A), which is every (E, A) no commit through
this database touched and no read through it cached, the entry is stored with its own version
as the slot's, and so compares fresh to the next latest reader. Where a slot
exists, its version is the latest write's and the stored entry compares
stale, so a latest reader rebuilds.

## What was observed

The fixture: `:tags` is cardinality-many, one entity `alice` is given `a` at
`tx1` and then `b`. A latest read warms the cache.
`m := d.Matcher().(*storage.PatternMatcher).AsOf(tx1)`.

- `[?e :tags ?v]` on `m` answers `[a]` at `ea54ba8` and `e4e7087`, and
  `[a b]` at `d1e765c`.
- `[alice :tags ?v]` on `m` answers `[a b]` at all three.
- `d.AsOf(tx1)` answers `[a]` on both patterns at all three.
- A query naming `m` as `$then` through `WithSources`,
  `[:find ?v :in $then :where [$then ?e :tags ?v]]`, answers `[a b]` at
  `d1e765c`.

The miss direction was observed at `d1e765c` over a second `Database` opened
on the store the first wrote, so its cache holds no slot for the (E, A). The
copy `d2.Matcher().(*PatternMatcher).AsOf(tx1)` reads `[alice :tags ?v]` as
`[a]`. After it, `d2`'s latest queries answer `[a]` for
`[:find ?v :in $ ?e :where [?e :tags ?v]]` and for
`[:find ?v :where [?e :tags ?v]]`, where a third `Database` on the same store
answers `[a b]`.

## Contracts it contradicts

- `PatternMatcher.AsOf`'s doc: "AsOf is a copy of this matcher reading the
  view as of target: each attribute resolved over the datoms at or before
  it."
- `cacheKey`'s doc, quoted above: latest and as-of keys "live in separate
  Cache instances ... so the key never crosses snapshots".
- The comments on the cache guards in `LookupAttribute`,
  `LookupAllAttributes` and `matchUnboundScan`: "Concrete AsOf matchers use
  transaction-scoped cache keys". No key names a transaction.
- `docs/reference/CRDT.md`, under "Three database modes" (line 876): "The
  cache stores current value only. History and as-of queries bypass the cache
  and scan storage directly." An as-of `Database` handle no longer bypasses
  the cache; it reads a private one (`Database.AsOf`, `database.go`, the
  `cache: NewCache()` field at line 739). An as-of copy of a latest matcher
  neither bypasses the cache nor reads a private one.

## Reach

`d.Matcher()` answers the `executor.PatternMatcher` interface, which has no
`AsOf`. Reaching the defect takes a type assertion to
`*storage.PatternMatcher` and then `AsOf` on the result. No code in the
repository or `examples/` calls `PatternMatcher.AsOf` outside tests, and no
document under `docs/` directs a caller to it.
The tests that call it, `TestCardinalityOneAsOf` (`crdt_one_test.go`) and
`TestTupleBuilderCacheSharing` (`matcher_concurrency_test.go`), build their
matcher with `NewPatternMatcher(store)`, which carries no cache, so none of
them reaches a cache route on the copy.

So no run, import or shipped caller produces the wrong answer today. What it
costs is a public method whose answer depends on the cache of the matcher it
was copied from, and a latest cache an as-of read can write its view into.

## The fix, if taken

The fix tried in review: `withMode` gives the copy `NewCache()` when the mode
is `asOfMode` and `m.cache` is non-nil, as `Database.AsOf` does for its
handle. The as-of view is immutable, so the private cache fills lazily and
needs no invalidation. The History copy keeps the shared cache, which it
never reads, because `cacheKey` refuses it. `withMode`'s doc, `cacheKey`'s
doc, the three guard comments and `CRDT.md`'s sentence would then state what
the code does.

With it applied, the probes answered `[a]` on every route, the miss direction
left the latest view `[a b]`, and `TestCardinalityManyUnboundScanUsesCache`
(`cache_integration_test.go`), `TestTupleBuilderCacheSharing` and
`TestCardinalityOneAsOf` passed. The alternatives, a key that names the
target or a copy that reads no cache, are the owner's to weigh.

## Reproducer

Not committed, since nothing outside tests reaches the method. Fails on both
`assert` lines at `d1e765c`, each with `listB` holding `"a"` and `"b"`.

```go
package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestMatcherAsOfReadsItsOwnView pins that a matcher's as-of copy reads the
// view as of its target on every route, after the latest view of the same
// (E, A) has been cached.
func TestMatcherAsOfReadsItsOwnView(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil)})
	require.NoError(t, err)
	defer db.Close()

	s, err := schema.NewBuilder().
		Attribute(":item/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	require.NoError(t, db.SetSchema(s))
	tags := datalog.NewKeyword(":item/tags")
	item := datalog.NewIdentity("as-of-cache:item")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(item, tags, "a"))
	first, err := tx.Commit()
	require.NoError(t, err)
	tx = db.NewTransaction()
	require.NoError(t, tx.Add(item, tags, "b"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Warm the latest view of (item, :item/tags) in the database's cache.
	_, err = executor.CollectTuples(db.Query(`[:find ?v :in $ ?e :where [?e :item/tags ?v]]`, item))
	require.NoError(t, err)

	read := func(m executor.PatternMatcher, e query.PatternElement) []interface{} {
		v := query.Variable{Name: datalog.NewSymbol("?v")}
		pattern := &query.DataPattern{Elements: []query.PatternElement{e, query.Constant{Value: tags}, v}}
		rel, err := m.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)
		pos := query.SymbolIndex(rel.Symbols(), v.Name)
		tuples, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		var out []interface{}
		for _, tuple := range tuples {
			out = append(out, tuple[pos])
		}
		return out
	}
	unbound := query.Variable{Name: datalog.NewSymbol("?e")}
	bound := query.Constant{Value: item}

	// The database's as-of handle owns a private cache.
	handle := db.AsOf(first).Matcher()
	require.ElementsMatch(t, []interface{}{"a"}, read(handle, unbound))
	require.ElementsMatch(t, []interface{}{"a"}, read(handle, bound))

	// The matcher's as-of copy holds the database's cache.
	copied := db.Matcher().(*PatternMatcher).AsOf(first)
	assert.ElementsMatch(t, []interface{}{"a"}, read(copied, unbound),
		"the unbound set scan served the latest view")
	assert.ElementsMatch(t, []interface{}{"a"}, read(copied, bound),
		"the bound-entity route served the latest view")
}
```

The miss direction needs a second `Database` on the same store, opened after
the writes, whose cache holds no slot for the (E, A): read
`[item :item/tags ?v]` on its matcher's `AsOf(first)`, then expect that
database's latest `[:find ?v :where [?e :item/tags ?v]]` to answer `a` and
`b`. It answers `a` alone.

## Related

`BUG_ASOF_WILDCARD_PULL_IGNORES_TEMPORAL_VIEW.md`, resolved, is the same
leak on the `Database` handle: `AsOf()` shared the parent's cache under
(E, A) keys. Its resolution gave `Database.AsOf` a private `NewCache()` and
opened the cache guards to as-of mode. The guards it opened are the ones the
copy takes, and the copy was left holding the cache it was copied with.
