# BUG: Schemaless Reads Collapse CRDT Vector/Set Attributes to a Single LWW Value

**Date**: 2026-05-31
**Severity**: Correctness / silent data loss on read (High)
**Status**: ✅ RESOLVED (2026-05-31)
**Affected**: Any reader that opens a database without a schema (`storage.NewDatabase(path)`) and queries a `CardinalityVector` (RGA) or `CardinalityMany` (add-wins set) attribute. Includes the `datalog` CLI and `ednstats`.

## Resolution

Cardinality is **reconstructed from the stored CRDT ops at open** and installed
as the database's schema. Because every read (and write) path already consults
`d.schema`, populating it once at open makes them all resolve correctly with **no
query-path special-casing** — the fix lives entirely at construction time.

### Two corrections to the original report (confirmed during investigation)

1. **The original reproduction was invalid.** `[:find (count ?v) …]` returns 1
   whether the read is correct or collapsed: a vector binds `?v` to a single
   *list* value, so `count` is 1 either way. The regression tests instead assert
   read-equality — schemaless read == schema-aware read of the same bytes —
   inspecting the bound value's shape.

2. **The collapse had four+ schema-only decision sites, not two.** A schemaless
   `LookupAttribute` (its own inline schema default), the streaming
   `CRDTResolvingIterator` (unbound `?e`), the cache `rebuild`/`ResolveEntry`
   paths, and `PrefetchEntities` each independently defaulted a silent attribute
   to cardinality-one. Constructing the schema at open fixes all of them at once
   rather than patching each.

### Why a per-site op-peek does NOT work (a first attempt that was abandoned)

A first attempt added op-inference at each read site by peeking the **first**
stored datom's op. It broke `TestSchemalessRemove_ThenReAdd` because
**`OpCRDTRemove` is not unique to cardinality-many**: it is written for both a
cardinality-one tombstone and a cardinality-many member removal
(`database.go` `Remove`). Peeking a leading Remove misclassified a removed
cardinality-one attribute as many, and its re-Add (`OpNone`) then read as absent.

### The shipped fix

`datalog/storage/cardinality_inference.go`:

- `cardFromOp(op)` returns `(cardinality, decisive)`. `OpCRDTAdd`→many,
  `OpRGAInsert/OpRGATombstone`→vector, `OpNone`→one are **decisive**;
  `OpCRDTRemove` is **not** (ambiguous one-vs-many).
- `inferSchemaFromStore` does one keys-only pass over the **ATEV** index
  (`[A][Tx↓][E][V]`, which groups every datom by attribute) and classifies each
  attribute by its first **decisive** op — skipping leading `OpCRDTRemove`
  entries. An attribute whose entries are all removes resolves empty under either
  cardinality, so it defaults to one. `ValueType` is taken from a representative
  value (affects typed-vector formatting only).

`database.go` `NewDatabaseWithOptions`: when `opts.Schema == nil`, the effective
schema is `inferSchemaFromStore(store)`. A supplied schema is authoritative and
wins entirely (no inference). On an empty store this yields an empty schema,
equivalent to the prior nil behavior.

This is installed into `d.schema`, so it governs **reads and writes**: a
schemaless reopen that appends to an existing vector/many attribute now emits the
correct op (`OpRGAInsert`/`OpCRDTAdd`) instead of corrupting the group with
`OpNone`. (Cardinality is immutable per attribute in this engine — changing it on
existing data is unsupported — so a reconstructed cardinality cannot disagree
with future consistent writes.)

### Regression coverage

`datalog/storage/schemaless_crdt_read_test.go` — all assert schemaless ==
schema-aware on the same bytes; the read tests were verified to fail before the
fix:

- `TestSchemalessRead_VectorMatchesSchemaAware` — streaming (unbound `?e`) and
  cache (bound `?e`) query paths, cache on and off.
- `TestSchemalessLookupAttribute_VectorMatchesSchemaAware` — the `LookupAttribute`
  (Pull) path via the production `db.Matcher()`.
- `TestSchemalessRead_ManyMatchesSchemaAware` — cardinality-many set.
- `TestSchemalessPrefetch_VectorNotCollapsed` — the `EnableEntityPrefetch` path.
- `TestSchemalessReopen_WriteUsesInferredCardinality` — schemaless reopen + append
  extends the vector (RGA) rather than LWW-overwriting it.

## Summary

When a database is opened **without a schema**, querying a cardinality-vector attribute returns only its **last element**, and a cardinality-many attribute returns a single **member** — instead of the full collection. A `count` over a 56-element vector reports `1`.

The storage is intact: every vector datom is persisted as `OpRGAInsert` with its `AfterRef`, and every set datom as `OpCRDTAdd`. A schema-aware open of the same database returns the full collection. The loss is purely in read-resolution on the schemaless path, and it is **silent** — a plausible single value comes back with no error.

This is not "schemaless reads are unsupported." janus already contains an op-inferring resolver (`lookupAllAttributesFallback`) that reconstructs the correct cardinality from the datom ops with no schema. The cache-population path simply doesn't use it: it defaults schemaless attributes to cardinality-one and shadows the correct path.

## Discovery

Found while investigating an apparent `:module/vocabulary` collapse in a downstream scribe import. That attribute is `:db.cardinality/vector`, accumulated across ~50 separate `tx.Add` appends to one entity. Through the CLI it read as a single element:

```
$ datalog -db curse_of_grimholt.db -query '[:find (count ?v) :where [?e :module/vocabulary ?v]]'
| (count ?v) | 1 |
```

…which looked like a last-writer-wins **storage** collapse (the survivor was the last append). But a schema-aware reader showed all 56 entries present and correct, so the discrepancy is the schemaless read, not the data. Probing the raw datom with its Tx component still returns one row — confirming the reader resolves the attribute as cardinality-one (latest), rather than exposing the underlying RGA datoms:

```
$ datalog -db curse_of_grimholt.db -query '[:find (count ?v) :where [?e :module/vocabulary ?v ?tx]]'
| (count ?v) | 1 |
```

## Reproduction

Write a vector attribute with N elements **with a schema** (so the appends are written as `OpRGAInsert`), then reopen the same database **without a schema** (as the CLI does) and query:

```go
// Writer — schema declares :doc/lines as CardinalityVector.
dbw, _ := storage.NewDatabaseWithSchema(path, schemaWithVectorAttr)
e := datalog.NewIdentity("doc-1")
for i := 0; i < 5; i++ {
    tx := dbw.Store().NewTransaction()
    tx.Add(e, linesAttr, fmt.Sprintf("line %d", i)) // OpRGAInsert, AfterRef chained to prior
    tx.Commit()
}
dbw.Close()

// Reader — no schema, exactly how cmd/datalog opens (main.go:103).
dbr, _ := storage.NewDatabase(path)
// query: [:find (count ?v) :where [?e :doc/lines ?v]]
```

**Expected**: 5
**Actual**: 1 (the last element, `"line 4"`)

The same holds for a `CardinalityMany` attribute: the schemaless reader returns one member instead of the full set.

## Root Cause

Cardinality is resolved from the **schema**, and the schemaless `default` falls back to cardinality-one (LWW) instead of inferring cardinality from the datom ops — even though the data carries the ops needed to do so, and an op-inferring resolver already exists.

### The schemaless default (two sites)

`Cache.rebuild` — populates the cache (`datalog/storage/cache.go:394`):

```go
func (c *Cache) rebuild(key CacheKey, resolver CacheResolver) *CacheEntry {
    card := resolver.GetCardinality(key.A)   // schema-derived
    switch card {
    case schema.CardinalityOne:    return c.rebuildOne(key, resolver)
    case schema.CardinalityMany:   return c.rebuildMany(key, resolver)
    case schema.CardinalityVector: return c.rebuildVector(key, resolver)
    default:
        // Default to cardinality-one for schemaless
        return c.rebuildOne(key, resolver)   // LWW: latest element only
    }
}
```

`ResolveEntry` — the cache-disabled path (`datalog/storage/cache.go:478`) — carries the identical schemaless default (`cache.go:516`), calling `resolver.ResolveLWW`. So the collapse happens whether the EA cache is enabled or not.

`CacheResolver.GetCardinality` (`cache.go:461`) returns the schema cardinality; for an attribute with no schema declaration it yields no match, so both functions hit `default` → cardinality-one → LWW → a single value, which is then cached.

### The correct logic already exists, but is shadowed

`lookupAllAttributesFallback` (`datalog/storage/matcher.go:1019`) resolves an (E, A) **by inferring cardinality from the CRDT ops present in storage**, no schema required:

```
// - OpNone                       → LWW (cardinality-one): latest value by ElementID
// - OpCRDTAdd/OpCRDTRemove        → add-wins set (cardinality-many)
// - OpRGAInsert/OpRGATombstone    → RGA vector (cardinality-vector): reconstruct ordered list
```

It peeks the first datom's op and dispatches accordingly (`resolveVector` for `OpRGAInsert`). That is exactly the right behavior for a schemaless read. But `matcher.go:1008` reaches it only "when cache is not set" — and `rebuild` *sets* the cache, with the wrong cardinality-one entry. So the cached lookup branch (`matcher.go:996-1004`, `case CardinalityOne: return single value`) wins, and the op-aware fallback never runs.

### The triggering reader

`cmd/datalog/main.go:103` opens the database with `storage.NewDatabase(dbPath)` — no schema. The data is self-describing (every datom carries its op, and `key_encoder_binary.go` persists `AfterRef` for `OpRGAInsert`/`OpRGATombstone`), so cardinality is recoverable; the cache path just doesn't recover it.

## Why this is a bug, not "schema required"

The existence and documented intent of `lookupAllAttributesFallback` — "Infer cardinality from the CRDT ops present in the datoms… rather than returning raw datoms" — establishes that schemaless reads are *meant* to reconstruct CRDT values from the ops. The cache-population paths (`rebuild`, `ResolveEntry`) violate that contract by defaulting to cardinality-one. Two paths disagree on the same input, and the wrong one wins.

## Blast radius

- **Both** `CardinalityVector` and `CardinalityMany` attributes collapse to a single value under any schemaless read.
- Any consumer on `storage.NewDatabase(path)` without a schema: the `datalog` CLI (`-query`, counts, pulls), `ednstats`, and external tools.
- **Silent**: returns a plausible last value / single member, no error — easily mistaken for a storage-level LWW collapse (it is not).
- **Storage is unaffected.** Schema-aware readers, and `lookupAllAttributesFallback` on the no-cache path, return the full collection.

## Proposed Fix

In the schemaless `default` cases of both `Cache.rebuild` (`cache.go:408`) and `ResolveEntry` (`cache.go:516`), infer cardinality from the CRDT ops before resolving — mirror `lookupAllAttributesFallback`: peek the first datom's op for (E, A) and dispatch to the RGA / add-wins / LWW resolver accordingly. Centralizing this in `GetCardinality` (op-inference when the schema has no declaration for the attribute) would fix both sites, and any other `GetCardinality`-gated path, at once.

## Files involved

| File | Issue |
|------|-------|
| `datalog/storage/cache.go:394` (`Cache.rebuild`) | schemaless `default` → `rebuildOne` (LWW) collapses vector/many to one value; populates the cache, shadowing the op-aware fallback |
| `datalog/storage/cache.go:478` (`ResolveEntry`) | identical schemaless `default` → `ResolveLWW` on the cache-disabled path (`cache.go:516`) |
| `datalog/storage/cache.go:461` (`CacheResolver.GetCardinality`) | returns no cardinality for schemaless attrs → routes both to `default` |
| `datalog/storage/matcher.go:1019` (`lookupAllAttributesFallback`) | correct op-inference exists but runs only "when cache is not set" (`matcher.go:1008`) |
| `datalog/storage/matcher.go:996-1004` | cached lookup returns the single value for the (wrong) cardinality-one entry |
| `cmd/datalog/main.go:103` | CLI opens via `storage.NewDatabase(path)` — the schemaless reader that triggers it |

## Regression test (proposed)

Write a `CardinalityVector` attribute (N≥2 elements) and a `CardinalityMany` attribute with a schema; close; reopen the same path via `storage.NewDatabase(path)` (no schema); assert a query/count returns N (vector) and the full set (many), not 1. A second variant with the EA cache disabled covers the `ResolveEntry` path.
