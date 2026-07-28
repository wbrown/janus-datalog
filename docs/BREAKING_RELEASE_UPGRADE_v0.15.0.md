# v0.15.0 Breaking Release Upgrade Guide

Version `v0.15.0` makes the ordered storage backend injectable so the full
`Database` API can run under `js/wasm` without BadgerDB. Browser durability in
this release is host-managed Export/Import; IndexedDB/OPFS backends remain a
later adapter.

## Required source migrations

### `Database.Store()` returns `storage.Store`

`(*storage.Database).Store()` previously exposed the concrete Badger handle.
It now returns the `storage.Store` interface.

```go
store := database.Store() // storage.Store
```

`(*BadgerStore).CountKeys` was never on `Store` and is no longer reachable
through `db.Store()` — keep a concrete `*BadgerStore` handle if you need
Badger-only key counting.

### Scans take a typed `ScanBound`; there is no point lookup

`Store.Scan` and `Store.ScanKeysOnly` took `(index IndexType, start, end []byte)`
and now take a single `ScanBound` — an index plus the leading components of that
index's component order, bound to datalog values:

```go
it, err := store.ScanKeysOnly(storage.ScanBound{
    Index:  storage.AVET,
    Prefix: []datalog.Value{attr, value},
})
```

`Iterator.Seek` takes a `ScanBound` for the same reason. A backend that keys on
bytes projects the bound at its own boundary; one that compares typed components
directly never encodes.

`Store.Get(index, key)` is **removed**, along with `StoreReader.Get`. A complete
index key names one (E, A, V, Tx), but Tx is what CRDT resolution determines, so
a caller holding one has nothing left to ask. A scan whose bound binds all four
components is that point lookup, and returns at most one datom because Tx is
unique per operation.

`StoreReader.Encoder()` is also removed — the read seam exposes no binary
encoder. `Encoder()` remains on `Store` itself.

`Store.MaxElementIDForAttribute` is removed along with the cache gate it served,
which had no production caller. This removes an implementation, not a capability:
ATEV's `[A][Tx↓][E][V]` layout still makes the first key under `[A]` the
attribute's max-Tx datom, so a caller needing the mark can scan
`ScanBound{Index: ATEV, Prefix: []datalog.Value{attr}}` and read the first
entry's `ElementID`. `MaxElementID` remains on the interface.

Call sites that type-asserted to `*storage.BadgerStore` must either:

1. Depend only on `storage.Store` methods, or
2. Keep a concrete handle obtained from `storage.NewBadgerStore` before injection.

### `DatomFromKey` takes `BlobReader`

The exported `DatomFromKey` final parameter is now `storage.BlobReader` instead
of `*badger.DB`. Pass `nil` when Tier-3 values are not expected. There is no
exported `*badger.DB` adapter; in-tree callers use the scan-session blob reader.

### Injected store ownership

`DatabaseOptions.Store` / `db.WithStore` leave ownership with the caller on
constructor failure (the store is not `Close`d). On success, `Database.Close`
closes the injected store. Compression threshold options apply only when the
constructor creates the store — configure an injected encoder before injection.

### Injected backends

Open a database with an explicit store:

```go
d, err := db.OpenMemory() // pure-Go ordered memory backend
// or
d, err := db.Open("", db.WithStore(storage.NewMemoryStore(nil)))
```

Native `db.Open(path, ...)` continues to select Badger when no store is
injected. On `js/wasm`, the default backend is the memory store; Badger is not
linked.

### Custom backends

Implement `storage.Store` / `storage.StoreTx` / `storage.Iterator` with the
workspace-decoded scan contract:

- `Scan` and `ScanKeysOnly` return the same decoded iterator behavior
- `Datom()` returns the iterator's current workspace until `Next`, `Seek`, or `Close`
- callers that retain values after those calls must copy
- Tier-3 blob decode uses the active scan session; sticky `Error()` retains the
  first decode failure after valid preceding rows
- `Scanned() int` reports intake: how many datoms the iterator has taken in from
  the index so far

A scan yields **exactly** the datoms whose bound components equal the
`ScanBound`'s values — no more. That is an obligation on the backend, not on the
caller, and it is not free for a backend that projects the bound onto byte keys:
a V payload carries no length, so the keys for `"abcd"` sort inside the range for
`"abc"` interleaved with them, and no choice of endpoints separates the two. The
in-tree backends narrow by key length; see `EncodedRun` and `runMembership` in
`key_encoder_binary.go`. A backend that returns everything inside a byte range
will return datoms the caller did not ask for, and no test above this seam will
say so.

`Scanned()` is what makes that obligation auditable, which is why it is required
rather than optional. Count **before** narrowing: a key the range covered and the
membership rule rejected is intake, because the scan paid to look at it. Against
the consumer's own count of what survived, the ratio is the amplification the
index charged; an absent count is indistinguishable from a scan that read nothing
and from instrumentation that was never wired. A wrapping iterator delegates to
the scan beneath it — it reads no index of its own.

### The index-nested-loop join strategy is removed

`PlannerOptions.IndexNestedLoopThreshold` and `ExecutorOptions.IndexNestedLoopThreshold`
no longer exist, and neither does the `storage.IndexNestedLoop` value of
`storage.JoinStrategy`. Delete any line that sets or names them; binding-driven
scans use `HashJoinScan`, or `MergeJoin` for large high-selectivity
entity-position binding sets.

Removing the first constant renumbered the two that remain: `HashJoinScan` moved
from 1 to 0 and `MergeJoin` from 2 to 1. Nothing persists a `JoinStrategy`, so
this matters only to code comparing against a stored integer or asserting on the
numeric value in an annotation; compare against the constants, or against
`String()`, which now renders an unknown value as `JoinStrategy(N)`.

The strategy had been off by default since 2025-10 and was independently
incorrect — its precondition, bindings sorted in index order, is unmet for the
Tx and V positions, where `CompareValues` order deliberately differs from key
order. See item 29 in `docs/wip/DECISION_LEDGER.md`.

### The annotation handler is a field, and the engine no longer wraps it

`Database.AnnotationHandler()` and `Database.SetAnnotationHandler()` are gone.
The handler is a field:

```go
// before
db.SetAnnotationHandler(h)
h := db.AnnotationHandler()

// after
db.AnnotationHandler = h
h := db.AnnotationHandler
```

`db.Open(path, db.WithAnnotationHandler(h))` is unchanged and remains the usual
way to install one.

The pair existed because the handler was *copied* into the cache and every
matcher, and an assignment cannot fan out to duplicates. The copies are gone —
`Cache` no longer holds one, receiving the handler per call — so there is
nothing left for a setter to keep in step, and the mutex that guarded the field
against the setter is gone with it.

**The engine no longer wraps installed handlers, and `annotations.Synchronized`
is removed.** It emits from parallel workers, so a handler is called
concurrently.

Wrapping was dropped rather than moved because applying it at one assignment
path and not another would give one field two concurrency contracts depending on
how it was populated; it also put a process-wide lock on every event on the hot
path, whether or not the handler needed one.

`Synchronized` went with it, and its removal is the substantive half. **A
handler that only renders needs no lock**: every event carries what its output
reports, so a handler holding nothing between events is safe by construction.
One that *does* hold something is reading whichever worker wrote last, and a
mutex does not correct that — it serializes the writes and leaves the reading
wrong. Keeping the wrapper around made "serialize the consumer" look like the
answer to a consumer that should not have had the state.

Migration, if a handler of yours keeps cross-event state: move that state to
whatever produces the events, or have the handler derive its output from the
event alone. If it accumulates for its own reasons — counting, batching, writing
to a file — it owns its synchronization, and a two-line mutex wrapper at the
install site is the whole of it.

## Browser persistence

Persist by snapshotting the memory database through the existing EDN stream:

```go
var buf bytes.Buffer
if err := database.Export(&buf); err != nil { ... }
// host stores buf.Bytes() (IndexedDB, OPFS, remote, etc.)

if err := database.Import(bytes.NewReader(snapshot)); err != nil { ... }
```

See `examples/wasm_memory.go` for a worker-friendly round-trip.

## What did not change

- Query/planner/executor packages and public query syntax
- Index layouts, CRDT semantics, Pull, History/AsOf, Export/Import formats
- Native Badger on-disk databases remain compatible
