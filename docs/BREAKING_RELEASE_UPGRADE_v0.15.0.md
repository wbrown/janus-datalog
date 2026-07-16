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

`Store.Get(index, key)` remains on the interface (point lookup by full index
key; missing keys return `(nil, nil)`). `(*BadgerStore).CountKeys` was never on
`Store` and is no longer reachable through `db.Store()` — keep a concrete
`*BadgerStore` handle if you need Badger-only key counting.

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
