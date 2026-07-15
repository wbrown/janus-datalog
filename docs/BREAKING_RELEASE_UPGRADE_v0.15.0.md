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

Call sites that type-asserted to `*storage.BadgerStore` must either:

1. Depend only on `storage.Store` methods, or
2. Keep a concrete handle obtained from `storage.NewBadgerStore` before injection.

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
