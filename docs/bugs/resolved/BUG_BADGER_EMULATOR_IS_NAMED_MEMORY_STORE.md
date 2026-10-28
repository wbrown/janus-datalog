# The Badger emulator was the type named `MemoryStore`, and "the memory store" in the open API named the tree store

**Status**: Resolved.

Both backends answered correctly. The defect was a name that sent a reader
to the wrong type. A sentence saying "memory store" was read as
`MemoryStore`, and on every path a caller reaches by default it meant
`MemoryTreeStore`.

## The two types

`BadgerEmulatorStore` (`datalog/storage`, in `memory_store.go`) is the
Badger emulator. It is an ordered key store in memory whose keys are the
Badger backend's encoded keys. It exists so the backend contract runs
against Badger's key layout without a disk. Its roster name is
`badger-emulator`.

`MemoryTreeStore` is the typed datom-tree backend. It is the in-process
backend the engine opens by default. `db.OpenMemory` opens it.
`defaultBackendName` (`datalog/storage/backends_wasm.go`) names it under
js/wasm. `setDumpDestination` (`datalog/db`) lands a dump in it when no
backend is named. Its roster name is `memory-trees`.

## What the names were

The emulator was `MemoryStore`, constructed by `NewMemoryStore`, under the
roster name `memory`, and its closed error read "memory store closed". Its
doc said it was an "ordered, transactional, in-memory implementation of
Store" that "persists the same binary keys as BadgerStore", and did not say
that it emulated Badger; the sentence that said so sat on
`MemoryTreeStore`'s doc.

`db.Open`'s doc said "the memory store under js/wasm, where there is no
filesystem to hold it", and "A dump loads into the backend WithBackend
names, defaulting to the memory store". Both sentences meant
`MemoryTreeStore`. `DatabaseOptions.Path`'s doc and the constructor's
comment in `database.go` said the same of the wasm default, and
`storeVersion`'s doc called the tree store "the memory store".

## What exists

- The type is `BadgerEmulatorStore`, its constructor `NewBadgerEmulatorStore`,
  its closed error `errBadgerEmulatorStoreClosed` reading "badger emulator
  store closed", and its roster entry `badger-emulator`.
- Its doc states its role: the Badger emulator, holding in memory the
  binary keys Badger writes, so the backend contract runs against Badger's
  key layout in a process with no disk; and that `MemoryTreeStore` is the
  in-process backend the engine opens by default. `MemoryTreeStore`'s doc
  states the complement.
- `db.Open`'s doc names the tree store, `storage.MemoryTreeStore`, as the
  wasm default and as the backend a dump loads into when none is named.
  `DatabaseOptions.Path`'s doc, the constructor's comment and
  `storeVersion`'s doc name it the same way.
- A search of the tree's Go files and documents for `MemoryStore` and
  `NewMemoryStore` finds them in no Go file and in no live design document.
  They remain in:
  - the dated records under `docs/perf`, `docs/reviews` and `docs/archive`,
    and the sentences of `docs/perf`'s index that summarize those records;
  - the resolved records under `docs/bugs/resolved` written before the
    rename, which describe the emulator under the names it carried then;
  - `CLAUDE.md`'s quotation of a stale `go doc` report;
  - this record, whose title states the former names and whose quotations
    above repeat the former docs.
- A search for the roster string `memory` finds it in no Go file and in no
  document other than the dated measurement under `docs/perf` whose arms
  carry it, the index's pointer to that arm, and this record.
- A search for the phrase "the memory store" finds it in no Go file and in
  no document other than one dated review under `docs/reviews` and this
  record's title and quotations.

## Tests

- `expectedBackendNames` in `backend_cases_native_test.go` and
  `backend_cases_wasm_test.go` pins the roster: `badger-emulator`,
  `memory-trees`, and natively `badger`.
- `byteKeyBackends` in `optimizer_modes_test.go` and `forkingModes` in
  `database_fork_test.go` classify the backends by roster name and fail on
  one they do not name, so a roster entry that changes name fails there
  until it is classified.
