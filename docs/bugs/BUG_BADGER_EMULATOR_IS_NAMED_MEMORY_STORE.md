# The Badger emulator is the type named `MemoryStore`, and "the memory store" in the open API names the tree store

**Status**: Open.

Both backends answer correctly. The defect is a name that sends a reader to
the wrong type. A sentence saying "memory store" is read as `MemoryStore`,
and on every path a caller reaches by default it means `MemoryTreeStore`.

## The two types

`MemoryStore` (`datalog/storage`) is the Badger emulator. It is an ordered
key store in memory whose keys are the Badger backend's encoded keys. It
exists so the backend contract runs against a second implementation without
a disk. Its own doc says it is an "ordered, transactional, in-memory
implementation of Store" that "persists the same binary keys as
BadgerStore", and does not say that it emulates Badger. The sentence that
says so, and that the contract runs against both, sits on
`MemoryTreeStore`'s doc.

`MemoryTreeStore` is the typed datom-tree backend. It is the in-process
backend the engine opens by default. `db.OpenMemory` opens it.
`defaultBackendName` (`datalog/storage/backends_wasm.go`) names it under
js/wasm. `setDumpDestination` (`datalog/db`) lands a dump in it when no
backend is named.

The roster names are `memory` for the emulator and `memory-trees` for the
tree store.

## The sentences that send a reader to the wrong type

`db.Open`'s doc says "the memory store under js/wasm, where there is no
filesystem to hold it", and "A dump loads into the backend WithBackend
names, defaulting to the memory store". Both sentences mean
`MemoryTreeStore`. A reader who looks up "memory store" finds
`MemoryStore`, the emulator.

## The fix

Name the emulator for its role: `BadgerEmulatorStore`, under the roster
name `badger-emulator`. State that role on its own doc, and the complement
on the tree store's doc. Have the open API name a backend by its type or
its roster name, never as "the memory store".

The alternative is to delete the emulator. `MemoryTreeStore` is then the
one in-process backend, and the name question goes away with the emulator.
The cost is the second implementation the backend contract runs against.
Every matrix test that runs on `memory`, `memory-trees` and `badger` loses
the case that catches a tree-store defect by disagreement with an
implementation that shares none of its code. The maintainer decides between
the two.
