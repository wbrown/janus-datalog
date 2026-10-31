# The resolving iterator keeps its set state across groups, as a proof

## Sources

- The code: `datalog/storage/crdt_resolving_iterator.go` (`CRDTResolvingIterator`, `Next`, `startNewGroup`, `processAddWins`, `accumulateRGA`, `resolveRGAGroup`, `Seek`), `datalog/storage/hash_join_matcher.go` (`matchWithHashJoin`, `hashJoinIterator`), `datalog/storage/matcher_relations.go` (`matchCardinalityManyScanAllEntities`).
- The runtime: `internal/runtime/maps/map.go` in the Go distribution (`Map`, `NewMap`, `growToSmall`, `Clear`, `clearSmall`), the implementation of the builtin map.
- The failure: the allocation profile of the reproduction of `docs/bugs/JANUS_REPRO_FILLED.md`, its `unfilled` query through the CLI. The hash-join arm over `:cell/next` seeks one key at a time, and the resolving iterator resolves one group per seek. At every group start it makes two maps, and the group's first emitted value allocates the first map's slot group. Those three allocations per group are the profile's largest share on the storage side. `PERFORMANCE_STATUS.md` carries the figures.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **I** is a `CRDTResolvingIterator`, one per scan, reading its source's datoms in index order.
- A **group** is a maximal run of consecutive datoms of I's source, after the view's as-of filter, sharing an entity and an attribute.
- The **many state** of a group is `emitted`, the values the group has emitted so far, and `tombstones`, each removed value's highest remove Lamport seen so far.
- A **small map** is a Go map that has held at most eight entries, `abi.MapGroupSlots`, since it was made. It is one group of eight slots with no directory.
- **smallGroupValues** is the constant the change introduces, eight, the entries a small map holds.

## 2. Preconditions

- **P1 (the many state is a group's own).** Under cardinality many, `processAddWins` is the one reader and writer of `emitted` and `tombstones`, and it reads and writes them for the current group's datoms alone. `startNewGroup` runs at every group boundary before the group's first datom is processed, and makes both maps anew. `Seek` sets both to nil and `hasGroup` to false, so the datom after a seek starts a group. So at a group's first datom both maps hold nothing. Within a group nothing is deleted from either map, so each map's entry count rises through the group and stands, at the group's end, at the group's distinct emitted values and its distinct removed values.
- **P2 (the vector arm keeps its state's capacity).** `startNewGroup` resets `rgaElements` by reslicing it to length zero, keeping its capacity. The group's datoms accumulate into it and `resolveRGAGroup` reads it at the group's end.
- **P3 (a make allocates no slot group, and a first assignment allocates one).** `NewMap` with no hint returns without allocating a group. The first assignment into a map with no group allocates one group of eight slots (`growToSmall`).
- **P4 (a clear keeps the capacity and costs the capacity).** `Map.Clear` on a small map clears its one group in place and keeps it (`clearSmall`). On a map with a directory it clears every table and keeps the directory and the tables. On a map holding nothing it returns at once. So a map's capacity survives a clear, and a clear's cost is a walk of that capacity.
- **P5 (a map that never held more than eight entries is small).** The small-map case holds while the map has always contained at most `abi.MapGroupSlots` entries. A map grown past that holds a directory from then on.
- **P6 (the keys).** The maps are keyed by a value's hashable form, a byte slice by its string and every other value by itself. Nothing here changes that.
- **P7 (the driver on the failure).** On the reproduction the resolving iterator reads the hash-join arm's per-key seeks over `:cell/next`, one group per key, each group a cell's successors.

## 3. Theorems

**T1 (resolution unchanged).** Every group emits the datoms it emitted before, in the same order.

**T2 (allocation).** After the first group under cardinality many, a group whose two maps each held at most `smallGroupValues` entries at the previous such group's end allocates no map. While its distinct emitted values stay within eight, its emits allocate no slot group.

**T3 (a retained map is small).** Every map I keeps across a group boundary is small, so its clear is one slot group's clear.

## 4. Lemmas

### L1. Each group resolves from its own datoms (T1)

At a group's first datom the maps hold nothing: a map made anew holds nothing, and by P4 a cleared map holds nothing. By P1 the group's reads see only entries the group wrote. So the group's decisions, emit or skip per datom, are the same function of the group's datoms as before, and the emitted datoms and their order are unchanged. After a seek `hasGroup` is false, so the next datom's `startNewGroup` empties the maps before any read, whether or not the seek left them. ∎

### L2. The allocations (T2)

Under the change `startNewGroup` makes a map when it is nil or holds more than `smallGroupValues` entries, and clears it otherwise. By P4 a cleared small map keeps its slot group, so the next group's first emit assigns into that group and allocates none, where P3's allocation is for a map with no group. So a group after a small-mapped group allocates no map and no slot group while it stays within eight distinct values. ∎

### L3. The retained maps are small (T3)

A map I keeps across a boundary held at most `smallGroupValues` entries at the end of the group that last wrote it. By P1 a map's entries only rise within a group, so it held at most eight at every moment of that group, and by P5 it is small. By P4 its clear is `clearSmall`, one group's clear. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestCRDTResolvingIterator_ResolvesEachGroupFromItsOwnDatoms` (storage, new): over a cardinality-many attribute read by one scan, a group whose value was removed, then a group holding that value, then a group holding a value an earlier group emitted, each emit their own members: the removed value is emitted for the entity that holds it, and the shared value for every entity that holds it. A group of more than `smallGroupValues` members stands before a group of one member, and the latter emits its one member (T1, T3).
- `TestCRDTResolvingIterator_KeepsItsSetStateAcrossGroups` (storage, new): one scan over many entities each holding one member allocates no more bytes than one scan over one entity, within an allowance smaller than one group's two maps and slot group (T2).
- The cardinality-many tests of `datalog/storage` (`crdt_many_test.go`, `cardinality_many_set_bytes_test.go`), the V-validation reporting tests, and the two optimizer-mode differentials, run unchanged (T1).

## 6. The change the proof licenses

- `crdt_resolving_iterator.go`, `startNewGroup`: under cardinality many, clears `emitted` and `tombstones` when each is not nil and holds at most `smallGroupValues` entries, and makes it otherwise.
- `crdt_resolving_iterator.go`, `smallGroupValues`: the constant, with its reason.
- `crdt_resolving_iterator.go`, `Seek`: leaves the two maps for the next group start to empty, and its doc says so.
- The tests of §5.

## 7. What the proof does not cover

- `uniqueRemovedAt`, the key map a unique cardinality-one group makes.
- `rgaElements`, which `Seek` sets to nil.
- The per-key seek's own allocations, a cursor per key.
- The cache's resolution of an entry, which reads through other code.

## 8. Alternatives excluded

- Two maps made per group, the state before the change. Three allocations per group, whatever the group's size.
- Clearing without the bound. A clear costs the map's capacity, so a scan of many small groups after one large group pays that group's capacity at every group.
- A small inline set before the map. It is a second representation of the group's membership, with an equality of its own to keep in step with the map's.
- Making the maps at a group's second datom. The first emit still allocates a slot group per group.
