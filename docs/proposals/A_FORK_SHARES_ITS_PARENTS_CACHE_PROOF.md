# A fork shares its parent's cache, as a proof

## Sources

- The code:
  - `datalog/storage/hashtriemap.go`: `cacheTrie` and its `Load`, `LoadOrStore`, `Swap`, `CompareAndSwap`, `LoadAndDelete`, `CompareAndDelete`, `find`, `expand`, `Range` and `Clear`; `cacheTrieIndirect`; `cacheTrieEntry` and its `swap`, `compareAndSwap`, `loadAndDelete` and `compareAndDelete`.
  - `datalog/storage/cache.go`: `Cache`, `CacheEntry`, `cacheSlot`, `GetOrResolve`, `storeIfNotInFlight`, `BeginInFlight`, `UpdateMaxVersion`, `Invalidate`, `InvalidateRewind`, `InvalidateAttribute`, `Clear`, `TrackEntityAttr`, `GetCachedAttrs`, `PopulateFromDatoms`.
  - `datalog/storage/database.go`: `Database.Fork`, `Database.Close`, `NewDatabaseWithOptions`, `Database.ResolveEntityAttributes`, `Database.Assert`.
  - `datalog/reflect/writer.go`: the struct writer's cardinality-one write, which skips a value equal to the one stored.
- The design: `docs/proposals/BRANCHING_AND_SNAPSHOTS.md`.
  - §8.2 names one cache per branch as the naive mapping, N nearly identical resolved views. That is what `Fork` builds.
  - §8.3 and §8.6 recommend a per-branch overlay over a shared base, with memory in the branch's divergence and fork in constant time.
  - §9.5 and §10.9 state that an ancestor's view at a fork point is immutable and shared by every branch forked there.
  - §9.6 and §12.3 make a memory-tree branch a copy-on-write version of its parent's store as of the snapshot's point.
- The reference: `docs/reference/REFLECT.md`, "Updating Structs", the struct writer's update semantics for each cardinality.
- The open record: `docs/bugs/BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES.md`, the import path that writes under a dump's ElementIDs and notifies no cache.
- The failure: a downstream build store keeps a line of forks. Each fork is taken from the previous fork's snapshot, and the build re-saves its round into it through `SaveStruct`.
  - On a chain of ten kept forks re-saving an unchanged round, each fork holds 31.0 MB of live heap with the EA cache on and 0.59 MB with it off.
  - The forks' JDZL dumps grow 1.4 KB per re-saved round.
  - So the forks share their datoms, and each fork's cache holds its own copy of the line's resolved views.
  - The line runs about 160 forks deep, and each parent is closed while its child lives on.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A database's **cache** is its `Cache`: `slots`, a `cacheTrie` from `CacheKey` to `cacheSlot`, and `entityAttrs`.
- A **slot** is a key's `cacheSlot`: an entry and a version. It is **fresh** when its entry's version equals its version, as `GetOrResolve` reads it.
- A **node** is an internal node (`cacheTrieIndirect`) or an entry node (`cacheTrieEntry`).
- A **fork** F of a parent D at **point** P is the branch `Fork` answers. P is the ceiling `Fork` passes to the store, the point F holds D's state as of.
- A node is **owned** by the cache that made it. A node another cache made is **shared** with it.
- The **line** of F is D, D's parent, and so on up to a database that is no fork.
- F's **cutoff** for an ancestor A on its line is the least fork point on the path from A down to F.

## 2. Preconditions

- **P1 (reads).** `Load` walks from the root by atomic pointer loads and takes no lock.
- **P2 (writes).** `LoadOrStore`, `Swap`, `CompareAndSwap`, `LoadAndDelete` and `CompareAndDelete` lock the mutex of the internal node that holds the slot.
  - Under the lock they re-check the slot and the node's `dead` flag, and start over when either changed.
  - They publish with one atomic store into the node's child slot.
  - `expand` builds a subtree of new internal nodes and publishes it with one store.
- **P3 (entries).** An entry node's key and slot are not written after it is made. `Swap` and `CompareAndSwap` publish a new entry node.
  - Overflow chains are the exception. `swap`, `compareAndSwap`, `loadAndDelete` and `compareAndDelete` store into an existing entry's `overflow` pointer when the key is not the chain's head.
- **P4 (deletes).** `LoadAndDelete` and `CompareAndDelete` remove an emptied internal node through its `parent` pointer and mark it `dead`.
  - `InvalidateRewind` and `InvalidateAttribute` delete.
  - `Invalidate`, `BeginInFlight`, `UpdateMaxVersion` and `storeIfNotInFlight` swap.
- **P5 (versions).** A slot's version is the high-water mark of the ElementIDs written for its key.
  - `UpdateMaxVersion` raises it on every written datom and never lowers it.
  - `Invalidate` and `BeginInFlight` keep it.
  - `InvalidateRewind` deletes the slot.
- **P6 (the bound).** `GetOrResolve` serves a fresh entry to a bounded reader only when the slot's version lies within the bound. Otherwise it resolves through the reader's resolver.
  - It rests on a premise: a datom committed after a point carries an ElementID past it.
  - `Database.Assert` and every transaction stamp datoms from the database's clock through `Transaction.Add`.
  - In-process import breaks the premise. `Import` and `ImportBinary` write through `Store.Assert` under the dump's ElementIDs and notify no cache (`BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES.md`).
- **P7 (the fork).** `Fork` builds the branch through `NewDatabaseWithOptions` over the store the parent's store forks at P. So the branch starts with an empty cache.
  - The branch writes under a fresh ReplicaID, with a clock started past every ElementID it holds.
  - Neither database sees what the other commits afterward.
- **P8 (close).** `Close` rolls back the active transactions and closes the store. It does not touch the cache.
  - `Clear` replaces the trie's root and writes no existing node.
- **P9 (`entityAttrs`).** `entityAttrs` has one production reader, `ResolveEntityAttributes`.
  - Its two arms call `GetOrResolve` with the same arguments for each attribute, so the record decides nothing.
  - Its doc names a full-scan arm for many missing attributes, which the code does not have.
- **P10 (the struct writer).** `SaveStruct` writes nothing for a stored state it would not change.
  - A cardinality-one field equal to the stored value is skipped.
  - A cardinality-many field writes the difference.
  - A vector writes past the common prefix.
  - So re-saving an unchanged round reads every field and writes no datom.

## 3. Theorems

**T1 (the answers).** A read through a fork's cache answers what the same read answers with the cache off: the fork's resolution of the key at the time of the read.

**T2 (sharing).** A fork holds no copy of what it inherits. Its cache holds the nodes on the paths to the keys it writes or resolves itself.

**T3 (an unforked database).** A database that is no fork reads and writes its cache as it does today, with one owner comparison per level on the write path and one per hit.

**T4 (depth).** A lookup is one walk from the reader's root, at any depth of the line.

**T5 (close).** Closing a database leaves every node its forks reach readable.

**T6 (locks).** No database takes a lock on a node another database owns.

## 4. Lemmas

### L1. A slot F reaches through D's node answers F's view under D's cutoff (T1)

Let F reach a slot for key K through a node D owns.

- **F has not written K.** F's first write to K goes into a node F owns (L2). After that, F's path to K runs through F's node, so a later read of K never reaches D's node. A delete in F empties F's node, and the read misses.
- **A fresh slot with version v at most P.** Every datom D holds for K is at or below v, since v is the high-water mark (P5). Every datom at or below P is one D held at P, by P6's premise. So D's datoms for K are D's datoms for K at P, which are F's datoms for K (P7). The entry resolves them, so it is F's view.
- **A version past P.** D wrote K after P, or a commit in D holds K's in-flight sentinel. F resolves K itself and never waits on D's commit.
- **No entry.** The slot is absent or invalidated, and F resolves K itself.

So F serves a slot from D's node by P6's rule with P as the bound. The same holds for a slot owned by an ancestor A further up the line, with F's cutoff for A as the bound. Take a chain C, F1 at P1, F2 at P2: a write C made after P1 can carry a clock below P2, since the two clocks run independently after the fork. F2 bounds C's slots by the least of P1 and P2. ∎

### L2. A fork writes only into nodes it owns (T2, T6)

A write in F that reaches a node F does not own copies it first.

- **The copy.** The copy is an internal node owned by F. Its 16 children are loaded atomically from the shared node.
- **Publishing.** The copy is published into the slot of F's parent node, under that node's mutex, which is P2's lock. At F's root it is published by a compare-and-swap on the root pointer.
- **The re-check.** Under the lock, the writer checks that the slot still points to the shared node. If it does not, the writer starts over, as P2's protocol does.
- **Order.** Copies are made from the root down, so an F-owned node's parent is F-owned.
- **Once per node.** Each shared node is copied at most once per fork. After the copy is published, F's path reaches the copy.
- **Concurrent writes in the owner.** The owner may write the shared node while F copies it. Each child F loads is a state the owner published (P1). L1 decides each key on its own, so the copy needs no consistency across children.
- **Entries.** F writes a key by publishing a new entry node into its own internal node (P3). In an overflow chain F does not own, F copies the chain up to the entry it changes. It never stores into a shared entry's `overflow` pointer.
- **Deletes.** F copies the path before it deletes, so P4's collapse climbs only through F-owned `parent` pointers and never into another database's nodes.

So F takes no lock on a node it does not own, and holds only the paths it has written. ∎

### L3. A database's own nodes are as they are (T3)

D publishes only into nodes D owns and into D's own root. No fork publishes into D's nodes (L2).

- So D's walk never meets a node a fork made.
- A database that is no fork owns every node it reaches.
- Its write path differs from today's by one owner comparison per level, which always holds.
- Its read path differs by one owner comparison per hit, which always holds.

∎

### L4. A lookup is one walk (T4)

F's trie has one root. The nodes F shares with D may themselves be shared with D's parent, and the walk through them is one walk. A hit costs the walk and, for a slot F does not own, one lookup of F's cutoff for the slot's owner. The cutoffs are a map from ancestor to point, built at `Fork` from the parent's map and P. ∎

### L5. Close leaves shared nodes readable (T5)

`Close` does not touch the cache, and `Clear` writes no existing node (P8). A node a fork reaches stays reachable from the fork's root, so it is not collected. A closed database writes nothing, so its nodes no longer change. ∎

### L6. `entityAttrs` goes (T2)

`entityAttrs` decides nothing (P9). Under sharing it would need its own copy-on-write or a per-fork rebuild. With it gone, `ResolveEntityAttributes` calls `GetOrResolve` for each attribute, as both of its arms do today. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestFork_ReadsItsParentsResolvedViews` (storage, new). A fork reads keys the parent resolved before the fork and resolves none of them, as a counting resolver shows. Its answers equal the cache-off answers (T1, T2).
- `TestFork_NeverServesAWriteItsParentMadeAfterThePoint` (storage, new). After the fork, the parent writes a key the fork read before. The fork reads the value at P, and the parent reads its own (T1, L1).
- `TestFork_BoundsAnAncestorsSlotByTheLeastFork` (storage, new). In a chain C, F1 at P1, F2 at P2, C writes a key after P1 under a clock below P2. F2 reads the value at P1 (L1).
- `TestFork_ServesItsOwnWrites` (storage, new). A fork writes a key and reads its own value. The parent's slot for the key is unchanged (T1, L2, L3).
- `TestFork_ChangesNoSharedOverflowChain` (storage, new). Two keys share a routing value. A fork changes one, and the parent's chain still answers both of its keys (L2).
- `TestFork_DeletesOnlyItsOwnPaths` (storage, new). A fork's `InvalidateAttribute` and its rewind leave the parent's slots answering (L2).
- `TestFork_ReadsItsParentsCacheAfterTheParentCloses` (storage, new). A fork's reads after its parent closes equal the cache-off answers (T5).
- `TestFork_AChainAnswersWithTheCacheOff` (storage, new). A chain of forks, with parents and forks writing and reading concurrently, answers every read as the same chain with the cache off does (T1, T6).
- `BenchmarkForkChainCache` (storage, new). A chain of kept forks re-saving an unchanged round reports live heap per fork (T2).
- `BenchmarkGetOrResolve_FreshHit`, `BenchmarkCacheResolutionOverhead`, `BenchmarkCachePathWithBindings` and `BenchmarkEACacheBypass` (storage), run before and after on memory-trees. An unforked database's hit and fill paths do not move (T3).
- The cache, matcher and fork tests of the storage package and its optimizer-mode differentials, run unchanged.

## 6. The change the proof licenses

- `datalog/storage/hashtriemap.go`:
  - `cacheTrieIndirect` and `cacheTrieEntry` carry the cache that owns them.
  - `Load` answers the owner of the entry node it reaches.
  - The write paths copy a node the trie does not own before writing it, publishing the copy under the parent node's mutex or by a compare-and-swap on the root.
  - An overflow chain the trie does not own is copied up to the entry changed.
  - A trie can be made from another trie's root.
- `datalog/storage/cache.go`:
  - `Cache` carries its cutoffs, a map from ancestor cache to point.
  - `GetOrResolve` serves a slot owned by another cache only when it is fresh and its version lies within the cutoff for its owner, and resolves otherwise.
  - `entityAttrs`, `TrackEntityAttr` and `GetCachedAttrs` go.
- `datalog/storage/database.go`:
  - `Fork` makes the branch's cache from the parent's root, with the parent's cutoffs and P, when the parent has a cache.
  - `ResolveEntityAttributes` calls `GetOrResolve` for each attribute, and its doc states the one arm.
- The tests of §5.

## 7. What the proof does not cover

- An AsOf handle's private cache, which `CacheKey`'s doc describes. The same sharing could serve it, and this change does not touch it.
- A parent that keeps writing after its forks. Its forks' inherited slots fail their cutoffs more often, and each fork's cache drifts toward a private one.
- The memory of the cutoff maps: one entry per ancestor per fork.
- In-process import into a database that has forks. It breaks P6's premise for the session bound and for this change alike (P6). Its fix is the single write boundary that `BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES.md` names, specified in `TRANSACTION_ENVELOPES.md`.

## 8. Alternatives excluded

- **Layered caches,** a branch's overlay over its parent's cache consulted on a miss, as `BRANCHING_AND_SNAPSHOTS.md` §8.3 and §10.9 lay out. Its memory is the branch's divergence, as here.
  - A view stays in the layer of the database that first resolved it.
  - In a line 160 deep, almost every hit walks about 160 layers.
  - Sharing the trie's nodes keeps §8's memory bound with one walk per hit (T4).
- **Copying the parent's cache at `Fork`.** That is the failure, §8.2's naive mapping: each fork holds a copy of the line's views.
- **Copy-on-write on both sides,** the parent also copying after a fork. The parent pays a path copy on its writes after every fork. The cutoff is still needed, since P can precede the fork.
- **Merging a closed parent's layer into its child.** A parent with several children would be copied into each.
- **A cutoff read from the replica of the slot's version.** A slot's version names the replica of its newest datom, not the cache that resolved it, so it cannot say which fork point bounds the slot.
- **Bounding the cache's size.** It lowers the memory, and every evicted view is resolved again.
