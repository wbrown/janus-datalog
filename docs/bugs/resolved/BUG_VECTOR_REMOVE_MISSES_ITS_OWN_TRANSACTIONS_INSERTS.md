# A vector write or read answers a vector the transaction or the view does not hold

**Status**: Resolved (2026-11-05, b5ca9d7 and 9f80575). The Starflight
disassembler (`disasm`) met the `Remove` defect below on janus-datalog v0.16.1.
It records a Forth machine's stacks as vectors and reads them back as of each
step, and it worked around the defect by committing before such a pop
(`factWriter.remove`). The record covers four defects, which the tests under
"Reproducer" reproduce:

1. `Transaction.Remove`, `Transaction.Set` and an ordered set's `Add` each
   read the vector without some of the transaction's own earlier writes to
   it.
2. On a handle from `Database.AsOf`, `GetVectorLength`, `GetVectorNth` and
   `WarmCache` resolve through a matcher of the latest view.
3. Three arms of the pattern matcher answer no vector for an emptied one.
4. `loadRGAElements` and `ResolveRGAFromDatoms` place a withdrawn element at
   HEAD when they read its withdrawal before its insert, so the readers
   through them answer an order the vector's datoms do not give.

`Transaction.Remove` on a cardinality-vector attribute tombstones "the most
recently added element matching value". It finds that element by scanning the
committed store, and of the open transaction it consults the pending
tombstones but not the pending inserts. When the most recently added element
of the value was added earlier in the same transaction, Remove does not see
it. It then either writes no tombstone at all, or tombstones an older,
committed element of the same value. Both are silent: Remove returns nil.

## The route

The vector arm of `Transaction.Remove` (`datalog/storage/database.go`):

1. It collects `pendingTombstones`, the `AfterRef` of every `OpRGATombstone`
   in `t.datoms` for this (E, A), so that "multiple Remove() calls in the same
   transaction ... work correctly".
2. It opens an EAVT scan of `t.db.store` bound to [E A V]. Tx descends, so a
   tombstone is seen before the insert it deletes.
3. The first `OpRGAInsert` whose Tx is in neither the scanned tombstones nor
   `pendingTombstones` is the target.
4. With no target, "Remove is a no-op (not an error)".

`t.datoms` is read for tombstones only. An `OpRGAInsert` that `Add` appended
to `t.datoms` (the vector arm of `Transaction.Add`) is never a candidate, so:

- If no committed insert of the value is live, there is no target and no
  tombstone. The element added in this transaction stays.
- If an older committed insert of the value is live, it is the target. The
  element added in this transaction stays, and the older one goes.

`Add` reads no storage and chains each element after the last one added in
the transaction (`lastVectorElement`). In the cases observed below, the
pending insert is the element of the value with the highest ElementID, and
the scan never looks there.

## What was observed

Observed on `db.OpenMemory` with the schema's vector attribute `:run/stack`
(int64), through `Add` and `Remove` on one transaction, then `PullInto`
after `Commit`:

| Writes | Answer | Expected |
|---|---|---|
| tx1: Add 5, Add 7, Add 5, Remove 5, Add 9 | `[5 7 5 9]` | `[5 7 9]` |
| tx1: Add 5 (committed); tx2: Add 7, Add 5, Remove 5, Add 9 | `[7 5 9]` | `[5 7 9]` |
| tx1: Add 5, Add 6 (committed); tx2: Add 7, Add 8, Remove 8, Add 9 | `[5 6 7 8 9]` | `[5 6 7 9]` |

The third row also shows that `Add` after a commit is right: 7 lands after the
committed 6, not at the head. Only the Remove is wrong.

With a Remove of a value whose last Add was committed in an earlier
transaction, every row answers as expected, which is the case the existing
tests cover.

In the application, a stack written one push (`Add`) and pop (`Remove`) per
operation, many operations to a transaction, grew to hundreds of cells where
the machine's stack held a handful: nearly every pop was of a cell pushed in
the same batch.

## Contracts it contradicts

- `Transaction.Remove`'s doc: "Cardinality-Vector: RGA LIFO tombstone; removes
  most recently added matching element". The most recently added matching
  element is the pending one.
- The design note in the vector arm and on `TestVectorRemoveMostRecent`
  (`crdt_vector_test.go`): "CRDT-friendly: 'most recent' = 'the one I most
  recently added'".
- `Transaction.Commit`'s per-operation Lamport: "datoms already have their Tx
  set by Add/Remove/Set". Each operation in a transaction is ordered, and a
  Remove after an Add in that order is answered as if the Add had not
  happened.
- The vector arm's own handling of the transaction: it reads `t.datoms` for
  the tombstones so that several Removes in one transaction compose. Inserts
  in the same transaction do not compose with them.

## Reach

- `Remove`: any caller that adds and removes the same vector value in one
  transaction, such as a stack, an undo log or a work queue.
  `TestVectorRemoveMostRecent` and `TestVectorRemoveNonExistent` commit their
  `Add`s before the `Remove`.
- `Set` and `SaveStruct`: any transaction that writes a vector's whole value
  after another write to the same vector.
- An ordered set's `Add`: any transaction that adds a value it removed, or
  set away, earlier.
- For each writer the answer is wrong at every read afterward, on the latest
  view, as of the commit and through `PullInto`, since the datoms themselves
  are wrong. A tombstone is missing or names the wrong element, or an insert
  is missing or repeated.
- The as-of accessors: any reader of an as-of handle whose cache
  `GetVectorLength`, `GetVectorNth` or `WarmCache` filled, and those
  accessors on a cold cache.
- The emptied vector: any pattern `[?e :a ?v]`, its value unbound, whose
  entity is an input, bound by a join or unbound.
- The withdrawn element: any vector two writers add to at once, once an
  element that another element follows is withdrawn.

## Set, SaveStruct and an ordered set's Add

The transaction's other vector writers each read the vector without some of
its earlier writes, by a rule of its own.

- The vector arm of `Transaction.Set` diffs the new slice against the
  committed vector, read through `Cache.GetOrResolve` or `ResolveEntry`. It
  applies none of the transaction's pending writes. The cardinality-many arm
  applies them: "This ensures Set() sees Add/Remove ops from earlier in the
  same transaction".
- `SaveStruct` writes a vector field through `Transaction.Set`
  (`StructWriter.updateSliceField` and `StructWriter.updateOrderedSetField`,
  `datalog/reflect`), so it diffs against the same vector.
- An ordered set's `Add` writes nothing for a value its uniqueness check
  finds. The check finds any pending insert of the value, whether or not a
  pending tombstone names it. Failing that, it finds the value in the
  committed vector, whether or not a pending tombstone names its element.

Each row is one transaction over a committed vector:

| Committed | Writes | Answer | Expected |
|---|---|---|---|
| `[a]` | `Add b`, `Set [a b c]` | `[a b c b]` | `[a b c]` |
| `[a]` | `Add b`, `SaveStruct [a b c]` | `[a b c b]` | `[a b c]` |
| `[a]` | `Set [a b]`, `Set [a c]` | `[a b c]` | `[a c]` |
| `[a b]` | `Remove b`, `Set [a b]` | `[a]` | `[a b]` |
| `[a]` | `Set [a b]`, `Remove b` | `[a b]` | `[a]` |
| ordered set `[a]` | `Remove a`, `Add a` | `[]` | `[a]` |
| ordered set `[a b]` | `Set [b]`, `Add a` | `[b]` | `[b a]` |

## An as-of handle's vector accessors and WarmCache

`Database.AsOf` gives the handle a cache of its own (`cache: NewCache()`),
whatever the parent's `DisableCache`. `PatternMatcher.cacheKey` keys it by
(E, A) as the latest cache is keyed, and states the design: the two "live in
separate Cache instances ... so the key never crosses snapshots".

- `Database.GetVectorLength` and `Database.GetVectorNth` resolve through
  `NewPatternMatcher(d.store)`, a matcher of the latest view, into
  `d.cache`. On an as-of handle `d.cache` is the handle's cache.
- `Database.WarmCache` fills `d.cache` through the same latest matcher.
- So on an as-of handle each leaves the latest vector in the handle's cache,
  and the accessors answer it.
- The handle's cache-first readers then answer the latest vector from that
  entry: `LookupAttribute`, `ResolveEntityAttributes`, `Pull`, `PullMany`,
  `PullInto`, a query's pull, get-else, and a query naming the entity as a
  constant.
- Where the handle's own readers fill the entry first, the accessors read
  the as-of vector from it. An absent vector is never cached, so as of a
  commit before the vector's first write the accessors answer the latest
  vector whatever came before.

Observed on a vector holding `[a b c]` as of one commit and `[a c]` after the
next. As of the first commit, after `Cache().Clear()` and a read through the
accessors, or after `WarmCache`, the readers above answer `[a c]`.

`Database.GetString`, `Database.GetStrings` and the other typed accessors
read through the handle's own query path (`QueryOneInto`, `QueryInto`).
`Database.LookupAttribute` reads through `d.Matcher()`, which carries the
handle's mode and target (`Database.matcherWithExecOptions`).

## An emptied vector at the pattern arms

A vector whose every element is withdrawn holds the empty vector, and the
empty vector is a value:

- `VectorResolutionResult.Present`: "the first has no value, the second has
  the empty vector".
- `PatternMatcher.ResolveRGA`: "An all-tombstoned vector resolves to the
  empty vector, which is a value and not an absence."
- `PatternMatcher.LookupAttribute`: it answers a vector "once the vector has
  been written, an emptied vector included".

The pattern matcher's vector arms answer it by two rules:

- `matchCardinalityVectorAsRelation`, for a pattern whose entity is given,
  binds the empty vector.
- `matchVectorWithBindings`, for an entity a binding carries, and
  `vectorScanAllEntitiesIterator.Next`, for an unbound entity, skip it when
  the pattern's value is unbound ("Unbound V: skip empty vectors").

Observed after a commit that withdraws a vector's last element, and after an
empty `Set`: the queries naming the entity as an input, binding it by a join
and leaving it unbound answer no vector. The query naming the entity as a
constant, the query whose attribute is a variable, and every API reader
answer `[]`.

`[?e :a []]` matches the emptied vector (`TestVectorLiteralMatch`, "string/
empty literal matches only empty vector"). A pattern that holds with the
constant `[]` at its value position holds with a variable there bound to
`[]`, so the skip contradicts that match. Three subtests of
`TestVectorLiteralMatch` assert the skip: "string/unbound V returns all
vectors", "int64/unbound V returns all non-empty" and "keyword/unbound V
returns all non-empty". Each binds the entity by a join.

## A withdrawn element read before its insert

`ReconstructRGA` and `ReconstructRGAWithIDs` place each element after the
element its insert names. A withdrawn element keeps its place, so the
elements that follow it stay reachable. The readers first gather a vector's
elements from its datoms, and they gather a withdrawn element by two rules:

- `PatternMatcher.loadRGAElements` and `ResolveRGAFromDatoms` make a
  placeholder for a withdrawal read before the insert it names. The
  placeholder follows the zero ElementID, HEAD ("Unknown until we see
  insert"). The insert read after it is passed over ("If existing has
  tombstone, keep the tombstoned version"), so the element stays at HEAD.
- `CRDTResolvingIterator.resolveRGAGroup` merges the insert into that
  placeholder ("Merge value/afterRef if we have it"), so the element follows
  the element its insert names.

A withdrawal is written after the insert it names, so its ElementID is the
higher of the two. EATV reads a vector's datoms highest ElementID first, so
`loadRGAElements` reads every withdrawal before its insert.

Observed on one database, with two transactions open at once:

1. T1 adds a, then b following a, then c following b.
2. T2 adds x at HEAD, drawing an ElementID between a's and b's.
3. Both commit, and every reader answers `[a b c x]`.
4. A third transaction removes b.

b keeps its place after a, so c comes before x, and the vector is
`[a c x]`. On both optimizer paths, with the cache on and off, the query
whose attribute is a variable answers `[a c x]`. Every other reader answers
`[a x c]`, the order that b's placement at HEAD gives: b after x, and c
following b.

## The fix

- A transaction writes against a vector as it stood when the transaction
  began, with its own writes applied. `NewTransaction` records its start,
  and its writers read the store as of it.
- Every element a transaction writes is drawn after it began, so it lands
  after its anchor's whole subtree, and everything after that subtree is
  already withdrawn in its view. So the vector it holds for (E, A) is the
  vector as of its start, less the elements its own withdrawals name,
  followed by its own inserts in the order it wrote them, less those its
  withdrawals name. `Set` diffs against it.
- `Remove` withdraws the live element of its value with the highest
  ElementID among the elements the transaction holds. Its own newest live
  element of the value, when it has one, outranks every element of the
  start. Otherwise the value's run on EAVT as of the start decides it,
  since a withdrawal carries the value of the element it names. An ordered
  set's `Add` writes nothing when a live element holds its value.
- An element another transaction draws after a transaction began is not in
  its view, so the merge keeps both writers' elements, as two replicas'
  writes are kept. One it drew before is in the view, whenever it is
  committed.
- `loadRGAElements`, `ResolveRGAFromDatoms` and `CRDTResolvingIterator`
  gather a vector's elements from its datoms by one rule,
  `vectorElements.gather`. An insert gives its element's value and the
  element it follows, and a withdrawal marks the element, whichever of the
  two is read first.
- `GetVectorLength`, `GetVectorNth` and `WarmCache` resolve through the
  handle's own matcher, `d.Matcher()`. An as-of handle then resolves as of
  its target, and a history handle reads past the cache, as
  `LookupAttribute` does.
- `matchVectorWithBindings` and `vectorScanAllEntitiesIterator.Next` bind an
  emptied vector as `matchCardinalityVectorAsRelation` does, and skip only a
  vector no write reached. The three subtests of `TestVectorLiteralMatch`
  named above then answer the emptied vector's entity too.

## Reproducer

- `TestVectorRemoveSeesItsOwnTransactionsInserts`
  (`datalog/storage/crdt_vector_test.go`) writes the three cases under "What
  was observed" and reads them through `PullInto`. On the unmodified writers
  it answers `[5 7 5 9]`, `[7 5 9]` and `[5 6 7 8 9]`.
- `TestVectorWritesReadBackAlikeThroughEveryReader`
  (`datalog/storage/vector_writes_test.go`) writes named scenarios of `Add`,
  `Remove`, `Set` and `SaveStruct` to a plain vector and an ordered set.
  After each commit, and as of each commit, it reads the vector through
  queries, pulls, the entity resolvers, `LookupAttribute` and the vector
  accessors. It reads with the cache on and off, and again after clearing
  and after warming the cache, on both optimizer paths. A reference model of
  the vector computes each scenario's stated vectors.
- `TestVectorWritesOnRandomRunsReadBackAsTheReference`
  (`datalog/storage/vector_writes_random_test.go`) writes generated
  transactions to two attributes of one entity and one of a second entity,
  and reads every vector back as the matrix does, against the reference
  model.
- `TestVectorConcurrentTransactionsReadBackAlikeThroughEveryReader`
  (`datalog/storage/vector_writes_test.go`) writes one vector from
  transactions open at once and reads it back as the matrix does, against
  the vectors the RGA rule resolves each case's datoms to. It writes the four
  steps under "A withdrawn element read before its insert". It also writes
  a `Set`, and a `SaveStruct`, of `[x y]` by a transaction that added `y`
  before another transaction added `x` and committed. On the unmodified
  writers every reader answers `[y x y]` there, where the vector is
  `[x x y]`: the transaction that sets `[x y]` began before the other drew
  `x`. Its other cases write a `Remove`, a `Set` and an ordered set's `Add`
  by a transaction that began before another drew the value they touch.

## Related

- `TestVectorRemoveMostRecent` (`crdt_vector_test.go`) is the LIFO contract
  across transactions, which holds.
- `TestAsOfVectorResolution` (`asof_vector_test.go`) reads as-of vectors
  through `LookupAttribute` on the handle's own matcher and through
  `PullInto`. It reads through neither the vector accessors nor `WarmCache`.
- `docs/bugs/resolved/BUG_EMPTY_VECTOR_LITERAL_MATCHES_NONEMPTY.md` records
  the fix that made the literal `[]` match only an emptied vector. The skip
  at `matchVectorWithBindings` and `vectorScanAllEntitiesIterator.Next` came
  in with that fix (a48e83d).
