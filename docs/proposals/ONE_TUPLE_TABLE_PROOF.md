# One table under the set, the index and the map, as a proof

## Sources

- The code: `datalog/executor/tuple_set.go` (`TupleSet`), `datalog/executor/join_index.go` (`JoinIndex`, `tupleKeysMatch`), `datalog/executor/tuple_key.go` (`TupleKey`, `TupleKeyMap`, `hashTuplePositions`), and their readers: `datalog/executor/join.go` (`HashJoinWithOptions`, `hashJoinIterator`, `SemiJoin`, `AntiJoin`), `datalog/executor/kept_branch.go` (`cachedBranch`), `datalog/executor/window_frame.go` (`windowFrame`), `datalog/executor/window_reader.go` (`reportWindow`), `datalog/executor/iterator_composition.go` (`DedupIterator`), `datalog/executor/relation.go` (`deduplicateTuples`), `datalog/executor/relation_ops.go` (`getUniqueCombinations`, `unionRelations`), `datalog/executor/rules.go` (`derivedSet`), `datalog/executor/or_fallback_relation.go` (`OrFallbackIterator`, `answeredKeys`), `datalog/executor/aggregation.go` (the batch and streaming groupings), `datalog/executor/symmetric_hash_join.go` (`symmetricHashJoinIterator`), `datalog/storage/hash_join_matcher.go` (`buildHashSet`, `hashJoinIterator`), `datalog/storage/unique_resolve.go` (`uniqueWalkState`), `datalog/storage/crdt_resolving_iterator.go` (`startNewGroup`).
- The equality: `datalog.ValuesEqual` and `datalog.HashValue`, and `TestValuesEqualIsAnEquivalenceAndEqualValuesHashAlike` (`datalog`), which pins that equal values hash alike.
- The prior proofs: `docs/proposals/FRAME_RUN_RECORD_PROOF.md`, which derives the set and its layout, and `docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md`, whose P3 states the index's probe.
- The decision: the three keyed structures become one table. The set, the index and the map hash a key at positions by `hashTuplePositions` and compare it by `ValuesEqual` at positions, and differ in the table that maps a hash to a key, a Go map with a chain, an open-addressed slice, a Go map with an overflow, and in what each holds beside a key.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **key** of a tuple t at positions P is the list of t's values at P, in P's order.
- Two keys are **E-equal** when they have the same length and are `ValuesEqual` position by position.
- The **table** is `TupleSet`: members held in place, each member's key read at the stored positions S, a probing tuple's key read at positions Q given per probe or fixed at construction, an open-addressed slice of member indices, a power of two in length and at most half full, probed linearly from the key's hash's slot. A member is held as the caller gave it.
- The **index** is `JoinIndex`: the table over the first tuple of each key's group, with a group of tuples per member, in added order.
- The **map** is `TupleKeyMap`: the table over the key tuples the callers own, with one value per member.
- A **capacity hint** is the count a constructor is given. A hint sizes the table and the members at the first add, and sizes nothing before it.
- A **whole-tuple key** is the key at every position of a tuple of width w, the positions 0 to w−1.

## 2. Preconditions

- **P1 (the equality).** Every keyed structure in the tree decides whether two keys are one key by `ValuesEqual` position by position, and hashes a key by `HashValue` per value folded in position order. Equal values hash alike. So a key is found in any of them exactly when a held key is E-equal to it, whatever table maps the hash to the key.
- **P2 (the table).** `TupleSet` holds one member per distinct key, tells two keys under one hash apart by their values, allocates nothing on a probe, holds a member as given, and places every member again at each doubling (`FRAME_RUN_RECORD_PROOF.md`, T5, and the tests of its §5).
- **P3 (the index's readers).** `hashJoinIterator` probes the index per probe tuple and reads the group it answers as a slice it does not write. `cachedBranch` adds a window's branch tuples and probes per window tuple. The symmetric join's two tables and the storage matcher's hash set hold a group of tuples per key and append to it, and the storage matcher reads the distinct keys in the order the binding relation first yields each.
- **P4 (the map's readers).** The dedup iterator, the dedup constructor, the derived set, the unique combinations, the union, and the fallback's deduplication key a whole tuple and store a value nothing reads. The fallback's answered keys store nothing read and are probed at positions known at the probe. The semi-join and anti-join key the right side at its join positions, store nothing read, and probe the left side at its join positions. The window reader's distinct count keys a window at the key positions and reads nothing stored. The groupings key a tuple at the group positions and read a group per key. The unique walk keys a bare value and reads a transaction id per key.
- **P5 (owned keys).** The map's constructors copy a key's values into a slice the map owns. Its readers that hold the tuple by reference, the dedup iterator under `copyTuples`, the derived set and the groupings, hand the map that same owned slice, so the slice is both the key and what the reader keeps. Its readers whose tuples come from a reusing source, the semi-join's and anti-join's right side, and the unique walk's datom, keep nothing of the tuple beyond the key.
- **P6 (the index's groups).** `JoinIndex.Add` allocates a one-tuple slice for every new key, and appends to it for a key held. Its probe answers the group's slice.
- **P7 (the index's sizing).** `NewJoinIndex` allocates its groups and its map at construction, sized to the hint. A build side whose size is unknown is given the default hint, and a build side that turns out empty leaves the allocation unused.
- **P8 (no key position).** An index with no key position groups every tuple under one key, and every probe answers the whole group. `hashTuplePositions` over no position is the fold's seed, and `tupleKeysMatch` over no position holds.

## 3. Theorems

**T1 (answers unchanged).** Every query answers the tuples it answered before. Each reader of a set, an index or a map holds, finds and reads what it held, found and read before.

**T2 (the three contracts).** The set holds one member per distinct key. The index holds every added tuple in its key's group, in added order, and reports whether every key groups one tuple. The map holds one value per key, the last put, and a put-if-absent keeps the first and reports the key held.

**T3 (allocation).** A probe of any of the three allocates nothing. An add to the set allocates the table's doubling and nothing per member. An add to the index allocates a group slice only for a key's second tuple, a key of one tuple being answered from the members themselves. An add to the map allocates its value's slot growth and nothing per key beyond the member the caller gives. A capacity hint allocates nothing before the first add.

**T4 (the whole-tuple key).** A reader keying a whole tuple of width w holds the tuple as its member under the positions 0 to w−1, and the member is the tuple the reader keeps.

## 4. Lemmas

### L1. Membership is the equality's (T1)

By P1 every structure in the tree answers a probe by E-equality of keys. By P2 the table answers a probe exactly when a member's key is E-equal to the probing tuple's at the given positions. So a probe answers in the table exactly when it answered in the Go map with a chain or the Go map with an overflow, since each of those compared keys by `ValuesEqual` after the hash. The index over the table answers the group of the member found, which is the group the key's first tuple heads, and the map answers the value at the member found. ∎

### L2. Each reader keeps what it kept (T1, T4)

By P4 and P5 the readers that key a whole tuple keep the tuple they key, so the tuple is the member under positions 0 to w−1, and the reader's answers read the members it kept. The semi-join and anti-join under a reusing right side keep a copy of the key's values under positions 0 to k−1, as the map owned before, and under a stable right side hold the right tuple itself at its join positions. The unique walk keeps a one-tuple holding the value and probes with a one-tuple it fills. The groupings keep the owned key tuple under positions 0 to k−1, which is the group's key as before. The storage matcher's hash set holds each binding tuple at its key position and probes with a one-tuple it fills from the datom. So every reader finds under the table what it found under the map. ∎

### L3. The index's groups and their order (T2)

A key's first tuple is the member. A second tuple of the key allocates the group's slice over the member and the second tuple, and later tuples append. A probe of a key of one tuple answers the member as a slice of one over the members themselves, capped so that an append cannot write into the next member. A probe of a key of more answers the group. Each group holds its tuples in added order, and the index reports fanout when any key took a second tuple. ∎

### L4. The hint (T3)

The table built at the first add is sized to the hint's next power of two at or above twice the hint, and the members to half of it, so no add up to the hint doubles. Before the first add neither exists. An index whose build is empty therefore allocates nothing beyond its own struct. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestTupleSet_HoldsOneMemberPerKeyProbedByThePositionsGiven`, `TestTupleSet_TwoKeysUnderOneHashAreToldApart`, `TestTupleSet_ProbeAllocatesNothing`, `TestTupleSet_AMemberIsHeldAsGiven` and `TestTupleSet_AddAllocatesOnlyItsGrowth` (executor), run unchanged (T2, T3).
- `TestJoinIndex_ACapacityHintAllocatesNothingBeforeTheFirstAdd` (executor, new): an index given a hint allocates nothing at construction, and adds up to the hint allocate nothing beyond the first add's table and members (T3, L4).
- `TestJoinIndex_GroupsTuplesByTheirKeyInAddedOrder`, `TestJoinIndex_ProbesByTheProbingRelationsPositions`, `TestJoinIndex_WithNoKeyEveryProbeAnswersEveryTuple`, `TestJoinIndex_TwoKeysUnderOneHashAreToldApart`, `TestJoinIndex_AKeyAddedAgainJoinsItsGroup`, `TestJoinIndex_KeysUniqueIsTheCandidateKeyProof`, `TestJoinIndex_ProbeAllocatesNothing` and `TestJoinIndex_MatchesAnIdentityByValue` (executor), run unchanged (T2).
- `TestJoinIndex_AKeyOfOneTupleAllocatesNoGroup` (executor, new): adding distinct keys allocates nothing per key beyond the table's growth, and a probe of such a key answers a slice of one whose capacity is one (T3).
- `TestJoinIndex_GroupsAreSizedToTheKeysHeldNotTheHint` (executor, new): an index hinted for many tuples over one key allocates, at the key's second tuple, the group and the groups for the keys held, and nothing sized to the hint (T3).
- `TestTupleSet_KeysAgreeingInTheirLowHashBitsSpreadOverTheTable` (executor, new): members whose hashes agree in every low bit, integers that are multiples of a power of two, sit within a bounded run of slots from their home, so a probe reads a bounded run (P2, T3).
- `TestTupleKeyMap_HoldsOneValuePerKeyProbedByThePositionsGiven` (executor, new): a value put under a key is answered by a probe at the probing positions, a later put replaces it, a put-if-absent keeps the first and reports the key held, and a key absent answers nothing (T2).
- `TestTupleKeyMap_TwoKeysUnderOneHashAreToldApart` (executor, new): two keys under one hash hold two values (T2).
- `TestSymmetricHashJoin_AKeyHeldTwiceOnEitherSideAnswersEachPair` (executor, new): a key held by two tuples of the left side and by two of the right answers one joined tuple per pair, each holding that pair's values, with the join symbol at different positions on the two sides, so each side's index is probed at the other side's positions (T1, P3, L3).
- `TestSemiAndAntiJoinsReadKeysFromAWorkspaceReusingRightInput` (executor, new): a right side that hands back one reused slice per tuple, with the join symbol at a different position from the left's, still answers the semi-join's matches and the anti-join's misses by each key's values, which a member aliasing the reused slice would lose (T1, L2).
- The equality tests the map carried, rewritten over the table: an integer of one width finds a member of another, a time finds a member at the same instant in another location, and a vector of equal elements finds a member, each a case of `ValuesEqual` (T1, L1).
- The readers' own tests, run unchanged (T1): the hash join's set and property tests, `TestSemiAndAntiJoinsPreserveLeftProperties` and `TestSemiAndAntiJoinsCopyWorkspaceReusingLeftInput`, `TestDedupIterator`, the set-semantics tests, the aggregation tests, the symmetric join tests, the fallback and union tests, the rules tests, the window and frame tests, and in storage the binding-driven match tests, the per-key seek tests, the unique-resolution tests and the two optimizer-mode differentials.

## 6. The change the proof licenses

- `tuple_set.go`, `TupleSet`: `NewTupleSet(probePos, storedPos, capacity)` with the hint; `Find(tuple)` and `FindAt(tuple, probePos)` answering the member's index; `Add(member)` answering the index it took; `Probe` as `Find`'s truth; `Len`; the table built at the first add.
- `join_index.go`, `JoinIndex`: the table over each key's first tuple, with `groups` beside the members, nil for a key of one tuple; `Add(tuple)` answering whether the key is new; `Probe` answering the group or the member as a slice of one; `KeysUnique` and `Len` as before.
- `tuple_key.go`, `TupleKeyMap`: the table over the callers' key tuples, with `values` beside the members; `NewTupleKeyMap(probePos, storedPos, capacity)`; `Get(tuple)`, `Put(member, value)`, `PutIfAbsent(member, value)`, `Len`. `TupleKey`, `NewTupleKey`, `NewTupleKeyFull`, `NewTupleKeyMapWithCapacity`, `PutValue`, `GetValue`, `Exists`, `GetPositions`, `PutIfAbsentPositions`, `mapEntry`, `tupleValuesEqual` and `tupleValuesEqualPositions` go.
- `allPositions(w)`: the positions 0 to w−1, a prefix of one shared slice for a width within the slice's length, and a slice of its own for a width past it. The query package's `SymbolIndexTable` and `SymbolPositions` answer a relation's positions by named symbol and allocate a table per call, and a reader keying a whole tuple holds no symbol list at the key, so no component answers a width's own positions without allocating.
- The readers of P3 and P4, each constructing the shape P4 and L2 name for it.
- The tests of §5.

## 7. What the proof does not cover

- The cost of the open-addressed table against the Go map on the hash join's probe path. The reproduction's profile and the join benchmarks measure it, and their figures belong in `PERFORMANCE_STATUS.md`.
- The order of a map's members, which no reader reads.
- The resolving iterator's many state, two Go maps keyed by a value, which `RESOLVING_ITERATOR_GROUP_STATE_PROOF.md` governs.

## 8. Alternatives excluded

- One struct carrying every payload, with nil for the payloads an instance lacks. A probe for a group on a set answers nothing and a put on an index answers nothing, each a method that is wrong on most instances and says so to no one.
- A payload type parameter over the table. Its instantiations' inlining is unmeasured, and the two payloads are two fields.
- A group boxed as the map's value, the symmetric join's shape. The boxing allocates per key what the index holds unboxed.
- The Go map with a chain as the one table, the former index. A map slot and a chain link per key beside the member, where the table is four bytes per slot at most half full.
- The map's owned key slices kept beside held members. A second copy of every key a reader already holds.
