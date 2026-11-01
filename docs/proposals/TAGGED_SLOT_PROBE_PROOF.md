# A tag before a key, and one hash per add, as a proof

## Sources

- The code: `datalog/executor/tuple_set.go` (`TupleSet`, `Find`, `FindAt`, `Probe`, `Add`, `grow`, `place`, `slot`), `datalog/executor/join_index.go` (`JoinIndex.Add`, `JoinIndex.Probe`), `datalog/executor/tuple_key.go` (`TupleKeyMap.Get`, `Put`, `PutIfAbsent`, `hold`), and the readers that find a key and add a member for it: `datalog/executor/iterator_composition.go` (`DedupIterator.Next`), `datalog/executor/relation.go` (`deduplicateTuples`), `datalog/executor/relation_ops.go` (`getUniqueCombinations`, `unionRelations`), `datalog/executor/rules.go` (`derivedSet.absorb`), `datalog/executor/or_fallback_relation.go` (`OrFallbackIterator.answer`, `recordAnswered`, `unanswered`), `datalog/executor/join.go` (`SemiJoin`, `AntiJoin`), `datalog/executor/window_frame.go` (`windowFrame.windowRun`), `datalog/executor/window_reader.go` (`windowReader.reportWindow`).
- The equality and the hash: `datalog.ValuesEqual` and `datalog.HashValue`, and `TestValuesEqualIsAnEquivalenceAndEqualValuesHashAlike` (`datalog`), which pins that equal values hash alike.
- The prior proofs: `docs/proposals/FRAME_RUN_RECORD_PROOF.md`, which derives the table, and `docs/proposals/ONE_TUPLE_TABLE_PROOF.md`, which makes it the one table under the index and the map and takes a slot from the hash's product with the golden ratio's fraction.
- The decision: a walk compares a member's key only at a slot whose tag agrees with the probing key's tag, and an add after a find hashes the key once, the find answering the hash it computed and the add taking it.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **key** of a tuple t at positions P is the list of t's values at P, in P's order.
- Two keys are **E-equal** when they have the same length and are `ValuesEqual` position by position.
- The **hash** of a key is `hashTuplePositions` over its tuple at its positions.
- The **mix** of a hash is its product with the golden ratio's fraction, the sixty-four-bit constant `slot` multiplies by.
- A table's **width** is the count of bits that name a slot, the logarithm of its length.
- The **slot** of a key is the top width bits of its mix.
- The **tag** of a key is the eight bits of its mix below the slot's bits, with the lowest of them set. A tag is never zero.
- The **table** holds, per slot, a tag and a member's index. A slot whose tag is zero is empty.
- A **walk** for a key is the slots from the key's slot on, in order, to the first empty slot.
- A **find** is `Find` or `FindAt`: a walk that answers the index of a member whose key is E-equal to the probing key, and the probing key's hash.
- An **add** is `Add`: a member and its key's hash, the member placed at the first empty slot of its key's walk.

## 2. Preconditions

- **P1 (the equality).** Two keys are one key exactly when they are E-equal, and E-equal keys hash alike.
- **P2 (the table as it stands).** The table holds one member per distinct key, probed linearly from the key's slot. A find compares the member's key by value at every occupied slot its walk visits, and answers the member whose key is E-equal to the probing key, or none at the first empty slot. An add hashes the member's key at the stored positions and places the member at the first empty slot of its walk. A doubling hashes every member again and places it in the new table (`FRAME_RUN_RECORD_PROOF.md`, T5; `ONE_TUPLE_TABLE_PROOF.md`, P2, L1).
- **P3 (the readers find first).** Every reader of the tree that adds a member finds first, with a probing tuple whose key at the probing positions is E-equal to the member's key at the stored positions. The dedup iterator, the dedup constructor, the derived set, the fallback's deduplication, the frame's run and the window reader's distinct count find the tuple they add, or a copy of it. The unique combinations and the union find the source tuple at the symbols' positions and add the tuple built from those values at the positions 0 to k−1. The fallback's answered keys find at the branch's positions and add the tuple held at them. The semi-join and anti-join find the right tuple at its join positions and add it, or a copy of its key's values at the positions 0 to k−1. The index finds the tuple at the stored positions and adds it. The map finds the member at the stored positions and holds it. A test that adds without a find hashes the member itself.
- **P4 (the index's and the map's readers).** `JoinIndex.Add`, `JoinIndex.Probe`, `TupleKeyMap.Get`, `Put` and `PutIfAbsent` keep their signatures, and their readers in the executor and storage packages read nothing of the table.
- **P5 (the width).** A table's width is at most fifty-six bits, so the tag's eight bits lie below the slot's bits.

## 3. Theorems

**T1 (answers unchanged).** Every find answers the member it answered before, or none as before.

**T2 (compares).** A walk compares a member's key only at a slot whose tag equals the probing key's tag. A member whose key's tag differs from the probing key's is never read.

**T3 (one hash per add).** A find and the add that follows it hash the key once, in the find. An add with no find before it hashes the key once, in its caller.

**T4 (allocation).** A probe allocates nothing. An add allocates the table's doubling, one byte per slot beside the index's four, and nothing per member. A capacity hint allocates nothing before the first add.

## 4. Lemmas

### L1. A tag that differs marks a key that differs (T1, T2)

The tag of a key is a function of the key's hash and the table's width. By P1 E-equal keys hash alike, so E-equal keys have equal tags at one width. A slot whose tag differs from the probing key's holds a member whose key is not E-equal to it, and a find that reads no such member drops no answer. A slot whose tag agrees holds a member whose key is compared by value, as P2 compares every occupied slot's, so the member answered is the member P2 answered. ∎

### L2. The walk ends where it ended (T1)

A slot is empty under the tag exactly when no member was placed in it: a placed member's tag is never zero (its lowest bit is set), and a slot no member was placed in holds the zero a made slice holds. So a walk under the tag visits the slots P2's walk visited and ends at the slot it ended at, with the member found or none. ∎

### L3. The hash a find answers is the member's (T3)

By P3 the probing key and the member's key are E-equal, so by P1 the hash the find computed at the probing positions is the hash an add would compute for the member at the stored positions. The add takes it and computes none. A test that adds with no find hashes the member at the stored positions, the one hash. ∎

### L4. A doubling re-tags (T1)

The tag's bits are read below the slot's, and the slot's bits grow with the width. A doubling hashes every member again at its stored positions, as P2 states, and places it with the slot and the tag the new width gives, so every member's tag in the new table is its key's tag at that width. ∎

### L5. The empty marker costs no fill (T4)

A made slice of tags is zero, zero marks an empty slot, and a member's tag is never zero. So a doubling fills nothing, and the table's index slice is read only at a slot whose tag agrees, which a placed member filled. ∎

### L6. A walk's compares (T2)

Under a hash that spreads, two keys at one slot agree in their tags one time in a hundred and twenty-eight, the count of tags with the lowest bit set. So a walk over n occupied slots compares at the slot of the member it finds, and at one in a hundred and twenty-eight of the others, where P2 compared at every one. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestTupleSet_AProbeComparesNoMemberWhoseTagDiffers` (executor, new): a set holds members whose keys take the probing key's slot and carry tags other than its own. A probe of the absent key allocates what its hash allocates and nothing more. A key compare of two typed-vector keys boxes each element it compares, and a hash of one boxes each element, so the compares a walk makes are read in the probe's allocations beyond the hash's (T2, L1, L6). Red before the change: the probe allocates two boxings per member walked.
- `TestTupleSet_FindAnswersTheKeysHashAndAddHoldsTheMemberUnderIt` (executor, new): a find of an absent key answers the hash `hashTuplePositions` computes at the probing positions, and a member added under that hash is found by a probe at the set's positions and at positions given per probe (T3, L3).
- `TestTupleSet_FindAnswersTheMembersIndexAtThePositionsGiven`, `TestTupleSet_HoldsOneMemberPerKeyProbedByThePositionsGiven`, `TestTupleSet_TwoKeysUnderOneHashAreToldApart`, `TestTupleSet_ProbeAllocatesNothing`, `TestTupleSet_AMemberIsHeldAsGiven`, `TestTupleSet_AddAllocatesOnlyItsGrowth` and `TestTupleSet_KeysAgreeingInTheirLowHashBitsSpreadOverTheTable` (executor), their adds taking the hash their finds answer, asserting what they asserted (T1, T4).
- `TestTupleSet_WithAnUnknownCapacityStartsEmpty`, `TestTupleSet_UnifiesIntegerWidths`, `TestAJoinOfTwoSetsAnswersEachPairOnce`, `TestExecuteWithRelations_SeededEqualsWhole` and `requireSamePages` (executor), the same (T1).
- `TestJoinIndex_ACapacityHintAllocatesNothingBeforeTheFirstAdd` (executor), its bound on the first add's build raised from three allocations to four, the struct, the tags, the table and the members, the tags being a slice beside the table (T4). Withdrawn from the bullet below, which named it as run unchanged.
- The index's, the map's and the readers' own tests, run unchanged (T1, P4): the other join index tests, the tuple key map tests, the hash join's set and property tests, the semi-join and anti-join tests, `TestDedupIterator`, the set-semantics tests, the aggregation tests, the symmetric join tests, the fallback and union tests, the rules tests, the window and frame tests, and in storage the binding-driven match tests, the per-key seek tests, the unique-resolution tests and the two optimizer-mode differentials.

## 6. The change the proof licenses

- `tuple_set.go`, `TupleSet`:
  - `tags []uint8` beside `table []int32`.
  - `Find(tuple)` and `FindAt(tuple, probePos)` answer the index, the hash and whether a member holds the key.
  - `Add(member, hash)` answers the index it took.
  - `Probe` is `Find`'s truth.
  - `grow` makes the tags zeroed and the table unfilled.
  - `place(idx, hash)` places a member under the hash given.
  - The slot and the tag are read from one mix.
- The readers of P3, each taking the hash from its find into its add.
- `join_index.go` and `tuple_key.go`: `Add`, `Probe`, `Get`, `Put`, `PutIfAbsent` and `hold` through that surface, their own signatures unchanged.
- The tests of §5.

## 7. What the proof does not cover

- The cost of the tag read against the key compare it replaces, and of the hash an add no longer computes. The join and dedup benchmarks and the reproduction measure them.
- The cost of `ValuesEqual`'s domain check on its second operand, `typeRank`, paid at every compare the walk still makes. It is the `datalog` package's and this change leaves it.
- The order of a set's members, which no reader reads.

## 8. Alternatives excluded

- The member's hash kept beside it, eight bytes per member, which `FRAME_RUN_RECORD_PROOF.md` §8 excludes for its memory. The tag is one byte per slot, two per member at the table's load.
- A find that answers the slot its walk ended at, for the add to place into. The slot holds until the next add or doubling, a handle the caller must not keep across either. The hash holds for the set's life.
- The tag read from the mix's low bits. The low bits of a product depend on the low bits of its factors alone, and an interned pointer's address repeats its low bits at the allocator's stride. The bits below the slot's depend on every lower bit of the hash.
- The tag and the index in one struct per slot, eight bytes under alignment, with the index's bytes read at every slot of a walk. Two slices read the tags alone until a tag agrees.
- A compare counted through a hook in the equality, for the test. The equality is a package function with no seam, and the boxing a typed vector's compare makes is a count the runtime already keeps.
