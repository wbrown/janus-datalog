# A join's output is a set, as a proof

## Sources

- The code: `datalog/executor/join.go` (`HashJoinWithOptions`, `hashJoinIterator.Next`, `combineTuplesIndexed`, `SemiJoin`, `AntiJoin`, `materializeFilteredLeft`), `datalog/executor/symmetric_hash_join.go` (`SymmetricHashJoinWithOptions`, `symmetricHashJoinIterator.processLeftBatch`, `symmetricHashJoinIterator.processRightBatch`, `symmetricHashJoinIterator.combineTuples`), `datalog/executor/join_index.go` (`JoinIndex.Add`, `JoinIndex.Probe`, `tupleKeysMatch`), `datalog/executor/tuple_key.go` (`NewTupleKey`, `NewTupleKeyFull`, `TupleKeyMap.PutIfAbsent`, `TupleKeyMap.Get`, `tupleValuesEqual`), `datalog/executor/relation_properties.go` (`RelationProperties`, `joinProperties`, `hasKeyWithin`, `appendDistinctKeys`, `deduplicatedProperties`, `projectionPreservesSet`), `datalog/executor/relation.go` (the `Relation` interface, `NewMaterializedRelation`, `NewMaterializedRelationWithOptions`, `NewMaterializedRelationWithProperties`, `newMaterializedRelationFromSet`, `NewMaterializedRelationFromSet`, `deduplicateTuples`, `NewStreamingRelation`, `sliceIterator`), `datalog/executor/rules.go` (`derivedSet.absorb`, `derivedSet.relation`, `ruleEvaluation.runStratum`, `seedRelation`, `executeRuleInvocation`), `datalog/storage` (`DefaultPlannerOptions`).
- The fixtures that violate the set contract, each stated in place as a deliberate violation: `BenchmarkHashJoinIdentityDuplicates` (`datalog/executor/hash_join_identity_bench_test.go`), `TestSemiAndAntiJoinsDeduplicateUnkeyedLeftInput` (`datalog/executor/relation_properties_test.go`), and the `DuplicateHandling` case of `TestSymmetricHashJoin` (`datalog/executor/symmetric_hash_join_test.go`), which reaches a join through `NewStreamingRelation` over `newMockIterator` (`datalog/executor/iterator_composition_test.go`).
- The design: `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`: the law "A Relation is a set", its line "Natural join and Cartesian product of sets produce sets", the "Not required" list under "Deduplication Rules", the candidate-key consumers, and the mandatory gates.
- The failure: `docs/bugs/JANUS_REPRO_FILLED.md`. Its `unfilled` query over the kernel's facts runs for minutes, and the record names what shows the fix: that query answering in the same order of time as its `reach` query over the same cells. A CPU profile of the run (the CLI's `-cpuprofile`, read with `go tool pprof -list HashJoinWithOptions`) places most of the hash join's samples on the `seen` map's `PutIfAbsent` at the materialized arm's emit, and the rest on the combine and the probe.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **E** is `datalog.ValuesEqual`, the equality on values.
- Two tuples are **equal** when they have one length and are E at every position: `tupleValuesEqual`.
- A **relation** is a `Relation`: its symbols and its tuples. It is a **set** when no two of its tuples are equal. That is the criterion `deduplicateTuples` applies, and the law the record states.
- A **key** of a relation S is a symbol list K such that two tuples of S that are E at every position of K are equal: `RelationProperties.Keys`, "candidate symbol sets whose values uniquely identify a tuple".
- The **join** of L and R on J, J a list of symbols both carry, is the set of pairs (l, r), l in L and r in R, with l and r E at every position of J. The **combined tuple** of a pair is l whole, followed by the values of r at R's positions outside J, in R's order: `combineTuplesIndexed` with the left tuple first, and `combineTuples`.
- **seen** is the `TupleKeyMap` an arm keys every combined tuple into through `NewTupleKeyFull` and `PutIfAbsent`, appending the tuple when `PutIfAbsent` reports it absent.

## 2. Preconditions

- **P1 (the set contract).** Every `Relation` is a set: the `Relation` interface's note ("Relations are IMMUTABLE and DEDUPLICATED at creation") and the record's law. `NewMaterializedRelation`, `NewMaterializedRelationWithOptions` and `NewMaterializedRelationWithProperties` establish it through `deduplicateTuples`; `newMaterializedRelationFromSet` and `NewMaterializedRelationFromSet` take it as the caller's warrant. A set's iterator yields each of its tuples once. The three fixtures in Sources are the only inputs that break the contract, each built from a raw struct literal or from an iterator over a raw slice, and each says so in place.
- **P2 (one equality).** `deduplicateTuples`, `derivedSet.absorb` and every seen map key a tuple by `NewTupleKeyFull` and compare through `tupleValuesEqual`, which is E per position. `JoinIndex.Add` and `JoinIndex.Probe` compare keys through `tupleKeysMatch`, E per key position. The symmetric arm's tables key by `NewTupleKey` and compare through `tupleValuesEqual` in `TupleKeyMap.Get`. So "equal" means the same thing at every site this proof reads.
- **P3 (the index groups by key).** `JoinIndex.Add` appends a tuple to the group whose first tuple's key equals its own, and opens a group when none does; `JoinIndex.Probe` answers the one group whose key equals the probing tuple's, and nil when none does. So a held tuple is in exactly one group, and `Probe(p)` is exactly the held tuples whose key equals p's, each once.
- **P4 (the combine).** `combineTuplesIndexed(first, second, idx, width)` copies `first` into the leading positions and writes `second[idx[k]]` after them, idx being the right side's positions outside J in order. In both hash arms `first` is the build tuple when the build side is the left and the probe tuple otherwise, so `first` is always the left tuple and `second` the right. `combineTuples` copies the left tuple and appends the right's values at its non-join positions in order.
- **P5 (the arms' enumeration).** The materialized arm pulls each probe tuple once from `probeRel.Iterator()`, and for each tuple of `Probe(p)` builds one combined tuple and appends it when seen reports it absent. The streaming arm, `hashJoinIterator.Next`, does the same as the consumer pulls. The symmetric arm processes batches: each left tuple probes the right table for the right tuples added so far, emits one combined tuple per match seen reports absent, and is then added to the left table under its key; each right tuple does the same against the left table and is added to the right table. A table's `Put` appends a tuple to its key's slice, so each table holds every processed tuple of its side, grouped by key.
- **P6 (the key derivation).** `joinProperties` keeps the left keys when some right key lies within J (`hasKeyWithin`), keeps the right keys when some left key lies within J, and derives nothing else. Each hash arm and the symmetric arm build seen exactly when the derived keys are empty.
- **P7 (the fixpoint's relations).** `derivedSet.relation`, the delta relations `runStratum` builds under each predicate's delta symbol, and `seedRelation` carry `deduplicatedProperties(symbols)`: one key over all the relation's symbols.
- **P8 (semi-join and anti-join).** `SemiJoin` and `AntiJoin` pull each left tuple once and keep it on a membership test of its key against the right side's keys. `materializeFilteredLeft` constructs the result through `newMaterializedRelationFromSet` when the left has a key, and through `NewMaterializedRelationWithProperties`, which runs `deduplicateTuples`, when it has none.
- **P9 (the default path).** `storage.DefaultPlannerOptions` sets `EnableStreamingJoins` and `EnableSymmetricHashJoin` false, so a database query joins through the materialized arm unless its caller opts in.

## 3. Theorems

**T1 (the join of two sets is a set).** For L and R sets, each arm emits exactly the combined tuples of the join of L and R on J, one per pair, and no two emitted tuples are equal.

**T2 (seen excludes nothing).** For L and R sets, `PutIfAbsent` on seen reports every combined tuple absent, in each arm. The arm with seen removed emits the same tuples in the same order.

**T3 (the composite key).** For K_L a key of L and K_R a key of R, the symbols of K_L followed by those of K_R not among them form a key of the join of L and R on J. The two rules P6 states are a different fact, a key of one side alone, and stand beside it.

**T4 (what changes).** The tuples `HashJoinWithOptions`, `SymmetricHashJoinWithOptions`, `SemiJoin` and `AntiJoin` answer are unchanged for every pair of sets. A join's `Properties()` gains the composite keys T3 names, and every key it reports today it reports still. No arm holds a seen map or a `seen` field, and no caller reads one.

**T5 (semi-join and anti-join).** For L a set, the tuples `SemiJoin` and `AntiJoin` keep form a set, and the `deduplicateTuples` pass `materializeFilteredLeft` runs for a keyless left removes nothing.

**T6 (cost).** Per combined tuple, the materialized and streaming arms stop computing a hash of the whole tuple and probing or inserting a map entry; the symmetric arm the same; and no arm allocates the map or grows it with the result. No call is added on any path, so the compiler's inlining report has nothing to decide; the benchmarks in §5 measure the change, and their figures are recorded in `PERFORMANCE_STATUS.md`.

## 4. Lemmas

### L1. A combined tuple determines its pair (T1)

Let t be the combined tuple of (l, r), with w the width of L. By P4, t at positions 0 to w is l, and t after w is r at R's positions outside J, in order. r's values at the positions of J are E to l's at J's positions: P3's `tupleKeysMatch` for the hash arms, and the table's key equality for the symmetric arm (P2). So the tuple read out of t, l's values at R's join positions and t's suffix at R's other positions, is equal to r, and in a set R equality names one member (P1, P2). Two pairs that differ in l or in r therefore have unequal combined tuples. ∎

### L2. The hash arms enumerate each matching pair once (T1)

By P1 the probe iterator yields each probe tuple p once. By P3, `Probe(p)` is exactly the build tuples whose key equals p's, each once. So each pair with a matching key is enumerated once, as (p, b), and ordered into (l, r) by P4. A pair whose keys differ is never enumerated. ∎

### L3. The symmetric arm enumerates each matching pair once (T1)

Each tuple of a side is processed once (P1, P5). Take l in L and r in R with equal keys. If l is processed before r, then when r is processed l is in the left table, so (l, r) is emitted then, and when l was processed r was not yet in the right table, so (l, r) was not emitted then. If r is processed before l, the same with the sides exchanged. Both tables hold a processed tuple once, under its key, so no probe answers a tuple twice. So each matching pair is emitted once, and a pair whose keys differ is never emitted. ∎

T1 follows: by L2 or L3 the emitted tuples are the combined tuples of the join's pairs, one each, and by L1 no two are equal. T2 follows from T1 and P2: `PutIfAbsent` reports a tuple present only when an earlier key compares equal through `tupleValuesEqual`, and no earlier emitted tuple is equal to it, so every emitted tuple is appended whether or not seen exists.

### L4. The composite key (T3)

Let t and t' be combined tuples of the pairs (l, r) and (l', r') that are E at every position of the composite. They are E at K_L's positions, which are among l's, so l and l' are equal, K_L being a key of L. They are E at K_R's positions, each read from t at R's non-join positions or, for a symbol of J, at L's position of that symbol, where the value is E to r's by L1; so r and r' are equal, K_R being a key of R. So the pairs are one pair and t and t' are one tuple. ∎

### L5. The reproduction's joins (T6)

Every relation a rule body reads for a predicate of its own stratum carries one key over all its symbols (P7). A body that joins two of them on a proper subset of either's symbols, as the bodies of `through`, `clear` and `unfilled` in the reproduction do, presents a left key and a right key neither of which lies within J, so P6 derives no key and the arm builds seen (P6), paying T6's cost for every combined tuple of every iteration. Under T3 the same join's result carries the composite of the two keys, every symbol of the result. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, mapped to the record's mandatory gates.

- Gate 1, the semantic differential: `TestAJoinOfTwoSetsAnswersEachPairOnce` (executor, new): two set relations whose join symbol repeats on both sides with a distinct payload per tuple, joined in the materialized arm, the streaming arm and the symmetric arm; the collected tuples are asserted equal, as a set, to the pairs a nested loop over the inputs computes with E at the join positions, each pair's combined tuple once and no tuple twice (T1, T2). The storage package's `TestQueriesAnswerAlikeUnderBothOptimizerModes` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` are the differentials over real queries and over rule evaluation, run unchanged.
- Gate 2, the shapes: the same test's cases for an empty side, one tuple on each side, a key repeated on one side alone, on both sides, and a large input (T1).
- Gate 3, iterator and close errors: the executor's iterator-contract tests of the join's build and probe errors, run unchanged; the change touches no error path.
- Gate 4, workspace reuse: a case of the new test whose probe side is a workspace-reusing stream (`newReusingWorkspaceStream`), asserting the same pair set (T1).
- Gate 5, keys: `TestJoinPropertiesDeriveTheCompositeOfEachSidesKeys` (executor, new, red first): a left key and a right key neither within J derive their composite; a left key within J keeps the right keys and adds no second copy of the composite they equal; a side with no key contributes no composite (T3). The existing `TestHashJoinPreservesLeftKeyWhenRightJoinSymbolsAreKey`, `TestHashJoinPreservesRightKeyWhenLeftJoinSymbolsAreKey`, `TestHashJoinDoesNotPreserveLeftKeyWhenRightJoinSymbolsAreNotKey`, `TestMaterializedAndSymmetricHashJoinsPreserveCandidateKeys` and `TestRelationPropertyPropagation` keep every key assertion, each of which T3 leaves as it is, and lose their assertions on the `seen` field (T4).
- Gate 6, the physical path: the new test reads the join's events through a handler and asserts that the `JoinProbe` event's result count equals the pair count in each hash arm and that the `JoinStrategy` event reports the symmetric arm for the symmetric case, so every pair was emitted and none rejected (T2).
- Gate 7, focused benchmarks on valid inputs: `BenchmarkHashJoinIdentityKeys`, `BenchmarkHashJoinIdentityHighFanout`, `BenchmarkHashJoinIdentityLargeResult`, `BenchmarkHashJoinMaterializedVsStreaming` and `BenchmarkSymmetricHashJoin`, before and after (T6). `BenchmarkHashJoinIdentityDuplicates` is deleted: the record rejects a benchmark that violates the set contract.
- Gate 8: `BenchmarkComplexQueryCheckpoint` (`datalog/storage/optimization_matrix_test.go`), before and after.
- Gate 9: `make test`, the executor package under `-race`, and the fixed-seed rules test above.
- The failure: the reproduction's `unfilled` query through the CLI, before and after.
- `TestSemiAndAntiJoinsPreserveLeftProperties` and `TestSemiAndAntiJoinsCopyWorkspaceReusingLeftInput`, unchanged, pin T5; `TestSemiAndAntiJoinsDeduplicateUnkeyedLeftInput` is deleted with the contract violation it was built on.

## 6. The change the proof licenses

- `relation_properties.go`: `joinProperties` derives, after its two rules, the composite of each left key with each right key through `appendDistinctKeys`; its doc states the three rules.
- `join.go`: the materialized arm appends every combined tuple and builds no map; the streaming arm builds no map and `hashJoinIterator` has no `seen` field; `hashJoinIterator.Next` emits every combined tuple. `SemiJoin` and `AntiJoin` construct their result through `newMaterializedRelationFromSet` over the left's symbols and properties, and `materializeFilteredLeft` is deleted.
- `symmetric_hash_join.go`: `symmetricHashJoinIterator` has no `seen` field and both batch loops emit every combined tuple.
- Tests: the three fixtures are deleted; `mockIterator` and `newMockIterator` are deleted, a raw tuple stream being `sliceIterator` over its slice and a relation input being a set built through `NewMaterializedRelation`; the `seen` assertions leave the property tests; the two new tests land.
- `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`: the candidate-key origins gain the composite of a join's two sides, the consumers lose the hash join's dedup elision, and the completed changes gain this one. `PERFORMANCE_STATUS.md` records the benchmarks.

## 7. What the proof does not cover

- The input side of P1. A caller that hands a join a relation that is not a set gets a result that is not a set, where today it got one deduplicated by seen. The record names such inputs invalid, and no production constructor admits one.
- The default path. The streaming and symmetric arms are proven alike and measured by their own benchmarks; P9 keeps them opt-in, and this change does not alter the default.
- The order a body's clauses run in, which decides how many relations a delta copy joins before its delta; the planner is not touched.
- `derivedSet.absorb`, which keys every tuple a body answers into the predicate's set. That is the union across bodies and iterations, which the record requires to restore set semantics, and it stays.
- `crossProduct`, which constructs through `NewMaterializedRelationWithOptions` and so runs a dedup pass over a product of sets; the record lists it as avoidable, and no failure names it.
- `NewMaterializedRelation` and its two siblings, which deduplicate at the package boundary, and every `NewDedupIterator` site, each a projection or union the record requires.
