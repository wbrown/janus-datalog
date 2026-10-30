# The binding-driven match answers its input where it changes nothing, as a proof

## Sources

- The code: `datalog/storage/matcher_relations.go` (`MatchWithConstraints`, `matchWithBindingsFromCache`, `restoreScanSetSemantics`), `datalog/storage/matcher.go` (`typedPositionBindingCheck`), `datalog/executor/relation.go` (`MaterializedRelation.ProjectFromPattern`, `MaterializedRelation.Project`, `MaterializedRelation.Rename`, `MaterializedRelation.carryErr`, `MaterializedRelation.Iterator`, `Select`, `NewMaterializedRelationWithOptions`, `NewMaterializedRelationFromSet`, `newMaterializedRelationFromSet`, `deduplicateTuples`, `projectionPreservesSet`), `datalog/query` (`DataPattern.Symbols`), `datalog/planner/framing.go` (`WindowFraming`), `datalog/executor/bound_pattern.go` (`executeBoundPattern`), `datalog/executor/kept_branch.go` (`buildBranchTuples`).
- The design record: `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`. Its law that a set-preserving operator does not deduplicate again, and the relational invariants of the janus `CLAUDE.md`: every relation is a set, and a relation is never changed in place.
- The scope design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4, "A data pattern over a streaming group": the pattern is one body under the header of the variables it shares with the group, and its frame's run is held under `scope/required`.
- The failure: the allocation profile of the reproduction of `docs/bugs/JANUS_REPRO_FILLED.md`, its `unfilled` query through the CLI, attributes to this path three allocations per binding that answer nothing. The projection of the bindings onto the pattern's symbols copies every binding when the symbols are the bindings' own. The typed selection copies the whole binding list when it rejects nothing. The cache match's constructor builds a key map over a result that is a set. `PERFORMANCE_STATUS.md` carries the figures.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **set** is a relation in which each complete tuple appears at most once. Every relation is a set.
- A **held relation** is a `*MaterializedRelation`: its symbols, its tuples, its options, its properties and its deferred error, read through `Symbols`, `Iterator`, `Get`, `Size`, `Options`, `Properties` and the iterator's `Error`.
- A **stream** is any other relation.
- **B** is the binding relation of a match: the relation `MatchWithConstraints` reads bindings from, after `ProjectFromPattern` and the typed selection.
- The **identity projection** of a held relation R is `Project(S)` with S equal to R's symbols, element by element, in R's order.
- A **selection rejecting nothing** is `Select(R, p)` with p true of every tuple of R.
- The **read positions** of the cache match are the pattern's entity variable and, when the pattern's attribute is a variable, its attribute variable. The cache match reads nothing else of a binding.
- The **cache match's result** is the relation `matchWithBindingsFromCache` constructs over the tuples it builds, one per answer.

## 2. Preconditions

- **P1 (a relation is never written after construction).** Every write of a held relation's error is on an object the writing function constructed itself; `carryErr` writes a derived relation's error only when that error is nil and the source's is not. `Tuples` has no reader. `Rename` answers a relation holding the same tuple slice as its input. So two relations may share tuples, and a reader of a relation reads what was constructed.
- **P2 (the projection before a match).** `ProjectFromPattern` lists the pattern's variables the binding relation carries in the order entity, attribute, value, transaction, and projects through `Project`. `DataPattern.Symbols` lists the pattern's variables in that same order.
- **P3 (`Project`).** For any symbols, `Project` allocates one new tuple per input tuple. It constructs through the set constructor when `projectionPreservesSet` holds, a retained key or a permutation of the full symbol list, and through the deduplicating constructor otherwise. The answer carries the input's error. Its properties are the input's projected onto the symbols.
- **P4 (a windowed pattern's bindings).** A data pattern over a streaming group runs as a body over a frame whose required run is the pattern's symbols the subject carries, in the pattern's order (`WindowFraming`), and the body's pattern reads its bindings from that run, held under `scope/required`. So the binding relation of such a match carries the pattern's own symbols it carries, in the pattern's order.
- **P5 (`Select`).** `Select` reads its input once, keeps each tuple the predicate accepts, copying it when the input says copy, and constructs a held relation over the input's symbols, options and properties, carrying the input iterator's error. A held relation's iterator error is its deferred error.
- **P6 (the typed selection).** `typedPositionBindingCheck` accepts a binding whose entity position holds an Identity, whose attribute position holds a Keyword, and, under a constant attribute, whose value position holds no vector. It is the predicate `MatchWithConstraints` selects B under.
- **P7 (the cache match).** Per binding tuple the cache match reads the read positions and nothing else. It answers, per binding, at most one tuple under a constant value, one tuple per member of the entry's set under no value, or one tuple holding the entry's vector. The members of an entry's set are distinct, since the set is keyed by value. Each result tuple carries the binding's entity and attribute and the answer's value at the pattern's positions.
- **P8 (the constructors).** `NewMaterializedRelationWithOptions` deduplicates its tuples through a key map sized to their count. `NewMaterializedRelationFromSet` deduplicates nothing and checks each value's domain. `restoreScanSetSemantics` returns a held relation unchanged.
- **P9 (B is a set).** The relation `ProjectFromPattern` answers is a set by P3. A selection of a set is a set. So B is a set.
- **P10 (one position per variable).** `query.UnifyPatterns`, a step of `Prepare`, writes every data pattern that names one variable at several positions as the pattern over distinct variables followed by their equalities. So a pattern reaching the matcher names each variable at one position, and a result tuple's entity, attribute and value stand at distinct positions.

## 3. Theorems

**T1 (the identity projection is the relation).** For a held relation R, the identity projection of R answers R itself. Every reader reads R's symbols, tuples, options, properties and error as it read the fresh relation's before.

**T2 (a selection rejecting nothing is the relation).** For a held relation R and a predicate true of every tuple of R, `Select` answers R itself. For a stream, and for a predicate that rejects a tuple, `Select` answers as before.

**T3 (the cache match is a set when the bindings carry the read positions alone).** When B's symbols are the read positions and nothing else, the cache match's tuples are pairwise distinct, so constructing them through `NewMaterializedRelationFromSet` answers the relation the deduplicating constructor answered. When B carries another of the pattern's variables, two bindings can agree on the read positions, each answers the same tuples, and the deduplicating constructor stays.

**T4 (answers unchanged).** Every query answers the tuples it answered before.

**T5 (what the path allocates).** The identity projection allocates nothing. A selection rejecting nothing over a held relation allocates nothing. The cache match over bindings carrying the read positions alone allocates its result tuples, their slice and the relation, and no key map.

## 4. Lemmas

### L1. Answering the relation itself (T1, T2)

By P1 nothing writes R after its construction, so a reader of R at any later time reads what R was constructed with. By P3 the fresh relation the identity projection constructed carried R's symbols in R's order, one tuple per tuple of R holding the same values at the same positions, R's options, R's properties projected onto R's own symbols, and R's error. `RelationProperties.project` keeps every ordering clause and every key whose symbols are all among the projection's, and R's own symbols are all of them, so that projection is R's properties. By P5 the fresh relation a selection rejecting nothing constructed carried R's symbols, R's tuples in R's order, R's options, R's properties and R's deferred error. Each is equal to R in every part a reader reads. So answering R reads the same. `Rename` already answers a relation sharing its input's tuples, so a tuple read through two relations is the established case. ∎

### L2. A windowed pattern projects by the identity (T1)

By P4 B's symbols before the projection are the pattern's symbols the subject carries, in the pattern's order. By P2 `ProjectFromPattern`'s list is the pattern's variables B carries, in the order of `DataPattern.Symbols`, which is the pattern's order. The two lists hold the same symbols in the same order. So the projection is the identity projection, and by T1 it answers B. ∎

### L3. The cache match's tuples are distinct (T3)

Let B's symbols be the read positions alone. By P9 B is a set, so two binding tuples differ at a read position. By P7 and P10 each result tuple carries its binding's values at the read positions, each at its own position, so the results of two bindings differ. Within one binding the results are at most one tuple, or one per distinct member, or one tuple, so they are distinct. So the result is a set, and by P8 the set constructor and the deduplicating constructor answer the same tuples. Let B carry another of the pattern's variables, the value or the transaction. Two binding tuples may then agree at the read positions and differ at that variable. By P7 each answers the same tuples, so the result repeats each of them, and the deduplicating constructor removes the repeats. ∎

### L4. The typed selection on a frame's run (T2)

The selection is `Select(B, p)` with p from P6. When every binding holds an Identity at the entity position, a Keyword at the attribute position, and no vector at the value position, p is true of every tuple, and by T2 the selection answers B. When some binding fails p, the selection answers the fresh relation over the survivors as before. ∎

### L5. The answers (T4)

By L1 the identity projection and the selection rejecting nothing answer a relation equal in every readable part to the one they answered before. By L3 the cache match answers the same tuples under either constructor. Every later step reads those relations as before. ∎

### L6. The allocations (T5)

The identity projection returns R and allocates nothing. The selection over a held relation reads R's tuples through its slice and, rejecting nothing, returns R, allocating nothing. The cache match's set constructor allocates the relation alone over the tuples and slice the match built, where the deduplicating constructor allocated the key map and a second slice beside them. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestProjectOntoItsOwnSymbolsAnswersTheRelation` (executor, new): a held relation's identity projection is the relation itself, with its properties and its deferred error; its projection onto a permutation of its symbols is another relation holding the same tuples under the new order (T1).
- `TestSelectRejectingNothingAnswersTheRelation` (executor, new): a selection rejecting nothing over a held relation is the relation itself, with its deferred error; a selection rejecting one tuple is another relation holding the survivors; a selection over a stream is a held relation holding the stream's tuples (T2).
- `TestCacheMatchOverBindingsCarryingTheValueAnswersEachTupleOnce` (storage, new): two bindings sharing an entity and differing at the pattern's value variable, over a cardinality-many attribute with two members, answer each member's tuple once (T3, the deduplicating arm). By L3 a set constructor taken for those bindings answers each member's tuple twice, which the assertion refuses.
- `TestSelect`, `TestSelectCarriesScanError` and `TestProject` (executor), and the storage tests of the binding-driven match, run unchanged (T4).
- `TestQueriesAnswerAlikeUnderBothOptimizerModes` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (storage), the differentials, run unchanged (T4).

## 6. The change the proof licenses

- `relation.go`, `MaterializedRelation.Project`: answers the relation itself when the symbols are its own in its own order.
- `relation.go`, `Select`: over a held relation, answers the relation itself when the predicate rejects nothing, and otherwise the held relation over the survivors as before; over a stream, as before.
- `matcher_relations.go`, `matchWithBindingsFromCache`: constructs through `NewMaterializedRelationFromSet` when the binding relation's symbols are the read positions alone, and through the deduplicating constructor otherwise.
- `matcher_relations.go`, `restoreScanSetSemantics`'s doc: a held result is a set by its constructor, deduplicating or proven.
- The tests of §5.

## 7. What the proof does not cover

- `StreamingRelation.Project` and `ProductRelation.Project`. A stream's identity projection still wraps a projecting iterator.
- `Select` over a stream, which still drains the stream into a held relation.
- The cache match over bindings carrying the pattern's value or transaction variable, which still deduplicates. Reading the bound value from the binding, so that every binding answers at most the tuples agreeing with it, changes which tuples the match answers and is not licensed here.
- `matchFromCache`, the cache match without bindings, which constructs as before.
- The value-domain check `NewMaterializedRelationFromSet` runs over the result's values. It allocates nothing. The benchmarks of §5 price it.

## 8. Alternatives excluded

- A projecting view over a held relation's tuples for every projection. It allocates an iterator per read, and the identity is the one projection that needs nothing.
- The typed selection skipped under a value-type property of the binding relation. Relations carry no such property, and a property is a second computation of what the selection reads once.
- The cache match reading the bound value from a binding that carries it, so that every binding relation answers a set. It changes the match's answer and is a change of its own.
- An exported set constructor without the value-domain check, for the matcher. It is a second boundary constructor beside the one the package has, and the check costs no allocation.
