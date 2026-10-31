# The cache match answers the datoms agreeing with its bindings, as a proof

## Sources

- The code: `datalog/storage/matcher_relations.go` (`MatchWithConstraints`, `matchWithBindingsFromCache`, `matchWithVValidation`, `validatingVBoundIterator`), `datalog/storage/hash_join_matcher.go` (`matchWithHashJoin`, `compileBindingMatchPlan`, `bindingMatchPlan.matches`, `hashJoinIterator`), `datalog/storage/matcher.go` (`typedPositionBindingCheck`, `extractValue`, `matchesDatom`), `datalog/storage/cache.go` (`CacheEntry`, `OneValue`, `ManySet`), `datalog/executor/query_executor.go` (`Execute`, `executePattern`), `datalog/executor/relations.go` (`Relations.Collapse`, `hasSharedSymbols`, `FindBestForPattern`), `datalog/executor/indexed_memory_matcher.go` (`boundDatomIterator`), `datalog/executor/pattern_match.go` (`bindPatternFromTuple`), `datalog/query` (`PatternExtractor.ExtractV`, `UnifyPatterns`), `datalog` (`ValuesEqual`).
- The prior proof: `docs/proposals/BINDING_MATCH_PROOF.md`. Its §7 and §8 name this change as a change of its own. Its definitions and its preconditions P1 to P10 are read here as given.
- The failure: the allocation profile of the reproduction of `docs/bugs/JANUS_REPRO_FILLED.md`, its `unfilled` query through the CLI. The rule `(unfilled ?m ?n)` is invoked with both arguments bound, so the magic rewrite binds the head's `?n` before the body's `[?c :cell/next ?n]`. The pattern's bindings carry `?c` and `?n`. The cache arm reads `?c` alone and answers every successor of each cell, and the deduplicating constructor over that answer is the profile's largest share after the frame's record. `PERFORMANCE_STATUS.md` carries the figures.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **B** is the binding relation of a match, as `BINDING_MATCH_PROOF.md` §1 defines it.
- **G** is the group B is read from: the relation `FindBestForPattern` chooses among the groups. B is G projected onto the pattern's variables it carries and selected by the typed check.
- The **carried positions** of B are the pattern's variables B carries, each at the position it names in the pattern: entity, attribute, value or transaction.
- A binding's **agreeing datoms** are the datoms of the matcher's view under the pattern's constants whose value at every carried position equals the binding's value there under `ValuesEqual`.
- The **read positions** of the cache match are, under a constant attribute, the pattern's entity variable and, when B carries it, the pattern's value variable. Under a variable attribute they are the entity variable and the attribute variable.
- The **answer** is the relation `matchWithBindingsFromCache` returns.
- **R** is the answer before the change. **R'** is the answer after it.

## 2. Preconditions

- **P1 (the executor joins the answer with its group).** `Execute`'s pattern arm appends the answer to the groups and calls `Collapse`. `Collapse` joins every two relations sharing a symbol through `Join`. The answer carries the pattern's symbols, and G carries B's symbols among them. So the answer and G end in one group, joined on B's symbols, and the query reads `G ⋈ R` and never R alone.
- **P2 (the other arms answer the agreeing datoms).** The hash-join arm compares each resolved datom with each binding sharing its key at every carried position, through `compileBindingMatchPlan` and `bindingMatchPlan.matches`. The V-validation arm reads each binding's value through `PatternExtractor.ExtractV` and emits a candidate only when the winner's value equals it under `ValuesEqual`. The in-memory matcher binds every carried position of the pattern from the binding through `bindPatternFromTuple` before it matches. So on every arm but the cache arm, a binding's answer is its agreeing datoms, one tuple each.
- **P3 (the cache arm before the change).** Under a constant attribute the cache arm reads each binding's entity and nothing else. Under cardinality one it answers the entry's current value. Under cardinality many it answers every member of the entry's set. A binding's value, when B carries it, is read by nothing on this arm.
- **P4 (the entry is the view's resolution).** An entry's one value, or its set, is the view's resolution of the entity's datoms under the attribute. The V-validation arm reads the same entry through `GetOrResolve` to decide whether a candidate's winner carries the bound value, so the entry and the scan arms resolve the same datoms.
- **P5 (the typed selection under a constant attribute).** Under a constant attribute every binding of B holds an Identity at the entity position and no vector at the value position, by `typedPositionBindingCheck`.
- **P6 (membership and equality agree).** `ManySet` is keyed by each member's hashable form: a byte slice by its string, and every other value by itself. Under a constant attribute a binding's value is a scalar of the domain, by P5. `ValuesEqual` on two scalars of one type is `==`, and integer inputs are normalized to `int64` at the boundary. So a binding's value equals a member under `ValuesEqual` exactly when the set holds its key, which is how the arm already decides membership for the pattern's constant value.
- **P7 (the cache arm's cardinalities under a constant attribute).** The cache arm runs only outside History. A declared vector attribute under a constant attribute is answered by `matchVectorWithBindings` before the cache arm. An attribute the schema does not declare is read under cardinality one. So under a constant attribute the cache arm's cardinality is one or many, and its vector arm is not reached.
- **P8 (B is a set, and each variable has one position).** `BINDING_MATCH_PROOF.md` P9 and P10.
- **P9 (the join compares by value).** `Join` compares the shared symbols' values under the typed equality `ValuesEqual` defines.

## 3. Theorems

**T1 (the query's answer is unchanged).** `G ⋈ R' = G ⋈ R`.

**T2 (the arms agree).** After the change, under a constant attribute, the cache arm's answer per binding is its agreeing datoms, one tuple each, as the other arms answer.

**T3 (the answer is a set when B carries the read positions alone).** After the change, when B's symbols are the read positions and nothing else, the answer's tuples are pairwise distinct, so the set constructor answers the relation the deduplicating constructor would.

**T4 (what the arm allocates).** After the change the cache arm builds at most one tuple per binding under a constant attribute. Under T3's condition it allocates the tuples, their slice and the relation, and no key map.

## 4. Lemmas

### L1. The agreeing datoms per binding (T2)

Fix a binding b under a constant attribute, with entity e and, when B carries the value, value v. By P4 the entry under e is the view's resolution of e's datoms under the attribute. Under cardinality one the entry holds one current value w, or none. The datom agreeing with b exists exactly when w exists and equals v under `ValuesEqual`. After the change the arm compares w with v under `ValuesEqual` and answers the tuple exactly then. Under cardinality many the entry holds the members. A datom agreeing with b exists exactly when v is a member, and by P6 exactly when the set holds v's key. After the change the arm answers that one tuple exactly then. When B carries no value the arm answers as before, which is every datom of e under the attribute, each agreeing with b at the entity. ∎

### L2. The join discards what the change withholds (T1)

By L1 and P3, R' is the subset of R holding the tuples that agree with some binding at every carried position. Let r be a tuple of R not in R'. Then no binding agrees with r at every carried position. By P1 and P9, r joins a tuple g of G exactly when r and g agree on B's symbols, which are the carried positions. By P5 a g whose projection onto B's symbols is a binding would have to agree with r, and none does. A g whose projection is no binding fails the typed check, so its entity is no Identity, and r's entity is one, so they disagree. So r joins nothing in G. Hence `G ⋈ R = G ⋈ R'`. ∎

### L3. The answer is a set (T3)

Let B's symbols be the read positions alone. By P8 B is a set, so two bindings differ at a read position. By L1 each binding answers at most one tuple, and by P7 no binding reaches the vector arm. By P8 a tuple carries its binding's entity and value at distinct positions. So two bindings' tuples differ, and the answer is a set. By `BINDING_MATCH_PROOF.md` P8 the set constructor then answers what the deduplicating one answers. ∎

### L4. The allocations (T4)

By L1 the arm builds at most one tuple per binding. Under L3's condition the set constructor allocates the relation alone over the tuples and their slice, where the deduplicating constructor allocated a key map and a second slice beside them. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestCacheMatchOverBindingsCarryingTheValueAnswersTheAgreeingTuplesAlone` (storage, new): over a cardinality-many attribute, two bindings sharing an entity, one carrying a member and one carrying a value no member equals, answer the member's tuple and nothing for the other. Over a cardinality-one attribute, a binding carrying a value other than the entity's answers nothing, and a binding carrying the entity's value answers its tuple (T1, T2).
- `TestCacheMatchOverBindingsCarryingTheValueAllocatesItsAnswersAlone` (storage, new): bindings carrying each entity and its value under a cardinality-one attribute allocate, per match, no more bytes than the answers weigh, one tuple of two values and its slice element per binding, plus an allowance for the relation. A key map over the answers holds an entry per answer and weighs more than the allowance (T3, T4).
- `TestCacheMatchOverBindingsCarryingTheValueAnswersEachTupleOnce` (storage), run unchanged (T1, T3): two bindings over one entity, each carrying one of the entity's two members, answer each member's tuple once. Its description is restated under this proof: each binding answers its own member, and the set constructor answers them.
- `TestCacheMatchAllocatesItsAnswersAlone`, `TestBoundEntityNonMatchOnValuePositionJoin`, `TestBoundEntityNonMatchAgreesAcrossJoinStrategies`, the scan-funnel reports of the cache arm, and the storage tests of the binding-driven match, run unchanged (T1).
- `TestQueriesAnswerAlikeUnderBothOptimizerModes` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (storage), the differentials, run unchanged (T1).

## 6. The change the proof licenses

- `matcher_relations.go`, `matchWithBindingsFromCache`: under a constant attribute, finds the pattern's value variable among B's symbols once, and per binding reads the value there in place of the pattern's constant value, for the cardinality-one comparison and the cardinality-many membership. The read positions count the value variable when B carries it under a constant attribute. The set constructor is taken when B's symbols are the read positions alone.
- `matcher_relations.go`, the comment at the constructor choice, restated for the read positions as §1 defines them.
- `cache_match_bindings_test.go`, the description of `TestCacheMatchOverBindingsCarryingTheValueAnswersEachTupleOnce`, restated as §5 states it.
- The tests of §5.

## 7. What the proof does not cover

- The cache arm under a variable attribute, which reads the entity and the attribute alone and deduplicates when B carries the value, as before. The typed selection admits a vector at the value position there, and a vector is no key of a set.
- The pattern's transaction variable, which the cache arm reads from no binding.
- History, under which the cache arm is not taken.
- The vector arm of the cache match, which is reached under a variable attribute alone.
- The other arms of the match, which answer the agreeing datoms already.
- `matchFromCache`, the cache match without bindings.

## 8. Alternatives excluded

- The answer as a superset, joined down by the collapse, which is the state before the change. It builds the entry's every value per binding and a key map over all of them, and the join discards what the arm could have withheld.
- Routing B carrying the value past the cache arm to the V-validation arm. It opens a candidate scan per binding where the entry answers in one lookup.
- The matcher joining its answer with B before returning it. It is a second join of what the collapse joins.
- Reading the bound value under a variable attribute too. The typed selection admits a vector at the value position there, and the set's key admits none.
