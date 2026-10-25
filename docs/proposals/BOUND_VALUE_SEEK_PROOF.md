# A bound value as a seek, as a proof

## Sources

- The code: `datalog/storage/matcher_strategy.go` (`analyzeReuseStrategy`, `chooseBestMultiPositionStrategy`), `datalog/storage/matcher_relations.go` (`MatchWithConstraints`, `matchWithVValidation`, `validatingVBoundIterator.Next`, `validatingVBoundIterator.validateCandidate`, `matchUnboundScan`, `matchWithBindingsFromCache`), `datalog/storage/hash_join_matcher.go` (`chooseJoinStrategy`, `matchWithHashJoin`, `keyScanBounds`, `patternScanBoundWithBinding`, `scanBoundForValues`, `hashJoinIterator.Next`, `matchWithMergeJoin`, `mergeJoinIterator.Next`), `datalog/storage/crdt_resolving_iterator.go` (`CRDTResolvingIterator.Next`, `startNewGroup`), `datalog/storage/matcher.go` (`chooseIndex`, `LookupAttribute`), `datalog/storage/cache_resolver.go` (`GetCardinality`), `datalog/executor/options.go` (`SmallBindingBoundary`).
- The design: `docs/reference/INDEX_SELECTION_PROOF.md` (INDEX), Theorems 1, 2, 3, 3b and 5 and the selection matrix; `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §4.6; the janus `CLAUDE.md`, "Storage Design" and "Cache Path CardinalityOne Tombstone Gap".

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **pattern** P is a data pattern `[e a v]` or `[e a v t]`. Its positions are E, A, V and T.
- A **binding** B is the relation the matcher is given beside P, over symbols S_B. A position of P is **bound** under B when its variable is in S_B, and **constant** when P holds a constant there. A binding's **keys** at a bound position are the distinct values B holds for that position's variable.
- An **(E, A) group** is the set of datoms with one entity and one attribute. The group's **resolution** is what `CRDTResolvingIterator` produces for it: for cardinality one, the datom with the highest Tx within the matcher's temporal bound, the **winner**; for cardinality many, each value whose highest-Tx operation is an assertion; for a vector, the RGA order.
- A **run** is a `ScanBound`, an index with the leading components bound to values, which `OpenKeyScan` reads in index order.
- A run **holds the whole resolution decision** of a group when every datom resolution reads to decide the group lies in the run and is adjacent in it to the group's other datoms.
- A run is **narrowed to V** when the bound value is among its prefix components: AVET `[A][V]` or VAET `[V]`. `scanBoundForValues` narrows AVET and VAET to a value only when the value is an identity; any other value leaves the prefix at `[A]` or empty.
- The **extent** of P under an index is the run bound by P's constants alone (`patternScanBound`): AVET `[A]` for a constant attribute.
- A **strategy** is `analyzeReuseStrategy`'s result: a type, the **seek dimension** (the position whose keys the scan follows), an index, and whether validation is requested.
- The **per-key seek** opens one run per key at the seek dimension, each the one-key narrowing of the extent (`keyScanBounds`, `perKeySeekIterator`), when the keys number at most `executor.SmallBindingBoundary`, which is 1000, or at the attribute position at any count. More keys, or keys the index cannot narrow to, are probed against one scan of the extent (`matchWithHashJoin`, `chooseJoinStrategy`).
- **Candidate and validate** is `matchWithVValidation`: per binding tuple, a run narrowed to its V, each datom resolution produces checked against the (E, A) winner read from EATV (`validateCandidate`), kept when the winner carries the bound value, for an attribute resolving as cardinality one or unknown (`validatingVBoundIterator.Next`).
- The cardinality an attribute **resolves under** is its declaration's, and cardinality one for an attribute the schema does not declare and for a matcher with no schema. `GetCardinality`, `chooseIndex`, `matchUnboundScan` and `LookupAttribute` state it so, and `CRDTResolvingIterator.startNewGroup` reads an undeclared attribute as `CardinalityUnknown` and resolves it as one (INDEX Theorem 3b).

## 2. Preconditions

- **P1** (INDEX Theorem 1a). A run on EATV or AETV whose prefix ends at or before the attribute holds every (E, A) group it meets whole, in Tx descending order, so the first datom of the group is its winner.
- **P2** (INDEX Theorem 2). A run narrowed to V holds, of a cardinality-one group, the datoms carrying that value alone. The group's winner may carry another value and lie outside the run.
- **P3** (INDEX Theorem 3). A run narrowed to V holds the whole add-wins decision of (E, A, V) for a cardinality-many group, since every assertion and withdrawal of a value carries that value.
- **P4** (INDEX Theorem 3b). An undeclared attribute resolves as cardinality one.
- **P5**. `CRDTResolvingIterator.Next` opens a group when the datom's (E, A) differs from the previous datom's, and for a cardinality-one or unknown group emits the group's first datom and skips every later datom of the group.
- **P6**. AVET orders A, then V, then E, then Tx descending. Within a run bound by `[A]` alone, one entity's datoms carrying two values are separated by every other entity's datoms carrying values between them.
- **P7**. `hashJoinIterator.Next` and `mergeJoinIterator.Next` verify every emitted datom against a binding tuple through the full pattern (`bindingMatchPlan.matches`, `matchesWithBindingTuple`) and emit a datom at most once.
- **P8**. `matchWithBindingsFromCache` answers a binding-driven match from the cache only when E is a bound variable and the matcher has a cache and is not in history mode. With E unbound, or the cache off, the strategy path runs.
- **P9**. `validatingVBoundIterator.Next` validates every candidate whose attribute resolves as cardinality one or unknown, and passes a many or vector candidate through.
- **P10**. `chooseBestMultiPositionStrategy` takes as the seek dimension the bound position with the most distinct values, A preferred on a tie, V among the candidates for every attribute, and requests no validation.
- **P11**. `MatchWithConstraints` intercepts a pattern whose attribute is declared a vector before the cache arm and before the strategy, and calls `matchVectorWithBindings`, which resolves the RGA of each entity the binding binds and compares the pattern's constant V, and which answers an empty relation with no error when the binding does not bind E. A vector datom's V is one element, and the pattern's V names the whole collection. The unbound path resolves every entity's vector and compares the bound V through `matchVectorScanAllEntities`.
- **P12**. A value's encoding is injective on the engine's equality: two values `ValuesEqual` holds equal encode to one key. `NormalizeValue` (`datalog/value_encoding.go`), which `Type` and `ValueBytes` read before encoding and which the writes, the query inputs, the builder's constants and the custom functions call, takes every integer width as `int64` and a float's signed zero as `0.0`, the one pair the encoding would otherwise split. So a run narrowed to a value's encoding holds every datom carrying a value equal to it.

## 3. Theorems

**T1 (a run narrowed to V is not resolution for cardinality one).** Under P2 and P5, the iterator over a run narrowed to V emits, for a cardinality-one group whose winner carries another value, the group's latest datom carrying the bound value, a datom resolution does not produce. The per-key seek, the one-key narrowing and the VAET run are all such runs.

**T2 (the extent on AVET is not resolution for cardinality one).** Under P5 and P6, the iterator over the extent `[A]` on AVET opens a cardinality-one group once per value the entity ever carried, and emits each opening's first datom, so every value the entity ever carried is emitted. The mechanism differs from T1: the group is fragmented, not filtered.

**T3 (a seek on E, A or T is resolution).** Under P1, a strategy seeking on E (AETV under a constant attribute, EATV otherwise), on A (AETV) or on T (TAEV) opens runs holding whole groups in Tx order, so the iterator emits each group's winner, and the bound value compared downstream by P7 keeps exactly the tuples whose current value is the bound one, for every cardinality.

**T4 (validation restores resolution for a single bound V).** Under P9, P12 and INDEX Theorem 5, candidate and validate over a run narrowed to V emits exactly the entities whose (E, A) winner carries the bound value, for an attribute resolving as cardinality one, at every key count: P12 puts every candidate carrying an equal value in the run, and the validation keeps those whose winner carries it.

**T5 (a declared many narrows).** Under P3, for a declared cardinality many, a run narrowed to V and the extent on AVET alike hold the whole add-wins decision of each (E, A, V), so the iterator's emission is resolution without validation, per key and in the extent.

**T6 (one fact, two consequences).** The correctness of a strategy whose binding touches V is decided by one fact, the cardinality the constant attribute resolves under. With V the one bound position, validation is required exactly when the attribute resolves as cardinality one (T1, T2, T4, T5). With several positions bound, V is a correct seek dimension exactly when the attribute is declared many (T1, T2, T3, T5), and a correct seek dimension always exists, since another bound position does. When the attribute is a variable of P, no constant decides, and the candidate's own attribute decides per datom (P9).

**T7 (the sites).** The tree computes T6's fact at three sites by one rule, inline at each: `matchUnboundScan`, `chooseIndex` and the gate in `MatchWithConstraints` read the attribute as resolving under cardinality one unless its declaration says otherwise. `analyzeReuseStrategy` requests validation for every single bound V, and the gate keeps the request unless the attribute is declared many or vector, a second computation of the fact the analysis is built on. `chooseBestMultiPositionStrategy` reads no cardinality and seeks on V for every attribute (P10). L2 and L3 are the runs the hash join opens when validation is cleared, and L4 the run P10 opens.

**T8 (a declared vector never narrows).** A vector's datoms each carry one element and the pattern's V names the collection, so no run narrowed to the bound value holds any datom of the group, and the extent on AVET fragments the group by element (P6). A bound V on a declared vector is therefore compared only against a resolved vector, never used to narrow a run. Under P11 the binding path resolves each bound entity's vector and compares, which is resolution. With the entity unbound the binding path answers nothing (P11), where the entities whose resolved vector is the bound value are the answer, which the unbound path's `matchVectorScanAllEntities` computes: it scans AEVT under the attribute, resolves each entity's vector once, and keeps the entity when the resolved vector equals the bound value. L7 is the run.

## 4. Lemmas

### L1. The runs each strategy opens (T1, T2, T3)

With V the one bound position and the attribute constant, `analyzeReuseStrategy` returns AVET with validation requested. When the gate keeps the request, `matchWithVValidation` runs (T4). When it clears it, `matchWithHashJoin` runs: one key narrows the extent to `[A][V]` when the key is an identity and leaves it at `[A]` otherwise; up to the boundary of identity keys are sought one run each, `[A][V_i]`; more keys, or keys that are not identities, are probed against the extent `[A]`. With V the one bound position and the attribute a variable, the index is VAET and validation is per datom. With several positions bound, `chooseBestMultiPositionStrategy` picks the seek dimension by P10, and the hash join opens the same runs for V as above, or AETV and EATV runs for E, AETV for A and TAEV for T. The merge join is chosen only for E (`chooseJoinStrategy`), so every V-position strategy reads through the hash join or candidate and validate. ∎

### L2. The failing run under T1

Let `a` carry `:t/ref` d1 at T1 and d2 at T2 > T1, and let the pattern be `[?a :t/ref ?d]` with `?d` bound and validation cleared, so the hash join runs. With the key d1 alone, the run `[:t/ref][d1]` holds `(a, :t/ref, d1, T1)`, which opens a group and is emitted; the probe matches it to the binding; the answer is `[a d1]`, where resolution gives `a` no datom carrying d1. With the keys d1 then d2, the seeks `[:t/ref][d1]` then `[:t/ref][d2]` yield `(a, :t/ref, d1)` then `(a, :t/ref, d2)`; the first opens the group and is emitted, the second has the same (E, A) and is skipped (P5); the answer is `[a d1]` alone. With the keys d2 then d1 the answer is `[a d2]`. That is the order dependence N2 observed. ∎

### L3. The failing run under T2

With the same data, `:t/ref` undeclared, and one key more than the boundary, the hash join probes the extent `[:t/ref]`. Under P6 the run holds `(a, :t/ref, d1)` and `(a, :t/ref, d2)` separated by the other referents' datoms. Each opens a group and is emitted (P5), both keys are present, and the answer holds `[a d1]` beside `[a d2]`. ∎

### L4. The failing run under P10

With the same data and `:t/ref` declared cardinality one or undeclared, let the binding be `{(a, d1), (a, d2)}` over `[?a ?d]`. E has one distinct value and V two, so P10 seeks on V with no validation, and the runs are L2's: the answer is `[a d1]`. With the cache on, P8 answers the match from the cache and the strategy is never reached, which is why the run is observed with the cache off. ∎

### L5. The correct run for L4's binding (T3)

Seeking on E, the run is AETV `[:t/ref][a]`, which holds `(a, :t/ref, d2, T2)` then `(a, :t/ref, d1, T1)`. The iterator emits the first, d2 (P1, P5). The probe matches it to the binding tuple `(a, d2)` and nothing to `(a, d1)`. The answer is `[a d2]`. ∎

### L6. Cost (T6)

P10's heuristic minimises the runs opened: one per key of the seek dimension. Under T6 the V dimension is unavailable for an attribute resolving as cardinality one, so the runs number the distinct keys of E, A or T, which may exceed the distinct values. For a declared many the V dimension stays available and the heuristic stands as it is. The bound is correctness: a cheaper strategy that answers a superseded value answers nothing. A single bound V of such an attribute goes through candidate and validate, whose runs number the binding tuples, so the multi-position seek on E costs no more than the single-position path on the same keys. ∎

### L7. The vector binding without the entity (T8)

Let `a` carry the vector `["x" "y"]` and `b` the vector `["x"]` under `:t/vec` declared a vector, and let the query be `[:find ?e :in $ ?v :where [?e :t/vec ?v]]` given `["x" "y"]`. The input binds `?v` alone, so the matcher is given a binding over `?v` and no E. P11's interception runs, `matchVectorWithBindings` finds no E among the binding's symbols and answers the empty relation with no error, and the query answers nothing, where `a` is the answer. With `?e` bound beside `?v`, the same function resolves `a`'s or `b`'s vector and compares, and the answer is the entity whose vector is the bound one. ∎

## 5. What the proof finds

- **F1.** The fact is one, the cardinality a constant attribute resolves under, and it has two consequences for a binding-driven match: whether a single bound V's candidates are validated, and whether V may be the seek dimension (T6). The gate computes the first at one site and `chooseBestMultiPositionStrategy` computes the second nowhere (T7).
- **F2.** The gate is a second computation of what `analyzeReuseStrategy` already decides the shape of. The analysis requests validation and the gate reads the cardinality to keep it; one site reading the cardinality decides both consequences.
- **F3.** The cache masks L4. A test binding E and V with the cache on never reaches the strategy, since P8 answers from the cache, so the strategy path for an E-bound binding is covered only with the cache off, the shape the janus `CLAUDE.md` records under the tombstone gap: many tests, one path.
- **F4.** T6's second consequence is INDEX's selection matrix applied to a binding: with E bound the run holds the whole group and V is compared downstream; with E unbound and A bound the AVET candidates are post-validated; a bound V enters the prefix only for a declared many. `chooseIndex` applies the matrix to the unbound path and `chooseBestMultiPositionStrategy` applies none of it.
- **F5.** The vector arm of the binding path carries the rule for one of its two states and a guard for the other. With E bound it resolves and compares, T8's rule. With E unbound it answers empty under a comment that the state does not arise, where L7 reaches it through a query input. The unbound path's `matchVectorScanAllEntities` is the computation that state needs, and the arm does not call it.

## 6. The contracts the tests pin

Each contract runs under every backend and optimizer mode, and where the strategy path is the subject, with the cache off.

- `analyzeReuseStrategy` requests validation for a single bound V when the attribute is declared cardinality one, undeclared under a given schema, or undeclared with no schema, and requests none for a declared many (T6).
- `analyzeReuseStrategy` seeks on E for a binding over the entity and the value with more distinct values than entities when the attribute is declared cardinality one, undeclared under a given schema, or undeclared with no schema, and seeks on V when the attribute is declared many (T6, P10's heuristic within T6).
- The matcher answers the current value of a cardinality-one attribute under one key, under several in either order, under more keys than the seek takes, and under a binding over the entity and the value, declared and undeclared, with the cache on and off (T1 to T4, L2 to L5): `TestUndeclaredAttribute_TheMatcherAnswersTheCurrentValueUnderEveryBinding`.
- The matcher answers a declared many's members under a binding over the entity and the value with more distinct values than entities, seeking on V (T5).
- A declared vector under a binding of the value alone answers the entities whose vector is the bound one, and under a binding of the entity and the value answers the entity when its vector is the bound one and nothing when it is another, with the cache on and off (T8, L7).
- A vector at a pattern's value position under a constant attribute not declared a vector, bound by any route (a ground, an input of every form, an expression's collection binding, a subquery argument, a value carried from a declared vector attribute, a rule body's argument) or written as the pattern's literal, answers no tuple with no error, on every backend and both modes, since a datom's value is a scalar and the vector is the domain's one composite (`datalog.IsVector`); the same vector under the declared vector attribute answers the entity whose vector it is (T8): `TestVectorAtValuePosition_*` (`datalog/storage/vector_at_value_position_test.go`) and `TestCLI_AVectorGivenToAScalarInputAnswersNoTuple` (`cmd/datalog`).
- The query paths of `TestUndeclaredAttribute_TheCurrentValueAnswersUnderEveryKeyOrder` and `TestUndeclaredAttribute_AMovedRefAnswersItsCurrentTargetAlone` stand.

## 7. The resolution

`analyzeReuseStrategy` is the matcher's method. It reads the constant attribute's declaration once and holds the cardinality the attribute resolves under. With V the one bound position and the attribute constant, it requests validation exactly when that cardinality is one. With several positions bound, it tells `chooseBestMultiPositionStrategy`, a function taking that verdict, whether V may be the seek dimension, which holds when the cardinality is many. The gate in `MatchWithConstraints` has no job and goes. `chooseBestMultiPositionStrategy` leaves V out of its candidates when the verdict is no, and its heuristic over the remaining positions stands.

`matchVectorWithBindings`, given a binding that does not bind E, answers through `matchVectorScanAllEntities` with the pattern's constant V, every entity's vector resolved once, and the binding's values are compared at the collapse; the guard that answers empty goes.

A vector at a value position under any other constant attribute is the typed non-match of the equality join, the fact `typedPositionBindingCheck` already computes for a non-Identity at the entity position and a non-Keyword at the attribute position: the binding-driven match selects its bindings under the check once at its entry (`MatchWithConstraints`), after the vector arm and before the cache arm and the strategy choice, so the strategy chooser's counting, the cache arm and every join strategy read typed bindings alone, and `matchUnboundScan` answers the empty relation for the pattern's own vector literal once the attribute's cardinality is read and before any arm runs. The classifier is `datalog.IsVector`, the one `typeRank` ranks the composite by.

The derivation: T6 names one fact with two consequences, F1 and F2 show the tree deciding one consequence at a second site and the other nowhere, and the site that chooses both the validation request and the seek dimension is the analysis. T8 and F5 show the vector arm holding the rule for one state and a guard for the other, and the computation the guarded state needs already in the unbound path. The alternative is the gate kept for the first consequence and a second gate added after the analysis for the second, moving a strategy off V when the cardinality is not many. Its cost is the fact read at two sites for one match and a strategy built and then rebuilt, where the analysis reads it once and builds once.
