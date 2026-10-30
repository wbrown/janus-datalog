# A pull-based hash join, as a proof

## Sources

- The code: `datalog/executor/join.go` (`HashJoin`, `HashJoinWithOptions`, `hashJoinIterator`, `combineTuplesIndexed`, `SemiJoin`, `AntiJoin`, `crossProduct`, `emitJoinStrategyAnnotation`), `datalog/executor/join_index.go` (`JoinIndex.Add`, `JoinIndex.Probe`, `JoinIndex.Len`), `datalog/executor/tuple_key.go` (`NewTupleKeyMapWithCapacity`), `datalog/executor/relation.go` (the `Relation` and `Iterator` interfaces, `copyTuple`, `collectTuplesInto`, `ForEach`, `CollectTuples`, `CachingIterator`, `StreamingRelation.Iterator`, `StreamingRelation.Size`, `StreamingRelation.RequiresCopy`, `StreamingRelation.Materialize`, `MaterializedRelation.Join`, `StreamingRelation.Join`, `NewProductRelation`, `ProductRelation.Size`), `datalog/executor/relations.go` (`Relations.Collapse`, `reportJoin`), `datalog/executor/query_executor.go` (`DefaultQueryExecutor.Execute`, `renderConstantFindSymbols`, `existenceVerdict`, `executePattern`, `tryFuseAttributeFetchBundle`, `executeExpression`, `executePredicate`, `executeSubquery`, `findOuterRelation`, `applyFindPulls`, `replaceConsumedGroups`), `datalog/executor/executor.go` (`ExecuteWithRelations`, `ExecuteRealized`, `executeRealizedPlan`, `runPhases`, `declaredTupleCount`, `executePhase`), `datalog/executor/executor_utils.go` (`BindQueryInputs`), `datalog/executor/scope.go` (`requiredRun`, `scopeResult`, `materialized`, `hasTuple`, `executeOrJoinClause`, `executeOrDefaultJoinClause`, `executeNotJoinClause`), `datalog/executor/bound_pattern.go` (`streamsThrough`, `executeBoundPattern`), `datalog/executor/bound_invocation.go` (`executeBoundInvocation`), `datalog/executor/rules.go` (`executeRuleInvocation`, `invocationIterator`, `seedRelation`), `datalog/executor/windowed_form.go` (`newWindowedIterator`), `datalog/executor/window_reader.go` (`windowReader`), `datalog/executor/or_fallback_relation.go` (`OrFallbackRelation`), `datalog/executor/relation_ops.go` (`materializeRelationsForPattern`, `filterWithPredicateAndLookup`, `evaluateExpressionWithLookup`, `unionRelations`), `datalog/executor/theta_join.go` (`thetaJoinPair`), `datalog/executor/buffered_iterator.go` (`BufferedIterator`), `datalog/executor/iterator_composition.go` (`ProjectIterator`, `PredicateFilterIterator`, `FunctionEvaluatorIterator`, `DedupIterator`), `datalog/executor/limit_relation.go` (`LimitRelation.take`), `datalog/executor/aggregation.go` (`StreamingAggregateRelation.fold`), `datalog/executor/phase_boundary.go` (`materializePhaseBoundary`), `datalog/executor/symmetric_hash_join.go` (`ChooseJoinStrategy`), `datalog/storage/convenience.go` (`Database.Query`), `datalog/storage/database.go` (`DefaultPlannerOptions`).
- The proof this one stands on: `docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md`. Its §1 defines a set, a key, the join and the combined tuple. Its P2 states the one equality E. Its L2 enumerates each matching pair once in both hash arms.
- The design record: `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`. Its law that `Materialize` guarantees replayability and not eager realization. Its "Materialization That Should Remain" list, with hash build state on it. Its mandatory gates.
- The scope design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md`.
  - §4.2, "The window": every subject is read in windows, and a subject of several groups is a fold of the hash join with the join so far as the streaming probe side, so the subject reports no size.
  - §4.4, "A data pattern over a streaming group" and "A bound invocation at the top level": a pattern or invocation over a group reporting no size takes the window, and a group held in memory binds through the matcher.
  - §4.8: a streaming subject is read once, and a reader that needs a result twice materializes the result.
- The failure: `docs/bugs/JANUS_REPRO_FILLED.md`. Its `unfilled` query over the kernel's facts runs for minutes, and the record names what shows the fix. `PERFORMANCE_STATUS.md`, in its "Join output is a set" entry, records that run's allocation profile, read with the CLI's `-memprofile`: the join's bytes are the materialized arm's results slice and its objects are `combineTuplesIndexed`'s, one combined tuple per pair.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **E**, a **set**, a **key**, the **join** of L and R on J, and the **combined tuple** of a pair are as `JOIN_OUTPUT_IS_A_SET_PROOF.md` §1 defines them.
- A **stream** is a `StreamingRelation`. Its `Iterator` may be called once. A second call without `Materialize` before it panics. `Materialize` marks it for caching, and the next `Iterator` builds the cache through a `CachingIterator`, which copies every tuple it hands back into the cache.
- The **copy answer** is `Iterator.RequiresCopy`. It is true when the slice `Tuple` hands back is one the next `Next` overwrites, so a reader that keeps a tuple past the next pull copies it. It is false when every tuple is the caller's to keep. `Relation.RequiresCopy` is the answer of the iterator `Iterator` hands out. A `StreamingRelation` answers false once it caches and its iterator's answer otherwise.
- The **size answer** is `Relation.Size`. A `StreamingRelation` answers its cache's length when the cache is ready, its known size, its materialization's size, or its counter's count once its iterator is done. Otherwise it answers -1 without opening the iterator.
- The **index** is the `JoinIndex` the build side is held in: one group of held tuples per distinct key, probed by positions with no allocation.
- The **join's iterator** is `hashJoinIterator`: the index, the probe iterator, the deferred build error, the projection plan, the current probe tuple, the current match group and its position.
- The **workspace** is one slice of the result's width the join's iterator owns, fills on each pull, and hands back from `Tuple`.
- An **empty build** is an index holding no tuple after the build side is drained.
- A **consumer** of a join result is a function that receives the relation `HashJoinWithOptions` answers, through `HashJoin`, a relation type's `Join` or `HashJoin`, `Relations.Collapse` or `findOuterRelation`.

## 2. Preconditions

- **P1 (the join of sets, in two arms).** `HashJoinWithOptions` has two arms after the symmetric case.
  - Both drain the build side into the index, copying each tuple when the build side says copy.
  - The materialized arm pulls each probe tuple once, builds one combined tuple per index match through `combineTuplesIndexed`, appends it to a results slice sized from the build count, and constructs through `newMaterializedRelationFromSet`.
  - The streaming arm answers a `StreamingRelation` over the join's iterator. Its `Next` pulls each probe tuple once, copies it when the probe side says copy, and builds one combined tuple per index match through `combineTuplesIndexed` as the consumer pulls. Its `Tuple` answers that tuple. Its `RequiresCopy` answers false.
  - L2 of `JOIN_OUTPUT_IS_A_SET_PROOF.md` states that both arms enumerate each matching pair once, and T1 there that the tuples are a set.
  - `ExecutorOptions.EnableStreamingJoins` selects the arm. `findOuterRelation` forces it true for its fold. `DefaultPlannerOptions` sets it false.
- **P2 (the iterator contract and its readers).** `Iterator.Next` advances to the next tuple and `Iterator.Tuple` answers the current one. Every consumer that keeps a tuple past the next pull reads the copy answer or copies regardless.
  - Readers that read the answer and copy when it is true:
    - `collectTuplesInto`, and through it `requiredRun`, `materialized` in scope.go, `materializePhaseBoundary` and `OrFallbackRelation.Materialize`
    - `LimitRelation.take`
    - `windowReader`, and through it every windowed form
    - `filterWithPredicateAndLookup`
    - `evaluateExpressionWithLookup`
    - `tryFuseAttributeFetchBundle`
    - `seedRelation`
    - `SemiJoin` and `AntiJoin` on their left
    - `HashJoinWithOptions` on its build side
  - Readers that copy regardless:
    - `CachingIterator`
    - `BufferedIterator`, and through it `thetaJoinPair`
    - `applyFindPulls`
    - `CollectTuples`
  - Readers that pass the answer through:
    - `invocationIterator` when it yields the source's tuple
    - `PredicateFilterIterator`
    - `FunctionEvaluatorIterator` when it unifies
    - `DedupIterator` when it does not copy
  - Readers that keep no source tuple:
    - `unionRelations`
    - `StreamingAggregateRelation.fold`
    - `ProjectIterator`
    - `FunctionEvaluatorIterator` when it extends
    - `invocationIterator` when it projects
    - `thetaJoinPair`'s combined tuple
    - `combineTuplesIndexed`
- **P3 (single use and its readers).** A stream is read once unless materialized first.
  - The consumers that read a join result more than once materialize it first: `materializeRelationsForPattern` on a group sharing a pattern's symbol, `NewProductRelation` on every operand but the first, `seedRelation` on a contributing group, and the entity prefetch in `Execute` on the first pattern's last group.
  - One consumer reads a relation twice without materializing it: `executeRealizedPlan` reads the relation `BindQueryInputs` answers. It projects the environment symbols out of it and reads the projection's size. When constant symbols are to be dropped, it reads the relation again through `Materialize().Project`.
  - `BindQueryInputs` answers the fold of `Join` over materialized input relations. Today that fold's last `Join` answers a materialized relation, so both reads hold.
- **P4 (the size readers).**
  - `reportJoin` reads the result's size under a handler and reads nothing when the answer is -1.
  - `streamsThrough` reads the size of every group sharing a pattern's or invocation's symbols and routes the clause through the window when one answers below zero.
  - `HashJoinWithOptions` reads its sides' sizes to choose the build side when both are materialized.
  - `HashJoinWithOptions` reads the build side's size for the index's capacity, falling back to `DefaultHashTableSize` or 256 when the answer is negative.
  - `SemiJoin` and `AntiJoin` read the right side's size for the key map's capacity.
  - `hasTuple` reads a size, and otherwise pulls one tuple and closes, completing a filling cache.
  - `declaredTupleCount` answers -1 when any group declines.
  - `ProductRelation.Size` answers -1 when any operand declines.
  - `ExecuteWithRelations` reads the sizes of the relations the caller gave.
- **P5 (the windowed route).**
  - `executeBoundPattern`, `executeBoundInvocation`, `executeNotJoinClause`, `executeSubquery` and the two or-forms with a subject each build their subject through `findOuterRelation`.
  - `findOuterRelation` joins the groups holding a needed symbol by a fold of `HashJoinWithOptions`, with the later group as the build side and the join so far as the probe side.
  - Each hands the subject to `newWindowedIterator`, which opens the subject's iterator once and reads it through `windowReader` into windows, copying each tuple when the subject says copy. It never reads the subject's size.
  - `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2 states that every partition of the subject into windows answers the same tuples.
  - §4.4 states that a pattern or invocation over a group reporting no size takes this route, and that a group held in memory binds the pattern through the matcher.
- **P6 (the empty join).**
  - `JoinIndex.Probe` answers nil for every probe tuple when the index holds no tuple.
  - So the join of L and R on J is empty whenever the build side is empty, whatever the probe side holds.
  - Both arms today drain the probe side whatever the index holds: the materialized arm in its probe loop, and the streaming arm's `Next` as the consumer pulls to the end.
- **P7 (the key map's capacity).** `NewTupleKeyMapWithCapacity` passes its argument to `make` as the map's size hint. A negative argument is the size answer of a stream.
- **P8 (the public boundary).**
  - `Database.Query` answers the executor's final relation as it is, under its stated contract: the caller iterates through `Iterator` and closes the iterator.
  - `Relation` and `Iterator` state the copy answer a caller keeping a tuple reads.
  - `ExecuteRealized` reads the final relation once, through `Sort`, `TopNRelation`, a streaming `Project`, `NewLimitRelation` or `applyFindPulls`, each of which copies what it keeps.
  - `DefaultPlannerOptions` sets `EnableTrueStreaming` true, so a query ending in a scan answers a stream, its final relation answers -1 for its size, and a caller that counts iterates.

## 3. Theorems

**T1 (the answers).** For L and R sets, in every setting of the options, the join's iterator yields exactly the combined tuples of the join of L and R on J, one per pair. The tuple `Tuple` hands back at each pull is E-equal to that pull's combined tuple.

**T2 (every consumer).** With `BindQueryInputs` answering a held relation, every consumer of a join result reads it once or materializes it first, and every consumer that keeps a tuple past the next pull copies it. So the tuples every query answers are unchanged.

**T3 (realization).** The join holds tuples in the index alone. Beyond the index it holds one workspace and no slice of results.

**T4 (allocation).** Per pair the join allocates nothing. Per probe tuple it allocates nothing, on a hit and on a miss alike. Per join it allocates the index, the workspace, the iterator and the relation.

**T5 (the empty build).** When the build side is empty the join answers the empty relation over the result's symbols and properties, carrying the build side's deferred error, without opening the probe side. The probe side is left as its next reader finds it.

**T6 (the route).** A join result answers -1 for its size. A later pattern or invocation sharing its symbols takes the windowed route, which reads the join result once and answers the tuples the matcher's route answers. A scan failure on that route surfaces when the result is driven, where the matcher's route surfaced it from the call. At the public boundary a query whose final relation is a join result, or a projection of one, answers -1 for its size, as a query ending in a scan does (P8), and a caller that counts iterates.

**T7 (cost).** The per-pair path loses the call to `combineTuplesIndexed` and the allocation it made, and gains no call: the fill is written in `hashJoinIterator.Next`. The benchmarks in §5 measure the change, and their figures are recorded in `PERFORMANCE_STATUS.md`.

## 4. Lemmas

### L1. The workspace carries the combined tuple (T1)

At each pull the join's iterator takes the next member b of the current match group and the current probe tuple p. It writes into the workspace what `combineTuplesIndexed` wrote into a fresh slice: the left tuple whole, then the right tuple's values at the plan's positions, the left and right being b and p as `buildIsLeft` orders them. The workspace has the result's width. The plan is the one `HashJoinWithOptions` computed. The values written are the same values. So the workspace is E-equal at every position to the combined tuple of (b, p), and `Tuple` hands it back before the next pull overwrites it. The enumeration of the pairs is the streaming arm's, which L2 of `JOIN_OUTPUT_IS_A_SET_PROOF.md` proves yields each matching pair once. ∎

### L2. The probe tuple needs no copy (T4)

The join's iterator pulls the probe iterator only when the current match group is exhausted. Within a group it reads the current probe tuple's values into the workspace at each pull. It reads nothing of the probe tuple after the group. A probe iterator that reuses its buffer overwrites it only on its own `Next`. So every value the join reads from a probe tuple is read before the probe iterator is pulled again. The copy the streaming arm makes of a probe tuple that says copy protects nothing the join reads. ∎

### L3. A kept workspace is a later pair (T2)

A consumer that keeps the workspace past the next pull holds, after that pull, the next pair's values. By P2 every consumer that keeps a tuple past the next pull reads the copy answer or copies regardless, and the join's iterator answers true. So no consumer holds the workspace past a pull. ∎

### L4. The binder's fold (T2)

`BindQueryInputs` folds `Join` over materialized input relations, each `Join` after the first taking the fold so far as its left. Under this change each such `Join` answers a stream. The next `Join` reads that stream once, as its probe side, since its right side is materialized and `HashJoinWithOptions` takes the materialized side as the build. The fold's last answer is read twice by `executeRealizedPlan` (P3). With the binder collecting its fold through `collectTuplesInto` into a proven-set materialized relation before answering, each join result in the fold is read once, by the next join or by the collection. Both of `executeRealizedPlan`'s reads are then of a held relation. The collection copies under the copy answer (P2). ∎

### L5. Every other consumer (T2)

By P3 every consumer that reads a join result more than once materializes it first, and the cache copies every tuple it keeps. By P2 every consumer that reads it once copies what it keeps or keeps nothing. By P5 every windowed form reads its subject once through the window reader, which copies under the copy answer. By P8 the final relation's readers read once and copy what they keep. With L4 the one reader outside these classes reads a held relation. So every consumer reads each join result once or from a cache, and no consumer holds its workspace. ∎

### L6. The empty build (T5)

By P6 the join is empty when the index holds no tuple, whatever the probe holds. So the empty relation over the result's symbols and properties is the answer, and no pull of the probe side is needed to compute it. The build side was drained, so its deferred error is in hand and the result carries it. The probe side's iterator is not called. So a stream among the probe sides keeps its one use, and a caching stream records no incomplete materialization. ∎

### L7. The route (T6)

By P4 `streamsThrough` reads the size of every group sharing the clause's symbols. By the definition of the size answer a join result answers -1. So a pattern or invocation sharing a join result's symbols is routed through `executeBoundPattern` or `executeBoundInvocation`. By P5 that route reads its subject once, copying under the copy answer. `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2 states that the window answers the tuples the subject's join with the clause answers. Today the same clause over a materialized join result is routed through `executePattern`, which materializes the group for the matcher's bindings and joins the matches at the collapse. Both routes answer the join of the group with the clause's relation. ∎

### L8. The key map (T2)

`SemiJoin` and `AntiJoin` size their key map from the right side's size answer, and a join result answers -1 (P7). `NewTupleKeyMapWithCapacity` hands that answer to `make` as the map's hint, and a negative hint starts the map empty, as `TestTupleKeyMapWithAnUnknownCapacityStartsEmpty` pins. So the map is the map a right side of unknown size gets today. The semi-join and anti-join read their right side once and their left once as before. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, mapped to the record's mandatory gates.

- Gate 1, the semantic differential:
  - `TestAJoinOfTwoSetsAnswersEachPairOnce` (executor), run with `EnableStreamingJoins` true and false, asserts the same pair set as a nested loop computes, each pair once, in every arm (T1, T6).
  - `TestQueriesAnswerAlikeUnderBothOptimizerModes` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (storage) are the differentials over real queries and rule evaluation, run unchanged (T2).
- Gate 2, the shapes: the same test's cases for an empty side, one tuple on each side, a key repeated on one side, on both, and a large input (T1).
- Gate 3, iterator and close errors:
  - `TestHashJoinPropagatesBuildAndProbeCloseErrors` (executor). The build case runs unchanged. The probe case drives the join through `CollectTuples`, which reads what `Close` returns, since the join's iterator returns the probe's close error from `Close` (P2).
  - `TestHashJoinSurfacesErroredEmptySide` (executor), run unchanged. It joins a relation with an errored empty one. Whichever side the join takes as its build, the error reaches the result: as the build's deferred error carried by the empty-build answer, or as the probe's error when the probe is opened under a non-empty build (T5).
- The materialized arm's contract, which the executor's older tests read, moves to the stream's (T4, T6):
  - `TestHashJoin`, `TestJoinMultipleSymbols`, `TestEmptyJoin`, `TestDatomJoinScenario`, `TestStreamingRelationSingleUseInJoin`, `TestEqualVectorValuesJoin`, `TestSignedZeroHashesConsistentlyAcrossSetOperations` and `TestRelationsCollapse` count the tuples they collect in place of reading the join result's size.
  - `collectTuples` in `join_test.go` and `assertNoDuplicates` in `set_semantics_test.go` copy a kept tuple under the copy answer.
  - `TestHashJoinEmitsStructuredStrategyBuildAndProbeAnnotations` asserts the streaming mode.
  - `TestOrderByScalarInputConstantKeyIsNoOp` and `TestExpressionOntoBoundVariableUnifies` collect a query's final relation in place of reading its size and indexing it (P8).
  - `TestRelationInput_PropagatesMatcherErrorAfterAnEarlierPattern` drives the result and reads the matcher's error there.
  - `TestAEVTIndexBugDirect` (`datalog/storage/aevt_bug_test.go`) collects the query's final relation through `CollectTuples` and counts the tuples collected in place of reading its size (P8).
- The route's continuation: an invocation after a join inside a form's scope takes the windowed route in a window whose join has a build and the held route in one whose build is empty (T5, T6). `INVOCATION_CONTINUATION_PROOF.md` proves that both routes continue one evaluation, and its §5 lists the storage tests that pin it.
- Gate 4, workspace reuse:
  - The same test's case whose probe side is a workspace-reusing stream, asserting the same pair set (T1, L2).
  - A new assertion in it: the join's iterator answers `RequiresCopy` true, and the slice it hands back at one pull holds the next pair's values after the next pull (T4, L3).
  - `TestSemiAndAntiJoinsCopyWorkspaceReusingLeftInput`, run unchanged.
- Gate 5, keys: `TestJoinPropertiesDeriveTheCompositeOfEachSidesKeys` and the key-preservation tests of `relation_properties_test.go`, run unchanged. The change touches no property.
- Gate 6, the physical path: the new assertions read the join's events.
  - The `JoinStrategy` event reports the streaming mode under both option settings (T6).
  - The `JoinProbe` event's result count equals the pair count (T1).
  - For an empty build over a stream probe, the `JoinProbe` event reports no probe tuple, and the probe stream's iterator was not called (T5).
  - The `JoinBuildCopy` and `JoinBuild` events are unchanged.
- The binder: `TestBindQueryInputsAnswersAHeldRelation` (executor, new). The relation `BindQueryInputs` answers for two relation inputs sharing a symbol reports its size and is read twice (L4). The `:in` tests of `bind_query_inputs_streaming_test.go` run unchanged.
- The key map: `TestTupleKeyMapWithAnUnknownCapacityStartsEmpty` (executor, new). `NewTupleKeyMapWithCapacity(-1)` answers an empty map that admits a key (L8).
- The route: `TestAPatternOverAJoinResultTakesTheWindow` (executor, new). A pattern after a join of two groups is answered through the windowed route, the window event reporting the pattern's form, and the answers equal the matcher route's over the same data (T6, L7).
- Gate 7, focused benchmarks on valid inputs: `BenchmarkHashJoinIdentityKeys`, `BenchmarkHashJoinIdentityHighFanout`, `BenchmarkHashJoinIdentityLargeResult`, `BenchmarkHashJoinMaterializedVsStreaming` and `BenchmarkSymmetricHashJoin`, before and after (T4, T7).
- Gate 8: `BenchmarkComplexQueryCheckpoint` (`datalog/storage/optimization_matrix_test.go`), before and after, the before on this branch at the commit before the change.
- Gate 9: `make test`, the executor package under `-race`, and the fixed-seed rules test above.
- The failure: the reproduction's `unfilled` query through the CLI, before and after, with its allocation profile.

## 6. The change the proof licenses

- `join.go`, `HashJoinWithOptions`:
  - answers the streaming relation over the join's iterator in every setting of `EnableStreamingJoins`
  - loses the materialized arm, its results slice, its probe-consumed check and its probe event
  - reports the streaming mode in the `JoinStrategy` event
  - when the index is empty after the build, answers the empty relation over the result's symbols and properties through `newMaterializedRelationFromSet`, carrying the build error, emits the `JoinProbe` event with no probe tuple, and does not call the probe side's `Iterator`
- `join.go`, `hashJoinIterator`:
  - owns a workspace of the result's width
  - fills it on each pull in place of building a fresh tuple, and hands it back from `Tuple`
  - answers `RequiresCopy` true
  - copies no probe tuple, and `probeNeedsCopy` goes
- `executor_utils.go`: `BindQueryInputs` collects its fold through `collectTuplesInto` into a proven-set materialized relation before answering it.
- `tuple_key.go`: `NewTupleKeyMapWithCapacity`'s doc states that a negative size, a stream's size answer, starts the map empty. The code is unchanged, since `make` reads a negative hint so.
- `datalog/storage/database.go`: `DefaultPlannerOptions`'s comment on `EnableStreamingJoins` says the option selects nothing in the hash join.
- Tests: the new assertions and tests of §5. The cases of `TestAJoinOfTwoSetsAnswersEachPairOnce` that assert the materialized arm's probe event mode assert the streaming mode.
- `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`: the completed changes gain this one. `PERFORMANCE_STATUS.md` records the benchmarks and the reproduction.

## 7. What the proof does not cover

- A probe side that already carries a recorded failure, a materialized relation whose collection failed or a stream whose cache completed with an error, under an empty build. The join opens nothing, so the failure is not raised here. It surfaces where the relation is next driven, and a relation nothing else drives keeps it unraised. Today the materialized arm drains the probe and raises it. The alternative, opening the probe's iterator and closing it without a pull, raises a materialized relation's recorded failure and records an incomplete materialization on a caching stream.
- The symmetric arm, which `ChooseJoinStrategy` selects under `EnableSymmetricHashJoin`. It builds its tuples fresh and answers `RequiresCopy` false as before.
- `crossProduct`, reached by `Join` on sides sharing no symbol. It opens its right side once per left tuple and so cannot take a stream as its right side. No consumer in the enumeration hands it a join result as its right side: `Collapse` joins only groups sharing a symbol, and `findOuterRelation` joins under the common symbols, empty or not, through the hash join.
- The cost of the windowed route against the matcher's route for a pattern after a join. Gate 8 and the reproduction measure it, and this proof does not bound it.
- `reportJoin`'s size reads of the left and right sides, which it makes of every side that is not a `StreamingRelation`. The record lists annotation realization among its remaining opportunities.
- Whether `EnableStreamingJoins` is retired from `ExecutorOptions` and `PlannerOptions`. It selects nothing in the hash join under this change and stays where it is.
