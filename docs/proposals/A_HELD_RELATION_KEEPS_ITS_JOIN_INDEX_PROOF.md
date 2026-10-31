# A held relation keeps its join index, as a proof

## Sources

- The code: `datalog/executor/rules.go` (`derivedSet`, `newDerivedSet`, `derivedSet.absorb`, `derivedSet.relation`, `ruleEvaluation.extend`, `ruleEvaluation.runStratum`, `stratumEvaluation.run`, `executeRuleInvocation`, `invocationSource`), `datalog/executor/join.go` (`HashJoinWithOptions`, `hashJoinIterator`, `emitJoinStrategyAnnotation`), `datalog/executor/join_index.go` (`JoinIndex`, `JoinIndex.Add`, `JoinIndex.Probe`), `datalog/executor/tuple_set.go` (`TupleSet`, `TupleSet.Find`, `TupleSet.FindAt`, `TupleSet.Add`), `datalog/executor/relation.go` (`MaterializedRelation`, `newMaterializedRelationFromSet`, `MaterializedRelation.Rename`, `MaterializedRelation.Join`, `MaterializedRelation.HashJoin`), `datalog/executor/relations.go` (`Relations.Collapse`), `datalog/executor/query_executor.go` (`DefaultQueryExecutor.Execute`, its `*query.RuleInvocation` case, `findOuterRelation`), `datalog/executor/context.go` (`Context.Derived`, `Context.WithDerived`).
- The proofs this one stands on: `PULL_BASED_HASH_JOIN_PROOF.md` (T1 the answers, L1 the workspace, T4 allocation, T5 the empty build); `ONE_TUPLE_TABLE_PROOF.md` (T2 the index's contract, T3 a probe allocates nothing); `JOIN_OUTPUT_IS_A_SET_PROOF.md` (the join of two sets is a set); `DELTA_LEADS_ITS_COPY_PROOF.md` (T2 the delta leads its copy, T3 the clauses after it read what the delta reaches); `DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` (P1 the semi-naive step, T1 the answers).
- The design: `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4, the bullet on an invocation of a predicate the scope holds, which reads the held relation and joins it at the collapse, where the hash join holds the held relation and streams the group once; its §4.6, the kept branch's `JoinIndex`, kept across windows and probed per subject tuple; `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3, which states that the derived relation an invocation reads is materialized through `derivedSet.relation`; `docs/reference/RULES.md`, "Evaluation", the paragraph on what a body's invocations read.
- The failure: the bound `unfilled` query of the downstream report `docs/plans/JANUS_REPRO_FILLED.md` in `janus-repro-filled-2026-10-30.tar.gz`, which costs several times the whole relation. Its annotated run shows every phase of the rewritten stratum joining a delta of a few tuples against a derived relation of thousands, the derived relation drained into the join's index when it is the build side and streamed whole when it is the probe side.
- The decision: a derived set keeps, per key its joins read, the index over its tuples, extended with the tuples admitted since it was last extended, and a join into the set's relation at that key probes the index with the other side's tuples and drains nothing.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **set** S is a `derivedSet`: a predicate's tuples over its canonical symbols, each held once, appended in the order `absorb` admits them. The **ordinal** of a tuple is its position in `S.tuples`. The **count** of S at a moment is the tuples it holds then.
- A **view** of S at n is the relation `derivedSet.relation` answers when S's count is n: a `MaterializedRelation` over `S.tuples[:n]`.
- A **held relation** is a relation the scope holds under a predicate symbol (`Context.Derived`): a view, a delta relation, or the required run under `scope/required`.
- A **key** K of S is a list of positions of its tuples. The **key of a tuple** t at K is t's values at K, in K's order.
- The **index** of S at K is the structure this proof licenses: the one table (`TupleSet`) over the first tuple of each distinct key at K, with, per member, the ordinals of S's tuples holding that key, ascending. The index **holds** m tuples when the first m tuples of S have entered it.
- A **probe** of the index at K by a tuple p at positions Q, **bounded** at n, answers the ordinals under p's key at Q that lie below n, a prefix of the member's ordinals.
- A **join into a view** is `HashJoinWithOptions(L, R, J)` with the view as L or as R, and J the join symbols. The view's key for the join is its positions of J.
- The **group** at an invocation is the relation the clauses before it in the body's plan produced.
- The **build side** and the **probe side** of a join are as `HashJoinWithOptions` names them: the side drained into the join's index, and the side streamed through it.

## 2. Preconditions

- **P1 (the join as it stands).** `HashJoinWithOptions` takes as the build side a materialized side against a stream, the smaller of two materialized sides, and the right side when neither rule decides. It drains the build side into a fresh `JoinIndex` and streams the probe side through it. So a join into a view costs the view's tuples: drained when the view is the build side, streamed when it is the probe side.
- **P2 (the view).** `derivedSet.relation` answers `newMaterializedRelationFromSet` over `S.tuples[:n:n]` under the set's symbols and keeps no index. `runStratum`'s `view` takes one view per set for each run it makes, `extend` holds the stratum's final views in `ev.derived`, and the delta relations, the same-stratum deltas and the lower deltas alike, are fresh materialized relations per run.
- **P3 (the invocation's read).** For a held predicate, `executeRuleInvocation` answers `Rename` of the view when every argument is a distinct variable the environment does not bind: a materialized relation over the view's tuples, shared, under the arguments' names, at the view's positions. Otherwise it answers a streaming selection over the view through `invocationIterator`.
- **P4 (the collapse).** `Execute` appends the invocation's relation to the groups and `Collapse` joins it with each group sharing a symbol, through `Join`, which is `HashJoinWithOptions` on the common symbols with the group so far as the left side.
- **P5 (the delta leads).** In a copy the delta invocation is selected first and the clauses after it read what the delta reaches (`DELTA_LEADS_ITS_COPY_PROOF.md` T2, T3). So at a held invocation of a copy the group is the delta's tuples and what they reached.
- **P6 (the table).** `TupleSet.FindAt` finds a member by a probing tuple's key at positions given per probe and allocates nothing, and `TupleSet.Add` holds a member under its key at the stored positions (`ONE_TUPLE_TABLE_PROOF.md` T2, T3).
- **P7 (the set grows only).** `absorb` appends a tuple the set does not hold and admits nothing else, and no tuple leaves a set. So an ordinal names one tuple for the set's life, and the first n tuples of S are the same at every later moment.
- **P8 (the join of sets).** The join's iterator yields each matching pair once, its workspace holding the pair's combined tuple at each pull (`PULL_BASED_HASH_JOIN_PROOF.md` T1, L1).
- **P9 (a held invocation is never windowed).** `Execute` takes the windowed route only for an invocation of a predicate the scope does not hold, so a held invocation's relation always meets the group at the collapse.
- **P10 (a run reads its own views).** Every body of a run reads the views `view` took for that run, and the next run takes views of its own. A body's result is pulled when it is absorbed, and the views it reads are fixed slices, so a body never reads a tuple a set admitted after its views were taken.

## 3. Theorems

**T1 (answers unchanged).** Every join into a view answers the pairs it answered. So every stratum derives the sets it derived, and every query answers what it answered.

**T2 (the view is exact).** A probe bounded at the view's count answers exactly the view's tuples holding the probe's key, and no tuple the set admitted after the view.

**T3 (a join drains nothing of a view).** A join into a view at key K probes the index at K once per tuple of the other side and drains no tuple. Under P1 the join drained its build side and streamed its probe side. When the view was the build side, the probes are the same and the drain is gone. When the view was the probe side, the other side was the build side and holds at most as many tuples as the view, so the probes are at most as many and the drain is gone.

**T4 (a tuple enters an index once).** Over an evaluation, each tuple of S enters the index at K once, at the first extension past its ordinal. So the index at K costs each tuple of S one add, and the table's doublings.

**T5 (nothing else changes).** A join none of whose sides is a view, a view joined under no common symbol, and a held invocation read through a streaming selection are as they were.

## 4. Lemmas

### L1. The index answers the view's matches (T2)

Let the view be at n and the index at K hold m ≥ n tuples when probed. A probe by p at Q answers the ordinals under p's key at Q that lie below n. By P6 the member found is the first tuple of S whose key at K equals p's key at Q, and by the index's construction the member's ordinals are every tuple among the first m with that key, ascending. By P7 the tuples of S below n are the view's tuples. So the ordinals answered name exactly the view's tuples whose key equals p's, each once, and no tuple admitted after the view. ∎

### L2. The join's pairs are the same (T1)

Under this proof the join's iterator reads, per probe tuple p, the tuples the index answers for p, and fills its workspace with the combined tuple of each, as it fills it from a group of a built index (P8). By L1 those tuples are exactly the view's tuples matching p on J. Under P1, when the view was the build side the built index's group for p's key held exactly the view's tuples matching p on J, and when the view was the probe side the iterator paired each view tuple with the other side's group matching it. Either way the pairs are the matching pairs of the join, each once, and under this proof they are the same pairs, each once, their combined tuples equal position by position. ∎

### L3. The sets and the answers (T1)

A stratum's sets are its bodies' answers absorbed. A body's answer is the collapse of its clauses' relations, and by L2 each join of that collapse answers the pairs it answered. So each body answers the tuples it answered, each run absorbs the tuples it absorbed, and each stratum derives the sets it derived. A query's answer is a projection of its groups, joined at the same collapse. ∎

### L4. The probes and the drain (T3)

Under this proof the join's build phase over a view adds nothing to any index, since the index exists. Its probe phase pulls the other side once and probes once per tuple pulled. Under P1 the join drained one side whole and pulled the other whole. When the view was the build side, the view was drained and the other side pulled, which it now is alone. When the view was the probe side, the other side was the build side: it was materialized and no larger than the view, or the view was a stream, which a view is not (P2). So the other side's tuples, now pulled, are at most the view's, which were pulled before. ∎

### L5. The extension (T4)

The index at K is extended from the count it holds to the set's count at the extension, each tuple between them added once under its key, by ordinal. By P7 the ordinals never change and an extension never revisits a tuple below the count held. So each tuple enters once. ∎

### L6. The step reads what it read (T1, T2)

By P10 a body reads the views taken for its run, each at the count its set held then. A probe through such a view is bounded at that count, so a tuple an earlier body of the same run admitted into the set is in the set and not in the view's answers, as it is not in the view's tuples. So the semi-naive step reads at each run exactly what it read (`DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` P1, T1). ∎

### L7. Nothing else (T5)

A join whose sides carry no set's index builds and probes as `HashJoinWithOptions` does. A view joined under no common symbol goes to `crossProduct` as before. An invocation with a constant, a repeated variable, a blank, or a variable the environment binds answers a streaming selection (P3), which carries no index, so its join builds as before. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, each written before the code and run red before it where a red exists.

- `TestDerivedSet_AnIndexAnswersTheViewsTuplesUnderAKey` (executor, new): a set absorbing tuples in several steps with a view taken between the steps; a probe through each view at a key answers that view's tuples holding the key and none admitted after it, and a probe through a later view answers the later tuples too (T2, T4, L1, L5).
- `TestHashJoin_AViewIsProbedAndNeverDrained` (executor, new): a group joined with a view answers the pairs a nested loop computes, each once, with the view on either side and under both planner paths; the strategy event names the kept index as the build side, no `join/build.copy` event fires, and the probe event's tuple count is the group's (T1, T3, L2, L4).
- `TestRunStratum_ACopysJoinIntoAHeldPredicateProbesTheDeltaAlone` (executor, new): over `[(step ?a ?b) [?a :page/next ?b]] [(reach ?x ?y) (step ?x ?y)] [(reach ?x ?z) (reach ?x ?y) (step ?y ?z)]` on a chain of pages, at every iteration after the first the probe event of the copy's join into `step` reports as many probes as the iteration's delta of `reach` holds, no build event drains `step`, and the answer is the chain's closure (T1, T3, L4, L6).
- `TestAJoinOfTwoSetsAnswersEachPairOnce`, `TestHashJoinEmitsStructuredStrategyBuildAndProbeAnnotations` and the join index tests (executor), run unchanged (T5).
- `TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty`, `TestEvaluateRules_TwoRecursiveOccurrences`, `TestRunStratum_RunsTheCopiesOfThePredicatesThatGainedTuples`, `TestRunStratum_ADeltaCopySeeksItsPatternOnTheDeltasKeys` and the rule evaluation tests (executor), run unchanged (T1, L6).
- `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs`, `TestRules_ScopeShapesOnRandomGraphsMatchTheReference`, `TestRules_InvocationUnderAFormEvaluatesOncePerQuery`, `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` and `TestQueriesAnswerAlikeUnderBothOptimizerModes` (storage), run unchanged (T1).
- The downstream reproduction measures the change, and its figures go to `PERFORMANCE_STATUS.md`.

## 6. The change the proof licenses

- `datalog/executor/rules.go`, `derivedSet`: keeps its indexes by key. An index is made at the first join that asks for its key, over the tuples then held, and extended to the set's count when a join asks for it through a view past the count it holds. `derivedSet.relation` answers a view that carries the set and the view's count.
- `datalog/executor/relation.go`, `MaterializedRelation`: a view carries the set it views and its count, and `Rename` carries both over, since a rename changes no position. No other derivation of a relation carries them.
- A file beside `join_index.go` for the index over ordinals: the one table with ordinals beside the members, probed at positions given per probe and bounded at a count.
- `datalog/executor/join.go`, `HashJoinWithOptions`: a side that is a view is the build side, its set's index at the join key probed at the other side's positions and bounded at the view's count, with nothing drained; when both sides are views, the side with more tuples is the build side. The strategy event reports the kept build side, the build event reports the index's tuples and nothing copied, and the probe event reports the other side's tuples. The candidate-key check reads the index's fanout as it read the built index's. `hashJoinIterator` reads a kept index's matches through the set's tuples by ordinal.
- `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4, the bullet on a held invocation: the hash join probes the index the set keeps and streams the group once. `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3 and `docs/reference/RULES.md`, "Evaluation": the derived relation an invocation reads is materialized and keeps an index per key its joins read.
- The tests of §5.

## 7. What the proof does not cover

- A held invocation read through a streaming selection (P3), whose join drains the selection's tuples as before.
- The memory of an index: one ordinal per tuple per key, and the table.
- A set joined at a key once, whose index costs the drain the join would have made and is then kept.
- The delta relations, which lead their copies and are the probe side of their joins (P5), and the required run under `scope/required`.
- Which keys a set is joined at, which the planner's order decides (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1).

## 8. Alternatives excluded

- The join as it stands (P1): it drains or streams the view at every join, the failure.
- An index built at the invocation's read, over the view, once per run: it drains the view once per run per key, where the kept index drains each tuple once.
- The executor choosing the build side by the sides' sizes at run time: the view's tuples are the cost on either side, and the planner is the one orderer (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1).
- The held invocation taking the windowed route over the group: §4.4 excludes it, a framed plan per window of a body's result at every iteration of every stratum.
- An index at every key of a set up front: the keys a set's joins read are the planner's, known at the first join, and the subsets of a set's positions are exponential in its width.
- A probe unbounded by the view's count: a body would read tuples an earlier body of its run admitted, which changes what each run reads. What a copy reads is the subject of `A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md`, and this proof changes nothing of it.
