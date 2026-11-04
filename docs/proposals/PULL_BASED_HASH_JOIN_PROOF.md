# A pull-based hash join, as a proof

## Sources

- The code: `datalog/executor/join.go` (`HashJoin`, `HashJoinWithOptions`, `hashJoinIterator`, `combineTuplesIndexed`, `SemiJoin`, `AntiJoin`, `crossProduct`, `emitJoinStrategyAnnotation`), `datalog/executor/join_index.go` (`JoinIndex.Add`, `JoinIndex.Probe`, `JoinIndex.Len`), `datalog/executor/tuple_set.go` (`NewTupleSet`), `datalog/executor/relation.go` (the `Relation` and `Iterator` interfaces, `copyTuple`, `collectTuplesInto`, `ForEach`, `CollectTuples`, `CachingIterator`, `StreamingRelation.Iterator`, `StreamingRelation.Size`, `StreamingRelation.RequiresCopy`, `StreamingRelation.Materialize`, `MaterializedRelation.Join`, `StreamingRelation.Join`, `NewProductRelation`, `ProductRelation.Size`), `datalog/executor/relations.go` (`Relations.Collapse`, `reportJoin`), `datalog/executor/query_executor.go` (`DefaultQueryExecutor.Execute`, `renderConstantFindSymbols`, `existenceVerdict`, `executePattern`, `tryFuseAttributeFetchBundle`, `executeExpression`, `executePredicate`, `executeSubquery`, `findOuterRelation`, `applyFindPulls`, `replaceConsumedGroups`), `datalog/executor/executor.go` (`ExecuteWithRelations`, `ExecuteRealized`, `executeRealizedPlan`, `runPhases`, `declaredTupleCount`, `executePhase`), `datalog/executor/executor_utils.go` (`BindQueryInputs`), `datalog/executor/scope.go` (`requiredRun`, `scopeResult`, `materialized`, `hasTuple`, `executeOrJoinClause`, `executeOrDefaultJoinClause`, `executeNotJoinClause`), `datalog/executor/bound_pattern.go` (`streamsThrough`, `executeBoundPattern`, `patternJoinIterator`), `datalog/executor/bound_invocation.go` (`executeBoundInvocation`), `datalog/executor/rules.go` (`executeRuleInvocation`, `invocationIterator`, `seedRelation`), `datalog/executor/windowed_form.go` (`newWindowedIterator`), `datalog/executor/window_reader.go` (`windowReader`), `datalog/executor/or_fallback_relation.go` (`OrFallbackRelation`), `datalog/executor/relation_ops.go` (`materializeRelationsForPattern`, `filterWithPredicateAndLookup`, `evaluateExpressionWithLookup`, `unionRelations`), `datalog/executor/theta_join.go` (`thetaJoinPair`, `crossJoinWithExpression`), `datalog/executor/expression_binding.go` (`bindingTuples`, `bindingAlignment.apply`), `datalog/executor/buffered_iterator.go` (`BufferedIterator`), `datalog/executor/iterator_composition.go` (`ProjectIterator`, `PredicateFilterIterator`, `FunctionEvaluatorIterator`, `DedupIterator`), `datalog/executor/limit_relation.go` (`LimitRelation.take`), `datalog/executor/aggregation.go` (`StreamingAggregateRelation.fold`), `datalog/executor/phase_boundary.go` (`materializePhaseBoundary`), `datalog/executor/symmetric_hash_join.go` (`ChooseJoinStrategy`), `datalog/storage/convenience.go` (`Database.Query`), `datalog/storage/database.go` (`DefaultPlannerOptions`).
- The code P9 to P13 read: `datalog/executor/windowed_form.go` (`canFailOnAValue`, `subqueryFoldCanFail`, `canFailOnAValueOver`, `expressionFailsOnNoValueOver`, `entityIsAnIdentity`, `windowedIterator.Next`, `windowedIterator.failedTuple`, `windowedIterator.resume`, `tupleToAnswer`), `datalog/executor/window_reader.go` (`carriedFailure`, `failureAt`, `unknownAt`, `windowFailure`), `datalog/executor/iterator_composition.go` (`failedTupleIterator`), `datalog/executor/bound_pattern.go` (`patternJoinIterator.failedTuple`, `patternJoinIterator.resume`), `datalog/executor/aggregation.go` (`aggregateOps`, `resolveAggregate`), `datalog/executor/relation.go` (`CountingIterator`, `ProductIterator`, `productEvaluates`), `datalog/executor/relation_properties.go` (`RelationProperties`, `AnswerIdentities`, `joinProperties`, `orProperties`), `datalog/executor/theta_join.go` (`thetaJoinWithPredicate`), `datalog/executor/subquery.go`, `datalog/executor/not_join.go`, `datalog/executor/or_fallback_relation.go` (`NewOrFallbackRelation`), `datalog/query/database_function.go` (`toIdentity`), `datalog/storage/matcher.go` (`typedPositionBindingCheck`), `datalog/storage/matcher_relations.go`, `datalog/storage/hash_join_matcher.go`, `datalog/storage/convenience.go` (`sessionMatcher`).
- The proof this one stands on: `docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md`. Its §1 defines a set, a key, the join and the combined tuple. Its P2 states the one equality E. Its L2 enumerates each matching pair once in both hash arms.
- The design record: `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`. Its law that `Materialize` guarantees replayability and not eager realization. Its "Materialization That Should Remain" list, with hash build state on it. Its mandatory gates.
- The scope design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md`.
  - §4.2, "The window": every subject is read in windows, and a subject of several groups is a fold of the hash join with the join so far as the streaming probe side, so the subject reports no size.
  - §4.4, "A data pattern over a streaming group" and "A bound invocation at the top level": a pattern or invocation over a group reporting no size takes the window, and a group held in memory binds through the matcher.
  - §4.8: a streaming subject is read once, and a reader that needs a result twice materializes the result.
- The failure: the `unfilled` query of the downstream's first report, over the dump and the rule set that report shipped, which the tree does not carry. The query over the kernel's facts runs for minutes, and the report names what shows the fix. `PERFORMANCE_STATUS.md`, in its "Join output is a set" entry, records that run's allocation profile, read with the CLI's `-memprofile`: the join's bytes are the materialized arm's results slice and its objects are `combineTuplesIndexed`'s, one combined tuple per pair.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **E**, a **set**, a **key**, the **join** of L and R on J, and the **combined tuple** of a pair are as `JOIN_OUTPUT_IS_A_SET_PROOF.md` §1 defines them.
- A **stream** is a `StreamingRelation`. Its `Iterator` may be called once. A second call without `Materialize` before it panics. `Materialize` marks it for caching, and the next `Iterator` builds the cache through a `CachingIterator`, which copies every tuple it hands back into the cache.
- The **copy answer** is `Iterator.RequiresCopy`. It is true when the slice `Tuple` hands back is one the next `Next` overwrites, so a reader that keeps a tuple past the next pull copies it. It is false when every tuple is the caller's to keep. `Relation.RequiresCopy` is the answer of the iterator `Iterator` hands out. A `StreamingRelation` answers false once it caches and its iterator's answer otherwise.
- The **size answer** is `Relation.Size`. A `StreamingRelation` answers its cache's length when the cache is ready, its known size, its materialization's size, or its counter's count once its iterator is done. Otherwise it answers -1 without opening the iterator.
- The **index** is the `JoinIndex` the build side is held in: one group of held tuples per distinct key, probed by positions with no allocation.
- The **join's iterator** is `hashJoinIterator`: the index, the probe iterator, the deferred build error, the projection plan, the current probe tuple, the current match group and its position.
- The **symmetric join's iterator** is `symmetricHashJoinIterator`, which `ChooseJoinStrategy` selects under `EnableSymmetricHashJoin` when both sides stream. It keeps an index per side, reads each side in batches into its own index, and probes the other side's index with each tuple.
- The **join's failure rule** is `joinFailure`, over what a join read of each side: whether the side yielded a tuple, and the failure it ended with. Where either side ended with no tuple and no failure, the rule answers no failure. Otherwise it answers the first side's failure, else the second's.
- The **workspace** is one slice of the result's width the join's iterator owns, fills on each pull, and hands back from `Tuple`.
- An **empty build** is an index holding no tuple after the build side is drained.
- A **consumer** of a join result is a function that receives the relation `HashJoinWithOptions` answers, through `HashJoin`, a relation type's `Join` or `HashJoin`, `Relations.Collapse` or `findOuterRelation`.
- An **evaluation failure** is an error a clause's function raises on a tuple, such as `+` over an operand that is not a number (`docs/reviews/ANTIPATTERN_AUDIT_2026_07.md`: arithmetic operands must be numbers, and a non-numeric operand is an error; `arithmetic_strict_test.go` pins it).
- The **failure contract**: an evaluation failure fails the query only at an assignment where every other clause holds.
  - The goal it serves: a query that fails under one optimizer path fails under the other, the two paths observationally equivalent for every query the engine accepts (the Optimizer Mode Matrix header, `datalog/storage/optimizer_modes_test.go`).
  - A reading that raises the failure wherever a plan evaluates the function depends on which clause binds the function's inputs first, so two plans of one query answer differently, against that goal.
  - Under the contract, an assignment where some clause does not hold fails no query, whatever another clause raises there. That is the conjunction's reading, commutative and associative over the clauses, so every plan answers alike.
  - `TestAJoinWithAnEmptySideAnswersEmptyThoughTheOtherSideFails` (storage) pins it.
  - The engine departs from the contract in shapes §7 lists.
- The **pattern join iterator** is `patternJoinIterator`: the windowed join of a subject with a pattern. At its first pull it reads whether the pattern alone has a match, through the matcher and `hasTuple`, and it answers the windowed join's tuples only where the pattern has one.
- The **identity symbols** of a relation are `RelationProperties.Identities`: the symbols whose every value is an identity, as a data pattern's entity position binds them.
- The **evaluating mark** is `RelationProperties.Evaluates`. It says that reading the relation evaluates, as tuples are pulled, a function that can fail on a value, which `canFailOnAValue` decides:
  - an expression whose function reads a variable
  - a predicate function or a transaction range in a lazy filter
  - a database-function predicate whose function reads a variable, which fails on an entity value that is no identity
  - a windowed form's body holding one of these
  - a subquery whose `:find` sums or averages, whatever its call passes, since sum and avg fail on a value that is no number and on a NaN sum (`aggregateOps.failsOnAValue`, `subqueryFoldCanFail`)
- For one predicate or expression over a relation, `canFailOnAValueOver` decides the mark from the relation's identity symbols.
  - A database function whose entity is an identity symbol fails on no value, read as a predicate or under an expression's scalar form (`expressionFailsOnNoValueOver`).
  - An expression's tuple, collection and relation forms read the value's shape, and fail on a value of another shape whatever the entity (`bindingTuples`).
- A held relation carries no evaluating mark.
- A **failed tuple** is the tuple an iterator's own evaluation failure arose on. A `failedTupleIterator` answers it over the iterator's symbols, with no value at the positions the failing evaluation would have filled.

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
    - `filterWithPredicateAndLookup`, when it reads the relation at the clause; its stream is a `PredicateFilterIterator`'s otherwise (P12)
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
    - `FunctionEvaluatorIterator` when no binding symbol extends the tuple, and through it the stream `evaluateExpressionWithLookup` answers
    - `DedupIterator` when it does not copy
    - `patternJoinIterator`, the windowed join's answer
  - Readers that keep no source tuple:
    - `unionRelations`
    - `StreamingAggregateRelation.fold`
    - `ProjectIterator`
    - `FunctionEvaluatorIterator` when a binding symbol extends the tuple
    - `invocationIterator` when it projects
    - `thetaJoinPair`'s combined tuple
    - `combineTuplesIndexed`
- **P3 (single use and its readers).** A stream is read once unless materialized first.
  - The consumers that read a join result more than once materialize it first: `materializeRelationsForPattern` on a group sharing a pattern's symbol, `NewProductRelation` on every operand but the first, `seedRelation` on a contributing group, and the entity prefetch in `Execute` on the first pattern's last group.
  - One consumer reads a relation twice without materializing it: `executeRealizedPlan` reads the relation `BindQueryInputs` answers. It projects the environment symbols out of it and reads the projection's size. When constant symbols are to be dropped, it reads the relation again through `Materialize().Project`.
  - `BindQueryInputs` answers the fold of `Join` over materialized input relations. Today that fold's last `Join` answers a materialized relation, so both reads hold.
- **P4 (the size and emptiness readers).**
  - `reportJoin` reads the result's size under a handler and reads nothing when the answer is -1.
  - `streamsThrough` reads the size of every group sharing a pattern's or invocation's symbols and routes the clause through the window when one answers below zero.
  - `HashJoinWithOptions` reads its sides' sizes to choose the build side when both are materialized.
  - `HashJoinWithOptions` reads the build side's size for the index's capacity, falling back to `DefaultHashTableSize` or 256 when the answer is negative.
  - `SemiJoin` and `AntiJoin` read the right side's size for the key set's capacity.
  - `hasTuple` reads no size. It pulls one tuple and closes, completing a filling cache, and answers the failure the iterator reports: a held relation's, a replayed cache's or a fold's.
  - `hasTuple`'s callers are the existence verdict, over the groups it decides and over the groups the find reads, the pattern join iterator, and the hash join over an empty build that failed. Each is the last reader of the relation it gives `hasTuple`.
  - `declaredTupleCount` answers -1 when any group declines.
  - `ProductRelation.Size` answers -1 when any operand declines.
  - `ExecuteWithRelations` reads the sizes of the relations the caller gave.
- **P5 (the windowed route).**
  - `executeBoundPattern`, `executeBoundInvocation`, `executeNotJoinClause`, `executeSubquery` and the two or-forms with a subject each build their subject through `findOuterRelation`.
  - `findOuterRelation` joins the groups holding a needed symbol by a fold of `HashJoinWithOptions`, with the later group as the build side and the join so far as the probe side.
  - Each hands the subject to `newWindowedIterator`, which opens the subject's iterator once and reads it through `windowReader` into windows, copying each tuple when the subject says copy. It never reads the subject's size.
  - `newWindowedIterator` takes the subject's iterator and pulls no tuple of it until its own first pull.
  - `executeBoundPattern` wraps its windowed iterator in the pattern join iterator, and its result is the stream over that iterator.
  - `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2 states that every partition of the subject into windows answers the same tuples.
  - §4.4 states that a pattern or invocation over a group reporting no size takes this route, and that a group held in memory binds the pattern through the matcher.
- **P6 (the empty join).**
  - `JoinIndex.Probe` answers nil for every probe tuple when the index holds no tuple.
  - So the join of L and R on J is empty whenever the build side is empty, whatever the probe side holds.
  - Both arms today drain the probe side whatever the index holds: the materialized arm in its probe loop, and the streaming arm's `Next` as the consumer pulls to the end.
- **P7 (the key map's capacity).** `NewTupleKeyMapWithCapacity` passes its argument to `make` as the map's size hint. A negative argument is the size answer of a stream. Withdrawn: `ONE_TUPLE_TABLE_PROOF.md` holds every key in the one table, and `SemiJoin` and `AntiJoin` size their key set with `NewTupleSet`, which sizes the set at its first add for a capacity above zero.
- **P8 (the public boundary).**
  - `Database.Query` answers the executor's final relation as it is, under its stated contract: the caller iterates through `Iterator` and closes the iterator.
  - `Relation` and `Iterator` state the copy answer a caller keeping a tuple reads.
  - `ExecuteRealized` reads the final relation once, through `Sort`, `TopNRelation`, a streaming `Project`, `NewLimitRelation` or `applyFindPulls`, each of which copies what it keeps.
  - `DefaultPlannerOptions` sets `EnableTrueStreaming` true, so a query ending in a scan answers a stream, its final relation answers -1 for its size, and a caller that counts iterates.
- **P9 (the expression clause).**
  - `executeExpression` evaluates a clause over the group holding its inputs through `evaluateExpressionWithLookup`. Over several groups it calls `crossJoinWithExpression`, which calls `evaluateExpressionWithLookup` over the groups' lazy product or over their join at the clause (P12).
  - An expression whose symbols no group carries, a ground expression or one the environment alone binds, extends every group through `evaluateExpressionWithLookup`. With no group it stands over the executor's unit relation.
  - `evaluateExpressionWithLookup` answers a stream over a `FunctionEvaluatorIterator` on its input's iterator, carrying the database function's lookup and the environment's tuple. It pulls nothing before answering.
  - The iterator evaluates the expression on a source tuple when it pulls that tuple from its source. An evaluation failure stops it, and `Error` answers the failure.
  - An expression reading no symbol of its source has one value over it. The iterator evaluates it at the first tuple it pulls, and answers every later tuple from that value (`invariant`).
  - The `$` source's reads in one query observe one snapshot (`sessionMatcher`).
  - `bindingTuples` answers the tuples a binding reads from a value, each once: one under the scalar and tuple forms, one per distinct element under the collection form, and one per distinct inner vector under the relation form.
  - The iterator answers each source tuple joined with each such tuple through `bindingAlignment.apply`. `apply` keeps a tuple only where every bound position unifies, answering a fresh tuple when a binding symbol extends the source tuple and the source tuple itself when none does.
- **P10 (the readers that stop early).**
  - `LimitRelation.take` pulls at most its limit's tuples.
  - `existenceVerdict` reads each group the find does not read until the group shows a tuple, ends, or fails, and decides once every group is read. With a failure to raise, it reads each group the find reads through `hasTuple`, and answers empty when one of them holds no tuple and no failure.
  - `ProductIterator.Next`'s first pull reads every member to its first tuple, its end or its failure. A member ending with no tuple and no failure ends the product with no failure. Otherwise a member that failed ends the product with its failure.
  - Every other reader reads a relation to its end (P2), or reads a held relation.
  - The evaluating mark is set by:
    - `evaluateExpressionWithLookup`, unless the expression fails on no value over its relation's identity symbols (`expressionFailsOnNoValueOver`)
    - `EvaluateFunction` on the stream and on the lazy sequence, when `canFailOnAValueOver` says the function can fail over the relation's identity symbols
    - `FilterWithPredicate` on the stream and on the lazy sequence, when `canFailOnAValueOver` says the predicate can fail over them
    - `executeSubquery`, when its body can fail on a value or its `:find` sums or averages (`subqueryFoldCanFail`)
    - `executeNotJoinClause`, when its body can fail on a value
    - `NewOrFallbackRelation`, when a branch can fail on a value
    - `executeBoundInvocation`, when the invoked rules can fail on a value
  - `joinProperties` takes it from either side.
  - `ProductRelation.Properties` takes it from any member, since the product reads every member as it is pulled: the first as a stream, every other through the cache its first pass fills.
  - Every other derivation of properties carries it.
  - `NewMaterializedRelationWithProperties` clears it.
  - `newMaterializedRelationFromSet` clears it.
  - An expression or a database-function predicate reading no variable answers alike on every tuple, so it fails on the first tuple or on none.
- **P11 (the identity symbols).**
  - Every datom's entity is an identity, and a binding at a pattern's entity position is one (`typedPositionBindingCheck`). `AnswerIdentities` names a data pattern's entity variable.
  - Each storage match arm whose pattern has an entity variable states it among its answer's identity symbols.
  - `executeBoundPattern` states the subject's identity symbols and the pattern's entity variable.
  - `tryFuseAttributeFetchBundle` states its entity variable, since it answers an input tuple only where the tuple's entity is an identity.
  - `clone`, `renameSymbols` and `project` carry them. `addSymbol` drops the symbol it adds, and `withoutReboundSymbols` drops the symbols it rebinds.
  - `joinProperties` takes each side's, since a join keeps each side's values and a shared symbol's are equal on both sides. `ProductRelation.Properties` takes every member's.
  - The held constructors keep the properties they are given. A union states none (`unionRelations`, through `deduplicatedProperties`).
  - `canFailOnAValueOver` and `expressionFailsOnNoValueOver` read them where a filter or an evaluation is built.
- **P12 (the clause-level readers).**
  - `filterWithPredicateAndLookup` answers a stream over a `PredicateFilterIterator`, carrying the evaluating mark, when the predicate can fail on a value of the relation (`canFailOnAValueOver`) or the relation carries the mark. Any other filter reads the relation at the clause into a held relation.
  - `thetaJoinWithPredicate` filters `NewProductRelation` of its groups that way when a group carries the mark (`productEvaluates`) or the predicate can fail on a value. Any other theta join reads its groups at the clause through `thetaJoinPair`.
  - `crossJoinWithExpression` evaluates its expression over `NewProductRelation` of its groups when a group carries the mark, and otherwise over the groups `thetaJoinPair` joins at the clause.
  - `executePredicate` filters the unit relation for a predicate the environment alone binds. A verdict carrying the mark joins the groups as a group of no symbol, and so does a failure the verdict met at the clause, held.
  - `tryFuseAttributeFetchBundle` reads its input group whole at the clause, and declines a group carrying the mark.
- **P13 (the failed tuple and the window).**
  - `PredicateFilterIterator` and `FunctionEvaluatorIterator` answer their failed tuple and resume past it (`failedTuple`, `resume`). The filter's failed tuple has a value at every position. The evaluator's has the source tuple's values, and no value at each binding symbol that extends them.
  - `CountingIterator`, a stream's iterator unless the stream caches, answers the failed tuple of the iterator it counts.
  - A windowed form's iterator answers the output it answered for a marked window tuple (`windowedIterator.failedTuple`), and `patternJoinIterator` answers its windowed join's. No other iterator answers one: `hashJoinIterator` and `CachingIterator` answer none.
  - `newWindowedIterator` gives the window reader the positions the form reads of a subject tuple: the required run's, the key's, and those of the find variables the subject carries.
  - `windowReader.next` takes a failed tuple holding a value at each of those positions into the window, marked with its failure and the positions it holds no value at (`windowFailure`), and resumes the subject (`carriedFailure`). Any other failure of the subject ends the reader with the failure.
  - Each form records the window tuple whose outputs it answers (`tupleToAnswer`): the subquery, which also serves the bound pattern and the bound invocation, the not-join and the fallback.
  - When the form answers an output for a marked tuple, `windowedIterator.Next` stops with the tuple's failure, and `failedTuple` answers the output with the positions the tuple holds no value at (`unknownAt`). `resume` drops the failure, and `Next` answers on from the window in hand.
  - Each form's result lays out its subject's symbols first. The subquery's and the fallback's results are the subject's symbols, then the ones it lacks (`executeSubquery`, `NewOrFallbackRelation`). The bound pattern's are the subject's, then the pattern's other variables (`executeBoundPattern`). The not-join answers the subject tuple. So a failed output's unknown positions are its subject tuple's.
  - A form's result carries its subject's evaluating mark: the subquery's and the fallback's through `orProperties`, and the not-join's as its subject's properties.
  - The fallback deduplicates no output of a marked tuple.

## 3. Theorems

**T1 (the answers).** For L and R sets, in every setting of the options, the join's iterator yields exactly the combined tuples of the join of L and R on J, one per pair. The tuple `Tuple` hands back at each pull is E-equal to that pull's combined tuple.

**T2 (every consumer).** With `BindQueryInputs` answering a held relation, every consumer of a join result reads it once or materializes it first, and every consumer that keeps a tuple past the next pull copies it. So the tuples every query answers are unchanged.

**T3 (realization).** The join holds tuples in the index alone. Beyond the index it holds one workspace and no slice of results.

**T4 (allocation).** Per pair the join allocates nothing. Per probe tuple it allocates nothing, on a hit and on a miss alike. Per join it allocates the index, the workspace, the iterator and the relation.

**T5 (the empty build and a side's failure).** When the build side is empty the join answers the empty relation over the result's symbols and properties. A build that read no failure leaves the probe side unopened, as its next reader finds it. A build that failed reads the probe side to its first tuple, its end or its failure, its one use, and the result carries the build's failure only where the probe side held a tuple or a failure. When the build side holds a tuple, the join's iterator answers the build's failure only where the probe side yielded a tuple or failed. The symmetric join's iterator answers a side's failure only where the other side yielded a tuple or failed.

**T6 (the route).** A join result whose build holds a tuple answers -1 for its size, and an empty build's result, the empty relation held (T5), answers 0. A later pattern or invocation sharing the first's symbols takes the windowed route, which reads the join result once and answers the tuples the matcher's route answers, and one sharing the second's takes the held route over the empty relation. A scan failure on the windowed route surfaces when the result is driven, where the matcher's route surfaced it from the call. At the public boundary a query whose final relation is a join result with a build, or a projection of one, answers -1 for its size, as a query ending in a scan does (P8), and a caller that counts iterates; a query whose final relation is an empty build's answers 0.

**T7 (cost).** The per-pair path loses the call to `combineTuplesIndexed` and the allocation it made, and gains no call: the fill is written in `hashJoinIterator.Next`.

**T8 (the expression clause).** An expression clause's result is a set, reports no size, and evaluates nothing at the call. That holds over the groups carrying the expression's symbols, and over every group or the unit relation where none does. The expression is evaluated on a source tuple only when the result is pulled to it, so a source tuple nothing pulls is never evaluated, and an evaluation failure surfaces when the result is driven. An expression reading no symbol of its source is evaluated once, at the first tuple pulled, and its value answers every tuple. A later pattern or invocation sharing the result's symbols takes the windowed route (T6).

**T9 (the windowed pattern's empty side).** When the pattern alone has no match, the windowed route's result answers nothing and pulls nothing of its subject. The route reads the pattern's matches at its first pull, so a failure reading them surfaces when the result is driven, as the route's scan failures do (T6).

**T10 (a failure under an empty side).** When an expression clause's result is the probe side of a join whose build is empty (T5), or the subject of a pattern with no match (T9), its evaluation failures are not raised, and the join answers the empty relation. A build side's failure beside a probe side holding no tuple and no failure is not raised either (T5). That is the failure contract's answer: no assignment satisfies the empty side's clause, so none fails the query.

**T11 (a reader that stops early).** A failure at a tuple of a relation carrying the evaluating mark, at an assignment where every other clause holds, is the query's error through every reader.
- `:limit` reads an evaluating source to its end after the tuples it keeps.
- The existence verdict reads an evaluating group to its end once every group has shown a tuple, and reads no group past its first tuple when one has none.
- A group's failure in place of a first tuple, an evaluating group's or one the group keeps from reading its source, is the query's error only when no group is empty, neither a group the verdict decides nor a group the find reads.
- A product's member failing in place of a first tuple fails the product only when no member is empty.
- A relation that carries no mark keeps the early stop. A `:limit 0` stops before its first tuple (§7).

**T12 (a clause-level reader of a relation that can fail).** A filter or theta reader whose result can fail on a value raises none of its evaluation failures at its clause, and the fusion bundle reads no group carrying the evaluating mark. Each answers a stream carrying the mark, or leaves one to the pattern's windowed route, so the failure surfaces where the stream is driven.

**T13 (a windowed form's failed subject tuple).** Let a windowed form's subject fail at a tuple its iterator answers, the tuple holding a value at every position the form reads. The form reads its subject past the tuple. It raises the failure only where it answers an output for the tuple, and drops the tuple with its failure where it answers none. Where it raises the failure, it answers the output as its failed tuple, so a windowed form over its result decides the output in turn.

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

### L6. The empty build and the build's failure (T5)

By P6 the join is empty when the index holds no tuple, whatever the probe holds. So the empty relation over the result's symbols and properties is the answer, and no pull of the probe side is needed to compute it. The build side was drained, so its deferred error is in hand.

A build that read no failure leaves the probe side's iterator uncalled. So a stream among the probe sides keeps its one use, and a caching stream records no incomplete materialization.

A build that failed has the join read the probe side through `hasTuple` (P4). The join is the probe side's last reader, since it answers without the probe side's tuples. A probe side with no tuple and no failure holds at no assignment, so the build's failure fails no query, and the result carries none. Otherwise the result carries the build's failure, and the probe side's own failure is not raised beside it.

When the build holds a tuple, the join's iterator holds the current probe tuple. Its `Error` answers the build's failure once a probe tuple was read or the probe failed. Otherwise it answers the probe side's error, which is nil for a probe side that ended with no tuple.

The join's iterator computes this answer through the join's failure rule. The build side is a side that yielded a tuple, and the probe side is one that yielded once a probe tuple was read. The empty build's branch computes its answer through the rule too, the build side as one that yielded none and the probe side as `hasTuple` read it.

The symmetric join's iterator reads each side in batches until the side ends, by its end or its failure. Its `Next` answers false only once both sides have ended. It records whether each side yielded a tuple, and its `Error` answers the rule over the two sides. A side that ended with no tuple and no failure holds at no assignment, so no failure is raised beside it. Where both sides yielded a tuple or failed, the rule answers the left side's failure, else the right side's. ∎

### L7. The route (T6)

By P4 `streamsThrough` reads the size of every group sharing the clause's symbols. By the definition of the size answer a stream answers -1 and a held relation its count, so a join result with a build answers -1 and an empty build's result answers 0 (L6). So a pattern or invocation sharing a join result's symbols is routed through `executeBoundPattern` or `executeBoundInvocation` when the build holds a tuple, and through the held route over the empty relation when it does not. By P5 the windowed route reads its subject once, copying under the copy answer. `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2 states that the window answers the tuples the subject's join with the clause answers. Today the same clause over a materialized join result is routed through `executePattern`, which materializes the group for the matcher's bindings and joins the matches at the collapse. Both routes answer the join of the group with the clause's relation. ∎

### L8. The key set (T2)

`SemiJoin` and `AntiJoin` size their key set from the right side's size answer, and a join result with a build answers -1, an empty build's result 0. `NewTupleSet` sizes the set at its first add for a capacity above zero, so a negative capacity starts the set empty, as `TestTupleSet_WithAnUnknownCapacityStartsEmpty` pins, as a capacity of 0 does (`ONE_TUPLE_TABLE_PROOF.md`). So the set is the set a right side of unknown size, or of no tuple, gets. The semi-join and anti-join read their right side once and their left once as before. ∎

### L9. The expression clause's stream (T8)

By P9 every expression clause reaches `evaluateExpressionWithLookup`. It does so over the group holding its inputs, over the groups `crossJoinWithExpression` joins, or, where no group carries its symbols, over every group or the unit relation. `evaluateExpressionWithLookup` pulls nothing before answering, and the iterator evaluates a source tuple only when it pulls that tuple. So the call evaluates nothing, and a failure is the iterator's error, answered when the result is driven. The result is a stream, so it answers -1 for its size (P4), and a later clause sharing its symbols is routed through the window (L7).

An expression reading no symbol of its source reads only constants and the environment's tuple, which binds alike into every evaluation. A database function among such expressions looks one entity and attribute up, and on `$` the query's reads observe one snapshot (P9). So every evaluation on `$` answers the value the first answers, and the iterator's one evaluation answers each tuple what evaluating it there answers.

The source is a relation, so it holds each tuple once. Each answered tuple is `apply`'s output over its source tuple and carries that tuple whole at the source's positions, so two source tuples never answer the same tuple. Over one source tuple, `bindingTuples` answers each tuple once (P9). Two distinct such tuples that both unify agree at every bound position and at every position of a symbol written twice, so they differ at an extending position and answer distinct tuples. So the result holds each tuple once. ∎

### L10. The windowed pattern's empty side (T9)

A binding only narrows a pattern, so every match the pattern answers under the subject's bindings is a match of the pattern alone. Where the pattern alone has none, the windowed join is empty. By P5 the route's result is the stream over the pattern join iterator, whose first pull reads the pattern alone's matches through `hasTuple` (P4). Where they hold none, it answers nothing and returns before pulling the windowed iterator, which by P5 pulls no tuple of the subject until its own first pull. A failure from the matcher or from `hasTuple` is held and answered by `Error`, so it surfaces when the result is driven. ∎

### L11. A failure under an empty side (T10)

By L6 a join whose build is empty raises none of its probe side's failures. It leaves the probe side unopened when the build read no failure, and otherwise carries the build's failure or none. By L10 a pattern with no match leaves its subject unread. By L9 an expression clause's result evaluates a source tuple only when pulled, so in neither case is a failure of the expression raised. Both answer the empty relation. Under the failure contract, an assignment where the empty side's clause does not hold fails no query, and that clause holds at no assignment, so the empty relation is the query's answer. ∎

### L12. A reader that stops early (T11)

By P10 the readers that stop before a relation's end are `LimitRelation.take`, `existenceVerdict` and a product's first pull, and a relation carries the evaluating mark wherever its reading evaluates, as tuples are pulled, a function that can fail on a value.

Any other relation without the mark evaluates nothing that can fail past its first tuple. Its tuples come from scans, from held relations, and from evaluations of three kinds:

- a predicate that fails on no value
- a function reading no variable, an expression's or a database-function predicate's, which fails on every tuple alike
- a database function whose entity is an identity symbol, read as a predicate or under an expression's scalar form, which fails on no value (P11)

So a prefix of one tuple or more raises every evaluation failure it holds, and the early stop answers what a read to its end answers. A `:limit 0` reads no tuple of it, so a failure at its first tuple is not raised (§7).

A relation with the mark is read to its end by both readers. A limit keeps its first tuples and reads the rest of the source, so a failure past them is the limit's error.

The existence verdict reads past a first tuple only once every group has shown one. Each tuple of an evaluating group then meets a tuple of every other group, so a failure on it is at an assignment where every other clause holds, and the failure contract makes it the query's error. When a group has none, no assignment holds every group, and the verdict raises no failure past a first tuple, as T10 states for an empty side.

A group can fail in place of a first tuple. An evaluating group's first pull can fail, and `hasTuple` answers the failure a group keeps (P4). The verdict reads every group to its first tuple, its end, or its failure before it decides. When some group ends with no tuple and no failure, no assignment holds every group, so no group's failure is raised. When none does, the first failure is the verdict's. Where every other group holds a tuple, the groups are disjoint, so the failing tuple meets a tuple of each in an assignment where every other clause holds. Where another group failed in place of a tuple too, the verdict raises the first failure (§7).

With a failure to raise, the verdict reads each group the find reads through `hasTuple`, as their last reader (P4, P10). One with no tuple and no failure holds at no assignment, so the verdict answers empty and raises nothing. Otherwise the failure is the query's error.

A product's first pull reads its members as the verdict reads its groups (P10). A member with no tuple and no failure ends the product with no failure, since no assignment holds every member. Otherwise the first failed member's failure is the product's error. ∎

### L13. A clause-level reader of a relation that can fail (T12)

By P12 a filter over a relation carrying the mark, or under a predicate that can fail on a value of the relation, answers a stream over a `PredicateFilterIterator`, carrying the mark. A theta reader over a group carrying the mark, or under such a predicate, filters or evaluates over `NewProductRelation` of its groups, a stream. Neither pulls a tuple when it answers: the iterator and the product take their inputs, and read them at the stream's first pull.

A predicate the environment alone binds is evaluated at its clause, on the unit relation's one tuple. Its verdict joins the groups as a group of no symbol, holding any failure the evaluation met, so the failure is not raised at the clause.

The fusion bundle declines a group carrying the mark. Every held constructor clears the mark (P10), so such a group is a stream, which reports no size, and the pattern takes the windowed route (P5). The route pulls nothing of its subject at the call.

So no clause-level reader raises an evaluation failure at its clause where its result can fail on a value. ∎

### L14. A windowed form's failed subject tuple (T13)

By P13 the reader takes such a tuple into its window, marked with its failure, and resumes the subject, so it reads on to the subject's later tuples. The tuple holds a value at the required run's positions and the key's, so its key enters the window's run, and each branch is built over the window's keys (P5). The form answers outputs for a window tuple from its branches' results at the tuple's key, read against the positions it reads, all of which the tuple holds. So its answer for the failed tuple is its answer for an assignment extending the tuple's values.

Where the form answers an output, its clause holds at such an assignment, and every clause that built the subject's tuple holds there too. `Next` stops with the failure, and the form answers the output as its failed tuple, its unknown positions the subject tuple's (P13). The failure reaches the form's reader with the subject's mark, so the readers that stop early read it (T11). A windowed form reading this form's result carries the output into its window where the output holds a value at each position it reads, and decides it at its own key, by this lemma.

Where the form answers none, its clause holds at no assignment extending the tuple, so the failure fails no query, and the tuple is dropped. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, mapped to the record's mandatory gates.

- Gate 1, the semantic differential:
  - `TestAJoinOfTwoSetsAnswersEachPairOnce` (executor), run with `EnableStreamingJoins` true and false, asserts the same pair set as a nested loop computes, each pair once, in every arm (T1, T6).
  - `TestQueriesAnswerAlikeUnderBothOptimizerModes` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (storage) are the differentials over real queries and rule evaluation, run unchanged (T2).
- Gate 2, the shapes: the same test's cases for an empty side, one tuple on each side, a key repeated on one side, on both, and a large input (T1).
- Gate 3, iterator and close errors:
  - `TestHashJoinPropagatesBuildAndProbeCloseErrors` (executor). The build case runs unchanged. The probe case drives the join through `CollectTuples`, which reads what `Close` returns, since the join's iterator returns the probe's close error from `Close` (P2).
  - `TestHashJoinSurfacesErroredEmptySide` (executor), run unchanged. It joins a relation with an errored empty one. Whichever side the join takes as its build, the error reaches the result: as the build's deferred error carried by the empty-build answer, or as the probe's error when the probe is opened under a non-empty build (T5).
  - `TestHashJoinRaisesABuildFailureOnlyWhereTheProbeSideHoldsATuple` (executor, new). A build failing at its first tuple, its index empty, or past it fails nothing beside an empty probe side, and fails the join beside a probe side with a tuple (T5, T10).
  - `TestEveryJoinStrategyAnswersASideFailureOnlyWhereTheOtherSideHoldsATuple` (executor, new). Under the hash join and the symmetric join, a side failing at its first tuple or past it answers no tuple and no failure beside an empty side, and the failure beside a side with a tuple. Both sides failing answer the failure, and both holding tuples answer their pair (T5, T10).
  - `TestJoinFailureIsTheFirstSidesFailureUnlessASideEndedEmpty` (executor, new). The join's failure rule answers each case of what two sides yielded and ended with (T5).
  - `TestReviewMark_AFailureBesideAnEmptyJoinSideFailsNoQueryUnderEveryJoin` (storage, new) and `TestRefuteMark_EveryJoinStrategyAnswersAFailureBesideAnEmptySideAlike` (db, new). Under `EnableSymmetricHashJoin`, a failing group joined with an empty group answers empty in either clause order, on both optimizer paths, as under the hash join (T5, T10).
  - `TestAFailureBesideAClauseHoldingNowhereFailsNoQuery` (storage, new). A failing group joined with an empty group on the failing expression's output answers empty in either clause order, on both optimizer paths (T5, T10).
- The materialized arm's contract, which the executor's older tests read, moves to the stream's (T4, T6):
  - `TestHashJoin`, `TestJoinMultipleSymbols`, `TestEmptyJoin`, `TestDatomJoinScenario`, `TestStreamingRelationSingleUseInJoin`, `TestEqualVectorValuesJoin`, `TestSignedZeroHashesConsistentlyAcrossSetOperations` and `TestRelationsCollapse` count the tuples they collect in place of reading the join result's size.
  - `collectTuples` in `join_test.go` and `assertNoDuplicates` in `set_semantics_test.go` copy a kept tuple under the copy answer.
  - `TestHashJoinEmitsStructuredStrategyBuildAndProbeAnnotations` asserts the streaming mode.
  - `TestOrderByScalarInputConstantKeyIsNoOp` and `TestExpressionOntoBoundVariableUnifies` collect a query's final relation in place of reading its size and indexing it (P8).
  - `TestRelationInput_PropagatesMatcherErrorAfterAnEarlierPattern` drives the result and reads the matcher's error there.
  - `TestAEVTIndexBugDirect` (`datalog/storage/aevt_bug_test.go`) collects the query's final relation through `CollectTuples` and counts the tuples collected in place of reading its size (P8).
  - `TestExecutorEmptyResult` and `BenchmarkParallelDecorrelation` (executor) count the tuples they collect in place of reading the final relation's size (P8).
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
- The key set: `TestTupleSet_WithAnUnknownCapacityStartsEmpty` (executor). `NewTupleSet` with a capacity of -1 answers an empty set that admits a key (L8).
- The route: `TestAPatternOverAJoinResultTakesTheWindow` (executor, new). A pattern after a join of two groups is answered through the windowed route, the window event reporting the pattern's form, and the answers equal the matcher route's over the same data (T6, L7).
- The expression clause (T8, L9):
  - `TestExpressionEvaluatesAsItsResultIsPulled` (executor, new). Executing `(+ ?n 1)` over a string answers the relation over `?n ?m` with no error, and the failure surfaces when the relation is driven.
  - `TestEvaluateExpressionUnifyPassThroughAnswersItsSourcesCopyAnswer` (executor). The unify pass-through answers a workspace-reusing source's copy answer (P2).
  - `TestSubquery_ACollectionArgumentNamingAMemberTwiceCountsItOnce` and `TestQueryShapesOnRandomGraphsMatchTheReference` (storage), run unchanged. A collection holding a member twice reads it once.
  - `TestExecuteExpression`, `TestExecuteEnumerateUnderTheTupleForm`, `TestCustomFunctionQueryEndToEnd`, `TestEvaluateExpressionWithLookup_PropagatesIteratorError` and `TestEvaluateExpressionWithLookup_PropagatesEvalError` (executor) count the tuples they collect and read a failure where the result is driven (P8).
  - `TestAnExpressionReadingNoSymbolOfItsRelationIsEvaluatedOnce` (executor, new). An expression reading no symbol of its relation is evaluated once over three tuples.
  - `TestReviewEval_AFailureBesideAnEmptyClauseFailsNoQuery` (storage). An expression over an input alone, and a ground expression, each beside an empty pattern, answer empty on both optimizer paths.
  - `TestAFailureBesideAClauseHoldingNowhereFailsNoQuery` (storage, new). A failing ground expression extending the find's group beside an empty group answers empty, and the ground expression's group alone answers its failure.
  - `TestGroundExpressionProducingNaNIsError`, `TestExecuteTupleGround` and `TestExecuteRelationGround` (executor) read a ground expression's failure where the result is driven (P8).
- The windowed pattern's empty side (T9, L10):
  - `TestExecuteBoundPatternWithNoMatchLeavesItsSubjectUnread` (executor, new). A pattern with no match answers no tuple, and its subject's iterator is never pulled.
  - `TestRelationInput_PropagatesMatcherErrorAfterAnEarlierPattern` (executor), run unchanged. The matcher's failure surfaces at the drive, not the call (T6).
- The failure contract: `TestAJoinWithAnEmptySideAnswersEmptyThoughTheOtherSideFails` (storage, T10, L11).
  - Four queries joining a failing expression with a side holding no tuple answer the empty relation with no error.
  - The failing side alone answers its failure.
  - With a fact on the other side, the queries joining on the expression's output answer the failure, and the queries joining on the entity answer empty.
- The readers that stop early (T11, L12):
  - `TestAnEarlyReaderReadsAnEvaluatingRelationToItsEnd` (executor, new). A limit and the existence verdict over an expression's result answer a failure past the tuple they keep, and the verdict pulls one tuple of a relation evaluating no function.
  - `TestTheExistenceVerdictRaisesAFailureOnlyWhereEveryGroupHolds` (executor, new). A union over a failing branch, holding the tuples before the failure or none, and an expression's result failing at its first tuple each fail the verdict beside a group with a tuple, and fail nothing beside an empty group, on either side of it, whether the other group is the verdict's own or one the find reads.
  - `TestTheExistenceVerdictLeavesAHeldGroupUnreadWithoutAFailure` (executor, new). With no failure to raise, the verdict pulls no tuple of a group the find reads.
  - `TestProductRelation_AMembersFailureIsItsFailureOnlyWhereEveryOtherMemberHasATuple` (executor, new). A member failing in place of a first tuple fails the product beside a member with a tuple, and fails nothing beside an empty member, on either side of it.
  - `TestAFailureBesideAClauseHoldingNowhereFailsNoQuery` (storage, new). A group failing at its first tuple, or past it, beside the find's empty group answers empty. So does a find or an aggregating find spanning a failing group and an empty group, in either order, on both optimizer paths.
  - `TestTheExistenceVerdictReadsAFailureARelationKeepsBehindItsSize` (executor, new). An aggregate whose fold of an expression's result failed, and a stream whose replay cache completed with its source's failure, each fail the verdict beside a group with a tuple, and fail nothing beside an empty group, on either side of it.
  - `TestALimitOverEvaluateFunctionAnswersAFailurePastItsTuples` (executor, new). On the stream and the lazy sequence, a limit over a failing function's result answers the failure, and over a function reading no variable it pulls the one tuple it keeps.
  - `TestALimitOverFilterWithPredicateReadsAsThePredicateCanFail` (executor, new). On the stream and the lazy sequence, a limit over a transaction range answers its failure past the kept tuple, and over a comparison it pulls the one tuple it keeps.
  - `TestCanFailOnAValue` (executor, new). Each clause kind's verdict, through header forms, a subquery's body and the rule bodies an invocation reaches. A clause that can fail keeps the verdict when a function reading no variable follows it.
  - `TestCanFailOnAValueReadsASubquerysFoldingAggregates` (executor, new). A subquery whose `:find` sums or averages can fail on a value, whether or not its call passes a variable. One whose `:find` counts or takes a min or a max cannot.
  - `TestAnAggregatingSubqueryWhoseAggregateFailsOnAValueFailsTheQueryThroughEveryReader` (storage, new). Under sum and avg, a limit, the existence verdict, a not-join body, an or-join branch and `QueryOneInto` over a correlated subquery whose aggregate fails at a group an outer tuple reaches fail the query, on both optimizer paths.
  - `TestACorrelatedAggregateFailingAtOneGroupAnswersByTheFailureContractOnBothOptimizerPaths` (storage, new). Under sum and avg, a `:limit 0` over a correlated subquery whose aggregate fails at a group an outer tuple reaches fails the query, on both optimizer paths.
  - `TestACorrelatedSubqueryWhoseBodyFailsAtOneKeyAnswersByTheFailureContractOnBothOptimizerPaths` (storage, new). A correlated subquery whose body fails at a key an outer tuple reaches fails the query, read whole or through a `:limit 0`, on both optimizer paths.
  - `TestALimitOverAProductAnswersAMembersFailurePastItsTuples` (executor, new). A limit over a product, or over its projection, answers a member's failure past the tuple it keeps, the evaluating member first or second, and over members evaluating nothing it pulls the one tuple it keeps.
  - `TestAnEarlyReaderReadsAJoinOfAnEvaluatingSideToItsEnd` (executor, new). A limit and the existence verdict over a hash join or a symmetric hash join of an expression's result with a held relation answer the expression's failure past the tuple they keep, the expression's result on either side (`joinProperties`). A limit over a join of sides evaluating nothing pulls the one tuple it keeps.
  - `TestAFailureWhereEveryOtherClauseHoldsFailsTheQueryWhereverItStands` (storage, new). Two groups the find does not read, an order-by with a limit, a limit over a find spanning the failing group and another, a correlated subquery, an or-join and a not-join run over the group, and an or-join and an or-default-join with no required run, on both optimizer paths.
  - `TestAMissingFailureWhereEveryOtherClauseHoldsFailsTheQueryFromAWindowedBody` (storage, new). A `missing?` over a value naming no entity fails the query from a not-join body, an or-join branch and a correlated subquery's body run over a group the find does not read, on both optimizer paths.
  - `TestReviewEval_AFailureWhereEveryOtherClauseHoldsFailsTheQuery`, `TestReviewEval_AFailureDoesNotDependOnTheOrderAScanYields` and `TestRefuteEval_AFailureWhereEveryOtherClauseHoldsFailsEveryReadEntry` (storage), and `TestReviewEval_CLIAFailureWhereEveryOtherClauseHoldsIsTheQuerysError` (cmd/datalog).
  - `TestRefuteEval_AFailingGroupBesideAnEmptyGroupFailsNoQuery` (storage). A group failing at its first tuple beside an empty group answers empty, on either side of it, on both optimizer paths.
  - `TestLimitRelationStopsScanAfterN` (executor), run unchanged. A limit over a relation without the mark stops early.
  - `TestOrFallbackMaterializationPreservesProperties` (executor), run unchanged. A fallback whose branch reads no variable carries no mark.
  - `TestStreamingRelationPropertyPropagation` (executor). `EvaluateFunction` over a function reading variables answers the source's ordering and keys with the mark.
- A clause-level reader of a relation that can fail (T12, L13):
  - `TestAFailureBesideAClauseHoldingNowhereFailsNoQuery` (storage, new). Each query answers empty on both optimizer paths, and each failing clause alone answers its failure:
    - a missing? predicate's group beside the find's empty group
    - a missing? over an input alone, and over a constant, beside an empty pattern
    - a comparison over a failing group beside the find's empty group
    - a predicate and an expression over a failing group and an empty group
    - a predicate and an expression over a failing group and a group with a tuple, before an empty pattern
  - `TestReviewEval_AFailureBesideAnEmptyClauseFailsNoQuery` (storage). A missing? predicate before an empty pattern, or on its input, and a get-else and a get-some expression before an empty pattern, answer empty on both optimizer paths. Its missing? case before the empty pattern pins the fusion bundle's decline of a group carrying the mark.
  - `TestThetaJoinPair_PropagatesPredicateEvalError` (executor) reads the failure where the result is driven (P8).
  - `TestSubqueryInFilter` (executor) counts the tuples it collects in place of reading the final relation's size. Its filter reads an averaging subquery's result, which carries the mark, so the filter answers a stream (P12, P8).
- A windowed form's failed subject tuple (T13, L14):
  - `TestAFilterAnswersTheTupleItsFailureAroseOnAndResumesPastIt`, `TestAnEvaluatorAnswersTheTupleItsFailureAroseOnAndResumesPastIt` and `TestAStreamsIteratorAnswersItsSourcesFailedTupleAndResumesIt` (executor, new). The filter, the evaluator and a stream's iterator over the evaluator each answer the tuple their own failure arose on and resume past it. The filter and the evaluator answer none for their source's failure.
  - `TestWindowReader_CarriesAFailedTupleTheFormReadsOnlyWhereItHoldsValues` (executor, new). The reader carries a failed tuple into its window and reads on where the form reads only the tuple's known positions, and ends with the failure where the form reads the failing binding's position.
  - `TestABranchFormDecidesItsSubjectsFailedTupleAtItsKey` (executor, new). A union and a fallback drop a failed tuple and answer the subject's other tuples where no branch holds at its key, and answer the failure where a branch does.
  - `TestAFailureBesideAClauseHoldingNowhereFailsNoQuery` (storage, new). On both optimizer paths, a correlated subquery over a failing group answers each numeric key's tuple where its body holds at no key of the failing tuple, and the failure where its body holds at that key. A not-join whose body holds at the failing tuple's key answers each numeric key's tuple.
  - `TestReviewEval_AFailureBesideAnEmptyClauseFailsNoQuery` (storage). An arithmetic expression and a missing? predicate before an empty correlated subquery answer empty on both optimizer paths.
  - `TestAFormOverAnotherFormsOutputDecidesAFailedTupleAtItsKey` (storage, new). On both optimizer paths, a not-join, a subquery, an or-join, an or-default-join or a pattern holding at the failing tuple, then a subquery, a not-join or a pattern holding nowhere there, answer the numeric items. Where the second clause holds there, the failure is the query's error.
- The identity symbols (§1, P11):
  - `TestRelationPropertiesCarryIdentities` (executor, new). Each derivation of properties keeps, renames, drops or joins the identity symbols as P11 states.
  - `TestAPatternsAnswerCarriesItsEntityVariableAmongItsIdentities` (storage, new). Each storage match arm whose pattern has an entity variable states it among its answer's identity symbols, on every backend.
  - `TestAnExecutorRoutesAnswerCarriesThePatternsEntityVariable` (storage, new). The windowed route and the fusion bundle state the pattern's entity variable, on both optimizer paths.
  - `TestCanFailOnAValueOverARelationsIdentities` (executor, new). A missing? predicate, and a get-else or get-some under the scalar form, whose entity is an identity symbol fail on no value. The same functions over another symbol, and under the tuple, collection and relation forms, can fail.
  - `TestADatabaseFunctionOverAnIdentitySymbolEvaluatesNothingThatCanFail` (executor, new). Each filter and evaluation of a database function over an identity symbol carries no mark, and over a relation naming no identity each carries it.
  - `TestAnEvaluationDestructuringADatabaseFunctionsValueCarriesTheMarkOverAnIdentitySymbol` (executor, new). The query's evaluation under the tuple, collection and relation forms carries the mark over an identity symbol.
  - `TestADestructuredLookupFailsTheQueryOnlyWhereEveryOtherClauseHolds` (storage, new). A limit and the existence verdict over a get-else destructuring a string fail the query. A comparison over a destructuring get-else or get-some before an empty pattern answers empty. Both hold on both optimizer paths.
  - `TestAETVCardinalityOneRelationProperties`, `TestAVETValidatedRelationProperties` (storage) and `TestAttributeFetchBundleTraversesInputOnce` (executor): the expected properties name the pattern's entity variable among the identity symbols.

## 6. The change the proof licenses

- `join.go`, `HashJoinWithOptions`:
  - answers the streaming relation over the join's iterator in every setting of `EnableStreamingJoins` when the build holds a tuple
  - loses the materialized arm, its results slice, its probe-consumed check and its probe event
  - reports the streaming mode in the `JoinStrategy` event
  - when the index is empty after the build, answers the empty relation over the result's symbols and properties through `newMaterializedRelationFromSet` and emits the `JoinProbe` event with no probe tuple
  - over an empty build that read no failure, does not call the probe side's `Iterator`
  - over an empty build that failed, reads the probe side through `hasTuple`, and carries the build's failure only where the probe side held a tuple or a failure (T5)
- `join.go`, `hashJoinIterator`:
  - owns a workspace of the result's width
  - fills it on each pull in place of building a fresh tuple, and hands it back from `Tuple`
  - answers `RequiresCopy` true
  - copies no probe tuple, and `probeNeedsCopy` goes
  - answers the build's failure from `Error` only once a probe tuple was read or the probe failed (T5)
- `join.go`, `joinFailure`: the join's failure rule, which `hashJoinIterator.Error` and the empty build's branch of `hashJoinWithSet` answer through (T5).
- `symmetric_hash_join.go`, `symmetricHashJoinIterator`: records whether each side yielded a tuple, and its `Error` answers `joinFailure` over the two sides (T5).
- `executor_utils.go`: `BindQueryInputs` collects its fold through `collectTuplesInto` into a proven-set materialized relation before answering it.
- `tuple_key.go`: `NewTupleKeyMapWithCapacity`'s doc states that a negative size, a stream's size answer, starts the map empty. The code is unchanged, since `make` reads a negative hint so. Withdrawn: `ONE_TUPLE_TABLE_PROOF.md` replaced the key map with the one table, and `NewTupleSet`'s doc states that it sizes the set at its first add for a capacity above zero.
- `datalog/storage/database.go`: `DefaultPlannerOptions`'s comment on `EnableStreamingJoins` says the option selects nothing in the hash join.
- Tests: the new assertions and tests of §5. The cases of `TestAJoinOfTwoSetsAnswersEachPairOnce` that assert the materialized arm's probe event mode assert the streaming mode.
- `docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md`: the completed changes gain this one.
- `relation_ops.go`, `evaluateExpressionWithLookup`: answers the stream over a `FunctionEvaluatorIterator`, with the properties it answered before and no error result. Its callers, `executeExpression` and `crossJoinWithExpression` (`theta_join.go`), take the relation (T8).
- `query_executor.go`, `executeExpression`: an expression whose symbols no group carries extends every group through `evaluateExpressionWithLookup`, and with no group stands over the unit relation, in place of its evaluation at the clause (T8).
- `iterator_composition.go`, `FunctionEvaluatorIterator`:
  - takes the expression, the database function's lookup and the environment
  - joins each source tuple with each tuple `bindingTuples` reads, through `bindingAlignment.apply`
  - evaluates an expression reading no symbol of its source once, at the first tuple it pulls (T8)
  - answers its `Symbols`
  - serves `StreamingRelation.EvaluateFunction` and `LazySeqRelation.EvaluateFunction`, which build it from a scalar expression
- `expression_binding.go`, `bindingTuples`: answers each tuple once under the collection and relation forms (T8).
- `bound_pattern.go`, `executeBoundPattern`: wraps its windowed iterator in `patternJoinIterator` (T9).
- `relation_properties.go`, `RelationProperties`: gains `Evaluates`, the evaluating mark (§1), which its derivations carry (P10).
- `relation_properties.go`, `RelationProperties`: gains `Identities`, the identity symbols (§1), which its derivations keep, rename, drop or join (P11). `AnswerIdentities` names a data pattern's entity variable.
- `relation.go`: the held constructors, the stream's `EvaluateFunction` and `FilterWithPredicate`, and `ProductRelation.Properties` decide the mark (P10). `ProductRelation.Properties` takes every member's identity symbols (P11).
- `lazy_seq_relation.go`: `EvaluateFunction` and `FilterWithPredicate` decide the mark (P10).
- `relation_ops.go`, `evaluateExpressionWithLookup`: sets the mark unless the expression fails on no value over its relation's identity symbols (P10).
- `windowed_form.go`, `canFailOnAValue`: decides whether running clauses can fail on a value (§1), every clause kind named, and a subquery's folds through `subqueryFoldCanFail`.
- `windowed_form.go`, `subqueryFoldCanFail`: a subquery whose `:find` holds an aggregate whose fold can fail on a value, whatever its call passes (§1).
- `aggregation.go`, `aggregateOps`: gains `failsOnAValue`, which `resolveAggregate` sets for sum and avg (§1).
- `windowed_form.go`, `canFailOnAValueOver`, `expressionFailsOnNoValueOver` and `entityIsAnIdentity`: decide whether one predicate or expression can fail on a value over a relation's identity symbols (§1).
- `datalog/storage/matcher_relations.go` and `datalog/storage/hash_join_matcher.go`: each match arm whose pattern has an entity variable states it among its answer's identity symbols (P11).
- `bound_pattern.go`, `executeBoundPattern`, and `query_executor.go`, `tryFuseAttributeFetchBundle`: state their entity variable among their answer's identity symbols (P11).
- `executeSubquery` (`query_executor.go`), `executeNotJoinClause` (`scope.go`), `NewOrFallbackRelation` (`or_fallback_relation.go`) and `executeBoundInvocation` (`bound_invocation.go`): set the mark (P10).
- `query_executor.go`, `executeSubquery`: asks `canFailOnAValue` of the subquery clause, so its mark reads the body and the `:find`'s folds (P10).
- `limit_relation.go`, `LimitRelation.take`: reads as T11 states.
- `query_executor.go`, `existenceVerdict`: decides as T11 states and L12 proves. It takes the groups the find reads beside the groups it decides, and reads them only with a failure to raise (P10).
- `relation.go`, `ProductIterator.Next` and `ProductIterator.Error`: the first pull reads every member to its first tuple, its end or its failure, and a member with no tuple and no failure ends the product with no failure (P10, T11).
- `scope.go`, `hasTuple`: reads as P4 states.
- `relation_ops.go`, `filterWithPredicateAndLookup`: answers a stream over a `PredicateFilterIterator`, carrying the mark, when the predicate can fail on a value of the relation or the relation carries the mark (P12).
- `iterator_composition.go`, `PredicateFilterIterator`: takes the database function's lookup and the environment, as `FunctionEvaluatorIterator` does.
- `theta_join.go`, `thetaJoinWithPredicate` and `crossJoinWithExpression`: read the lazy product of their groups when a group carries the mark, and `thetaJoinWithPredicate` also when its predicate can fail on a value (P12).
- `relation.go`, `productEvaluates`: the product's mark, which `ProductRelation.Properties` and the two theta readers read.
- `query_executor.go`, `executePredicate`: a predicate the environment alone binds joins the groups as a group of no symbol when its verdict carries the mark or met a failure (P12).
- `query_executor.go`, `tryFuseAttributeFetchBundle`: declines a group carrying the mark (P12).
- `iterator_composition.go`, `failedTupleIterator`: `PredicateFilterIterator` and `FunctionEvaluatorIterator` answer their failed tuple and resume past it (P13).
- `relation.go`, `CountingIterator`: answers the failed tuple of the iterator it counts, and resumes it (P13).
- `window_reader.go`, `windowReader`: carries a failed tuple holding a value at each position the form reads into its window, marked with its failure and the positions it holds no value at (`windowFailure.unknown`, `unknownAt`), and reads on (P13).
- `windowed_form.go`, `newWindowedIterator`, `windowedIterator.Next` and `tupleToAnswer`: the positions the form reads, and the failure raised where the form answers an output for a marked tuple (T13).
- `windowed_form.go`, `windowedIterator.failedTuple` and `resume`: the output answered for a marked tuple, with the tuple's unknown positions, and the resumption past it (P13, T13).
- `bound_pattern.go`, `patternJoinIterator.failedTuple` and `resume`: its windowed join's (P13).
- `or_fallback_relation.go`, `OrFallbackIterator.answer`: deduplicates no output of a marked tuple (P13).

## 7. What the proof does not cover

- A probe side that already carries a recorded failure, a materialized relation whose collection failed or a stream whose cache completed with an error, under an empty build that read no failure. The join opens nothing, so the failure is not raised here. It surfaces where the relation is next driven, and a relation nothing else drives keeps it unraised. Today the materialized arm drains the probe and raises it. The alternative, opening the probe's iterator and closing it without a pull, raises a materialized relation's recorded failure and records an incomplete materialization on a caching stream.
- A windowed subject that already carries a recorded failure, under a pattern with no match (T9). The route pulls nothing of the subject, so the failure is not raised there. A recorded failure is a fault of the store, not an evaluation failure, and the failure contract does not decide whether it is raised.
- The symmetric arm, which `ChooseJoinStrategy` selects under `EnableSymmetricHashJoin`, beyond its failure answer (T5). Its iterator builds each tuple fresh and answers `RequiresCopy` false as before.
- `crossProduct`, reached by `Join` on sides sharing no symbol. It opens its right side once per left tuple and so cannot take a stream as its right side. No consumer in the enumeration hands it a join result as its right side: `Collapse` joins only groups sharing a symbol, and `findOuterRelation` joins under the common symbols, empty or not, through the hash join.
- The cost of the windowed route against the matcher's route for a pattern after a join, which this proof does not bound.
- `reportJoin`'s size reads of the left and right sides, which it makes of every side that is not a `StreamingRelation`. The record lists annotation realization among its remaining opportunities.
- Whether `EnableStreamingJoins` is retired from `ExecutorOptions` and `PlannerOptions`. It selects nothing in the hash join under this change and stays where it is.
- A windowed form whose body holds a database function over an identity symbol. `executeSubquery`, `executeNotJoinClause`, `NewOrFallbackRelation` and `executeBoundInvocation` judge their bodies through `canFailOnAValue`, which reads no identity symbols, so such a form carries the mark.
- A get-some's pair under a tuple form of two variables, over an identity symbol. The pair always destructures, and `expressionFailsOnNoValueOver` exempts the scalar form alone, so the evaluation carries the mark.
- A database function reading no symbol of its source, on a named source that a commit changes during the execution. P9 states one snapshot for `$` alone, and the iterator's one evaluation answers every tuple (L9).
- A build side's failure beside a probe side whose tuples carry no key the failing build tuple could hold. The join raises the failure once the probe side holds a tuple (T5), whatever key the tuple carries, since the failure carries no key.
- Two groups the existence verdict decides, or two members of a product, each failing in place of a first tuple. The verdict and the product raise the first failure, and whether the other group holds at any assignment past its own failure is not read.
- A failed subject tuple whose unknown positions include one the form reads, such as a subquery whose input is the failing expression's output. The reader ends with the failure before the form runs at any key (P13).
- A failed subject tuple under a subject that is a join of several groups or a stream marked for caching. `hashJoinIterator` and `CachingIterator` answer no failed tuple (P13), so the reader ends with the failure, though the form may hold at no assignment extending the tuple (`docs/bugs/BUG_A_FAILED_TUPLE_RAISES_AT_AN_OPERATOR_THAT_DOES_NOT_CARRY_IT.md`).
- A `:limit 0` over a relation without the evaluating mark whose first pull fails, such as a function reading no variable (L12). `LimitRelation.take` opens no iterator of a source without the mark when its limit is 0, so the failure is not raised, where a read of one tuple raises it. That was read in `LimitRelation.take` and not run.
- A correlated subquery that fails on a value at a key no outer tuple reaches, with the algebra optimizer on (`docs/bugs/BUG_OPTIMIZER_PATHS_ANSWER_DIFFERENTLY_WHEN_A_CORRELATED_SUBQUERY_FAILS_ON_A_VALUE.md`).
  - Rule 1 rewrites a subquery whose `:find` aggregates into one aggregate over every value of the correlation variable (`docs/ALGEBRA.md`, Rule 1).
  - Rule 4 rewrites a subquery whose body holds a filtering clause into one subquery over every value of it (`docs/ALGEBRA.md`, Rule 4).
  - Each rewritten call passes no variable, so its form runs it once, over the unit relation, at the first window (`docs/ALGEBRA.md`, OR-Fallback Branch Caching). There it folds every group and evaluates the body at every key.
  - So a failure at a key no outer tuple reaches fails the query, where with the optimizer off the correlated subquery evaluates only the keys an outer tuple reaches.
  - The bug record holds the reproducer of both shapes, and its table records what each optimizer path answers.
