# Concurrency in the engine, as a proof

## Sources

The query entry:

- `queryUnderPlannerOptions` and `sessionMatcher` in `datalog/storage/convenience.go`.
- `ExecuteWithRelations`, `executeRealizedPlan` and `runPhases` in `datalog/executor/executor.go`.

The snapshot:

- `badgerReadSession` in `datalog/storage/read_session_badger.go`.
- `memoryReadSession` in `datalog/storage/read_session_memory.go`.
- `MemoryTreeStore.NewReadSession` in `datalog/storage/memory_tree_store.go`.
- `versionHolder` in `datalog/storage/store_version.go`.

The matcher:

- `PatternMatcher`, `AttachReadSession`, `cacheBound` and `getTupleBuilder` in `datalog/storage/matcher.go`.
- `matchWithHashJoin`, `keyScanBounds`, `buildHashSet` and `hashJoinIterator` in `datalog/storage/hash_join_matcher.go`.
- `perKeySeekIterator` in `datalog/storage/per_key_seek_iterator.go`.
- The value-bound gate and `matchWithVValidation` in `datalog/storage/matcher_relations.go`.
- `Cache` in `datalog/storage/cache.go`.
- `cacheTrie` in `datalog/storage/hashtriemap.go`.

The executor:

- `DefaultQueryExecutor.Execute`, `executePattern` and `executeSubquery` in `datalog/executor/query_executor.go`.
- `Relations.Collapse` in `datalog/executor/relations.go`.
- `materializeRelationsForPattern` in `datalog/executor/relation_ops.go`.
- `executeBoundPattern` in `datalog/executor/bound_pattern.go`.

The relations:

- `Relation`, `Iterator`, `MaterializedRelation`, `StreamingRelation` and `CachingIterator` in `datalog/executor/relation.go`.
- `hashJoinIterator` in `datalog/executor/join.go`.
- `JoinIndex` in `datalog/executor/join_index.go`.

The windowed forms:

- `framing`, `frameBranches` and `windowedIterator` in `datalog/executor/windowed_form.go`.
- `windowReader` in `datalog/executor/window_reader.go`.
- `windowFrame` in `datalog/executor/window_frame.go`.
- `cachedBranch` and `buildBranchTuples` in `datalog/executor/kept_branch.go`.
- `OrFallbackRelation` in `datalog/executor/or_fallback_relation.go`.

The rules:

- `ruleEvaluation`, `stratumEvaluation`, `derivedSet`, `runStratum` and `ruleContinuations` in `datalog/executor/rules.go`.

The context and the shared scans:

- `Context` and `forkContext` in `datalog/executor/context.go`.
- `ScanRegistry` in `datalog/executor/scan_registry.go`.
- `LazySeq` in `datalog/executor/lazy_seq.go`.

The aggregation:

- `ExecuteAggregations` in `datalog/executor/aggregation.go`.

The caches:

- `PlanCache` in `datalog/planner/cache.go`.
- `InternedTupleBuilder` in `datalog/query/tuple_builder_interned.go`.

The design:

- `NESTED_SCOPES_AS_QUERIES.md` (NESTED) §5.4.
- `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) goals 2 and 7, §4.1 to §4.6, §7.
- `RELATION_INPUT_PROOF.md` T5, L3 and §8.
- `REWRITE_ORDER_PROOF.md` §6.
- `docs/reference/INDEX_SELECTION_PROOF.md` Theorem 3b.
- The janus `CLAUDE.md`: the relational invariants under "Use Pure Relational-Algebra Vocabulary" and the handler contract under "Performance Monitoring".
- `PERFORMANCE_STATUS.md` entry 17.

## 1. Definitions

- A **query** runs on the goroutine that called `Query`. Its **snapshot** S is the read session `queryUnderPlannerOptions` opens through `sessionMatcher`, one per query. Every storage read of the query goes through S (`AttachReadSession`), and the session-bound result closes S.
- A **scan** is `OpenKeyScan(reader, report, bound)`: the datoms of one run of one index under S.
- **Resolution** is `CRDTResolvingIterator` over a datom stream: the current value of each (E, A) group, taken in the stream's order.
- A **relation** is a set of tuples. A **materialized** relation holds stable tuples and never changes. A **streaming** relation owns one iterator and is read once. `Materialize` makes a streaming relation replayable through a cache filled as it is read (`CachingIterator`).
- An **operator** is a function from input relations to an output relation. An operator instance holds private state: a hash join's index and current probe (`hashJoinIterator`), a binding-driven scan's probe set and workspace (`hash_join_matcher.go`), a resolver's group state.
- A **job** is one evaluation the engine could hand to another goroutine: a scan, a rule body's execution in one iteration, a branch's build over a run or a slice of a run, an aggregation over a partition.
- The **per-query state** is what one job writes that another job of the same query reads: §2 lists it.
- A **pool** is W goroutines under a bound W given by one option. W = 1 is the execution as it stands.

## 2. Preconditions

- **P1 (one snapshot, read concurrently by construction).** `queryUnderPlannerOptions` opens one read session per query and attaches it to the matcher, so every scan of the query reads one state. Each backend supports concurrent scans of one session. The badger session serializes handle creation behind its mutex, "while iteration on the created iterators runs concurrently, which the same contract permits for read-only transactions" (`read_session_badger.go`). The memory session clones the key tree at open, and "the cloned tree is never written, so the iterator takes its chunks from it with no store lock" (`read_session_memory.go`). The tree store's session retains a version, "a held pointer, not a lock and not a copy", and "a reader never blocks a writer and a writer never blocks a reader" (`memory_tree_store.go`, `store_version.go`). A cache read under a session is bounded to the session's high-water mark, computed once (`cacheBound`).
- **P2 (relations are sets, and a materialized one never changes).** "Relations are IMMUTABLE and DEDUPLICATED at creation" (`relation.go`). A materialized relation returns stable tuples (`RequiresCopy` false). A streaming relation is single-use. So an operator over a materialized input may run on any goroutine, and an operator over a streaming input is the one reader of that input.
- **P3 (an operator's state is private to its iterator).** "This iterator is NOT thread-safe. Each goroutine must create its own iterator by calling `Relation.Iterator()`" (`join.go`). `JoinIndex.Add`, `derivedSet.absorb`, `windowFrame.ran` and `TupleKeyMap` carry no lock.
- **P4 (the shared caches are built for concurrent readers and writers).** `PlanCache` and the parse cache read under a read lock. `tupleBuilderCache` reads under a read lock, and `InternedTupleBuilder.getTxPtr` writes under its own mutex. The interners are `sync.Map`. The EA `Cache` is a trie of atomic pointers with a mutex per node for mutation, and its per-entity attribute set is a `sync.Map`. `ScanRegistry` is mutex-guarded. A `LazySeq` cell is realized under one `sync.Once`. A streaming relation's replay cache is mutex-guarded, "lazy-seq semantics for concurrent access".
- **P5 (the context).** "Not safe for concurrent use — the lazily created scanRegistry is mutable per query; forkContext derives a context with a scan registry of its own" (`context.go`). `forkContext` shares the environment, the rules, the derived relations, the continuations and the cancellation, and gives the fork a scan registry of its own. `ruleContinuations.byClause` is an unguarded map, written when a form execution first evaluates an invocation clause (`rules.go`).
- **P6 (the handler).** "The engine emits through one handler and does not serialize it: the query path runs on the goroutine that called `Query`, and two queries on two goroutines share one handler. That is safe because every event carries what its output reports — so a handler holding nothing between events needs no lock" (the janus `CLAUDE.md`). The scan funnel's counters are per arm: `hashJoinIterator.Next` counts resolved and matched, and `perKeySeekIterator` counts peers and intake, each on one goroutine.
- **P7 (a result's order is not a contract).** A relation is a set, and `ExecuteRealized` applies `:order-by` and `:limit` at the boundary over the whole result.
- **P8 (cancellation).** `ctx.cancelled()` is read at the query's entry and at each fixpoint iteration, and the context's error is the query's error.
- **P9 (the scheduler's cost is measured).** `PERFORMANCE_STATUS.md` entry 17 records a goroutine per small unit of work, funnelled through a semaphore, at about seventy percent of CPU in the scheduler and the collector, and a fixed pool fed by a bounded channel as the shape that removed it.
- **P10 (the engine's query path runs on one goroutine).** The engine's own sources launch one goroutine, in the binary import's chunk pool (`export_bin.go`). Planning, pattern matching, joins, windowed forms, subqueries, rules and aggregation each run on the goroutine that called `Query`.

## 3. Theorems

**T1 (scans commute).** A scan's result is a function of its bound and S. Two scans of one query produce on two goroutines what they produce in sequence, with handle creation serialized on badger and nothing else serialized (P1).

**T2 (operators commute).** Two operator instances whose inputs are materialized relations, or distinct single-use relations, produce on two goroutines what they produce in sequence (P2, P3). A single-use relation two jobs would read is materialized once before the fan-out, as `materializeRelationsForPattern` materializes the groups a pattern binds from.

**T3 (an iteration of the fixpoint is a parallel map and a sequential reduce).** In `runStratum`, the bodies of one run read `held`, a map of relations built by `view()` from the sets as they stand, and the lower deltas. `absorb` writes the sets, the window's additions and the evaluation's total after every body's result is in hand. The set after a run is the union of the bodies' results over the set before, under `absorb`'s key, in whatever order the results arrive. So the bodies run on W goroutines, each over `forkContext(scope)`, and their results are absorbed on the query's goroutine in any order. The fixpoint is the same, since each iteration's delta is the same set (L4). The strata run in order, the iterations of a stratum run in order, and the seed is absorbed first.

**T4 (a branch build is a parallel map over the run's slices, and a union's branches over each other).** SCOPE §4.2 states, for a branch holding no invocation, run(plan, X ∪ Y) = run(plan, X) ∪ run(plan, Y). `buildBranchTuples` runs the plan over the run and reads the result whole, and `cachedBranch.Add` holds the tuples. So a window's run is split into W slices, each built on its goroutine over `forkContext(ctx)`, and the slices' tuples are added to the index on the query's goroutine (L5). Under the union every branch runs over one run, a materialized relation (`windowRun`), so the branches commute as well. Under the fallback the next branch's run is the keys the earlier branches left unanswered (`narrow`, `built`), so the branches run in order and each branch's run is sliced. A branch holding an invocation is one job per window: its evaluation's sets are continued across the windows (`extend`), and the seed is the window's run whole.

**T5 (the per-key seek is a parallel map over the keys).** `matchWithHashJoin` computes one bound per distinct key (`keyScanBounds`), and `perKeySeekIterator` opens the bounds in turn as one stream. The stream's consumer, `hashJoinIterator`, probes a hash set per datom and never repositions the stream (`Seek` panics). Resolution is per (E, A) group in the stream's order (L3). Each seek's run is a distinct prefix. At the entity position it is [E] or [E, A], and at the attribute position [A], so every (E, A) group lies within one seek. At the value position it is [A, V]. A one-valued attribute never takes this route: a declared cardinality one is validated per candidate (`matchWithVValidation` under `NeedsValidation`), and an attribute with no declaration is validated the same way (INDEX_SELECTION_PROOF Theorem 3b, the gate's contract). A many-valued or vector attribute resolves per value, and a value's datoms lie within one seek. So the seeks run on W goroutines, each resolved on its own, and their resolved streams are consumed in any order (L3, L7). Above the boundary the strategy is one scan of the extent, or a merge join over entity keys, which reads the extent's order and is one job.

**T6 (a phase's ready set is a parallel map).** `Execute` runs the planned clauses in order, each pattern over the groups it shares symbols with, materialized, and collapses after each. Take a set of consecutive clauses of a phase such that each clause's inputs are available in the groups as they stand before the set, and no clause of the set provides an input of another (`ClauseReady`, `datalog/query/clause_ready.go`). The clauses of the set commute (T1, T2), their shared groups materialized once. Their results then collapse (`Relations.Collapse`), and the early termination on empty follows the set. The planner's order inside a phase stands, since the set is formed from consecutive clauses, so a scan the planner placed after a bound pattern stays after it. Fanning out across phases is excluded, since a phase reads the previous phase's result. Reordering the planner's clauses is excluded by `REWRITE_ORDER_PROOF.md` §6.

**T7 (aggregation and sort partition).** `ExecuteAggregations` folds per group key. The input partitioned by the group key's hash into W partitions has disjoint groups per partition, so each partition folds on its goroutine and the results concatenate. With no group key, `count`, `sum`, `min` and `max` combine partial results, and `avg` combines as a sum and a count. A sort partitions and merges. Both read a materialized input, or read a streaming input once into the partitions.

**T8 (what runs in sequence by nature).** The window reader. The fallback's branch order. The strata and their iterations. A single-use relation's consumption. The collapse's fold of joins. An invocation's continuation across windows. The find boundary's verdict, one pull per group.

**T9 (the relation input under the join).** The bound group enters the first phase, and the first pattern reading its symbols takes the matcher's binding-driven scan: at or below the boundary one seek per distinct key, above it one scan of the extent. So the work a per-tuple loop would spread over its workers, N narrowed scans, is N seeks under T5, on W goroutines, below the relation boundary, with one plan, one find boundary and set semantics untouched. The plan's later phases run over one relation, under T6. A loop paid the plan's machinery N times (`RELATION_INPUT_PROOF.md` L3), and T5 pays it once.

## 4. Lemmas

### L1. One snapshot for every goroutine of a query (T1)

`queryUnderPlannerOptions` calls `sessionMatcher`, which opens the session and attaches it to the matcher. A temporal handle's matcher copies `reader` and `sessionBounded` (`PatternMatcher.AsOf`). The source router routes the default source to that matcher. The cache bound is computed once per matcher under `boundOnce`. So every scan a job opens, on any goroutine, reads S. ∎

### L2. A scan reads nothing written during the query (T1)

The badger session pins one read-only transaction. The memory session reads a tree cloned at open. The tree store's session holds a version that a later commit replaces and never mutates. So a scan's datoms are fixed at the session's open, whatever commits after it. ∎

### L3. Resolution is per seek (T5)

`CRDTResolvingIterator` resolves each (E, A) group from the datoms it sees consecutively. The sequential stream presents one seek after another. A group that lies within one seek is resolved from that seek's datoms alone, in both the sequential stream and a per-seek stream. At the entity and attribute positions every group lies within one seek, since the seek's prefix fixes E or A. At the value position the route resolves per value under a many-valued or vector declaration, and a value's datoms lie within one seek, since the prefix fixes A and V. ∎

### L4. The fixpoint's set is order-free (T3)

`absorb` admits a tuple once, under `NewTupleKeyFull`. Set union is commutative and associative. `view()` builds the next run's `held` after every result of the run is absorbed, and `derivedSet.relation` returns the tuples as they stand, the slice capped at its length, so a run reads nothing a later absorption adds. The delta of an iteration is the union of the bodies' new tuples. By induction on the iterations, the sets after each iteration equal the sequential run's. The iteration bound and the tuple bound are counted at absorb, in sequence. ∎

### L5. A branch's slices compose (T4)

SCOPE §4.2's identity gives the union. `buildBranchTuples` runs the plan over the run through `runPlan`, and the run is the only input that differs between slices. The plan and the frames' environment relation are shared and never written. The scan registry is mutex-guarded (P4). The index is written on the query's goroutine. So each slice's build is a function of its slice and S, and the union of the slices' builds is the build over the run. ∎

### L6. Errors and cancellation under a pool (every theorem)

A job's error is the query's error, and the first error wins, as a seek's failure ends the per-key stream today ("a failed seek never reads as the key having no datoms", `per_key_seek_iterator.go`). A pool drains its jobs after the first error and reports that error. Each job reads `ctx.cancelled()` at its start, and a fixpoint reads it per iteration (P8), so no job starts after the context is done. ∎

### L7. Events under a pool (every theorem)

The handler is called from workers under P6. The counters the sequential path kept per arm stay per job, and the arm's report sums them at close, as `perKeySeekIterator` sums each scan's intake at its close. The stratum's begin and complete events fire on the query's goroutine, once each, as `runStratum` and `complete` fire them. ∎

## 5. The data flow

### 5.1 The per-key seek under a pool (T5)

```
matchWithHashJoin
  buildHashSet ─► keys k1 … kN            (distinct, at or below SmallBindingBoundary)
  keyScanBounds ─► bounds b1 … bN         (one prefix per key, disjoint)
                       │
   pool of W ──┬── OpenKeyScan(S, b1) ─► CRDTResolvingIterator ─► resolved stream 1
               ├── OpenKeyScan(S, b2) ─► CRDTResolvingIterator ─► resolved stream 2
               ⋮                                                  (badger: handle creation serialized)
               └── OpenKeyScan(S, bN) ─► CRDTResolvingIterator ─► resolved stream N
                       │
   fan-in, any order ─► hashJoinIterator: probe the hash set per datom ─► streaming relation
   per-seek reports ─► summed into the arm's report at close
```

### 5.2 An iteration of the fixpoint under a pool (T3)

```
runStratum, iteration i
  held := view() + the deltas of iteration i−1           (relations as they stand, never written during the run)
                       │
   pool of W ──┬── body 1 over forkContext(scope) ─► result 1
               ├── body 2 over forkContext(scope) ─► result 2
               ⋮
               └── body M over forkContext(scope) ─► result M
                       │
   absorb, on the query's goroutine, in any order ─► the sets, the deltas of iteration i, the bounds
```

### 5.3 A window's build under a pool (T4)

```
windowedIterator.runWindow
  run := frame.windowRun(window)                          (a materialized relation)
  slices r1 … rW of the run
                       │
   pool of W ──┬── buildBranchTuples(branch, r1) ─► tuples 1
               ├── buildBranchTuples(branch, r2) ─► tuples 2
               ⋮
               └── buildBranchTuples(branch, rW) ─► tuples W
                       │
   cachedBranch.Add, on the query's goroutine ─► the index the window's tuples probe
   under the union: every branch at once; under the fallback: the next branch over the keys left unanswered
```

## 6. What the proof finds

- **F1.** The query path runs on one goroutine from entry to result (P10). The one goroutine launch in the engine is the binary import's.
- **F2.** The substrate is already built for concurrent readers: one snapshot per query that every backend reads concurrently (P1), caches that read and write under their own guards (P4), relations that are sets and materialized relations that never change (P2). What runs in sequence is the executor's and the matcher's control flow, not the state under it.
- **F3.** The per-query state a pool must respect is small and enumerable: the continuations (P5), the scan registry (guarded), a windowed form's frames, index and policy record, a fixpoint's sets and counters, each operator's iterator, and the scan funnel's per-arm counters (P6).
- **F4.** The grain of the parallelism is the job of §1, not the plan run. A loop that ran the plan once per input tuple on a pool paid the plan's machinery per tuple (`RELATION_INPUT_PROOF.md` L3) and parallelized above the relation boundary. The same work's natural grain is the seek (T5) and the ready set (T6), below the boundary, where set semantics are untouched.
- **F5.** One option carries the bound W. `EnableParallelSubqueries` and `MaxSubqueryWorkers`, which named a mechanism the engine did not have and were read by no decision, are removed; `docs/reference/PLANNER_OPTIONS.md` lists them among the removed options.
- **F6.** A pool is fixed and fed by a bounded channel, never a goroutine per seek, per body or per slice (P9). Each worker owns one context fork (P5).

## 7. The contracts the tests pin

- Every parallel path answers the sequential path's tuples: the random-shape differential and the rules differential run under W = 1 and under W > 1, each against the Go reference, on every backend and both planner paths.
- The executor and storage packages are clean under the race detector with W > 1.
- A seek's failure is the match's error under the pool, and no key after it is read further.
- The scan funnel's totals under the pool equal the sequential totals: scanned, resolved, matched and peers.
- A cancelled query stops its pool: no job starts after the context is done, and the query's error is the context's.
- A stratum's begin and complete events fire once, and its iterations are numbered in sequence, under W > 1.
- A pool holds at most W jobs' state beyond the sequential path's.
- W = 1 runs the sequential code: one code path, the pool of one running inline, so the option off costs nothing (SCOPE goal 7).

## 8. The resolution

Concurrency returns to the engine at the job's grain, under one bound, in this order:

1. The per-key seek (T5): the seeks of one binding-driven scan on the pool, each resolved on its own, fanned in to the hash join in any order, with per-seek reports summed into the arm's. This is the finest grain, it touches no executor state, and it covers every bound pattern, the bound group of a relation input among them (T9).
2. The fixpoint's iteration (T3): the bodies of one run on the pool, absorbed in sequence, each body over its own context fork, a body whose branch holds an invocation running on the query's goroutine or over continuations of its own.
3. The window's build (T4): the slices of a run on the pool, the union's branches with them, merged into the index on the query's goroutine.
4. The phase's ready set (T6): the consecutive independent clauses of a phase on the pool, their shared groups materialized once, collapsed after.
5. Aggregation and sort (T7).

One option carries W. Its name is the owner's choice; the two options that named the gone mechanism are removed (F5). W of zero or one is the sequential path, which is the one code path, so a query under the default pays nothing for the pool. What makes a component safe under concurrent use is kept through every removal, whatever the call graph of the day does: the mutexes of the test matchers, the yield of a workspace-reusing test iterator, the handlers' own synchronization, `forkContext`, the goroutine-leak test and every concurrent-query test, since the pool returns on them and a single-goroutine suite cannot tell a safe component from an unsafe one.

The alternative is a per-tuple loop over a relation input on a pool, the one parallel path the engine had. It is excluded by `RELATION_INPUT_PROOF.md` §8, which derives the join for every `:in` form, and by F4, which places the same work's parallelism below the relation boundary, where it is one plan, one find boundary and the same tuples.
