# The relation input, as a proof

## Sources

- The code: `datalog/executor/executor.go` (`ExecuteWithRelations`, `executeRealizedPlan`, `hasRelationInput`, `executeRealizedWithRelationInputIteration`, `executeRealizedWithRelationInputIterationSequential`, `executeRealizedWithRelationInputIterationParallel`, `prepareIteration`, `preparedIteration.buildBoundRelation`, `preparedIteration.Run`), `datalog/executor/executor_utils.go` (`BindQueryInputs`, `environmentRelationFromInputs`), `datalog/storage/database.go` (`convertInputsToRelations`), `datalog/executor/query_executor.go` (`executeSubquery`), `datalog/executor/window_frame.go` (`windowFrame.windowRun`, `expandKeys`), `datalog/executor/subquery.go` (`subqueryIterator.probe`), and `datalog/storage/hash_join_matcher.go` (`chooseJoinStrategy`, the per-key seek).
- The design: `NESTED_SCOPES_AS_QUERIES.md` (NESTED) goal 12, §2.5, §5.4; `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) goals 2, 7 and 8, §2.2, §3, §4.2, §4.4, §4.6, §7; the janus `CLAUDE.md`, "RelationInput and Subquery Iteration" and the relational invariants under "Use Pure Relational-Algebra Vocabulary"; `docs/bugs/resolved/BUG_SCALAR_PLUS_RELATION_INPUT_DROPS_OTHER_INPUTS.md`; `docs/archive/2025-10/SUBQUERY_PERFORMANCE_ANALYSIS.md`.
- The literature: Datomic's query reference, the bindings section (`bind-scalar`, `bind-tuple`, `bind-coll`, `bind-rel`) and the aggregates section. J. D. Ullman, Principles of Database and Knowledge-Base Systems, volume I (1988), the algebraic laws of the relational algebra.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **query** Q is `[:find F :in $ … :where W]`. F is the find, W the body.
- An **input relation** R is the relation one `:in` entry binds, over the entry's symbols S. `convertInputsToRelations` builds it over those symbols for every form: one tuple over one symbol for a scalar, n tuples over one symbol for a collection, one tuple over n symbols for a tuple input, and n tuples over n symbols for a relation input. Its own comment states that every input form binds a set of tuples over a symbol list.
- The **body relation** B is the relation W denotes over its variables V. SCOPE §4.2 states that every clause the language admits denotes a relation.
- The **group keys** G are the non-aggregate elements of F. A find with no aggregate has G = F.
- **agg_F(X)** is F applied to a relation X: the projection π_F(X) when F has no aggregate, and otherwise one tuple per value of π_G(X), each aggregate taken over the tuples of X with that value of G.
- The **join reading** of Q over R is **J** = agg_F(R ⋈ B). The join is natural, on S ∩ V, and a product where S and V share nothing.
- The **iteration reading** of Q over R is **I** = ∪_{t ∈ R} agg_F({t} ⋈ B), the union taken as a set. For a tuple t, {t} ⋈ B is the **slice** of t.
- The **per-tuple loop** is `executeRealizedWithRelationInputIteration` with its sequential and parallel variants. `executeRealizedPlan` diverts a plan into it when `hasRelationInput` finds a relation input in the prepared `:in`.
- The **bound group** is the relation `BindQueryInputs` builds from the input relations: one relation per binding `:in` entry over the entry's symbols, joined into one through `Relation.Join`.
- The **environment** is the one-tuple relation `environmentRelationFromInputs` builds from the scalar and tuple inputs, ambient in every clause scope (SCOPE §2.2).
- A **run** of the loop is one call of `preparedIteration.Run`: every phase of the plan executed once over a bound group of one tuple.

## 2. Preconditions

- **P1.** Every relation is a set (the relational invariants of the janus `CLAUDE.md`). `NewMaterializedRelation` deduplicates, so R, the bound group and the loop's union are sets.
- **P2.** Every clause of W denotes a relation (SCOPE §4.2). So B exists for every admitted body, a body holding a negation, a fallback, a subquery or an invocation included.
- **P3.** At the database boundary the input relation carries the `:in` entry's own symbols (`convertInputsToRelations`). A relation over other symbols reaches the executor only through a direct caller of `ExecuteWithRelations`.
- **P4.** A collection input enters as the bound group (`BindQueryInputs`, the `CollectionInput` arm), and a tuple input and a scalar enter the same way. Only a relation input is diverted into the loop (`executeRealizedPlan`).

## 3. Theorems

**T1 (no aggregate).** When F has no aggregate, I = J for every R and every B.

**T2 (aggregates).** When F has an aggregate, I = J whenever every group of R ⋈ B under G lies within one slice, which holds whenever S ⊆ G. When a group spans two slices, I holds one tuple per slice, each carrying that slice's aggregate, and J holds one tuple carrying the group's aggregate. The two coincide only when every slice's aggregate equals the group's. For `count` and `sum` over two non-empty slices they never coincide.

**T3 (one fact, three rules).** The tree computes the aggregate over an input of several values by three rules at three sites. A top-level collection input is aggregated over the whole join (P4). A top-level relation input is aggregated per tuple (the loop). A collection input of a subquery's inner query is aggregated per member, and the members' answers are merged as a set (`executeSubquery`, `windowFrame.windowRun`, `subqueryIterator.probe`). Over the entities p{:c 0}, q{:c 1} and s{:c 2 :c 1}, the count `[:find (count ?e) :in $ [[?c] ...] :where [?e :c ?c]]` given `[[1] [0]]` answers `[[1] [2]]`, and the same count over `[?c ...]` given `[1 0]` answers `[[3]]`. With `?c` in the find, `[:find ?c (count ?e) :in $ [[?c] ...] …]` answers `[[0 1] [1 2]]`, which is J's answer and I's alike (T2).

**T4 (the join subsumes the loop).** For every F, I_F = π_F(J_{F ∪ S}): the iteration reading of a find is the join reading of that find with the input symbols added as group keys, projected back. So every answer the loop gives has a spelling under the join. The converse fails: J_F for a find whose group keys do not determine the input tuple, the count over the whole join among them, has no spelling under the loop at the top level, and under the subquery frame's per-member rule it has none inside a subquery either (T3).

**T5 (cost).** Under the join the matcher opens no more scans than under the loop, at every pattern, and the plan's machinery is paid once where the loop pays it once per input tuple (L3). Under the join a result not sorted and not aggregated streams, where the loop holds every per-tuple result un-pulled until the input is exhausted and then materializes their union (`executeRealizedWithRelationInputIterationSequential` after its loop, and the aggregation after `wg.Wait` in the parallel variant). The parallel variant spreads the runs over workers, and the join is one execution, so the two's wall-clock on a given shape is a benchmark, not a claim (SCOPE goal 7).

**T6 (one evaluation).** NESTED §5.4 excludes the required run as a `query.RelationInput` in a scope's `:in` because the loop runs a plan once per tuple, and SCOPE §4.4 runs a bound invocation and a data pattern over a streaming group once over the window. A bound invocation inside a query with a relation input runs its rewrite and its fixpoint once per run under the loop, which is the fixpoint per subject tuple SCOPE goal 2 names. The top-level relation input is the one evaluation in the engine that still runs per tuple.

## 4. Lemmas

### L1. Join and projection distribute over union (T1)

R is the union of its singletons, R = ∪_t {t}. A natural join distributes over union, so R ⋈ B = ∪_t ({t} ⋈ B). A projection distributes over union, so π_F(R ⋈ B) = ∪_t π_F({t} ⋈ B). Both sides are sets by P1. The right side is I and the left is J. ∎

### L2. Grouping (T2)

Let X = R ⋈ B and let g be a value of π_G(X). Under J the aggregate for g is taken over {x ∈ X : π_G(x) = g}. Under I it is taken, for each t, over {x ∈ X : π_G(x) = g and π_S(x) = π_S(t)}, and I holds one tuple (g, a_t) per t whose set is non-empty. When G functionally determines S on X, the set for g is non-empty for one t alone and equals J's set, so the tuples agree. S ⊆ G gives that dependency outright. When two slices t1 and t2 meet g, I holds (g, a_t1) and (g, a_t2) where J holds (g, a). For `count` and `sum`, a_t1 < a and a_t2 < a when both slices are non-empty with positive values, so the tuples differ. For `max`, `min` and `avg`, a_t1 = a_t2 = a holds only when the slices' aggregates coincide. ∎

### L3. Scans and machinery (T5)

Let P be a data pattern of the plan. Under the loop P runs once per input tuple, bound by the run's tuple and by the run's intermediate relation, so it opens one narrowed scan per run for a key the run binds, and one per key of the run's intermediate otherwise. Under the join P runs once, bound by the bound group and by the intermediate relation of the one execution. The matcher seeks per distinct key at or below `executor.SmallBindingBoundary` and scans the extent once above it (SCOPE §4.6, `chooseJoinStrategy`). The distinct keys of the one execution are at most the sum of the keys of the runs, since a key two runs share is sought twice under the loop and once under the join. Above the boundary one extent scan replaces the runs' seeks. So the scan count under the join is at most the loop's at every pattern.

The plan's machinery, the phase loop, the collapse at each phase, the materialization of each non-final phase boundary (`materializePhaseBoundary` in `preparedIteration.Run`) and the environment copy, runs once per run under the loop and once under the join. The loop then pulls every run's result and materializes the union. The join returns the last phase's relation as any query does. ∎

### L4. A form reading an input symbol (T1, T2)

Let a nested form's required run hold a symbol of S. Under the loop the symbol is an environment symbol of the run, one value, and SCOPE §2.2 gives the form the environment's tuple as its run. Under the join the symbol is a group symbol, and SCOPE §4.2 gives the form the window's projection onto its header. In both the form's result for a required tuple is a function of that tuple's projection onto the header, and the subject tuples the form keeps are those whose projection the branches answer. The union over the runs of the first equals the second by L1. ∎

### L5. Two relation inputs (T1)

Under the join two relation inputs R1 and R2 are the bound group R1 ⋈ R2, a natural join on their shared symbols and a product otherwise, which `BindQueryInputs` builds through `Relation.Join`. Under the loop `executeRealizedWithRelationInputIteration` keeps the last relation input alone and `prepareIteration` rewrites every relation input to the kept one's symbols, so the first's symbols are bound twice and the second's never, and the hash of an unbound value panics. The join reading is the one Datomic's grammar gives two `bind-rel` entries. ∎

## 5. The data flow

The three diagrams show the three rules of T3. Each box is a function of the code, and each arrow carries the relation named on it.

### 5.1 The per-tuple loop, the relation input before §8

```
ExecuteWithRelations
  │ Prepare ─► initialBindings: every symbol of S marked bound ─► PlanQuery ─► refuseUnknownAttributes
  ▼
ExecuteRealized ─► executeRealizedPlan
                       │ hasRelationInput(plan.Query): yes
                       ▼
     executeRealizedWithRelationInputIteration
       R := the last RelationInput of the :in           (the first's symbols bound twice, L5)
       prepareIteration: :in $ [[?j ?f]]  ─►  :in $ ?j ?f   (one ScalarInput per symbol)
       buildBoundRelation: boundTuple = [ ?j: _ , ?f: _ , constants… ]   one workspace, shared by every run
                       │
   R ──iterator──┬──► t1 ──► Run(t1): boundTuple[slots] := t1; env := copy(boundTuple)
                 │            phases 1..k over {boundRelation} ─► result_1, un-pulled
                 ├──► t2 ──► Run(t2): boundTuple[slots] := t2 ─► result_2, un-pulled
                 ⋮                                                  (result_1 now reads t2's values)
                 └──► tN ──► Run(tN)                             ─► result_N, un-pulled
       after the loop: collectTuplesInto(result_1 … result_N) ─► NewMaterializedRelation, deduplicated
                       │
ExecuteRealized: :order-by and :limit over the union
```

The parallel variant streamed R into a jobs channel, each worker owning one `preparedIteration` and one workspace, and aggregated the workers' un-pulled results after `wg.Wait`.

### 5.2 The join, every input under §8

```
ExecuteWithRelations
  │ Prepare ─► initialBindings ─► PlanQuery ─► refuseUnknownAttributes
  ▼
ExecuteRealized ─► executeRealizedPlan
                       │
                       ▼
     BindQueryInputs(plan.Query, inputRelations)
       R over [?j ?f] ─► one bound group G_R, N tuples          (joined with the other inputs' groups, L5)
                       │
     phase 1: [?e :a ?j] over G_R ─► the matcher's binding-driven scan
                keys = π_?j(G_R): one seek per distinct key at or below SmallBindingBoundary,
                                   one scan of the extent above it            (SCOPE §4.6)
              ─► a streaming relation over [?e ?j …], joined with G_R at the collapse
     phases 2..k: once, over the one group
     the find boundary: agg_F over the whole, grouped by G                    (SCOPE §3)
                       │
ExecuteRealized: :order-by and :limit; the result streams unless sorted or aggregated
```

### 5.3 The subquery frame with a collection argument, the third rule

```
outer tuple u carries ?ns = [1 2 3]
executeSubquery: members = {?n}                                   (the inner :in's collection inputs)
  window of the subject ─► windowRun: key(u) expanded at ?n's position by expandKeys
                           ─► run tuples (u,1) (u,2) (u,3)
  the inner query, framed with ?n among its group keys, built once over the run
                           ─► (u,1) → 1   (u,2) → 2   (u,3) → 1
  probe(u): one probe per member key, the matches merged distinct on the outputs
                           ─► {1, 2}, two tuples, where the whole join gives 4
```

Under §8 the run tuple is u, the members bind as a group inside the scope, and the inner find aggregates over the whole: 4.

## 6. What the proof finds

- **F1.** The loop was not chosen for speed. The janus `CLAUDE.md` entry "RelationInput and Subquery Iteration" states the per-tuple reading as the semantics, "aggregations compute per-tuple not globally", under the heading "Correctness first, performance second", and records its sequential execution as a performance optimization still needed. The analysis it cites, `SUBQUERY_PERFORMANCE_ANALYSIS.md`, concerns the decorrelation of correlated subqueries and names no relation input. The loop's later commits are performance work paying for the reading: the parallel variant, the forwarding of the other inputs (`BUG_SCALAR_PLUS_RELATION_INPUT_DROPS_OTHER_INPUTS.md`), and the hoisting of the per-run setup into `preparedIteration` with a pre-built bound relation whose tuple slots are mutated per run. That shared workspace is the mechanism by which a result pulled after the next run reads the next tuple's values.
- **F2.** No test in the tree distinguishes I from J. Every top-level relation-input query in the tree whose find aggregates names every input symbol in its find, so S ⊆ G and T2 makes the two readings agree. Every other relation-input query in the tree has no aggregate, where T1 makes them agree. The two queries in this engine's known downstream that bind a relation input have no aggregate either.
- **F3.** The engine answers one text differently by the spelling of its input (T3). Goal 12 says a spelling of janus's own is a synonym of a Datomic form, never a form with a meaning of its own, and the collection input, the tuple input and the relation input are three spellings of one thing, a set of tuples over a symbol list, which `convertInputsToRelations` already states.
- **F4.** Datomic's reading is J. Its bindings section binds a relation as a set of tuples the query joins, and its aggregates section groups by the non-aggregate find elements over the whole result. Goal 12 names Datomic's meanings for the `:in` forms.
- **F5.** J is the more expressive reading (T4). An author who wants the per-tuple aggregate names the input symbols as group keys, which Datomic's author does too. An author who wants the aggregate over the whole has no spelling under I.
- **F6.** The subquery frame's per-member rule is the third site of T3's fact, and the same derivation decides it. A collection argument's members are a relation of one symbol inside the inner query, as a collection input is at the top level, and the inner find decides the grouping.
- **F7.** The loop's own defects have no site under the join. The shared workspace, the last-input-wins, the positional slots and the width-skip are properties of `prepareIteration` and `preparedIteration.Run`. Under the join a relation input enters through `BindQueryInputs` like every other input, and the one correction that path needs is reading the relation's tuples by its own symbols rather than by position, a run lacking a symbol being the query's error (SCOPE §4.1's rule for a run).
- **F8.** The loop is the one per-tuple evaluation left in the engine (T6). Everything else runs over a window or a group once.

## 7. The contracts the tests pin

Each contract below is the subject of one test, and every one runs under the optimizer-mode matrix.

- A relation input with a pure aggregate in the find answers the aggregate over the whole join, on every backend: the count over `[[?c] ...]` given `[[1] [0]]` answers one tuple.
- A relation input with its symbols among the group keys answers the per-tuple aggregate, which is today's answer: the existing tests of `relation_input_test.go`, `badger_profile_test.go`, `relation_input_over_storage_test.go` and `ohlc_subquery_performance_test.go` stand as they are.
- A collection input, a relation input of one symbol and a tuple input of one tuple answer the same tuples for the same values, with and without an aggregate (T3's one rule).
- A collection argument of a subquery with an aggregating inner query answers the aggregate over all its members per outer combination: `[(q [:find (count ?e) :in $ [?n ...] :where [?e :r ?n]] $ ?ns) [[?c]]]` with `?ns` = `[1 2 3]` over a:r1, b:r2, c:r2, d:r3 answers 4.
- Two relation inputs are the natural join of the two, a product when they share no symbol (L5).
- A symbol repeated in a relation input, a tuple input or an expression binding unifies, selecting the tuples whose values agree (NESTED §2.5's rule for a repeated binding name).
- A relation given to `ExecuteWithRelations` binds each `:in` symbol from the relation's own symbol of that name, a relation lacking one is the query's error, and a relation whose symbols are permuted answers as the `:in` names them.
- A relation-input query whose result streams answers from its own input tuples whatever the order it is read in: the shape `[:find ?j ?f :in $ [[?j ?f] ...] :where [?e :a ?v] :order-by [?e]]` over `[[1 2] [2 3]]` answers both tuples on every read.
- A query's `:order-by` and `:limit` apply once to the whole result of a relation-input query: the existing tests of `executor_sorting_test.go` and `limit_test.go` stand.
- A nested form whose required run holds an input symbol answers the same tuples under the join as under the environment (L4): the existing `not_spanning_groups_test.go` and `orjoin_in_bound_correlate_test.go` cases stand.
- A bound invocation whose arguments a relation input binds runs one rewrite and one evaluation, continued across the windows, with one `RulesMagicSets` event (T6, SCOPE §7's contract for a bound invocation at the top level).
- The random-shape differential's generator produces relation inputs with and without an aggregate in the find, and its Go reference computes J.

## 8. The resolution

A relation input is a relation the query joins, as every other `:in` form is. It enters through `BindQueryInputs` as the bound group, joined with the other inputs' groups, and the plan runs once over it. `executeRealizedPlan` diverts nothing. `executeRealizedWithRelationInputIteration`, its sequential and parallel variants, `prepareIteration`, `preparedIteration`, `perTupleInputBuilder` and `perTupleInputSession` have no job and go. `BindQueryInputs` reads each `:in` symbol's values from the input relation's own symbol of that name, through `query.SymbolPositions`, unifies a symbol the entry carries twice through `query.AgreesAt`, and a relation lacking a symbol its entry names is the query's error.

Inside a subquery, a collection argument binds its members as a group inside the scope, from the holder `ScopeInput` mints for its call position and the opening identity `ReadSubqueryFrame` writes, the run tuple being the call's combination, so the inner find aggregates over all the members of one combination; a tuple or a relation argument binds one value per symbol at its call positions (`InputPositions`). `expandKeys` and the per-member probe of `subqueryIterator.probe` go with the per-member run. An invocation in the inner body is seeded from that group, one rewrite and one evaluation per form execution, which is what the per-member expansion was for.

The derivation: goal 12 fixes the meaning of every `:in` form as Datomic's (F4), T4 shows the join reading loses no answer the loop gave, T3 shows the tree holds three rules for one fact and goal 3 ranks consistency above compatibility, and T6 shows the loop is the shape SCOPE goal 2 removes everywhere else.

The alternative is the loop kept and repaired in place: a fresh one-tuple relation per run, every relation input iterated, slots found by symbol. Its cost is a meaning of janus's own for a Datomic spelling, against goal 12, a second executor path beside the phase pipeline with its own sequential and parallel variants, and the third rule of T3 left standing or made a fourth. The cost of the resolution is to any consumer that wrote a relation-input query with an aggregate and no group keys and meant the per-tuple answer. That query names the input symbols in its find and answers as before (T4). The records the resolution changes are SCOPE §4.4's two sentences on the collection input's expansion and SCOPE §7's contract "A collection input yields one run tuple per member", each restated as the members bound as a group inside the scope, the janus `CLAUDE.md` entry, restated as the join, and `OR_FALLBACK_RELATION_PROOF.md`'s sources, definitions and F5, restated over the frames without the per-member expansion.
