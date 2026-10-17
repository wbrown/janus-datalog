# Scope Execution and the Engine's Contracts

**Amends**: `NESTED_SCOPES_AS_QUERIES.md` §5.4 and §7. Its §1 to §5.3, §6 and §8 stand, except where a section below names them.

**Implements**: the rules and recursion of `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1 on the mechanism `NESTED_SCOPES_AS_QUERIES.md` corrects, with the executor contracts the merge-base `841caf8` carries kept on it.

**Baselines**: `841caf8` is the branch's merge-base with `main`, and `5b152c9`, the baseline `NESTED_SCOPES_AS_QUERIES.md` §6 names, is eight commits past it on the branch, `eacad7e` to `5b152c9`.

## 1. Goals

The eleven goals of `NESTED_SCOPES_AS_QUERIES.md` §1 stand, and this design adds these.

1. Rules serve a caller at per-call cost: a query invoking a rule set runs on every call, at a cost the caller measures as its gate. The shapes: reachability as a recursive rule bound by every member of a collection input; readiness derived with stratified negation and a recursive propagation rule; and a loop in the caller's language replaced by one query over a rule set whose body holds an `or-join`, seeded by a collection input. §8 states the acceptance tests.
2. A bound invocation costs one rewrite and one fixpoint seeded by every bound value as one relation, wherever the invocation stands (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3). The failure: an invocation under `not`, `not-join`, `or`, `or-join` or `or-default` reran the fixpoint once per outer combination, where one rewrite seeded by every bound value as one relation derives the same tuples once. A fixpoint per subject tuple is that shape again.
3. The overlay matcher of `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §3.1 composes with `SourceRouter`, predicate pushdown and the EA cache unchanged. Every read path the executor takes is one the overlay covers, so a substitution of a cache lookup for a scan holds only under the conditions the matcher states for it.
4. The witnesses of `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §2 record, per derived tuple, the rule and the body tuples of its first derivation, and `WhyNot` descends a rule's alternatives one at a time to the clause that produced no match. Their substrate is the scans of a planned query, which the witness rewrite extends with a fourth position bound to the matched datom's ElementID (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §2.1), and the fixpoint's record of each derived tuple's first derivation (the same section, under Rules). Every scope in this design is planned through `QueryPlanner.PlanQuery` (§4.1).
5. One grammar (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §6). Every form the inference writes is a form an author may write, and the parser, the builder, `Prepare` and the executor accept and refuse the same text.
6. Every error names the author's repair (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §5). A refusal that records a mechanism's limitation is not an error about the query.
7. Cost is measured, never claimed (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §7), and an option that is off costs nothing.
8. The optimizer never changes results, and every test of this design runs under the optimizer-mode matrix of `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §0.
9. The engine's contracts at the merge-base with `main`, `841caf8`, are the baseline. A change that drops a contract names the failure it removes, and a mechanism of the merge-base's that computed a form's interface from what it held is not a contract (`NESTED_SCOPES_AS_QUERIES.md` §4).

## 2. The language

§2.1 to §2.5 of `NESTED_SCOPES_AS_QUERIES.md` stand. Two amendments state what the inference already writes.

### 2.1 A header of required variables alone

`(or-join [[?e ?f]] b1 b2)` and `(or-default-join [[?e ?f]] b1 b2)` are legal. The form keeps a tuple of the required run when a branch answers for it, and binds nothing. The `or-join` takes every branch's answer for every tuple. The `or-default-join` answers a tuple from the first branch with a result for it, and which branches are evaluated for a tuple is §4.2's. A `not-join` header is all required by definition.

The inference writes this header, by the rule of `NESTED_SCOPES_AS_QUERIES.md` §2.3, for a headerless form none of whose shared variables every branch binds: `[?e :a _] [?f :b _] (or-default [?e :x _] [?f :y _])` infers `(or-default-join [[?e ?f]] [?e :x _] [?f :y _])`, and an `or` of predicates over a variable a pattern binds, `[?e :n ?n] (or [(> ?n 3)] [(< ?n 2)])`, infers `(or-join [[?n]] [(> ?n 3)] [(< ?n 2)])`, the shape `TestOrJoinWithARequiredOnlyHeaderFiltersTheOuter` pins. A refusal of the header would reject a query the merge-base answered: the query `TestOrBranchExpressionIsOrderedAfterItsBinder` runs infers to `(or-default-join [[?a]] (and [(identity ?l) ?x] [?l :k :q]) (and [?m ?a _] [?x :q ?m]))`, the merge-base answered it with five tuples, and the test pins the five tuples on both planner paths. The alternative is the sentence of `docs/reference/OR_FALLBACK_SEMANTICS.md` that a per-group filter with no outputs is `or-join`'s job, which §10 replaces: the two forms keep the same tuples, and the fallback evaluates a later branch run per tuple only for the tuples the earlier branches left unanswered (§4.2). `Validate` refuses an empty header and a required run that overlaps the outputs, and nothing else about the header's shape.

### 2.2 A required variable the environment alone binds

A required variable that only a query input binds is legal for every form. The run carries the input's value in every tuple (`NESTED_SCOPES_AS_QUERIES.md` §5.4), and a header of such variables alone has the environment's one tuple as its run and the unit relation as its subject. For `or-join` and `or-default-join` the branches read it as they read any required variable. For `not-join`, and for the `not` that infers to it, the body runs over that one tuple and decides the whole scope: a body with a result empties every group, and a body with none leaves them, which is Datomic's `not-join` with its variables bound by `:in`. A header of such variables shares no data variable with the scope, and that is not a defect of the query. The inference writes this header for a headerless `not` reading only an input, `[?e :entity/kind "thing"] (not [?x :entity/flag ?flag])`, so a refusal of the header would refuse that `not` with a message about a form its author never wrote, against goal 6.

### 2.3 Refusals

§2.4 of `NESTED_SCOPES_AS_QUERIES.md` stands. Each refusal names the form, the branch by its position counted from one where a branch is at fault, the variable, and the repair. The header checks run at every boundary through one function, under the rule set at `Prepare` and with none at the text boundaries (§6).

## 3. The find boundary

One rule for both find kinds. A group the find does not read is an existence verdict: a group with no tuple empties the result, and a group with a tuple leaves the others as they are. The verdict is taken by one pull of the group's iterator, and an iterator filling a replay cache is read to its end before it is closed. With set semantics the projection of a product onto one factor's symbols is that factor when the other factor is non-empty and empty otherwise, so the verdict is the projection's own answer, and goal 3 of `NESTED_SCOPES_AS_QUERIES.md` ranks one rule for both find kinds over the merge-base's two.

The groups the find reads are returned as the group holding every find symbol, or the product of the groups that together hold them. Under aggregation, find symbols that span groups sharing no symbol are refused with the merge-base's message, the contract goal 9 keeps. The framed aggregating subquery of §4.4 never reaches that refusal: an inner query whose body reads no input is aggregated once and paired with every run tuple, so its find spans one group.

The merge-base's boundary, the baseline goal 9 names:

- A zero-symbol group and an all-environment group are absorbed, a non-empty one dropped and an empty one emptying the result.
- Every data group is kept.
- Under aggregation more than one group is refused, an unread data group included, empty or not.
- Under projection the one group holding every find symbol is returned and the rest are discarded, whether or not they hold a tuple.
- The product is taken when the find symbols span groups.

The rule above drops two of these: the aggregation refusal of an unread data group, and the projection discard. Goal 9 asks the failure each removal answers. Under the discard, a `:where` holding a pattern no tuple matches, sharing no symbol with the find, answers the other group's tuples, where a conjunction holding an empty relation is empty. Under the refusal, the same `:where` under an aggregate is refused as disjoint relations, whether or not the unread group holds a tuple. That refusal records a mechanism's limitation, which goal 6 says is not an error about the query.

## 4. The executor

### 4.1 One primitive

A scope is a header and a clause list. Each branch of an `or-join` or `or-default-join` is a scope under the form's header, a `not-join` body is a scope under its all-required header, and a subquery's inner query is a scope under its `:find` with its `:in` as the header's required run.

The primitive has two operations:

- `plan(scope)` frames the scope as the query `[:find <header> :in $ % :where (scope/required <required run>) <clauses>]` and plans it through `QueryPlanner.PlanQuery` under the rule set. The text carries the binding, so the plan is cached under the text and made once per distinct scope text. The planner leads with `(scope/required …)`, and no rule set declares into `scope`. The readers of the `scope` kind are `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.1's.
- `run(plan, run)` executes the plan with `run`, a relation of the required run's symbols, held under `scope/required` in a context that carries every relation the enclosing context holds, with `scope/required` rebound to this scope's run, and no environment. It returns a relation of the header's symbols.

The planner is the one orderer. No clause of a scope runs in written order, and no loop beside the planner orders a branch's clauses. The alternative is the merge-base's `executeInnerClauses`, which ran a branch's clauses in written order. There an expression written before its binder was skipped: `executeExpression` returns the groups unchanged when no group carries the expression's symbols. The branch of the query §2.1 quotes ran without its expression, and the merge-base answered the five tuples because that expression binds a local nothing reads. The failure the one orderer removes is at `5b152c9`, where the clause-based path panicked on that query when a tuple holding a nil value reached the fallback's tuple key.

### 4.2 Two run sizes

A branch's result for a subject tuple is a function of that tuple's projection onto the required run, by `NESTED_SCOPES_AS_QUERIES.md` §2.2, at every depth: a nested form reads its own header, a subquery its inputs, and a flat-header fallback inside a branch nothing of the branch. So running the branch per subject tuple and running it once over the distinct projection of the subject give the same tuples, provided the fallback decision reads a branch's result for the run tuple before the join on the carried outputs filters it (§4.3). What differs is cost, laziness, and where an error or the derived-tuple bound can fire. The executor chooses per branch from the branch's own clauses, with the `(scope/required …)` invocation that opens every scope plan (§4.1) not counted as a rule invocation:

- **Built once.** The branch runs once over the distinct projection of the subject onto the required run. Its result is indexed on the required run and probed per subject tuple through the branch cache (§4.6). A branch is built once when its clauses hold a rule invocation at any depth, a subquery body included, or when every clause is a data pattern, the required run is non-empty and the form is `or-default-join`. That last case is the one branch the merge-base builds once with a key: it caches a branch only under its short-circuit forms, and an all-data-pattern branch only under join symbols.
- **Built once over the unit relation.** A branch that reads no required variable, one uncorrelated subquery among them, is a constant: it runs once over the unit relation, is probed with no key, and materializes nothing of the subject, as the merge-base runs an uncorrelated subquery with no input.
- **Run per tuple.** The branch runs once per subject tuple, with that tuple's projection as a one-tuple run. Every other branch runs this way, the subject streams through it, and a cost the phase boundary adds on this path is fixed in the path, never by a second loop beside the planner.

A branch holding an invocation is built once because the invocation then seeds one rewrite and one fixpoint from the whole run (goal 2). Run per tuple, it would seed one fixpoint per subject tuple, and inside a rule body it would read the stratum's delta once per tuple where the semi-naive iteration reads it once per body run (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3). A subquery inside such a branch runs once over the combination relation of the whole run (§4.4), so the zero-default count idiom with a rule inside its subquery, `(or-default [(q [:find (count ?r) :in $ % ?g :where (reaches ?g ?r)] $ % ?g) [[?n]]] [(ground 0) ?n])`, costs one fixpoint. `TestRules_InvocationUnderAFormEvaluatesOncePerQuery` pins the count for an invocation in a branch, and §7 adds the subquery case.

A data-pattern-only `or-join` branch runs per tuple, as the merge-base runs every `or-join` branch: building it once would materialize a subject the merge-base streams, and no failure stands behind that change.

A later branch of an `or-default-join` built once is built over the whole distinct run and read only for the tuples the earlier branches left unanswered. Building it over the unanswered run would hold the first result tuple until the earlier branches had run over every subject tuple. The subject is materialized either way, so the constraint is the result's laziness: built over the whole run, the result's first tuple follows the earlier branches' answer for its own run tuple. The price is that an error, or the derived-tuple bound, can fire on a tuple the per-tuple path never evaluates, and §7 pins the tuples' sameness under both run sizes beside that difference.

A single-group subject is materialized only to compute the distinct run of a branch built once, reported through `OrFallbackOuterMaterialized` with its reason. A subject of several groups is materialized to join them into one. A subject every branch runs per tuple is never materialized by the form.

### 4.3 The shell

`OrFallbackRelation` and `OrFallbackIterator` are the shell the two branch forms run in, kept from the merge-base and changed where it computed an interface. Goal 9 keeps it: the merge-base's contracts are the baseline, and the shell carries them. The alternative is a new shell carrying the same contracts, whose cost is retargeting every test §9's step 3 restores, since those tests pin the contracts by the names of the shell's internals. The shell:

- iterates its subject lazily and reports no size
- runs each branch per subject tuple through `run`, or probes its cache when it is built once
- projects a branch tuple onto the subject's symbols and the header's outputs, filling the gaps from the subject tuple
- joins an output the subject carries by keeping only branch tuples that agree with the subject's value, never superseding it (`NESTED_SCOPES_AS_QUERIES.md` §2.3)
- joins an output the environment binds with the environment's tuple
- deduplicates across branches on the full output under the union, and short-circuits under the fallback
- surfaces a subject scan failure and a branch failure when the result is driven, never as no match and never from the constructor
- copies a tuple at the boundary when its source reuses its workspace

The shell also gains:

- the branch cache and its narrowing for a branch built once under either form
- the invocation-holding branches in the built-once set (§4.2)
- `run` in place of a loop over a branch's clauses in written order
- the fallback decision for a run tuple taken on the branch's result for that tuple, before any filter on the carried outputs. At the merge-base `nextShortCircuit` filtered a correlated branch's result against the outer tuple before deciding whether the branch answered. An output bound into a branch before a non-monotone decision changes the decision, the third example of `NESTED_SCOPES_AS_QUERIES.md` §2.3, so the decision reads the branch's result first and the join on the carried outputs follows.

Where the merge-base's shell computed an interface from what it held, the reader `NESTED_SCOPES_AS_QUERIES.md` §4 names as the defect, this design reads the header:

- The branch's visible symbols are the required run. Under the fallback form an output the subject carries is joined after the branch runs, never bound into it, since an output bound in before the decision changes the decision. Under the union the or-join handler binds it as a semi-join, which §4.4 states.
- The relation's schema is the subject's symbols plus the header's outputs, and a local every branch binds is not in it.
- The join symbols are the required run.
- The environment contributes a required symbol only when no group binds it.
- A required variable the environment alone binds narrows a built-once branch as any required variable does.
- The projection's combination sites (`projectionPlan.project`, `emitCachedMatches`, `filterBranchToOuterTuple`, the EA-built branch) filter on the outputs the subject carries and let no branch value supersede one.
- Whether a branch is built once is read from its clauses by §4.2's conditions, never from whether the form has join symbols.
- A branch cache keyed on every subject symbol a branch shares has no input: a form with an empty required run has the unit relation as its subject.
- A refusal of a required variable no group binds has no input: a required variable the environment binds is legal (§2.2).
- A nil-scope arm that unifies a headerless form on every shared free variable has no input under `Prepare`.

### 4.4 The handlers

- **`or-join`.** The subject is the groups holding a header symbol, one group unmaterialized, several joined into one. A branch run per tuple runs with the subject's required-run values and carried outputs bound, a semi-join that answers the same tuples for a monotone union as binding the required run alone and joining the outputs after, since an output another clause binds is joined (`NESTED_SCOPES_AS_QUERIES.md` §2.3). A branch built once is probed on both. With no group holding a header symbol the subject is the unit relation, each branch runs once over it, and the union joins at the collapse as a new group. The result replaces the consumed groups (§4.8) and carries the subject's symbols and the outputs. `OrClauseComplete`'s `semantics` is `"union"` when the subject is the unit relation and `"correlated-union"` otherwise, the fact the header decides, and the handler emits the or-clause events for every `or-join`, written or inferred (§4.9).
- **`or-default-join`.** The same shell with short-circuit: per subject tuple, the first branch with a result answers. The subject is the groups holding the required run, and the carried outputs are filtered after the decision, since an output bound into a branch before a non-monotone decision would change the decision.
- **`not-join`.** The subject is the groups holding a header symbol. The handler reads the subject twice, for the body's run and for the anti-join, so it materializes the subject. §4.8's read-once rule is the shell's. The body is built once over the distinct projection of the subject onto the header variables the subject carries, and the subject is anti-joined with the body's result on those variables through `AntiJoin`, which keeps the subject's keys and ordering. A header variable the environment alone binds is a constant on both sides and is no key. A header the environment alone binds has the environment's one tuple as its run and the unit relation as its subject: the body runs once and decides the whole scope (§2.2). An empty subject yields an empty run and leaves the groups as they are. A failure of the body's scan is the form's error, never an anti-join over nothing. The body is never run per combination: one run over the combinations is what keeps an invocation in the body at one fixpoint (goal 2).
- **Subquery.** The inner query runs once over the relation of its argument combinations, on the primitive. An aggregating inner query whose body reads an input is framed with its inputs as group keys. One whose body reads no input is a pure aggregate: it is aggregated once and paired with every run tuple, since adding group keys to a pure aggregate changes its kind (the janus `CLAUDE.md`, "Decorrelation Pure Aggregation Bug"). A tuple or scalar binding keeps its one-tuple check per group. An argument bound to an attribute position is checked against the vocabulary once, over the combination relation. A scalar or tuple input is a position of the run. A collection or relation input expands into the run, one run tuple per member or per inner tuple beside the outer combination's values, so an invocation in the body is seeded once from every combination. Inside a branch run per tuple the combination relation has one tuple, and inside a branch built once it has the branch's whole run (§4.2).
- **Rule bodies.** `planBody` runs a body on the primitive as it does today.

### 4.5 Rules inside scopes

An invocation inside a scope evaluates at its clause, seeded from the scope's bindings at that clause, the required run and what the planned clauses before it provide, once per scope execution. The branch holding it is built once (§4.2), so the run in that seed is the whole run as one relation. Inside a rule body the scope's context carries the stratum's held relations, so a same-stratum invocation reads the delta and never derives at the clause. The magic rewrite reads the scope's planned clauses as `NESTED_SCOPES_AS_QUERIES.md` §5.5 states.

### 4.6 The branch cache and the EA arm

A branch built once is indexed on the required run in a `groupedTupleIndex` (`buildCachedBranch`) and probed per subject tuple, the probe returning a subslice of the index's backing with no allocation. The grouped index's contracts, one span per distinct key, tuples within a span sharing one key, a fan-out probe returning every tuple for the key in collection order, a mixed span resolving to the probed key's tuples alone, and zero allocations on hit, miss and collision, are pinned beside the index.

A branch of one data pattern on the default source, whose entity position is the one required variable the branch reads, with the attribute declared cardinality one and the matcher non-temporal, the conditions `PatternMatcher.CanFuseAttributeFetch` states for substituting a lookup for a scan, is built from the EA cache through `buildBranchFromEACache` under `EnableEntityPrefetch` (§4.10): one `LookupAttribute` per distinct entity, the environment-bound value narrowing the result when that value is in the required run. The source condition holds because an entity lookup is the default source's answer, which the executor's attribute-fetch fusion already requires of the pattern it fuses. The entity-alone condition follows from the cache's key: the EA-built branch is keyed on the entity, so a required `V` the subject carries would go unmatched. No warmth is read, and §7 pins that the arm answers as the scan path does whatever the cache holds. The arm runs under the option that runs the prefetch, whose warm cache is the arm's benefit, so a query with the option off pays nothing for it (goal 7), and §8 measures the arm with the option on and off. A branch that fails a condition takes the scan path, narrowed to the distinct join keys.

### 4.7 Properties

The shell derives the result's properties through `orProperties` from the subject's properties and the header: the outputs are what the branches produce, and nothing is overwritten, since an output the subject carries is joined and never superseded (§4.3), so no subject symbol drops its key or ordering on the branches' account. Whether every branch answers at most one tuple per subject tuple is judged against the header symbols the subject carries, the grain the result is produced at. A subject key and ordering survive branches that answer at most one tuple; multi-tuple branches derive a composite key. The derivation emits `OrPropertiesDerived`. Every reader of the properties, the projection's dedup elision, the join's strategy and build side, and the scan-boundary restoration, takes the decision it takes on the merge-base for a result the shell produces. A union over the unit relation (§4.4) is a new group with the full-header key and no ordering, and no subject property reaches it. The `not-join` keeps its subject's properties through `AntiJoin`.

### 4.8 Group replacement and read-once

The form's result replaces the consumed groups through `replaceConsumedOrGroups`, with no join at the collapse, and emits `OrOuterReplaced`. A streaming subject is read once: the result stands in the subject's place, and a reader that needs the result twice materializes the result, never the subject.

### 4.9 The events

Every name in `datalog/annotations/types.go` whose fact this mechanism produces emits with `main`'s payload: `OrClauseBegin` and `OrClauseComplete` from the or-join handler, `OrClauseUnion` and `OrClauseBranchComplete` from the unit-run route, `OrPropertiesDerived` from the derivation, `OrOuterReplaced` from the replacement, and `OrFallbackOuterMaterialized`, `OrFallbackIteratorCreated`, `OrFallbackOuterTuple`, `OrFallbackOuterExhausted`, `OrFallbackBranchNarrowed`, `OrFallbackBranchSuccess` and `OrFallbackCacheBuild` from the shell. The handlers' own contract:

- The fallback handler emits its shell's events and no or-clause event.
- The completion event of a streaming form fires at construction with size -1, and a branch failure surfaces when the result is driven.
- A scope's run emits no `PhaseBegin` and no `PhaseComplete`: the form's own events report a branch's execution, and a phase event per subject tuple would be a handler call per tuple on the per-tuple path.
- §7's counts of branch executions read the shell's events.

### 4.10 Prefetch

One option, `EnableEntityPrefetch`, governs every prefetch trigger, so a query with the option off pays nothing (goal 7). The executor's trigger after the first data pattern stands as on `main`. The fallback's trigger, Rule 6's first trigger point in `docs/ALGEBRA.md`, runs under the same option, the same entity threshold and Rule 6's temporal guard, before the shell's first per-tuple evaluation, when a branch reads the subject's entity symbol. It lands as its own step, after the contracts, with `BenchmarkComplexQueryCheckpoint` measured with the option on and off before and after it.

## 5. The algebra engine

§5.3 of `NESTED_SCOPES_AS_QUERIES.md` stands, with these statements, each true of the tree or landing at the step of §9 named beside it:

- A union of one branch is legal, in `parseOrClause`, `parseOrJoinClause` and `compileOrUnionCorrelated` (§9 step 6).
- `orderClausesForCompile` checks every list's readiness, a lone clause's included, with no early return for a one-clause list. The refusal of a one-clause branch reading outside the header is `Prepare`'s, pinned by `TestPrepare_OrJoinBranchOfOneClauseReadingOutsideTheHeaderIsRefused`, and §7 adds the pin of the fold's own check.
- `analyzeUnionBranches` keeps its header check, the analyzer's contract on the union node that every pass must keep, and `TestOrJoinSchemaDiagnosticExplainsHeaderContract`, restored from the merge-base at §9 step 6, is its test.
- `compileNotJoin` carries no check of a header symbol the body never mentions (§9 step 6): `Compile` takes a prepared query (`NESTED_SCOPES_AS_QUERIES.md` §5.3), `Prepare` refuses the shape, and a check nothing reaches is a branch no test can drive. A test that compiles written text calls `Prepare` first.
- The decompiler has no arm for `OrClause`, `NotClause` or `OrDefaultClause` (§9 step 6).
- The executor's entry order is the one `NESTED_SCOPES_AS_QUERIES.md` §5.1 states, with the vocabulary check moving after `PlanQuery` at §9 step 8.

## 6. Boundary parity

The header checks, whether every branch binds every output and whether a body reads outside its header, run at `ParseQuery`, `qb.Build` and the executor's entry alike, as they did at the merge-base. `NESTED_SCOPES_AS_QUERIES.md` §5.5 states that fact and the alternative it excludes. One function computes them: it infers a written form's branches under the form's header and runs `checkHeader`. The `Validate` methods call it with no rule set, under which an invocation provides every variable argument, so a text boundary refuses a subset of what `Prepare` refuses and never what the rule set would accept. `Prepare` calls it with the rule set. One message per fact carries `main`'s phrase and the parts `NESTED_SCOPES_AS_QUERIES.md` §2.4 names: the form, the branch counted from one, the variable, the repair.

Goal 5 binds the builder and the formatter the same way. `qb.Build` accepts an inner query with no `:in`, as the parser does, and `FormatQuery` renders a byte constant and an instant constant in the form the parser reads, so every phase the planner emits parses back (§9 step 9).

## 7. The contracts the tests pin

- The four worked examples of `NESTED_SCOPES_AS_QUERIES.md` §2.3, the query §2.1 quotes, the consumers' shapes of §8, and the generated queries over random graphs give one answer on both planner paths in every written order.
- A streaming subject of a form whose every branch runs per tuple yields its first result tuple after one pull, the result reports no size, and the subject is read once. A form with a built-once branch materializes its subject before its first result (§4.2).
- A subject scan failure and a branch failure surface when the result is driven, and a failed preferred branch never falls through to the default.
- A branch holding an invocation runs one rewrite and one fixpoint per form execution, whatever the subject's size, and a rule inside a subquery inside a branch costs the same one fixpoint.
- A branch built once is evaluated once and probed per tuple; a branch of seeks runs once per subject tuple; the grouped index holds its contracts.
- A branch built once over the whole run answers the tuples the per-tuple path answers, and an error or the derived-tuple bound in a later fallback branch fires on a tuple the per-tuple path never evaluates.
- The fallback decision for a run tuple reads the branch's result before the carried-output filter: a subject tuple whose carried output disagrees with branch 1's answer is answered by branch 1 and dropped, never passed to branch 2.
- The result's keys and ordering follow §4.7, and each reader of them decides as on `main` for a result the shell produces.
- A written flat `or-join` keeps its subject's key and ordering through branches that answer at most one tuple, replaces its consumed groups, and seeks on the subject's values.
- A flat-header `or-default-join` decides its fallback once: when branch 1 answers for any tuple, the default fires for none.
- The result replaces the consumed groups with no join, and `OrOuterReplaced`, `OrPropertiesDerived` and the shell's events carry their payloads.
- An output the subject carries is joined, and an output the environment binds is joined with the environment's tuple.
- A required-only header runs and filters.
- An environment-only header runs and decides the scope, over a data-pattern body and over a predicate-only body.
- A headerless `not` reading only an input reaches the answer its inferred header gives.
- A failure of a `not-join` body's scan is the query's error.
- The find boundary's rule of §3 holds for projection and aggregation.
- The three text boundaries refuse a subset of what `Prepare` refuses, with the same message for each shape they refuse.
- A `:where` of one form whose required run nothing binds is refused on both planner paths naming the form and the variable.
- The EA arm answers as the scan path for a cardinality-one attribute on a current handle whatever the cache holds, and never runs for a many-valued attribute, under `History`, for a pattern on a named source, or with `EnableEntityPrefetch` off.
- With `EnableEntityPrefetch` off no prefetch runs, and the base query's cost is unchanged by the trigger's presence.
- A subquery over its combination relation gives the per-combination tuples.
- A pure aggregate over a combination matching nothing binds no tuple.
- An aggregate whose body does not read the input is paired with every run tuple.
- A collection input yields one run tuple per member.

## 8. Measurement

Each number is taken under both optimizer modes, five runs, before and after the step that changes the path, and recorded in `PERFORMANCE_STATUS.md` beside the merge-base's and `5697a7d`'s:

- `BenchmarkOrJoinScope`, `BenchmarkOrDefaultJoinScope` and `BenchmarkNotJoinScope` at a thousand and a hundred thousand subject tuples, streaming and materialized, and a flat `or-join` over a streaming subject that carries its outputs at the same sizes
- `BenchmarkComplexQueryCheckpoint`, with `EnableEntityPrefetch` on and off
- `BenchmarkOrFallbackPropertyPropagation` at ten thousand and a hundred thousand tuples
- `BenchmarkTransitiveClosure` at a thousand, ten thousand and a hundred thousand edges, semi-naive against naive, and `BenchmarkBoundInvocation`
- `BenchmarkPrepare`
- the subquery benchmarks of the executor
- the storage package's wall time under wasm, whose rise `PERFORMANCE_STATUS.md` records as `TestRules_ConcurrentQueriesDeriveWhatSerialQueriesDerive` at `12e8930`, taken after that test's evaluations are reduced to what its contract needs, so the package runs under `go test`'s default

The consumers' shapes are acceptance tests, in a bindery domain no fixture uses:

- A `reaches` rule whose body holds an `or-join`, seeded by a collection input of changed sheets, answering the reference and emitting one `RulesMagicSets` event, with its time at a thousand and ten thousand sheets recorded.
- A readiness rule over predecessor quires whose latest lifecycle event is a given kind, with stratified negation and a recursive propagation rule, answering the reference.
- A reachability query over the quire graph for the quires still finishable from a given state, answering the reference.
- A reconcile query over N quires invoking a reachability rule inside a `not-join`, emitting one `RulesStratumBegin` per stratum whatever N is, with its time at N of a thousand and ten thousand recorded.
- A fallback over N quires whose first branch is a zero-default count subquery invoking a rule, emitting one `RulesStratumBegin` per stratum whatever N is.

No benchmark decides a contract of §4. The numbers confirm. A regression of the base query with an option off is a defect in the option's gating, and a cost the phase boundary adds on the per-tuple path is fixed in that path (§4.2).

## 9. Order of work

Each step is its tests, then its code, then the same tests, then the differential check, its differences named.

1. The algebra-path differences of the random-graph differential (`TestQueryShapesOnRandomGraphsMatchTheReference`): a `not` body holding a count subquery over a header variable its pattern does not bind, with the compile test that pins the shape, so every later gate starts clean.
2. The gate tests of §7 that the tree lacks, written as reds or pins before any executor code: the streaming read-once, the invocation-once count, the branch-local and branch-binding tests, the output join, the fallback per required group, the anti-join on the header, the plan served from the cache, the scope inside a body reading the stratum, the fallback decision's point, the two run sizes' sameness and their error difference, the flat `or-join`'s properties and replacement, the flat-header fallback's global decision, the `not-join` body scan failure, the one-form `:where`, and the prefetch option's isolation.
3. The primitive and the shell (§4.1 to §4.3, §4.6 to §4.9): the shell restored from the merge-base onto the primitive with the header reads of §4.3, both run sizes, the events, the properties, the replacement, the read-once, the branch cache with the grouped index's pins restored, and the EA arm under §4.6's conditions. The merge-base's deleted tests restored and retargeted, each dropped assertion named with §2.3 of `NESTED_SCOPES_AS_QUERIES.md` where the join replaces supersession.
4. The `not-join` handler (§4.4).
5. The subquery on the primitive (§4.4) and the rewrite on planned scopes (`NESTED_SCOPES_AS_QUERIES.md` §5.5).
6. The language amendments (§2), the find boundary (§3), boundary parity (§6), the algebra facts (§5), and the rest of the algebra targets `NESTED_SCOPES_AS_QUERIES.md` §6 lists against the merge-base, each with its compile test.
7. The fallback's prefetch trigger under the option (§4.10), measured.
8. The contracts weakened since the merge-base:
   - the roster on headers;
   - the engine's vocabulary;
   - the overlap test of two `SetSchema` calls, made to wait for the second call;
   - the cache contracts;
   - the schema-retention contract;
   - the parse-cache contract.
9. The remaining language and builder items:
   - the builder's `%` rule;
   - the builder's acceptance of an inner query with no `:in` (§6);
   - the formatter's byte and instant rendering (§6);
   - the test of the vocabulary check on a subquery's constant argument (`NESTED_SCOPES_AS_QUERIES.md` §5.4);
   - the rename of the tuple position in the executor's bound-attribute check;
   - the free invocation of a predicate whose strata hold a required one;
   - the headerless arms of the fixpoint's delta marking and of the magic rewrite's clause copier, which bodies with inferred headers never reach (`NESTED_SCOPES_AS_QUERIES.md` §5.1).
10. The benchmarks of §8, the reduction of `TestRules_ConcurrentQueriesDeriveWhatSerialQueriesDerive`'s evaluations with the wasm run's time after it, and the acceptance tests.
11. The records of §10.
12. `make test`, native and wasm.

## 10. Records

- `NESTED_SCOPES_AS_QUERIES.md` §5.4 states the primitive, the two run sizes and the shell once step 3 lands.
- `NESTED_SCOPES_AS_QUERIES.md` §7 states the two paths and the measurements of §8 once step 10 lands.
- `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3 and §4 state the rewrite's order, the negated invocation, the decline, the engine's vocabulary and the roster as `NESTED_SCOPES_AS_QUERIES.md` §5.5 and §8 state them, in place of the sentences that cite `magicRewriter.rewriteBody`, `deriveInvocation`, `engineAttributes` and `Query.collectRosters`, once steps 5, 8 and 9 land.
- `docs/reference/OR_FALLBACK_SEMANTICS.md`: the required-only header, the environment-bound required variable, and the union and correlated routes by the header.
- `docs/ALGEBRA.md`: "OR-Fallback Branch Caching" states the built-once condition of §4.2, and Rule 6's "What needs to exist" names the fallback trigger under the option.
- `docs/reference/PLANNER_OPTIONS.md` and the `EnableEntityPrefetch` comment in `datalog/storage/database.go`: one option, every trigger.
- `docs/reference/RULES.md`: an invocation inside a scope evaluates once per scope execution, and the sentences on a subquery body's order, the passing and the decline follow `NESTED_SCOPES_AS_QUERIES.md` §5.5.
- `RELATIONAL_ALGEBRA_OVERVIEW.md` and `PERFORMANCE_STATUS.md`: the mechanism and the numbers.
- The janus `CLAUDE.md`: "Handling Disjoint Relations in Query Execution" states §3.
