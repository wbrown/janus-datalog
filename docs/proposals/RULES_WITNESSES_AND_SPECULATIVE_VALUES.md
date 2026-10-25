# Proposal: Rules, Witnesses, and Speculative Values

**Builds on:**

- [../reference/RULES.md](../reference/RULES.md): the rule syntax, rules passed as `%`, semi-naive evaluation, stratification
- [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md): `Fork`, named snapshots, the overlay as a `PatternMatcher` source (its §2 and §9.6)
- [../reference/MULTI_SOURCE.md](../reference/MULTI_SOURCE.md): named sources, `SourceRouter`, `MemoryPatternMatcher`
- [../reference/CRDT_STREAMING_RESOLUTION.md](../reference/CRDT_STREAMING_RESOLUTION.md): resolution as index ordering plus filtering
- [../reference/PLANNER_OPTIONS.md](../reference/PLANNER_OPTIONS.md): where per-query options live
- [../INPUT_PARAMETER_SEMANTICS.md](../INPUT_PARAMETER_SEMANTICS.md): `ClauseBlockers`, the error that names a missing symbol

---

## Summary

This proposal makes six additions. Each one lands on a seam the engine already has.

1. **Rules and recursion.** Rules are passed as `%`. The engine evaluates them to a fixpoint with semi-naive iteration, stratifies negation, and keeps the bounds a caller sets. The derived relation joins the rest of the query through the invocation clause.
2. **Witnesses.** For a result tuple, the base datoms and rules that derived it. For an expected tuple that is absent, the first clause that failed and the bindings reached before it. For a datom, the commit that asserted it, and the commit carries the caller's attributes.
3. **Speculative values.** `With` is a read-only database value over the base plus an uncommitted overlay of adds and removes, on every backend. `Diff` evaluates one query against two database values and returns the two differences of the results.
4. **Schema as facts.** Attribute definitions are stored as system datoms. Patterns can query them, and dumps carry them.
5. **Errors and bounds.** Rule errors name the variable, the cycle, or the predicate at fault. An evaluation has an iteration cap, a derived-tuple cap, and cancellation.
6. **Surface.** The EDN query form plus `%` rules stays the one grammar. A second grammar is deferred, and §6 records the mapping question it would have to answer.

Everything below is design. Nothing in it describes what the engine does today unless the sentence says so.

---

## Goals

The design answers to these six goals, one per addition.

1. A caller expresses a transitive closure as a query, never as a loop in Go.
2. A result explains itself: the datoms and rules behind a tuple, the clause that failed for an expected tuple that is absent, and the commit behind a datom.
3. A caller evaluates a query against a state it has not committed, and against two states, on every backend.
4. The schema is a fact of the store, queryable and carried by a dump.
5. Every error a query author can repair names the thing to repair, and every evaluation is bounded and cancellable.
6. One grammar.

---

## 0. The substrate each addition lands on

- A pattern's fourth position binds the datom's ElementID. The branch-record query in `datalog/storage/fork.go` reads it (`[?b :db.branch/replica _ ?written]`).
- The optimizer's contract is `query.Query` in, an algebra tree in the middle, and `query.Query` out. The algebra never constructs a plan. The executor runs over `Relation` operators (`ARCHITECTURE.md`, "How the Planner Shapes Execution"). A transform pass lives in `datalog/algebra`, beside `rewrite_decorrelate.go` and `rewrite_getelse.go`.
- `datalog/algebra/provenance.go` records optimizer rewrite decisions as `RewriteRecord` values. That name is taken by transform provenance, so this proposal says _witness_ for data provenance throughout. Its pass file is `rewrite_witness.go`.
- A relation input, `:in $ [[?x ?y] ...]`, binds a relation over its symbols, which the query joins once, by symbol (`BindQueryInputs`, `datalog/executor/executor_utils.go`). An aggregate in the `:find` is taken over the whole join, grouped by the find's variables (`NESTED_SCOPES_AS_QUERIES.md` §2.5).
- `SourceRouter` routes each pattern to exactly one `PatternMatcher`, by the pattern's source symbol. No source reads two matchers as one.
- `Database.Fork(name)` starts a branch from a named snapshot on the memory-tree store. The branch writes as its own replica. `BadgerStore` does not implement the `forker` interface.
- `Transaction.Remove` makes a cardinality-one attribute absent in the current view (`docs/CORE_MODEL.md` §1).
- A commit writes one `:db/txInstant` datom on its transaction entity. `Transaction` carries no caller attributes. Only `SetTime` touches the transaction entity.
- A `ReadSession` pins one snapshot for every read a query makes.
- Every query-executing test runs under both optimizer modes, and the optimizer never changes results.

---

## 1. Rules and recursion

This section describes what the engine does.

**The contracts the tests pin.** These tests pin §1:

- `datalog/query/rules_test.go`: the set and its refusals.
- `datalog/planner/magic_test.go`: the rewrite.
- `datalog/parser/rules_test.go`: the parser.
- `datalog/executor/rules_test.go`: the fixpoint, the invocation's rename and selection, the naive step against the semi-naive one, and the bound invocation.
- `datalog/storage/rules_test.go`: the closure on a chain and on a cycle, stratified negation, invocations inside a subquery, inside `or`, `or-join`, `or-default` and `or-default-join` branches, and under `not-join`, bound, constant and blank arguments, the required head, the caps, and one snapshot per query, each on the backend and optimizer matrix.
- `datalog/qb/rules_test.go` and `cmd/datalog/rules_test.go`: the builder and the CLI.

`BenchmarkTransitiveClosure` in `datalog/executor` and in `datalog/storage` measures the closure at a thousand, ten thousand and a hundred thousand edges, semi-naive against naive. The naive baseline is the semi-naive step driven with the full relation at every occurrence. It has no production flag and no second evaluator.

A production flag is excluded twice over. As a package-level variable, "No Global Configuration State" in the janus `CLAUDE.md` forbids it. As an option on `PlannerOptions`, nothing in production would set it, and its one reader would be the benchmark's naive leg.

A second evaluator is excluded, as §1.3 states.

`BenchmarkBoundInvocation` beside each measures the closure with its start bound to one node. The numbers are in `PERFORMANCE_STATUS.md`.

### 1.1 Syntax and types

The syntax is the one [../reference/RULES.md](../reference/RULES.md) states. A rule set is a vector of rules. Each rule is `[(name ?args...) body...]`. The set is passed as the `%` input. A query invokes a predicate as `(name args...)` in `:where`. An argument is a variable, a constant or `_`. Several rules for one head are alternatives. A head may begin with a vector of variables, the required-binding form `[(rule [?x] ?y) ...]`. The caller binds those positions.

Types in `datalog/query`:

- `Rule`: the head name, the head symbols, the required run, and the body clauses.
- `RuleSet`: the rules grouped by name, each predicate's arity and required positions, the strata, and the attribute roster the bodies name, collected once. `Required` reports a predicate's required positions. `planner.Rewrite` (`datalog/planner/magic.go`) rewrites the set for a binding pattern, in the planner's order (§1.3).
- `RuleInvocation`: a clause holding the name and the arguments. Its scope comes from the rule set the query runs with. `ScopeOf(clause, rules)` makes the variables at the predicate's required positions mandatory correlates and provides the rest.
- `RulesInput`: the `%` input spec.
- The engine mints predicates in namespaces of its own. `adorned.bf/reach` is `reach` adorned with a binding pattern. `magic.bf/reach` is its magic predicate. `delta/reach` holds the tuples the last iteration of its stratum added (`DeltaSymbol`). `scope/required` is the required run a nested scope runs over (`ScopeRequired`). The kind leads the namespace, and the predicate's own namespace follows it. It mints variables the same way: `?unify/x.2` is a pattern's later occurrence of `?x` (`UnifiedVariable`), `?scope/input.2` the holder of a collection input's whole value (`ScopeInput`), and `?magic.bf/reach.0` a seed predicate's head position. The namespaces are the engine's for predicates and variables alike: `NewRuleSet` refuses a rule set that declares a predicate in one of them or names a variable in one, and `Prepare` refuses a query naming a variable in one, each before the inference writes the engine's own, naming the variable, the namespace and the repair (`engineNamespace`, `EngineNamespaceError`). `ParseQuery` and `qb.Build` accept the spelling, since the prepared text carries the engine's variables and every phase the planner emits parses back through `ParseQuery` (`NESTED_SCOPES_AS_QUERIES.md` §5.5). The alternative, minting names no user text can spell, is excluded by that parse-back.

`parser.ParseRules` parses a rule set. `parseInputSpec` accepts `%`. A list clause whose head names no reserved form is an invocation. An invocation requires `%` in its query's own `:in`, and so does a subquery call that passes `%`. The query builder has `qb.Rules`, `qb.Rule`, `qb.DefineRule`, `qb.RuleSet`, `qb.MustRuleSet` and `RuleBuilder.Require`. The CLI reads the `-in` value at the `%` position as rule text.

### 1.2 Safety and stratification

A rule is safe when every variable it must bind is bound. The variables it must bind are the head symbols and every symbol that a predicate, an expression, a subquery input, an explicit-join header or a body invocation's required position consumes. A positive body clause binds a symbol, and so does the head's required run. An invocation binds its variable arguments at the positions its predicate does not require.

`query.NewRuleSet` refuses an unsafe rule as `UnsafeRuleError`, naming the rule and the symbol. It refuses an arity disagreement as `RuleArityError`. It refuses an invocation of an undefined predicate as `UnknownRuleError`, naming the defined predicates. It refuses a required run that does not lead the head, naming the rule. It refuses a required run that differs between two rules of one predicate, naming the rule.

The stratifier builds the predicate dependency graph. An edge through `not` or `not-join` is negative. The strongly connected components are the strata. A component holding a negative edge is refused as `UnstratifiableError`, naming its predicates. The rest are ordered.

A same-component invocation may stand at the top level of a body. It may stand inside an `or` or `or-join` branch. It may stand inside a subquery whose inner query does not aggregate, since that subquery is a join over its combinations and evaluation stays monotone (`NESTED_SCOPES_AS_QUERIES.md` §2.5). Under `or-default`, `or-default-join` or an aggregating inner query, at any depth, it is refused as `NestedRecursionError`, naming the rule, the predicate and the form. Fallback is non-monotone, and an aggregating inner query is aggregation inside the component.

Aggregation inside a recursive component is refused. Deferring it costs the queries §9 lists, shortest paths and counts over closures, which a caller computes in Go over the closure. Admitting it would cost a monotonicity condition the evaluator checks per aggregate, since a non-monotone aggregate in a cycle has no fixpoint. A rule body aggregates over a completed stratum through a subquery, as a query does.

### 1.3 Evaluation

The rule set rides the execution context, as the environment does. `Database.convertInputsToRelations` (`datalog/storage/database.go`) consumes the `%` input. `Database.Query` puts the rule set on the `executor.Context`. The alternative is a relation slot for the rule set. Every `:in` walker would then treat it as a relation. `BindQueryInputs` (`datalog/executor/executor_utils.go`) binds a relation for every scalar, collection, tuple and relation input, and `convertInputsToRelations` would hand it the rule set as one more.

`ExecuteWithRelations` prepares the query through `Prepare` (`datalog/query/prepare.go`), which checks every invocation against the set. The executor then plans the query under `query.Bindings` (`datalog/query/bindings.go`): the symbols the inputs bind, and the rule set. The planner reads an invocation's scope from the rule set, so it orders the invocation after the clauses that bind its required positions.

The planner reads from an invoked predicate's name whether a seed restricts its relation (`SeedRestricted`). A magic or adorned predicate, the delta of one, and `scope/required` are seed-restricted. The planner schedules such an invocation before the patterns, and the patterns then seek on what it binds. Two alternatives are excluded. A mark on the rule would be a second representation of a fact the name already carries. A planner input naming the held predicates would be a fact the text does not render, which the parse-back rule below excludes.

A clause with no visible selectivity is scheduled only when no other clause is selectable. Visible selectivity is a constant beyond a pattern's attribute or an invocation's name, a variable that is available when the clause is selected, or a seed restriction. An invocation with an available argument waits on no such scan. So a scan of a whole extent never binds a bound invocation further (`NESTED_SCOPES_AS_QUERIES.md` §5.2, `REWRITE_ORDER_PROOF.md` T3).

The planner's input is the query and those bindings. Every phase it returns declares `%` when one of the phase's own clauses uses `%`, by invoking a predicate or by passing `%` to a subquery. Every phase parses back to itself. The alternative is a clause mark the text does not render. Under it a phase does not parse back: a body passing only `%` printed a phase the parser refused.

An invocation evaluates its predicate when its clause runs. The derived relation it reads is materialized (`derivedSet.relation`, `datalog/executor/rules.go`). Two alternatives are excluded. A subquery pattern has no inner query to run, since a fixpoint is not a query. Evaluation before planning derives the predicate whole and leaves the invocation to filter it, at the cost the closure measurement below prices.

An invocation's bound positions are the ones holding a constant, or a variable the environment or a relation group binds. With none bound, the needed strata evaluate to a fixpoint and the invocation reads the predicate whole (`deriveInvocation`, `datalog/executor/rules.go`). There is one exception. When a needed stratum holds a predicate with required positions, the set is rewritten with the invocation's head all free, and each required predicate is adorned from the clauses before its invocation (`NESTED_SCOPES_AS_QUERIES.md` §5.5).

With some positions bound, the rule set is rewritten for the binding pattern. Four alternatives are excluded. Deriving the predicate whole and filtering it to the bindings costs the whole closure whatever the invocation binds: at a hundred thousand edges the closure costs 0.3 to 1.1 s on the backends, and the bound invocation 0.09 to 0.17 ms (`PERFORMANCE_STATUS.md`, "Rules and Recursion, Bounds and Cancellation"). A fixpoint per bound value runs the evaluator once per value, the N+1 shape. A second evaluator is a parallel implementation, which "Stop Creating V2 Versions" in the janus `CLAUDE.md` excludes. An index on the bound positions narrows the filter but still derives the closure whole.

Each predicate the bindings reach gets an adorned copy, `adorned.bf/reach`. A magic predicate, `magic.bf/reach`, guards it. The bound values, projected from the relation groups, seed the magic predicate. The rewritten set evaluates as any set does. The invocation reads the adorned predicate.

The bindings pass through each body in the planner's order of that body as a scope, and flow into the invocations after them. A subquery in a body is read as a scope of the rewrite, as a branch is. `magicRewriter.rewriteClauses` (`datalog/planner/magic.go`) walks the planned clauses (`NESTED_SCOPES_AS_QUERIES.md` §5.5). The non-monotone forms are a negation, a fallback form and an aggregating inner query. An invocation under one of them receives the bindings of the clauses before the first invocation of the rule's own stratum. That bound set is the sealed passing. So no magic rule depends, through such a form, on that stratum's adorned predicate (`NON_MONOTONE_FORMS_PROOF.md` T6).

Some sites under a non-monotone form cannot be adorned. Either adorning the site would place its adorned predicate in the rule's own stratum, or the sealed passing leaves a required position free. Such a site is left unadorned, its predicate's original rules join the set, and the set is rebuilt without it. So every rewrite is accepted, and none is declined (`NON_MONOTONE_FORMS_PROOF.md` T6, L4). A predicate with required positions at such a site is derived at its clause, under the positions the branch binds there (its T5). Each rewrite is recorded as a `rules/magic-sets` event carrying the predicate and its adornment (`magicSetsEvent`, `datalog/executor/rules.go`).

A recursive stratum iterates semi-naively. Iteration 0 absorbs the seed, when the stratum has one, and runs the rules with no same-stratum invocation. Each later iteration runs one copy of each body per same-stratum occurrence in it. In that copy the occurrence invokes the predicate's delta symbol, `delta/reach`. The scope holds the previous iteration's delta under that symbol, so the occurrence reads it. Every other occurrence reads the full relation. The tuples not yet derived become the next delta. The stratum ends when every delta is empty.

A body is an ordinary `query.Query`, planned under the rule set. Its plan enters the plan cache under the body's own structure, which carries `%` and the delta symbols, so the cached plan is the one the fixpoint would make. The alternative is keeping body plans out of the cache. That would be an exception to the cache's one rule: every plan is served under its query, its rule set and the symbols bound when it was made (`NESTED_SCOPES_AS_QUERIES.md` §5.2). A body is planned under no bound symbol, so the rule covers it. `stratumEvaluation.run` (`datalog/executor/rules.go`) runs each body's plan through `executeRealizedPlan`, the phase executor. So base patterns stream from storage, and an invocation's relation joins at collapse. The whole fixpoint runs inside the query's one `ReadSession`, so every iteration reads one snapshot.

The invoking query receives the derived relation through the invocation clause, under the call's substitution. The relation is renamed to the call's variables. It is selected on the constants, on the repeated variables, and on the environment's bindings. The blanks' positions are projected away. Phasing and selectivity scoring see an invocation as a data source. It provides its variables and requires the positions its predicate declares as required.

### 1.4 Bounds

`PlannerOptions` has two fields for this. `MaxRuleIterations` caps the iterations per stratum. `MaxDerivedTuples` caps the tuples per evaluation of an invocation, with the magic predicates counted. Zero is unbounded. Exceeding either is `RuleIterationLimitError` or `DerivedTupleLimitError`, naming the predicates, the limit and the count reached.

---

## 2. Witnesses

A witness of a result tuple is one derivation of it. For each scan in the derivation it holds the datom the scan matched. For each rule step it holds the rule and the body tuples. The engine computes witnesses per query and stores none. A result's derivations change with the datoms, so a stored witness goes stale at the next commit, while a query over its snapshot recomputes them.

**The contracts the tests pin.** Each is the subject of one test:

- A three-scan query with a self-join, witnesses checked against the polynomial computed by hand, in both optimizer modes, with the rewrite's decision recorded through `RewriteSink` and asserted.
- A query with `get-else` whose default fires for some entities and not others.
- An aggregate query whose group witnesses are checked against the group members' datoms.
- A closure over a cycle whose recorded witness is the least derivation of minimum depth, computed by hand, and the same under both optimizer modes.
- `WhyNot` on a query with a scan that matches nothing, a predicate that rejects everything, and a `not` that excludes the expected tuple, each naming the right clause.
- `Transaction.Entity()` attributes read back through `(commit ...)` on the live database and on a dump of it.
- A benchmark: the OHLC query with the witness rewrite on and off.

### 2.1 Why a tuple holds: the witness rewrite

For a query without rules, a transform pass in `datalog/algebra/rewrite_witness.go` runs after the existing passes:

- Every `Scan` whose pattern has three positions gets a fourth, a fresh symbol bound to the matched datom's ElementID.
- Every `_` in a scan becomes a fresh symbol, so the scan's bound elements together with the ElementID name the matched datom.
- The `:find` is extended with the fresh symbols, in scan order.

The unmodified executor then returns one tuple per derivation. Tuples that agree on the original `:find` symbols are the derivations of one result. A `Relation` is a set, so two derivations with the same witnesses in the same positions are one tuple. The polynomial the result encodes therefore has boolean coefficients.

The fold at the result boundary chooses what to report, per query, through a planner option. Two alternatives are excluded. A package-level fold setting is forbidden by "No Global Configuration State" in the janus `CLAUDE.md`. A database option, the shape `RefuseUnknownAttributes` has in §4, does not fit, because the witness result extends the query's own `:find` with the witness symbols, which is a fact of one query and not of the database. The folds:

- _lineage_: the union of the witness datoms over the derivations of a result
- _why_: the set of witness sets, one per derivation
- _how_: the witness sets with their scan positions, which is the polynomial up to multiplicity

A fold ranges over the derivations the engine enumerates: one tuple per derivation through the scans, and for a derived tuple the one derivation the fixpoint records (see Rules below).

A scan the optimizer introduced is a scan the pass sees. The `get-else` rewrite in `rewrite_getelse.go` turns a database function into a scan with a left outer join. So a `get-else` value carries the datom it read, or no witness when the default fired. `get-some` and `missing?` will be read through the same rewrite once each has a scan form. Until then the pass refuses a query that uses them with witnesses on, naming the function.

**Negation.** A `not` body is not rewritten. A derivation through an anti-join carries no witness for the absence it depends on. §2.2 explains the absence instead, run on the negated body with the outer bindings.

**Aggregation.** Witness symbols change group membership, so the aggregate must not see them. The pass evaluates the aggregate over the unrewritten body. In the same plan it evaluates the rewritten body, projected to the group keys and the witness symbols. It pairs the two on the group keys at the boundary. An aggregate result's witnesses are the union of its group's witnesses.

**Rules.** Witness tuples do not terminate on a cyclic rule, since every longer walk around the cycle is a new tuple. The fixpoint instead records one derivation per derived tuple: the rule and the body tuples. The alternative is a minimal-witness label, the set of every minimal derivation of the tuple. No goal needs it. Goal 2 is met by one derivation. `Diff` (§3.3) marks a changed tuple's dependence on the overlay by one derivation, since every derivation of a tuple in `added` uses an overlay datom. The label would cost a fixpoint over labels, because a tuple's label can gain a minimal witness after the tuple first appears, so a stratum would iterate until its labels stabilize. A closure tuple's why-label holds one witness per simple path, exponential in the graph, so goal 5 would need a cap on it. The derivation recorded is the least one of minimum depth. The body's scans carry their ElementIDs, so each derivation of a head tuple is a distinct body tuple. Semi-naive iteration finds a tuple first at its minimum depth. That iteration produces every derivation of that depth, since each one uses a body tuple of the depth before at some occurrence, and the copy reading the delta at that occurrence produces it. The fixpoint keeps the least of them. The order compares the rule's position in the set first, then the body tuple's values position by position, witness symbols included. The alternative is the first derivation produced. That is the plan's choice among those of minimum depth, so the optimizer would change the witness, against §0. A derived tuple's witness is the chain of recorded derivations down to base datoms.

### 2.2 Why a tuple does not hold

`WhyNot(q, expected Tuple, inputs...)` binds the query's `:find` symbols to the expected values. It runs the `:where` clauses in planner order against those bindings. It returns the first clause that produced no match, together with the bindings reached before it. For a rule invocation it descends into the rule's clauses the same way, one alternative at a time, and reports each alternative's failing clause. `Explain` is the plan explain, which §5 lists among the callers of `WithContext`.

The executor already stops a phase on an empty relation group and emits that as an annotation event. The addition is a return value: the clause, its position in the planned order, the bindings available, and whether the clause is a scan, a predicate, a negation or an invocation.

### 2.3 The commit a datom belongs to

Two additions to the transaction layer:

- `Transaction.Entity()` returns the transaction entity's identity, and `Transaction.Add` accepts it. So a caller writes attributes on the commit in the commit itself, atomically with its datoms and under the same marks. The `:db/txInstant` datom the commit already writes is one of them.
- A database function `[(commit $ ?w) ?c]` binds a datom's ElementID to its transaction entity. A datom's commit is the mark with the lowest ElementID at or above the datom's own, on the same replica. The ATEV index puts the transaction ahead of the entity under an attribute (`datalog/storage/key_encoder_binary.go`), so one seek under `:db/txInstant` answers it.

With both, a datom's commit and the commit's attributes are one pattern:

```clojure
[:find ?v ?t ?who
 :where [?e :order/status ?v ?w]
        [(commit $ ?w) ?c]
        [?c :db/txInstant ?t]
        [?c :commit/author ?who]]
```

Import bypasses the transaction layer and preserves ElementIDs. So an imported commit entity keeps its attributes and its marks, and the function answers the same on a loaded dump.

### 2.4 Rendering

`Relation.Table()` renders a result. A witness result renders as EDN. Each result tuple lists its derivations. Each derivation is a vector of the datoms it matched, in `[e a v tx]` form, and for a rule step the rule name and the body tuples. A `Diff` result (§3.3) renders the same way, with `added` and `removed` sections. The renderer lives beside the table formatter in `datalog/executor/table_formatter.go`.

---

## 3. Speculative values

**The contracts the tests pin.** Each is the subject of one test:

- `With` over a store with cardinality-one, many and vector attributes: adds, removes, and an add superseding a base value, each read back through a query, a pull and `PullInto`.
- `With` against `Fork` on the memory-tree store: the same transaction applied both ways and every query in the matrix giving the same result.
- `With` under predicate pushdown and under an `AsOf` base, checked against the base's own results plus the overlay's.
- `Diff` on a conjunctive query, on an aggregate, and on a closure, checked by hand.
- A benchmark: `With` on the OHLC store with a thousand overlay datoms, against the base query.

### 3.1 With

`Database.With(build func(*Transaction) error) (*Database, error)` runs the function against a transaction it never commits. It returns a read-only database value. The value reads the base plus the transaction's adds and removes as one source. Queries, pulls and the temporal operators of the base run on the value. Its ElementIDs come from the base's clock, advanced in memory and not persisted. So the overlay's writes order after everything in the base, and resolution treats them as the newest.

Resolution across the two is index ordering plus filtering, as `CRDT_STREAMING_RESOLUTION.md` describes for one source. The overlay matcher merges the base scan and the overlay's datoms in the index's order, per `(E, A)` group, and hands the merged stream to `CRDTResolvingIterator`. A remove in the overlay is a tombstone the resolver already interprets. The matcher lands on the `PatternMatcher` seam, so `SourceRouter`, predicate pushdown and the EA cache compose with it unchanged. A `With` value has a cache of its own, as an `AsOf` handle does.

`With` composes: `d.With(a).With(b)` reads `b` over `a` over `d`. It works on every backend. On the memory-tree store the same value is also reachable as a fork with the transaction applied. The two agree on every query, which the differential test asserts.

### 3.2 Fork

`Fork` exists and needs no change for this proposal. A branch is the persistent form of `With`: a snapshot named with `Snapshot`, then `Fork` from it, then commits on the branch. Withdrawal on a branch is `Remove`. Fork on the Badger store stays deferred, as [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) has it.

### 3.3 Diff

`Diff(q, a, b *Database, inputs...) (added, removed Relation, err error)` evaluates `q` against `a` and against `b`, and materializes both results. It returns the tuples in `b` and not in `a` as `added`, and the tuples in `a` and not in `b` as `removed`. The difference is computed through `TupleKeyMap`, so it respects the value domain's equality. An aggregate result that changed appears as one tuple removed and one added.

With witnesses on, each tuple in `added` and `removed` carries its witnesses from the side it exists on. A witness that names a datom only the overlay holds marks the change as depending on that datom.

---

## 4. Schema as facts

This section describes what the engine does.

**The contracts the tests pin.** `datalog/storage/schema_facts_test.go` and `unknown_attribute_test.go`, the dump tests in `datalog/db/dump_source_test.go` and `cmd/datalog/open_database_test.go`, and `datalog/db/refuse_unknown_test.go`.

A declaration of an attribute is an entity named by its seven words. So the same declaration made anywhere converges on one entity. A declaration never changes: one that differs in a word is another entity. It carries one fact per field of the definition, under the well-known words `:db/ident`, `:db/valueType`, `:db/cardinality`, `:db/unique`, `:db/uniqueElements`, `:db/neverZeroValue` and `:db/doc`. The definition of an attribute is the entity `db.attribute/<ident>`, named by the ident. It carries one ref, `:db.attribute/declaration`, to its current declaration. A later declaration moves the ref, so the ref's history is the definition's history. A value type, uniqueness constraint or doc string that a later declaration drops stops resolving through the definition. Two replicas' declarations of one attribute resolve to one of them, whole.

A schema given at open is declared into the store in one transaction. The transaction holds each definition the store does not already hold word for word. So a reopen with an unchanged schema writes nothing, and the database runs with what its store states. The alternative is the given schema overlaid in memory over the stored facts. That is a second home for the running schema. Under it, two overlapping `SetSchema` calls left the store holding both changes while the database ran one declaration whole.

A store opened with no schema reads its definitions back and runs with them. It infers from the ops only the attributes that carry datoms and no definition.

A dump carries the declarations and the refs as datoms. `Import` and `ImportBinary` replay them with the rest and end by reading them, so `db.Open` on a dump resolves a set as its writer declared it, and a dump carrying a definition the schema does not admit is the import's error at that reading, in the reader's words. A database opened with a schema declares it again after the import. The given schema then supersedes the dump's definitions for the idents it names, since a later declaration moves the ref.

A fork opens the branch as any database does, on its store as of the snapshot, and declares its parent's declared definitions into it. `TruncateTo` leaves the database running with the declarations that survive the rewind, and writes nothing.

The facts are queryable with patterns:

```clojure
[:find ?ident ?type ?card
 :where [?a :db.attribute/declaration ?d]
        [?d :db/ident ?ident]
        [?d :db/valueType ?type]
        [?d :db/cardinality ?card]]
```

The engine has words of its own: `:db/txInstant`, `:db/id`, the snapshot and branch records, the definition's ref and the seven words above. They form a schema the engine installs over every running schema, in memory and never as facts, each word of cardinality one with its type (`engineSchema`, `datalog/storage/schema_facts.go`). So each word resolves by its declaration, whatever the store holds. The engine's own queries therefore run on a database that refuses unknown attributes and never minted a marker. A user's write of an engine word is refused (`validateAttributeStorable`). A given schema that declares one is refused at open and at `SetSchema` (`engineWordRedeclared`). `NESTED_SCOPES_AS_QUERIES.md` §8 states the same.

The refusal of §5 is opt-in, through the `RefuseUnknownAttributes` database option, and the option requires a given schema. The alternative is a refusal on every store with facts. `ValidateDatom` (`datalog/schema/validation.go`) accepts a write of an attribute the schema does not declare. So a mandatory refusal would refuse every query on such an attribute, with no way to decline it.

The refusal runs at execution entry, in `Executor.ExecuteWithRelations` and in the `Pull` family. It does not run in the parser, because `ParseQuery` has no database, and the parse cache maps text to a query that does not depend on the store.

The executor asks each source the query runs over, `$` and each named source its `:in` declares, whether it refuses. Only a source that refuses is asked about a roster. A source is asked about the keyword constants the query's patterns and the rule bodies' patterns write in the attribute position on it, an inner query's under the sources its call passes, and `$` about the pull patterns' and the database functions' attributes too. A keyword a variable carries into an attribute position, through an input, a ground, a form's output, a subquery's binding or a rule's head, is a value and is asked of no source. The matcher owns the schema, so it answers `AttributeVocabulary`. `SourceRouter` routes the question to the source named, which answers for itself. A database passed as a named source refuses as it does as `$`. A typed accessor names its attribute in Go and asks the database about it before its query runs.

The attributes a query names are collected from the prepared text when its plan is made, through `query.CollectRosters`. The planner option `CollectRosters` turns the collection on. An executor sets the option from its matcher: a router is asked about every source it holds, and any other matcher about itself as `$`. The roster is held with the plan (`RealizedPlan.Rosters`, `datalog/planner/types.go`), and the executor's entry reads it there after planning. A plan made for sources that refuse nothing carries no roster and costs no walk (`NESTED_SCOPES_AS_QUERIES.md` §5.5).

---

## 5. Errors and bounds

**The contracts the tests pin.** A query built to produce each rule and attribute error below produces it, and the test asserts the error's named parts. Those tests are in `datalog/query/rules_test.go`, `datalog/planner/magic_test.go` and `datalog/storage/rules_test.go`. A query cancelled before execution returns the context's error from `Query`, `QueryInto` and `Analyze`. A closure cancelled from its first iteration event stops at the next iteration. Those tests are in `datalog/executor/cancellation_test.go`, `datalog/storage/cancellation_test.go` and `datalog/db/cancellation_test.go`.

Every error a query author can repair names the thing to repair. The rule and attribute errors exist. The witness error is design:

- an unsafe rule: the rule and the symbol (`query.UnsafeRuleError`)
- an unstratifiable rule set: the predicates on the negative cycle (`query.UnstratifiableError`)
- a recursive invocation inside a fallback form or an aggregating inner query: the rule, the predicate and the form (`query.NestedRecursionError`)
- an unknown rule invocation: the invoked name and the rule set's predicate names (`query.UnknownRuleError`)
- an invocation at the wrong arity: the predicate, the arity expected and the arity found (`query.RuleArityError`)
- a required position left free: the predicate, the variable and the invoking rule (`query.RequiredBindingError`)
- an attribute the schema does not declare, on a database opened to refuse them: the attribute alone (`schema.UnknownAttributeError`). A nearest-ident suggestion is excluded. It would cost a string-distance function, a threshold and a result cap, two constants pinned by tests, for an error whose repair is the attribute's spelling.
- a witness fold refused: the clause or function that blocks it
- a bound exceeded: the predicates, the cap, and the count reached (`query.RuleIterationLimitError`, `query.DerivedTupleLimitError`)

`ClauseBlockers` (`datalog/query/clause_ready.go`) is the pattern: the error carries the symbols, not only a sentence.

The bounds are the iteration and tuple caps of §1.4 and query cancellation through `WithContext`. `WithContext` is a query option passed among the inputs of `Query`, `QueryInto`, `QueryOneInto`, `Explain`, `ExplainAlgebra` and `Analyze` (`storage.WithContext`, re-exported as `db.WithContext`). The alternative is a positional `context.Context` parameter on `Query` and its siblings. A cancellation is optional per call, since a query with none runs. A positional parameter would make every caller pass a default, the accreting-by-position shape. `Query`'s optional per-call settings already ride the `QueryOption` on its inputs, the carrier `WithSources` established. A query whose context is done is refused at entry, before any work. A fixpoint whose context is done stops at its next iteration. The context's error is the query's error. The context is read at entry and once per iteration, never per tuple. The fixpoint is the one loop that can run without bound, and the caps are its primary guard. `Diff` (§3) takes the same option.

---

## 6. Surface

The EDN query form plus `%` rules is the grammar. The rule forms are Datomic's, in the syntax [../reference/RULES.md](../reference/RULES.md) states.

A second grammar, Horn clauses with `:-`, is deferred. Deferring it costs no expressiveness, since every rule a Horn clause states, the EDN form states. Two things decide it when it is reopened. A Prolog-style predicate is n-ary and a janus scan is a pattern over one attribute, so an extensional predicate has to be declared as a pattern over attributes, and an intensional one is a derived relation. Typed relation declarations in the Soufflé style are subsumed by §4, since the schema facts already carry the types. If added, it is a parser onto the same `query.Rule` and `query.RuleSet` types. A second evaluator is excluded, as §1.3 states.

---

## 7. Measurement

Before each choice below is taken, the number is taken first, on the existing benchmarks where one exists:

- the OHLC query with the witness rewrite off and on, and with each fold
- closure over the three edge-set sizes `BenchmarkTransitiveClosure` measures (§1), semi-naive against naive
- `With` against the base query at one hundred, one thousand and ten thousand overlay datoms
- `With` against `Fork` on the memory-tree store at the same sizes
- `Diff` over results of one thousand and one hundred thousand tuples

Each runs under both optimizer modes. A benchmark that regresses the base query with the feature off is a defect in the feature's gating, since an option that is off costs nothing.

---

## 8. Order of work

§1 and §4 describe the engine as built, with the bounds and the cancellation of §5. The open items, in order:

1. `With` and `Diff` (§3).
2. Witnesses (§2): the rewrite, the fixpoint's recorded derivations, `WhyNot`, the commit function and transaction attributes, the renderer.
3. The deferred items (§9) as they are reopened.

---

## 9. Open

- `With` over a `History` handle: whether the overlay's datoms appear unresolved beside the base's history, or `With` refuses a history base.
- Recursive aggregation (shortest paths, counts over closures): deferred, with the monotone cases named when reopened.
- Fork on the Badger store: deferred, per [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md).
- A second grammar: deferred, per §6.
- A source on a database function and on a rule invocation: deferred. Datomic's grammar takes a source as a database function's first argument, `(get-else $src ?e :a "d")`, and a source prefix on a rule expression, `[$src (rule ...)]` or `($src rule ...)`. Today `SourceRouter` delegates `get-else`, `get-some` and `missing?` to `$` through `EntityLookupMatcher`, and an invocation holds its name and its arguments and no source (§1.1). When reopened, the language admits both forms under goal 12 of `NESTED_SCOPES_AS_QUERIES.md`: the function reads the source it names, the invocation's rules evaluate over that source, and the roster names an attribute on whichever source the function or the rule body reads (§4).
- The completion event of a query that runs: deferred. `QueryComplete` is emitted for a refusal alone, at the executor's entry and on the database's `Query` path, while the `KeySuccess` doc in `datalog/annotations/types.go` and the formatter's success arm in `datalog/annotations/output.go` describe a completion carrying the relation and tuple counts for a query that runs. A query's result is a streaming relation whose tuples are counted as it is drained, and the session-bound result releases its read session when it is exhausted or closed, after `Query` has returned, so that is the one place the count is known. When reopened, the completion of a running query fires at the session-bound result's exhaustion or close, with `success` true and the tuples drained, so a handler sees one invocation and one completion per query and `Analyze` attributes a running query's duration to an event. The alternative, the doc and the formatter restated to the refusal alone, deletes a stated contract its renderer implements.
- A blank at a binding position: deferred. Datomic's bind-tuple is `[(variable | '_')+]` and its bind-rel is `[[(variable | '_')+]]`, each admitting `_` at a position whose value the binding does not name, and its bind-coll, `[variable '...']`, admits no blank. Today `parseTupleBinding`, `parseRelationBinding` and `inputVariables` (`datalog/parser/parser.go`) refuse an element that is not a variable, `_` among them, so a blank is refused in an `:in` tuple or relation input, an expression's tuple or relation binding and a subquery's relation binding. When reopened, the tuple and relation forms admit the blank at every binding site under goal 12 of `NESTED_SCOPES_AS_QUERIES.md`, and the collection form refuses it as Datomic's does: the parser's three readers take `_` as a position carrying no symbol, `Query.String` renders it back as `_`, and the binding machinery stands, since `query.SymbolGroups` (`datalog/query/symbol_index.go`) already puts a position carrying no symbol in no group, so it binds nothing and joins no unification, and `BindQueryInputs` and the expression's `bindingAlignment` read their groups from it.
