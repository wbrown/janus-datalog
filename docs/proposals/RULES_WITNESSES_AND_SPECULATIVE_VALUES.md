# Proposal: Rules, Witnesses, and Speculative Values

**Builds on:**
- [RECURSIVE_RULES.md](RECURSIVE_RULES.md): Datomic-syntax rules passed as `%`, semi-naive evaluation, stratification
- [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md): `Fork`, named snapshots, the overlay as a `PatternMatcher` source (its §2 and §9.6)
- [../reference/MULTI_SOURCE.md](../reference/MULTI_SOURCE.md): named sources, `SourceRouter`, `MemoryPatternMatcher`
- [../reference/CRDT_STREAMING_RESOLUTION.md](../reference/CRDT_STREAMING_RESOLUTION.md): resolution as index ordering plus filtering
- [../reference/PLANNER_OPTIONS.md](../reference/PLANNER_OPTIONS.md): where per-query options live
- [../INPUT_PARAMETER_SEMANTICS.md](../INPUT_PARAMETER_SEMANTICS.md): `ClauseBlockers`, the error that names a missing symbol

---

## Summary

Six additions, each landing on a seam the engine already has:

1. **Rules and recursion.** Rules passed as `%`, evaluated to a fixpoint with semi-naive iteration, stratified negation, and caller-set bounds. The derived relation joins the rest of the query through the invocation clause.
2. **Witnesses.** For a result tuple, the base datoms and rules that derived it. For an expected tuple that is absent, the first clause that failed and the bindings reached. For a datom, the commit that asserted it, with the commit carrying caller attributes.
3. **Speculative values.** `With`, a read-only database value over the base plus an uncommitted overlay of adds and removes, on every backend. `Diff`, a query evaluated against two database values with the two differences of the results returned.
4. **Schema as facts.** Attribute definitions stored as system datoms, queryable with patterns and carried by dumps.
5. **Errors and bounds.** Rule errors that name the variable, the cycle, or the predicate at fault. A fixpoint iteration cap, a derived-tuple cap, and query cancellation.
6. **Surface.** The EDN query form plus `%` rules stays the one grammar. A second grammar is deferred, with the mapping question it would have to answer recorded.

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
- The optimizer contract is `query.Query` to algebra tree to `query.Query`. The algebra never constructs a plan, and the executor runs over `Relation` operators (`ARCHITECTURE.md`, "How the Planner Shapes Execution"). A transform pass lives in `datalog/algebra` beside `rewrite_decorrelate.go` and `rewrite_getelse.go`.
- `datalog/algebra/provenance.go` records optimizer rewrite decisions as `RewriteRecord` values. That name is taken by transform provenance. This proposal uses *witness* for data provenance throughout, and its pass file is `rewrite_witness.go`.
- A relation input, `:in $ [[?x ?y] ...]`, runs a query once per tuple of the relation.
- `SourceRouter` routes each pattern to exactly one `PatternMatcher` by the pattern's source symbol. There is no source that reads two matchers as one.
- `Database.Fork(name)` starts a branch from a named snapshot on the memory-tree store, writing as its own replica. `BadgerStore` does not implement the `forker` interface.
- `Transaction.Remove` makes a cardinality-one attribute absent in the current view (`docs/CORE_MODEL.md` §1).
- A commit writes one `:db/txInstant` datom on its transaction entity. `Transaction` carries no caller attributes, and only `SetTime` touches the transaction entity.
- A `ReadSession` pins one snapshot for every read a query makes.
- Attribute definitions are facts: a schema given to a database is declared into its store, each declaration's words on an entity named by them and on each `db.attribute/<ident>` entity a ref to its current declaration; a store opened with no schema reads them back, and a dump carries them (§4).
- `ClauseBlockers` in `datalog/query/clause_ready.go` names the symbols a clause still lacks, and the error that reports an unplannable clause names them.
- Every query-executing test runs under both optimizer modes, and the optimizer never changes results.

---

## 1. Rules and recursion

This section describes what the engine does.

### 1.1 Syntax and types

The syntax is [RECURSIVE_RULES.md](RECURSIVE_RULES.md)'s. A rule set is a vector of rules, each `[(name ?args...) body...]`. It is passed as the `%` input. A predicate is invoked as `(name args...)` in `:where`. An argument is a variable, a constant or `_`. Several rules for one head are alternatives. A head may begin with a vector of variables, the required-binding form `[(rule [?x] ?y) ...]`. The caller binds those positions.

Types in `datalog/query`:

- `Rule`: the head name, the head symbols, the required run, and the body clauses.
- `RuleSet`: the rules grouped by name, each predicate's arity and required positions, the strata, and the attribute roster the bodies name, collected once. `Required` reports a predicate's required positions. `Magic` rewrites the set for a binding pattern.
- `RuleInvocation`, a clause: the name and the arguments. Its scope is read from the rule set the query runs with: `ScopeOf(clause, rules)` makes the variables at the predicate's required positions mandatory correlates and provides the rest.
- `RulesInput`: the `%` input spec.
- The predicates the engine mints are symbols in namespaces of the engine's: `adorned.bf/reach` is `reach` adorned with a binding pattern, `magic.bf/reach` its magic predicate, `delta/reach` the tuples the last iteration of its stratum added (`DeltaSymbol`), and `scope/required` the required run a nested scope runs over (`ScopeRequired`). The kind leads the namespace and the predicate's own namespace follows it. `NewRuleSet` refuses a rule set that declares a predicate in one of these namespaces (`engineNamespace`).

`parser.ParseRules` parses a rule set. `parseInputSpec` accepts `%`. A list clause whose head names no reserved form is an invocation. An invocation, or a subquery call passing `%`, requires `%` in its query's own `:in`. The query builder has `qb.Rules`, `qb.Rule`, `qb.DefineRule`, `qb.RuleSet`, `qb.MustRuleSet` and `Require`. The CLI reads the `-in` value at the `%` position as rule text.

### 1.2 Safety and stratification

A rule is safe when every variable it must bind is bound. The variables it must bind are the head symbols and every symbol a predicate, an expression, a subquery input, an explicit-join header or a body invocation's required position consumes. A symbol is bound by a positive body clause or by the head's required run. An invocation binds its variable arguments at the positions its predicate does not require. `query.NewRuleSet` refuses an unsafe rule as `UnsafeRuleError` naming the rule and the symbol. It refuses an arity disagreement as `RuleArityError`. It refuses an invocation of an undefined predicate as `UnknownRuleError` naming the defined predicates. It refuses a required run that does not lead the head, or that differs between two rules of one predicate, naming the rule.

The stratifier builds the predicate dependency graph. An edge through `not` or `not-join` is negative. The strongly connected components are the strata. A component holding a negative edge is refused as `UnstratifiableError` naming its predicates. The rest are ordered. A same-component invocation stands at the top level of a body or inside an `or` or `or-join` branch, where evaluation is monotone. Under `or-default`, `or-default-join` or a subquery it is refused as `NestedRecursionError` naming the rule, the predicate and the form: fallback is non-monotone, and a subquery is where aggregation over the component would hide. `NESTED_SCOPES_AS_QUERIES.md` §2.5 and §5.5 narrow the refusal to a fallback form and an aggregating inner query, a subquery whose inner query does not aggregate being a join over its combinations. Aggregation inside a recursive component is refused. Deferring it costs the queries §9 lists, shortest paths and counts over closures, which a caller computes in Go over the closure; admitting it costs a monotonicity condition the evaluator checks per aggregate, since a non-monotone aggregate in a cycle has no fixpoint. A rule body aggregates over a completed stratum through a subquery, as a query does.

### 1.3 Evaluation

The rule set rides the execution context as the environment does. `convertInputsToRelations` consumes the `%` input. `Database.Query` puts the rule set on the `executor.Context`. The alternative is a relation slot for the rule set, which every `:in` walker would treat as a relation: `boundAttributes` (`datalog/executor/executor.go`) advances a relation index for every scalar, collection, tuple and relation input, and `convertInputsToRelations` would hand it the rule set as one.

`ExecuteWithRelations` prepares the query through `Prepare` (`datalog/query/prepare.go`), which checks every invocation against the set, and plans it under `query.Bindings` (`datalog/query/bindings.go`): the symbols the inputs bind and the rule set. The planner reads an invocation's scope from the rule set, so it orders the invocation after the clauses that bind its required positions.

The planner reads from the invoked predicate's name whether a seed restricts its relation (`SeedRestricted`): an invocation of a magic or adorned predicate, of the delta of one, or of `scope/required` is scheduled before the patterns, which then seek on what it binds. The alternatives are a mark on the rule, a second representation of a fact the name carries, and a planner input naming the held predicates, a fact the text does not render, which the rule of the next paragraph excludes.

A clause with no visible selectivity, no constant beyond a pattern's attribute or an invocation's name, no variable available when it is selected and no seed restriction, is scheduled only when no other clause is selectable, and an invocation with an available argument waits on no such scan, so a bound invocation is never bound further by a scan of a whole extent (`NESTED_SCOPES_AS_QUERIES.md` §5.2, `REWRITE_ORDER_PROOF.md` T3).

The planner's input is the query and those bindings, and every phase it returns declares `%` when a clause of its own uses `%`, invoking a predicate or passing `%` to a subquery, and parses back to itself. The alternative is a clause mark the text does not render, under which a phase does not parse back to itself: a body passing only `%` printed a phase the parser refused.

An invocation evaluates its predicate when its clause runs, and the derived relation it reads is materialized (`derivedSet.relation`, `datalog/executor/rules.go`). The alternatives are a subquery pattern, which has no inner query to run, since a fixpoint is not a query, and evaluation before planning, under which the predicate is derived whole and the invocation filters it, the cost the whole closure's measurement below prices.

Its bound positions are those holding a constant, or a variable the environment or a relation group binds. With none bound, the needed strata evaluate to a fixpoint and the invocation reads the predicate whole (`deriveInvocation`, `datalog/executor/rules.go`). `NESTED_SCOPES_AS_QUERIES.md` §5.5 adds one exception: a free invocation whose needed strata hold a predicate with required positions is rewritten, its head all free.

With some bound, the rule set is rewritten for the binding pattern. Four alternatives are excluded. The predicate derived whole and filtered to the bindings costs the whole closure whatever the invocation binds: at a hundred thousand edges the closure costs 0.3 to 1.1 s on the backends and the bound invocation 0.09 to 0.17 ms (`PERFORMANCE_STATUS.md`, "Rules and Recursion, Bounds and Cancellation"). A fixpoint per bound value is the evaluator run once per value, the N+1 shape. A second evaluator is a parallel implementation "Stop Creating V2 Versions" (the janus `CLAUDE.md`) excludes. An index on the bound positions narrows the filter and leaves the closure derived whole.

Each predicate the bindings reach gets an adorned copy (`adorned.bf/reach`) guarded by a magic predicate (`magic.bf/reach`). The bound values, projected from the relation groups, seed the magic predicate. The rewritten set evaluates as any set does. The invocation reads the adorned predicate.

The bindings pass through each body in the planner's order of the body as a scope, into the invocations after them, and a subquery in a body is read as a scope of the rewrite, as a branch is: `magicRewriter.rewriteClauses` (`datalog/planner/magic.go`) walks the planned clauses (`NESTED_SCOPES_AS_QUERIES.md` §5.5). An invocation under a non-monotone form, a negation, a fallback form or an aggregating inner query, receives the bindings of the clauses before the first invocation of the rule's own stratum, the sealed passing, so no magic rule depends through such a form on that stratum's adorned predicate (`NON_MONOTONE_FORMS_PROOF.md` T6).

A site under a non-monotone form whose adornment the rewritten set cannot carry, because adorning it would place its adorned predicate in the rule's own stratum or because the sealed passing leaves a required position free, is left unadorned with its predicate's original rules in the set, and the set is rebuilt without it, so every rewrite is accepted and none is declined (`NON_MONOTONE_FORMS_PROOF.md` T6, L4). A predicate with required positions at such a site is derived at its clause under the positions the branch binds there (its T5). Each rewrite made is recorded as a `rules/magic-sets` event carrying the predicate and its adornment (`magicSetsEvent`, `datalog/executor/rules.go`).

A recursive stratum iterates semi-naively. Iteration 0 absorbs the seed, when the stratum has one, and runs the rules with no same-stratum invocation. Each later iteration runs, for each rule and each same-stratum occurrence in its body, a copy of the body in which that occurrence invokes the predicate's delta symbol, `delta/reach`. The scope holds the previous iteration's delta under that symbol, so the occurrence reads it. Every other occurrence reads the full relation. The tuples not yet derived become the next delta. The stratum ends when every delta is empty. A body is an ordinary `query.Query`, planned under the rule set. Its plan enters the plan cache under its own text, which carries `%` and the delta symbols, so the cached plan is the one the fixpoint would make. The alternative is keeping body plans out of the cache, an exception to the cache's one rule, every plan served under its text and the symbols bound when it was made (`NESTED_SCOPES_AS_QUERIES.md` §5.2), which a body is, planned under none. `stratumEvaluation.run` (`datalog/executor/rules.go`) runs each body's plan through `executeRealizedPlan`, the phase executor, so base patterns stream from storage and an invocation's relation joins at collapse. The whole fixpoint runs inside the query's one `ReadSession`, so every iteration reads one snapshot.

The invoking query receives the derived relation through the invocation clause under the call's substitution: renamed to the variables, selected on the constants, the repeated variables and the environment's bindings, with the blanks' positions projected away. Phasing and selectivity scoring see an invocation as a data source. It provides its variables. It requires the positions its predicate declares as required.

### 1.4 Bounds

Two fields on `PlannerOptions`: `MaxRuleIterations` per stratum, and `MaxDerivedTuples` per evaluation of an invocation, the magic predicates counted. Zero is unbounded. Either exceeded is `RuleIterationLimitError` or `DerivedTupleLimitError` naming the predicates, the limit and the count reached.

### 1.5 Tests

`datalog/query/rules_test.go` (the set and its refusals) and `datalog/query/magic_test.go` (the rewrite), `datalog/parser/rules_test.go`, `datalog/executor/rules_test.go` (the fixpoint, the invocation's rename and selection, the naive step against the semi-naive one, the bound invocation), `datalog/storage/rules_test.go` (the closure on a chain and on a cycle, stratified negation, invocations in a subquery, in `or`, `or-join`, `or-default` and `or-default-join` branches and under `not-join`, bound, constant and blank arguments, the required head, the caps, one snapshot, on the backend and optimizer matrix), `datalog/qb/rules_test.go` and `cmd/datalog/rules_test.go`.

`BenchmarkTransitiveClosure` in `datalog/executor` and in `datalog/storage` measures the closure at a thousand, ten thousand and a hundred thousand edges, semi-naive against naive. The naive baseline is the semi-naive step driven with the full relation at every occurrence, with no production flag and no second evaluator.

A production flag is excluded twice over. As a package-level variable it is excluded by "No Global Configuration State" (the janus `CLAUDE.md`). As an option on `PlannerOptions` it is excluded by the fact that nothing in production would set it, its one reader being the benchmark's naive leg.

A second evaluator is excluded as §1.3 states.

`BenchmarkBoundInvocation` beside each measures the closure with its start bound to one node. The numbers are in `PERFORMANCE_STATUS.md`.

---

## 2. Witnesses

A witness of a result tuple is one derivation of it: for each scan in the derivation, the datom it matched, and for each rule step, the rule and the body tuples. The engine computes witnesses per query. It stores none: a result's derivations change with the datoms, so a stored witness is left stale by the next commit, while the query over its snapshot recomputes them.

### 2.1 Why a tuple holds: the witness rewrite

For a query without rules, a transform pass in `datalog/algebra/rewrite_witness.go` runs after the existing passes:

- Every `Scan` whose pattern has three positions gets a fourth, a fresh symbol bound to the matched datom's ElementID.
- Every `_` in a scan becomes a fresh symbol, so the scan's bound elements together with the ElementID name the matched datom.
- The `:find` is extended with the fresh symbols, in scan order.

The unmodified executor then returns one tuple per derivation. Tuples that agree on the original `:find` symbols are the derivations of one result. A `Relation` is a set, so two derivations with the same witnesses in the same positions are one tuple, and the polynomial the result encodes has boolean coefficients.

The fold at the result boundary chooses what to report, per query, through a planner option. Two alternatives are excluded: a package-level fold setting, which "No Global Configuration State" (the janus `CLAUDE.md`) forbids, and a database option, the shape `RefuseUnknownAttributes` has in §4, which the witness result excludes by extending the query's own `:find` with the witness symbols, a fact of one query and not of the database. The folds:

- *lineage*: the union of the witness datoms over the derivations of a result
- *why*: the set of witness sets, one per derivation
- *how*: the witness sets with their scan positions, which is the polynomial up to multiplicity

A fold ranges over the derivations the engine enumerates: one tuple per derivation through the scans, and for a derived tuple the one derivation the fixpoint records (Rules, below).

A scan the optimizer introduced is a scan the pass sees. The `get-else` rewrite in `rewrite_getelse.go` turns a database function into a scan with a left outer join, so a `get-else` value carries the datom it read, or no witness when the default fired. `get-some` and `missing?` are read through the same rewrite once each has a scan form. Until then the pass refuses a query that uses them with witnesses on, naming the function.

**Negation.** A `not` body is not rewritten. A derivation through an anti-join carries no witness for the absence it depends on. The absence's explanation is §2.2's, run on the negated body with the outer bindings.

**Aggregation.** Witness symbols change group membership, so the aggregate must not see them. The pass evaluates the aggregate over the unrewritten body and, in the same plan, the rewritten body projected to the group keys and the witness symbols, and pairs the two on the group keys at the boundary. An aggregate result's witnesses are the union of its group's witnesses.

**Rules.** Witness tuples do not terminate on a cyclic rule: every longer walk around the cycle is a new tuple. The fixpoint instead records, per derived tuple, one derivation: the rule and the body tuples. No goal needs the alternative, a minimal-witness label, the set of every minimal derivation of the tuple: goal 2 is met by one derivation, and `Diff` (§3.3) marks a changed tuple's dependence on the overlay by one, since every derivation of a tuple in `added` uses an overlay datom. The label costs a fixpoint over labels, since a tuple's label can gain a minimal witness after the tuple first appears, so a stratum would iterate until its labels stabilize. A closure tuple's why-label holds one witness per simple path, exponential in the graph, so goal 5 would need a cap on it. The derivation recorded is the least of minimum depth. The body's scans carry their ElementIDs, so each derivation of a head tuple is a distinct body tuple. Semi-naive iteration finds a tuple first at its minimum depth, and that iteration produces every derivation of that depth, since each uses a body tuple of the depth before at some occurrence, which the copy reading the delta at that occurrence produces. The fixpoint keeps the least of them under the order that compares the rule's position in the set and then the body tuple's values position by position, witness symbols included. The alternative, the first derivation produced, is the plan's choice among those of minimum depth, under which the optimizer changes the witness, against §0. A derived tuple's witness is the chain of recorded derivations down to base datoms.

### 2.2 Why a tuple does not hold

`WhyNot(q, expected Tuple, inputs...)` binds the query's `:find` symbols to the expected values, runs the `:where` clauses in planner order against those bindings, and returns the first clause that produced no match together with the bindings reached before it. For a rule invocation it descends into the rule's clauses the same way, one alternative at a time, and reports each alternative's failing clause. `Explain` is the plan explain, which §5 lists among the callers of `WithContext`.

The executor already stops a phase on an empty relation group and emits that as an annotation event. The addition is a return value: the clause, its position in the planned order, the bindings available, and whether the clause is a scan, a predicate, a negation or an invocation.

### 2.3 The commit a datom belongs to

Two additions to the transaction layer:

- `Transaction.Entity()` returns the transaction entity's identity, and `Transaction.Add` accepts it, so a caller's attributes on the commit are written in the commit, atomically with its datoms and under the same marks. The `:db/txInstant` datom the commit already writes is one of them.
- A database function `[(commit $ ?w) ?c]` binds a datom's ElementID to its transaction entity. A datom's commit is the mark with the lowest ElementID at or above the datom's own on the same replica, and the ATEV index, whose layout puts the transaction ahead of the entity under an attribute (`datalog/storage/key_encoder_binary.go`), answers that with one seek under `:db/txInstant`.

With both, a datom's commit and the commit's attributes are one pattern:

```clojure
[:find ?v ?t ?who
 :where [?e :order/status ?v ?w]
        [(commit $ ?w) ?c]
        [?c :db/txInstant ?t]
        [?c :commit/author ?who]]
```

Import bypasses the transaction layer and preserves ElementIDs, so an imported commit entity keeps its attributes and its marks, and the function answers the same on a loaded dump.

### 2.4 Rendering

`Relation.Table()` renders a result. A witness result renders as EDN: for each result tuple, its derivations, each derivation a vector of the datoms it matched in `[e a v tx]` form, and for a rule step the rule name and the body tuples. A `Diff` result (§3.3) renders the same way with `added` and `removed` sections. The renderer lives beside the table formatter in `datalog/executor/table_formatter.go`.

### 2.5 Tests

- A three-scan query with a self-join, witnesses checked against the polynomial computed by hand, in both optimizer modes, with the rewrite's decision recorded through `RewriteSink` and asserted.
- A query with `get-else` whose default fires for some entities and not others.
- An aggregate query whose group witnesses are checked against the group members' datoms.
- A closure over a cycle whose recorded witness is the least derivation of minimum depth, computed by hand, and the same under both optimizer modes.
- `WhyNot` on a query with a scan that matches nothing, a predicate that rejects everything, and a `not` that excludes the expected tuple, each naming the right clause.
- `Transaction.Entity()` attributes read back through `(commit ...)` on the live database and on a dump of it.
- A benchmark: the OHLC query with the witness rewrite on and off.

---

## 3. Speculative values

### 3.1 With

`Database.With(build func(*Transaction) error) (*Database, error)` runs the function against a transaction it never commits and returns a read-only database value that reads the base plus the transaction's adds and removes as one source. Queries, pulls and the temporal operators of the base run on the value. Its ElementIDs come from the base's clock, advanced in memory and not persisted, so the overlay's writes order after everything in the base and resolution treats them as the newest.

Resolution across the two is index ordering plus filtering, as `CRDT_STREAMING_RESOLUTION.md` describes for one source: the overlay matcher merges the base scan and the overlay's datoms in the index's order per `(E, A)` group and hands the merged stream to `CRDTResolvingIterator`. A remove in the overlay is a tombstone the resolver already interprets. The matcher lands on the `PatternMatcher` seam, so `SourceRouter`, predicate pushdown and the EA cache compose with it unchanged. The cache for a `With` value is its own, as an `AsOf` handle's is.

`With` composes: `d.With(a).With(b)` reads `b` over `a` over `d`. It works on every backend. On the memory-tree store the same value is also reachable as a fork with the transaction applied, and the two agree on every query, which the differential test asserts.

### 3.2 Fork

`Fork` exists and needs no change for this proposal. A branch is the persistent form of `With`: a snapshot named with `Snapshot`, then `Fork` from it, then commits on the branch. Withdrawal on a branch is `Remove`. Fork on the Badger store stays deferred as [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) has it.

### 3.3 Diff

`Diff(q, a, b *Database, inputs...) (added, removed Relation, err error)` evaluates `q` against `a` and `b`, materializes both results, and returns the tuples in `b` and not `a` as `added` and the tuples in `a` and not `b` as `removed`. The difference is computed through `TupleKeyMap`, so it respects the value domain's equality. An aggregate result that changed appears as one tuple removed and one added.

With witnesses on, each tuple in `added` and `removed` carries its witnesses from the side it exists on. A witness that names a datom only the overlay holds marks the change as depending on that datom.

### 3.4 Tests

- `With` over a store with cardinality-one, many and vector attributes: adds, removes, and an add superseding a base value, each read back through a query, a pull and `PullInto`.
- `With` against `Fork` on the memory-tree store: the same transaction applied both ways and every query in the matrix giving the same result.
- `With` under predicate pushdown and under an `AsOf` base, checked against the base's own results plus the overlay's.
- `Diff` on a conjunctive query, on an aggregate, and on a closure, checked by hand.
- A benchmark: `With` on the OHLC store with a thousand overlay datoms, against the base query.

---

## 4. Schema as facts

This section describes what the engine does. A declaration of an attribute is an entity named by its seven words, so the same declaration made anywhere converges on one entity, and a declaration never changes: one that differs in a word is another entity. It carries one fact per field of the definition under the well-known words `:db/ident`, `:db/valueType`, `:db/cardinality`, `:db/unique`, `:db/uniqueElements`, `:db/neverZeroValue` and `:db/doc`. The definition of an attribute is the entity `db.attribute/<ident>`, named by the ident, and it carries one ref, `:db.attribute/declaration`, to its current declaration. A later declaration moves the ref, so the ref's history is the definition's history, a value type, uniqueness constraint or doc string a later declaration drops stops resolving through the definition, and two replicas' declarations of one attribute resolve to one of them whole.

A schema given at open is declared into the store in one transaction holding each definition the store does not hold word for word, so a reopen with an unchanged schema writes nothing, and the database runs with what its store states. The alternative is the given schema overlaid in memory over the stored facts, a second home for the running schema, under which two overlapping `SetSchema` calls left the store holding both changes while the database ran one declaration whole.

A store opened with no schema reads its definitions back and runs with them, inferring from the ops only the attributes that carry datoms and no definition.

A dump carries the declarations and the refs as datoms, and `Import` and `ImportBinary` end by reading them, so `db.Open` on a dump resolves a set as its writer declared it. A database opened with a schema declares it again after the import, and the given schema supersedes the dump's definitions for the idents it names through that declaration, since a later declaration moves the ref.

A fork opens the branch as any database, on its store as of the snapshot, and declares its parent's declared definitions into it. `TruncateTo` leaves the database running with the declarations that survive the rewind, writing nothing.

The facts are queryable with patterns:

```clojure
[:find ?ident ?type ?card
 :where [?a :db.attribute/declaration ?d]
        [?d :db/ident ?ident]
        [?d :db/valueType ?type]
        [?d :db/cardinality ?card]]
```

The engine's own words, `:db/txInstant`, `:db/id`, the snapshot and branch records, the definition's ref and the seven words above, are a schema the engine installs over every running schema, in memory and never as facts, each word of cardinality one with its type (`engineSchema`, `datalog/storage/schema_facts.go`), so each resolves by its declaration whatever the store holds and the engine's own queries run on a database that refuses unknown attributes and never minted a marker; a user's write of one is refused (`validateAttributeStorable`), and a given schema declaring one is refused at open and at `SetSchema` (`engineWordRedeclared`), as `NESTED_SCOPES_AS_QUERIES.md` §8 states.

The refusal (§5) is opt-in through the `RefuseUnknownAttributes` database option, which requires a given schema. The alternative is a refusal on every store with facts. `ValidateDatom` (`datalog/schema/validation.go`) accepts a write of an attribute the schema does not declare, so a mandatory refusal would refuse every query on such an attribute, with no way to decline it.

The refusal runs at execution entry, in `Executor.ExecuteWithRelations` and in the `Pull` family, not in the parser: `ParseQuery` has no database, and the parse cache maps text to a query that does not depend on the store.

The executor asks each source the query names whether it refuses, and only a source that does is asked about a roster: the default source about the query's own attributes, the rule bodies' and the ones the inputs bind to a pattern's attribute position, and a named source about the patterns on it. The matcher owns the schema, so it answers `AttributeVocabulary`, and `SourceRouter` routes the question to the source named, which answers for itself; a database passed as a named source refuses as it does as `$`.

The attributes a query names are collected from the prepared text when its plan is made, through `query.CollectRosters` under the planner option `CollectRosters`, which a database sets for the executors it builds from whether a source the query runs over refuses, and held with the plan (`RealizedPlan.Rosters`, `datalog/planner/types.go`); the executor's entry reads them there after planning, and a plan made for sources that refuse nothing carries none and costs no walk (`NESTED_SCOPES_AS_QUERIES.md` §5.5).

Tests: `datalog/storage/schema_facts_test.go` and `unknown_attribute_test.go`, the dump tests in `datalog/db/dump_source_test.go` and `cmd/datalog/open_database_test.go`, and `datalog/db/refuse_unknown_test.go`.

---

## 5. Errors and bounds

Every error a query author can repair names the thing to repair. The rule and attribute errors exist. The witness error is design:

- an unsafe rule: the rule and the symbol (`query.UnsafeRuleError`)
- an unstratifiable rule set: the predicates on the negative cycle (`query.UnstratifiableError`)
- a recursive invocation inside a fallback or a subquery: the rule, the predicate and the form (`query.NestedRecursionError`)
- an unknown rule invocation: the invoked name and the rule set's predicate names (`query.UnknownRuleError`)
- an invocation at the wrong arity: the predicate, the arity expected and the arity found (`query.RuleArityError`)
- a required position left free: the predicate, the variable and the invoking rule (`query.RequiredBindingError`)
- an attribute the schema does not declare, on a database opened to refuse them: the attribute alone (`schema.UnknownAttributeError`). A nearest-ident suggestion would cost a string-distance function with a threshold and a result cap, two constants pinned by tests, for an error whose repair is the attribute's spelling
- a witness fold refused: the clause or function that blocks it
- a bound exceeded: the predicates, the cap, and the count reached (`query.RuleIterationLimitError`, `query.DerivedTupleLimitError`)

`ClauseBlockers` is the pattern: the error carries the symbols, not only a sentence.

Bounds: the iteration and tuple caps of §1.4, and query cancellation through `WithContext`. `WithContext` is a query option passed among the inputs of `Query`, `QueryInto`, `QueryOneInto`, `Explain`, `ExplainAlgebra` and `Analyze` (`storage.WithContext`, re-exported as `db.WithContext`). The alternative is a positional `context.Context` parameter on `Query` and its siblings. A cancellation is optional per call, since a query with none runs, so a positional parameter makes every caller pass a default, the accreting-by-position shape, where `Query`'s optional per-call settings already ride the `QueryOption` on its inputs, the carrier `WithSources` established. A query whose context is done is refused at entry, before any work. A fixpoint whose context is done stops at its next iteration. The context's error is the query's error. The context is read at entry and once per iteration, never per tuple: the fixpoint is the one loop that can run without bound, and the caps are its primary guard. `Diff` (§3) takes the same option.

Tests: each rule and attribute error above is produced by a query built to produce it, with its named parts asserted, in `datalog/query/rules_test.go`, `datalog/query/magic_test.go` and `datalog/storage/rules_test.go`. A query cancelled before execution returns the context's error from `Query`, `QueryInto` and `Analyze`. A closure cancelled from its first iteration event stops at the next. Those are in `datalog/executor/cancellation_test.go`, `datalog/storage/cancellation_test.go` and `datalog/db/cancellation_test.go`.

---

## 6. Surface

The EDN query form plus `%` rules is the grammar, and the rule forms are Datomic's, the syntax [RECURSIVE_RULES.md](RECURSIVE_RULES.md) states.

A second grammar, Horn clauses with `:-`, is deferred. Deferring it costs no expressiveness: every rule a Horn clause states is one the EDN form states. Two things decide it when it is reopened. A Prolog-style predicate is n-ary and a janus scan is a pattern over one attribute, so an extensional predicate has to be declared as a pattern over attributes and an intensional one is a derived relation. And typed relation declarations in the Soufflé style are subsumed by §4, since the schema facts already carry the types. If added, it is a parser onto the same `query.Rule` and `query.RuleSet` types, a second evaluator being excluded as §1.3 states.

---

## 7. Measurement

Before each choice below is taken, the number is taken first, on the existing benchmarks where one exists:

- the OHLC query with the witness rewrite off and on, and with each fold
- closure over the three edge-set sizes of §1.5, semi-naive against naive
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
