# Proposal: Rules, Witnesses, and Speculative Values

**Status:** Proposal
**Date:** 2026-10-11
**Builds on:**
- [RECURSIVE_RULES.md](RECURSIVE_RULES.md): Datomic-syntax rules passed as `%`, semi-naive evaluation, stratification
- [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md): `Fork`, named snapshots, the overlay as a `PatternMatcher` source (§2, §9.6)
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
- Attribute definitions are facts: a schema given to a database is asserted into its store on `db.attribute/<ident>` entities, a store opened with no schema reads them back, and a dump carries them (§4).
- `ClauseBlockers` in `datalog/query/clause_ready.go` names the symbols a clause still lacks, and the error that reports an unplannable clause names them.
- Every query-executing test runs under both optimizer modes, and the optimizer never changes results (`TODO.md`, the matrix ruling).

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
- The predicates the engine mints are symbols in namespaces of the engine's: `adorned.bf/reach` is `reach` adorned with a binding pattern, `magic.bf/reach` its magic predicate, and `delta/reach` the tuples the last iteration of its stratum added (`DeltaSymbol`). The kind leads the namespace and the predicate's own namespace follows it. `NewRuleSet` refuses a rule set that declares a predicate in one of these namespaces.

`parser.ParseRules` parses a rule set. `parseInputSpec` accepts `%`. A list clause whose head names no reserved form is an invocation. An invocation requires `%` in its query's own `:in`. The query builder has `qb.Rules`, `qb.Rule`, `qb.DefineRule`, `qb.RuleSet`, `qb.MustRuleSet` and `Require`. The CLI reads the `-in` value at the `%` position as rule text.

### 1.2 Safety and stratification

A rule is safe when every variable it must bind is bound. The variables it must bind are the head symbols and every symbol a predicate, an expression, a subquery input, an explicit-join header or a body invocation's required position consumes. A symbol is bound by a positive body clause or by the head's required run. An invocation binds its variable arguments at the positions its predicate does not require. `query.NewRuleSet` refuses an unsafe rule as `UnsafeRuleError` naming the rule and the symbol. It refuses an arity disagreement as `RuleArityError`. It refuses an invocation of an undefined predicate as `UnknownRuleError` naming the defined predicates. It refuses a required run that does not lead the head, or that differs between two rules of one predicate, naming the rule.

The stratifier builds the predicate dependency graph. An edge through `not` or `not-join` is negative. The strongly connected components are the strata. A component holding a negative edge is refused as `UnstratifiableError` naming its predicates. The rest are ordered. A same-component invocation stands at the top level of a body or inside an `or` or `or-join` branch, where evaluation is monotone. Under `or-default`, `or-default-join` or a subquery it is refused as `NestedRecursionError` naming the rule, the predicate and the form: fallback is non-monotone, and a subquery is where aggregation over the component would hide. Aggregation inside a recursive component is refused in this round. A rule body aggregates over a completed stratum through a subquery, as a query does.

### 1.3 Evaluation

The rule set rides the execution context as the environment does. `convertInputsToRelations` consumes the `%` input. `Database.Query` puts the rule set on the `executor.Context`. `ExecuteWithRelations` checks every invocation against the set and plans the query under `planner.Bindings`: the symbols the inputs bind and the rule set. The planner reads an invocation's scope from the rule set, so it orders the invocation after the clauses that bind its required positions. It reads from the invoked predicate's name whether a seed restricts its relation: an invocation of a magic or adorned predicate, or of the delta of one, is scheduled before the patterns, which then seek on what it binds. The planner's input is the query and those bindings, and every phase it returns declares `%` when it invokes a predicate and parses back to itself.

An invocation evaluates its predicate when its clause runs. Its bound positions are those holding a constant, or a variable the environment or a relation group binds. With none bound, the needed strata evaluate to a fixpoint and the invocation reads the predicate whole. With some bound, the rule set is rewritten for the binding pattern. Each predicate the bindings reach gets an adorned copy (`adorned.bf/reach`) guarded by a magic predicate (`magic.bf/reach`). The bindings pass left to right through each body into the invocations after them. A subquery body is not entered. The bound values, projected from the relation groups, seed the magic predicate. The rewritten set evaluates as any set does. The invocation reads the adorned predicate. A rewrite whose result does not stratify is declined. The predicate is then derived whole, and the decline is recorded as a `rules/magic-sets` event.

A recursive stratum iterates semi-naively. Iteration 0 absorbs the seed, when the stratum has one, and runs the rules with no same-stratum invocation. Each later iteration runs, for each rule and each same-stratum occurrence in its body, a copy of the body in which that occurrence invokes the predicate's delta symbol, `delta/reach`. The scope holds the previous iteration's delta under that symbol, so the occurrence reads it. Every other occurrence reads the full relation. The tuples not yet derived become the next delta. The stratum ends when every delta is empty. A body is an ordinary `query.Query`, planned under the rule set. Its plan enters the plan cache under its own text, which carries `%` and the delta symbols, so the cached plan is the one the fixpoint would make. The executor runs it through its phase executor, so base patterns stream from storage and an invocation's relation joins at collapse. The whole fixpoint runs inside the query's one `ReadSession`, so every iteration reads one snapshot.

The invoking query receives the derived relation through the invocation clause, projected onto its arguments: renamed to the variables, filtered by the constants, the repeated variables and the environment's bindings, with blanks dropped. Phasing and selectivity scoring see an invocation as a data source. It provides its variables. It requires the positions its predicate declares as required.

### 1.4 Bounds

Two fields on `PlannerOptions`: `MaxRuleIterations` per stratum, and `MaxDerivedTuples` per evaluation of an invocation, the magic predicates counted. Zero is unbounded. Either exceeded is `RuleIterationLimitError` or `DerivedTupleLimitError` naming the predicates, the limit and the count reached.

### 1.5 Tests

`datalog/query/rules_test.go` (the set and its refusals) and `datalog/query/magic_test.go` (the rewrite), `datalog/parser/rules_test.go`, `datalog/executor/rules_test.go` (the fixpoint, the invocation's projection, the naive step against the semi-naive one, the bound invocation), `datalog/storage/rules_test.go` (the closure on a chain and on a cycle, stratified negation, invocations in a subquery, in `or`, `or-join`, `or-default` and `or-default-join` branches and under `not-join`, bound, constant and blank arguments, the required head, the caps, one snapshot, on the backend and optimizer matrix), `datalog/qb/rules_test.go` and `cmd/datalog/rules_test.go`. `BenchmarkTransitiveClosure` in `datalog/executor` (semi-naive against naive) and in `datalog/storage` measures the closure at a thousand, ten thousand and a hundred thousand edges. `BenchmarkBoundInvocation` beside each measures the closure with its start bound to one node. The numbers are in `PERFORMANCE_STATUS.md`.

---

## 2. Witnesses

A witness of a result tuple is one derivation of it: for each scan in the derivation, the datom it matched, and for each rule step, the rule and the body tuples. The engine computes witnesses per query. It stores none.

### 2.1 Why a tuple holds: the witness rewrite

For a query without rules, a transform pass in `datalog/algebra/rewrite_witness.go` runs after the existing passes:

- Every `Scan` whose pattern has three positions gets a fourth, a fresh symbol bound to the matched datom's ElementID.
- Every `_` in a scan becomes a fresh symbol, so the scan's bound elements together with the ElementID name the matched datom.
- The `:find` is extended with the fresh symbols, in scan order.

The unmodified executor then returns one tuple per derivation. Tuples that agree on the original `:find` symbols are the derivations of one result. A `Relation` is a set, so two derivations with the same witnesses in the same positions are one tuple, and the polynomial the result encodes has boolean coefficients.

The fold at the result boundary chooses what to report, per query, through a planner option:

- *lineage*: the union of the witness datoms over all derivations of a result
- *why*: the set of witness sets, one per derivation
- *how*: the witness sets with their scan positions, which is the polynomial up to multiplicity

A scan the optimizer introduced is a scan the pass sees. The `get-else` rewrite in `rewrite_getelse.go` turns a database function into a scan with a left outer join, so a `get-else` value carries the datom it read, or no witness when the default fired. `get-some` and `missing?` are read through the same rewrite once each has a scan form. Until then the pass refuses a query that uses them with witnesses on, naming the function.

**Negation.** A `not` body is not rewritten. A derivation through an anti-join carries no witness for the absence it depends on. The absence's explanation is §2.2's, run on the negated body with the outer bindings.

**Aggregation.** Witness symbols change group membership, so the aggregate must not see them. The pass evaluates the aggregate over the unrewritten body and, in the same plan, the rewritten body projected to the group keys and the witness symbols, and pairs the two on the group keys at the boundary. An aggregate result's witnesses are the union of its group's witnesses.

**Rules.** Witness rows do not terminate on a cyclic rule: every longer walk around the cycle is a new row. The fixpoint instead records, per derived tuple, the rule and the body tuples of its first derivation. Semi-naive iteration finds each tuple first at a minimum iteration depth, so the first derivation is a shortest one. A derived tuple's witness is the chain of first derivations down to base datoms, and the base datoms carry their ElementIDs from the body's scans. Minimal-witness labels, the set of every minimal derivation, are deferred. The option that selects them is reserved.

### 2.2 Why a tuple does not hold

`Explain(q, expected Tuple, inputs...)` binds the query's `:find` symbols to the expected values, runs the `:where` clauses in planner order against those bindings, and returns the first clause that produced no match together with the bindings reached before it. For a rule invocation it descends into the rule's clauses the same way, one alternative at a time, and reports each alternative's failing clause.

The executor already stops a phase on an empty relation group and emits that as an annotation event. The addition is a return value: the clause, its position in the planned order, the bindings available, and whether the clause is a scan, a predicate, a negation or an invocation.

### 2.3 The commit a datom belongs to

Two additions to the transaction layer:

- `Transaction.Entity()` returns the transaction entity's identity, and `Transaction.Add` accepts it, so a caller's attributes on the commit are written in the commit, atomically with its datoms and under the same marks. The `:db/txInstant` datom the commit already writes is one of them.
- A database function `[(commit $ ?w) ?c]` binds a datom's ElementID to its transaction entity. A datom's commit is the mark with the lowest ElementID at or above the datom's own on the same replica, and the TAEV index answers that with one seek.

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
- A closure over a cycle whose first-derivation witness is checked to be of minimum depth.
- `Explain` on a query with a scan that matches nothing, a predicate that rejects everything, and a `not` that excludes the expected tuple, each naming the right clause.
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

This section describes what the engine does. Each attribute definition is an entity, `db.attribute/<ident>`, content-addressed from the ident so a definition asserted anywhere converges on one entity. It carries one fact per field of the definition under the well-known words `:db/ident`, `:db/valueType`, `:db/cardinality`, `:db/unique`, `:db/uniqueElements`, `:db/neverZeroValue` and `:db/doc`; a value type, uniqueness constraint or doc string a later declaration drops is withdrawn and stops resolving.

A schema given at open is asserted into the store in one transaction holding only the words that differ from what the store holds, so a reopen with an unchanged schema writes nothing. A store opened with no schema reads its definitions back and runs with them, inferring from the ops only the attributes that carry datoms and no definition. A dump carries the definitions as datoms, and `Import` and `ImportBinary` end by reading them, so `db.Open` on a dump resolves a set as its writer declared it. A database opened with a schema re-asserts it after the import, and the given schema supersedes the dump's definitions for the idents it names (§9). A fork asserts its parent's declared definitions into the branch, and `TruncateTo` runs with the given schema over the definitions that survive the rewind, writing nothing.

The facts are queryable with patterns:

```clojure
[:find ?ident ?type ?card
 :where [?a :db/ident ?ident]
        [?a :db/valueType ?type]
        [?a :db/cardinality ?card]]
```

The engine's own words carry no definitions: `:db/txInstant`, `:db/id`, the snapshot and branch records, and the seven words above form a roster in `datalog/storage` that the refusal below treats as known, so the engine's own queries run on a database that refuses unknown attributes and never minted a marker.

The refusal (§5) is opt-in through the `RefuseUnknownAttributes` database option, which requires a given schema, and runs at execution entry, in `Executor.ExecuteWithRelations` and in the `Pull` family, not in the parser: `ParseQuery` has no database, and the parse cache maps text to a query that does not depend on the store. A parsed query collects the attributes it names once, on its first execution. The matcher owns the schema, so it answers `AttributeVocabulary.UnknownAttributes`, and `SourceRouter` delegates the question to the `$` source; a named source's patterns are not checked.

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
- an attribute the schema does not declare, on a database opened to refuse them: the attribute (`schema.UnknownAttributeError`)
- a witness fold refused: the clause or function that blocks it
- a bound exceeded: the predicates, the cap, and the count reached (`query.RuleIterationLimitError`, `query.DerivedTupleLimitError`)

`ClauseBlockers` is the pattern: the error carries the symbols, not only a sentence.

Bounds: the iteration and tuple caps of §1.4, and query cancellation through `WithContext`. `WithContext` is a query option passed among the inputs of `Query`, `QueryInto`, `QueryOneInto`, `Explain`, `ExplainAlgebra` and `Analyze` (`storage.WithContext`, re-exported as `db.WithContext`). A query whose context is done is refused at entry, before any work. A fixpoint whose context is done stops at its next iteration. The context's error is the query's error. The context is read at entry and once per iteration, never per tuple: the fixpoint is the one loop that can run without bound, and the caps are its primary guard. `Diff` (§3) takes the same option.

Tests: each rule and attribute error above is produced by a query built to produce it, with its named parts asserted, in `datalog/query/rules_test.go`, `datalog/query/magic_test.go` and `datalog/storage/rules_test.go`. A query cancelled before execution returns the context's error from `Query`, `QueryInto` and `Analyze`. A closure cancelled from its first iteration event stops at the next. Those are in `datalog/executor/cancellation_test.go`, `datalog/storage/cancellation_test.go` and `datalog/db/cancellation_test.go`.

---

## 6. Surface

The EDN query form plus `%` rules is the grammar. The roadmap's ruling on Datomic surface parity holds for rules: the forms are Datomic's.

A second grammar, Horn clauses with `:-`, is deferred. Two things decide it when it is reopened. A Prolog-style predicate is n-ary and a janus scan is a pattern over one attribute, so an extensional predicate has to be declared as a pattern over attributes and an intensional one is a derived relation. And typed relation declarations in the Soufflé style are subsumed by §4, since the schema facts already carry the types. If added, it is a parser onto the same `query.Rule` and `query.RuleSet` types, with no second evaluator.

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

1. Schema as facts (§4) and the errors that read them (§5). Everything a query author writes depends on these.
2. Rules and recursion (§1), with the bounds and cancellation (§5).
3. `With` and `Diff` (§3).
4. Witnesses (§2): the rewrite, the fixpoint's first derivations, `Explain`, the commit function and transaction attributes, the renderer.
5. The deferred items (§9) as they are reopened.

---

## 9. Decisions and open questions

### Decisions this proposal takes

- Witnesses are computed per query and never stored. The semiring is a fold at the result boundary, chosen by a planner option.
- A rule invocation is a clause. It evaluates its predicate when it runs. The derived relation is materialized.
- Base scans in a rule body stream.
- The rule set rides the execution context as the environment does.
- Cancellation rides a query option, `WithContext`, among the inputs of `Query` and its siblings. No caller's signature changes.
- The query builder has rules: `qb.Rules`, `qb.Rule`, `qb.DefineRule`, `qb.RuleSet`.
- A same-stratum invocation stands at the top level of a body or inside an `or` or `or-join` branch. Under `or-default`, `or-default-join` or a subquery it is refused.
- A bound invocation evaluates only what its bindings reach, through the magic-set rewrite seeded by the bound values (ruled 2026-10-12). Whole evaluation is the case where nothing is bound.
- The required-binding head is accepted. Its positions are the invocation's mandatory correlates, and the planner orders the invocation after their providers.
- Sideways information passing in the rewrite is left to right. It stops at a subquery.
- A rewrite whose result does not stratify is declined, and the predicate evaluates whole. The decline is recorded as an annotation event.
- A rule body's plan enters the plan cache under its own text, which carries `%` and the delta symbols, so the cached plan is the one the fixpoint would make.
- No fact reaches the planner on a clause field the text does not render (ruled 2026-10-12). The planner takes the rule set as input beside the bound symbols, the delta is a predicate with a symbol of its own, and a phase that invokes a predicate declares `%`.
- The planner reads from an invoked predicate's name whether a seed restricts its relation, and schedules an invocation of a magic or adorned predicate, or of the delta of one, before the patterns (ruled 2026-10-12).
- The predicates the engine mints are named in namespaces of the engine's, `adorned`, `magic` and `delta`, and a rule set may not declare a predicate there.
- The naive baseline the benchmark measures is the semi-naive step driven with the full relation at every occurrence. There is no production flag and no second evaluator.
- Negation is stratified and witnesses stop at a negated body, whose explanation is `Explain`'s.
- An aggregate's witnesses are its group's witnesses, computed beside the aggregate and paired on the group keys.
- A recursive predicate's witness is its first derivation. Minimal-witness labels are deferred.
- `With` is an overlay matcher on the `PatternMatcher` seam, on every backend. `Fork` is the persistent form and stays as it is.
- The commit function reads the marks the commit already writes. No per-datom commit reference is stored.
- One grammar.
- A schema given at open supersedes the store's schema facts for the idents it names (ruled 2026-10-11).
- The unknown-attribute refusal is opt-in through a database option, and the error names the attribute alone (ruled 2026-10-11).

### Open

- `With` over a `History` handle: whether the overlay's datoms appear unresolved beside the base's history, or `With` refuses a history base.
- Recursive aggregation (shortest paths, counts over closures): deferred, with the monotone cases named when reopened.
- Minimal-witness labels for recursive predicates: deferred.
- Fork on the Badger store: deferred, per [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md).
- A second grammar: deferred, per §6.
