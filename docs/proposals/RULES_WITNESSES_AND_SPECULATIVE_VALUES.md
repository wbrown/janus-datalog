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

1. **Rules and recursion.** Rules passed as `%`, evaluated to a fixpoint with semi-naive iteration, stratified negation, and caller-set bounds. The derived relation joins the rest of the query through the executor's relation inputs.
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

### 1.1 Syntax and types

The syntax is [RECURSIVE_RULES.md](RECURSIVE_RULES.md)'s: a rule set is a vector of clauses, each `[(name ?args...) body...]`, passed as the `%` input and invoked as `(name ?args...)` in `:where`. Several clauses for one head are alternatives.

Types in `datalog/query`: `Rule` (head name, head symbols, body clauses), `RuleSet` (rules grouped by head name), and `RuleInvocation` as a clause. The parser accepts `%` in `parseInputSpec` and a list whose head is a rule name in the clause dispatch, which today recognizes a fixed set of keywords.

### 1.2 Safety and stratification

A rule is safe when every head symbol, and every symbol in a negated or comparison literal, appears in a positive body literal. An unsafe rule is a parse error naming the rule and the symbol.

The stratifier builds the predicate dependency graph, marks an edge through `not` or `not-join` negative, finds the strongly connected components, refuses any component holding a negative edge, and orders the rest. The refusal names the predicates on the cycle. Negation in a body reads only strata already complete. Aggregation inside a recursive component is refused in this round. A rule body may aggregate over a completed stratum through a subquery, as a query can today.

### 1.3 Evaluation

Each stratum evaluates bottom-up to a fixpoint. Semi-naive iteration joins only the previous iteration's delta against the full relations, and stops when the delta is empty. The derived relation is a `MaterializedRelation`, since fixpoint detection needs the whole set, and set semantics give the delta for free: a tuple already present is not new.

A rule body is an ordinary `query.Query` whose rule invocations are replaced by relation inputs. Each iteration runs the body with the delta bound as the relation input for the invoked predicate and the full relation for the others. The executor plans and runs the body as it plans any query. Base-fact patterns in a body stream from storage through the usual matcher. The whole fixpoint runs inside one `ReadSession`, so every iteration reads one snapshot.

The invoking query receives the derived relation as a relation input and joins it on the invocation's symbols. Phasing, selectivity scoring and the Cartesian-product check see a rule invocation as a clause that provides its head symbols.

### 1.4 Bounds

Two fields on `PlannerOptions`: a maximum iteration count per stratum and a maximum derived-tuple count per rule set. Either exceeded is an error naming the predicate and the count reached.

### 1.5 Tests

- A transitive closure on a chain and on a cycle, checked against a closure computed by hand, in both optimizer modes.
- A stratified rule set with negation over a completed stratum, checked by hand.
- An unstratifiable rule set refused with the cycle named.
- An unsafe rule refused with the symbol named.
- A rule invocation inside a subquery, inside an `or-join` branch, and under a `not-join`.
- The iteration cap and the tuple cap each tripped on a rule set built to exceed them.
- A benchmark: closure over edge sets of one thousand, ten thousand and one hundred thousand edges.

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

This section describes what the engine does. Each attribute definition is an entity, `db.attribute/<ident>`, content-addressed from the ident so a definition asserted anywhere converges on one entity. It carries one fact per field of the definition under the well-known words `:db/ident`, `:db/valueType`, `:db/cardinality`, `:db/unique`, `:db/uniqueElements`, `:db/neverZeroValue` and `:db/doc`; a value type or uniqueness constraint a later declaration drops is withdrawn and stops resolving.

A schema given at open is asserted into the store in one transaction holding only the words that differ from what the store holds, so a reopen with an unchanged schema writes nothing. A store opened with no schema reads its definitions back and runs with them, inferring from the ops only the attributes that carry datoms and no definition. A dump carries the definitions as datoms, and `Import` and `ImportBinary` end by reading them, so `db.Open` on a dump resolves a set as its writer declared it. A database opened with a schema re-asserts it after the import, and the given schema supersedes the dump's definitions for the idents it names (§9). A fork asserts its parent's declared definitions into the branch, and `TruncateTo` runs with the definitions that survive the rewind.

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

Every error a query author can repair names the thing to repair:

- an unsafe rule: the rule and the symbol
- an unstratifiable rule set: the predicates on the negative cycle
- an unknown rule invocation: the invoked name and the rule set's predicate names
- an attribute the schema does not declare, on a database opened to refuse them: the attribute
- a witness fold refused: the clause or function that blocks it
- a bound exceeded: the predicate, the cap, and the count reached

`ClauseBlockers` is the pattern: the error carries the symbols, not only a sentence.

Bounds: the iteration and tuple caps of §1.4, and query cancellation through a `context.Context` accepted by `Query`, `Explain` and `Diff`, which the roadmap lists as timeout and cancellation. A cancelled query returns the context's error from the iterator's `Error()` at the next `Next()`, through every operator, under the iterator-error contract.

Tests: each error above produced by a query built to produce it, and its named parts asserted. A cancelled closure stopping within one iteration.

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
- A rule invocation is a relation input to its body, and the derived relation is materialized. Base scans stream.
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
