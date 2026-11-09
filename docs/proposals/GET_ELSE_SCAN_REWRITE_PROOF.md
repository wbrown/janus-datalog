# The get-else scan rewrite answers what the lookup answers, as a proof

## Sources

- The rewrite: `datalog/algebra/rewrite_getelse.go` (`getElseScanRewriteTransform`, `originatesAtRequiredRunAlone`), `datalog/algebra/decompile.go` (`decompileLeftOuterJoin`), and `DefaultPasses`.
- The rewritten clause's execution: `datalog/executor/scope.go` (`executeOrDefaultJoinClause`), `datalog/executor/or_fallback_relation.go` (`NewOrFallbackRelation`, `orBranchesEmitAtMostOne`, `patternAnswersAtMostOne`, `eaArmShape`, `buildFromCache`, `buildBranchFromEACache`, `recordAnswered`, `unanswered`, `nextShortCircuit`), and `datalog/executor/query_executor.go` (`tryFuseAttributeFetchBundle`).
- The get-else's execution: `datalog/executor/query_executor.go` (`executeExpression`, `entityLookup`), `datalog/executor/iterator_composition.go` (`admitExpressionResult`, `FunctionEvaluatorIterator`), and `datalog/query/database_function.go` (`GetElseFunction.EvalWithLookup`, `toIdentity`).
- The sources: `datalog/executor/source_router.go` (`Match`, `MatchWithConstraints`, `LookupSource`, `CanFuseAttributeFetch`), `datalog/executor/annotated_matcher.go` and `datalog/executor/scan_sharing_matcher.go` (`Match`, `LookupSource`, `CanFuseAttributeFetch`), and `datalog/executor/interfaces.go` (`EntityLookupMatcher`, `AttributeFetchFusable`).
- Storage: `datalog/storage/matcher.go` (`CanFuseAttributeFetch`, `LookupAttribute`, `TypeDefault`, `chooseIndex`, `typedPositionBindingCheck`), `datalog/storage/matcher_relations.go` (`MatchWithConstraints`), `datalog/storage/crdt_resolving_iterator.go` (`Next`, `startNewGroup`, `processUniqueEntry`), `datalog/storage/cache_resolver.go` (`ResolveLWW`), and `walkUniqueEntityValue`, `Cache.GetOrResolve`, `Database.matcherWithExecOptions`, `Database.installLocked`, `Database.Match` and `Database.Matcher`.
- The prior proofs: `docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md`, whose P1, P2, P4, P7, P9 and L1 are read here as given, and `docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md`, whose §1 states the failure contract and whose §7 states its reach. `docs/ALGEBRA.md` states the rewrite as Rule 5, and the round trip every rewrite keeps.
- The failure: applied at every source, the rewrite answers a get-else on a cardinality-many attribute of a database's `$` with one tuple per member, where the lookup answers the set (`TestAGetElseAnswersAlikeUnderBothOptimizerPathsWhereAMatchAnswersOtherwise`). On a history handle it answers one tuple per datom, where the lookup answers the latest value (`TestReviewAlgebra_GetElseOnATemporalSourceAnswersAlikeUnderBothOptimizerPaths`).

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **g** is a get-else clause `[(get-else S ?e A d) ?v]`. **S** is the source it reads (`ReadsFrom`), **A** its attribute, **d** its default, a constant or a variable, and **?v** its binding symbol.
- **K** is g's required run: `?e` and d's variables (`GetElseFunction.RequiredSymbols`). A **key** is a tuple of values over K.
- **g(k)** is g's answer at a key k: `EvalWithLookup` over k with the lookup the executor's matcher gives for S (`entityLookup`). It is a value or a failure.
- **The lookup at e** is that lookup's `LookupAttribute(e, A)`, which finds a value or none.
- **The match at e** is the tuples the executor's matcher answers for the pattern `[S ?e A ?v]` under a binding of `?e` to e.
- **F(S, A)** is the fact that, for every Identity e, the match at e is the one tuple (e, v) when the lookup at e finds v, and no tuple when the lookup finds none.
- **The gate** is `CanFuseAttributeFetch(S, A)` asked of the executor's matcher. `AttributeFetchFusable`'s doc states F as the fact a true answer makes: a source answers true only where its match of A answers, for every entity, the one value its lookup answers.
- **The rewritten clause** is the clause the rewrite decompiles to, `(or-default-join [[K] ?v] [S ?e A ?v] [(get-else S ?e A d) ?v])`. **Branch 1** is its data pattern and **branch 2** its get-else.
- **The subject** is the relation the rewritten clause runs over, the groups holding K (`executeOrDefaultJoinClause`).

## 2. Preconditions

- **P1 (what the rewrite builds).** `getElseScanRewriteTransform` replaces `Map(g)` over a child with a `LeftOuterJoin` on `?e` of the child and `Scan([S ?e A ?v])`. The pattern's source is S, and nil when S is `$`. The join's `DefaultGetElse` is g. `decompileLeftOuterJoin` emits the child's clauses followed by the rewritten clause, whose required run is `?e` and d's variables. The transform applies only where `?e` is a variable the child provides, `?v` is a symbol the child does not provide, d's variables are symbols the child provides, and `?e` does not originate at the scope's required run alone. So the subject carries K.
- **P2 (branch 2 runs g).** A header form's branch is compiled over a childless placeholder providing the form's required run (`originatesAtRequiredRunAlone`). Branch 2's entity originates there alone, so the pass declines it, and branch 2 evaluates g at each key it runs over. `DefaultPasses` runs decorrelation and then the get-else pass, so no pass reshapes the rewritten clause once it is built.
- **P3 (the fallback's rule).** `executeOrDefaultJoinClause` runs the rewritten clause through `NewOrFallbackRelation` under the fallback rule. `nextShortCircuit` answers each subject tuple with the matches of the first branch whose index holds the tuple's key, and drops a tuple no branch answers. It reads a branch's matches for the key before an output the subject or the environment binds filters them. `unanswered` runs branch 2 only over the keys branch 1's runs left unanswered (`recordAnswered`).
- **P4 (branch 1's routes).** Branch 1's answer at a key comes from one of three routes.
  - Its plan matches the pattern through the executor's matcher.
  - Where S is `$` and the gate answers true, attribute-fetch fusion (`tryFuseAttributeFetchBundle`) extends a tuple whose entity is an Identity e by the value the lookup of `$` finds at e. It drops a tuple whose lookup finds none, and skips a tuple whose entity is no Identity.
  - Where S is `$`, the gate answers true and a window was prefetched, the EA arm (`buildFromCache`, `buildBranchFromEACache`) gives (e, v) for an entity e whose lookup of `$` finds v. It skips an entity that is no Identity.
- **P5 (one name reaches one source).** The router sends a pattern to the source it holds under the pattern's source, `$` when the pattern names none (`SourceRouter.Match`, `MatchWithConstraints`). The executor takes g's lookup through `LookupSource(S)` (`entityLookup`), and the router gives the lookup the source it holds under S answers for `$`. The router answers the gate from that source too, and a source it does not hold, or one without the capability, answers false. `AnnotatedMatcher` and `ScanSharingMatcher` hand the name to the matcher they wrap for the gate and the lookup. `AnnotatedMatcher.Match` returns its wrapped matcher's answer. `ScanSharingMatcher.Match` hands a match whose bindings bind a variable of its pattern to its wrapped matcher with them. It answers any other match with its wrapped matcher's answer to the pattern with no bindings, which a storage matcher gives alike (`MatchWithConstraints`). A storage `PatternMatcher` answers all three for itself under any name. So for each S whose source is a storage matcher m, the pattern, the lookup and the gate each reach m.
- **P6 (the implementers).** `gopls implementation` of `AttributeFetchFusable` lists `SourceRouter`, `AnnotatedMatcher`, `ScanSharingMatcher`, storage's `PatternMatcher` and `*storage.Database`, beside test fixtures. The first three answer from the source they reach (P5). A database answers the gate through `Database.Matcher`, a storage matcher it mints at the call with its effective schema and its view, and its `Match` and `LookupSource` answer through the same method. The fixtures are `fusionOracle`, which answers from a function its test gives, and four wrappers that embed `*storage.Database` and answer through it.
- **P7 (storage's gate).** `PatternMatcher.CanFuseAttributeFetch` answers false in History. In a resolved view, the latest or one as of a target, it answers true when the matcher's schema is nil, holds no definition of A, or declares A cardinality-one.
- **P8 (one schema per matcher).** `Database.matcherWithExecOptions` gives a matcher the database's effective schema at the matcher's creation. `Database.installLocked` composes each install's effective schema afresh through `schema.Overlay` and stores it as the installed one, leaving the earlier one as it was. So a matcher answers its gate and resolves its reads by one schema for its life.
- **P9 (storage's match).** A storage matcher m outside History matches a pattern with a constant attribute and an entity variable the bindings bind as follows.
  - A binding whose entity is no Identity drives no seek and yields no tuple (`typedPositionBindingCheck`, applied in `MatchWithConstraints` before the cache arm and the join strategies).
  - A declared vector attribute is answered by `matchVectorWithBindings` before the other arms.
  - Every other arm, the cache arm included, answers each binding its agreeing datoms of m's resolved view (`BOUND_VALUE_CACHE_MATCH_PROOF.md` P2, P4 and L1).
  - Bindings that bind none of the pattern's variables leave the pattern scanned unbound, and the executor joins that answer with the group on `?e` (P1 there).
- **P10 (storage's resolution at a cardinality-one attribute).** `CRDTResolvingIterator` resolves an attribute m's schema leaves undefined (`CardinalityUnknown`) as it resolves a cardinality-one attribute. It reads each (E, A) group in Tx-descending order and emits the group's first datom, or nothing when that datom's op is `OpCRDTRemove`. Under a unique declaration it emits the first datom `walkApplyEntry` emits (`processUniqueEntry`). Every scan arm constructs it with m's schema and m (`gopls references` of `NewCRDTResolvingIterator`). For such an attribute `chooseIndex` picks EATV when the entity is bound and AETV when it is not, and each orders a group by Tx descending. So m's view holds at most one datom for an entity at A.
- **P11 (storage's lookup at a cardinality-one attribute).** `PatternMatcher.LookupAttribute` resolves A under the cardinality m's schema declares, and under cardinality one where it holds no definition. Under cardinality one it answers the value `ResolveLWW` resolves. That is the first datom of the EATV group within the view, or none when its op is `OpCRDTRemove`. Under a unique declaration it is the value `walkUniqueEntityValue` resolves through `walkApplyEntry`. `ResolveLWW` takes that walk when the definition `LookupAttribute` passes it declares a unique. A cache entry answers the lookup only when it was resolved under the reader's cardinality and uniqueness rule and its version lies within the reader's snapshot (`Cache.GetOrResolve`). Otherwise the entry is rebuilt through m, which calls `ResolveLWW`.
- **P12 (g's answer).** `GetElseFunction.EvalWithLookup` reads the entity from the key and converts it through `toIdentity`: an Identity as it is, a string through `NewIdentity`, and any other value a failure. It answers the value the lookup finds. Where the lookup finds none, it answers the default the key gives, typed by the lookup's `TypeDefault`. `admitExpressionResult` admits the value unchanged unless it is NaN, which the write and input boundaries exclude.
- **P13 (one state per execution).** The theorems compare the two paths over one state of each source. The query's `$` reads through the query's read session, with the schema its matcher took at creation (P8). A source passed through `WithSources` is read with no session, and a database passed so mints a matcher per call (`Database.Match` through `Database.Matcher`). Such a source holds one state over an execution when nothing commits to it and no schema is installed on it meanwhile. That is also the condition under which two lookups of an unrewritten get-else on it read one state.
- **P14 (failures).** An evaluation failure fails the query exactly at an assignment where every other clause holds (`PULL_BASED_HASH_JOIN_PROOF.md` §1). The contract does not decide whether a store failure is raised (§7 there). So the theorems compare executions whose store reads fail nowhere, and compare evaluation failures by the assignments at which they occur.

## 3. The conditions the change establishes

- **G1.** The pass applies the rewrite to g only where the gate answers true for (S, A).
- **G2.** The S the gate is asked about is the name under which the rewritten clause's pattern and get-else reach their source when the plan executes.
- **G3.** The matcher executing a plan answers each pair the plan's rewrites asked as it answered when the plan was made.

§7 names the code that establishes each.

## 4. Theorems

**T1 (a true gate states F).** Where the executor's matcher answers the gate true for (S, A), F(S, A) holds.

**T2 (the rewritten clause answers g).** Under G1 to G3, the rewritten clause answers each subject tuple t with key k by the one tuple t extended by `?v = g(k)` when g(k) is a value, and fails at t exactly when g fails at k.

**T3 (the query answers alike).** Under G1 to G3, a query and the query the rewrite makes of it answer the same tuples, and each fails exactly where the other does.

**T4 (a declined rewrite changes nothing).** Where the gate answers false, the pass leaves `Map(g)`, which decompiles to g, the per-tuple lookup the optimizer-off planner runs.

**T5 (the form's judgment holds).** Under G1 to G3, `orBranchesEmitAtMostOne` judges the rewritten clause's branches to answer at most one tuple per subject tuple, and they do.

**T6 (each condition is needed).** Without any one of G1, G2 and G3, some query answers differently on the two optimizer paths.

## 5. Lemmas

### L1. A true gate states F (T1)

Let a storage matcher m answer the gate true. By P7, m reads a resolved view, not History, and m's schema leaves A undefined or declares it cardinality-one. Fix an Identity e. By P9, the match at e answers e's datoms at A in m's resolved view. By P10, that view holds at most one such datom: the group's first unless its op is `OpCRDTRemove`, or the first the unique walk emits. By P11, the lookup at e resolves the same group by the same rule, through `ResolveLWW` or through an entry resolved under the same rule within the same snapshot. Both read m's one schema (P8) and one state (P13). So the match at e is the tuple (e, v) exactly when the lookup finds v, and no tuple exactly when it finds none. That is F for m.

A router or a wrapper answers true from the source it reaches under S, and by P5 the pattern and the lookup reach that same source. When that source is a storage matcher, the first paragraph gives F. When it is a database, the gate, the pattern and the lookup each reach a matcher the database mints (P6). By P13 nothing commits to the database and no schema is installed on it during the execution, so by P8 the matchers it mints hold one schema, and they read one state. The first paragraph's argument then runs with the match read through one of them and the lookup through another, and gives F for the database. By P6 every other implementer is a test fixture. ∎

### L2. The rewritten clause per subject tuple (T2)

Fix a subject tuple t with key k, and let x be its value at `?e`. g reads K alone, so g(k) is the answer g gives at t.

- x is no Identity. Branch 1 answers nothing at x on each of its routes (P4, P9). By P3 branch 2 runs at k, and by P2 it answers g(k) or fails where g fails. A string x is looked up as `NewIdentity(x)` by branch 2 as by g (P12).
- x is an Identity. By G1 and G3 the executing matcher answers the gate true, and by G2 the clause's pattern and get-else reach the source it answered for. By T1, F(S, A) holds of that source. Fusion and the EA arm read the lookup of that source, which is `$` on those routes (P4).
  - If the lookup at x finds v, branch 1's matches at x are the one tuple (x, v). By P3 branch 1 answers t with `?v = v`, and branch 2 does not run at k. By P12, g(k) is v, and g does not fail at k.
  - If the lookup at x finds none, branch 1 answers nothing at x. By P3 branch 2 runs at k and answers g(k), the default typed by `TypeDefault`, or fails where g fails.

In each case the clause answers t with the one value g(k), or fails at t exactly where g fails at k. ∎

### L3. The query (T3)

The two queries differ in one clause. `decompileNode` emits each node's clauses from its children's, and the rewrite changes one `Map(g)` node, so every other clause is the same. By L2 each of the two clauses relates every key k to the one value g(k), and fails at the same keys. g's expression unifies `?v` with a binding of it the tuple carries (`FunctionEvaluatorIterator`). The rewritten clause decides from its branches first and filters an output bound elsewhere after (P3). So each clause denotes the relation of the pairs (k, g(k)), which another binding of `?v` joins alike. A query's answer is the join of the relations its clauses denote, the semantics `docs/ALGEBRA.md`'s round trip keeps. Its failures are decided by the contract from the assignments at which a clause fails (P14). Both are the same for the two queries. ∎

### L4. Declining (T4)

Each decline in `getElseScanRewriteTransform` returns the node with its children rebuilt (`rebuildWithChildren`), or the node itself, so `Map(g)` stays in the tree. A `Map` decompiles to its child's clauses followed by its expression (`docs/ALGEBRA.md`, the Map round trip). So g runs per tuple through its source's lookup (`executeExpression`), as it runs on the optimizer-off planner, which plans the clauses as written. ∎

### L5. The form's judgment (T5)

`orBranchesEmitAtMostOne` accepts branch 2, an expression under the scalar binding, as answering one tuple per key. For branch 1 it asks `patternAnswersAtMostOne`, which reads the gate for the pattern's source and attribute, the pair G1 asked, and finds it true under G1 to G3. By T1, branch 1 answers at most one tuple per entity. So the judgment `NewOrFallbackRelation` passes to `orProperties` is true and holds. ∎

### L6. Each condition is needed (T6)

- Without G1. On a cardinality-many attribute of a database's `$`, the lookup answers the set as one value, `[one [blue red]]`, and the match one tuple per member, `[one blue]` and `[one red]` (`TestAGetElseAnswersAlikeUnderBothOptimizerPathsWhereAMatchAnswersOtherwise`). On a history handle the match answers every datom, `[one old]` beside `[one new]` (`TestReviewAlgebra_GetElseOnATemporalSourceAnswersAlikeUnderBothOptimizerPaths`).
- Without G2. An aggregating subquery's inner get-else reads the inner `$`, the call passes a history handle there, and the outer `$` is a database declaring A cardinality-one. Rule 1 compiles the inner query into the outer tree under the inner names, so a gate asked under the inner `$` asks the outer database and answers true. The decompiled call passes the history handle, whose match answers every datom.
- Without G3. One query text runs on a database's `$`, and then with `$` bound to its history handle through `WithSources`. The handles share the database's plan cache, and the plan made under the database's true answer serves the history handle. `SetSchema` declaring A cardinality-many between two runs does the same, since the install leaves the plan cache as it is.

## 6. Tests

The change carries these tests, each pinning the theorem named.

- `TestAGetElseAnswersAlikeUnderBothOptimizerPathsWhereAMatchAnswersOtherwise` (storage): a cardinality-many attribute on a database's `$`, an attribute no schema declares, and one query text over a database's plan cache (T3, T6).
- The `TestReviewAlgebra_`, `TestRefuteAlgebra_`, `TestReviewRoute_` and `TestRefuteRoute_` families (storage, db): sources named and as `$`, history handles named and as `$`, a subquery's `$`, a rule body and `qb.GetElseFrom` (T3, T6).
- `TestASchemalessDatabaseAnswersWhetherAMatchAnswersItsLookup` (storage): storage's answer on a database opened with no schema (T1).
- `TestFusionGate_ModeAndCardinality`, `TestFusion_AnAttributeTheSchemaDoesNotDefineFusesAndAnswersAlike` and `TestFallbackServesAnAttributeTheSchemaDoesNotDefineFromTheCache` (storage): the gate's other readers at an attribute with no definition (T1, T5).
- `TestExplainAlgebraGetElse` (storage): the rewrite applies on a database opened with no schema (T1, T3).
- `TestQueriesAnswerAlikeUnderBothOptimizerModes` (storage): the optimizer-mode differential (T3).
- `TestAGetElseAnswersAlikeWhenThePlanCacheServesItsTextOnAHistoryHandle` (storage): a database's `$` and its history handle running one text through the shared plan cache (G3, T6).
- `TestAGetElseAnswersAlikeAfterSetSchemaDeclaresItsAttributeCardinalityMany` (storage): `SetSchema` changing A from cardinality-one to cardinality-many between two runs of one text (G3, T6).
- `TestAGetElseInAnAggregatingSubqueryAnswersAlikeOnTheSourceTheCallPasses` (storage), with `TestADecorrelatedAggregateReadsTheSourcesItsCallPasses` and `TestUnderCallSourcesRenamesTheInnerQueryToTheSourcesItsCallPasses` (algebra): an aggregating subquery whose call passes, as its inner `$`, a source holding two values at A, with the outer `$` declaring A cardinality-one (G2, T6).
- `TestExplainAlgebraDeclinesTheGetElseRewriteOnAHistoryHandle` (storage): `ExplainAlgebra` of a get-else on a history handle passed through `WithSources`, recording the rewrite declined with its reason (T4).
- `TestExplainAndAnalyzePlanWithTheQuerysSources` (storage): `Explain` and `Analyze` planning with the query's `WithSources` (G2, G3).
- `TestGetElseScanRewrite_AsksTheSourceTheGetElseReads` (algebra): the pass declining a get-else whose source answers false, and asking with the source the get-else reads (G1, T4).
- `TestThePlanCacheServesAPlanOnlyWhereTheSourcesAnswerAsWhenItWasMade`, `TestThePlanCacheReadsTheAnswersOfTheBodyPlansAPlanEmbeds` and `TestAPlannerGivenNoAnswerDeclinesTheGetElseRewrite` (planner): one plan cache serving a plan only where the requesting planner's sources answer as they did when it was made, the questions of the body plans it embeds among them, and a planner given no answer declining the rewrite (G1, G3).
- `TestThePlanCacheReadsTheAnswersOfTheScopePlansARewriteReads` (planner): a plan recording the questions of the scope plans a magic-sets rewrite reads, a rule's body, a form's branch and a subquery's inner query, so one plan cache does not serve it to a planner whose sources answer otherwise (G3, T6).
- The `TestRefuteGate_` and `TestGateReview_` families (storage, db): a bound invocation whose body holds a get-else, its text run on a database and then on its history handle or after `SetSchema`, through each entry point (G3, T6).
- `TestADatabasePassedAsANamedSourceAnswersWhetherAMatchAnswersItsLookup` (storage): a database passed as a named source answering the gate through its matcher (T1).

## 7. The change the proof licenses

- `datalog/storage/matcher.go`, `PatternMatcher.CanFuseAttributeFetch`: true in a resolved view, the latest or one as of a target, at an attribute the matcher resolves cardinality-one, a nil schema and a missing definition included (P7).
- `datalog/storage/database.go`, `Database.CanFuseAttributeFetch`: the gate answered through `Database.Matcher`, as `Match`, `LookupAttribute` and `LookupSource` answer (P6, L1).
- `datalog/executor/interfaces.go`, `AttributeFetchFusable`'s doc: a true answer states F.
- `datalog/planner`, `PlannerOptions`: the sources' answer, which `executor.NewExecutorWithOptions` sets from its matcher's `CanFuseAttributeFetch`. A planner given none declines every get-else rewrite (G1, and G3 within one execution).
- `datalog/algebra`, `GetElseScanRewritePass` and `DefaultPasses`: take the answer. The pass declines a get-else whose source answers false and records the reason (G1, T4).
- `datalog/planner`: each pair a planning call asks is recorded on the plan with those of the body plans it embeds and of the scope plans its evaluations' magic-sets rewrites read, whose clauses the rewritten rule bodies hold, and the plan cache's `madeUnder` asks the requesting planner's sources each recorded pair (G3).
- `datalog/algebra/rewrite_decorrelate.go`, Rule 1: the inner query's clauses and `:in` sources are renamed by the call's substitution before the inner query is compiled, as `ReadSubqueryFrame` renames a framed scope's clauses (G2).
- `datalog/storage`, `Explain`, `ExplainAlgebra` and `Analyze`: plan with the query's `WithSources`, through the router `Query` builds (G2, G3).
- `docs/ALGEBRA.md`, Rule 5: the source precondition, the rewritten clause as `decompileLeftOuterJoin` emits it, and a pointer to this proof.

## 8. What the proof does not cover

- A store read that fails. The rewrite reads the store where the lookup does not, the match at every key, and the contract does not decide whether a store failure is raised (P14).
- A source from outside the tree whose `CanFuseAttributeFetch` answers true where F fails. T1 covers each implementer outside the tests, and any other source is held to `AttributeFetchFusable`'s doc.
- A router held as a source of a router. The outer router asks it the gate under S and its lookup under `$`, and hands it the pattern, which it routes by S. The three reach one source only when it holds one source under both names.
- A source that is written, or given a schema, during the execution through anything but the query's read session (P13).
- A plan executed by a matcher other than the one whose planner made it, outside the plan cache. G3 is a condition on that matcher.
- Whether the gate is necessary. A false answer declines whether or not F holds.
- The other passes, whose preservation of answers Rules 1 and 4 of `docs/ALGEBRA.md` carry.
- The rewrite's cost.

## 9. Alternatives excluded

- Declining every source but `$`. A database's `$` overridden through `WithSources`, a history handle queried as `$`, and a cardinality-many attribute on `$` each break F under that name (L6).
- A branch 2 of ground values. A string entity matches no datom (P9) and is looked up as `NewIdentity` (P12), so only g answers it as g does. The typed default is g's `TypeDefault` too.
- Keying the plan cache on which source is bound. `$` is a new session matcher for every query, and one source's answer changes under `SetSchema` with no new binding.
- Computing the answers before planning. That computation repeats the planner's renaming of sources, a second computation of the source a get-else reads.
- Running the get-else pass before decorrelation. It drops Rule 5 inside aggregating subqueries, which `docs/ALGEBRA.md` measured at 13.7 s against 28.4 s.
- Declaring the attributes of a database opened with no schema. A database with no schema is a supported mode, and at an attribute with no definition the match and the lookup resolve alike (L1).
