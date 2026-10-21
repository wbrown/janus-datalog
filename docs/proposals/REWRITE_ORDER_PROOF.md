# The rewrite's order as a proof

## Sources

- The code on the branch `rules-witnesses-and-speculative-values`: `datalog/planner/magic.go` (`Rewrite`, whose result is a `Magic`, with `rewritePredicate`, `rewriteClauses`, `magicRuleReads` and `adornInvocation`), `datalog/query/minted.go` (`SeedRestricted`), `datalog/executor/rules.go` (`deriveInvocation`, `invocationEvaluation`, `boundPositions`, `seedRelation`), `datalog/executor/query_executor.go` (the find boundary of `Execute`, `existenceVerdict`), `datalog/planner/clause_phasing.go` (`selectPhaseClauses`) and `datalog/planner/clause_utils.go` (`scoreClause`, `selection.clauseSelectable`, `selection.invocationDependsOnPendingProvider`).
- The design: `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` (RULES) §1.3, `NESTED_SCOPES_AS_QUERIES.md` (NESTED) §2.5, §5.2 and §5.5, and `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §3, §4.1, §4.2, §4.4 and §4.6.
- The literature: C. Beeri and R. Ramakrishnan, "On the power of magic", Journal of Logic Programming 10 (1991), which defines a sideways information passing strategy over an ordering of a body and proves magic-set rewriting correct under every such strategy; J. D. Ullman, Principles of Database and Knowledge-Base Systems, volume II (1989), chapter 13.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **rule** r is the head `(p H)` over the symbols H and the body c_1, …, c_n (`Rule`).
- A **binding pattern** β of p marks each position bound or free. **H_B** is the head symbols at the bound positions (`rewritePredicate`'s `boundVars`).
- An **order** of r's body is a permutation of its clauses. The written order is the body's own, which `rewriteClauses` walks. The planner's order is the clauses of the phases `PlanQuery` returns for the body framed as a scope under H_B, less the `scope/required` invocation that opens them (NESTED §5.5, SCOPE §4.1).
- For an invocation q in the body, **P(q)** is the clauses before q under the order. **P*(q)** is the clauses of P(q) a magic rule reads: a clause that is not a `not` or a `not-join`, every mandatory correlate of which is bound before it, an `or-default-join` among them read as the union of its branches over its header, which binds what the form binds and holds every tuple it yields (`magicRuleReads`, `rewriteClauses`).
- **bound(q)** is H_B with every symbol a clause of P*(q) provides, as `rewriteClauses` accumulates `before.bound`.
- The **adornment** γ of q binds a position whose argument is a constant, or a variable of bound(q) at the variable's first position (`adornInvocation`).
- The **guard** of r is `(magic.p^β H_B)`.
- The **magic rule** of q is `(magic.q^γ A_γ) :- guard P*(q)`, with A_γ the bound arguments and a constant argument grounded into a fresh symbol (`adornInvocation` over `in.prefix`).
- The **adorned rule** of r is `(adorned.p^β H) :- guard c'_1 … c'_n`, each adorned invocation in its clause's place (`rewritePredicate`).
- The **seed** is the top-level invocation's bound values projected onto H_B (`seedRelation`), the facts of `magic.p^β` no magic rule derives.
- The **rewritten set** M is the adorned and magic rules of every predicate the bindings reach, built through `newRuleSet` (`Magic`).
- Two clauses are **adjacent** when they share a variable. The **seed component** of a magic rule's body is the guard and every clause connected to it by adjacency within that body.
- The **find boundary** (SCOPE §3): a magic rule's body runs as a query finding the rule's head (RULES §1.3). Its clauses collapse into groups that share symbols. A group the find reads no symbol of is an existence verdict (`existenceVerdict`). When the find's symbols lie in one group, that group is projected. When they span several groups, the answer is the groups' product (`Execute`).
- **Visible selectivity** (`scoreClause`'s doc, SCOPE §4.6): a data pattern has it when a constant stands at a position other than the attribute, or a variable of it is available when it is selected. An invocation has it when an argument is a constant, or an argument is available when it is selected, or the seed restricts its predicate (`SeedRestricted`: a magic or adorned predicate, the delta of one, or `scope/required`). Every other clause has it when a variable it reads or binds is available when it is selected: a form with outputs through its header, a subquery through its inputs and its binding, an expression through its inputs and its binding, and a predicate through its inputs. A `not-join` has none: it provides nothing, so it links no chain of bindings, and its body runs over its subject's windows whichever unrelated clause runs first, so its place among the clauses without it is its score's. A clause without it is a **scan of a whole extent**: the pattern reads every datom of its attribute, the invocation reads its predicate whole, a form runs each branch over the unit relation (SCOPE §4.4), a subquery runs its inner query over no input, and an expression computes from constants alone.
- The **planner's order** within a phase: `selectPhaseClauses` picks, among the clauses `clauseSelectable` admits, the clause `scoreClause` scores highest, the first in list order on a tie, and makes the symbols it provides available to the clauses after it. A clause is selectable when every mandatory correlate of it is available and no gate holds it. The **gate** `invocationDependsOnPendingProvider` holds an invocation of a predicate the seed does not restrict while a selectable clause other than a subquery or an invocation provides one of its unavailable variable arguments.

## 2. Preconditions

- **P1.** The rule set is safe and stratifies (`NewRuleSet`), and so does M: a site whose adornment would not stratify is left unadorned and the set rebuilt without it, so every rewrite is accepted (`NON_MONOTONE_FORMS_PROOF.md` T6, L4).
- **P2.** Each stratum of M evaluates to its fixpoint (SCOPE §4.2).
- **P3.** The relation "q′ stands in P(q)" over the body's invocations is acyclic. An order gives it.

## 3. Theorems

**T1 (correctness under every order).** For every order of every body, `adorned.p^β` holds exactly the tuples of p whose bound positions hold a seed tuple, and `adorned.q^γ` holds exactly the tuples of q whose γ-bound positions hold a tuple of `magic.q^γ`, for every adorned q^γ of M.

**T2 (a binding from outside the seed component).** Let c be a clause of P*(q) outside the seed component of q's magic rule, providing a variable x at a position of q, and let q have a position bound from the seed component. Let γ be q's adornment and γ⁻ the adornment with x's position free. Then:

- (a) the magic rule's find spans the seed component's group and the group holding c, and `magic.q^γ` is their product projected onto A_γ. When c's group is the only other group the find reads, |magic.q^γ| = |magic.q^γ⁻| · |π_x(G_c)|, with G_c the group holding c.
- (b) `adorned.q^γ` is `adorned.q^γ⁻` restricted to the tuples whose x-position lies in π_x(G_c). The adorned rule of p holds c in every order, so p's result is the same under γ and γ⁻.
- (c) Each iteration of q's stratum reads at least |magic.q^γ| keys. Binding x bounds the work by |π_x(G_c)|, the extent of c's relation less what c's own constants leave, a number the engine cannot see (SCOPE §4.6). Under γ⁻ the work is bounded by what the seed reaches.

**T3 (the planner's order under a rank).** Let the planner select among the selectable clauses by rank first and score second, where a clause without visible selectivity ranks below every other clause, and let the gate hold an invocation with an available argument only behind a provider with visible selectivity. Then in every phase an invocation one of whose positions the seed component binds is selected before any scan of a whole extent, and every clause before it in the order has visible selectivity.

**T4 (the closure).** Under T3's planner, the recursion-first closure `[(follows ?a ?c) (follows ?a ?b) [?b :page/next ?c]]` under `bf` adorns its invocation `bf`, with the magic rule `(magic.bf/follows ?a) :- (magic.bf/follows ?a)`. The binder-after closure `[(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]` under `bf` adorns its invocation `bf`, with the magic rule `(magic.bf/follows ?b) :- (magic.bf/follows ?a) [?a :page/next ?b]`. Both hold on both planner paths and whichever way each body is written. The written order gives the same two rewrites. Under the planner as it stands, the recursion-first body is ordered pattern first, so a rewrite reading that order adorns `bb`, and T2 applies with c the pattern.

**T5 (one rule for what is bound).** `boundPositions` binds a top-level invocation's position by a constant, the environment, or a relation group in hand, the result of the planned clauses before it. The rewrite under NESTED §5.5 binds by a constant, H_B, and the planned clauses before it. The two sites compute one fact by one rule, and T3's correction serves both. Under the planner as it stands, `[:find ?c :in $ % ?a :where (follows ?a ?b) [?b :page/next ?c]]` is ordered pattern first, `boundPositions` binds both positions, and `seedRelation` seeds `magic.bb/follows` with every page carrying a next beside `?a`: the product of T2, reached with no rewrite reading an order.

## 4. Lemmas

### L1. Soundness: `adorned.q^γ` ⊆ q for every adorned predicate of M

By induction on the iterations of the fixpoint. At iteration zero every adorned relation is empty. A tuple derived at iteration i by an adorned rule of q^γ is derived by that rule's body, which is an original rule's body with the guard conjoined and each invocation replaced by its adorned predicate. By the hypothesis each adorned predicate holds a subset of its original, and the guard only removes tuples, so the same original rule derives the tuple in the original set. ∎

### L2. Completeness: every tuple of q whose γ-bound positions hold a tuple of `magic.q^γ` is in `adorned.q^γ`

Let t be a tuple of p whose bound positions hold a seed tuple, and take a derivation of t in the original set of least depth d. The claim is proved by induction on d, and at each depth by induction on the clause position within the body of the rule r that derives t under the substitution σ.

The guard holds for t by the hypothesis. Walk r's body in the order. A clause c_i that is a data pattern, a predicate or an expression holds under σ, as in the derivation. For an invocation q at position i, every clause of P*(q) holds under σ: a pattern holds in the store, an adorned invocation before q holds its tuple σ(q′) by the inner induction, a fallback form is read as the union of its branches over its header, which holds under σ since the branch the fallback answered σ from holds under σ, and a negation is not read (`magicRuleReads`). So the magic rule of q derives σ(A_γ), and σ(A_γ) is in `magic.q^γ`. The tuple σ(q) of q has a derivation of depth below d, so by the outer induction σ(q) is in `adorned.q^γ`. Every clause of r's adorned body holds under σ, and `adorned.p^β` derives t. ∎

P3 enters at the inner induction: the magic rule of q reads adorned invocations before q alone, so their tuples are established before q's magic tuple is needed.

### L3. A cyclic prefix function derives nothing

Take `[(p ?a ?d) (q ?a ?b) (r ?a ?c) [?b :e ?d] [?c :f ?d]]` under `bf`, and let the prefix function give each invocation every other clause connected to H_B through the body, so r stands in P(q) and q in P(r). Then bound(q) holds `?a`, `?c`, `?d` and `?b`, q is adorned `bb`, and its magic rule reads `adorned.bb/r`. Symmetrically r is adorned `bb` and its magic rule reads `adorned.bb/q`. From the seed alone `magic.bb/q` has no tuple until `adorned.bb/r` has one, which waits on `magic.bb/r`, which waits on `adorned.bb/q`, which waits on `magic.bb/q`. The fixpoint leaves all four empty and `adorned.bf/p` empty, where the original set derives p's tuples. So the prefix function is an order (P3), never the set of clauses connected to H_B. ∎

### L4. The product (T2a)

In `Execute`'s find boundary over several groups, a find symbol in no group is an error, a group holding every find symbol is projected and returned, and otherwise the groups' product is projected. The magic rule's find is A_γ. The argument the seed component binds lies in the seed component's group, and x lies in G_c. The two groups are distinct, since c is outside the seed component and the groups are the body's connected components. So the find spans two groups, the product is taken, and its projection onto A_γ is the product of the projections. ∎

### L5. The restriction and the work (T2b, T2c)

(b) By T1, `adorned.q^γ` is q restricted to `magic.q^γ`, which by L4 is `magic.q^γ⁻` crossed with π_x(G_c). So it is `adorned.q^γ⁻` restricted to the tuples whose x-position lies in π_x(G_c). The clause c stands in p's adorned body in every order, since `rewritePredicate` keeps every clause, so a tuple of `adorned.q^γ⁻` whose x-position lies outside π_x(G_c) is dropped at c, and p's result is the same.

(c) Each rule of q^γ opens with the guard `(magic.q^γ A_γ)`, a seed-restricted invocation the planner leads with (`scoreClause`, `SeedRestricted`), so each iteration of the stratum joins the guard's every tuple with the body's first pattern, seeking per key (SCOPE §4.6): at least |magic.q^γ| keys per iteration. ∎

### L6. The rank (T3)

Let q be an invocation one of whose positions the seed component binds. The seed component's path from H_B to that argument is a chain of clauses, each sharing a variable with the one before, so each has visible selectivity once the one before has been selected. Under the rank, a scan of a whole extent is selected only when no other clause is selectable. While the chain's next link is selectable, no scan is selected. A link a gate holds waits on a clause the gate names, which is selectable and ranks above a scan: a pattern waits on a pending expression, a subquery on a provider, an invocation on a provider. So the chain completes before any scan is selected. When q's argument is available, q has visible selectivity and is selectable, since the gate holds it only behind a provider with visible selectivity, which is selected before q and before any scan. So q is selected before any scan, and every clause selected before q has visible selectivity. ∎

### L7. The closure (T4)

Under T3's planner with H_B = {`?a`} available, the recursion-first body: `(follows ?a ?b)` has an available argument and ranks above `[?b :page/next ?c]`, which has none, and the gate releases it, since the pattern providing `?b` has no visible selectivity. So the invocation is selected first, P*(q) is empty, γ is `bf`, and the magic rule is the guard alone. The binder-after body: `[?a :page/next ?b]` has an available variable, `(follows ?b ?c)` has no available argument and the gate holds it behind that pattern. So the pattern is selected first, P*(q) is the pattern, γ is `bf` with `?b` bound, and the magic rule reads the guard and the pattern. Written the other way round, each body gives the same two selections, since the rank and the gate read availability and not list order. On the algebra path the decompiled clauses enter the same selection. `rewriteClauses` in the written order makes the same two choices. Under the planner as it stands, the recursion-first body's invocation scores 110 against the pattern's 220, and the gate holds it behind the pattern, so the pattern is selected first, P*(q) is the pattern, and γ is `bb`. ∎

### L8. The top level (T5)

`boundPositions` reads the environment's symbols and the groups' symbols and marks a variable argument bound when either holds it. A query input is in the environment. The groups are the results of the clauses the planner placed before the invocation in its phase and the phases before it. So the invocation's bound set is the environment and what the planned clauses before it provide, the rule NESTED §5.5 states for the rewrite with H_B in the environment's place. For the query of T5 the planner as it stands selects the pattern first by L7's scores and gate, the group holds `?b`, and `seedRelation` builds one seed tuple per combination of `?a`'s value and the group's `?b` values. ∎

## 5. What the proof finds

- **F1.** The bindings pass through an order (L3). A set of clauses connected to the bound head positions, read as the prefix of every invocation, leaves mutually connected invocations each waiting on the other's magic tuples. The rewrite therefore reads an order and computes none of its own, which is NESTED §5.5's rule and SCOPE §4.1's one orderer.
- **F2.** Correctness is the order's independent of its choice (T1). What the order decides is the magic set's size (T2), and a clause outside the seed component placed before an invocation the seed component binds multiplies the magic set by an extent the engine cannot see.
- **F3.** The planner as it stands places a scan of a whole extent before an invocation with an available argument at two sites, the gate and the score, each sufficient (L7). The written order places the invocation first for the closure, so the rewrite's reading of the planner's order would adorn `bb` where the written order adorns `bf` (T4).
- **F4.** The same placement binds a top-level invocation after a scan of the attribute the scan reads, and seeds the magic set with the product (T5). The defect is the planner's, and the top level pays it with no rewrite reading an order.
- **F5.** The rank and the gate's condition exclude the product for every clause outside the seed component that has no visible selectivity (T3). A clause with a constant beyond its name outside the seed component may still precede the invocation. The product it makes is bounded by what its constants leave of its relation, the bet the planner makes for every clause with a constant (`scoreClause`'s doc).
- **F6.** Within a rank the score and the list order decide, and the list order on the algebra path is the decompiled order. Two orders that differ there give two adornments, each correct by T1, as NESTED §5.5 states.
- **F7.** A form whose header no available symbol binds runs over the unit relation (SCOPE §4.4), so it ranks with the scans and follows the pattern that binds its header, which is then its subject, and a flat `or-join` keeps the seek on the subject's values SCOPE §7 states.

## 6. The correction

The planner's selection within a phase takes a rank before the score.

- A clause without visible selectivity has rank zero. Every other selectable clause has rank one. The selection picks the highest rank and, within it, the highest score, the first in list order on a tie. Among the clauses of rank one the scores and the gates stand as they are.
- `invocationDependsOnPendingProvider` holds an invocation with no available variable argument behind any selectable provider of one of its arguments, as it does. It holds an invocation with an available variable argument behind a provider with visible selectivity alone.
- `subqueryDependsOnPendingProvider` holds a subquery behind a pending provider of one of its binding variables that is selectable, as it does, or that waits through the pending clauses on no output of the subquery. Under the rank a subquery reading an available input precedes a scan, so a provider the scan feeds is no longer placed ahead of the subquery by the score, and the gate reaches it through `pendingBlockers` as `patternDependsOnPendingExpression` reaches an expression. A provider waiting on the subquery's own output never holds it, so the phase advances.

Each contract below is the subject of one test:

- T1 under the written order, the rewritten set answering the Go reference over generated graphs: `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs` and `TestRules_ScopeShapesOnRandomGraphsMatchTheReference` (`datalog/storage`).
- The two closures of T4 are ordered on both planner paths, the recursion-first body's invocation before its pattern and the binder-after body's pattern before its invocation: `TestPlannedClausesPlaceABodysInvocationAgainstItsPatterns` (`datalog/planner/scope_test.go`).
- A scan of a whole extent is selected last: a pattern with an available variable before a three-variable scan, an invocation with an available argument before a scan providing its other argument, and an expression with available inputs before a scan: `TestCreatePhasesGreedy_AScanOfAWholeExtentIsSelectedLast` (`datalog/planner`).
- An invocation with an available argument waits on a provider with visible selectivity and on no scan, and one with no available argument waits on any provider: `TestCreatePhasesGreedy_AnInvocationWaitsOnAProviderByItsSelectivity` (`datalog/planner`).
- A subquery binding a variable a pending clause provides waits on that clause: `TestUncorrelatedSubqueryDefersBehindPendingBindingProviders` and `TestSubqueryDeferralNeverStallsOnGatedProvider` (`datalog/planner/subquery_scheduling_test.go`).
- A flat `or-join` follows the pattern that binds its header and seeks on that subject: `TestFlatOrJoinKeepsItsSubjectsPropertiesAndReplacesItsGroups` and `TestOuterTupleEventFiresPerSubjectTuple` (`datalog/storage/scope_contracts_test.go`).
- A top-level bound invocation followed by a scan is rewritten for its bound position alone: `TestRules_ABoundInvocationBeforeAScanIsRewrittenForItsBindingAlone` (`datalog/storage/rules_order_test.go`), reading the `rules/magic-sets` event's adornment.
