# A predicate derived whole is read whole, as a proof

## Sources

- The code: `datalog/planner/magic.go` (`Rewrite`, `magicRewriter`, `magicRewriter.enqueue`, `magicRewriter.includeOriginal`, `rewritePredicate`, `adornInvocation`, `excludedSite`, `refusedSites`), `datalog/planner/rule_bodies.go` (`EvaluatedSet`, `PlanEvaluation`, `NeededStrata`, `neededPredicates`), `datalog/query/rules.go` (`NewSeededRuleSet`, `stratify`, `RuleSet.ReachesRequired`, `RuleSet.CheckRequired`), `datalog/executor/rules.go` (`deriveInvocation`, `invocationSource`, `ruleEvaluation.extend`, `seedRelation`).
- The proofs this one stands on: `REWRITE_ORDER_PROOF.md` (§1 the magic rule and the adorned rule, T1 correctness, L1 soundness, L2 completeness); `NON_MONOTONE_FORMS_PROOF.md` (§1 the rule R2, L3 the original predicates' strata, L5 the asked bindings lie in the magic set, L6 a site under R2, T5 a predicate with required positions); `A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md` (T3 a join into a held relation probes the group's tuples and drains nothing).
- The design: `docs/reference/RULES.md`, "Evaluation", which states that each predicate the bindings reach gets a copy named for the pattern, and that an invocation left with its original name reads the predicate derived whole in its own stratum; `NESTED_SCOPES_AS_QUERIES.md` §5.5; `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3.
- The failure: the bound `unfilled` query of the downstream report `docs/plans/JANUS_REPRO_FILLED.md` in `janus-repro-filled-2026-10-30.tar.gz`. Its rewritten set derives several predicates whole, for the negations and the free invocation that keep their original rules in the set, and derives each of them again under one or more adornments inside the recursive stratum.
- The decision: a predicate the evaluation derives whole is read whole at every invocation of it, and the rewrite emits no adorned copy and no magic predicate for it. When the invoked predicate itself is derived whole, the invocation evaluates the set as given.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **M**, the **rewrite** for the invocation of r under β, the **adorned rule**, the **magic rule**, the **guard** and the **seed** are as `REWRITE_ORDER_PROOF.md` §1 defines them. The **root** of M is `adorned.β/r`.
- A **kept site** is an invocation `adornInvocation` leaves under its own name: a free invocation of a predicate reaching no required predicate, a site under a non-monotone form the sealed passing binds short of a required position, and an excluded site. At each, `includeOriginal` adds the predicate's own rules and the rules of every predicate they invoke, unchanged.
- The **needed predicates** of M are `neededPredicates(M, root)`: the root and, transitively, every predicate a needed predicate's rules invoke, a predicate with required positions excluded and not walked through.
- **W(M)**, the **whole predicates**, are the needed predicates of M that are original predicates, invoked under their own names. Each is derived whole in its own stratum by the evaluation (`NeededStrata`).
- A **whole site** is a bound invocation, in a rule M emits, of a predicate of W(M), which the rewrite as it stands adorns.
- The **set as given** is the rule set the query passed, which `EvaluatedSet` evaluates for a free invocation of a predicate reaching no required predicate.

## 2. Preconditions

- **P1 (what M holds).** `rewritePredicate` emits the adorned rules of each predicate on the worklist and the magic rules of their bodies' bound sites, `adornInvocation` enqueues a predicate at each bound site, `includeOriginal` adds original rules, and nothing else enters M.
- **P2 (the originals reach originals).** A rule `includeOriginal` adds invokes original names alone. So an original predicate's stratum in M is its stratum in the set as given, and it reaches no adorned predicate (`NON_MONOTONE_FORMS_PROOF.md` L3).
- **P3 (the magic set covers a site's bindings).** At a bound site of q in a body of p, the bindings the body presents at q's bound positions are the projection of the guard joined with every clause before q, which is contained in `magic.q^γ`, the projection of the guard joined with the clauses the magic rule reads (`REWRITE_ORDER_PROOF.md` L2, `NON_MONOTONE_FORMS_PROOF.md` L5).
- **P4 (the adorned relation).** `adorned.q^γ` holds exactly the tuples of q whose γ-bound positions hold a tuple of `magic.q^γ` (`REWRITE_ORDER_PROOF.md` T1).
- **P5 (the cost of a read).** A join into a held relation probes the group's tuples and drains nothing of the relation, whatever the relation holds (`A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md` T3).
- **P6 (the free root).** `EvaluatedSet` evaluates the set as given for a free invocation of a predicate reaching no required predicate, and the evaluation then derives the needed strata whole and joins the predicate's relation with the group at the collapse. A whole evaluation under the windowed route runs at the first window and nothing at a later one.
- **P7 (the exclusion loop).** `Rewrite` builds M through `NewSeededRuleSet`, excludes the sites a refusal names, and walks again, and the loop ends at an accepted set (`NON_MONOTONE_FORMS_PROOF.md` L4).

## 3. Theorems

**T1 (answers unchanged).** With every whole site reading its predicate under its own name, every rule of M derives what it derived, every magic rule reading a whole site derives what it derived, and the invocation answers the tuples it answered.

**T2 (the set is accepted).** M with its whole sites reading by name and without the adorned and magic rules of the predicates of W(M) is accepted by `NewSeededRuleSet` whenever M was.

**T3 (less work).** The evaluation derives every relation it derived less the adorned copies and the magic predicates of the predicates of W(M), and reads each whole site at the cost the adorned read had.

**T4 (the root).** When r is in W(M), the evaluation of the set as given answers the invocation's tuples.

**T5 (nothing else changes).** A site of a predicate outside W(M) is adorned as it was, and a predicate with required positions is never in W(M).

## 4. Lemmas

### L1. A whole site answers the same (T1)

Let σ be a whole site of q in a body of p with adornment γ. Under the rewrite as it stands σ reads `adorned.q^γ`, and under this proof q. The body's join at σ presents, at γ's bound positions, bindings in `magic.q^γ` (P3). A tuple of q matching the group at σ lies at such a binding, so it is in `adorned.q^γ` (P4), and a tuple of `adorned.q^γ` is a tuple of q (`REWRITE_ORDER_PROOF.md` L1). So the join of the group with `adorned.q^γ` at σ equals the join of the group with q at σ. The same holds at a site under a negation or a fallback, where membership at the asked tuple is what is read (`NON_MONOTONE_FORMS_PROOF.md` L5, L6), and for a magic rule reading σ among the clauses before a later invocation. ∎

### L2. The strata (T1)

M's strata run in order, and the whole predicates are derived whole in strata below every adorned predicate that reads them (P2). In an adorned predicate's rules each site reads what it read, by L1 at a whole site and unchanged elsewhere, so each rule derives what it derived and each stratum its sets. The root's relation, joined with the group under the seed, is unchanged. ∎

### L3. Acceptance (T2)

The dependency edges of the new set are M's less those into the adorned and magic predicates of W(M)'s predicates, plus edges from the rules holding the whole sites into the predicates of W(M) by name. An edge into an original predicate reaches a stratum that reaches no adorned predicate (P2), so no new edge lies on a cycle with an adorned one. Removing rules removes edges. A negative or non-monotone edge within a stratum of the new set would be within a stratum of M, which M had none of. So the constructor accepts the set. When M is itself refused, the exclusion loop runs as before (P7), the whole predicates computed on each accepted candidate. ∎

### L4. The work (T3)

The adorned and magic predicates of W(M)'s predicates are not emitted, so their strata, their bodies and the joins those bodies made are not run. Every other body runs as it ran, and each whole site's join probes the group's tuples as the adorned read's did (P5). ∎

### L5. The root (T4)

Let r be in W(M). Its original rules and those of every predicate they reach are in M and needed, and by L2 they derive r whole in its own stratum. The invocation's answer under M is `adorned.β/r` joined with the group at the bound positions, the seed being the group's projection onto them (`seedRelation`), which by L1's argument at the top level equals r joined with the group. The set as given holds the same original rules, so its evaluation derives the same r and the collapse joins it with the group (P6). ∎

### L6. Nothing else (T5)

A site whose predicate is not in W(M) is adorned by `adornInvocation` as before. `neededPredicates` excludes a predicate with required positions and walks nothing through it, so no such predicate is in W(M), and its sites are adorned as before, with its adorned copy derived once in M where the sealed passing binds its positions (`NON_MONOTONE_FORMS_PROOF.md` T5). ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, each written before the code and run red before it where a red exists.

- `TestMagic_AnInvocationOfAPredicateDerivedWholeReadsItWhole` (planner, new): over `[(p ?x ?z) [?x :page/next ?y] (s ?y ?z) (r ?k)] [(r ?k) [?k :page/kind "hub"] (s ?k _)] [(s ?a ?b) [?a :page/next ?b]]` rewritten for `p` under its first position bound, the set holds `adorned.bf/p` whose site of `s` reads `s`, the original rules of `r` and `s`, and no adorned or magic predicate of `s` (T1, T2, T5, L1).
- `TestMagic_ARootDerivedWholeEvaluatesTheSetAsGiven` (planner, new): over `[(p ?x ?y) [?x :page/next ?y] (q ?a ?b)] [(q ?a ?b) (p ?a ?b)]` for `p` under its first position bound, `Rewrite` answers the set as given with `p` as the predicate and no seed, and `PlanEvaluation` reports no rewrite (T4, L5).
- `TestRules_ABoundInvocationOfAPredicateDerivedWholeDerivesNoAdornedCopy` (storage, new): the query over the first set, bound at `p`'s first position, answers what the free query answers at that binding, and its stratum begin events name no adorned copy of `s` (T1, T3).
- `TestMagic_ANegatedSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned`, `TestMagic_AFallbackSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned` and `TestMagic_AFreeSiteUnderAFallbackWhoseAllFreeNameWouldNotStratifyKeepsTheOriginalRules` (planner): each keeps a predicate's original rules at one site and adorns the same predicate at another, so under the decision the other site reads the predicate by name and the expected set holds no adorned copy of it (T1, T5). Every other rewrite test whose expected set holds an adorned copy of a predicate beside that predicate's original rules is of the same kind.
- `TestMagic_AnAggregatingSubqueryBeforeTheRecursionIsNotRead` and `TestMagic_ASubqueryWhoseReadNoLongerBindsItsFindIsNotRead` (planner): each keeps the root's own recursive site under its name, so under the decision the root is whole and `Rewrite` answers the set as given (T4), which leaves no rewritten set for the assertion on the subquery's inner invocation. Each moves the subquery and the invocation after it into a non-recursive root invoking a separate recursive predicate whose rules do not read the inner invocation's predicate, so the inner invocation stays adorned from the clauses before the form, the invocation after the subquery keeps its predicate's name and original rules, that predicate is whole, and the root is rewritten (T1, T5).
- `TestRules_TheKeptSiteFixturesAnswerTheReferenceUnderEveryBinding` (storage, new): the five planner fixtures above, over a fixed graph and random graphs, with the root invoked free, bound at either position and at both, under a pattern keyed on either position and on both, twice in a chain, and under a not-join, each answering a reference computed outside the engine (T1, T4).
- `TestMagic_UnboundInvocationKeepsOriginalRules`, `TestMagic_AFreeRootAdornsTheRequiredPredicatesItReaches` and `TestMagic_ARequiredPredicateUnderANegation` (planner), run unchanged (T5, L6).
- `TestRules_PredicateInvokingARequiredBindingPredicateEvaluatesWhole`, `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs`, `TestRules_ScopeShapesOnRandomGraphsMatchTheReference` and `TestQueriesAnswerAlikeUnderBothOptimizerModes` (storage), run unchanged (T1).
- The downstream reproduction measures the change, and its figures go to `PERFORMANCE_STATUS.md`.

## 6. The change the proof licenses

- `datalog/planner/magic.go`, `Rewrite`: after a walk, the whole predicates are read from the candidate set as `neededPredicates` finds them; when an enqueued adorned predicate's original is among them, the walk runs again with those predicates marked whole, and the loop ends when no enqueued predicate's original is whole, which L3 bounds beside the exclusion loop. When r is whole, `Rewrite` answers the set as given with r as the predicate and no seed.
- `datalog/planner/magic.go`, `adornInvocation`: a site of a predicate marked whole keeps the invocation and includes the original rules, at a bound position as at a free one.
- `docs/reference/RULES.md`, "Evaluation": each predicate the bindings reach gets a copy named for the pattern, unless the evaluation derives it whole, in which case every invocation reads it whole.
- `NESTED_SCOPES_AS_QUERIES.md` §5.5 and `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3: the same sentence where each states what the bindings reach.
- The tests of §5.

## 7. What the proof does not cover

- The cost of the whole predicates' derivation, which the evaluation pays with or without the decision.
- A predicate with required positions kept at a site under a non-monotone form, which the evaluation derives at its clause per run of the body (`NON_MONOTONE_FORMS_PROOF.md` T5), and whose adorned copies elsewhere stand (T5).
- A root in W(M) whose rules reach a predicate with required positions: the set as given derives that predicate at its clause, as M's original rules did.

## 8. Alternatives excluded

- The rewrite as it stands: a predicate derived whole for one site is derived again adorned for another, the failure.
- The adorned copy kept and the original rules dropped: the kept sites read the original under a negation or a fallback, where the adorned copy's magic rule would depend on the adorned predicate through the form (`NON_MONOTONE_FORMS_PROOF.md` T1), and at a free site, where no binding seeds a copy.
- The whole predicates read from the first walk's `includeOriginal` set alone: it holds predicates the evaluation never derives, those reached only through a predicate with required positions, which are not held and are derived at their clauses.
