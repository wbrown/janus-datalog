# What passes into a non-monotone form, as a proof

## Sources

- The code: `datalog/planner/magic.go` (`Rewrite`, `refusedSites`, `rewritePredicate`, `rewriteClauses`, `rewriteClause`, `rewriteSubquery`, `invokesStratumOf`, `magicRuleReads`, `adornInvocation`, `includeOriginal`), `datalog/query/rules.go` (`NewSeededRuleSet`, `stratify`, `invocationEdge.negative`, `invocationEdge.nonMonotone`, `CheckRequired`), `datalog/query/rule_errors.go` (`UnstratifiableError`, `NestedRecursionError`, `RequiredBindingError`) and `datalog/executor/rules.go` (`invocationSource`, `deriveInvocation`, `invocationEvaluation`, `magicSetsEvent`, `neededPredicates`, `runStratum`).
- The design: `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` (RULES) §1.2 and §1.3, `NESTED_SCOPES_AS_QUERIES.md` (NESTED) §2.4, §2.5, §5.4 and §5.5, `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §1 goals 2 and 6 and §4.2, and `REWRITE_ORDER_PROOF.md` §1, T1, T2 and L1.
- The literature: I. Balbin, G. S. Port, K. Ramamohanarao and K. Meenakshi, "Efficient bottom-up computation of queries on stratified databases", Journal of Logic Programming 11 (1991). It shows that passing bindings into a negated subgoal can leave the rewritten program unstratified, and it restores stratification by limiting what is passed.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **non-monotone form** F is one of two kinds. It is a `not` or a `not-join`, which `invocationEdge.negative` names. Or it is an `or-default`, an `or-default-join` or a subquery whose inner query aggregates, which `invocationEdge.nonMonotone` names.
- A **stratum** of a set is a strongly connected component of its dependency graph. `stratify` finds the components with Tarjan's algorithm, so two predicates share a stratum when each reaches the other.
- The **constructor** is `NewSeededRuleSet`. It refuses a negative edge within a stratum as `UnstratifiableError`, naming the stratum's predicates (`stratify`). It refuses a non-monotone edge within a stratum as `NestedRecursionError`, naming the rule, the predicate and the form. A set it builds is **accepted**.
- **p**, **r**, **S**, **β**, **H_B**: the rule r under rewrite derives p. S is p's stratum in the original set. β is p's binding pattern. H_B is the head symbols at β's bound positions (`REWRITE_ORDER_PROOF.md` §1).
- A **site** σ is an invocation of q inside a non-monotone form F in r's body. Its position is the planner's: r's body is ordered as a scope, and F's branches are ordered under F's header (NESTED §5.5).
- **c_k** is the first clause of that order that invokes, at any depth, a predicate of S (`invokesStratumOf`).
- **P_k** is the clauses before c_k that a magic rule reads (`magicRuleReads`). Those are the clauses that are neither a negation nor a fallback form, every mandatory correlate of which is bound before the clause.
- **B_σ** is the planned clauses of F's branch before σ. They pass into σ inside the form under every rule below (NESTED §5.5). They are read through the branch's own scope, so their locals unify with nothing of r's body. By P1, none of them invokes a predicate of S.
- Three rules for what passes into F are compared:
  - **R0**: the bindings and the clauses before F, whatever they invoke. In `rewriteClauses` this is the passing named `before`.
  - **R1**: the bindings and the clauses before c_k. In `rewriteClauses` this is the passing named `negated`, sealed at c_k.
  - **R2**: nothing. The invocation keeps its name, and q's original rules join the set unchanged (`includeOriginal`).
- **M_i** is the rewritten set under rule i. **γ** is σ's adornment under the rule. `adorned.q^γ` and `magic.q^γ` are the predicates the adornment mints (`adornInvocation`).
- The **recursion returns at c_k** when the predicate c_k invokes, adorned as r's planned body adorns it, lies in `adorned.p^β`'s stratum of M. It returns when c_k invokes p under β itself, the recursion through one rule of `REWRITE_ORDER_PROOF.md` T4.
- **Evaluation at the clause** is what the executor does with an invocation of a predicate the context does not hold. `invocationSource` and `deriveInvocation` derive the predicate at its clause, under the positions the scope binds there (`boundPositions`), through the rewrite for those bindings. The derivation is an evaluation of its own, continued across the windows of the form execution the clause stands in (`ruleContinuations`). A fixpoint holds the predicates the invoked one needs (`neededPredicates`). It leaves out a predicate with required positions. An adorned rule has no required run, because its guard binds every position its predicate requires (`rewritePredicate`), so the adorned copy of a required predicate is held and derived in the fixpoint.
- **E(q)** is the extent of q, every tuple its rules derive. `REWRITE_ORDER_PROOF.md` T2(c) says the engine cannot see that number.

## 2. Preconditions

- **P1.** The original set is accepted by `NewRuleSet`. It stratifies, and no rule invokes a predicate of its own stratum under a non-monotone form (NESTED §2.5).
- **P2.** By P1, q at a site is a predicate of a stratum below S.
- **P3.** `REWRITE_ORDER_PROOF.md` T1: the rewrite is correct under every order at every positive site. An evaluation at the clause is a rewrite of q over q's own rules, correct by the same theorem.
- **P4.** A required position of q at σ is bound by F's header or by a clause of F's branch. NESTED §2.4 refuses the rest at build, and the planner orders that clause before σ (NESTED §2.5).

## 3. Theorems

**T1 (R0 is the defect).** Suppose the planner places c_k before F and the recursion returns at c_k. Under R0, σ's magic rule reads c_k's adorned invocation, since `magicRuleReads` admits a monotone clause whose correlates are bound. So `magic.q^γ` depends on a predicate of `adorned.p^β`'s stratum. Then `adorned.q^γ` lies in that stratum, and σ's edge into F is an edge within it. A negative edge there is refused by `stratify`, and a non-monotone one by `NestedRecursionError`. So R0 is not a rule for what passes into F on that shape. It is the defect, and the written order carries it for any form written after the recursion. When the recursion does not return at c_k, R0 at σ is accepted and correct (L5), and it reads more than R1.

**T2 (R1 and R2 are correct).** M_2, and M_1 when it is accepted, derive `adorned.p^β` as exactly the tuples of p whose bound positions hold a seed tuple (L5, L6).

**T3 (R2 is always accepted; R1 is not).** M_2 is accepted whenever P1 holds (L3). M_1 is refused when (q, γ) has, at a positive site after c_k in some rule of S, a magic rule that reads S. Then `adorned.q^γ` lies in `adorned.p^β`'s stratum (L2), and σ's edge is within it. The shape of `TestMagic_ANegatedSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned` is the instance: s is invoked positively after the recursion in one rule of p, and negated in another.

**T4 (the cost at a site).** Under R2 the work at σ is E(q) plus the extents of q's callees, derived once in M's fixpoint. Under R1 it is the derivation of `adorned.q^γ` over `magic.q^γ`. That magic set holds at most the seed's reach through P_k, projected onto γ's bound positions, plus the guard's join per iteration. When γ binds no position, `query.CountBound` is zero, and R1 is R2 at that site. When M_1 is refused, R1 has no accepted set. An engine under R1 alone then either fails the query or derives p whole, at E(p) plus the extent of everything below it, which exceeds R2's cost at every site of r.

**T5 (a predicate with required positions at a site).** Under R2, and at any site the rewrite leaves unadorned, q is not held (`neededPredicates`). So the invocation derives q at its clause (`deriveInvocation`): a rewrite of q for the positions the branch binds there, and an evaluation of its own, once per execution of F. F executes once per run of r's body, and the fixpoint of S runs that body at every iteration (`runStratum`). P4 makes that evaluation answer. Under R1, when H_B, P_k and B_σ together bind every required position of q, q is adorned. `adorned.q^γ` has no required run, so it is derived once in M's fixpoint over every seed. When the sealed passing and B_σ leave a required position free, which happens when a header variable is bound only after c_k, R1 alone has no adornment for the site. The only answer R1 alone gives is a refusal, which records the rewrite's limitation as the query's error (SCOPE goal 6), for a query P4 says the author wrote correctly.

**T6 (the rule that dominates both).** Let **R1'** be this rule at every site under a non-monotone form. The site takes R1 when the sealed passing binds every required position of q and adorning σ does not place `adorned.q^γ` in `adorned.p^β`'s stratum. Otherwise the site is left as R2 leaves it. Then M_1' is always accepted (L4). Its cost at every site is the lesser of R1's and R2's (T4, T5). A predicate with required positions is derived once in M wherever R1 derives it, and at its clause elsewhere (T5). R1' refuses nothing `NewRuleSet` accepts.

## 4. Lemmas

### L1. A magic rule from a sealed site never reads S

P_k holds no clause that invokes a predicate of S at any depth. c_k is the first such clause, and `magicRuleReads` admits a clause only from the clauses before it. B_σ holds none by P1. So the magic rule σ contributes to `magic.q^γ` depends on the guard `magic.p^β` and on predicates below S. σ alone never places `adorned.q^γ` in `adorned.p^β`'s stratum. ∎

### L2. Where `adorned.q^γ` reaches S

`adorned.q^γ` depends on S only through a magic rule of (q, γ) whose prefix holds an adorned invocation of a predicate of S. That is a positive invocation of q with the same adornment after c_k in some rule of S, or a chain of such sites through a predicate adorned the same way. By L1 no sealed site is such a rule. ∎

### L3. M_2 is accepted

Every non-monotone edge of M_2 runs from an adorned rule to an original predicate, since under R2 no invocation inside a non-monotone form is adorned. An original predicate's rules invoke original predicates alone. So its stratum in M_2 is its stratum in the original set, which by P1 holds no predicate of S. The edge leaves the stratum. Positive edges among adorned and magic predicates make no stratum unstratifiable. ∎

### L4. R1' terminates at an accepted set

Each round builds M under the current exclusions. The constructor either accepts it or names a refused edge: a stratum holding a negative edge, or a rule, a predicate and a form. The refused edge runs from an adorned rule to an adorned predicate within one stratum, under a non-monotone form. So it is a site under such a form that the round adorned, and that site joins the exclusions. Excluding a site removes its magic rule and its edge. It replaces `adorned.q^γ` at σ with q and adds q's original rules, whose edges run among original predicates and reach no adorned one. The next round's graph is the last one's less those edges, plus edges into original strata, which reach back to no adorned predicate. So no stratum grows, each round excludes a site adorned before it, and sites are finite. When every site under a non-monotone form is excluded, the set is M_2, which L3 accepts. So the loop ends at an accepted set, at the latest there. ∎

### L5. R1 is correct at a site (T2)

Soundness: `adorned.q^γ` is contained in q, by `REWRITE_ORDER_PROOF.md` L1.

Completeness: when the adorned rule of p evaluates F, the bindings q is asked with are the projection, onto γ's bound positions, of the join of the guard with every clause before F and with B_σ over the header. Those positions hold variables of H_B and of what P_k and B_σ provide. The join over every clause before F and B_σ, projected onto those variables, is contained in the join over the guard, P_k and B_σ, projected onto them, since a natural join over more clauses restricts. That second projection is `magic.q^γ`. So every binding q is asked with is in the magic set, and `adorned.q^γ` answers it as q does. For a negation, membership of the asked tuple in `adorned.q^γ` equals its membership in q. For a fallback, whether the branch has a result for the asked tuple is the same. For an aggregating inner query, the aggregate over the asked key reads every tuple of q at that key. The same containment holds for any set of monotone clauses before F a magic rule reads, so R0 is correct wherever it is accepted. ∎

### L6. R2 is correct at a site (T2)

The invocation inside F is the original q, and F is evaluated as the original rule evaluates it. A required q is derived at the clause from the positions the branch binds. That is correct by P3, and it answers by P4. ∎

## 5. What the proof finds

- **F1.** R0 is the defect. The planner places a same-stratum invocation before a non-monotone form that reads its output, and the form's inner invocation is adorned from it (T1). The written order carries the same defect for a form written after the recursion.
- **F2.** R1 and R2 are both correct (T2). They differ in cost and in acceptance.
- **F3.** R2 buys acceptance at every site. It pays the extent of every predicate under a non-monotone form, the number the engine cannot see (T4). It evaluates a required predicate at its clause once per execution of the form, at every iteration of the stratum (T5).
- **F4.** R1 restricts every site to the seed's reach through the sealed prefix. It derives a required predicate once in M (T4, T5). On the shapes of T3 it has no accepted set. It has no adornment for a required predicate whose position is bound after the recursion, which the evaluation at the clause answers (T5).
- **F5.** R1' pays the lesser at every site. It is always accepted. It refuses nothing `NewRuleSet` accepts (T6).
- **F6.** The code implements R1'. §7 states how.
- **F7.** R1' seals at the original stratum's first invocation, R1's point. At a site where the recursion does not return at c_k, R0 is accepted and passes more (T1). A cascade of R0, R1 and R2 per site under L4's loop would take that. It is excluded. It differs from R1' only where the adornment the recursion returns under leaves `adorned.p^β`'s stratum, and no run, import or test names a failure on that shape, so a tier for it removes no failure.
- **F8.** NESTED §5.5 states that a fallback form is read as the union of its branches over its header, so that its outputs join the bound set. `magicRuleReads` reads an or-default-join as it reads any clause whose correlates the bound set holds. `rewriteClauses` appends the union over its header and its rewritten branches to the clauses a magic rule reads, and adds its outputs to the bound set. The magic rule stays monotone, since the union holds every tuple the form yields. `TestMagic_AFallbackFormIsReadAsTheUnionOfItsBranchesOverItsHeader` and the case "a fallback the recursion reads is read as the union of its branches over its header" of `TestMagic_MagicRuleReadsReadyMonotoneClauses` pin it. The fact is outside this proof's rule, which governs what passes into the form.

## 6. The contracts the tests pin

Each contract below is the subject of one test.

- A negation after the recursion, whose predicate is also invoked positively after it, is rewritten with the negated site unadorned. The site names the original predicate, the constructor accepts the set, and the query answers. `TestMagic_ANegatedSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned` (`datalog/planner`) pins the set. `TestExecuteWithRelations_ANegatedSiteLeftUnadornedAnswers` and `TestExecuteWithRelations_ARequiredPredicateWithANegatedSiteLeftUnadornedAnswers` (`datalog/executor`) pin the bound evaluation's answer. `TestRules_PredicateInvokingARequiredBindingPredicateEvaluatesWhole` (`datalog/storage`) pins the accepted rewrite's answer.
- A fallback the planner places after the recursion is rewritten with its inner invocation adorned from the sealed passing, in a set the constructor accepts: the fallback case of `TestMagic_MagicRuleReadsReadyMonotoneClauses` (`datalog/planner`). A fallback site whose adornment would not stratify is left unadorned: `TestMagic_AFallbackSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned` (`datalog/planner`).
- An aggregating inner query invoking a lower predicate after the recursion is rewritten the same way: a case beside the fallback case.
- A predicate with required positions under a negation, its positions bound by the sealed passing, is adorned, and the query answers with one rewrite of that predicate. A case in `datalog/planner/magic_test.go` asserts the adorned invocation and its magic rule. A case in `datalog/storage/rules_test.go` asserts the answer and the one `RulesMagicSets` event for it.
- A predicate with required positions under a negation, its position bound only after the recursion, is left unadorned with its rules in the set, and the query answers. A case in `datalog/planner/magic_test.go` asserts the invocation's name and the rules. A case in `datalog/storage/rules_test.go` asserts the answer.
- A bound invocation's `RulesMagicSets` event carries the predicate and its adornment: `TestRules_ABoundInvocationBeforeAScanIsRewrittenForItsBindingAlone` (`datalog/storage`), reading the adornment.

## 7. The implementation

R1' is the rule for every non-monotone form. The code implements it in four places.

`rewriteClauses` walks a scope's clauses in the planner's order under two passings, `before` and `negated`. `before` is R0's passing. At the first clause that invokes a predicate of the rule's own stratum (`invokesStratumOf`), the walk copies `before` into `negated` and marks the passing sealed. From then on `negated` stays at R1's point while `before` keeps growing.

`rewriteClause` chooses the passing by the form. A `not-join` body, an `or-default-join`'s branches and an aggregating subquery's inner query walk under the sealed passing alone, marked non-monotone, and the walk inside them starts unsealed, since the sealed passing holds nothing of the rule's stratum. An `or-join`'s branches and a non-aggregating subquery's inner query walk under both passings, as the body does. Every form's branches are planned under the form's header before they are walked, so the clauses before a site are the planner's.

`adornInvocation` decides the site. A position is bound when it holds a constant, a variable the passing binds, or a variable standing for a constant the call passes. When the adornment leaves a required position free, `CheckRequired` refuses. At a positive site that refusal is `RequiredBindingError`. At a site under a non-monotone form the invocation stands and its predicate's original rules join the set (`includeOriginal`). The same happens with no bound position, and at a site under a non-monotone form that a refusal of the set has excluded. Otherwise the invocation names the adorned predicate, the site is recorded for a refusal to name, and the magic rule is emitted.

`Rewrite` builds the set through `NewSeededRuleSet`. When the constructor refuses it with `UnstratifiableError` or `NestedRecursionError`, `refusedSites` names every recorded site under a non-monotone form that the refusal covers. Under an `UnstratifiableError` those are the sites whose rule and adorned predicate both lie in the refused stratum. Under a `NestedRecursionError` they are the sites in the rule it names that adorn the predicate it names. `Rewrite` adds them to the excluded set and walks again. L4 says the loop ends at an accepted set. Any other error names no site and is returned as it is.

`NewSeededRuleSet` carries no refusal of a rule that invokes a predicate with required positions under a negation. The executor derives such a predicate at its clause (T5).
