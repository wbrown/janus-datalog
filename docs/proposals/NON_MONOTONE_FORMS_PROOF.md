# What passes into a non-monotone form, as a proof

## Sources

- The code on the branch `rules-witnesses-and-speculative-values`: `datalog/planner/magic.go` (`Rewrite`, `rewritePredicate`, `rewriteClauses`, `rewriteClause`, `invokesStratumOf`, `magicRuleReads`, `adornInvocation`, `includeOriginal`), `datalog/query/rules.go` (`NewSeededRuleSet`, `stratify`, `invocationEdge.negative`, `invocationEdge.nonMonotone`, `CheckRequired`), `datalog/query/rule_errors.go` (`UnstratifiableError`, `NestedRecursionError`, `RequiredBindingError`) and `datalog/executor/rules.go` (`invocationSource`, `deriveInvocation`, `invocationEvaluation`, `magicSetsEvent`, `neededPredicates`, `runStratum`).
- The design: `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` (RULES) §1.2 and §1.3, `NESTED_SCOPES_AS_QUERIES.md` (NESTED) §2.4, §2.5, §5.4 and §5.5, `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §1 goals 2 and 6 and §4.2, and `REWRITE_ORDER_PROOF.md` §1, T1, T2 and L1.
- The literature: I. Balbin, G. S. Port, K. Ramamohanarao and K. Meenakshi, "Efficient bottom-up computation of queries on stratified databases", Journal of Logic Programming 11 (1991), which shows that passing bindings into a negated subgoal can leave the rewritten program unstratified, and restores stratification by what is passed.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **non-monotone form** F is a `not` or a `not-join`, which `invocationEdge.negative` names, or an `or-default`, an `or-default-join` or a subquery whose inner query aggregates, which `invocationEdge.nonMonotone` names.
- A **stratum** of a set is a strongly connected component of its dependency graph, which `stratify` finds with Tarjan's algorithm, so two predicates share a stratum when each reaches the other. The **constructor** is `NewSeededRuleSet`. It refuses a negative edge within a stratum (`stratify`, `UnstratifiableError`, naming the stratum's predicates) and a non-monotone edge within a stratum (`NestedRecursionError`, naming the rule, the predicate and the form). A set it builds is **accepted**.
- **p**, **r**, **S**, **β**, **H_B**: the rule r under rewrite derives p, S is p's stratum in the original set, β is p's binding pattern, and H_B is the head symbols at β's bound positions (`REWRITE_ORDER_PROOF.md` §1).
- A **site** σ is an invocation of q inside a non-monotone form F in r's body, under the planner's order of r's body as a scope and of F's branches under its header (NESTED §5.5). **c_k** is the first clause of that order that invokes, at any depth, a predicate of S (`invokesStratumOf`). **P_k** is the clauses before c_k that a magic rule reads (`magicRuleReads`: a clause that is not a negation or a fallback form, every mandatory correlate of which is bound before it). **B_σ** is the planned clauses of F's branch before σ, which pass into σ inside the form under every rule below (NESTED §5.5), read through the branch's own scope so its locals unify with nothing of r's body; by P1 none of them invokes a predicate of S.
- The three rules for what passes into F:
  - **R0**: the bindings and the clauses before F, whatever they invoke: `before` in `rewriteClauses`.
  - **R1**: the bindings and the clauses before c_k: `negated` in `rewriteClauses`, the passing sealed at c_k.
  - **R2**: nothing. The invocation keeps its name, and q's original rules join the set unchanged (`includeOriginal`).
- **M_i** is the rewritten set under rule i. **γ** is σ's adornment under the rule, and `adorned.q^γ` and `magic.q^γ` are the predicates it mints (`adornInvocation`).
- The **recursion returns at c_k** when the predicate c_k invokes, adorned as r's planned body adorns it, lies in `adorned.p^β`'s stratum of M. It returns when c_k invokes p under β itself, the recursion through one rule of `REWRITE_ORDER_PROOF.md` T4.
- The **decline**: an `UnstratifiableError` from `Rewrite`, on which `invocationEvaluation` evaluates p whole and reports `RulesMagicSets` with `applied` false, or, for a p with required positions, returns the refusal as the query's error. A `NestedRecursionError` is not declined: `Rewrite` returns it, and the query fails with it.
- **Evaluation at the clause**: an invocation of a predicate the context does not hold derives it at its clause (`invocationSource`, `deriveInvocation`), under the positions the scope binds there (`boundPositions`), through the rewrite for those bindings, as an evaluation of its own, continued across the windows of the form execution the clause stands in (`ruleContinuations`). A fixpoint holds the predicates the invoked one needs (`neededPredicates`), which leaves out a predicate with required positions. An adorned rule has no required run, since its guard binds every position its predicate requires (`rewritePredicate`), so the adorned copy of a required predicate is held and derived in the fixpoint.
- **E(q)**: the extent of q, every tuple its rules derive, the number `REWRITE_ORDER_PROOF.md` T2(c) says the engine cannot see.

## 2. Preconditions

- **P1.** The original set is accepted by `NewRuleSet`: it stratifies, and no rule invokes a predicate of its own stratum under a non-monotone form (NESTED §2.5).
- **P2.** By P1, q at a site is a predicate of a stratum below S.
- **P3.** `REWRITE_ORDER_PROOF.md` T1: the rewrite is correct under every order at every positive site. An evaluation at the clause is a rewrite of q over q's own rules, correct by the same theorem.
- **P4.** A required position of q at σ is bound by F's header or by a clause of F's branch, since NESTED §2.4 refuses the rest at build, and the planner orders that clause before σ (NESTED §2.5).

## 3. Theorems

**T1 (R0 is the defect).** Under R0, when the planner places c_k before F and the recursion returns at c_k, σ's magic rule reads c_k's adorned invocation, since `magicRuleReads` admits a monotone clause whose correlates are bound. So `magic.q^γ` depends on a predicate of `adorned.p^β`'s stratum, `adorned.q^γ` lies in that stratum, and σ's edge into F is an edge within it: a negative one refused by `stratify`, a non-monotone one refused by `NestedRecursionError`. R0 is therefore not a rule for what passes into F on that shape. It is the defect, and the written order carries it for a form written after the recursion. When the recursion does not return at c_k, R0 at σ is accepted and correct (L5), and it reads more than R1.

**T2 (R1 and R2 are correct).** M_2, and M_1 when it is accepted, derive `adorned.p^β` as exactly the tuples of p whose bound positions hold a seed tuple (L5, L6).

**T3 (R2 is always accepted; R1 is not).** M_2 is accepted whenever P1 holds (L3). M_1 is refused when (q, γ) has, at a positive site after c_k in some rule of S, a magic rule that reads S: then `adorned.q^γ` lies in `adorned.p^β`'s stratum (L2), and σ's edge is within it. The shape of `TestMagic_DeclinesUnstratifiableRewrite` is the instance: s invoked positively after the recursion in one rule of p and negated in another.

**T4 (the cost at a site).** Under R2 the work at σ is E(q) and the extents of q's callees, derived once in M's fixpoint. Under R1 it is the derivation of `adorned.q^γ` over `magic.q^γ`, which holds at most the seed's reach through P_k projected onto γ's bound positions, plus the guard's join per iteration. When γ binds no position, `query.CountBound` is zero, and R1 is R2 at that site. On a refusal R1 pays the decline, E(p) and the extent of everything below it, which exceeds R2's cost at every site of r, or, for a p with required positions, the query's failure.

**T5 (a predicate with required positions at a site).** Under R2, and at any site the rewrite leaves unadorned, q is not held (`neededPredicates`), so the invocation derives q at its clause (`deriveInvocation`): a rewrite of q for the positions the branch binds there and an evaluation of its own, once per execution of F. F executes once per run of r's body, which the fixpoint of S runs at every iteration (`runStratum`). P4 makes that evaluation answer. Under R1, when H_B, P_k and B_σ bind every required position of q, q is adorned, and `adorned.q^γ`, having no required run, is derived once in M's fixpoint over every seed. When the sealed passing and B_σ leave a required position free, a header variable bound only after c_k, `adornInvocation` refuses the rewrite through `CheckRequired` as `RequiredBindingError`, with the repair "bind it with a clause before the invocation", a repair P4 says the author has already made: a refusal that records the rewrite's limitation (SCOPE goal 6).

**T6 (the rule that dominates both).** Let **R1'** be, at every site under a non-monotone form: R1 when the sealed passing binds every required position of q and adorning σ does not place `adorned.q^γ` in `adorned.p^β`'s stratum, and otherwise the site left as R2 leaves it. Then M_1' is always accepted (L4), its cost at every site is the lesser of R1's and R2's (T4, T5), a predicate with required positions is derived once in M wherever R1 derives it and at its clause elsewhere (T5), and R1' refuses nothing `NewRuleSet` accepts.

## 4. Lemmas

### L1. A magic rule from a sealed site never reads S

P_k holds no clause that invokes a predicate of S at any depth, since c_k is the first such clause and `magicRuleReads` admits a clause only from the clauses before it, and B_σ holds none by P1. So the magic rule σ contributes to `magic.q^γ` depends on the guard `magic.p^β` and on predicates below S. σ alone never places `adorned.q^γ` in `adorned.p^β`'s stratum. ∎

### L2. Where `adorned.q^γ` reaches S

`adorned.q^γ` depends on S only through a magic rule of (q, γ) whose prefix holds an adorned invocation of a predicate of S. That is a positive invocation of q with the same adornment after c_k in some rule of S, or a chain of such sites through a predicate adorned the same way. By L1 no sealed site is such a rule. ∎

### L3. M_2 is accepted

Every non-monotone edge of M_2 runs from an adorned rule to an original predicate, since under R2 no invocation inside a non-monotone form is adorned. An original predicate's rules invoke original predicates alone, so its stratum in M_2 is its stratum in the original set, which by P1 holds no predicate of S. The edge leaves the stratum. Positive edges among adorned and magic predicates make no stratum unstratifiable. ∎

### L4. R1' terminates at an accepted set

Each round builds M under the current exclusions, and the constructor either accepts it or names a refused edge: a stratum holding a negative edge, or a rule, a predicate and a form. The refused edge runs from an adorned rule to an adorned predicate within one stratum under a non-monotone form, so it is a site under such a form that the round adorned, and that site joins the exclusions. Excluding a site removes its magic rule and its edge, replaces `adorned.q^γ` at σ with q, and adds q's original rules, whose edges run among original predicates and reach no adorned one. The next round's graph is the last one's less those edges, plus edges into original strata, which reach back to no adorned predicate. So no stratum grows, each round excludes a site adorned before it, and sites are finite. When every site under a non-monotone form is excluded, the set is M_2, which L3 accepts, so the loop ends at an accepted set at the latest there. ∎

### L5. R1 is correct at a site (T2)

Soundness: `adorned.q^γ` is contained in q, by `REWRITE_ORDER_PROOF.md` L1. Completeness: when the adorned rule of p evaluates F, the bindings q is asked with are the projection onto γ's bound positions of the join of the guard with every clause before F and with B_σ over the header. Those positions hold variables of H_B and of what P_k and B_σ provide. The join over every clause before F and B_σ projected onto those variables is contained in the join over the guard, P_k and B_σ projected onto them, since a natural join over more clauses restricts. That projection is `magic.q^γ`. So every binding q is asked with is in the magic set, and `adorned.q^γ` answers it as q does. For a negation, membership of the asked tuple in `adorned.q^γ` equals its membership in q. For a fallback, whether the branch has a result for the asked tuple is the same. For an aggregating inner query, the aggregate over the asked key reads every tuple of q at that key. The same containment holds for any set of monotone clauses before F a magic rule reads, so R0 is correct wherever it is accepted. ∎

### L6. R2 is correct at a site (T2)

The invocation inside F is the original q, and F is evaluated as the original rule evaluates it. A required q is derived at the clause from the positions the branch binds, correct by P3 and answering by P4. ∎

## 5. What the proof finds

- **F1.** R0 is the defect: the planner places a same-stratum invocation before a non-monotone form that reads its output, and the form's inner invocation is adorned from it (T1). The written order carries the same defect for a form written after the recursion.
- **F2.** R1 and R2 are both correct (T2). They differ in cost and in acceptance.
- **F3.** R2 buys acceptance at every site with the extent of every predicate under a non-monotone form, the number the engine cannot see (T4), and evaluates a required predicate at its clause once per execution of the form at every iteration of the stratum (T5).
- **F4.** R1 restricts every site to the seed's reach through the sealed prefix and derives a required predicate once in M (T4, T5). On the shapes of T3 it pays the decline, where it derives p whole and costs more than R2 at every site of the rule, and it refuses a required predicate whose position is bound after the recursion, which the evaluation at the clause answers (T5).
- **F5.** R1' pays the lesser at every site, is always accepted, and refuses nothing `NewRuleSet` accepts (T6). Under it the decline has no case.
- **F6.** The code and R1' differ, form by form:
  - `rewriteClause`'s negation and fallback arms implement R1, with the decline as a refusal's cost (`invocationEvaluation`).
  - `rewriteSubquery` implements R0 for an aggregating inner query.
  - `adornInvocation` refuses as `RequiredBindingError` a site under a non-monotone form whose sealed passing leaves a required position free, where R1' leaves the site unadorned (T5).
  - `Rewrite` returns the constructor's refusal, where R1' rebuilds with the refused site excluded (L4).
  - `NewSeededRuleSet` carries no refusal of a rule invoking a predicate with required positions under a negation, and the executor derives such a predicate at its clause (T5).
- **F7.** R1' seals at the original stratum's first invocation, R1's point. At a site where the recursion does not return at c_k, R0 is accepted and passes more (T1). A cascade of R0, R1 and R2 per site under L4's loop would take it, and is excluded: it differs from R1' only where the adornment the recursion returns under leaves `adorned.p^β`'s stratum, and no run, import or test names a failure on that shape, so a tier for it removes no failure.
- **F8.** NESTED §5.5 states that a fallback form is read as the union of its branches over its header, so that its outputs join the bound set. `magicRuleReads` reads an or-default-join as it reads any clause whose correlates the bound set holds, `rewriteClauses` appends the union over its header and its rewritten branches to the clauses a magic rule reads and adds its outputs to the bound set, and the magic rule stays monotone, since the union holds every tuple the form yields. `TestMagic_AFallbackFormIsReadAsTheUnionOfItsBranchesOverItsHeader` and the case `a fallback the recursion reads is read as the union of its branches over its header` of `TestMagic_MagicRuleReadsReadyMonotoneClauses` pin it. The fact is outside this proof's rule, which governs what passes into the form.

## 6. The contracts the tests pin

Each contract below is the subject of one test.

- A negation after the recursion whose predicate is also invoked positively after it is rewritten with the negated site unadorned, naming the original predicate, in a set the constructor accepts, and the query answers: `TestMagic_DeclinesUnstratifiableRewrite` (`datalog/planner`) respecified to the accepted set; `TestExecuteWithRelations_DeclinedRewriteEvaluatesWhole` and `TestExecuteWithRelations_DeclinedRewriteOfARequiredPredicateIsAnError` (`datalog/executor`) respecified to the bound evaluation's answer; the declined case of `TestRules_PredicateInvokingARequiredBindingPredicateEvaluatesWhole` (`datalog/storage`) respecified to the accepted rewrite.
- A fallback the planner places after the recursion is rewritten with its inner invocation adorned from the sealed passing, in a set the constructor accepts: the fallback case of `TestMagic_MagicRuleReadsReadyMonotoneClauses` (`datalog/planner`).
- An aggregating inner query invoking a lower predicate after the recursion is rewritten the same way: a case beside it.
- A predicate with required positions under a negation, its positions bound by the sealed passing, is adorned, and the query answers with one rewrite of that predicate: a case in `datalog/planner/magic_test.go` asserting the adorned invocation and its magic rule, and one in `datalog/storage/rules_test.go` asserting the answer and the one `RulesMagicSets` event for it.
- A predicate with required positions under a negation, its position bound only after the recursion, is left unadorned with its rules in the set, and the query answers: a case in `datalog/planner/magic_test.go` asserting the invocation's name and the rules, and one in `datalog/storage/rules_test.go` asserting the answer.
- A bound invocation's `RulesMagicSets` event carries the predicate and its adornment: `TestRules_ABoundInvocationBeforeAScanIsRewrittenForItsBindingAlone` (`datalog/storage`), reading the adornment.

## 7. The correction

R1' is the rule for every non-monotone form.

- `rewriteClause`: `not`, `not-join`, `or-default` and `or-default-join` walk their clauses under the sealed passing, as the negation arms do. `rewriteSubquery` walks an aggregating inner query under the sealed passing and a non-aggregating one under the passing before it, since the latter is monotone as a branch is (NESTED §2.5).
- `adornInvocation` at a site under a non-monotone form whose sealed passing leaves a required position of q free keeps the invocation and includes q's original rules, in place of `RequiredBindingError`. A positive site keeps the refusal (NESTED §5.5), since there the planned clauses before the invocation are the positions the executor binds at the clause.
- `Rewrite` builds the set, and on an `UnstratifiableError` or a `NestedRecursionError` adds to the excluded sites every site under a non-monotone form in the rules the refusal names whose adorned predicate the refusal names, and walks again. `adornInvocation` at an excluded site keeps the invocation and includes the original rules. The loop ends at an accepted set (L4).
- `invocationEvaluation`'s decline branch, the error it returns for a predicate with required positions, and `magicSetsEvent`'s `applied` and `reason` have no input and go, with the readers of `applied`.
- NESTED §5.5's bullets on the invocation under a non-monotone form and on a predicate with required positions under one, SCOPE §4.2's sentence on that invocation, RULES §1.3's paragraphs on the passing and the decline, and `docs/reference/RULES.md`'s sentences on the passing and the decline state R1' in place of R2 and the decline, and NESTED §1 carries no product choice on the negated invocation, since this proof derives it. SCOPE §7 carries §6's contracts, and SCOPE §10 names this record beside `REWRITE_ORDER_PROOF.md`.
