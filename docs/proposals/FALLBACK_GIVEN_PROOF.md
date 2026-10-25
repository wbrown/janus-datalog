# What a fallback's branch is given, as a proof

## Sources

- The code: `datalog/planner/framing.go` (`GivenSymbols`), `datalog/planner/scope_planning.go` (`planFormScope`), `datalog/planner/magic.go` (`rewriteClause`, `passing.within`, `adornInvocation`, `magicRuleReads`, `readUnion`), `datalog/executor/or_fallback_relation.go` (`NewOrFallbackRelation`), `datalog/executor/rules.go` (`invocationSource`, `deriveInvocation`) and `datalog/query` (`InferHeaders`).
- The design: `NESTED_SCOPES_AS_QUERIES.md` (NESTED) §2.1, §2.2, §2.3, §5.4 and §5.5, `SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §1 goal 9, §2.1, §4.2, §4.3, §4.4 and §7, `NON_MONOTONE_FORMS_PROOF.md` (NMF) §1, L5 and T6, and `REWRITE_ORDER_PROOF.md` T1 and L1 as NMF cites them.

## 1. Definitions

- **F** is a fallback form in rule r's body, `(or-default-join [[R] O] B_1 … B_n)`, with required run R and outputs O. A headerless `or-default` is F after `InferHeaders`: a shared variable every branch binds is in O, and every other shared variable is in R (NESTED §2.3). A **flat header** has R = ∅.
- **S** is the relation the clauses before F produce in r's body under r's head bindings. S binds a variable when every tuple of S carries it. **O_S** = O ∩ vars(S) is the outputs S carries, and **O_S(ρ)** is the values of O_S that S pairs with ρ.
- A **run tuple** ρ is a tuple of S projected onto R. With R = ∅ it is the unit tuple, one for the whole of S (SCOPE §4.2).
- **B_i(ρ)** is branch i evaluated over ρ alone: R bound to ρ and nothing else of S bound (NESTED §2.2, SCOPE §4.3).
- **F(ρ)** = B_j(ρ) for the least j with B_j(ρ) ≠ ∅, and ∅ when none. F's contribution to r is S ⋈ ⋃_ρ ({ρ} × F(ρ)), the join on R ∪ O_S taken after the decision (SCOPE §4.3).
- **G** is the set of variables a branch is given: the variables bound inside the branch from outside it. The executor's G is `GivenSymbols(R, O_S, true)` = R (`NewOrFallbackRelation`, `planFormScope`). The rewrite's G is the set `rewriteClause` passes to `within`, which binds inside the branch each symbol of it the passing binds.
- **σ** is an invocation of q at a clause of B_i, and **B_σ** is B_i's planned clauses before σ. **γ** is σ's adornment: position j is bound when its argument is a constant, a variable of G the passing binds, or a variable B_σ provides (`adornInvocation`). `adorned.q^γ` and `magic.q^γ` are the predicates the adornment mints, and **r^β** is r rewritten for its binding β (NMF §1).
- **B_i^γ(ρ)** is branch i of r^β evaluated over ρ. Inside it σ names `adorned.q^γ`, a predicate the context holds, so the invocation reads that relation with the positions the branch binds matched and every other position free (SCOPE §4.4, `invocationSource`).
- **E(q)** is the extent of q.

## 2. Preconditions

- **P1.** The original set is accepted, and the rewrite is correct at every positive site under every order (REWRITE_ORDER_PROOF T1).
- **P2.** `adorned.q^γ` ⊆ q, and `adorned.q^γ` holds every tuple of q whose γ-bound positions hold a tuple of `magic.q^γ` (REWRITE_ORDER_PROOF L1, NMF L5).
- **P3.** `magic.q^γ` holds the projection, onto γ's bound positions, of the join of the guard with the clauses before F and with B_σ over G (NMF L5, whose join is over what F gives its branch).
- **P4.** The executor evaluates F as §1 defines it: a branch is given R alone, the decision is taken on B_i(ρ) before the join on O_S, and a flat header is decided once over the unit tuple (SCOPE §4.2, §4.3, §7).

## 3. Theorems

**T1 (a flipped decision is a derived tuple).** Let D be a database on which, for some ρ of r^β's run, B_1^γ(ρ) = ∅ while B_1(ρ) ≠ ∅, no tuple of B_1(ρ) agrees with S on O_S, and some tuple of B_2(ρ) does. Then r^β derives on D a tuple r does not. Proof. On D, F^γ(ρ) = B_2^γ(ρ) where F(ρ) = B_1(ρ). The tuple of B_2(ρ) that agrees with S on O_S survives the join of §1 and yields a tuple of r^β through ρ, provided B_2^γ(ρ) holds it, which it does when σ stands in B_1 alone. Through ρ, r yields only tuples of B_1(ρ) that agree with S on O_S, of which there are none. ∎

**T2 (an output is never a bound position).** If γ binds a position of σ whose variable is an output o ∈ O_S, there is a database on which r^β derives a tuple r does not. Proof. Let σ stand first in B_1. By P3 the o-values of `magic.q^γ` beside ρ are O_S(ρ). Inside the branch o is free (§1), so by P2 B_1^γ(ρ) = {t ∈ B_1(ρ) : t.o ∈ O_S(ρ)}. L3 is a database on which B_1(ρ) ≠ ∅, no tuple of it has t.o ∈ O_S(ρ), and B_2(ρ) holds a tuple that does. On it B_1^γ(ρ) = ∅, and T1 applies. ∎

**T3 (the required run is the greatest sound set).** With G = R, B_i^γ(ρ) = B_i(ρ) for every ρ of r^β's run, so r^β is sound and complete. Proof. At ρ, q is asked inside B_i with R's positions holding ρ's values and B_σ's positions holding what B_σ provides at ρ, every other position free. ρ ranges over S restricted by the guard, so by P3 every asked binding is a tuple of `magic.q^γ`, and by P2 `adorned.q^γ` answers it as q does. The free positions are unrestricted. So B_i^γ(ρ) = B_i(ρ), the decision is r's, and the join on O_S after it is r's; with P1 at the positive sites, r^β derives what r derives and nothing more. A bound position beyond R and what B_σ provides is a variable of O_S or a branch local. A local is never bound by the passing (L1), and an output is unsound (T2). So R with what B_σ provides is the greatest set a sound adornment binds. ∎

**T4 (a flat header gives nothing).** With R = ∅, G = ∅: a site whose B_σ provides none of its variables is adorned at no position, keeps q's original rules or is named all free when q reaches a required predicate (`adornInvocation`), and the executor derives q whole at the clause. The free evaluation pays E(q) at the same place, since a form given nothing runs its branch once over the unit tuple (SCOPE §4.2), and the decision reads whether that whole result is empty, which no restriction of q to S's values preserves (T2, with O_S(ρ) the whole of S's output values under the unit tuple). So no sound adornment restricts σ, and the rewrite adds no cost the free evaluation does not pay. ∎

**T5 (the union is a different fact).** Under `(or-join [[R] O] B_1 … B_n)` the form's answer is ⋃_i (S ⋈ ({ρ} × B_i(ρ))) on R ∪ O_S, with no decision. Join distributes over union, so B_i restricted to O_S(ρ) at ρ answers the same join (SCOPE §4.4, the monotone branch's seek). So G = R ∪ O_S is sound under the union and tighter than R, and the two arms of `GivenSymbols` are forced by the forms: the fallback's decision makes O_S unsound (T2), and the union's absence of one makes it sound. ∎

**T6 (the header decides the key).** Which variables are in R is the text's, under `InferHeaders` for a headerless form, and never the rewrite's. In `[?a :page/next ?b] (or-default (marked ?b ?d) [?b :page/kind ?d])` both branches bind `?b` and `?d`, so both are outputs, R = ∅, and the decision is taken once over `marked` whole; a decision per `?b` is written `(or-default-join [[?b] ?d] …)`. The rewrite `(adorned.bf/marked ?b ?d)` with `magic.bf/marked(?b) ← (magic.bf/walk ?a) [?a :page/next ?b]` is T3's rewrite for the header `[[?b] ?d]`, and for the flat header it is unsound by T2: L4 is the instance. ∎

## 4. Lemmas

### L1. The passing binds inside a branch only what it is given

`within` builds the branch's bound set from the symbols it is passed, marking each the passing binds and nothing else (`passing.within`). A branch local that shares a body variable's name is therefore never bound by the passing, whatever the body binds (NESTED §2.1). ∎

### L2. The executor gives a fallback's branch the required run

`NewOrFallbackRelation` frames its branches over `GivenSymbols(required, carried, shortCircuit)`, which returns the required run alone under the fallback, and `planFormScope` frames the same at `PlanQuery` (SCOPE §4.1, §4.3). ∎

### L3. The instance of T2, with a required run

Facts: `x1 :link m1`, `m1 :edge k9`, `m1 :tag "c"`. Rules: `low(?a ?b) ← [?a :edge ?b]` and `p(?a ?k) ← [?a :link ?m] (or-default-join [[?m] ?k] (low ?m ?k) [(identity ?m) ?k])`. Free: ρ = (m1), B_1(m1) = {k9}, and p = {(x1, k9)}. p bound at its second position to m1: S = {(a = x1, m = m1, k = m1)} and O_S(m1) = {m1}. With G = R ∪ O, γ = bb, `magic.bb/low` = {(m1, m1)}, `adorned.bb/low` = ∅, B_1^γ(m1) = ∅, the default answers k = m1, which agrees with S, and r^β derives p(x1, m1), which r does not. With G = R, γ = bf, `magic.bf/low` = {m1}, `adorned.bf/low` = {(m1, k9)}, B_1^γ(m1) = {k9}, the join on k drops it, and r^β derives nothing, as r does. `TestFallbackBoundOutput_AnOutputAtTheHeadsBoundPositionNeverNarrowsABranch` (`datalog/storage`) is this instance, red under G = R ∪ O and green under G = R on every backend and both optimizer modes. ∎

### L4. The instance of T6, with a flat header

Facts: `a :page/next b1`, `b1 :page/kind d1`, `d1 :page/next e1`, `b9 :page/marked d9`. Rules: `marked(?b ?d) ← [?b :page/marked ?d]`, `walk(?a ?c) ← [?a :page/next ?c]` and `walk(?a ?c) ← [?a :page/next ?b] (or-default (marked ?b ?d) [?b :page/kind ?d]) (walk ?d ?c)`. The header infers to `[?b ?d]`. Free: the third rule at a has S = {(a = a, b = b1)}, ρ is the unit tuple, B_1 = {(b9, d9)} ≠ ∅, the join on ?b drops it, the form contributes nothing at a, and walk = {(a, b1), (d1, e1)}. walk bound first at a under the rewrite of T6: `magic.bf/marked` holds b1 and nothing of b9, `adorned.bf/marked` = ∅, B_1^γ = ∅, the default answers (b1, d1), `adorned.bf/walk(d1, ?c)` answers e1, and r^β derives walk(a, e1), which r does not. Under G = ∅, `marked` is derived whole, B_1^γ = B_1, and r^β derives walk(a, b1) alone, as r does. ∎

## 5. What the proof finds

- **F1.** A fallback is the one form where a narrower branch yields more tuples: its decision reads a branch's emptiness, so soundness needs every branch complete at every run tuple (T1).
- **F2.** A branch's bound positions are the required run and what the branch's own clauses provide, and an output the enclosing scope binds is never one (T2, T3). That is `GivenSymbols` under the fallback, the executor's computation, and the rewrite reads it.
- **F3.** Under a flat header the branch is given nothing, and its first invocation derives its predicate whole, the cost the free evaluation pays at the same place (T4).
- **F4.** Under a union the carried outputs are given soundly, so the two arms of `GivenSymbols` are forced by the forms' semantics and are not a choice (T5).
- **F5.** The header, written or inferred, decides the key, and the rewrite decides nothing about it (T6). A decision per a variable the body binds before the form is written with that variable in the required run.
- **F6.** An expectation that adorns an invocation of a flat-header fallback's branch from the enclosing scope is the per-key header's rewrite on the wrong form (T6, L4). The contract such an expectation pins, an adorned predicate inside a fallback below the recursive stratum, exists under the per-key header, and under a flat header the predicate stands unadorned with its original rules.

## 6. The contracts the tests pin

Each contract below is the subject of one test.

- An output of a fallback bound before the form, by an input, a pattern, a constant, the head's bound position or an invocation the planner places before the form, binds nothing inside a branch (T2, T3): `TestFallbackBoundOutput_AnOutputBoundBeforeTheFormNeverNarrowsABranch` and `TestFallbackBoundOutput_AnOutputAtTheHeadsBoundPositionNeverNarrowsABranch` (`datalog/storage`), the chain span of `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (`datalog/storage`), and `TestMagic_AFallbackBranchIsGivenTheRequiredRunAlone` (`datalog/planner`), whose union case pins T5.

- A flat-header fallback's branch is given nothing, and its invocation derives its predicate whole (T4, L4): `TestFallbackBoundOutput_AFlatHeaderFallbacksBranchIsGivenNothing` (`datalog/storage`), and the flat-header case of `TestMagic_MagicRuleReadsReadyMonotoneClauses` (`datalog/planner`), which reads the fallback as the union of its branches with the invocation unadorned and its original rule in a stratum below the recursive one.
- A fallback whose required run carries the body's variable adorns the branch's invocation at it, with the adorned predicate below the recursive stratum (T3, T6): the `(or-default-join [[?b] ?d] …)` case of `TestMagic_MagicRuleReadsReadyMonotoneClauses` (`datalog/planner`), and `TestMagic_AFallbackSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned` (`datalog/planner`) over `(or-default-join [[?b]] …)`, whose site is adorned and then excluded by the refusal (NMF L4).

## 7. The implementation

`rewriteClause` passes `within` the symbols `GivenSymbols` gives the form's branches: the required run with the outputs for an `or-join`, the required run alone for an `or-default-join`. `within` binds inside the branch each of them the passing binds and records them as the level's given symbols, each standing for the enclosing scope's symbol of its name in a magic rule's read. An output the branch provides is the branch's own and takes a fresh name there (`adornInvocation`). `GivenSymbols` has three readers: `NewOrFallbackRelation`, `planFormScope` and `rewriteClause`.
