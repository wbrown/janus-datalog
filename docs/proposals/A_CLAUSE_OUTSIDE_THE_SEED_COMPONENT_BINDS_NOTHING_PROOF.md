# A clause outside the seed component binds nothing, as a proof

## Sources

- The code: `datalog/planner/magic.go` (`rewriteClauses`, `magicRuleReads`, `readClauses`, `passing.within`, `passing.enteringSubquery`, `passedInputs`, `adornInvocation`, `plannedBranches`, `scopeLevel`), `datalog/planner/rule_bodies.go` (`BoundPositions`), `datalog/query/rules.go` (`RuleSet.CheckRequired`), `datalog/planner/clause_utils.go` (`scansAWholeExtent`, `scoreClause`).
- `REWRITE_ORDER_PROOF.md` §1: the seed component and the clauses a magic rule reads.
- `REWRITE_ORDER_PROOF.md` T1: correctness under every order.
- `REWRITE_ORDER_PROOF.md` T2: a binding from outside the seed component.
- `REWRITE_ORDER_PROOF.md` T3: the planner's order under the rank.
- `REWRITE_ORDER_PROOF.md` T5: what is bound at the top level and in the rewrite.
- `REWRITE_ORDER_PROOF.md` F5: the bet for a clause with a constant.
- `NESTED_SCOPES_AS_QUERIES.md` §5.5: an invocation inside a scope adorned by the scope's clauses before it.
- `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1: the one orderer.
- `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.3: what a form gives its branches.
- `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.6: the engine sees no extent.
- The failure: `BUG_FIXPOINT_READS_THE_WHOLE_MAGIC_SET_PER_ITERATION.md`, item 1.
- The decision: a position of an invocation is bound from a clause outside the seed component only when the clause is a data pattern with an entity constant, or the predicate requires the position. The planner's order does not change. A top-level invocation binds from every group in hand, as it does.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **scope** is a rule's body, a form's branch, or a subquery's inner query, walked in the planner's order (`rewriteClauses`).
- A scope's **seed** is the symbols bound when its walk begins, and its **extent roots** are the symbols it is given that lie in the enclosing scope's extent set.
- At a body, the seed is the bound head positions, and there are no extent roots.
- In a branch, the seed is the given symbols in the enclosing scope's bound set, and the extent roots are the given symbols in its extent set (`within`).
- In an inner query, the seed is the `:in` symbols whose arguments are constants or variables in the enclosing scope's bound set, and the extent roots are the `:in` symbols whose arguments are variables in its extent set (`enteringSubquery`).
- A clause read before the current one is **seed-connected** when it is a data pattern with an **entity constant**, a constant at the entity position, which names one entity, or when shared variables connect it to the seed or to such a pattern through the clauses read before the current one, in any order: the seed-connected clauses are the connected component of the seed and the entity-constant patterns under shared variables. Every other clause read before the current one is an **extent clause**.
- The **bound set** at a clause is the seed and every variable of the seed-connected clauses.
- The **extent set** at a clause is the extent roots and the provides of the extent clauses, less the bound set. A clause sharing variables with extent clauses alone is an extent clause: the chain hanging off a scan or a value-constant pattern is the scan's.
- A **required position** is a position the predicate's rules declare bound by the caller (`Rule.Required`), which `CheckRequired` refuses to leave free.
- The **extent chain** of a symbol is the extent clauses providing it and every extent clause shared variables connect to them: their connected component among the extent clauses.

## 2. Preconditions

- **P1 (the walk as it stands).** `rewriteClauses` adds every provide of a clause a magic rule reads to the bound set, whatever the clause shares with it.
- **P2 (scopes bind from the passing).** `within` binds inside a branch only the given symbols the passing binds, and `enteringSubquery` binds an inner `:in` symbol only when its argument is a constant or a variable the passing binds.
- **P3 (the invocation).** `adornInvocation` binds a position whose variable is in the bound set, grounds a constant the call passes, and refuses a required position left free; a level's read passes `level.in` and `level.inputs` as the level was entered.
- **P4 (correctness under every order).** `REWRITE_ORDER_PROOF.md` T1: the adorned relation holds exactly the tuples of the predicate whose bound positions hold a magic tuple, and the rule's result is the same under every adornment, since the adorned body keeps every clause (T2b).
- **P5 (the product).** A position bound from a clause outside the seed component multiplies the magic set by that clause's projection onto the variable (`REWRITE_ORDER_PROOF.md` T2a).
- **P6 (the order).** At a body, the rank places a whole-extent scan after an invocation with an available argument, and a pattern with a constant beyond the attribute before it (`REWRITE_ORDER_PROOF.md` T3, F5). Inside a flat-header branch, `plannedBranches` plans under the required run alone, so a scan may precede the invocation.
- **P7 (the top level).** `BoundPositions` binds a top-level invocation's position from a constant, the environment, or a group in hand.
- **P8 (a read runs per value).** A scope's read is a subquery over the scope's clauses, correlated on the symbols it passes, and runs once per value of them.

## 3. Theorems

**T1 (answers unchanged).** Every invocation derives the tuples it derived.

**T2 (what binds).** An invocation's position is bound from the seed, from a seed-connected clause, or at a required position from its extent chain, and from nothing else.

**T3 (no product from outside).** The magic set of an invocation with a seed-bound position is never the seed's magic set times the extent of a clause outside the seed component, except through a required position.

**T4 (through forms and calls).** Inside a branch and inside an inner query, T2 holds for the given symbols and the call's arguments as for a body's clauses.

**T5 (the order and the top level stand).** The planner's order is unchanged, and a top-level invocation binds from every group in hand.

## 4. Lemmas

### L1. Fewer bound positions lose no tuple (T1)

Let γ bind a subset of the positions γ′ bound. By P4, `adorned.q^γ` holds every tuple of q whose γ-bound positions hold a tuple of `magic.q^γ`, and `adorned.q^γ′` those whose γ′-bound positions hold one of `magic.q^γ′`. The magic rule under γ reads the clauses providing γ's bound positions, so every tuple of `magic.q^γ′` projected onto γ's positions is in `magic.q^γ`, and `adorned.q^γ ⊇ adorned.q^γ′`. The adorned body keeps every clause, so the rule's result is the same (T2b). A clause left out of the magic rule widens the magic set and loses no tuple (`magicRuleReads`). ∎

### L2. The walk binds what T2 says (T2)

At the invocation the clauses read before it fall into the seed-connected clauses, the connected component of the seed and the entity-constant patterns under shared variables, and the extent clauses, the rest. The bound set is the seed and the seed-connected clauses' variables, and the extent set is the extent roots and the extent clauses' provides less the bound set, whatever order the clauses were read in. A variable in the bound set binds its position, a variable in the extent set binds it only where required, and a variable in neither is free. So a position is bound from the seed, from a seed-connected clause, or at a required position from the extent set. ∎

### L3. The product needs a binding from outside (T3)

By P5 the product arises when a position is bound from a clause outside the seed component. By L2 such a binding happens only at a required position. An entity-constant pattern names one entity, so a chain rooted at it is bounded by what that entity reaches through the chain's seeks, not by an extent. ∎

### L4. Forms and calls carry the sets (T4)

By P2 a branch's seed is the given symbols in the enclosing bound set and an inner query's seed is the arguments in it, and the extent roots are the given symbols and the arguments in the enclosing extent set. So a given symbol or an argument of extent kind binds a required position inside and nothing else, as an extent clause's symbol does. A level's read passes an extent root only when a clause the read carries mentions it, the chain of a required position or a seed-connected clause the root is connected to, and the enclosing scope then reads the root's own extent chain. A read correlated on an extent root nothing it carries mentions would run once per value of it by P8, the product inside the magic rule, so no read is. ∎

### L5. Nothing of the order changes (T5)

The rule reads the planner's order and computes none of its own (`REWRITE_ORDER_PROOF.md` F1); `scansAWholeExtent`, the rank, the gates and the score are as they are. `BoundPositions` is as it is, so the top level binds from every group in hand (P7). An adorned body executes its seed-restricted invocation first whatever the walk's order, since `scoreClause` leads with it. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named. Each is written before the code. None has run at this writing; the run that shows each red before the code and green after it is the one the findings record names.

- `TestMagic_AnInvocationIsBoundFromAnEntityConstantAndNotFromAValueConstant` (planner, `magic_test.go`): a rule invoking the closure under a bound first position beside a value-constant pattern on its second argument adorns the closure `bf`, the pattern after the guard and before the invocation in the adorned body, its magic rule the guard alone; beside an entity-constant pattern it adorns the closure `bb`, the magic rule reading the pattern (T2, T3, L2).
- `TestMagic_AScanOutsideTheSeedComponentBindsNothingInsideABranch` (planner): under a flat-header union whose branch holds a scan before the recursion, bound at the head's second position, the recursion is adorned at that position alone with no branch read (T2, T4).
- `TestMagic_AnExtentClauseBindsARequiredPositionAlone` (planner): a value-constant pattern outside the seed component before two invocations, one requiring the position it provides and one not, binds the required one, whose magic rule reads the symbol's chain, the pattern and the unrequired invocation, and not the other (T2, L2).
- `TestMagic_AnExtentGivenSymbolBindsARequiredPositionInsideABranch` (planner): a symbol given to a union's branches from an extent clause of the body binds, through the branch's clause providing a second symbol, a required position inside the branch, the branch read passing the given symbol and the body reading the extent clause; at a position not required the second symbol binds nothing and the invocation stands (T4, L4).
- `TestRuleEvaluation_ABodysInvocationBesideAValueConstantPatternDerivesWhatTheSeedReaches` (executor, `rule_evaluation_test.go`): over the chain, the closure invoked under a bound first position beside `[?x :page/kind "leaf"]` runs adorned for the first position and under no adornment of both, and answers the leaves the page reaches (T1, T3).
- `TestMagic_ClosureBoundFirst`, `TestMagic_AFreeRootAdornsTheRequiredPredicatesItReaches`, `TestMagic_InvocationInsideABranchIsAdornedByTheBranchsClausesBeforeIt`, `TestMagic_RequiredPositionBoundByThePlannedOrder`, `TestMagic_EveryScopeIsWalkedInThePlannersOrder`, `TestCreatePhasesGreedy_AScanOfAWholeExtentIsSelectedLast`, `TestCreatePhasesGreedy_AnInvocationWaitsOnAProviderByItsSelectivity` (planner), `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs`, `TestRules_ScopeShapesOnRandomGraphsMatchTheReference`, `TestRules_IsolatedRewriteCasesOnTheFourEdgeGraph`, `TestRules_InvocationUnderAFormEvaluatesOncePerQuery`, `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` (storage), run unchanged (T1, T5).
- `TestMagic_ABranchReadFindsWhatTheBranchProvides`, its first case, and `TestMagic_AFallbackBranchIsGivenTheRequiredRunAlone` bind an invocation from a scan outside the seed component through a form; each keeps its subject with a fixture whose binder is seed-connected, which the user words.
- The reproduction's `unfilled` query measures the change; its figures go to `PERFORMANCE_STATUS.md`.

## 6. The change the proof licenses

- `datalog/planner/magic.go`, `passing`: carries `extent` beside `bound`, and the body's seed.
- `datalog/planner/magic.go`, `scopeLevel`: carries the level's seed and extent roots on entry, the clauses read in it, and for a branch its given symbols, for a call the call; its `in` and `inputs` are computed at the invocation.
- `datalog/planner/magic.go`, `rewriteClauses`: reads a clause when every correlate of it is in the bound set or the extent set, and after each clause read computes `bound` as the seed's connected component over the clauses read and `extent` as the rest of what they provide.
- `datalog/planner/magic.go`, `within` and `enteringSubquery`: seed a scope with the given symbols or the arguments in the enclosing bound set, and root its extent set at those in the enclosing extent set.
- `datalog/planner/magic.go`, `adornInvocation`: binds an extent variable at a required position alone; each level's read carries the level's seed-connected clauses and the extent chains the read needs and passes the extent roots those mention, and the body reads its own seed-connected clauses and the chains of the roots the reads pass down to it.
- `REWRITE_ORDER_PROOF.md` §1, the clauses a magic rule reads: edited before the code.
- `REWRITE_ORDER_PROOF.md` F5, the bet for a clause with a constant: edited before the code.
- `REWRITE_ORDER_PROOF.md` T5, what is bound at the top level and in the rewrite: edited before the code.
- `NESTED_SCOPES_AS_QUERIES.md` §5.5, the clauses that adorn an invocation inside a scope: edited after the code.
- `docs/reference/RULES.md`, where it states what a bound invocation derives: edited after the code.
- `BUG_FIXPOINT_READS_THE_WHOLE_MAGIC_SET_PER_ITERATION.md`: item 1's status.
- The tests of §5.

## 7. What the proof does not cover

- The top level binds from a group in hand that a value-constant pattern produced, so a query writing the reproduction's shape at the top level still pays the product there. That is the decision's, and the top level and a rule body read one order under two binding rules.
- Inside a flat-header branch, `plannedBranches` plans under the required run alone while the executor runs the branch under the given symbols, so the walk's order inside the branch is not the order the branch runs; the rule binds nothing from the scan whatever that order, and the order itself is a one-orderer defect (SCOPE §4.1) outside this change.
- The cost of reading an extent chain for a required position, which is the extent's as today.
- A subquery call whose argument is of extent kind: the call runs once over its argument combinations whatever the inner adornment, and the inner invocation binds the argument at a required position alone.

## 8. Alternatives excluded

- Ranking a value-constant pattern as a whole-extent scan in the planner: it reorders every query with no invocation in it, against `scoreClause`'s priority, and the top-level invocation beside such a pattern changes from `bb` to `fb`; neither was ruled.
- The rule at the top level too, `BoundPositions` reading provenance: a working query, `[?x :type :node] (reach ?x ?s)`, changes behaviour.
- Binding by the size of the group in hand at execution: the adornment is decided at `PlanQuery`, and the plan carries one evaluation per invocation.
- Reading every extent clause into the magic rule as a verdict group: a verdict that binds nothing restricts no magic tuple and costs its evaluation per run of the magic rule.
- Classifying a clause by the bound set as it stands when the walk reads it, one pass in the planner's order: a value-constant pattern read before the seed-connected clause sharing its variable is never read by the magic rule, which reads it today, and a seed-connected clause correlating on an extent symbol is read into a magic rule that provides the symbol nowhere, a rule `NewSeededRuleSet` refuses.
