# A delta copy runs on its delta, as a proof

## Sources

- The code: `datalog/planner/rule_bodies.go` (`Bodies`, `PlannedBody`, `PlanStratum`, `markDelta`, `markDeltaWalk`), `datalog/executor/rules.go` (`ruleEvaluation.extend`, `ruleEvaluation.runStratum`, `stratumEvaluation.run`, `derivedSet.absorb`).
- The design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2, which states that each derivation fires in exactly one window when the semi-naive delta copies cover every occurrence of a predicate that changed, and `docs/reference/RULES.md`, "Evaluation", which states that each later iteration executes each rule once per recursive invocation in its body.
- The failure: `docs/bugs/BUG_FIXPOINT_READS_THE_WHOLE_MAGIC_SET_PER_ITERATION.md`, item 3.
- The tests that pin the step: `TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty` and `TestEvaluateRules_TwoRecursiveOccurrences` (`datalog/executor/rules_test.go`), `TestRuleEvaluation_ALaterWindowReadsALowerStratumsNewTuplesAlone` and `TestRuleEvaluation_RunStratumReturnsWhatTheWindowAdded` (`datalog/executor/rule_evaluation_test.go`), and `TestPlanStratum_PlansEachBodyAsTheQueryFindingTheRulesHead` and `TestPlanStratum_AnOccurrenceInsideASubqueryBodyIsADeltaCopy` (`datalog/planner/rule_bodies_test.go`).
- The decision: an iteration runs a delta copy only when the predicate the copy is marked on gained tuples in the iteration before, as a window's iteration zero runs a lower copy only when its lower predicate gained tuples in the window, and the predicate a copy is marked on is carried by the planned body in the one field that carries a lower copy's.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **body** is a rule's clauses, planned as the query finding the rule's head (`planBody`).
- An **occurrence** is an invocation in a body of a predicate of the body's own stratum, at the top level, inside an or-join branch, or inside a subquery's inner query (`query.Invocations`).
- A **delta copy** of a body is the body with one occurrence replaced by an invocation of the predicate's delta symbol (`markDelta`), planned as the body is. A body has one delta copy per occurrence (`PlanStratum`, `Bodies.Delta`).
- A **lower copy** is the same replacement for an occurrence of a seed-restricted predicate of a lower stratum (`Bodies.Lower`).
- The **marked predicate** of a copy is the predicate whose delta symbol the copy invokes.
- The **set** of a predicate is its `derivedSet`: the tuples derived so far, each held once (`absorb`).
- The **delta** of an iteration is, per predicate, the tuples the iteration before added to its set (`runStratum`'s `delta`).
- A **derivation** of a tuple through a body is an assignment of the body's variables under which every clause holds, the occurrences reading the sets.
- A derivation **fires** in an iteration when a copy run in that iteration answers its tuple.

## 2. Preconditions

- **P1 (the semi-naive step).** A derivation through a body with an occurrence fires in the iteration after the one that added its last-added premise, through the copy marked on that premise's predicate, whose delta holds the premise then, and in no other iteration (SCOPE §4.2). A derivation whose premises are all in the sets at the seeding run fires at iteration zero, through the plain body.
- **P2 (the copies).** `PlanStratum` plans one delta copy per occurrence, in the order `markDeltaWalk` counts them, and one lower copy per occurrence of each seed-restricted lower predicate. `markDelta` replaces the counted occurrence and shares every other clause.
- **P3 (the stratum loop as it stands).** At every iteration after the first, `runStratum` holds every predicate's delta under its delta symbol, an empty one as an empty relation, and runs every delta copy of the stratum. At a later window's iteration zero it runs the lower copies whose lower predicate gained tuples in the window, and no other.
- **P4 (the set).** `absorb` adds the tuples of a relation the set does not hold and answers them; a tuple the set holds is answered by nothing.
- **P5 (the field).** `PlannedBody.Lower` names a lower copy's marked predicate. A delta copy's planned body names no predicate.
- **P6 (the events).** `rules/iteration` reports, per predicate, the tuples the iteration added. `phase/begin` fires once per phase of every plan run.

## 3. Theorems

**T1 (answers unchanged).** After every iteration, every set holds what it held under P3.

**T2 (iterations unchanged).** Every iteration's delta is what it was under P3, so a stratum runs the iterations it ran and reports the `rules/iteration` events it reported.

**T3 (the work).** An iteration after the first runs the delta copies marked on the predicates whose delta holds a tuple, and no other copy.

**T4 (one field).** The predicate a copy is marked on is carried by one field of the planned body, for a delta copy as for a lower copy, and a plain body names none.

## 4. Lemmas

### L1. A copy whose delta is empty answers held tuples (T1)

Let c be a delta copy marked on p, run at an iteration whose delta holds no tuple of p. A tuple c answers has a derivation through c's body. Its marked occurrence reads p's delta, which is empty, so the derivation holds under no assignment that reads that occurrence. A derivation that reads no marked occurrence stands inside an or-join branch other than the marked one, since at the top level and inside a subquery's inner query every assignment reads the marked occurrence. Such a derivation reads the sets through its other occurrences and the store through its patterns. By P1 it fired in the iteration after its last-added premise was added, through the copy marked on that premise's predicate, or at iteration zero through the plain body, which `PlanStratum` plans for a body whose occurrences all sit inside branches. So its tuple is in the set, and by P4 c's run adds nothing. ∎

### L2. Skipping such a copy leaves the sets and the delta as they were (T1, T2)

By induction over the iterations. At iteration zero nothing is skipped. Suppose the sets and the delta at the start of iteration i are as they were. The copies run at i are those marked on a predicate whose delta holds a tuple. A copy skipped at i is one L1 covers, and under P3 it added nothing to any set and nothing to the next delta. The copies run at i read the same sets and deltas, so they answer the same tuples, and `absorb` adds the same tuples. So the sets and the delta at the start of iteration i+1 are as they were. ∎

### L3. The iterations and their events (T2)

The loop runs while the delta holds a tuple, and its event reports the delta's sizes (P6). By L2 the delta at every iteration is as it was. So the loop runs the iterations it ran, and the events report what they reported. ∎

### L4. The copies run (T3)

The loop filters the delta copies by their marked predicate's delta, as the lower copies are filtered at a window's iteration zero (P3). A delta copy whose marked predicate's delta holds a tuple runs, and one whose delta holds none does not. ∎

### L5. The marked predicate (T4)

`markDeltaWalk` replaces the counted occurrence and knows its predicate at the replacement. Answering that predicate beside the clauses lets `PlanStratum` carry it on the planned body, in the field a lower copy carries its lower predicate in, since the fact is the same for both: the predicate whose delta symbol the copy invokes. A plain body replaces no occurrence and carries none. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, written and run red before the code.

- `TestRunStratum_RunsTheCopiesOfThePredicatesThatGainedTuples` (executor, `rules_test.go`): over a stratum of two mutually recursive predicates, each with one delta copy marked on the other, the `phase/begin` events between consecutive `rules/iteration` events number the phases of the copies marked on the predicates the earlier iteration added tuples to, and no more; the closure's answer and its iteration events are the chain's (T1, T2, T3, L2, L3, L4).
- `TestPlanStratum_ACopyIsMarkedOnThePredicateOfItsOccurrence` (planner, `rule_bodies_test.go`): a body with occurrences of two predicates of its stratum, one inside an or-join branch, plans one delta copy per occurrence, each marked on that occurrence's predicate in the occurrences' order; a lower copy is marked on its lower predicate; a plain body is marked on none (T4, L5).
- `TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty`, `TestEvaluateRules_TwoRecursiveOccurrences`, `TestRuleEvaluation_ALaterWindowReadsALowerStratumsNewTuplesAlone`, `TestRuleEvaluation_RunStratumReturnsWhatTheWindowAdded` and `TestRuleEvaluation_CompleteReportsEachStratumOnce` (executor), run unchanged (T1, T2).
- `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs` and `TestRules_ScopeShapesOnRandomGraphsMatchTheReference` (storage), run unchanged (T1).
- The planner's `rule_bodies_test.go` and the executor's readers of the lower field, the field renamed and their assertions unchanged (T4).

## 6. The change the proof licenses

- `rule_bodies.go`:
  - `PlannedBody.Lower` becomes `PlannedBody.Marked`, the predicate whose delta symbol the copy invokes, nil for a plain body.
  - `markDelta` answers the marked predicate beside the clauses.
  - `PlanStratum` sets `Marked` on every delta copy and every lower copy.
  - `Bodies`' doc names the field.
- `rules.go`:
  - `runStratum` runs, at each iteration after the first, the delta copies whose marked predicate's delta holds a tuple, filtered as the lower copies are at a window's iteration zero.
  - `extend` and `runStratum` read `Marked` where they read `Lower`.
- `docs/reference/RULES.md`, "Evaluation": each later iteration executes each rule once per recursive invocation of a predicate that gained tuples in the iteration before.
- `docs/bugs/BUG_FIXPOINT_READS_THE_WHOLE_MAGIC_SET_PER_ITERATION.md`: item 3's status.
- The tests of §5.

## 7. What the proof does not cover

- The cost a skipped copy's run paid, the index builds and the scans opened before its empty delta ended its join. The reproduction measures it.
- The joins inside a copy that runs, the bug report's items 1 and 2.
- A delta copy inside a window's branch or a subquery, which runs as the branch or the inner query does; the filter applies to the copy as a whole.

## 8. Alternatives excluded

- A field beside `Lower` naming a delta copy's predicate. Two fields for one fact, the predicate whose delta symbol the copy invokes, and a reader asking which of them applies.
- The executor reading the marked predicate from the copy's plan, by finding the delta symbol among its clauses. The planner knows the predicate at the replacement, and the executor would walk every copy's clauses at every iteration to learn it again.
- The copy run and its empty delta ending the join. That is what stands: the joins before the delta are built and the scans before it opened at every iteration, for a result the delta's emptiness decides before the plan runs.
