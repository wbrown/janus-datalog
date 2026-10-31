# A copy reads the tuples added since it ran, as a proof

## Sources

- The code: `datalog/executor/rules.go` (`ruleEvaluation.extend`, `ruleEvaluation.runStratum`, `stratumEvaluation.run`, `stratumState`, `derivedSet.absorb`, `derivedSet.relation`), `datalog/planner/rule_bodies.go` (`PlanStratum`, `Bodies`, `PlannedBody`, `markDelta`, `neededPredicates`), `datalog/query/rules.go` (`RuleSet.Strata`, `RuleSet.Rules`, `Invocations`, `stratify`, `Stratum`), `datalog/annotations/types.go` (`RulesIteration`, `RulesStratumBegin`, `RulesStratumComplete`).
- The proofs this one stands on: `DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` (§1 the copies and the marked predicate, P1 the semi-naive step, T3 the copies a run runs, T4 the marked predicate); `DELTA_LEADS_ITS_COPY_PROOF.md` (T2 the delta leads its copy); `REWRITE_ORDER_PROOF.md` (P2 each stratum evaluates to its fixpoint).
- The design: `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2, the fixpoint continued across windows, under which each window's closure from the carried sets is the least fixpoint above them; `docs/reference/RULES.md`, "Evaluation", the paragraph on the semi-naive iteration, and "Bounds", the iteration bound; `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3, the paragraph on the semi-naive iteration; `NESTED_SCOPES_AS_QUERIES.md` §2.5, where a same-stratum invocation may stand.
- The literature: M. S. Hecht and J. D. Ullman, "A simple algorithm for global data flow analysis problems", SIAM Journal on Computing 4 (1975), which iterates a monotone system over a graph in the depth-first order of the graph's nodes, so that a pass carries a value along every edge but the back edges.
- The failure: the bound `unfilled` query of the downstream report `docs/plans/JANUS_REPRO_FILLED.md` in `janus-repro-filled-2026-10-30.tar.gz`. The rewrite places the recursive predicates and every lower predicate their demand reaches in one stratum, and that stratum's annotated run adds a few tuples to a few predicates per iteration, each iteration carrying a delta one predicate down the chain of lower predicates or one predicate back up.
- The decision: a copy's delta is the tuples its marked predicate gained since the copy last ran, every other invocation of the copy reads the sets as they stand when the copy runs, a pass visits each delta copy of the stratum once in the pass order and runs it when its delta is not empty, and a stratum ends at a pass that runs no copy.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **body**, an **occurrence**, a **delta copy**, a **lower copy** and a copy's **marked predicate** are as `DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` §1 defines them.
- A **set** S of a predicate is its `derivedSet`: its tuples, each held once, appended in the order `absorb` admits them. The **ordinal** of a tuple is its position in `S.tuples`, and the **count** of S at a moment is the tuples it holds then.
- The **mark** of a copy c is the count of its marked predicate's set at the start of c's last run, and zero before its first run.
- The **delta** of c at a run is the tuples of its marked predicate's set from c's mark to the set's count at the run's start.
- A **pass** is one visit of every delta copy of the stratum, in the pass order, each copy whose delta is not empty run over its delta and its result absorbed before the next copy is visited.
- The **dependency graph** D of a stratum has a vertex per predicate of the stratum and an edge p → q for each invocation of q, a predicate of the stratum, in a body of p (`Invocations`), which is an occurrence by `NESTED_SCOPES_AS_QUERIES.md` §2.5.
- The **walk** is a depth-first walk of D, started at each predicate of the stratum not yet visited, in `Stratum.Predicates`' order, visiting a predicate's successors in the order of its rules and, within a rule, of its body's invocations. An edge p → q is a **back edge** when q is on the walk's stack as the walk from p reaches q.
- The **pass order** is the walk's postorder: a predicate stands after every predicate its walk reached from it, and a predicate's copies stand in `PlanStratum`'s order, by rule and then by occurrence.
- A **derivation** through a body is an assignment under which every clause of the body holds, its occurrences reading the sets, and its **premises** are the set tuples its occurrences read. A derivation **fires** when a copy answers its tuple.

## 2. Preconditions

- **P1 (the step as it stands).** `runStratum` holds, at each iteration after the first, under each predicate's delta symbol the tuples the iteration before added, takes one view of every set before any copy runs, runs every delta copy marked on a predicate whose delta holds a tuple, and absorbs the results after every copy has run. A derivation fires in the iteration after the one that added its last-added premise, through the copy marked on that premise's predicate (`DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` P1, T3).
- **P2 (the copies).** `PlanStratum` plans one delta copy per occurrence and one lower copy per occurrence of a seed-restricted lower predicate (`DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` P2).
- **P3 (monotone bodies).** A same-stratum occurrence stands only where evaluation is monotone, at the top level, in an or-join branch or in a non-aggregating subquery (`NESTED_SCOPES_AS_QUERIES.md` §2.5, `NewSeededRuleSet`'s refusal of nested recursion). So a body's answers over larger sets include its answers over smaller ones.
- **P4 (the set grows only).** `absorb` appends a tuple the set does not hold and admits nothing else, and no tuple leaves a set. So an ordinal names one tuple for the set's life, and the tuples from one count to a later count are those admitted between them.
- **P5 (the windows).** `extend` runs a stratum per window: a window's seed enters the seed predicate's set as the window's first delta, a later window's first run runs the lower copies whose lower predicate gained tuples in the window over those tuples, and the sets persist across windows (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2).
- **P6 (the fixpoint).** A stratum's sets at its end are the least fixpoint of its bodies over the lower strata's relations and the seed (`REWRITE_ORDER_PROOF.md` P2, `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2).
- **P7 (the events and the bound).** `rules/iteration` reports, per predicate, the tuples an iteration added, numbered from zero per window, and `MaxRuleIterations` bounds the iterations per window.
- **P8 (a view is a fixed slice).** A view is `derivedSet.relation`'s relation over `S.tuples[:n:n]`, and a body's result is pulled when it is absorbed.

## 3. Theorems

**T1 (the same fixpoint).** Every stratum's sets at its end are what they were under P1. So every evaluation derives what it derived, and every query answers what it answered.

**T2 (each tuple once per copy).** Over an evaluation, a copy reads each tuple of its marked predicate in its delta once.

**T3 (every derivation fires).** A derivation whose premises are in the sets fires through the copy marked on the occurrence that reads the premise admitted last, at that copy's first run after the admission, and the stratum ends at a pass that runs no copy, which is when the sets are closed under the bodies.

**T4 (a pass carries a chain).** Let d_1, …, d_k be derivations, each d_{i+1} through a body of p_{i+1} with an occurrence of p_i, its premise admitted last being d_i's tuple and every other premise admitted before it. Then every tuple of the chain is admitted within b + 1 passes from the pass that admits d_1's tuple, where b is the number of back edges among the edges p_{i+1} → p_i.

**T5 (nothing else changes).** The plain bodies run at the first window's first run and the lower copies at a later window's first run, as before. A copy whose delta is empty does not run. The begin and complete events fire as before, and the iteration events report what each pass added.

## 4. Lemmas

### L1. Soundness (T1)

Every tuple a copy answers has a derivation through its body whose premises are in the sets at the run: the delta is a slice of the marked predicate's set, every other occurrence reads a view of a set, and a pattern reads the store. By induction over the runs, every set holds tuples of the least fixpoint, since the fixpoint is closed under every body (P6). ∎

### L2. Completeness (T3)

Let a derivation's premises be in the sets, let t be the premise admitted last, into the set of q at ordinal o, and let c be the copy marked on the occurrence that reads t. A mark advances only at a run, so at c's first run after t's admission the mark of c is at most o, and t is in c's delta (P4). Every other premise was admitted before t, so each is in the set its occurrence reads, and c reads the sets as they stand at its run. So c answers the derivation's tuple, and `absorb` admits it unless the set holds it already. A copy whose delta is not empty runs in the next pass at the latest, since a pass visits every copy. A pass that runs no copy is one in which every copy's mark equals its predicate's count, so no derivation remains whose last-admitted premise a copy has not read, and the sets are closed under the bodies. With L1 the sets are the least fixpoint. ∎

### L3. Termination (T3)

Every run of a copy advances its mark to its predicate's count. Where the fixpoint is finite the counts are bounded, so the marks are bounded and the passes end at one that runs no copy. `MaxRuleIterations` counts passes, so a fixpoint it bounded it bounds. ∎

### L4. Each tuple once per copy (T2)

A copy's delta at a run is the tuples from its mark to the count at the run's start, and the mark is then set to that count. By P4 successive runs read disjoint intervals that together cover the set. The marks persist across windows with the sets (P5). ∎

### L5. The views at a run (T1, T2)

A copy's run reads views taken at the run's start, and its result is absorbed as it is pulled (P8). The delta is the slice from the mark to the count at the run's start, capped at that count. So a tuple the copy's own result admits lies beyond its views and its delta, and is read at a later run. ∎

### L6. The chain (T4)

Within a pass the copies run in the pass order. For an edge p_{i+1} → p_i that is no back edge, p_i precedes p_{i+1} in the postorder: along a tree or forward edge p_i is a descendant of p_{i+1} and finishes first, and along a cross edge p_i finished before the walk from p_{i+1} reached it. So when d_i's tuple is admitted in a pass, the copy of p_{i+1} marked on p_i is visited later in the same pass, finds the tuple in its delta, reads d_{i+1}'s other premises in the sets, and fires d_{i+1} in that pass (L2). Along a back edge p_i follows p_{i+1} in the postorder, so d_{i+1} fires in the pass after. Counting from the pass that admits d_1's tuple, each back edge costs one further pass and no other edge costs one, so the chain's tuples are admitted within b + 1 passes. ∎

### L7. Nothing else (T5)

A window's first run runs the plain bodies, or at a later window the lower copies whose lower predicate gained tuples, as `runStratum` does, each over views taken at its run. A copy whose delta is empty is not run, as `DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` T3 runs no copy whose marked predicate's delta is empty. The begin event fires at the stratum's first run and the complete event once at the evaluation's completion. The iteration event reports the tuples the pass added per predicate, and the bound counts passes (L3). ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, each written before the code and run red before it where a red exists.

- `TestRunStratum_APassCarriesAChainOfDerivations` (executor, new): over the ring `[(a ?x ?y) [?x :page/next ?y]] [(b ?x ?y) (a ?x ?y)] [(c ?x ?y) (b ?x ?y)] [(a ?x ?z) (c ?x ?y) [?y :page/next ?z]]` on a chain of pages, the answer of `a` is the chain's closure, every pass but the last adds to `a`, `b` and `c` alike, and the `rules/iteration` events number one more than the chain's longest path's edge count, where the step as it stands carried one hop through `b` and `c` back to `a` in three iterations (T1, T4, L2, L6).
- `TestPlanStratum_DeltaCopiesStandInThePassOrder` (planner, new): over the same ring the delta copies stand in the walk's postorder, `b`'s copy, then `c`'s, then `a`'s, and over the ring of two predicates `odd` and `even`, whose first rule reads neither, `even`'s copy then `odd`'s (§1, L6).
- `TestRunStratum_RunsTheCopiesOfThePredicatesThatGainedTuples` (executor): under the decision a copy runs in a pass exactly when its marked predicate gained since the copy last ran. Over its ring of two predicates the pass order is `even`'s copy, marked on `odd`, then `odd`'s copy, marked on `even`, so `even`'s copy runs in a pass when `odd` gained in the pass before and `odd`'s copy when `even` gained in the same pass. Its assertion becomes that: the phases begun in a pass are `even`'s copy's when the pass before added to `odd` and `odd`'s copy's when the pass added to `even`, and the passes compared number three, where the step as it stands compared four (T5).
- `TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty`, `TestEvaluateRules_TwoRecursiveOccurrences`, `TestRunStratum_ADeltaCopySeeksItsPatternOnTheDeltasKeys`, `TestRuleEvaluation_ALaterWindowReadsALowerStratumsNewTuplesAlone`, `TestRuleEvaluation_RunStratumReturnsWhatTheWindowAdded` and `TestRuleEvaluation_CompleteReportsEachStratumOnce` (executor), run unchanged: each reads a stratum of one predicate, whose pass is its iteration (T1, T5).
- The tests that set `MaxRuleIterations` or read a stratum's iteration count over a closure of one predicate, the cancellation tests among them, run unchanged for the same reason (T5).
- `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs`, `TestRules_ScopeShapesOnRandomGraphsMatchTheReference`, `TestRules_InvocationUnderAFormEvaluatesOncePerQuery`, `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` and `TestQueriesAnswerAlikeUnderBothOptimizerModes` (storage), run unchanged (T1).
- The downstream reproduction measures the change, and its figures go to `PERFORMANCE_STATUS.md`.

## 6. The change the proof licenses

- `datalog/planner/rule_bodies.go`, `PlanStratum`: `Bodies.Delta` is in the pass order, computed by the walk over the stratum's dependency graph, and `Bodies`' doc states the order.
- `datalog/executor/rules.go`, `stratumState`: carries the marks, one per delta copy.
- `datalog/executor/rules.go`, `runStratum`: after the window's first run, passes as §1 defines them, each copy's views taken at its run and its result absorbed before the next copy is visited, the iteration event per pass, and the bound counting passes.
- `datalog/executor/rules.go`, `stratumEvaluation.run`: keeps its contract, the bodies given run under the relations given, and a pass hands it one copy.
- `docs/reference/RULES.md`, "Evaluation", the paragraph on the semi-naive iteration: a copy reads the tuples its predicate gained since the copy last ran, every other invocation reads the relation as it stands, and a pass runs the copies in the pass order; "Bounds": the iteration bound counts passes.
- `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3, the paragraph on the semi-naive iteration: the same.
- `DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md`: its P1, P3 and T2 describe the step this proof replaces, and its T3 stands as this proof's T5 states it.
- The tests of §5.

## 7. What the proof does not cover

- The number of passes a given program takes, which the walk's edge order decides and the reproduction measures.
- A derivation two of whose premises were admitted since the two copies reading them last ran: it fires through both copies, and `absorb` admits its tuple once, as under the step as it stands when both premises lay in one iteration's deltas.
- The order of the lower copies at a later window's first run, which read the window's additions and not the marks.
- The strata and their order, which are `Strata`' as before.

## 8. Alternatives excluded

- The step as it stands (P1): a pass carries a delta one edge of the dependency graph, whatever the edge, the failure.
- The copies in `Bodies.Delta`'s planned order with the marks alone: correct by L1 to L3, and a pass bounded only by the chain's length, since the order may place a consumer before its producer at every edge.
- Views taken once per pass: a copy would read nothing of what the copies before it in the pass admitted, which is the step as it stands under another name.
- A nested fixpoint per original stratum inside a stratum the rewrite merged: it needs the original set's strata beside the rewritten set's, carried through the plan, where the walk reads the stratum's own graph.
