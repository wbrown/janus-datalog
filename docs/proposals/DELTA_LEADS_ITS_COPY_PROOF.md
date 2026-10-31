# The delta leads its copy, as a proof

## Sources

- The code: `datalog/planner/clause_phasing.go` (`selectPhaseClauses`), `datalog/planner/clause_utils.go` (`scansAWholeExtent`, `selection.invocationDependsOnPendingProvider`, `scoreClause`), `datalog/planner/rule_bodies.go` (`PlanStratum`, `markDelta`, `planBody`), `datalog/query/minted.go` (`DeltaSymbol`, `SeedRestricted`), `datalog/executor/rules.go` (`runStratum`).
- The design: `REWRITE_ORDER_PROOF.md` §1 and §6, the rank and the gate; `DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md`, the copies an iteration runs; `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1, the one orderer, §4.5, an invocation of a held predicate, and §4.6, the binding-driven strategies; `docs/reference/RULES.md`, "Evaluation", the paragraph on what a body's invocations read.
- The failure: `BUG_FIXPOINT_READS_THE_WHOLE_MAGIC_SET_PER_ITERATION.md`, item 2.
- The tests that pin the order: `TestPlanStratum_UnguardedBodyKeepsThePatternFirst` and `TestPlanStratum_BodyInvocationLeadsItsPatterns` (`datalog/planner/rule_bodies_test.go`), `TestCreatePhasesGreedy_AScanOfAWholeExtentIsSelectedLast` and `TestCreatePhasesGreedy_AnInvocationWaitsOnAProviderByItsSelectivity` (`datalog/planner/invocation_scheduling_test.go`), `TestPlannedClausesPlaceABodysInvocationAgainstItsPatterns` (`datalog/planner/scope_test.go`).
- The decision: in every delta copy and every lower copy, the invocation of the delta symbol is the first clause selected, whatever its predicate, and the clauses after it are ordered as the rank and the score order them.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **copy** is a delta copy or a lower copy of a body (`DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` §1): the body with one occurrence replaced by an invocation of the predicate's delta symbol.
- The **delta invocation** of a copy is that replaced occurrence. A copy holds one.
- A **delta symbol** is a symbol `DeltaSymbol` mints: its namespace's kind is `delta`.
- The **rank** of a clause at a selection is 0 when it scans a whole extent and 1 otherwise, and the **selection** takes the highest rank, the highest score within it, and the first in list order on a tie (`REWRITE_ORDER_PROOF.md` §6).
- The **gate** holds an invocation behind a pending provider of one of its arguments (`invocationDependsOnPendingProvider`).
- A pattern **seeks** on a symbol when it is selected after a clause that provides the symbol: the matcher serves it under the bindings in hand, through the strategies of SCOPE §4.6.

## 2. Preconditions

- **P1 (the delta's content).** The relation held under a delta symbol is the tuples the iteration before added to the predicate's set, or, for a lower copy at a window's iteration zero, the tuples the window added to the lower predicate, and nothing else (`runStratum`, SCOPE §4.2).
- **P2 (the order as it stands).** A delta invocation of a magic or adorned predicate is seed-restricted: rank 1, scored 500 above an invocation of any other predicate, and held by no gate. A delta invocation of any other predicate has, at its copy's first selection, no constant and no available argument, so it scans a whole extent: rank 0, selected only when no other clause is selectable, and held by the gate behind any selectable provider of its arguments. Within rank 1 the score reads an invocation's constants, its available arguments and the symbols it provides, 10 each, so a seed-restricted delta invocation of fewer positions scores below a seed-restricted invocation of more.
- **P3 (the copy's answers).** A copy's answers are the derivations through its body whose marked premise is in the delta (`DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md` P1).
- **P4 (the rewrite reads no delta).** The rewrite plans the bodies as the rule set holds them, which carry no delta symbol; `markDelta` runs after the rewrite, on the set it produced (`REWRITE_ORDER_PROOF.md` §1, `PlanStratum`).
- **P5 (a held predicate at its clause).** Inside a body, an invocation of a predicate the fixpoint holds reads the held relation at its clause and derives nothing (SCOPE §4.5).
- **P6 (the join).** The relations a copy's clauses denote are joined, and the join of a set of relations is one relation whatever the order the join is taken in; the order decides the cost alone (`REWRITE_ORDER_PROOF.md` T1, F2).

## 3. Theorems

**T1 (answers unchanged).** Every copy answers the tuples it answered, every adorned predicate is adorned as it was, and every stratum derives the sets it derived.

**T2 (the delta leads).** In every copy the delta invocation is the first clause of the first phase, on both planner paths.

**T3 (the work is the delta's).** In a copy, a pattern sharing a variable with the delta invocation seeks on the delta's tuples, and the copy's joins read the delta's tuples and what they reach.

## 4. Lemmas

### L1. The order changes no answer (T1)

A copy's clauses denote relations the plan joins, and the join's result is the same under every order (P6). The delta invocation reads the held delta at its clause under every order (P5). The rewrite's adornments read the planner's order of bodies that carry no delta symbol (P4), so a rank for a delta invocation reaches no adornment. The sets a stratum derives are the copies' answers absorbed (P3), which are unchanged. ∎

### L2. The rank places the delta first (T2)

A delta invocation takes a rank above 1, and the gate holds it behind nothing. A copy holds one delta invocation. So at the copy's first selection the delta invocation is the one clause of the highest rank, and the selection takes it. On the algebra path the decompiled clauses enter the same selection (`REWRITE_ORDER_PROOF.md` L7). ∎

### L3. The clauses after the delta seek on it (T3)

After the delta invocation is selected, its arguments are available. A pattern sharing a variable with it has an available variable, so it scans no whole extent, ranks 1, and is selected before any clause that scans a whole extent. Selected after the delta, it is matched under the bindings in hand, and the matcher seeks per key at or below the boundary and scans the extent once above it (SCOPE §4.6). A seed-restricted invocation sharing a variable with the delta ranks 1 and joins the delta's tuples on the shared symbols. So every join of the copy reads the delta's tuples or what the clauses before it reached from them. Under P2 a copy of an unadorned predicate scanned its pattern's whole extent before the delta joined it, and a copy of an adorned predicate joined its whole seed-restricted relations before a delta of fewer positions. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, written and run red before the code.

- `TestPlanStratum_ADeltaCopyLeadsWithItsDelta` (planner, `rule_bodies_test.go`), in the place of `TestPlanStratum_UnguardedBodyKeepsThePatternFirst`, whose premise the decision reverses: for the closure no magic predicate guards, every delta copy's first clause is its delta invocation, and for the closure rewritten for a bound first position, every delta copy and every lower copy of the adorned stratum opens with its delta invocation, on both planner paths (T2, L2).
- `TestRunStratum_ADeltaCopySeeksItsPatternOnTheDeltasKeys` (executor, `rule_evaluation_test.go`): over the closure no magic predicate guards on a chain of six pages, the keys the edge pattern is matched against through a binding carrying its value variable total the closure's pairs, one per delta tuple, and the closure's answer is the chain's (T1, T3, L3).
- `TestPlanStratum_BodyInvocationLeadsItsPatterns`, `TestPlannedClausesPlaceABodysInvocationAgainstItsPatterns`, `TestCreatePhasesGreedy_AScanOfAWholeExtentIsSelectedLast`, `TestCreatePhasesGreedy_AnInvocationWaitsOnAProviderByItsSelectivity` and `TestCreatePhasesGreedy_ASubqueryReadingAnAvailableInputPrecedesAScan` (planner), run unchanged (P4, L1).
- `TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty`, `TestEvaluateRules_TwoRecursiveOccurrences`, `TestRunStratum_RunsTheCopiesOfThePredicatesThatGainedTuples` and the rule evaluation tests (executor), and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_ShadowedLocalsOnRandomGraphsMatchTheReference`, `TestRules_InvocationBoundByItsBranchOnRandomGraphs` and `TestRules_ScopeShapesOnRandomGraphsMatchTheReference` (storage), run unchanged (T1).
- `BenchmarkTransitiveClosure` (storage) measures the closure's cost under the change; its figures go to `PERFORMANCE_STATUS.md`.

## 6. The change the proof licenses

- `datalog/query/minted.go`: `IsDeltaSymbol` reports whether a symbol is one `DeltaSymbol` mints.
- `datalog/planner/clause_utils.go`:
  - `scansAWholeExtent` answers false for a delta invocation.
  - `invocationDependsOnPendingProvider` holds no delta invocation.
- `datalog/planner/clause_phasing.go`, `selectPhaseClauses`: a delta invocation ranks above every other clause.
- `datalog/planner/rule_bodies.go`, `planBody`'s doc: the delta invocation leads its copy, a magic or adorned predicate leads the patterns, and any other invocation joins them at collapse.
- `REWRITE_ORDER_PROOF.md` §1, the definition of a clause that scans a whole extent, and §6, the rank: a delta invocation scans no whole extent and ranks above every clause, edited before the code the rank governs.
- `docs/reference/RULES.md`, "Evaluation": the paragraph on what a body's invocations read states that the delta leads its copy.
- `BUG_FIXPOINT_READS_THE_WHOLE_MAGIC_SET_PER_ITERATION.md`: item 2's status.
- The tests of §5.

## 7. What the proof does not cover

- The order among the clauses after the delta, which the rank and the score decide as they did. In the bug report's body, a seed-restricted invocation of three positions precedes the pattern that would seek on the delta's cell, and the magic set's size under `bbb` is item 1's.
- The cost of a copy whose delta is large, the seeding iteration's additions among them, which seeks per delta tuple where a scan read the extent once. The closure benchmark measures it.

## 8. Alternatives excluded

- A score for the delta invocation above the seed-restricted ones, within rank 1. The score reads the symbols a clause provides, so a delta of fewer positions would need a bonus above every provides count, which is the rank by another name.
- The delta invocation named seed-restricted. A delta of an unadorned predicate is restricted by the iteration, not by a seed, and `SeedRestricted` states what the rewrite minted.
- The executor ordering a copy's joins by the relations' sizes at run time. The planner's order is the one orderer (SCOPE §4.1), and the delta's smallness is known at planning from the copy's shape.
