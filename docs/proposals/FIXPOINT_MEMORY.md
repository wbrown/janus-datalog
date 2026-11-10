# The fixpoint's memory

## Goals

- A rule evaluation holds each predicate's closure while the semi-naive step and the bodies above it read it, and no longer.
- A held tuple costs its own values, one slot in the set's table, and one ordinal per index its joins read, and nothing twice.
- The evaluation's answer leaves it as a relation over the answer predicate's tuples and carries no set.

## Sources

- The evaluation: `datalog/executor/rules.go` (`ruleEvaluation`, `stratumState`, `newRuleEvaluation`, `ruleEvaluation.extend`, `ruleEvaluation.runStratum`, `ruleEvaluation.complete`, `derivedSet`, `newDerivedSet`, `derivedSet.absorb`, `derivedSet.index`, `derivedSet.relation`, `stratumEvaluation.run`, `DefaultQueryExecutor.deriveInvocation`, `DefaultQueryExecutor.invocationSource`).
- The table and the index: `datalog/executor/tuple_set.go` (`TupleSet`, `TupleSet.Add`, `TupleSet.grow`), the ordinal index beside `datalog/executor/join_index.go` (`ordinalIndex`, `ordinalIndex.extend`).
- The proofs this one stands on: `ONE_TUPLE_TABLE_PROOF.md` (T4, each tuple held once under its whole-tuple key), `A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md` (P7 the set grows only, T4 a tuple enters an index once, T6 no relation carries a set, §7 the memory of an index), `A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md` (the delta copy's reads, which the sets serve), `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2 (an evaluation continued across a scope's windows).
- The measurement: `TestRules_ABoundInvocationOverTheKernelFactsReportsLessWorkThanTheWholeRelation` (`datalog/storage/kernel_facts_test.go`) alone, on `memory-trees`, under `GOOS=js GOARCH=wasm` through the Node runner with `GODEBUG=gctrace=1`, Apple M5, Homebrew go1.27.2. The test logs the tuples each run derives per predicate; the trace gives the heap at each cycle, numbered across both legs, and a figure below names its cycle and its leg.
- The record this proposal points at and does not restate: `docs/bugs/BUG_THE_MAGIC_SETS_OUTNUMBER_THE_TUPLES_THEY_SAVE.md`, the bound query deriving 693,918 tuples against the whole relation's 457,286.
- The record this proposal is not about: `docs/perf/RULES_EVALUATION_ALLOCATIONS_2026-10-28.md`, the js/wasm collector's stalled mark under a high allocation rate, which retains allocations the fixpoint never holds.

## 1. What the fixpoint must hold

- **P1 (the closure is read twice over).** A pass's copy joins its delta against the sets as they stand, and a body's result is absorbed by probing the set for each tuple it yields: a tuple the set holds is rejected, one it does not is added (`derivedSet.absorb`, `runStratum`'s pass). The termination of the stratum is the pass that runs no copy, which is every mark at its set's count. So each recursive predicate's whole closure is read by the next pass's joins and by every absorption, until the stratum converges. A relation that streams its tuples once cannot serve either read, and the closure's size is the output's: `unfilled` is 70,458 pairs because that is the relation.
- **P2 (what streams).** The delta a pass hands to the next is a view over the set's tuples from the copy's mark to the set's count (`runStratum`, the pass loop), the lower deltas a window hands up are the tuples the window added (`extend`'s `added`), and the answer is a view over the answer predicate's tuples (`derivedSet.relation`). None is a copy of the closure.

## 2. What a set holds per tuple

- **P3 (the set).** A `derivedSet` holds, per tuple:
  - the tuple's values, its own backing array of one interface value per position;
  - its header in `tuples`, a slice in admission order;
  - its header again in the `TupleSet` under `keys`, whose `members` is a slice in admission order beside an open-addressed table of one `int32` index and one `uint8` tag per slot, at most half full;
  - per key its joins read, an `ordinalIndex`: a `TupleSet` over the first tuple of each distinct key, one `int32` ordinal per tuple, and, under fanout, the key's ordinals grouped.
- **P4 (the two slices).** `absorb` appends each admitted tuple to `tuples` and adds it to `keys`, and `TupleSet.Add` appends it to `members`, so every held tuple's header is held in two slices of the set, and each slice grows by doubling. `relation` and `index` read `tuples`; nothing reads `members` outside the table.

## 3. How long the sets live

- **P5 (every stratum's sets, for the evaluation's life).**
  - `extend` makes every stratum's sets at the first window and keeps them in `ev.strata`.
  - `runStratum` runs every body under a context carrying every set of every stratum (`ctx.WithSets`).
  - `complete` emits each stratum's complete event with its sets' tuple counts and drops nothing.
  - An evaluation of one run is dropped when `deriveInvocation` returns. An evaluation continued across a form's windows is held by the form execution's continuations (`ctx.continued.byClause`) for the form's life.
- **P6 (a whole evaluation runs once).** For an evaluation with no seed, `extend` runs every stratum at the first window and none at a later one: a stratum runs at a later window only when the window carries a seed or a lower predicate it invokes gained tuples, and a whole evaluation's windows carry neither. A later window returns the answer predicate's relation as the first window left it.
- **P7 (a seeded evaluation runs again).** A seeded evaluation's later window enters the seed predicate's stratum as a delta and runs every stratum above it that a changed predicate reaches, over the sets as they stand. Its sets are read at every window.

## 4. The measurement

The whole `unfilled` relation over the kernel facts derives twenty strata and 457,286 tuples: `open` 119,152, `through` 118,912, `unfilled` 70,458, `clear` 70,326, `passes` 11,326, `calls` 10,044, `reach` 9,865, `run` 9,628, `flows` 9,309, `plain` 8,011, `kept` 5,900, `step` 5,208, `between` 4,031, `inner` 1,920, `complete` 1,807, `called` 963, `threads` 262, `wildcell` 64, `leaves` 39, `dispatcher` 20, `uses-vector` 16, `vector` 12, `wildly` 12 and `wild` 1. The answer is one of them.

The heap, each figure at the trace's cycle. On the reference leg, the first cycle after the facts load leaves 111 MB live (gc 15, 117→146→111 MB), and the leg's largest goal is 625 MB, at the cycle that leaves 350 MB live as the whole relation's run ends (gc 27); the trace does not separate the two queries' cycles on this leg. On the default profile, algebra, sharing, fusion and aggregation on, the first cycle after the load leaves 130 MB live (gc 32, 146→183→130 MB), the whole relation's run ends at a 590 MB goal with 346 MB live (gc 44), and the bound query runs under a 693 MB goal with 294 MB live (gc 45). Every cycle on both legs collects to under 400 MB, so the evaluation's working set is the sets and the joins over them, and no leg holds more than the other.

## 5. Tests

Each pins the statement named in §6, written before the change and run red where a red exists.

- `TestRuleEvaluation_AWholeEvaluationHoldsNoSetAfterItsFirstWindow` (executor): an invocation of a whole evaluation continued across a form's windows holds no set of any stratum after its first window, answers the same tuples at every later window, and its complete event reports each stratum's tuple count (S1).
- `TestRuleEvaluation_ASeededEvaluationKeepsItsSetsAcrossTheWindows` (executor): a seeded evaluation's sets are read at a later window and derive what the seed reaches (S1, the arm that keeps them).
- `TestDerivedSet_HoldsEachTupleHeaderOnce` (executor): a set's relation and its index read the tuples the table holds, and the set holds no second slice of them (S2).
- `BenchmarkKernelFacts` (storage), `-benchmem`, before and after: the bytes per operation of the whole relation and the bound query are the measurement each change is judged by.
- `TestRules_TheWholeRelationsOverTheKernelFactsAreTheRecorded`, `TestRules_ABoundInvocationOverTheKernelFactsAnswersTheWholeRelationAtItsBindings`, `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`, `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` and the rule evaluation tests of the executor package, unchanged: the answers.

## 6. What changes

- **S1 (a whole evaluation drops its sets).** After the first window of an evaluation with no seed, nothing reads its sets (P6): no stratum runs again, and the answer is a view over the answer predicate's tuples, which carries no set (T6 of the held-index proof). So once that window's strata have run, the evaluation keeps the answer relation and each stratum's tuple count for its complete event, and drops every stratum's set, its keys and its indexes with it. The answer predicate's tuples stay referenced by the relation alone. A seeded evaluation keeps its sets (P7). Excluded: dropping the sets at `complete`, which a continued evaluation reaches at the form's end, after every window has held them; and dropping a lower stratum's set as soon as the strata above it converge within the first window, which frees the keys and indexes earlier but leaves the tuples referenced by the views the running strata hold, and is a second rule for a part of what S1 frees whole.
- **S2 (one slice of headers).** The set's tuples are the table's members: `tuples` goes, and `relation` and `index` read `keys.members`, which holds the headers in admission order (P3, P4). The ordinal of a tuple stays its position in that slice, so the held-index proof's P7 and T4 hold as stated. Excluded: a table that holds no members and reads keys from the set's slice, which is the layout the one-table proof replaced.
- **S3 (the magic sets).** The bound query's derived tuples outnumber the whole relation's, and the record names the rewrite's choice of adornment as the cause. That is the rewrite's question, decided there, and this proposal changes nothing of it.

## 7. What this proposal does not cover

- The per-pass relations: the views `runStratum` takes per run, the delta relations, and the hash joins' indexes, which the allocation record measures and which are garbage after the pass.
- The js/wasm collector's stall, which retains garbage the evaluation never holds: the perf record states its mechanism and names the allocation rate as its lever.
- A set's index at a key a join reads once, which the held-index proof leaves as it is (its §7).
