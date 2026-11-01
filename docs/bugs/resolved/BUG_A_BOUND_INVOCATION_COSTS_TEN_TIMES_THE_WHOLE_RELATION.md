# A bound invocation costs ten times the whole relation

**Status**: resolved. Each item below names the proof that resolves it, and `PERFORMANCE_STATUS.md` carries the timings under the entry named for each proof.

The reproduction is the downstream's dump,
`datalog/storage/testdata/janus-repro/kernel-facts.jdzl`, with the rule set
the `kernel-facts.jdzl.queries.edn` beside it carries as the `%` input. The
whole relation
is `[:find ?m ?c :in $ % :where (unfilled ?m ?c)]`. The bound query is
`[:find ?r :in $ % :where [?r :relies/cell ?c] [?r :relies/mem ?m] (unfilled ?m ?c)]`.
The figures below are read on the tree at a14c607 through
`go run ./cmd/datalog`: each query once without annotations for its time, the
whole relation as its `(count ?c)`, each once under `-verbose` for its strata,
and the bound query once under `-cpuprofile`. The phase, hash join and
subject-tuple counts are the downstream report's, read under `Analyze` on the
same tree.

## The figures

| | whole | bound |
|---|---:|---:|
| answer | 70,458 pairs | 16 tuples |
| time | 2.76 s | 24.7 s |
| strata | 20 | 19 |
| recursive strata, iterations each | 131, 47, 98, 97 | 2, 131, 2,265 |
| iterations over the recursive strata | 373 | 2,398 |
| tuples derived | 457,286 | 1,055,097 |
| phases, the downstream's count | 1,311 | 110,058 |
| hash joins, the downstream's count | 3,191 | 222,837 |
| subject tuples, the downstream's count | 378,138 | 12,542,762 |

The bound run's CPU profile covers 25.28 s and holds 38.68 s of samples. Of
them, `executor.(*hashJoinIterator).Next` holds 13.10 s cumulative,
`executor.(*JoinIndex).Probe` 9.87 s, `executor.(*TupleSet).FindAt` 12.29 s
cumulative and 5.82 s flat, `executor.hashTuplePositions` 4.71 s,
`datalog.HashValue` 2.39 s flat, `executor.(*JoinIndex).Add` 5.38 s,
`runtime.scanObject` 8.06 s and `runtime.tryDeferToSpanScan` 5.05 s. The time
is the number of probes and builds, not the cost of one.

## The evaluation

The whole relation's twenty strata are the original set's: `vector`,
`uses-vector`, `calls`, `threads`, `leaves`, `flows`, `dispatcher`,
`complete`, `wildcell`, `reach wild`, `open`, `plain`, `inner`, `between`,
`kept`, `run`, `step`, `called`, `through clear passes` and
`unfilled wildly`. Four are recursive: `reach wild` completes at 131
iterations with 9,866 tuples, `run` at 47 with 9,628, `through clear passes`
at 98 with 200,564, and `unfilled wildly` at 97 with 70,470. Every other
stratum is derived in one pass.

The bound run adorns `unfilled` as `bb`. Its nineteen strata are
`magic.bb/unfilled`, `magic.bf/unfilled magic.b/wildly`, then `leaves`,
`flows`, `calls`, `threads`, `dispatcher`, `complete`, `wildcell`,
`wild reach`, `uses-vector`, `plain` and `between` derived whole at the whole
run's counts, then one recursive stratum of fifty-two predicates, then
`magic.bb/calls`, `adorned.bb/calls`, `magic.bb/threads`,
`adorned.bb/threads` and `adorned.bb/unfilled`. Three are recursive:
`magic.bf/unfilled magic.b/wildly` completes at 2 iterations with 24 tuples,
`wild reach` at 131 with 9,866, and the fifty-two-predicate stratum at 2,265
with 917,694. The fifty-two are the magic and adorned predicates of `vector`,
`step`, `through`, `unfilled`, `calls`, `threads`, `run`, `kept`, `open`,
`plain`, `inner`, `passes`, `wildly`, `wildcell`, `flows`, `between`, `clear`,
`dispatcher`, `called` and `reach`, and under the handler that stratum holds
72.05 s of the run's 73.1 s.

The merge is the rewrite's own structure. Each lower predicate's magic
predicate is derived from the adorned predicate of the recursive stratum
whose body invokes it, since the magic rule reads the clauses before the
invocation, the guard and the recursive occurrence among them. The adorned
predicate of the recursive stratum reads the lower predicate's adorned copy
in its body. So every lower predicate the recursion's demand reaches lies in
one strongly connected component with it (`docs/reference/RULES.md`,
"Stratification"; `REWRITE_ORDER_PROOF.md` §1).

## 1. A held relation is drained at every join

**Status**: resolved on this tree under
`docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md`: a derived set
keeps, per key its joins read, an index over its tuples by ordinal, and a
join into the set's view is built on that index, probed with the other
side's tuples and bounded at the view's count. Through the CLI against a
checkout of a14c607 run the same way, the bound query answers in 3.76 s and
3.95 s on two adjacent runs against 24.2 s, and the whole relation's count in
3.05 s and 3.03 s against 2.73 s and 2.80 s. The whole relation's profile on
this tree holds 1.43 s of 3.73 s of samples in `TupleSet.FindAt` under the
kept index's probe, 1.06 s of it flat, against 1.10 s of 3.57 s and 0.62 s
flat at a14c607, at fewer finds; the cause of the per-find cost is not
established.

At a14c607, each phase of the fifty-two-predicate stratum joined a delta of
one to sixteen tuples against a derived relation of hundreds to thousands.
In the phases that followed iteration 1000, a materialized delta of twelve
tuples was the hash side and a materialized derived relation of three
thousand was streamed as the probe side (`join/strategy.selected
build_side:left left_size:12 right_size:3000`), and the phase's second join,
whose left side was the first join's unsized stream, built a derived
relation of 225 tuples whole (`build_side:right left_size:-1
right_size:225`, `join/build.copy passthru:225`). A delta copy's plan leads
with its delta, so the clauses after it read what the delta reaches
(`DELTA_LEADS_ITS_COPY_PROOF.md` T3), but a derived relation was read by a
hash join that either drained it into a fresh index or streamed it whole,
since `derivedSet.relation` answered a materialized relation over the tuples
so far and kept no index on any position (`datalog/executor/rules.go`). So a
phase cost the size of the derived relations it read, not the delta's, and
the profile's probes and builds were that cost summed over the joins.

## 2. A pass carries a delta one hop

**Status**: resolved on this tree under
`docs/proposals/A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md`: a
copy's delta is the tuples its marked predicate gained since the copy last
ran, every other invocation reads the sets as they stand, and a pass visits
the copies in the postorder of a depth-first walk of the stratum's
dependency graph, absorbing each copy's result before the next is visited,
so a pass carries a derivation along every edge but a back edge. The
annotated bound query completes its fifty-two-predicate stratum at 711
passes against 2,265 iterations at a14c607, at the same 917,694 tuples,
`wild reach` at 128 against 131, and the two-predicate magic stratum at 1
against 2, and runs 45.1 s under the handler against 73.1 s. Through the
CLI, two adjacent runs: the bound query answers in 3.74 s and 3.64 s against
3.76 s and 3.95 s on the tree with item 1's change alone, and the whole
relation's count in 2.95 s and 3.20 s against 3.05 s and 3.03 s.

At a14c607, iteration 1000 of the fifty-two-predicate stratum added one
tuple each to `magic.bf/step`, `adorned.b/plain`, `magic.bf/inner`,
`adorned.bf/run`, `magic.b/between`, `adorned.b/between`, `magic.bf/flows`,
`magic.b/reach` and `adorned.b/reach`, twelve to `adorned.bb/through` and
sixteen to `magic.bb/flows`. Iteration 1004 added twelve to
`adorned.bf/unfilled` and to `adorned.bbf/clear` and one each to nine other
predicates. A new tuple of `adorned.bf/unfilled` fed `magic.bf/step` at the
next iteration, `adorned.bf/step` needed `adorned.bf/run` the iteration
after, and so on down the chain of lower predicates and back up: every
iteration read the deltas the iteration before added and nothing added in
its own run (`runStratum`, `datalog/executor/rules.go`). So one hop of the
original recursion cost as many iterations as the chain of lower predicates
was deep, 2,265 against the whole relation's 373, and each iteration ran
the copies of every predicate that gained a tuple.

## 3. A predicate derived whole is derived again adorned

**Status**: resolved on this tree under
`docs/proposals/A_PREDICATE_DERIVED_WHOLE_IS_READ_WHOLE_PROOF.md`.
`PERFORMANCE_STATUS.md`, the entry "A predicate derived whole is read whole",
carries the change and the timings of the bound query and the whole
relation's count under it.

The rewrite as it stood adorned a predicate at every bound site, so `flows`,
`calls`, `threads`, `dispatcher`, `plain`, `wildcell`, `between` and `reach`,
derived whole for the negations and the free invocation of `wild` that kept
their original rules in the set, were derived again under one or more
adornments inside the fifty-two-predicate stratum: `flows` under `bf`, `fb`
and `bb`, `calls` under the same three, `reach` under `b`. Each adorned copy
derived again, with its magic rules, a subset of a relation the evaluation
had already derived whole.

## What follows

Item 1 is the per-join cost and generalizes: every join of a delta into a
derived relation, in the whole evaluation as in the bound one, drains or
streams the relation. Item 2 is the iteration structure of a stratum the
rewrite merged. Item 3 is a dominance in the rewrite. The downstream's
acceptance is the bound query answering in no more time than the whole
relation over the same rules.
