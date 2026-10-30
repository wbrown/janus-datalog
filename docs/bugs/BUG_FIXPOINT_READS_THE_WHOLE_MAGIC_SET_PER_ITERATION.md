# The fixpoint reads the whole magic set at every iteration, and the magic set is the whole graph per key

**Status**: resolved; each item below names the proof it is resolved under.

The reproduction is the `unfilled` query of `JANUS_REPRO_FILLED.md` over
`janus-repro/kernel-facts.jdzl`, with the rule set under that document's
heading "The rules" passed as the `%` input. Its answer is the empty
relation. Every figure below is read on the tree at `a495238` on an Apple
M5. The store holds 980 cells under `[?x :cell/ends true]` and 9,348
`:cell/next` datoms, counted by one query over the dump.

## The profile

A CPU profile of the run, read with `go tool pprof -top`, covers 16.25 s and
holds 16.88 s of samples. The leaders, cumulative unless flat is named:

| function | seconds |
|---|---|
| `executor.(*CountingIterator).Next` | 13.88 |
| `executor.(*hashJoinIterator).Next` | 12.84, 2.34 flat |
| `executor.(*JoinIndex).Probe` | 7.64 |
| `executor.(*TupleSet).FindAt` | 6.98, 2.55 flat |
| `executor.hashTuplePositions` | 3.88 |
| `datalog.HashValue` | 2.10 flat |
| `runtime.typedslicecopy` | 1.40 |

Hashing the probe's key, `hashTuplePositions`, is 3.88 s of `FindAt`'s
6.98 s. The time is the number of probes, not the cost of one.
`windowReader.next` holds 12.75 s, all of it under
`windowedIterator.nextWindow` and 12.63 s of it in `CountingIterator.Next`.

## The evaluation

Under an annotation handler the run reports three strata. `rules/magic-sets`
adorns `unfilled` as `bb`. The stratum of `magic.bb/unfilled` completes at 11
iterations with 46 tuples. The stratum of `magic.bf/unfilled` completes at 1
iteration with 1 tuple. The third stratum holds twenty predicates,
`adorned.bb/unfilled`, `magic.b/vector`, `magic.bb/through`,
`magic.bb/calls`, `adorned.b/vector`, `adorned.bb/through`, `magic.bb/open`,
`magic.b/plain`, `magic.bf/inner`, `magic.bb/passes`, `adorned.bf/unfilled`,
`magic.bf/calls`, `adorned.bb/calls`, `adorned.bb/open`, `adorned.b/plain`,
`adorned.bf/inner`, `adorned.bb/passes`, `magic.bbb/clear`,
`adorned.bf/calls` and `adorned.bbb/clear`, and completes at 1,123 iterations
with 384,230 tuples. Of those, 375,554 are `magic.bbb/clear`. Its
iterations add a few tuples at a time: at iteration 74 two predicates gain
one tuple each, at iteration 600 six predicates gain one to three and
`magic.bbb/clear` gains 517, at iteration 1122 `magic.bbb/clear` alone
gains one, and iteration 1123 adds nothing. The handler's own cost makes
that run's timings its own, so the figures taken from it are counts.

The window forms of that run report 9,498,276 `scope/subject.tuple` events.

## 1. The magic set carries a position a scan bound

**Status**: resolved under
`docs/proposals/A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md`.
A position of an invocation is bound from a clause outside the seed
component only when the clause is a data pattern with an entity constant,
which names one entity, or the predicate requires the position; the
planner's order and a query's own invocation are as they were. In the
rewritten set a verbose run renders, `clear` is adorned `bbf`, and by T2 of
that proof its magic rules read the guard alone, `[?x :cell/ends true]`
sharing no variable with the bound positions. Under an annotation handler
the twenty-predicate stratum completes at 1,074 iterations with 8,714
tuples, against 1,123 iterations with 384,230 tuples below, and the run
reports 29,769 `scope/subject.tuple` events against 9,498,276. The finding
below describes the tree at `a495238`.

On that tree `clear` was adorned `bbb`. `passes` invokes it as
`(clear ?m ?k ?x)` under `k` and `m` bound, with `[?x :cell/ends true]`
beside it. The pattern holds a constant at the value position, so
`scansAWholeExtent` (`datalog/planner/clause_utils.go`) answers false for
it, `invocationDependsOnPendingProvider` holds the invocation behind it, and
`x` was bound when the invocation was adorned. `REWRITE_ORDER_PROOF.md` T2a
prices the seeding: the magic set under `bbb` was the magic set under `bb`
times the scan's extent, the 980 cells marked as ends. F5 of that proof
named the bet: a clause with a constant beyond its name, outside the seed
component, may precede the invocation, and the product it made was bounded
by what its constants leave of its relation.

In `clear`'s own recursive body,
`[(clear ?m ?k ?n) (clear ?m ?k ?c) [?c :cell/next ?n] (through ?c ?m)]`,
the recursive occurrence is held behind `[?c :cell/next ?n]` the same way,
`n` being available from the guard. By the definition of a magic rule the
proof then carried, the occurrence's magic rule was
`magic.bbb/clear(m,k,c) :- magic.bbb/clear(m,k,n) [?c :cell/next ?n]`.
Under one `[?m ?k]` key the magic set was therefore every cell from which an
end is reached along `:cell/next`. Two tuples of `adorned.bbb/clear`'s delta
matched 19,205 tuples of the magic set on `[?m ?k]`, about 9,600 under each
key, against the store's 9,348 `:cell/next` datoms.

Under `bbf` the magic set would hold one tuple per `[?m ?k]` key, the pairs
`passes` is asked with. `adorned.bbb/clear` was the same relation under
either adornment (T2b). The `bbb` adornment restricted nothing here and cost
the magic set's size at every read of it (T2c).

## 2. A delta copy joins its whole relations before its delta

**Status**: resolved under `docs/proposals/DELTA_LEADS_ITS_COPY_PROOF.md`
and `docs/proposals/A_VALUE_BINDING_ABOVE_THE_BOUNDARY_SCANS_ONCE_PROOF.md`.
A delta invocation ranks above every other clause of its copy, so the copy's
joins read the delta's tuples and what they reach, and a pattern binding the
delta at its value position on a cardinality-one attribute is served above
the matcher's boundary by one resolved scan of the attribute, where a delta
of the closure benchmark's size paid a candidate scan per key. The finding
stands below as it was made.

The rewritten recursive body of `clear` is `magic.bbb/clear(m,k,n)`,
`adorned.bbb/clear(m,k,c)`, `[?c :cell/next ?n]`, `adorned.bb/through(c,m)`.
Its delta copy on `through` runs at every iteration as the join of
`[?m ?k ?n]`, the magic set, with `[?m ?k ?c]`, `adorned.bbb/clear`, on
`[?m ?k]`, then the join with `[?c ?m]`, `delta/adorned.bb/through`, on
`[?m ?c]`, then the pattern as a window form over the stream. The first join
builds on the `clear` side and streams the magic set. Early in the run it
reads 9,880 magic tuples against 11 `clear` tuples and answers 108,680. At
the end it reads 375,540 against 474, matches 370,795 and answers
4,675,024. The join after it holds one delta tuple or none, so it answers
nothing or a few thousand. The run holds 289 `join/probe.complete` events
whose result passes a million tuples and 1,145 whose probe passes a hundred
thousand.

The cause is the order. `scoreClause` scores an invocation 80, plus 100 per
constant argument, 10 per available argument, 10 per symbol it provides, and
500 when `SeedRestricted`, which the delta of a seed-restricted predicate
is. In the delta copy every invocation is seed-restricted, so the two
three-argument invocations outscore the two-argument delta by the symbols
they provide, and `Relations.Collapse` (`datalog/executor/relations.go`)
joins the relations in list order. The delta, the smallest relation of the
copy by construction, is read nowhere in the order.

With the delta first, the copy's joins would read the delta's tuple, the
`clear` tuples holding its `c`, and the magic tuples under each of their
`[?m ?k]` keys: tens of thousands of tuples against millions. With the
pattern read at the delta's `c` before the magic set, tens.

The delta copy on `clear`'s own occurrence reads the magic set the same way.
At one iteration it streams 248,508 magic tuples against two delta tuples
and answers 19,205, each of which the window form then checks against
`:cell/next`. The subject tuples the window forms report are that stream
across the run.

With item 1 resolved, the magic set is one tuple per key and both copies read
a few hundred tuples per iteration. The order stays wrong for a body whose
whole relations are large under a small magic set.

## 3. A delta copy runs with an empty delta

**Status**: resolved under `docs/proposals/DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md`.
`PlannedBody.Marked` names the predicate whose delta symbol a copy invokes,
and `runStratum` runs a delta copy only when that predicate gained tuples in
the iteration before. The finding stands below as it was made.

`runStratum` (`datalog/executor/rules.go`) held every predicate's delta at
every iteration, an empty one as an empty relation, and runs every plan of
`bodies.Delta`. A copy whose delta is empty derives nothing. Its plan still
runs, and the joins before the delta are built: in an iteration whose
`through` delta is empty, the copy of item 2 builds the index over
`adorned.bbb/clear`'s 474 tuples and opens the join, and the empty delta
ends it. A lower copy is skipped when its lower delta is empty, through the
`Lower` field of `PlannedBody`. A delta copy has no field naming the
predicate whose delta it invokes. The profile bounds this item at the time
outside the joins, under 2.5 s.

## What follows

Item 1 is the root, and items 2 and 3 stand on their own with or without it.

Item 1 is a rule for the positions the rewrite binds from a clause outside
the seed component, which `REWRITE_ORDER_PROOF.md` F5 leaves as a bet. Under
the bet, binding such a position multiplies the magic set by the clause's
extent and restricts the adorned relation to that extent's values. The win
is a smaller derivation when the extent is small. The loss is the product,
carried through every recursive magic rule under the bound position, when
the extent is large. The engine does not see the extent (SCOPE §4.6). It
sees a constant at the entity position, which names one entity, and the
schema's uniqueness of an attribute, under which a constant at the value
position names at most one entity. The rule and its proof are the user's
decision.

Item 2 is an order within a delta copy: the delta before every other clause,
and after it the clauses by what the available symbols let them seek, under
`REWRITE_ORDER_PROOF.md`'s rank and score.

Item 3 is the field a delta copy lacks: `PlannedBody` naming the predicate
whose delta the copy invokes, as `Lower` names a lower copy's, and
`runStratum` skipping a delta copy whose delta holds no tuple, as it skips a
lower copy.
