# The magic sets outnumber the tuples they save

**Status**: open.

The reproduction is the bound query of the kernel facts,
`[:find ?r :in $ % :where [?r :relies/cell ?c] [?r :relies/mem ?m] (unfilled ?m ?c)]`,
over the storage package's fixture `datalog/storage/testdata/janus-repro/kernel-facts.jdzl`
with the rule set its `kernel-facts.jdzl.queries.edn` carries as the `%`
input, against the whole relation `[:find ?m ?c :in $ % :where (unfilled ?m ?c)]`
over the same rules. `TestRules_ABoundInvocationOverTheKernelFactsReportsLessWorkThanTheWholeRelation`
(`datalog/storage/kernel_facts_test.go`) logs both runs' events, and
`BenchmarkKernelFacts` measures both queries; the allocation profiles below
are the benchmark's `bound` and `unfilled` cells on memory-trees with the
algebra optimizer on, one execution each under `-memprofile`, read with
`go tool pprof -sample_index=alloc_space`. Every figure is read on the tree
at de67b6a on an Apple M5.

## The figures

| | whole | bound |
|---|---:|---:|
| answer | 70,458 pairs | 16 tuples |
| time, count 3 | 2.90–3.15 s | 1.52–1.92 s |
| bytes, count 3 | 516 MB | 898–912 MB |
| objects, count 3 | 2.99 M | 6.32–6.58 M |
| tuples derived | 457,286 | 693,918 |
| tuples the joins probed | 113,271,348 | 41,398,269 |
| tuples the joins drained | 142,059 | 93,752 |
| subject tuples read into windows | 378,138 | 423,039 |
| hash joins | 3,126 | 48,425 |
| phases | 1,256 | 22,342 |

The bound query answers in about half the whole relation's time and reads a
third of the tuples through its joins, which is the acceptance
`BUG_A_BOUND_INVOCATION_COSTS_TEN_TIMES_THE_WHOLE_RELATION.md` records. It
allocates 1.7× the whole relation's bytes and 2.1× its objects, and derives
1.5× its tuples, for sixteen answers.

## The evaluation

The whole run derives twenty strata, 457,286 tuples: `open` 119,152,
`through` 118,912, `unfilled` 70,458, `clear` 70,326, `passes` 11,326,
`calls` 10,044, `reach` 9,865, `run` 9,628, `flows` 9,309, `plain` 8,011,
`kept` 5,900, `step` 5,208, `between` 4,031, `inner` 1,920, `complete`
1,807, `called` 963, `threads` 262, `wildcell` 64, `leaves` 39,
`dispatcher` 20, `uses-vector` 16, `vector` 12, `wildly` 12 and `wild` 1.

The bound run derives 693,918 tuples. Of them 43,469 are the predicates
derived whole, at the whole run's counts: `calls`, `reach`, `flows`,
`plain`, `between`, `complete`, `threads`, `wildcell`, `leaves`,
`dispatcher`, `uses-vector` and `wild`. The rest are the rewrite's own
predicates, each magic set beside the adorned copy it restricts, with the
whole run's relation beside them:

| predicate | magic set | adorned copy | whole |
|---|---:|---:|---:|
| `unfilled` `bf` | 12 | 70,458 | 70,458 |
| `unfilled` `bb` | 16 | 16 | |
| `through` `bb` | 70,458 | 70,094 | 118,912 |
| `open` `bb` | 70,458 | 70,322 | 119,152 |
| `clear` `bbf` | 5,937 | 39,688 | 70,326 |
| `step` `bb` | 93,878 | 19 | 5,208 |
| `step` `bf` | 5,843 | 5,191 | |
| `run` `bb` | 93,878 | 19 | 9,628 |
| `run` `bf` | 5,873 | 9,601 | |
| `kept` `b` | 9,847 | 5,855 | 5,900 |
| `passes` `bb` | 5,937 | 5,841 | 11,326 |
| `inner` `bf` | 5,862 | 1,902 | 1,920 |
| `inner` `fb` | 495 | 1,911 | |
| `called` `b` | 495 | 495 | 963 |
| `vector` `b` | 12 | 12 | 12 |
| `wildly` `b` | 12 | 12 | 12 |

The magic sets hold 369,013 tuples and the adorned copies 281,436. The whole
run's relations of the same predicates hold 413,817, so the copies save
132,381 tuples and the magic sets that restrict them cost 2.8 times that.

## 1. A site bound at more positions than a sibling site is adorned apart, and its magic set is the sibling's demand times the extra bindings

`unfilled`'s recursive body is
`[(unfilled ?m ?n) (unfilled ?m ?c) (step ?c ?n) (through ?c ?m)]`. Under
the root's `bb` adornment, `m` and `n` bound, the recursive occurrence binds
`m` alone and is `bf`, and it provides `c`; so at `(step ?c ?n)` both
positions are bound, the site is adorned `bb`, and its magic rule reads the
seed and the occurrence: `magic.bb/step` holds, for each of the sixteen
demanded pairs, every cell the recursion reaches for the pair's vector,
16 × 5,867 = 93,878 tuples, and `adorned.bb/step` holds 19. The same body
under the `bf` copy, `n` free, adorns `(step ?c ?n)` `bf`, with the magic
set the cells the recursion reaches, 5,843, and the copy 5,191 tuples. The
`bb` copy's answers are among the `bf` copy's: both demands project onto
the same cells at the first position, and the `bf` copy holds every step
from those cells. `run`, invoked from `step`'s body under the same
bindings, is the same again: 93,878 magic tuples for 19 answers beside a
`bf` copy of 9,601.

The rewrite adorns each site by the positions bound at it
(`adornInvocation`, `docs/proposals/REWRITE_ORDER_PROOF.md` §1), and reads
nothing of the other sites of the predicate, so a predicate demanded under
two binding patterns is derived once per pattern, and the pattern that
binds a position the sibling leaves free pays the product over that
position's bindings, 16 here, for a copy the sibling already holds.

## 2. A magic set as large as the relation it restricts

At `(through ?c ?m)` in the same body both positions are bound from the
recursion, and `magic.bb/through` holds every pair the recursion reaches,
70,458, the whole `unfilled` relation; `adorned.bb/through` holds 70,094
against the whole run's 118,912 for `through`. `open`, invoked from
`through`'s bodies, is restricted the same way: a magic set of 70,458 for a
copy of 70,322 against 119,152. The two magic sets cost 140,916 tuples to
save 97,648.

The demand here covers the relation. `wildly` puts every word's entry into
`unfilled` for a vector once a wild cell is reached, and the kernel's entry
reaches its wild jumps, so `adorned.bf/unfilled` holds the whole relation,
70,458 pairs, and the bound query derives it before its sixteen answers are
read off it. A magic set over a demand that covers the relation restricts
nothing and costs its own size at every pass that reads it.

## 3. The passes

The bound run's rewritten stratum takes hundreds of passes, each visiting
every delta copy and running those whose delta is not empty, so it
completes 48,425 hash joins and 22,342 phases against the whole run's
3,126 and 1,256. The fixed cost of a copy run, the delta relation and the
views made for it, the renames, the join's and the stream's constructions
and the scope per run, comes to about 83 MB of the 972 MB the bound run's
evaluation allocates, read from the profile's flat bytes at
`newMaterializedRelationFromSet`, `NewStreamingRelationWithProperties`,
`HashJoinWithOptions`, `executeRuleInvocation`, `stratumEvaluation.run`,
`DefaultQueryExecutor.Execute`, `Relations.Collapse` and
`executeBoundPattern`. The counts are passes times copies, and their cost is
a tenth of the allocation.

## The allocation

The bound profile samples 1,341 MB, 972 MB of it under the rule evaluation
(`executeRuleInvocation`), the rest the fixture's load. Its leaders, flat
unless cumulative is named: `TupleSet.grow` 258 MB, all under `TupleSet.Add`
from the kept indexes' extension (82 MB), the sets' absorption (75 MB), the
dedup of each copy's result (49 MB), the frame records (28 MB), the
materialized dedup (18 MB) and the fresh join indexes (6 MB);
`derivedSet.absorb` 130 MB flat and 205 MB cumulative; the pass's appends of
the tuples each copy added, 97 MB; the fused attribute fetches 162 MB
cumulative, one point lookup per tuple of the group, 107 MB of it under
`LookupAttribute`; the kept indexes' extension 107 MB cumulative; the
projection of each copy's result 27 MB flat and 247 MB cumulative; the
window reader's copies 91 MB cumulative and the frame runs 53 MB; the joins
143 MB cumulative.

The whole profile samples 930 MB, 540 MB under the evaluation: `TupleSet.grow`
121 MB, `derivedSet.absorb` 100 MB flat and 140 MB cumulative, the fused
fetches 132 MB cumulative, the projection 183 MB cumulative, the window
reader 65 MB and the frame runs 46 MB, the kept indexes' extension 35 MB.

Every leader is per tuple derived, indexed, projected or read, and the
bound run derives 1.5 times the whole run's tuples: item 1's product and
item 2's magic sets are where the bytes go.

## What follows

Item 1 is a question for the rewrite: whether a site whose bound positions
include a sibling site's, with the sibling's copy derived in the same set,
reads the sibling's copy and filters at its extra positions, or whether the
adornment of a site is chosen to bound the magic set rather than to bind
every available position. Item 2 is the shape of this program's demand and
of any demand that covers its relation; what it costs is the magic sets'
size at every pass that reads them. Item 3 is measured and small. The
decision is the user's.
