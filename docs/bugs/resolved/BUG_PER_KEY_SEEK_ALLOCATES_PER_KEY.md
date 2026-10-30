# The per-key seek opens one scan per key, and the opens cost nothing the one scan does not

**Status**: Resolved, and its measurement superseded. `BenchmarkPerKeySeek`
opened its database with the EA cache on, under which a binding at the
entity position is served from the cache and the per-key seek arm never
runs, so every figure below is the cache path's and not the arm's. The
benchmark opens the cache off now, and the arm opens one scan over the
pattern's extent sought per key (`docs/proposals/ONE_SCAN_SOUGHT_PER_KEY_PROOF.md`);
its figures are in `PERFORMANCE_STATUS.md` under "One scan sought per key".

The seek answers the keys' datoms, so no answer is wrong. This was a cost
item. The one measured class the window branch loses to its merge-base is
the one where the per-key seek replaces a scan, and the allocation each
seek pays had not been measured. `BenchmarkPerKeySeek` was taken to have
measured it, and the conclusion drawn, that the seek's opens and encodings
cost nothing the one scan under the same binding does not, rested on the
cache path.

## The strategy

`matchWithHashJoin` (`datalog/storage/hash_join_matcher.go`) serves a binding
of several keys at or below `executor.SmallBindingBoundary`, or at the
attribute position at any count, by one narrowed scan per distinct key when
the index binds every key, reported as `annotations.ScanPerKeySeek`.
`perKeySeekIterator.Next` drains the open scan and opens the next key's
through `OpenKeyScan`, so a binding of N keys opens N scans, each with its
encoded bound and its own iterator. One key past the boundary the join
scans the extent once and probes the binding's index.

## The question

`PERFORMANCE_STATUS.md`, the entry "Scope forms under the window, with the
per-key seek": the or-join over a materialized subject is the one class the
branch loses, three times the merge-base's time at a thousand tuples on both
backends, with three to nine times the allocations. The merge-base built
that branch once with one scan of the attribute. The branch seeks each key
of each window, and at a thousand tuples the subject's keys are the
attribute's whole extent, so a thousand seeks replace one scan of a thousand
datoms. The ratio did not say whether the cost was in the N opens, in the N
encodings, or in neither.

## The measurement

`BenchmarkPerKeySeek` (`datalog/storage/per_key_seek_benchmark_test.go`)
runs `[:find ?e ?v :in $ [?e ...] :where [?e :t/n ?v]]` with a collection of
k keys over an attribute carried by n entities. The keys name the fixture's
first k entities. It runs at k of one, thirty-two, the boundary's thousand
and one past it, over a thousand and a hundred thousand entities, on every
backend and in both optimizer modes. A thousand keys over a thousand
entities is the window's case above: the keys are the attribute's whole
extent, served by a thousand seeks. The arm one key past it serves the
same extent by one scan and a probe. The two modes agree on every cell, so each cell below pools
them, ten runs of five per mode, on the tree at `8565cdd` with the benchmark
added, on an Apple M5.

Allocations per op:

| entities, backend | keys 1 | keys 32 | keys 1000 (seeks) | keys 1001 (one scan) |
|---|---|---|---|---|
| 1,000, memory | 388 | 699 | 9,446–9,447 | 9,460–9,461 |
| 1,000, memory-trees | 118 | 429 | 9,174–9,175 | 9,183 |
| 1,000, badger | 137 | 448 | 9,197–9,198 | 9,224–9,225 |
| 100,000, memory | 388 | 699 | 9,446–9,450 | 9,455–9,458 |
| 100,000, memory-trees | 118 | 429 | 9,176–9,181 | 9,185–9,188 |
| 100,000, badger | 146 | 457–458 | 9,212–9,215 | 9,221–9,228 |

Time per op, in µs:

| entities, backend | keys 1000 (seeks) | keys 1001 (one scan) |
|---|---|---|
| 1,000, memory | 332–354 | 336–356 |
| 1,000, memory-trees | 374–390 | 395–410 |
| 1,000, badger | 272–282 | 269–301 |
| 100,000, memory | 451–506 | 413–488 |
| 100,000, memory-trees | 595–1,665 | 492–720 |
| 100,000, badger | 369–454 | 388–474 |

One memory run at a hundred thousand entities and a thousand keys read
8,413 µs and 9,533 allocations, outside every other run of its cell, and
is left out of the ranges.

From one key to thirty-two the count rises by 311 on every backend, ten per
key. From thirty-two to a thousand it rises by 8,745 to 8,758, nine per
key. The arm one key past the boundary allocates four to twenty-eight more
than the seek arm, within three keys' share, at both extents. So a seek
allocates what the one scan's probe of the same key allocates, and the
thousand opens, with their bounds and iterators, add nothing the counts can
see.

## What follows

Neither fix the open item named follows. An encoder and iterator reused
across the keys could save at most the difference between the arms, which
is within three keys' share. The one-scan arm when the keys cover the
extent is the keys-1001 arm at a thousand entities, and it allocates the
same and runs in the same time. The materialized leg's three to nine times
the merge-base's allocations are therefore not the seek's form. What that
leg pays over an unbound scan of the attribute, the binding path both arms
share, this benchmark does not measure, and the entry prices the leg as a
path the scope design removes.

On memory-trees over a hundred thousand entities the seek arm's time stands
above the one-scan arm's, 595–1,665 µs against 492–720, where on memory
and badger the two overlap. No cause for that leg is established.
