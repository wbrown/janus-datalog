# One scan sought per key, as a proof

## Sources

- The code: `datalog/storage/per_key_seek_iterator.go` (`perKeySeekIterator`), `datalog/storage/hash_join_matcher.go` (`matchWithHashJoin`, `keyScanBounds`, `patternScanBound`), `datalog/storage/memory_tree_iterator.go` (`storeVersion.scan`, `boundProbeDatom`, `memoryTreeIterator.Seek`, `memoryTreeIterator.Next`), `datalog/storage/memory_store.go` (`memoryIterator.Seek`, `memoryIterator.Next`), `datalog/storage/datom_decoder_badger.go` (`KeyOnlyIterator.Seek`), `datalog/storage/badger_store.go` (`BadgerIterator.Seek`), `datalog/storage/scan_report.go` (`scanReport`, `OpenKeyScan`, `reportedIterator`), `datalog/storage/store.go` (the `Iterator` interface), `datalog/annotations/types.go` (`KeyScansOpened`), `datalog/annotations/output.go` (the scan completion's rendering).
- The design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.6, which states the per-key seek strategy and that its completion event carries the funnel and the key count.
- The tests that pin the strategy: `TestPerKeySeekAtOrBelowTheBoundary` and `TestQueryBindingIsSoughtPerKeyAtOrBelowTheBoundary` (`datalog/storage/per_key_seek_test.go`), and the two of `datalog/storage/per_key_seek_allocation_test.go`, written before this change.
- The decision: the per-key seek arm opens one scan over the pattern's extent and seeks it to each key's run, in place of opening one scan per key, and the completion event's count in the bound's place is named for the runs the arm addressed.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- The **extent** of a pattern under an index is the bound `patternScanBound` answers: the pattern's constants bound to the index's leading components.
- A **key's run** is the bound `patternScanBoundWithBinding` answers for the key: the extent's prefix with the key bound after it.
- A **scan** is the iterator `OpenKeyScan` answers for a bound, wrapped in the report's accounting.
- A **seek** is `Iterator.Seek`: the iterator narrowed to the run a bound names, its start, its end and its membership.
- The **stream** is `perKeySeekIterator`: the datoms of every key's run, in the keys' order, as one iterator the hash join consumes.
- The **peers** of a report are the runs an arm addressed when it addressed no single run, counted by the arm and emitted in the bound's place.
- The **probe** of a memory-trees iterator is the datom its comparisons read the bound's values from.

## 2. Preconditions

- **P1 (the seek).** On every backend a seek narrows an open iterator to the run its bound names and keeps the iterator's intake count. The memory-trees iterator replaces its probe and its bound count and restarts from the tree at the run's start. The byte-keyed memory iterator replaces its end and its membership, and refills from the tree at the run's start when the run lies outside the chunk in hand. The Badger iterator replaces its end and its membership and seeks its own iterator to the run's start, an absolute seek.
- **P2 (every key's run lies in the extent).** `keyScanBounds` answers bounds only when every key's run extends the extent's prefix. A run whose prefix extends another's lies inside it.
- **P3 (the byte-keyed memory scan's range).** The byte-keyed memory iterator clamps a seek's start to the scan's own range start. So a scan opened over one key's run cannot reach a run before it, and a scan opened over the extent reaches every key's run.
- **P4 (the stream as it stands).** The stream opens one scan per key through `OpenKeyScan`, counts one peer per key, closes each scan as its run ends and accrues its intake then, keeps the first error of a scan and its close, and panics on a seek of its own, since the join never seeks it.
- **P5 (the report).** `peers` is emitted under `scans.opened` when the report names no single run. Every writer of that key today addresses one run per scan it opens: the stream per key, the per-binding arm per binding, the prefetch per entity, the entity resolve per scan, and the batch pull and `ResolveAllAttributes`, which write one beside a named bound. The formatter renders the count as scans over bindings. The two seek tests assert the count equals the distinct keys. §4.6 of the scope design says the event carries the key count.
- **P6 (the probe).** The memory-trees scan allocates its probe once through `boundProbeDatom`, and `Seek` allocates a fresh one the same way. The probe is read by the iterator's own prefix comparisons and by nothing else.
- **P7 (intake).** Each backend counts a datom as intake when its walk takes it in, and none counts the datom at which a run's end is found. The memory-trees walk counts after the prefix check, and the byte-keyed walks return at the end before counting.

## 3. Theorems

**T1 (answers unchanged).** The stream yields each key's datoms, in the keys' order, as it yielded them, and the arm's intake is the keys' datoms, as it was.

**T2 (one scan).** The arm opens one scan whatever the key count, and seeks it once per key.

**T3 (the report).** The completion event carries, in the bound's place, the runs the arm addressed, one per key, under a key named for that fact, and every other writer's value under the key is what it was.

**T4 (allocation).** On memory-trees a key sought costs the arm what the key's bound costs and nothing more, and a seek of the memory-trees iterator allocates nothing.

## 4. Lemmas

### L1. A seek inside the extent answers the key's run (T1)

By P2 every key's run lies inside the extent, and by P1 a seek narrows the open scan to the run the key's bound names, its end and its membership included. So the datoms the stream yields after the seek are the key's run's, which are the datoms the scan opened over that run yielded under P4. ∎

### L2. The keys' order is kept (T1)

The stream seeks the keys in the order the binding yields them, a key whose run lies before the one just read included. The memory-trees iterator restarts from the tree at the run's start (P1). The byte-keyed memory iterator refills from the run's start, which lies at or after the extent's start, the scan's range start, by P2 and P3. The Badger iterator's seek is absolute (P1). So each key's run is reached wherever it lies in the extent. ∎

### L3. Intake accrues once (T1)

Each backend's intake count accumulates across seeks (P1), and the datom at which a run's end is found is not counted (P7). The stream closes its one scan once, and the report accrues the scan's count then. So the arm's intake is the sum of the keys' runs' datoms, as it was under one scan per key. ∎

### L4. The report's value is unchanged (T3)

The stream counts one peer per key as it seeks, as it counted one per scan it opened (P4). Every other writer addresses the runs it addressed (P5). So the number under the key is what it was for every writer, and the key is renamed for the fact every writer reports, the runs the arm addressed. ∎

### L5. The probe filled in place (T4)

The probe is private to the iterator and read by its own comparisons alone (P6). A seek that writes the bound's values into the probe it holds, every position of it, leaves the comparisons reading what a fresh probe would carry. So the seek allocates nothing. ∎

### L6. A key's cost (T2, T4)

The arm opens its scan at the first pull and never again (T2). Per key it computes the key's bound, seeks, and counts a peer. The bound's cost is the bound's, the seek allocates nothing on memory-trees (L5), and the count is a field. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestPerKeySeekAllocatesPerKeyWhatItsBoundsAllocate` (storage, `per_key_seek_allocation_test.go`): on memory-trees, a binding of several keys at the entity position, matched and drained, allocates per key beyond a smaller binding what `keyScanBounds` allocates per key, and nothing more (T2, T4, L6).
- `TestMemoryTreeIterator_SeekAllocatesNothing` (storage, `per_key_seek_allocation_test.go`): a seek of an open memory-trees scan allocates nothing, and the datoms it yields after are the sought run's, for a run after and a run before the one read last (T4, L5, L2).
- `TestPerKeySeekAtOrBelowTheBoundary` and `TestQueryBindingIsSoughtPerKeyAtOrBelowTheBoundary` (storage), reading the renamed key, their assertion that it equals the distinct keys unchanged, under the message that the arm addresses one run per distinct key (T1, T3, L1, L2, L3).
- The scan bound reporting, scan funnel reporting, V-validation reporting and AEVT matcher tests' reads of the key (storage), the constant renamed and their values unchanged (T3, L4).
- The storage package's suite, the binding-driven match tests, the per-key seek benchmark's correctness and the two optimizer-mode differentials among it, run unchanged (T1).

## 6. The change the proof licenses

- `per_key_seek_iterator.go`, `perKeySeekIterator`:
  - A field holding the extent, the bound the one scan is opened over.
  - `Next` opens the scan once, at the first pull, and seeks it to each key's bound in turn, counting a peer per key.
  - A seek's failure, the iterator's sticky error, ends the stream before the key's run is read as empty.
  - `Close` closes the one scan.
- `hash_join_matcher.go`, `matchWithHashJoin`: the stream is built with the extent beside the keys' bounds.
- `memory_tree_iterator.go`:
  - `boundProbeDatom` fills a probe the caller holds, every position of it.
  - `storeVersion.scan` allocates the probe once and fills it.
  - `Seek` fills the iterator's probe in place.
- `annotations/types.go`: `KeyScansOpened` and its string become a key named for the runs an arm addressed, its doc stating that fact.
- `annotations/output.go`: the per-binding and per-key rendering names runs.
- The six writers of the key, the renamed constant, their values unchanged.
- `scan_report.go`: the comments at `peers` and at `close` name the runs addressed.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.6: the strategy's sentence states one scan of the extent sought per distinct key.
- The tests of §5.

## 7. What the proof does not cover

- The cost of a seek on the byte-keyed memory store and on Badger, each of which encodes the run per seek. The memory store is no measurement target, and the Badger seek's cost is the per-key seek benchmark's to measure.
- The time the arm's opens cost the reproduction. The reproduction and the per-key seek benchmark measure it.
- The allocation of each key's bound, two appends to its prefix per key, which the arm paid before and pays after.

## 8. Alternatives excluded

- The scan opened over the first key's run and sought from there. P3 excludes it: the byte-keyed memory iterator cannot reach a run before the scan's range start.
- The keys sorted into the index's order before they are sought, so that every seek moves forward. The seek reaches any run of the index on every backend (L2), and the sort costs the arm a pass over the keys it does not need.
- `scans.opened` kept as the count of scans, one under this change, beside a new key for the key count. Two keys for a fact that is one number at every writer but the stream, and a constant under the first for the arm the design names the count for.
- A probe allocated per seek, as it stands. One allocation per key on the backend the reproduction runs on, for a value no reader outside the iterator holds.
