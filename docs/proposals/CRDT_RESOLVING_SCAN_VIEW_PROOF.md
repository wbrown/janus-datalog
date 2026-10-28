# The CRDT-resolving scan's view, as a proof

## Sources

- The code: `datalog/storage/crdt_resolving_iterator.go` (`CRDTResolvingIterator`, `NewCRDTResolvingIterator`, `CRDTResolvingIterator.Next`, `startNewGroup`), `datalog/storage/matcher.go` (`PatternMatcher`, `temporalMode`, `shouldFilterTx`, `PatternMatcher.AsOf`, `PatternMatcher.History`, `withMode`), `datalog/storage/database.go` (`Database`, `Database.AsOf`, `Database.History`, `matcherWithExecOptions`), `datalog/storage/snapshot.go` (`AsOfSnapshot`), the constructor's callers `matchWithHashJoin` and `matchWithMergeJoin` (`datalog/storage/hash_join_matcher.go`), the `Next` of `datalog/storage/matcher_iterator_nonreusing.go`, `matchUnboundScan` and `openCRDTScan` (`datalog/storage/matcher_relations.go`), the route that reads the scan without a second check, `validatingVBoundIterator.Next`, and the one that checks each candidate, `validateCandidate` (`datalog/storage/matcher_relations.go`), and `datalog/element_id.go` (`ElementID.Less`).
- The design: `docs/reference/CRDT.md`, "Three database modes"; `docs/reference/CRDT_STREAMING_RESOLUTION.md`, "Call Site Conditions"; `docs/proposals/BRANCHING_AND_SNAPSHOTS.md` §2 and §10.4.
- The tests: `TestAsOfZeroTargetProbe` (`datalog/storage/as_of_zero_target_probe_test.go`); `TestCRDTResolvingIterator_SourceDatomErrorPropagates` and `TestCRDTResolvingIterator_UniqueWalkErrorIsDeferred` (`datalog/storage/iterator_error_propagation_test.go`).
- The compiler: `go build -gcflags=-m ./datalog/storage/`, the inlining report.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **view** is the pair (mode, asOf) a handle reads: `Database.mode` and `Database.asOf`, copied onto every matcher the database mints as `PatternMatcher.mode` and `PatternMatcher.asOf`. The mode is one of latest, history and as-of (`temporalMode`), and asOf is the target of an as-of view, the zero `ElementID` in the other two modes (the fields' doc).
- A datom d **lies within** a view when the view's mode is latest or history, or when its mode is as-of and d.Tx is at or before asOf. The code's one statement of the complement is **S(m, d)** = `m.shouldFilterTx(d.Tx)` = (m.mode = as-of ∧ m.asOf < d.Tx), with `<` the order `ElementID.Less` gives.
- A **CRDT-resolving scan** is a `CRDTResolvingIterator` over a source scan, resolving each (E, A) group by the attribute's cardinality. `NewCRDTResolvingIterator` takes the source, a schema, a target txID and a matcher.
- The scan's **present filter** is **F_old(d)** = (txID ≠ 0 ∧ txID < d.Tx), the test at the head of `CRDTResolvingIterator.Next`'s loop. A datom the filter holds for is skipped.
- The **proposed filter** is **F_new(d)** = S(m, d), with m the matcher the scan was constructed with.
- The **emitted set** of a scan is the set of datoms its `Next` yields over a source.

## 2. Preconditions

- **P1.** Every production construction of the scan passes the matcher's own target as txID and the matcher itself: `matchWithHashJoin` and `matchWithMergeJoin` pass (`m.asOf`, `m`); the `Next` of `matcher_iterator_nonreusing.go`, `matchUnboundScan` and `openCRDTScan` pass (`it.matcher.asOf`, `it.matcher`) or (`m.asOf`, `m`). `gopls call_hierarchy` on the constructor lists these five and the two tests.
- **P2.** Every production construction is guarded by `mode != historyMode`; under history the source scan is used unwrapped.
- **P3.** `PatternMatcher.asOf` has two writers. `matcherWithExecOptions` copies the database's mode and asOf together. `withMode` sets mode and asOf from its two arguments, and its callers are `PatternMatcher.AsOf`, passing (as-of, target), and `PatternMatcher.History`, passing (history, zero).
- **P4.** `Database.asOf` has one writer, `Database.AsOf`, which sets mode to as-of and asOf to its target in one literal. The root database and `Database.History`'s handle leave asOf at the struct's zero value. With P3: a matcher or database whose asOf is non-zero is in as-of mode.
- **P5.** The scan's txID field has one reader, the present filter.
- **P6.** `shouldFilterTx` is reported inlinable and is inlined at each of its fourteen callers (`ResolveLWW`, the hash and merge join iterators' `Next`, `validateDatomWithConstraints`, `lookupAllAttributesFallback`, `validateCandidate`, `PrefetchEntities`, `resolveWildcardEntity`, `observeSpan`, `observeDatom`, `walkUniqueEntityValue`, `resolveMaxOtherTxForValue`, `resolveAVLWW`, `loadRGAElements`). `ElementID.Less` is inlined at the present filter. The scan is the one place the view is computed by a rule of its own.
- **P7.** The zero `ElementID` is issued to no datom. `AsOfSnapshot`'s doc states it: a handle at the zero ElementID reads nothing, since every datom lies after the target. `shouldFilterTx`'s fourteen callers stand on the same premise. The proof inherits it and does not re-prove it.
- **P8.** `latestMode` is `temporalMode`'s zero value, so a zero-value `PatternMatcher` reads the latest view.

## 3. Theorems

**T1 (equivalence outside the zero target).** For every production scan and every datom d, F_new(d) = F_old(d) unless the scan's matcher is in as-of mode with the zero target.

**T2 (the zero target).** For a production scan whose matcher is in as-of mode with the zero target, F_old holds for no datom and F_new holds for every datom (P7). The emitted set goes from the whole source to nothing, which is the view `AsOfSnapshot`'s doc states for that handle and the view every route that checks its candidates through `shouldFilterTx` already reads (the probe's eight passing shapes). The route that reads the scan with no second check, `validatingVBoundIterator.Next`'s cardinality-many arm, is the one whose answer changes.

**T3 (the matcher requirement).** Requiring the constructor's matcher changes no production path: each production caller passes a method receiver or an iterator's matcher field that it has already dereferenced to read `mode` (P1, P2). Removing the `uniqueMatcher != nil` term from `startNewGroup`'s gate changes no production path, since the term is always true there, and no test path, since the two tests pass a nil schema and the gate is behind a schema lookup.

**T4 (no information lost).** The txID field's one reader is the filter (P5) and its value is the matcher's asOf (P1), so removing the field and the parameter loses nothing the matcher does not carry.

**T5 (cost).** After the change the filter reads the matcher's mode and asOf through the scan's matcher pointer and inlines `shouldFilterTx` and `ElementID.Less` (P6): one load more than reading the scan's own field, and no call frame. The report for the filter's line after the change is the measurement.

## 4. Lemmas

### L1. The case split (T1)

Fix a production scan with matcher m. By P1, txID = m.asOf. By P2, m.mode is latest or as-of.

- m.mode = latest. By P4, m.asOf = 0. F_old(d) = (0 ≠ 0 ∧ …) = false. F_new(d) = (latest = as-of ∧ …) = false. Equal.
- m.mode = as-of, m.asOf = T ≠ 0. F_old(d) = (T ≠ 0 ∧ T < d.Tx) = (T < d.Tx). F_new(d) = (as-of = as-of ∧ T < d.Tx) = (T < d.Tx). Equal.
- m.mode = as-of, m.asOf = 0. F_old(d) = false. F_new(d) = (0 < d.Tx). The two differ exactly here. ∎

For the two tests, which pass a nil matcher and the zero txID today and a zero-value matcher after the change: F_old(d) = false and, by P8, F_new(d) = (latest = as-of ∧ …) = false. Equal.

### L2. The zero target emits nothing (T2)

In the third case of L1, F_new(d) = (0 < d.Tx). By P7 every datom's Tx is non-zero, and the zero ElementID is the least under `ElementID.Less`, so F_new holds for every datom and the scan skips every one. A datom whose Tx were the zero ElementID would pass both filters, so even without P7 the change emits no datom F_old skipped. ∎

### L3. The routes that already check (T2)

Under an as-of handle the hash join's and merge join's `Next` test each candidate through `shouldFilterTx` before yielding it, `validateCandidate` tests the cardinality-one winner it reads, and the unique walk, the set resolver and the vector resolver test each datom they read. Under the zero target each of those already drops every datom. The scan's filter holding for the same datoms removes nothing they would have kept. `validatingVBoundIterator.Next` yields a cardinality-many candidate as the scan gives it, with no check of its own, so under the zero target it alone yields what the handle must not read. ∎

## 5. Tests

- `TestAsOfZeroTargetProbe`: nine query shapes over an as-of handle at the zero target, in both optimizer modes, each answering nothing. The cardinality-many V-bound open-attribute shape is red before the change and green after; the eight others are green before and after (L3).
- A test of the non-zero target on the same route, open and constant attribute, in both optimizer modes: a member written at or before the target answers and one written after does not. Green before and after; it pins the second case of L1.
- `TestCRDTResolvingIterator_SourceDatomErrorPropagates` and `TestCRDTResolvingIterator_UniqueWalkErrorIsDeferred`, constructing a zero-value matcher in place of nil: their assertions on error propagation are unchanged (L1's last paragraph).
- Every temporal and resolution test of the `storage` and `db` packages: unchanged by T1.
- The inlining report for the filter's line after the change (T5).

## 6. The change the proof licenses

- `CRDTResolvingIterator.Next`'s filter becomes `it.matcher.shouldFilterTx(datom.Tx)`.
- The txID field and `NewCRDTResolvingIterator`'s txID parameter go; the constructor requires its matcher, which carries the view and enables the unique walk; the field is named `matcher`.
- `startNewGroup`'s gate loses its nil term.
- The five production callers drop the target argument; the two tests pass a zero-value matcher.
- The comment above `openCRDTScan`'s construction, the field and constructor docs, "Call Site Conditions" in `CRDT_STREAMING_RESOLUTION.md`, and the sentences of `BRANCHING_AND_SNAPSHOTS.md` §2 and §10.4 that quote the scan's own filter and the zero ElementID as the spelling of the latest view state the one predicate.

The smaller shape, calling `shouldFilterTx` through the present field and keeping txID, has the same proof of behavior. It leaves a field no reader reads and a documented permission for a nil matcher that the filter would dereference.

## 7. What the proof does not cover

- P7 is inherited, not proven here: the clock's issue of ElementIDs and the import paths' preservation of written ones are outside these sources. L2 shows the change is safe either way.
- The documents' sentences are statements of the predicate, not behavior.
