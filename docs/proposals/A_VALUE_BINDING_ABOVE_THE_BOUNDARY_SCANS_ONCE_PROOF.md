# A value binding above the boundary scans once, as a proof

## Sources

- The code: `datalog/storage/matcher_relations.go` (`MatchWithConstraints`, `analyzeReuseStrategy`, `matchWithVValidation`, `validatingVBoundIterator`), `datalog/storage/hash_join_matcher.go` (`chooseJoinStrategy`, `matchWithHashJoin`, `keyScanBounds`, `patternScanBound`, `hashJoinIterator`), `datalog/storage/crdt_resolving_iterator.go` (`NewCRDTResolvingIterator`), `datalog/executor/options.go` (`SmallBindingBoundary`).
- The design: `docs/reference/INDEX_SELECTION_PROOF.md`, Theorem 2 (a value-primary scan is not CRDT-correct for cardinality one), Theorem 5 (candidate then validate) and the selection matrix (an attribute-bound scan of a cardinality-one attribute runs on AETV, whose first entry per entity is the current value); `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.6 and §7, the matcher's strategies by key count: a binding of several keys at or below the boundary is sought per key, and one above it is served by the hash-join scan.
- The failure: `PERFORMANCE_STATUS.md`, the entry "The delta leads its copy": the closure benchmark's cells at two to three times their time and four times their allocations, with the query's own time under `matchWithVValidation`.
- The tests that pin the arm: `TestVValidationReportsWhatItsScansCost` (`datalog/storage/vvalidation_reporting_test.go`), the value-bound cases of `datalog/storage/scan_bound_reporting_test.go`, `vbound_cache_validation_test.go`, `vbound_bytes_validation_test.go` and `scan_bound_test.go`, each over a binding of one value or a few.
- The decision: a binding at the value position of a pattern whose attribute is a constant resolving as cardinality one is served, above the boundary, by one scan of the attribute under AETV, resolved and probed by value against the binding, and at or below the boundary by the candidate-and-validate arm as it stands.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **value binding** is a binding relation carrying the pattern's value variable and no other variable of the pattern, so `analyzeReuseStrategy` reaches its value-bound arm.
- The **validation arm** is `matchWithVValidation`: per binding tuple, a candidate scan of the value-primary index, and per candidate a point lookup of the entity's current value, emitting the candidate when the current value is the bound one (Theorem 5).
- The **hash-join scan** is `matchWithHashJoin` with no per-key bounds: one scan of the pattern's extent under the index given, each datom resolved and probed at the key position against the binding's keys.
- The **boundary** is `SmallBindingBoundary`.
- A binding's **size** is `Size()` of the materialized binding `MatchWithConstraints` holds after the typed selection.

## 2. Preconditions

- **P1 (the routing as it stands).** A value binding on a constant attribute resolving as cardinality one, outside history mode, takes the validation arm at any size. An entity binding takes `chooseJoinStrategy`, which seeks per key at or below the boundary and scans the extent once above it.
- **P2 (the hash-join scan is generic over the position).** `matchWithHashJoin` takes the key's position and the index. `keyScanBounds` answers bounds only when every key extends the pattern's own bound past its extent components, and is asked only at the attribute position or at or below the boundary. Above the boundary the scan is one scan of `patternScanBound`'s extent, wrapped in the resolving iterator outside history mode, and each resolved datom is probed at the position.
- **P3 (AETV resolves cardinality one).** Under AETV an attribute's datoms are grouped by entity with the transaction descending, so the resolving iterator yields each entity's current value for the attribute, the selection matrix's row for an attribute-bound scan of a cardinality-one attribute.
- **P4 (the validation arm's answer).** The arm answers exactly the entities whose current value for the attribute is a bound value (Theorem 5).
- **P5 (the typed selection).** The binding the strategies read holds the tuples whose value at the pattern's position can be a datom's value (`typedPositionBindingCheck`), materialized, so its size is known.
- **P6 (the extent).** With the attribute a constant, the pattern's extent under AETV is the attribute's datoms.

## 3. Theorems

**T1 (answers unchanged).** Above the boundary the hash-join scan under AETV answers the tuples the validation arm answers.

**T2 (the boundary).** A value binding at or below the boundary is served by the validation arm, and one above it by one scan of the attribute.

**T3 (the cost).** Above the boundary the match reads the attribute's datoms once and probes each resolved one, where the arm opened a candidate scan per binding tuple and a lookup per candidate.

## 4. Lemmas

### L1. The resolved scan probed by value is the arm's answer (T1)

By P3 the resolving iterator over the attribute's AETV extent yields, per entity, the current value, and no superseded datom. A resolved datom whose value is a bound value is kept by the probe (P2), and one whose value is not is dropped. So the answer is the entities whose current value is a bound value, which is the arm's answer by P4. The constraints apply to the same datoms on either path. ∎

### L2. Above the boundary no per-key bound is asked (T2, T3)

`keyScanBounds` is asked at the value position only at or below the boundary (P2). Above it the scan is the extent's one scan, and the extent is the attribute's datoms (P6). ∎

### L3. The size read is the binding's (T2)

The binding the strategies read is materialized (P5), so its size is the tuples it holds, and the comparison with the boundary reads that size, as `chooseJoinStrategy` reads it for an entity binding. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named, written and run red before the code.

- `TestValueBindingAboveTheBoundaryIsServedByOneScan` (storage, `vbound_boundary_test.go`): on a cardinality-one attribute over entities one past the boundary, some of which held a bound value that a later write superseded, a value binding one past the boundary reports one scan completion under the hash-join strategy and none under the validation strategy, and answers exactly the entities whose current value is bound, the superseded ones left out; a binding at the boundary over the same store reports the validation strategy and answers the same entities; in both optimizer modes (T1, T2, L1, L2).
- `TestVValidationReportsWhatItsScansCost` and the value-bound cases of `scan_bound_reporting_test.go`, `vbound_cache_validation_test.go`, `vbound_bytes_validation_test.go` and `scan_bound_test.go` (storage), run unchanged, their bindings at or below the boundary (T2).
- `TestQueriesAnswerAlikeUnderBothOptimizerModes` and the rule differentials over random graphs (storage), run unchanged (T1).
- `BenchmarkTransitiveClosure` (storage) and the reproduction measure the change; the figures go to `PERFORMANCE_STATUS.md` (T3).

## 6. The change the proof licenses

- `matcher_relations.go`, `MatchWithConstraints`: in the arm `NeedsValidation` selects, a binding above the boundary on a constant attribute is served by `matchWithHashJoin` at the value position under AETV, and one at or below it by `matchWithVValidation`.
- `matcher_relations.go`, `analyzeReuseStrategy`'s comment at the value-bound arm names the boundary.
- `docs/reference/INDEX_SELECTION_PROOF.md`, the sentence after the selection matrix on the binding-driven match: a bound value is post-validated at or below the boundary, and above it the binding is probed against one resolved scan of the attribute under AETV; edited before the code.
- `SCOPE_EXECUTION_AND_CONTRACTS.md` §7, the bullet on a binding above the boundary: a value key on a cardinality-one attribute among the keys it names; edited before the code.
- The tests of §5.

## 7. What the proof does not cover

- A value binding on a variable attribute, whose candidates come from VAET and whose one scan would be the store's. The arm stands there at any size.
- A streaming binding, which reports no size and is small under the comparison, as it is for an entity binding.
- Where between one key and the boundary the arm's per-key cost crosses the scan's. The boundary is the one number the design carries for that trade, and the benchmark measures its two sides.

## 8. Alternatives excluded

- The hash-join scan under AVET, the arm's candidate index. Theorem 2: a value-primary scan hides an entity's other values from resolution, so a superseded value would be answered.
- The unbound scan returned and the executor's join left to probe. The matcher answers the pattern under its binding on every other path, and the hash-join scan is that answer with the probe inside it.
- The arm kept at every size. What stands, and the closure benchmark's cost.
