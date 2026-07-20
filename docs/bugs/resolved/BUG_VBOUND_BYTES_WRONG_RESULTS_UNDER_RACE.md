# BUG: V-bound cardinality-one byte queries return empty under `-race`

**Date**: 2026-05-24 **Severity**: High — silent wrong results for hash joins / dedup on byte-valued keys **Status**: Resolved (2026-05-25) — see Resolution below **Affected**: V-bound cardinality-one `TypeBytes` queries via the candidate+validate path (`validatingVBoundIterator`, AVET/VAET candidate scan + EATV validation). Observed in `datalog/storage/vbound_bytes_validation_test.go`.

## Summary

A V-bound query on a cardinality-one `TypeBytes` attribute — `[:find ?e :in $ ?v :where [?e :doc/hash ?v]]` with a `[]byte` value — returns **0 rows when it should return 1** when the package is run under the Go race detector (`-race`). The same tests pass under the standard test gate (`go test -count=1 ./...`, no `-race`).

The race detector does **not** report a data race for these failures. It is a wrong *result*, surfaced only under `-race` scheduling — which points at timing/ordering sensitivity in the V-bound validation path rather than an unsynchronized memory access the detector can see.

## How it was discovered

While fixing an unrelated, genuine data race in parallel-subquery annotation handling (see `BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md` work / `annotations.Synchronized`), the storage package was run under `-race`. That run surfaced these V-bound failures. `-race` is not the project's standard gate, so this had not been observed before.

## Reproduction

```bash
go test -count=1 -race ./datalog/storage/ -run 'TestVBoundCardinalityOneBytes'
```

Observed, consistently across runs:

```
--- FAIL: TestVBoundCardinalityOneBytes_NoPanic          ("[]" should have 1 item(s), but has 0)
--- FAIL: TestVBoundCardinalityOneBytes_MatchesByContent
--- PASS: TestVBoundCardinalityOneBytes_RejectsStaleCandidate
--- FAIL: TestVBoundCardinalityOneBytes_AfterOverwrite
--- PASS: TestVBoundCardinalityOneBytes_AfterRemove
```

Without `-race` (the standard gate), all five pass:

```bash
go test -count=1 ./datalog/storage/ -run 'TestVBoundCardinalityOneBytes'   # all PASS
```

## Established facts

- The three failing tests (`NoPanic`, `MatchesByContent`, `AfterOverwrite`) all assert that the query **finds** the entity by its current byte value (expect ≥1 row).
- The two passing tests (`RejectsStaleCandidate`, `AfterRemove`) assert the query returns **empty** (stale/removed value, expect 0 rows).
- So the failure bias is consistent: under `-race`, the V-bound query returns **empty when it should find a match**. It never (observed) returns a spurious extra row.
- The failure is an assertion failure (wrong row count), **not** a `WARNING: DATA RACE` from the detector.
- The full standard suite (`go test -count=1 ./...`) is green, including these tests.
- The query has no subqueries and a scalar `:in` input, so it does **not** go through the executor's parallel RelationInput iteration path.

## Not yet established (root cause undetermined)

The following are **hypotheses, not conclusions** — none have been verified, and they should not be treated as fact:

- It may be specific to `[]byte` values (all affected tests use bytes); whether the same query shape on string/int values also diverges under `-race` has not been tested.
- It may live in the candidate scan (AVET/VAET) or the EATV winner validation in `validatingVBoundIterator`, where `-race` scheduling could change iteration/seek ordering.
- It may involve storage-layer concurrency (BadgerDB iterators, value prefetch) whose timing `-race` perturbs.

Determining which requires dedicated investigation (e.g., narrowing to a single failing test, adding scan/validation annotations, and comparing the candidate set and EATV winner with vs. without `-race`).

## Impact

- If this reflects a genuine ordering/timing sensitivity, V-bound cardinality-one byte queries could return wrong (empty) results under real concurrent load, not just under `-race`. That is a correctness risk worth confirming.
- If it is purely a `-race`-environment artifact, the practical impact is limited to running the suite under `-race`. This has **not** been determined.

## Next steps

1. Confirm determinism and isolate to a single failing test under `-race`.
2. Determine whether it is `[]byte`-specific or affects all V-bound cardinality-one queries.
3. Instrument the candidate scan and EATV validation to capture the candidate set and chosen winner with and without `-race`.
4. Decide severity once the mechanism is known: real concurrency correctness bug vs. `-race`-only artifact.

## Related

- `docs/bugs/resolved/BUG_VBOUND_BYTES_VALIDATION_PANIC.md` — the original V-bound `[]byte` `==` panic (fixed in v0.11.4). Same code path; this is a different, result-correctness symptom.
- `datalog/storage/vbound_bytes_validation_test.go` — the tests that surface this.

---

## Resolution (2026-05-25)

**Resolved.** The hypotheses in this report (candidate scan / EATV validation timing) were wrong — that path is correct. The actual cause is a missing type case in the join/dedup hash function.

### Corrected root cause

`hashValue` in `datalog/executor/tuple_key.go` had cases for `string`, `int`, `Identity`, `Keyword`, etc., but **no case for `[]byte`**. A byte slice fell through to the default:

```go
default:
    return uint64(uintptr(unsafe.Pointer(&v))) // address of a local — not the content
```

That hashes the address of a local interface variable, not the byte content, so two equal `[]byte` values produce different, nondeterministic hashes. In a `TupleKeyMap` (the hash table behind joins and dedup) they land in different buckets and never meet — the `datalog.ValuesEqual` fallback only resolves collisions *within* a bucket, so it never runs across buckets.

The V-bound byte query joins the pattern result against the `:in ?v` input on `?v` (`[]byte`). The probe key hashes to a different bucket than the build key, the lookup misses, and the row is dropped → empty result. Without `-race`, the bogus addresses happened to collide (so the join accidentally matched); `-race` perturbs stack/heap layout so they differ → reliable miss. That is exactly the observed signature: `[]byte`-specific, `-race`-only, **no** data race reported (it is a single-goroutine logic bug, not a memory race), and biased toward returning empty (a missed lookup never produces a spurious extra row).

The matcher itself is correct: driving `Match` directly (bypassing the executor join) returns the entity under `-race`. The drop is purely in the join's hash bucketing.

### Scope

Broader than V-bound queries: this fixes correctness for **any hash join or dedup keyed on a byte-valued attribute**. V-bound byte queries were just the most visible and reliably reproducible path.

### Fix

One missing case in `hashValue`, so byte values hash by content like every other type (`hashBytes` already existed):

```go
case []byte:
    return hashBytes(val)
```

### Tests

- `datalog/storage/vbound_bytes_validation_test.go` — the five original repro tests now pass under `-race`; added `TestVBoundCardinalityOneBytes_DirectMatcher` (matcher in isolation, no join) and `TestVBoundCardinalityOneBytes_ValidationTrail` (asserts the match and captures the validation/join annotation trail).
- `datalog/executor/tuple_key_collision_test.go` — strengthened `TestTupleKeyMap_BytesAreDistinguishedByContent` to assert equal byte content hashes equally; its prior comment had rationalized the buggy address-hash behavior.

Full `go test -count=1 ./...` is green, and the byte-valued tests pass under `-race`.
