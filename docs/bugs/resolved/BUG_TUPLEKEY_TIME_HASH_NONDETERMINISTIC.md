# Bug: TupleKey Hash Is Non-Deterministic for time.Time

**Date**: 2026-05-25 **Severity**: High — silently dropped rows / wrong results; platform-dependent, so it passes locally and fails in CI **Status**: Resolved (2026-05-25) — CI green on linux/amd64 (PR #76) **Affected**: `datalog/executor/tuple_key.go` (`hashValue`), and every `TupleKeyMap` consumer — hash joins, join/union dedup, subquery input dedup

## Summary

`hashValue` (the per-value hash behind `TupleKey` / `TupleKeyMap`) has no `time.Time` case, so `time.Time` values fall through to the `default` branch, which returns `uint64(uintptr(unsafe.Pointer(&v)))` — the address of the function's interface parameter. That address varies call to call (it depends on stack/frame layout), so the *same* time value hashes to *different* values at different call sites.

`TupleKeyMap` buckets entries by hash and only compares values (via `datalog.ValuesEqual`) *within* a bucket. When two equal times hash to different buckets, the value comparison never runs and they are treated as distinct. In a hash join the build side stores a row under one hash and the probe side looks it up under another — the lookup misses and the row is silently dropped.

`datalog.ValuesEqual` *does* handle `time.Time` correctly (by instant, via `Equal`). The defect is purely that `hashValue` and `ValuesEqual` disagree about `time.Time`: equal values, unequal hashes.

## Trigger

Any query whose hash-joined or deduplicated tuples carry a `time.Time` value — e.g. attributes like `:created-at`, `:updated-at`, or a `(max ?ts)` subquery result. The failure is non-deterministic *across* platforms because it depends on stack addresses: it can pass on one OS/arch/Go-version and fail on another, and each outcome is stable within a platform.

## Evidence

- CI (`linux/amd64`, Go 1.25) went red the moment the unrelated PR #73 merged and stayed red; the last green run was #72. Failing tests: `TestGetElseComplex_OrSemantics`, `TestGetElseComplex_ParsedOr`, `TestGetElseComplex_QBOr`, and `TestCorrelatedSubqueryAlgebraOptimizerWithDefaults` — all queries over the items DB with `:created-at` / `:updated-at` timestamps.
- The symptom is a non-deterministic row count: the OR-union query should yield 9 rows; CI's base executor yields 8, dropping the `proj:2` row (the project with no items, whose timestamp is the `0001-01-01` ground default).
- The same suite passes locally (`darwin/arm64`, Go 1.26.3) every time — including 50× repeats, `GOMAXPROCS=2`, and `GOARCH=amd64` via Rosetta — because this machine's stack addresses happen to coincide between the build and probe hashes.
- Reproduced directly and deterministically with two unit tests (below): `hashValue(t)` returns different values for the same `time.Time` when called at different stack depths, and a `TupleKeyMap` loses a time-valued tuple on lookup.

## Root Cause

```go
// hashValue, default branch (datalog/executor/tuple_key.go)
default:
    // Fallback: use pointer as hash
    return uint64(uintptr(unsafe.Pointer(&v)))
```

`time.Time` is not in the type switch, so it hits this branch. `&v` is the address of the interface parameter, not anything derived from the value, so the hash is neither stable nor value-derived.

This is the *same class* of bug as the previously-fixed `[]byte` case, whose comment already warns: "Without this case, []byte falls through to the default pointer-address hash below, so two equal byte slices get different (nondeterministic) hashes and never collide in a TupleKeyMap." `time.Time` was simply overlooked.

PR #73 (removal of the name-based transaction dedup in `HashJoin`) did **not** cause this — it only perturbed execution/stack layout enough to flip the address coincidence on CI. The defect is latent and predates this session.

## Why Local Testing Missed It

The pre-push hook runs `go test ./...` on the developer machine (`darwin/arm64`, Go 1.26.3), where the addresses align and the tests pass. CI runs `linux/amd64` with the Go version from `go.mod` (1.25), where they do not. A green local run — and a green pre-push hook — is not evidence of a green CI run for layout-dependent bugs. CI is the source of truth.

## Fix

Add a `time.Time` case to `hashValue` that hashes by the absolute instant, consistent with `ValuesEqual`:

```go
case time.Time:
    return hashTime(val)

// ...
func hashTime(t time.Time) uint64 {
    const prime = 1099511628211
    hash := uint64(14695981039346656037)
    hash ^= uint64(t.Unix())
    hash *= prime
    hash ^= uint64(t.Nanosecond())
    hash *= prime
    return hash
}
```

`Unix()`+`Nanosecond()` (rather than `UnixNano()`) avoids int64 overflow for out-of-range times such as the `0001-01-01` ground default. Two times that compare `Equal` have the same `Unix()` and `Nanosecond()`, so they hash identically — keeping `hashValue` and `ValuesEqual` in agreement.

## Tests

- `TestHashValue_TimeIsDeterministic` — `hashValue(t)` must return the same value when called at different stack depths. Fails before the fix, passes after.
- `TestTupleKeyMap_TimeValuedTuplesRoundTrip` — a `TupleKeyMap` keyed on a time-valued tuple must find that tuple when the lookup key is built from a separately-constructed equal time at a different stack depth (the build-vs-probe pattern). Fails before, passes after.

## Broader Class (Follow-up)

`hashValue` and `ValuesEqual` must agree for *every* value type. They still disagree for two more cases that hit the same nondeterministic default:

- `query.Symbol` / `datalog.Symbol` used as a value (e.g. source markers). `ValuesEqual` handles `Symbol`; `hashValue` does not. (`BUG_SUBQUERY_INPUT_DEDUP_STRING_COLLISION` already works around this by excluding source markers from its dedup key.)
- Non-`[]byte` slices (`[]string`, `[]int64`, `[]interface{}`). `ValuesEqual` compares these recursively; `hashValue` only handles `[]byte`.

Neither is known to cause failures today (such values rarely appear in join/dedup keys), but both are the same time bomb. Options: add explicit cases, or replace the address-based `default` with a deterministic fallback so no type can ever hash non-deterministically. Tracked here for a decision.

## Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` green locally; the two new tests fail before the fix and pass after.

**Confirmed on CI**: the `TestGetElseComplex_*` and `TestCorrelatedSubqueryAlgebraOptimizerWithDefaults` failures are resolved on `linux/amd64` / Go 1.25 (PR #76 CI green) — the environment where the bug manifested and which cannot be reproduced on `darwin/arm64`.
