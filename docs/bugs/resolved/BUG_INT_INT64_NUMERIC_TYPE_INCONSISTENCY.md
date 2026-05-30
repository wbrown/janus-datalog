# BUG: Inconsistent `int` vs `int64` Handling (Silent Non-Match in Joins; Encode Panic on Write)

**Date**: 2026-05-30
**Severity**: Correctness / Robustness (Medium)
**Status**: RESOLVED (2026-05-30) — Go integer widths are normalized to canonical `int64` at the API boundary (`datalog.NormalizeValue`), `Type`/`ValueBytes` coerce so encode never panics, and `ValuesEqual`/`CompareValues` agree on integer widths (int-vs-float stays strict). See [Resolution](#resolution).
**Affected**: `datalog/compare.go` (`ValuesEqual` vs `CompareValues`), `datalog/value_encoding.go` (`Type`, `ValueBytes`), all programmatic API paths that accept a Go `int` (query `:in` parameters, query-builder constants, `tx.Add`/`Set` values). EDN query *literals* are unaffected (the parser produces `int64`).

## Summary

The engine has no single normalization point for Go integer types, and its two
value-comparison functions disagree on whether `int` and `int64` are the same
value. This produces two distinct, verified failures, both triggered by a plain
Go `int` (rather than `int64`) entering through the programmatic API:

1. **Silent non-match.** `ValuesEqual(int(5), int64(5))` is **false**, but
   `CompareValues(int(5), int64(5))` is **0** (equal). Joins, pattern-constant
   matching, and dedup use `ValuesEqual`; comparison predicates use
   `CompareValues`. So an `int`-valued query parameter silently fails to match
   `int64`-valued stored data in a pattern/join, while the *same* value compared
   with `[(= ?x ?y)]` succeeds. Same inputs, opposite answers.

2. **Encode panic on write.** `Type(int(5))` and `ValueBytes(int(5))` both
   **panic** (`unknown value type: int` / `cannot encode value type: int`),
   because neither switch has an `int` case. Schema validation, however, *accepts*
   a Go `int` for `TypeLong` (`schema/validation.go:27`). So `tx.Add(e, attr, 5)`
   with an untyped Go `int` passes validation and then panics at commit when the
   datom is encoded.

The root cause is shared: there is no canonical "normalize incoming numeric to
`int64`" step at the API boundary, and the type is handled inconsistently across
comparison, encoding, and validation.

## Verified Behavior

Measured directly (throwaway unit test, since removed):

```
ValuesEqual(int(5), int64(5))   = false
ValuesEqual(int64(5), int64(5)) = true
CompareValues(int(5), int64(5)) = 0          // treated as equal
Type(int(5))        -> panic: unknown value type: int
ValueBytes(int(5))  -> panic: cannot encode value type: int
```

## Root Cause

### Facet 1 — equality vs comparison disagree

`ValuesEqual` is type-strict. Its numeric branch falls through to an interface
`==`, which is false across distinct dynamic types (`datalog/compare.go:288-290`):

```go
switch a.(type) {
case int, int64, float64, string, bool, uint64:
    return a == b      // int(5) == int64(5) is false in Go
}
```

`CompareValues` is type-lenient — it widens `int` to `int64` before comparing
(`datalog/compare.go:80-83`, `compareNumeric`):

```go
case int:
    return compareNumeric(int64(l), right)   // int normalized to int64
case int64:
    return compareNumeric(l, right)
```

Consumers split along this fault line:
- Joins / dedup: `TupleKey.Equal` → `datalog.ValuesEqual` (`executor/tuple_key.go:195`, `:208`).
- Pattern constant match: `BadgerMatcher.valuesEqual` → `datalog.ValuesEqual` (`storage/matcher.go:772-775`).
- Comparison predicates `= != < <= > >=`: `datalog.CompareValues` (`query/predicate.go:111`, `:242`).

Note `hashValue` hashes `int` and `int64` identically (`executor/tuple_key.go:117-121`),
so they land in the same hash bucket and are then separated by the strict
`ValuesEqual` — internally consistent for the map, but it means `int` and `int64`
are deliberately distinct join keys.

### Facet 2 — encoder has no `int` case

`Type` and `ValueBytes` (`datalog/value_encoding.go`) enumerate `int64` but not
`int`, and fall through to a `panic` default. The write path (`Transaction.Add`
→ `Commit` → `stx.Assert` → `EncodeValueBytes` → `EncodeValue`/`Type`) stores the
value as-is and never coerces, so a Go `int` reaches the encoder and panics.
`toStorageValue` (`storage/types.go:226`) *does* coerce `int`→`int64`, but it has
no non-test callers — it is dead code, not the write path.

## Expected Behavior

A Go `int` and the equivalent `int64` should behave identically everywhere, or
the API should reject non-`int64` integers with a clear error at the boundary.
In particular:
- A parameterized query with an `int` argument should match `int64` stored data
  the same way an `int64` argument does (and the same way the `=` predicate
  does).
- `tx.Add(e, attr, 5)` should either store `5` as `int64` or return a clear
  error — never panic at commit.

## Actual Behavior

- `[:find ?e :in $ ?age :where [?e :person/age ?age]]` invoked with a Go `int`
  argument returns **no rows**, because the input relation's `int` value never
  joins/matches the stored `int64`. The same query with an `int64` argument
  returns the expected rows. A `[(= ?age 30)]` predicate against the same data
  matches regardless.
- `tx.Add(entity, attr, 5)` (untyped Go `int`) passes schema validation, then
  **panics** during `Commit` at value encoding.

## Why This Is Subtle

- EDN query literals parse to `int64` (`edn/parser.go:179` →
  `strconv.ParseInt(...,64)`), so queries written as text never trip facet 1 —
  only values that arrive through Go code (`:in` params, qb constants) do.
- The two failure modes look unrelated (empty result vs panic) but share one
  root cause, so they may be filed/fixed separately and incompletely.
- Facet 1 is a *silent* wrong answer (empty result), not an error.
- Schema validation accepting `int` actively misleads: it signals "an `int` is a
  valid `TypeLong`," which then panics at encode time.
- `Go`'s untyped constant `30` becomes `int`, so the natural call
  `tx.Add(e, :age, 30)` is exactly the broken case; `int64(30)` is the working
  one.

## Reproduction Sketch

```go
// Facet 1: silent non-match (query inputs)
e := datalog.NewIdentity("alice")
tx := db.NewTransaction()
tx.Add(e, datalog.NewKeyword(":person/age"), int64(30)) // stored as int64
tx.Commit()

q := db.MustParseQuery(`[:find ?e :in $ ?age :where [?e :person/age ?age]]`)
rowsInt64, _ := db.Query(q, int64(30)) // matches: 1 row
rowsInt,   _ := db.Query(q, int(30))   // BUG: 0 rows (int != int64 in ValuesEqual)

// Facet 2: encode panic on write
tx2 := db.NewTransaction()
tx2.Add(e, datalog.NewKeyword(":person/age"), int(31)) // passes schema validation
_, err := tx2.Commit()                                 // BUG: panics at encode
```

Unit-level proof (no DB needed):

```go
datalog.ValuesEqual(int(5), int64(5))   // false
datalog.CompareValues(int(5), int64(5)) // 0  (equal)
datalog.Type(int(5))                    // panics
```

## Fix Direction

Pick one canonical integer representation (`int64`) and enforce it at every
boundary, rather than spreading per-type cases:

1. **Normalize at the API boundary (preferred).** Coerce incoming Go integers
   (`int`, `int32`, …) to `int64` where values enter the engine:
   - write path: in `Transaction.Add`/`Set`/`Remove` (or a single
     `normalizeValue`), before building the datom — this is what the dead
     `toStorageValue` already intended;
   - query input path: in `convertInputsToRelations` (`storage/database.go`) for
     scalar/collection/tuple/relation inputs;
   - query-builder constant construction.
   With normalization at the boundary, downstream `ValuesEqual` strictness is
   harmless because non-`int64` integers never reach it.

2. **And/or make `ValuesEqual` and `CompareValues` agree.** If `ValuesEqual` is
   meant to be the value-equality predicate, it should treat numeric values by
   magnitude like `CompareValues` does (`ValuesEqual := CompareValues(a,b)==0`
   for numerics), so equality and ordering never disagree. (Decide deliberately:
   making them agree changes join-key semantics for mixed-type data.)

3. **Stop validation/encoding from disagreeing.** Whatever policy is chosen,
   `schema.ValidateValue` (accepts `int`) and `Type`/`ValueBytes` (panic on
   `int`) must be consistent — either both accept and coerce, or validation
   rejects non-`int64` with a clear error before the encoder is reached.

Fixing only one facet leaves the other live; fixing only `ValuesEqual` still
leaves the write-path panic, and vice versa.

## Verification Plan

Regression tests that fail before the fix:

- `TestQuery_IntInputMatchesInt64StoredValue` — a parameterized query returns the
  same rows for an `int` argument as for the equivalent `int64` argument.
- `TestPredicateAndJoinAgreeOnIntInt64` — `[(= ?x ?y)]` and a join on the same
  values agree (both match or both don't).
- `TestWrite_GoIntValueDoesNotPanic` — `tx.Add(e, attr, 5)` either commits
  (value round-trips as `int64`) or returns a clear error; it never panics.
- `TestValuesEqual_NumericTypeAgreement` (unit) — `ValuesEqual` and
  `CompareValues==0` agree across `{int, int64}` (and document the chosen policy
  for `float64` vs integer if any).
- If normalization is chosen, an end-to-end test: write `int`, reopen, read back
  as `int64`, and confirm `int`/`int64` query parameters both match.

## Resolution

Policy chosen: **normalize to `int64` at the API boundary** (option 1), plus
**reconcile `ValuesEqual` with `CompareValues`** on integer widths (option 2) as
defense in depth. `int64` is the engine's single canonical integer — the EDN
parser, storage decode, and schema validation all already standardize on it.

- `datalog.NormalizeValue` coerces `int`/`int8`/`int16`/`int32` → `int64`
  (everything else, including `int64`, passes through untouched). It is applied
  where Go values enter: `Transaction.Add`/`Set`/`Remove`, the query `:in` input
  conversion (scalar/collection/tuple/relation), and `qb.V` constants.
- Facet 2 (encode panic): `Type` and `ValueBytes` now call `NormalizeValue`
  first, so a bare Go `int` encodes as `int64` instead of panicking. Schema
  validation already accepted these widths, so validation and encoding are now
  consistent.
- Facet 1 (silent non-match): a single `asInt64` helper unifies integer widths
  by magnitude in `ValuesEqual` and routes `CompareValues`/`compareNumeric`/
  `compareFloat`/`compareUint64`, so equality and ordering agree. `int`-vs-`float`
  stays strict in `ValuesEqual` (so mixed-numeric join keys aren't conflated)
  while `CompareValues` still orders them by magnitude. `hashValue` hashes all
  widths as `int64` so unified values share a `TupleKeyMap` bucket.

Hot path: `ValuesEqual` keeps the `==` fast path first; width unification runs
only when `==` already failed. Allocation-free microbenchmarks
(`compare_int_bench_test.go`, `tuple_key_int_bench_test.go`) measured zero added
allocations and per-call latency unchanged within noise on the common paths,
with ~1 ns added only on the rare unequal-integer (hash-collision) path —
immaterial against the allocation-dominated join.

Tests added:

- `datalog` (unit): `TestNormalizeValue_CoercesIntegerWidths`,
  `TestValuesEqual_CompareValues_AgreeOnIntegerWidths`,
  `TestValuesEqual_IntVsFloatStaysStrict`,
  `TestType_And_ValueBytes_GoIntEncodeAsInt64`.
- `datalog/executor` (unit): `TestTupleKey_UnifiesIntegerWidths` (int and int64
  share a hash bucket and compare equal; different magnitudes don't).
- `datalog/storage` (end-to-end): `TestWrite_GoIntValueDoesNotPanic`,
  `TestQuery_IntInputMatchesInt64StoredValue`,
  `TestPredicateAndJoinAgreeOnIntInt64`.

Full suite green (`go test -count=1 ./...`).
