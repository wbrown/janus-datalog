# BUG: V-Bound Cardinality-One Validation Panics on Byte Values

**Date**: 2026-05-22
**Severity**: Stability / Correctness (Medium)
**Status**: Resolved (2026-05-22)
**Affected**: V-bound queries on cardinality-one `TypeBytes` attributes, especially candidate + validate paths using AVET/VAET

## Summary

The V-bound cardinality-one validation path compares the current winner value with the bound query value using Go's `==` operator on `interface{}` values. This panics when either side is a slice, including the supported `[]byte` value type.

The codebase already has `datalog.ValuesEqual` for exactly this kind of comparison, but this path bypasses it.

## Root Cause

`validatingVBoundIterator.validateCandidate` point-lookups the current `(E, A)` winner via EATV and then compares the winner's value to the bound value directly:

```go
// Check if winner's V matches our bound V
matches := winner.V == it.currentBoundV
```

For `[]byte`, both `winner.V` and `it.currentBoundV` are interface values whose dynamic type is `[]uint8`. Comparing them with `==` causes:

```text
panic: runtime error: comparing uncomparable type []uint8
```

Other matcher paths use `m.valuesEqual(...)`, which delegates to `datalog.ValuesEqual(...)` and handles byte slices with `bytes.Equal`.

## Reproduction Sketch

```go
s := schema.NewSchema()
s.Add(&schema.AttributeDefinition{
    Ident:       datalog.NewKeyword(":doc/hash"),
    ValueType:   schema.TypeBytes,
    Cardinality: schema.CardinalityOne,
})

db, _ := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:   dir,
    Schema: s,
})

e := datalog.NewIdentity("doc-1")
a := datalog.NewKeyword(":doc/hash")
v := []byte{0xde, 0xad, 0xbe, 0xef}

tx := db.NewTransaction()
tx.Set(e, a, v)
tx.Commit()

results, err := executor.CollectTuples(db.Query(
    `[:find ?e :in $ ?v :where [?e :doc/hash ?v]]`,
    v,
))
```

Expected: one result containing `doc-1`.

Actual: the V-validation path can panic when it compares the EATV winner to the bound byte slice.

## Expected Behavior

V-bound queries should support all valid value types. For `[]byte`, equality should be byte-content equality.

## Actual Behavior

The candidate validation path can panic for byte slices and any other non-comparable slice value that reaches this comparison.

## Why This Is Subtle

- The bug only appears in a specific V-bound validation path, not every bytes query.
- Direct unbound reads and E/A-bound reads can work, giving confidence that byte values are supported.
- Existing comparison utilities already handle byte slices, so this is a one-line semantic bypass rather than a missing primitive.
- Many V-bound tests use strings, numbers, keywords, or refs, all of which are comparable.

## Impact

- Valid user queries can crash the process.
- Applications using content hashes, blobs, or binary IDs as query values are exposed.
- The same logical query may work or panic depending on planner/matcher strategy and whether validation is needed.

## Fix Direction

Replace direct interface equality with the canonical value comparison:

```go
matches := datalog.ValuesEqual(winner.V, it.currentBoundV)
```

or:

```go
matches := it.matcher.valuesEqual(winner.V, it.currentBoundV)
```

Use the same comparison policy consistently in all V-bound validation and vector/cache comparison paths.

## Verification Plan

Add regression tests that currently fail:

- `TestVBoundCardinalityOneBytes_NoPanic`
- `TestVBoundCardinalityOneBytes_MatchesByContent`
- `TestVBoundCardinalityOneBytes_RejectsStaleCandidate`
- `TestVBoundCardinalityOneBytes_AfterOverwrite`
- `TestVBoundCardinalityOneBytes_AfterRemove`

Run the tests through the public `db.Query` API, not only direct matcher calls, so the planner and candidate-validation strategy are exercised end to end.

## Resolution (2026-05-22)

One-line fix: `validatingVBoundIterator.validateCandidate` in
`datalog/storage/matcher_relations.go` now compares the EATV winner to the
bound value with `datalog.ValuesEqual` instead of raw `==`:

```go
matches := datalog.ValuesEqual(winner.V, it.currentBoundV)
```

`ValuesEqual` compares `[]byte` by content (`bytes.Equal`), so the comparison
no longer panics on the uncomparable `[]uint8` dynamic type. This was the only
offending site — the other `==` uses in that file are `== nil` and a comment.

The `Op == OpCRDTRemove` check (line 817) precedes the comparison, so a
tombstoned winner returns `false` before reaching `ValuesEqual`; the
`AfterRemove` test confirms this branch is unaffected.

### Tests

`datalog/storage/vbound_bytes_validation_test.go` — all five from the plan,
through the public `db.Query` API. `NoPanic`, `MatchesByContent`,
`RejectsStaleCandidate`, and `AfterOverwrite` reproduced the panic before the
fix and pass after it; `AfterRemove` passes throughout (it exercises the
tombstone branch that never reaches the comparison). Full suite green:
15 packages, 0 failures.

### Files changed

`datalog/storage/matcher_relations.go`, plus the new test file.
