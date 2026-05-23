# BUG: Cardinality-Many `Set` Panics on Byte Slice Members

**Date**: 2026-05-22
**Severity**: Stability / Correctness (High)
**Status**: Resolved (2026-05-22)
**Affected**: `Transaction.Set` for `CardinalityMany` attributes whose values are `[]byte` or other non-comparable slice types

## Summary

`Transaction.Set` for cardinality-many attributes builds Go maps keyed directly by set members. This works for comparable values like strings, ints, keywords, and identities, but panics for `[]byte` members because slices are not valid Go map keys.

`[]byte` is a documented supported value type and `schema.TypeBytes` can be used with `CardinalityMany`, so this is a valid user input that can crash the process.

## Root Cause

The cardinality-many `Set` path converts the input slice to `[]interface{}` and then uses each member directly as a `map[interface{}]bool` key:

```go
newSet := make(map[interface{}]bool, len(newSlice))
for _, val := range newSlice {
    newSet[val] = true
}
```

Later pending-operation maps use the same direct-key pattern:

```go
pendingAdds[datom.V] = datom.Tx
pendingRemoves[datom.V] = datom.Tx
```

This is inconsistent with other CRDT set paths, which already special-case `[]byte` by deriving a hashable key while preserving the original value in results.

## Reproduction Sketch

```go
s := schema.NewSchema()
s.Add(&schema.AttributeDefinition{
    Ident:       datalog.NewKeyword(":file/chunks"),
    ValueType:   schema.TypeBytes,
    Cardinality: schema.CardinalityMany,
})

db, _ := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:   dir,
    Schema: s,
})

e := datalog.NewIdentity("file-1")
a := datalog.NewKeyword(":file/chunks")

tx := db.NewTransaction()
err := tx.Set(e, a, [][]byte{
    []byte("chunk-a"),
    []byte("chunk-b"),
})
```

Expected: `Set` writes an add-wins replacement set.

Actual: panic similar to:

```text
panic: runtime error: hash of unhashable type []uint8
```

## Expected Behavior

`Set` should accept byte-valued cardinality-many attributes just like `Add`, `Retract`, direct matcher resolution, and query paths do.

At minimum, if a value type cannot be supported as a set member, `Set` should return a normal error during validation instead of panicking.

## Actual Behavior

The write path can panic before returning an error. This can crash applications even though the value type is valid according to the project type system.

## Why This Is Subtle

- Existing byte-value tests cover `Add`/`Retract` and resolution, not `Set` replacement.
- Most cardinality-many examples use strings or refs, which are comparable and do not expose the panic.
- The input is converted through `[]interface{}`, hiding the original typed slice until the map insertion.
- Similar code elsewhere already has `[]byte` handling, making the overall feature appear supported.

## Impact

- Applications storing sets of blobs, hashes, compressed fragments, or binary IDs can crash on a normal `Set`.
- The panic can occur after transaction construction begins, making it harder for callers to recover consistently.
- The replacement-set API is less reliable than incremental `Add`/`Retract` for the same declared schema type.

## Fix Direction

Use a canonical, hashable value key for set membership maps in the cardinality-many `Set` path.

The key should preserve type distinctions and value boundaries. Existing patterns include:

- type byte + `datalog.ValueBytes(value)` as a string key, or
- an existing tuple/value key primitive if one is already canonical for storage values.

The map should store original values alongside the canonical key so query results and datoms preserve the original `[]byte` type.

All pending-operation maps in this path must use the same keying scheme, not just `newSet`.

## Verification Plan

Add regression tests that currently fail:

- `TestCardinalityManySet_ByteValues_NoPanic`
- `TestCardinalityManySet_ByteValues_ReplacesExistingSet`
- `TestCardinalityManySet_ByteValues_PendingOpsInSameTransaction`
- `TestCardinalityManySet_ByteValues_DuplicateMembersDedupByContent`

Run each relevant test with cache enabled and disabled where readback uses query or pull paths, to ensure the write fix integrates with both CRDT resolution modes.

## Resolution (2026-05-22)

The cardinality-many `Set` path in `datalog/storage/database.go` keyed five
membership maps directly by the set member, which panics for `[]byte`. It was
also internally inconsistent — it read `currentResult.Members` (already keyed by
a derived hashable key) while building `newSet` from raw values, mixing two
keying schemes.

Fix: introduced `memberKey(v)`, a hashable map key built from a type tag plus
`datalog.ValueBytes(v)`:

```go
func memberKey(v interface{}) string {
    return string(append([]byte{byte(datalog.Type(v))}, datalog.ValueBytes(v)...))
}
```

This is byte-for-byte the same key `resolveAddWinsSet` already uses internally,
so the Set diff lines up with stored membership. (It is *not* canonical in the
strict sense — it keys floats by raw bits, which differs from
`datalog.ValuesEqual` at `±0.0`/`NaN` — but it matches the read path, which is
the property that matters. The old raw-value keying actually disagreed with
storage on floats; this change makes write and read agree.)

All five maps in the path now key by `memberKey` and carry the original value
for emission: `newSet`, `effectiveSet`, `pendingAdds`, `pendingRemoves`, and the
merged `pendingValues` (replacing the old `allPendingValues`). The Remove/Add
emission loops range `(key, originalValue)` and write `V: originalValue`, so
datoms keep their real `[]byte` type. Keying by content also dedups duplicate
slice members.

### Tests

`datalog/storage/cardinality_many_set_bytes_test.go` — all four from the plan,
reading back through the public `db.Query` API:

- `NoPanic`, `ReplacesExistingSet`, `DuplicateMembersDedupByContent`
- `PendingOpsInSameTransaction` specifically exercises the pending-op maps
  (the second panic site), guarding against a fix that only repairs `newSet`.

All reproduced the panic before the fix and pass after it. Full suite green:
15 packages, 0 failures.

### Not covered

The cardinality-**vector** `Set` path (RGA prefix-diff) has a similar
value-comparison shape and was not audited here — flagged as a follow-up.

### Files changed

`datalog/storage/database.go`, plus the new test file.
