# BUG: Attribute Keywords Collide After 32 Bytes

**Date**: 2026-05-22 **Severity**: Correctness (High) **Status**: Resolved (2026-05-22) **Affected**: Keyword interning, storage attribute encoding, schema lookup, all query/write paths using attributes longer than 32 bytes

## Summary

Attribute keywords are keyed by a fixed `[32]byte` value in both the global keyword intern cache and the storage representation. The code copies the keyword string into that buffer without rejecting or hashing strings longer than 32 bytes.

As a result, two distinct attributes with the same first 32 bytes become the same interned keyword and the same storage attribute. Writes, schema definitions, queries, and CRDT resolution can silently alias unrelated attributes.

## Root Cause

`InternKeyword` uses the 32-byte storage key as the intern key and truncates longer strings via `copy`:

```go
func InternKeyword(s string) Keyword {
    var key [32]byte
    copy(key[:], s)

    if val, ok := keywordIntern.cache.Load(key); ok {
        return val.(Keyword)
    }

    kw := &keyword{value: s}
    actual, _ := keywordIntern.cache.LoadOrStore(key, kw)
    return actual.(Keyword)
}
```

Storage does the same thing when converting a datom:

```go
var a Attribute
copy(a[:], d.A.String())
```

The comment on `Attribute` says "stored directly if <=32 bytes, SHA256 if longer", but no SHA256 path exists here. The implementation is "stored directly, truncated if longer."

## Collision Example

These are distinct keywords:

```go
a1 := datalog.NewKeyword(":very-long-prefix-0123456789-A")
a2 := datalog.NewKeyword(":very-long-prefix-0123456789-B")
```

If the differing suffix is beyond byte 32, both copy to the same `[32]byte` key. The second call to `NewKeyword` returns the first interned keyword pointer. From that point on, `a1 == a2` even though the source strings differ.

## Expected Behavior

Distinct keyword strings should remain distinct regardless of length, or the system should reject unsupported long attribute names with a clear error before writes/schema construction.

## Actual Behavior

Long attribute names can silently collide:

- Schema definitions for distinct attributes can overwrite or alias each other.
- Writes to one attribute can be returned by queries for another.
- CRDT resolution groups unrelated datoms under the same `(E, A)` pair.
- Pull and Query APIs inherit the same incorrect attribute identity.

## Why This Is Subtle

- Most test and example attributes are shorter than 32 bytes.
- The code comments imply a hash fallback exists, so reviewers may assume long names are handled.
- Once interning returns the same pointer, downstream equality checks look correct because they are comparing the same object.
- The bug can present as a schema, cache, CRDT, or query issue depending on which API observes the aliasing first.

## Reproduction Sketch

```go
prefix := ":attribute/name-with-shared-prefix-"
a1 := datalog.NewKeyword(prefix + "one")
a2 := datalog.NewKeyword(prefix + "two")

if a1 == a2 {
    panic("distinct attributes collided")
}

tx := db.NewTransaction()
tx.Set(entity, a1, "value-one")
tx.Set(entity, a2, "value-two")
tx.Commit()

// Querying a1 and a2 should return different values, but can alias.
```

The exact strings should be chosen so the first 32 bytes are identical and the first differing byte is after byte 32.

## Fix Direction

Pick one explicit attribute identity policy and enforce it everywhere:

- Hash long attributes as the `Attribute` comment claims, with a reverse mapping if full keyword strings must round-trip from storage, or
- Reject keyword strings whose encoded storage form exceeds 32 bytes, returning a clear schema/write/query construction error, or
- Store attribute IDs in a separate intern table instead of truncating the keyword string into the index key.

The fix must address both `InternKeyword` and `ToStorageDatom`. Fixing only storage leaves keyword pointer identity broken; fixing only interning leaves storage aliasing broken.

## Verification Plan

Add regression tests that fail before the fix:

- `TestKeywordInterning_LongNamesDoNotCollide`
- `TestStorage_LongAttributeNamesDoNotAlias`
- `TestQuery_LongAttributeNamesRemainDistinct`
- `TestSchema_LongAttributeDefinitionsRemainDistinct`

Also add a negative/contract test for the chosen policy:

- If hashing is chosen, verify full long keywords round-trip through query results and pull.
- If rejection is chosen, verify long attributes fail at construction or write time with a clear error.

## Resolution (2026-05-22)

**Chosen policy: reject (hard failure).** Attribute names whose UTF-8 form exceeds the 32-byte storage cap are refused with a clear error rather than silently truncated. The cap stays at 32; hashing and raising the cap were considered and declined (see "Decision" below).

Two independent defects, two independent fixes:

### 1. Interning collision (`datalog/intern.go`)

`InternKeyword` keyed its intern cache by a truncated `[32]byte`, so distinct keywords sharing their first 32 bytes interned to the same `*keyword` pointer. It now keys by the full string, so distinct names always get distinct pointers, at any length. `InternKeywordFromBytes` (the storage-decode path) was changed in lockstep — it trims the null padding to recover the string and keys by that — so a keyword decoded from storage stays pointer-identical to one created via `InternKeyword`. (This matters because keyword equality is pointer equality throughout the engine.)

This fix is independent of the storage cap: interning is now correct for names of any length, even ones that can never be stored.

### 2. Storage truncation (`copy(a[:], d.A.String())` into `[32]byte`)

A single source-of-truth constant, `datalog.MaxAttributeBytes = 32`, with a compile-time assertion in `datalog/storage/types.go` that the storage `Attribute` array stays the same size (`var _ [datalog.MaxAttributeBytes]byte = Attribute{}`). Over-length attributes are rejected at every write/definition boundary:

- `Transaction.Set`, `Add`, `Remove`, `Retract` call `validateAttributeStorable` and return an error before building any datom. (`AddEntity`/`AddMap`/ `SaveStruct` funnel through these.)
- `schema.Builder.Add()` records an error so `Build()` fails for an over-length ident.

Because `reflect.SchemaFromStruct` routes through the schema `Builder`, and `SaveStruct` routes through `tx.Add`/`Set`, reflect usage hard-fails cleanly too.

### Decision: why reject, and a reflect caveat

The cap was kept at 32 deliberately. During implementation, the reflect `OrderedSet` tests surfaced that `reflect` derives attribute names from Go struct names (`CharacterWithPreferences` → `:character-with-preferences/prefs`, 33 bytes), so an ordinary struct name can exceed the cap. The decision was that this *should* be a straight-up failure rather than be accommodated by a larger key or a hash dictionary. Consequence: reflect users must keep `struct-name + field` within 32 bytes. The two offending test fixtures were renamed to valid names (`CharacterPrefs`, `EntityOrderedRefs`); they test `OrderedSet`, not the cap, which has its own dedicated rejection tests.

### Tests

- `datalog/keyword_intern_collision_test.go`: `TestKeywordInterning_LongNamesDoNotCollide` — distinct 42-byte names intern to distinct pointers and round-trip their strings.
- `datalog/storage/attribute_truncation_collision_test.go` (rewritten for the reject policy):
  - `TestStorage_LongAttributeNameRejectedOnWrite` — Set/Add/Remove/Retract all
    return an error for an over-length attribute.
  - `TestSchema_LongAttributeDefinitionRejected` — `Build()` errors on an
    over-length ident.
  - `TestStorage_MaxLengthAttributeNameAccepted` — a name exactly at 32 bytes is
    accepted and round-trips (guards against off-by-one).

The original plan's `TestStorage_LongAttributeNamesDoNotAlias` / `TestQuery_LongAttributeNamesRemainDistinct` were written for the "make distinct and storable" semantics; under the reject policy they were replaced by the rejection/boundary tests above. Full suite green: 15 packages, 0 failures.

### Files changed

`datalog/intern.go`, `datalog/types.go`, `datalog/storage/types.go`, `datalog/storage/database.go`, `datalog/schema/builder.go`, `datalog/reflect/ordered_set_test.go` (fixture renames), plus the two new test files.
