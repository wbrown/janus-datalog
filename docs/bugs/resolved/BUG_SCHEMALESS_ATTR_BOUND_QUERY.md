# BUG: Schemaless Attributes Invisible to Bound Queries When Schema Present

**Status**: Resolved (2026-05-25) — see Resolution below
**Severity**: High — data written successfully but queries silently return no results
**Date**: 2026-02-06

## Summary

When a database has a schema defined, attributes NOT registered in the schema can be written successfully via `tx.Add()`, but bound queries via `:in` fail to find them. Unbound queries find the data correctly.

## Reproduction

```go
// Create database WITH schema (schema does NOT include :module/input)
db, _ := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:   dir,
    Schema: mySchema, // has many attributes, but NOT :module/input
})

// Write schemaless attribute — succeeds, no error
tx := db.NewTransaction()
tx.Add(entityID, datalog.NewKeyword(":module/input"), "some text")
tx.Commit() // succeeds

// Unbound query — FINDS the data
db.ExecuteQueryWithInputs(
    `[:find ?e ?a ?v :where [?e ?a ?v]]`,
) // returns tuple with :module/input ✓

// Bound query — FAILS to find the data
db.ExecuteQueryWithInputs(
    `[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
    entityID, datalog.NewKeyword(":module/input"),
) // returns empty []  ✗
```

## Expected Behavior

Bound queries should find schemaless data, same as unbound queries.

## Actual Behavior

- `tx.Add()` succeeds (ValidateDatom returns nil for unknown attributes — "additive schema")
- `tx.Commit()` succeeds (data written to all 7 indices)
- Unbound full-scan query finds the data (EATV scan, CRDTResolvingIterator works)
- Bound query with E and A via `:in` returns empty results

## Root Cause

Two problems:

1. **CRDTResolvingIterator gated on `m.schema != nil`**: When schema was nil on
   the matcher, CRDTResolvingIterator was not applied at all. Raw storage scans
   returned unresolved datoms.

2. **CardinalityUnknown used add-wins (wrong default)**: When CRDTResolvingIterator
   encountered an attribute not in schema, it defaulted to `CardinalityUnknown`
   which routed to `processAddWins()`. Since schemaless `tx.Add()` writes `OpNone`
   (not `OpCRDTAdd`), `processAddWins` silently dropped the datoms.

## Discovery Context

Found in the application scribe import workflow. The `:module/input` attribute (raw markdown text, ~36KB) was defined as a keyword constant but not registered in the schema builder. After the janus-datalog CRDT fix (v0.6.1-0.20260206164518-20348db45516), the import broke — `SetInput()` succeeded but the subsequent `HasAttribute` query returned false.

Adding the attribute to the schema immediately fixed the issue.

---

## Resolution Plan

### Design Decisions

#### Schemaless default is CardinalityOne (LWW)

Datomic defaults to CardinalityOne. Every database, key-value store, and user
mental model defaults to "write an attribute twice, get the latest value."
Add-wins as a default is surprising — writing `:name` twice returns both values.

Schemaless `tx.Add()` writes `OpNone`. If you want CardinalityMany or
CardinalityVector, declare it in schema. That's what schema is for.

#### CardinalityUnknown = CardinalityOne

When CRDTResolvingIterator encounters an attribute not in schema (or nil schema),
it defaults to CardinalityOne. No Op-sniffing. Cardinality is an attribute-level
property that belongs in schema, not something inferred from individual datoms.

#### OpNone is valid and means CardinalityOne

`OpNone` is not an error. It means "this is a CardinalityOne LWW assertion."
The `processAddWins` function correctly ignores it — `OpNone` datoms are not
add-wins operations. CardinalityOne resolution handles them: first entry wins.

#### tx.Set() and tx.Add() are the same for CardinalityOne

Both write `OpNone`. Both are LWW. `tx.Set()` only differs from `tx.Add()` for
CardinalityMany (replace entire set) and CardinalityVector (replace entire vector).

#### tx.Remove() works for all cardinalities

Remove writes `OpCRDTRemove` regardless of cardinality. Resolution determines
what "remove" means:

- **CardinalityOne**: first entry is `OpCRDTRemove` → attribute doesn't exist
- **CardinalityMany**: tombstone for that specific value (add-wins)
- **CardinalityVector**: tombstone for that element (RGA)

#### CRDTResolvingIterator is always applied

The `m.schema != nil` guard was wrong. CRDTResolvingIterator handles nil schema
correctly — it defaults to CardinalityOne for unknown attributes, which is the
correct schemaless behavior.

### Sequence

Tests are written FIRST. All new tests should fail before any implementation
changes are made. This validates that the tests actually catch the bugs.

1. Write all new tests (expect failures)
2. Update existing tests (expect failures)
3. Implement changes 1-5
4. All tests pass

### Changes

#### 1. Revert schemaless `tx.Add()` Op back to `OpNone`

**File**: `database.go`, schemaless path of `tx.Add()` (~line 1401)

Our earlier fix changed this from `OpNone` to `OpCRDTAdd`. That was wrong.
Schemaless default is CardinalityOne. Revert to no explicit Op (zero value = `OpNone`).

```go
// CURRENT (wrong):
Op: datalog.OpCRDTAdd,

// CORRECT:
// No Op field — defaults to OpNone (CardinalityOne LWW)
```

#### 2. CRDTResolvingIterator: CardinalityOne checks Op

**File**: `crdt_resolving_iterator.go`, CardinalityOne case (~lines 134-141)

Currently emits first entry unconditionally. Must check Op:

```go
case schema.CardinalityOne:
    if isNewGroup {
        if datom.Op == datalog.OpCRDTRemove {
            // Value was retracted — attribute doesn't exist. Skip group.
            continue
        }
        // First entry for this (E, A) — emit (LWW winner)
        it.currentDatom = datom
        return true
    }
    // Same (E, A) — skip (already emitted or skipped the winner)
    continue
```

#### 3. CRDTResolvingIterator: CardinalityUnknown defaults to CardinalityOne

**File**: `crdt_resolving_iterator.go`, `startNewGroup()` (~lines 168-197)

Change CardinalityUnknown to use CardinalityOne resolution. No state needed.

```go
// CURRENT:
case schema.CardinalityUnknown:
    // Same as CardinalityMany - use add-wins
    it.emitted = make(map[any]bool)
    it.tombstones = make(map[any]uint64)

// CORRECT:
case schema.CardinalityUnknown:
    // Default is CardinalityOne (LWW) — no state needed
```

And in the main loop, route CardinalityUnknown to CardinalityOne:

```go
// CURRENT:
case schema.CardinalityUnknown:
    if result := it.processAddWins(datom); result != nil {
        ...
    }

// CORRECT:
case schema.CardinalityUnknown:
    // Default is CardinalityOne — same as CardinalityOne path
    if isNewGroup {
        if datom.Op == datalog.OpCRDTRemove {
            continue
        }
        it.currentDatom = datom
        return true
    }
    continue
```

#### 4. tx.Remove() accepts CardinalityOne

**File**: `database.go`, `tx.Remove()` method

Currently rejects CardinalityOne with an error. Should write `OpCRDTRemove`.

```go
// CURRENT:
case schema.CardinalityOne:
    return fmt.Errorf("Remove() not supported for cardinality-one ...")

// CORRECT:
case schema.CardinalityOne:
    // Write tombstone — CRDTResolvingIterator checks Op on first entry
    elemID := t.db.clock.Next()
    t.datoms = append(t.datoms, datalog.Datom{
        E:  e,
        A:  a,
        V:  v,
        Tx: elemID,
        Op: datalog.OpCRDTRemove,
    })
```

#### 5. tx.Remove() schemaless default is CardinalityOne

**File**: `database.go`, `tx.Remove()` schemaless/undefined path

Currently defaults to CardinalityMany for schemaless removes. Should default
to CardinalityOne, consistent with the schemaless default.

```go
// CURRENT:
} else {
    card = schema.CardinalityMany
}

// CORRECT:
} else {
    card = schema.CardinalityOne
}
```

#### 6. CRDTResolvingIterator always applied (done)

Removed `m.schema != nil` guards from all 8 wrapping sites:

| # | File | Line | Context |
|---|------|------|---------|
| 1 | `matcher_relations.go` | 382 | Unbound mask iterator path |
| 2 | `matcher_relations.go` | 416 | Unbound regular iterator path |
| 3 | `hash_join_matcher.go` | 200 | Hash join scan |
| 4 | `hash_join_matcher.go` | 571 | Merge join scan |
| 5 | `matcher_iterator_reusing.go` | 73 | Reusing iterator (bound queries) |
| 6 | `matcher_iterator_nonreusing.go` | 80 | Non-reusing iterator (bound queries) |
| 7 | `simple_batch_scanner.go` | 76 | Batch scanner |
| 8 | `batch_iterator.go` | 259 | Batch iterator |

Sites NOT changed (cardinality lookups — correct as-is):

| File | Line | Context |
|------|------|---------|
| `matcher_relations.go` | 85 | Vector cardinality intercept |
| `matcher_relations.go` | 122 | V-bound NeedsValidation |
| `matcher_relations.go` | 236 | Unbound cardinality determination |
| `matcher_relations.go` | 1097 | Cardinality helper |
| `matcher.go` | 212 | Cache cardinality lookup |
| `matcher.go` | 555 | chooseIndex E+A bound |
| `matcher.go` | 620 | chooseIndex A-only bound |
| `matcher.go` | 798 | matchFromCache cardinality |
| `matcher.go` | 952 | matchWithBindingsFromCache cardinality |

### Test Changes

Tests are the contract of correctness. They are written FIRST and must fail
before implementation. Every test encodes a design decision from this plan.

#### Existing tests to update

**`crdt_schemaless_attr_test.go`**: Tests assumed schemaless = add-wins.
Schemaless = CardinalityOne (LWW).

- `TestSchemalessAttrMultipleWrites`: multiple writes to same (E, A) should
  return only the latest value, not all values
- `TestSchemalessAttrBoundQuery_BugRepro`: should continue to pass (this IS
  the original bug reproduction — schema exists, attr not registered, bound query)
- `TestSchemalessAttrUnboundQuery_BugRepro`: should continue to pass

**`TestRemoveCardinalityValidation`**: Remove on CardinalityOne should succeed,
not error. Remove on unknown/schemaless should still succeed (now as
CardinalityOne, not add-wins).

#### New tests: CardinalityOne Remove (schema-defined)

These use a database with schema where the attribute is explicitly CardinalityOne.

1. **Remove round-trip**: Add value, Remove, query → attribute doesn't exist.
   Test with BOTH bound and unbound queries.
2. **Remove after overwrite**: Add "Alice", Add "Bob", Remove (any V) →
   attribute doesn't exist. V is irrelevant for CardinalityOne remove — the
   remove has highest Tx, so the attribute is gone regardless of what V was
   passed to Remove().
3. **Remove then re-add**: Add "Alice", Remove, Add "Bob" → "Bob" is current.
   The Add has higher Tx than the Remove.
4. **Remove before any add**: Remove first, then Add → value exists. Add has
   higher Tx, wins over pre-existing tombstone.
5. **V is irrelevant**: Add "Alice", Remove("Bob") → attribute doesn't exist.
   Even though "Bob" was never the value, the OpCRDTRemove at highest Tx means
   the attribute doesn't exist. CardinalityOne has one value; Remove removes it.
6. **Multiple entities**: Add value for entity1 and entity2. Remove entity1's
   value. entity2's value unaffected.

#### New tests: Schemaless CardinalityOne (default)

These use a database WITHOUT schema (or with schema where the attribute is
not registered). Exercises the CardinalityUnknown → CardinalityOne default.

7. **Schemaless LWW**: multiple `tx.Add()` to same (E, A) → only latest
   returned. Test with BOTH bound and unbound queries to verify both paths
   use CardinalityOne resolution.
8. **Schemaless remove**: `tx.Add()` then `tx.Remove()` → attribute doesn't
   exist. Test with BOTH bound and unbound queries.
9. **Schemaless remove then re-add**: Same as test #3 but without schema.
10. **Schema exists, attribute not registered**: Database has schema with other
    attributes. Unregistered attribute defaults to CardinalityOne. Multiple
    writes return only latest. Remove works.

#### New tests: Query path coverage

These verify that all query paths produce consistent results after removes.

11. **Bound query after remove**: E and A bound via `:in`, value was removed →
    empty result. Exercises `matcher_iterator_reusing.go` and
    `matcher_iterator_nonreusing.go` paths.
12. **V-bound query after remove**: Query `[?e :name "Alice"]` after Remove →
    empty result. Exercises `validatingVBoundIterator` path.
13. **Unbound query after remove**: Full scan `[?e ?a ?v]` after Remove →
    entity/attribute pair absent from results. Exercises `matcher_relations.go`
    unbound paths.

#### New tests: Nil-schema matcher

14. **Nil-schema matcher**: Data written with schema (tx.Set, OpNone), queried
    through BadgerMatcher created without schema → CardinalityOne default
    works, returns data. Fixes `TestSchemaUniquenessValue`.

#### Regression: CardinalityMany unaffected

15. **CardinalityMany add-wins still works**: Schema-defined CardinalityMany
    attribute, multiple adds → all non-tombstoned values returned. Verifies
    CardinalityUnknown → CardinalityOne change doesn't affect schema-defined
    CardinalityMany.
16. **CardinalityMany remove still works**: Schema-defined CardinalityMany
    with tombstone → tombstoned value absent, others present.
17. **CardinalityVector still works**: Schema-defined CardinalityVector →
    ordered vector returned correctly. Verifies no regression from
    CRDTResolvingIterator changes.

### Files Modified (summary)

| File | Change |
|------|--------|
| `database.go` | Revert schemaless Add Op; Remove accepts CardinalityOne; schemaless Remove default |
| `crdt_resolving_iterator.go` | CardinalityOne checks Op; CardinalityUnknown = CardinalityOne |
| `crdt_schemaless_attr_test.go` | Update for LWW semantics |
| `crdt_many_test.go` | Update TestRemoveCardinalityValidation |
| New test file or additions | CardinalityOne remove tests |
| `matcher_relations.go` | Already done (8-site change) |
| `hash_join_matcher.go` | Already done (8-site change) |
| `matcher_iterator_reusing.go` | Already done (8-site change) |
| `matcher_iterator_nonreusing.go` | Already done (8-site change) |
| `simple_batch_scanner.go` | Already done (8-site change) |
| `batch_iterator.go` | Already done (8-site change) |

---

## Resolution (2026-05-25)

**Resolved.** The plan above was carried out. `CRDTResolvingIterator` routes `CardinalityUnknown` (schemaless / attribute not in schema) to CardinalityOne/LWW resolution: it emits the first (highest-Tx) entry, skips the group when that entry is `OpCRDTRemove`, and keeps no add-wins state (`crdt_resolving_iterator.go:236-245, 285-287`). The `m.schema != nil` guards were removed so resolution is always applied.

Verified: the original reproduction `TestSchemalessAttrBoundQuery_BugRepro` (schema present, attribute unregistered, bound query) passes in **both** cache-enabled and cache-disabled modes, as do `TestSchemalessAttrUnboundQuery_BugRepro`, `TestSchemalessAttrMultipleWrites` (LWW: multiple writes return only the latest), and `TestSchemalessAttr_UnregisteredDefaultsToCardinalityOne`.

This is the same "schemaless = CardinalityOne" treatment later applied to the V-bound validation gate — see `resolved/BUG_EMPTY_STRING_VALUE_MATCHES_AS_WILDCARD.md`.
