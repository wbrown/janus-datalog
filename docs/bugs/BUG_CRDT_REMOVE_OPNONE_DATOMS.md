# BUG: CRDT Remove does not cancel OpNone datoms from EDN import

## Status

**Open.** Discovered 2026-03-26 during downstream test investigation.

## Summary

`tx.Remove()` on a cardinality-many attribute has no effect when the existing
value was asserted via `Store().Assert()` with `Op: datalog.OpNone` (the
default for EDN import). The CRDT resolver does not treat `OpNone` as an add
that can be cancelled by `OpCRDTRemove`.

## Reproduction

```go
func TestRemoveDoesNotCancelOpNoneDatom(t *testing.T) {
    // Create database with schema
    db, cleanup := createTestDB(t)
    defer cleanup()

    // Register cardinality-many attribute
    s := schema.NewSchemaBuilder()
    s.Attribute(":entity/state").Type(schema.TypeKeyword).Many().Add()
    db.SetSchema(s.Build())

    entity := datalog.NewIdentity("test-entity")
    stateKW := datalog.NewKeyword(":entity/state")
    unconscious := datalog.NewKeyword(":entity.state/unconscious")

    // Step 1: Assert with OpNone (simulates EDN import path)
    err := db.Store().Assert([]datalog.Datom{{
        E:  entity,
        A:  stateKW,
        V:  unconscious,
        Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
        Op: datalog.OpNone,
    }})
    require.NoError(t, err)

    // Verify the value exists
    vals, err := db.Matcher().(*BadgerMatcher).LookupAllAttributes(entity, stateKW)
    require.NoError(t, err)
    require.Len(t, vals, 1, "state should exist after Assert")

    // Step 2: Remove via transaction (simulates SaveStruct with empty slice)
    tx := db.NewTransaction()
    err = tx.Remove(entity, stateKW, unconscious)
    require.NoError(t, err)
    _, err = tx.Commit()
    require.NoError(t, err)

    // Step 3: Verify the value was removed
    vals, err = db.Matcher().(*BadgerMatcher).LookupAllAttributes(entity, stateKW)
    require.NoError(t, err)
    assert.Empty(t, vals, "state should be empty after Remove — BUT THIS FAILS")
    // Actual: vals still contains [:entity.state/unconscious]
}
```

## How it manifests

A downstream application imports module data from EDN files via
`db.Store().Import(f)`, which calls `Store().Assert()` for each batch of
datoms. These datoms have `Op: datalog.OpNone`.

Later, test code tries to clear the `:entity/state` attribute on an entity
using `SaveStruct` with an empty `State` slice. `SaveStruct` calls
`tx.Remove()` for each existing value, which writes a `OpCRDTRemove` tombstone
datom. However, the CRDT resolver does not cancel the original `OpNone` datom,
so the state persists.

The same issue affects any code path that:
1. Reads data imported from EDN (OpNone)
2. Tries to remove cardinality-many values via `tx.Remove()` or `SaveStruct`

## Root cause (hypothesis)

The CRDT set resolver for cardinality-many attributes uses add-wins semantics:
a value is present if its latest add has a higher Lamport than its latest
remove. But `OpNone` datoms (from EDN import / `Assert()`) may not be
recognized as "adds" by the resolver, so the `OpCRDTRemove` tombstone has
nothing to cancel.

The relevant code path:
- `Transaction.Remove()` at `database.go:1383` writes `OpCRDTRemove` tombstone
- `BadgerMatcher.LookupAllAttributes()` at `matcher.go:1079` resolves the CRDT set
- The CRDT resolution logic needs to treat `OpNone` as equivalent to `OpCRDTAdd`

## Impact

- `SaveStruct` with empty slice on cardinality-many fields silently fails to
  clear values that were imported from EDN
- Any CRDT Remove against EDN-imported data is a no-op
- The `SaveStruct` documentation promises "empty slice clears all existing
  values" but this contract is broken for EDN-imported data

## Workaround

None known within the current API. Direct `Retract()` may work but uses a
different code path (legacy retraction vs CRDT tombstones).

---

## Investigation update (2026-03-26)

### Initial hypothesis was wrong

Per `BUG_CRDT_REMOVE_OPNONE_DATOMS_INVESTIGATION.md`, the OpNone premise is
incorrect. The EDN preserves CRDT ops faithfully:

- CardinalityMany attributes are exported/imported with `:op/add` (`OpCRDTAdd`)
- CardinalityOne attributes use `:op/none` (`OpNone`)
- The full pipeline (Transaction.Add → Export → Import → Assert) preserves ops

Checked actual EDN data for the failing entity — the `:entity/state` datom
has `:op/add`, not `:op/none`.

The Op is `:op/add`, not `:op/none`. The reproduction test in the original
report is artificial and doesn't match the real pipeline.

### Actual root cause: Lamport clock not advanced after Import

`Database.Import()` calls `d.store.Assert(datoms)` — raw `BadgerStore.Assert()`.
This writes datoms with their original Lamport values (e.g., Lamport=2830 from
the source database) directly to badger. **It does NOT advance the Database's
Lamport clock.**

The sequence:
1. `NewDatabase(...)` — opens DB, clock restored from `MaxElementID` (0 for fresh DB)
2. `db.Store().Import(f)` — writes datoms with Lamport=2830 directly to badger
3. Clock is still at Lamport=0
4. `tx.Remove()` calls `clock.Next()` → gets Lamport=1
5. Lamport 1 < Lamport 2830 → add-wins → Remove silently ignored

### Fix

`Database.Import()` must advance the Lamport clock after importing. The clock's
`Restore()` method does exactly this. After all batches are imported, scan for
the max ElementID and restore:

```go
// At end of Import(), after all batches:
maxElementID, err := d.store.MaxElementID()
if err != nil {
    return fmt.Errorf("failed to get max ElementID after import: %w", err)
}
if !maxElementID.IsZero() {
    d.clock.Restore(maxElementID)
}
```

This is the same call made in `NewDatabase()` at database open time
(database.go:139-145). Import just needs to repeat it after writing.